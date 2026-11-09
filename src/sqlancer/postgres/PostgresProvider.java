package sqlancer.postgres;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.postgres.gen.PostgresAlterTableGenerator;
import sqlancer.postgres.gen.PostgresAnalyzeGenerator;
import sqlancer.postgres.gen.PostgresClusterGenerator;
import sqlancer.postgres.gen.PostgresCommentGenerator;
import sqlancer.postgres.gen.PostgresDeleteGenerator;
import sqlancer.postgres.gen.PostgresDiscardGenerator;
import sqlancer.postgres.gen.PostgresDropIndexGenerator;
import sqlancer.postgres.gen.PostgresExplainGenerator;
import sqlancer.postgres.gen.PostgresIndexGenerator;
import sqlancer.postgres.gen.PostgresInsertGenerator;
import sqlancer.postgres.gen.PostgresNotifyGenerator;
import sqlancer.postgres.gen.PostgresReindexGenerator;
import sqlancer.postgres.gen.PostgresSequenceGenerator;
import sqlancer.postgres.gen.PostgresSetGenerator;
import sqlancer.postgres.gen.PostgresStatisticsGenerator;
import sqlancer.postgres.gen.PostgresTableGenerator;
import sqlancer.postgres.gen.PostgresTableSpaceGenerator;
import sqlancer.postgres.gen.PostgresTransactionGenerator;
import sqlancer.postgres.gen.PostgresTruncateGenerator;
import sqlancer.postgres.gen.PostgresUpdateGenerator;
import sqlancer.postgres.gen.PostgresVacuumGenerator;
import sqlancer.postgres.gen.PostgresViewGenerator;
import sqlancer.postgres.oracle.PostgresVerticalPartitioningOracle;
import sqlancer.postgres.oracle.PostgresVerticalPartitioningOracle.AdvancedFeature;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresIndex;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

// EXISTS
// IN
@AutoService(DatabaseProvider.class)
public class PostgresProvider extends SQLProviderAdapter<PostgresGlobalState, PostgresOptions> {

    private static final int VP_BASE_TABLES_PER_BATCH = 2;
    private static final int VP_REPLACEMENT_RETRY_COUNT = 10;
    private static final int VP_ROW_FILL_RETRY_FACTOR = 4;
    private static final int VP_SMALL_MIN_ROWS = 8;
    private static final int VP_SMALL_INSERT_ATTEMPTS = 12;
    private static final int VP_LARGE_INSERT_BATCH = 256;

    /**
     * Generate only data types and expressions that are understood by PQS.
     */
    public static boolean generateOnlyKnown;

    protected String entryURL;
    protected String username;
    protected String password;
    protected String entryPath;
    protected String host;
    protected int port;
    protected String testURL;
    protected String databaseName;
    protected String createDatabaseCommand;
    protected String extensionsList;

    public PostgresProvider() {
        super(PostgresGlobalState.class, PostgresOptions.class);
    }

    protected PostgresProvider(Class<PostgresGlobalState> globalClass, Class<PostgresOptions> optionClass) {
        super(globalClass, optionClass);
    }

    public enum Action implements AbstractAction<PostgresGlobalState> {
        ANALYZE(PostgresAnalyzeGenerator::create), //
        ALTER_TABLE(g -> PostgresAlterTableGenerator
                .create(g.getSchema().getRandomTable(t -> !t.isView() && !t.isPartitionChild()), g, generateOnlyKnown)), //
        CLUSTER(PostgresClusterGenerator::create), //
        COMMIT(g -> {
            SQLQueryAdapter query;
            if (Randomly.getBoolean()) {
                query = new SQLQueryAdapter("COMMIT", true);
            } else if (Randomly.getBoolean()) {
                query = PostgresTransactionGenerator.executeBegin();
            } else {
                query = new SQLQueryAdapter("ROLLBACK", true);
            }
            return query;
        }), //
        CREATE_STATISTICS(PostgresStatisticsGenerator::insert), //
        DROP_STATISTICS(PostgresStatisticsGenerator::remove), //
        ALTER_STATISTICS(PostgresStatisticsGenerator::alter), //
        DELETE(PostgresDeleteGenerator::create), //
        DISCARD(PostgresDiscardGenerator::create), //
        DROP_INDEX(PostgresDropIndexGenerator::create), //
        INSERT(PostgresInsertGenerator::insert), //
        UPDATE(PostgresUpdateGenerator::create), //
        TRUNCATE(PostgresTruncateGenerator::create), //
        VACUUM(PostgresVacuumGenerator::create), //
        REINDEX(PostgresReindexGenerator::create), //
        SET(PostgresSetGenerator::create), //
        CREATE_INDEX(PostgresIndexGenerator::generate), //
        SET_CONSTRAINTS((g) -> {
            StringBuilder sb = new StringBuilder();
            sb.append("SET CONSTRAINTS ALL ");
            sb.append(Randomly.fromOptions("DEFERRED", "IMMEDIATE"));
            return new SQLQueryAdapter(sb.toString());
        }), //
        RESET_ROLE((g) -> new SQLQueryAdapter("RESET ROLE")), //
        COMMENT_ON(PostgresCommentGenerator::generate), //
        RESET((g) -> new SQLQueryAdapter("RESET ALL") /*
                                                       * https://www.postgresql.org/docs/13/sql-reset.html TODO: also
                                                       * configuration parameter
                                                       */), //
        NOTIFY(PostgresNotifyGenerator::createNotify), //
        LISTEN((g) -> PostgresNotifyGenerator.createListen()), //
        UNLISTEN((g) -> PostgresNotifyGenerator.createUnlisten()), //
        CREATE_SEQUENCE(PostgresSequenceGenerator::createSequence), //
        EXPLAIN(PostgresExplainGenerator::create), //
        CREATE_VIEW(PostgresViewGenerator::create), //
        CREATE_TABLESPACE(PostgresTableSpaceGenerator::generate);

        private final SQLQueryProvider<PostgresGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<PostgresGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(PostgresGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    protected static int mapActions(PostgresGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        boolean preserveVerticalPartitioningRows = usesVerticalPartitioningOracle(globalState.getDbmsSpecificOptions())
                && !globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes;
        int nrPerformed;
        switch (a) {
        case CREATE_INDEX:
        case CLUSTER:
            nrPerformed = r.getInteger(0, 3);
            break;
        case CREATE_STATISTICS:
            nrPerformed = r.getInteger(0, 5);
            break;
        case ALTER_STATISTICS:
            nrPerformed = r.getInteger(0, 2);
            break;
        case DISCARD:
        case DROP_INDEX:
            nrPerformed = r.getInteger(0, 5);
            break;
        case COMMIT:
            nrPerformed = r.getInteger(0, 0);
            break;
        case ALTER_TABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case REINDEX:
        case RESET:
            nrPerformed = r.getInteger(0, 3);
            break;
        case DELETE:
        case RESET_ROLE:
        case SET:
            nrPerformed = a == Action.DELETE && preserveVerticalPartitioningRows ? 0 : r.getInteger(0, 5);
            break;
        case ANALYZE:
            nrPerformed = r.getInteger(0, 3);
            break;
        case VACUUM:
        case SET_CONSTRAINTS:
        case COMMENT_ON:
        case NOTIFY:
        case LISTEN:
        case UNLISTEN:
        case CREATE_SEQUENCE:
        case DROP_STATISTICS:
        case TRUNCATE:
            nrPerformed = a == Action.TRUNCATE && preserveVerticalPartitioningRows ? 0 : r.getInteger(0, 2);
            break;
        case CREATE_VIEW:
            nrPerformed = r.getInteger(0, 2);
            break;
        case CREATE_TABLESPACE:
            nrPerformed = r.getInteger(0, 2);
            break;
        case UPDATE:
            nrPerformed = r.getInteger(0, 10);
            break;
        case INSERT:
            nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            break;
        case EXPLAIN:
            nrPerformed = r.getInteger(0, 1);
            break;
        default:
            throw new AssertionError(a);
        }
        return nrPerformed;

    }

    @Override
    public void generateDatabase(PostgresGlobalState globalState) throws Exception {
        readFunctions(globalState);
        boolean usesVerticalPartitioningOracle = usesVerticalPartitioningOracle(globalState);
        if (usesVerticalPartitioningOracle) {
            PostgresVerticalPartitioningOracle.initializeStatsLogging();
            createVerticalPartitioningTableBatch(globalState);
        } else {
            createTables(globalState, Randomly.fromOptions(4, 5, 6));
        }
        if (!usesVerticalPartitioningOracle) {
            prepareTables(globalState);
        }

        extensionsList = globalState.getDbmsSpecificOptions().extensions;
        if (!extensionsList.isEmpty()) {
            String[] extensionNames = extensionsList.split(",");

            /*
             * To avoid of a test interference with an extension objects, create them in a separate schema. Of course,
             * they must be truly relocatable.
             */
            globalState.executeStatement(new SQLQueryAdapter("CREATE SCHEMA extensions;", true));
            for (int i = 0; i < extensionNames.length; i++) {
                globalState.executeStatement(new SQLQueryAdapter(
                        "CREATE EXTENSION " + extensionNames[i] + " WITH SCHEMA extensions;", true));
            }
        }
        if (usesVerticalPartitioningOracle) {
            PostgresVerticalPartitioningOracle.logSchemaShapeStats(globalState, "post-generate");
        }
    }

    private boolean usesVerticalPartitioningOracle(PostgresGlobalState globalState) {
        return usesVerticalPartitioningOracle(globalState.getDbmsSpecificOptions());
    }

    private static boolean usesVerticalPartitioningOracle(PostgresOptions options) {
        return options.getTestOracleFactory().stream().anyMatch(o -> o == PostgresOracleFactory.VERTICAL_PARTITIONING || o == PostgresOracleFactory.ECST);
    }

    public static void replaceVerticalPartitioningTableAfterSuccessfulCheck(PostgresGlobalState globalState,
            String oldTableName) throws Exception {
        String profile = PostgresVerticalPartitioningOracle.getCreatedBaseTableProfile(
                globalState.getDatabaseName(), oldTableName);
        String replacementProfile;
        switch (profile) {
        case "SMALL":
            replacementProfile = "EMPTY";
            break;
        case "EMPTY":
            replacementProfile = "SMALL";
            break;
        case "LARGE":
        case "UNDERFILLED":
            replacementProfile = globalState.getDbmsSpecificOptions().verticalPartitioningRows > 0 ? "LARGE" : "SMALL";
            break;
        default:
            replacementProfile = globalState.nextVerticalPartitioningProfile();
            break;
        }
        PostgresSchema.PostgresTable replacement = createValidVerticalPartitioningBaseTable(globalState,
                replacementProfile);
        if (replacement == null) {
            logVerticalPartitioningReplacementFailure(globalState, oldTableName);
            throw new IgnoreMeException();
        }
        if ("LARGE".equals(profile) && !"LARGE".equals(PostgresVerticalPartitioningOracle
                .getCreatedBaseTableProfile(globalState.getDatabaseName(), replacement.getName()))) {
            // A bounded population attempt can return an underfilled table. Keep the existing LARGE candidate
            // available until a replacement really reaches the configured target.
            globalState.executeStatement(new SQLQueryAdapter(
                    "DROP TABLE IF EXISTS " + quoteIdentifier(replacement.getName()) + " CASCADE", true));
            globalState.updateSchema();
            return;
        }
        globalState.executeStatement(
                new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(oldTableName) + " CASCADE", true));
        globalState.updateSchema();
    }

    public static void ensureVerticalPartitioningBaseTables(PostgresGlobalState globalState, int minBaseTables)
            throws Exception {
        globalState.updateSchema();
        int guard = 0;
        while (countVerticalPartitioningBaseTables(globalState) < minBaseTables && guard++ < minBaseTables * 20) {
            PostgresSchema.PostgresTable table = createValidVerticalPartitioningBaseTable(globalState);
            if (table == null) {
                throw new IgnoreMeException();
            }
            globalState.updateSchema();
        }
    }

    private static void createVerticalPartitioningTableBatch(PostgresGlobalState globalState) throws Exception {
        if (globalState.getDbmsSpecificOptions().verticalPartitioningDeepExploration) {
            createDeepPartitionCandidate(globalState);
        }
        for (int i = 0; i < VP_BASE_TABLES_PER_BATCH; i++) {
            PostgresSchema.PostgresTable table = createValidVerticalPartitioningBaseTable(globalState);
            if (table == null) {
                throw new IgnoreMeException();
            }
        }
    }

    private static void createDeepPartitionCandidate(PostgresGlobalState globalState) throws Exception {
        String suffix = String.valueOf(globalState.getSchema().getDatabaseTables().size());
        String parent = "partition_candidate_" + suffix;
        String lower = parent + "_lo";
        String upper = parent + "_hi";
        globalState.executeStatement(
                new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(parent) + " CASCADE", true));
        globalState.executeStatement(new SQLQueryAdapter("CREATE TABLE " + quoteIdentifier(parent)
                + " (vp_key BIGINT NOT NULL, vp_payload TEXT) PARTITION BY RANGE (vp_key)", true));
        globalState.executeStatement(new SQLQueryAdapter("CREATE TABLE " + quoteIdentifier(lower) + " PARTITION OF "
                + quoteIdentifier(parent) + " FOR VALUES FROM (MINVALUE) TO (0)", true));
        globalState.executeStatement(new SQLQueryAdapter("CREATE TABLE " + quoteIdentifier(upper) + " PARTITION OF "
                + quoteIdentifier(parent) + " FOR VALUES FROM (0) TO (MAXVALUE)", true));
        globalState.executeStatement(new SQLQueryAdapter(
                "INSERT INTO " + quoteIdentifier(parent) + " (vp_key, vp_payload) VALUES (-1, 'lower'), (1, 'upper')",
                true));
        globalState.updateSchema();
        PostgresSchema.PostgresTable table = findVerticalPartitioningBaseTable(globalState, parent);
        if (table != null) {
            mutateVerticalPartitioningCandidate(globalState, table, false);
        }
    }

    private static PostgresSchema.PostgresTable createValidVerticalPartitioningBaseTable(
            PostgresGlobalState globalState) throws Exception {
        return createValidVerticalPartitioningBaseTable(globalState, globalState.nextVerticalPartitioningProfile());
    }

    private static PostgresSchema.PostgresTable createValidVerticalPartitioningBaseTable(
            PostgresGlobalState globalState, String profile) throws Exception {
        // Keep the requested profile across retries: hard-to-populate schemas must not bias creation toward SMALL.
        boolean keepEmpty = "EMPTY".equals(profile);
        boolean small = "SMALL".equals(profile);
        for (int attempt = 0; attempt < VP_REPLACEMENT_RETRY_COUNT; attempt++) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size() + attempt);
            try {
                globalState.executeStatement(
                        new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(tableName) + " CASCADE", true));
                SQLQueryAdapter createTable = PostgresTableGenerator.generate(tableName, globalState.getSchema(),
                        generateOnlyKnown, globalState);
                addPostgres10VpCreateErrors(globalState, createTable);
                globalState.executeStatement(createTable);
                globalState.updateSchema();
                PostgresSchema.PostgresTable table = findVerticalPartitioningBaseTable(globalState, tableName);
                if (table != null) {
                    ensureVerticalPartitioningDefaultPartition(globalState, table);
                    globalState.updateSchema();
                    table = findVerticalPartitioningBaseTable(globalState, tableName);
                    if (table == null) {
                        continue;
                    }
                    ensurePostgresVpTypeColumns(globalState, table);
                    globalState.updateSchema();
                    table = findVerticalPartitioningBaseTable(globalState, tableName);
                    if (table == null) {
                        continue;
                    }
                    if (!keepEmpty && small) {
                        seedSmallVerticalPartitioningBaseTable(globalState, table);
                    }
                    mutateVerticalPartitioningCandidate(globalState, table, keepEmpty);
                    globalState.updateSchema();
                    table = findVerticalPartitioningBaseTable(globalState, tableName);
                    if (table != null && isValidVerticalPartitioningBaseTable(globalState, table)) {
                        if (small
                                && getVerticalPartitioningRowCount(globalState, table.getName()) < VP_SMALL_MIN_ROWS) {
                            globalState.executeStatement(new SQLQueryAdapter(
                                    "DROP TABLE IF EXISTS " + quoteIdentifier(tableName) + " CASCADE", true));
                            continue;
                        }
                        if (!keepEmpty && !small) {
                            padVerticalPartitioningBaseTable(globalState, table);
                            applyPostgresVpStressProfile(globalState, table);
                            recordPostgresVpProfileNonnullRows(globalState, table);
                            long filledRows = getVerticalPartitioningRowCount(globalState, table.getName());
                            if (filledRows < globalState.getDbmsSpecificOptions().verticalPartitioningRows) {
                                if (attempt + 1 < VP_REPLACEMENT_RETRY_COUNT) {
                                    globalState.executeStatement(new SQLQueryAdapter(
                                            "DROP TABLE IF EXISTS " + quoteIdentifier(tableName) + " CASCADE", true));
                                    continue;
                                }
                                // Keep the SQLancer-random table, but never label
                                // an under-filled table as LARGE or as the small
                                // 8-row profile when it already has substantial
                                // cardinality.
                                if (filledRows >= VP_SMALL_MIN_ROWS) {
                                    PostgresVerticalPartitioningOracle.recordCreatedBaseTableProfile(
                                            globalState.getDatabaseName(), table.getName(),
                                            filledRows < 192 ? "SMALL" : "UNDERFILLED");
                                    return table;
                                }
                                globalState.executeStatement(new SQLQueryAdapter(
                                        "DROP TABLE IF EXISTS " + quoteIdentifier(tableName) + " CASCADE", true));
                                continue;
                            }
                        }
                        PostgresVerticalPartitioningOracle.recordCreatedBaseTableProfile(globalState.getDatabaseName(),
                                table.getName(), keepEmpty ? "EMPTY" : small ? "SMALL" : "LARGE");
                        return table;
                    }
                }
                globalState.executeStatement(
                        new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(tableName) + " CASCADE", true));
            } catch (IgnoreMeException ignored) {
            }
        }
        return null;
    }

    /**
     * Random schemas only rarely contain PostgreSQL's collection, range, and catalog-backed types. Add nullable probe
     * columns so the VP oracle can exercise those executor paths without changing existing row semantics.
     */
    private static void ensurePostgresVpTypeColumns(PostgresGlobalState globalState, PostgresSchema.PostgresTable table)
            throws Exception {
        String tableName = quoteIdentifier(table.getName());
        ExpectedErrors errors = ExpectedErrors.from("already exists", "cannot alter table", "does not exist",
                "must be owner of relation", "cannot add column to a partitioned table");
        String token = globalState.getRandomly().getString().replace("'", "''");
        String jsonValue = globalState.getRandomly().getString().replace("'", "''");
        String query = Randomly.getBoolean() ? "alpha" : "alpha & beta";
        String collation = null;
        try (Statement s = globalState.getConnection().createStatement(); ResultSet rs = s.executeQuery(
                "SELECT quote_ident(n.nspname)||'.'||quote_ident(c.collname) FROM pg_collation c JOIN pg_namespace n ON n.oid=c.collnamespace WHERE c.collencoding IN (-1, pg_char_to_encoding(current_setting('server_encoding'))) AND c.collisdeterministic AND c.collname <> 'default' ORDER BY random() LIMIT 1")) {
            if (rs.next())
                collation = rs.getString(1);
        } catch (SQLException ignored) {
        }
        String[][][] profiles = {
                { { "vp_pg_array_int", "BIGINT[]", "ARRAY[(abs(hashtext(ctid::text)) % 1000), 1]::BIGINT[]" },
                        { "vp_pg_array_text", "TEXT[]",
                                "ARRAY[(" + quoteSqlText(token) + " || ctid::text), 'x']::TEXT[]" },
                        { "vp_pg_array_numeric", "NUMERIC[]",
                                "ARRAY[(abs(hashtext(ctid::text)) % 1000)::numeric, 0.5]::NUMERIC[]" } },
                { { "vp_pg_range", "int4range",
                        "int4range((abs(hashtext(ctid::text)) % 1000) - 500, (abs(hashtext(ctid::text)) % 1000) - 490, '[)')" },
                        { "vp_pg_daterange", "daterange",
                                "daterange(DATE '2020-01-01' + (abs(hashtext(ctid::text)) % 3000), DATE '2020-01-01' + (abs(hashtext(ctid::text)) % 3000) + 1 + (abs(hashtext(ctid::text || 'w')) % 30), '[)')" },
                        { "vp_pg_tsrange", "tsrange",
                                "tsrange(TIMESTAMP '2020-01-01' + (abs(hashtext(ctid::text)) % 3000) * interval '1 day', TIMESTAMP '2020-01-01' + ((abs(hashtext(ctid::text)) % 3000) + 1 + (abs(hashtext(ctid::text || 'w')) % 30)) * interval '1 day', '[)')" } },
                { { "vp_pg_inet", "inet", "('192.0.2.' || (1 + abs(hashtext(ctid::text)) % 200)::text)::inet" },
                        { "vp_pg_cidr", "cidr",
                                "('192.0.' || (abs(hashtext(ctid::text)) % 256)::text || '.0/24')::cidr" },
                        { "vp_pg_macaddr", "macaddr",
                                "('08:00:2b:' || lpad(to_hex(abs(hashtext(ctid::text)) % 256), 2, '0') || ':' || lpad(to_hex(abs(hashtext(ctid::text || 'b')) % 256), 2, '0') || ':' || lpad(to_hex(abs(hashtext(ctid::text || 'c')) % 256), 2, '0'))::macaddr" } },
                { { "vp_pg_uuid", "uuid", "md5(" + quoteSqlText(token) + " || ctid::text)::uuid" },
                        { "vp_pg_bytea", "bytea", "convert_to(" + quoteSqlText(token) + " || ctid::text, 'UTF8')" } },
                { { "vp_pg_tsvector", "tsvector", "to_tsvector('english', " + quoteSqlText(token) + " || ctid::text)" },
                        { "vp_pg_tsquery", "tsquery", "to_tsquery('english', '" + query + "')" } },
                { { "vp_pg_jsonb", "jsonb", "jsonb_build_object('k' || ctid::text, " + quoteSqlText(jsonValue)
                        + " || ctid::text, 'k', " + quoteSqlText(jsonValue)
                        + " || ctid::text, 'n', abs(hashtext(ctid::text)) % 10, 'nested', jsonb_build_object('n', abs(hashtext(ctid::text)) % 10, 'k', "
                        + quoteSqlText(jsonValue) + "), 'items', jsonb_build_array(ctid::text, 1))" } },
                { { "vp_pg_collated_text", "TEXT COLLATE " + collation, quoteSqlText(token) + " || ctid::text" } } };
        int mandatoryProfile = globalState.getRandomly().getInteger(0, profiles.length - 1);
        if (collation == null && mandatoryProfile == profiles.length - 1) {
            mandatoryProfile = 0;
        }
        for (int profileIndex = 0; profileIndex < profiles.length; profileIndex++) {
            String[][] profile = profiles[profileIndex];
            if (profileIndex != mandatoryProfile && !Randomly.getBoolean())
                continue;
            if (profile[0][0].equals("vp_pg_collated_text") && collation == null)
                continue;
            String profileName = profile[0][0].startsWith("vp_pg_array") ? "ARRAY_PROFILE"
                    : profile[0][0].contains("range") ? "RANGE_PROFILE" : profile[0][0].contains("inet")
                            || profile[0][0].contains("cidr") || profile[0][0].contains("mac")
                                    ? "NETWORK_PROFILE"
                                    : profile[0][0].contains("jsonb") ? "JSON_PROFILE"
                                            : profile[0][0].contains("collated") ? "COLLATION_PROFILE"
                                                    : profile[0][0].contains("tsvector")
                                                            || profile[0][0].contains("tsquery") ? "TEXT_SEARCH_PROFILE"
                                                                    : "POSTGRES_TYPE_COLUMNS";
            int selectedCount = globalState.getRandomly().getInteger(1, profile.length);
            List<Integer> candidateIndexes = new ArrayList<>();
            for (int i = 0; i < profile.length; i++) {
                candidateIndexes.add(i);
            }
            for (int i = 0; i < selectedCount; i++) {
                int candidatePosition = globalState.getRandomly().getInteger(0, candidateIndexes.size() - 1);
                String[] column = profile[candidateIndexes.remove(candidatePosition)];
                PostgresVerticalPartitioningOracle.recordProfileAttempt(profileName);
                globalState.executeStatement(new SQLQueryAdapter("ALTER TABLE " + tableName
                        + " ADD COLUMN IF NOT EXISTS " + quoteIdentifier(column[0]) + " " + column[1], errors, true));
                globalState.updateSchema();
                boolean created = globalState.getSchema().getDatabaseTables().stream()
                        .filter(t -> t.getName().equals(table.getName())).flatMap(t -> t.getColumns().stream())
                        .anyMatch(c -> c.getName().equals(column[0]));
                if (created)
                    PostgresVerticalPartitioningOracle.recordProfileColumnCreated(profileName);
                long rows = getVerticalPartitioningRowCount(globalState, table.getName());
                if (rows > 0) {
                    globalState.executeStatement(new SQLQueryAdapter(
                            "UPDATE " + tableName + " SET " + quoteIdentifier(column[0]) + " = " + column[2], errors,
                            true));
                    long nonNullRows = countNonNullRows(globalState, table.getName(), column[0]);
                    PostgresVerticalPartitioningOracle.recordProfileNonnullRows(profileName, nonNullRows);
                }
                PostgresVerticalPartitioningOracle.recordAdvancedFeature(AdvancedFeature.valueOf(profileName));
            }
        }
    }

    private static String quoteSqlText(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static long countNonNullRows(PostgresGlobalState globalState, String tableName, String columnName)
            throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + quoteIdentifier(tableName) + " WHERE " + quoteIdentifier(columnName)
                + " IS NOT NULL";
        try (Statement statement = globalState.getConnection().createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            return result.next() ? result.getLong(1) : 0L;
        }
    }

    private static void recordPostgresVpProfileNonnullRows(PostgresGlobalState globalState,
            PostgresSchema.PostgresTable table) throws Exception {
        globalState.updateSchema();
        PostgresSchema.PostgresTable current = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getName().equals(table.getName())).findFirst().orElse(table);
        for (PostgresColumn column : current.getColumns()) {
            String name = column.getName();
            String profile = name.startsWith("vp_pg_array_") ? "ARRAY_PROFILE" : name.contains("range")
                    ? "RANGE_PROFILE"
                    : (name.contains("inet") || name.contains("cidr") || name.contains("macaddr")) ? "NETWORK_PROFILE"
                            : name.contains("jsonb") ? "JSON_PROFILE"
                                    : name.contains("collated") ? "COLLATION_PROFILE"
                                            : (name.contains("tsvector") || name.contains("tsquery"))
                                                    ? "TEXT_SEARCH_PROFILE"
                                                    : name.startsWith("vp_pg_") ? "POSTGRES_TYPE_COLUMNS" : null;
            if (profile != null) {
                PostgresVerticalPartitioningOracle.recordProfileNonnullRows(profile,
                        countNonNullRows(globalState, table.getName(), name));
            }
        }
    }

    /**
     * The normal PostgreSQL mutation phase intentionally creates small random tables. VP needs a second, explicit size
     * phase to reach optimizer plans that only appear with real cardinality and statistics.
     */
    private static void padVerticalPartitioningBaseTable(PostgresGlobalState globalState,
            PostgresSchema.PostgresTable table) throws Exception {
        int targetRows = globalState.getDbmsSpecificOptions().verticalPartitioningRows;
        if (targetRows <= 0) {
            return;
        }
        long rowCount = getVerticalPartitioningRowCount(globalState, table.getName());
        int attempts = 0;
        int stalledAttempts = 0;
        int maxAttempts = Math.max(16, targetRows / VP_LARGE_INSERT_BATCH * VP_ROW_FILL_RETRY_FACTOR);
        while (rowCount < targetRows && attempts++ < maxAttempts && stalledAttempts < 4) {
            try {
                // Reuse rows that already satisfy this random schema instead of requiring every value in a
                // 256-row random VALUES statement to pass constraints. Unique constraints may reject copying;
                // retain random insertion as the fallback for those tables.
                int batchRows = Math.min(VP_LARGE_INSERT_BATCH, targetRows - (int) rowCount);
                List<PostgresColumn> writableColumns = table.getColumns().stream()
                        .filter(c -> !c.isGeneratedOrIdentity()).collect(Collectors.toList());
                if (rowCount > 0 && !writableColumns.isEmpty()) {
                    String columns = writableColumns.stream().map(c -> quoteIdentifier(c.getName()))
                            .collect(Collectors.joining(", "));
                    ExpectedErrors copyErrors = new ExpectedErrors();
                    PostgresCommon.addCommonInsertUpdateErrors(copyErrors);
                    copyErrors.add("duplicate key value violates unique constraint");
                    copyErrors.add("conflicting key value violates exclusion constraint");
                    copyErrors.add("no partition of relation");
                    SQLQueryAdapter copy = new SQLQueryAdapter("INSERT INTO " + quoteIdentifier(table.getName())
                            + " (" + columns + ") SELECT " + columns + " FROM "
                            + quoteIdentifier(table.getName()) + " CROSS JOIN generate_series(1, "
                            + ((batchRows + rowCount - 1) / rowCount) + ") AS vp_fill LIMIT " + batchRows, copyErrors);
                    if (globalState.executeStatement(copy)) {
                        long copiedRows = getVerticalPartitioningRowCount(globalState, table.getName());
                        if (copiedRows > rowCount) {
                            rowCount = copiedRows;
                            stalledAttempts = 0;
                            continue;
                        }
                    }
                }
                SQLQueryAdapter insert = PostgresInsertGenerator.insertRows(globalState, table,
                        rowCount == 0 ? 1 : batchRows);
                addPostgres10VpMutationErrors(globalState, insert);
                globalState.executeStatement(insert);
            } catch (IgnoreMeException ignored) {
                // Constraint conflicts are expected for generated schemas; retry
                // with a fresh INSERT shape until the bounded budget is exhausted.
            }
            long nextRowCount = getVerticalPartitioningRowCount(globalState, table.getName());
            stalledAttempts = nextRowCount > rowCount ? 0 : stalledAttempts + 1;
            rowCount = nextRowCount;
        }
    }

    private static void seedSmallVerticalPartitioningBaseTable(PostgresGlobalState globalState,
            PostgresSchema.PostgresTable table) throws Exception {
        for (int attempt = 0; attempt < VP_SMALL_INSERT_ATTEMPTS
                && getVerticalPartitioningRowCount(globalState, table.getName()) < VP_SMALL_MIN_ROWS; attempt++) {
            try {
                SQLQueryAdapter insert = PostgresInsertGenerator.insertRows(globalState, table);
                addPostgres10VpMutationErrors(globalState, insert);
                globalState.executeStatement(insert);
            } catch (IgnoreMeException ignored) {
                // Retry with a newly generated row batch after constraint conflicts.
            }
        }
    }

    /**
     * Add skewed values to optional VP probe columns so large-table plans see duplicates, NULLs, wide values, and
     * non-trivial collection shapes.
     */
    private static void applyPostgresVpStressProfile(PostgresGlobalState globalState,
            PostgresSchema.PostgresTable table) throws Exception {
        if (!Randomly.getBoolean()) {
            return;
        }
        String tableName = quoteIdentifier(table.getName());
        long rowCount = getVerticalPartitioningRowCount(globalState, table.getName());
        if (rowCount < 1000) {
            return;
        }
        String suffix = rowCount >= 10000 ? "10K" : "1K";
        ExpectedErrors errors = ExpectedErrors.from("column does not exist", "cannot update", "does not exist",
                "must be owner of relation", "index row size", "exceeds btree version", "violates check constraint",
                "violates not-null constraint", "violates partition constraint", "violates foreign key constraint",
                "duplicate key value violates unique constraint", "could not create unique index");
        String skew = "CASE WHEN abs(hashtext(ctid::text)) % 10 < 7 THEN NULL ELSE ";
        String rowIdentity = "((split_part(trim(both '()' from ctid::text), ',', 1)::bigint * 65536)"
                + " + split_part(trim(both '()' from ctid::text), ',', 2)::bigint)";
        String[] updates = {
                "vp_pg_array_int = " + skew + "ARRAY(SELECT CASE WHEN g = 1 THEN " + rowIdentity
                        + " ELSE (hashtext(ctid::text) % 17)::bigint END FROM generate_series(1, 32) g) END",
                "vp_pg_array_text = " + skew + "ARRAY(SELECT CASE WHEN g = 1 THEN ctid::text ELSE repeat('a', 128)"
                        + " || (g % 3)::text END FROM generate_series(1, 24) g)::text[] END",
                "vp_pg_array_numeric = " + skew + "ARRAY(SELECT CASE WHEN g = 1 THEN " + rowIdentity
                        + "::numeric ELSE 999999999999999999::numeric END FROM generate_series(1, 24) g) END",
                "vp_pg_jsonb = " + skew + "jsonb_build_object('row', ctid::text, 'dup', 1, 'nested',"
                        + " jsonb_build_object('deep', jsonb_build_object('value', repeat('x', 2048))), 'items',"
                        + " jsonb_build_array(1, 1, 1, repeat('z', 512))) END",
                "vp_pg_collated_text = " + skew + "repeat('wide-text-', 512) || ctid::text END" };
        for (String update : updates) {
            globalState.executeStatement(new SQLQueryAdapter("UPDATE " + tableName + " SET " + update, errors, true));
        }
        PostgresVerticalPartitioningOracle.recordBaseTableStressProfile("STRESS_" + suffix);
    }

    private static long getVerticalPartitioningRowCount(PostgresGlobalState globalState, String tableName)
            throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + quoteIdentifier(tableName);
        try (Statement statement = globalState.getConnection().createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            if (!result.next()) {
                throw new AssertionError("COUNT(*) returned no row for " + tableName);
            }
            return result.getLong(1);
        }
    }

    private static void ensureVerticalPartitioningDefaultPartition(PostgresGlobalState globalState,
            PostgresSchema.PostgresTable table) throws Exception {
        if (!table.isPartitioned() || !table.getPartitionChildren().isEmpty() || table.hasExpressionPartitionKey()
                || table.getPartitionKeyColumns().isEmpty()) {
            return;
        }
        ExpectedErrors errors = ExpectedErrors.from("relation already exists", "partition bound of relation",
                "cannot create partitioned table as inheritance child", "may not have a default partition");
        String temporary = table.getTableType() == PostgresSchema.PostgresTable.TableType.TEMPORARY ? "TEMP " : "";
        String strategy = getPartitionStrategy(globalState, table);
        List<PostgresColumn> keys = table.getPartitionKeyColumns().stream()
                .map(name -> table.getColumns().stream().filter(c -> c.getName().equals(name)).findFirst().orElse(null))
                .collect(Collectors.toList());
        if (keys.stream().anyMatch(c -> c == null)) {
            return;
        }
        String keyNames = keys.stream().map(c -> quoteIdentifier(c.getName())).collect(Collectors.joining(", "));
        if ("h".equals(strategy)) {
            String first = table.getName() + "_vp_hash0";
            String second = table.getName() + "_vp_hash1";
            boolean nested = globalState.getDbmsSpecificOptions().verticalPartitioningDeepExploration
                    && Randomly.getBooleanWithRatherLowProbability();
            String firstBound = " FOR VALUES WITH (MODULUS 2, REMAINDER 0)";
            String secondBound = " FOR VALUES WITH (MODULUS 2, REMAINDER 1)";
            String nestedClause = nested ? " PARTITION BY HASH (" + keyNames + ")" : "";
            globalState.executeStatement(new SQLQueryAdapter("CREATE " + temporary + "TABLE " + quoteIdentifier(first)
                    + " PARTITION OF " + quoteIdentifier(table.getName()) + firstBound + nestedClause, errors, true));
            globalState.executeStatement(new SQLQueryAdapter("CREATE " + temporary + "TABLE " + quoteIdentifier(second)
                    + " PARTITION OF " + quoteIdentifier(table.getName()) + secondBound, errors, true));
            if (nested) {
                createHashGrandchildren(globalState, first, temporary, errors);
            }
            return;
        }
        String boundary = partitionBoundaryTuple(keys);
        if (boundary != null && "r".equals(strategy)) {
            globalState.executeStatement(new SQLQueryAdapter(
                    "CREATE " + temporary + "TABLE " + quoteIdentifier(table.getName() + "_vp_range_lo")
                            + " PARTITION OF " + quoteIdentifier(table.getName()) + " FOR VALUES FROM "
                            + minValueTuple(keys.size()) + " TO " + boundary,
                    errors, true));
            globalState.executeStatement(new SQLQueryAdapter(
                    "CREATE " + temporary + "TABLE " + quoteIdentifier(table.getName() + "_vp_range_hi")
                            + " PARTITION OF " + quoteIdentifier(table.getName()) + " FOR VALUES FROM " + boundary
                            + " TO " + maxValueTuple(keys.size()),
                    errors, true));
            return;
        }
        if (boundary != null && "l".equals(strategy)) {
            globalState.executeStatement(
                    new SQLQueryAdapter("CREATE " + temporary + "TABLE " + quoteIdentifier(table.getName() + "_vp_list")
                            + " PARTITION OF " + quoteIdentifier(table.getName()) + " FOR VALUES IN ("
                            + nullValueTuple(keys.size()) + ", " + boundary + ")", errors, true));
            globalState
                    .executeStatement(
                            new SQLQueryAdapter(
                                    "CREATE " + temporary + "TABLE " + quoteIdentifier(table.getName() + "_vp_default")
                                            + " PARTITION OF " + quoteIdentifier(table.getName()) + " DEFAULT",
                                    errors, true));
            return;
        }
        String child = table.getName() + "_vp_default";
        globalState.executeStatement(new SQLQueryAdapter("CREATE " + temporary + "TABLE " + quoteIdentifier(child)
                + " PARTITION OF " + quoteIdentifier(table.getName()) + " DEFAULT", errors, true));
    }

    private static void createHashGrandchildren(PostgresGlobalState globalState, String parent, String temporary,
            ExpectedErrors errors) throws Exception {
        for (int remainder = 0; remainder < 2; remainder++) {
            String child = parent + "_p" + remainder;
            globalState.executeStatement(new SQLQueryAdapter(
                    "CREATE " + temporary + "TABLE " + quoteIdentifier(child) + " PARTITION OF "
                            + quoteIdentifier(parent) + " FOR VALUES WITH (MODULUS 2, REMAINDER " + remainder + ")",
                    errors, true));
        }
    }

    private static String partitionBoundaryLiteral(PostgresDataType type) {
        switch (type) {
        case INT:
        case DECIMAL:
        case MONEY:
            return "0";
        case FLOAT:
        case REAL:
            return "0.0";
        case BOOLEAN:
            return "false";
        case TEXT:
            return "'m'";
        case INET:
            return "'127.0.0.0'";
        case BIT:
            return "B'0'";
        default:
            return null;
        }
    }

    private static String partitionBoundaryTuple(List<PostgresColumn> keys) {
        List<String> values = keys.stream().map(c -> partitionBoundaryLiteral(c.getType()))
                .collect(Collectors.toList());
        return values.stream().anyMatch(v -> v == null) ? null : "(" + String.join(", ", values) + ")";
    }

    private static String minValueTuple(int arity) {
        return "(" + String.join(", ", java.util.Collections.nCopies(arity, "MINVALUE")) + ")";
    }

    private static String maxValueTuple(int arity) {
        return "(" + String.join(", ", java.util.Collections.nCopies(arity, "MAXVALUE")) + ")";
    }

    private static String nullValueTuple(int arity) {
        return "(" + String.join(", ", java.util.Collections.nCopies(arity, "NULL")) + ")";
    }

    private static String getPartitionStrategy(PostgresGlobalState globalState, PostgresSchema.PostgresTable table)
            throws SQLException {
        String sql = "SELECT partstrat FROM pg_partitioned_table WHERE partrelid = " + "to_regclass('"
                + table.getName().replace("'", "''") + "')";
        try (Statement statement = globalState.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "r";
        }
    }

    private static void mutateVerticalPartitioningCandidate(PostgresGlobalState globalState,
            PostgresSchema.PostgresTable table, boolean keepEmpty) throws Exception {
        String tableName = table.getName();
        int actions = Math.max(1, globalState.getOptions().getMaxNumberInserts())
                + globalState.getRandomly().getInteger(1, 11);
        for (int i = 0; i < actions; i++) {
            globalState.updateSchema();
            table = findVerticalPartitioningBaseTable(globalState, tableName);
            if (table == null) {
                return;
            }
            try {
                SQLQueryAdapter query = createVerticalPartitioningCandidateMutation(globalState, table, keepEmpty);
                globalState.executeStatement(query);
            } catch (IgnoreMeException ignored) {
            }
        }
        globalState.updateSchema();
        table = findVerticalPartitioningBaseTable(globalState, tableName);
        if (table == null) {
            return;
        }
        maybeExerciseCandidatePartitionAttachment(globalState, table);
        try {
            globalState.executeStatement(new SQLQueryAdapter("ANALYZE " + quoteIdentifier(table.getName()), true));
        } catch (IgnoreMeException ignored) {
        }
    }

    private static void maybeExerciseCandidatePartitionAttachment(PostgresGlobalState globalState,
            PostgresSchema.PostgresTable table) throws Exception {
        if (!globalState.getDbmsSpecificOptions().verticalPartitioningDeepExploration || !table.isPartitioned()
                || table.getPartitionChildren().isEmpty() || !Randomly.getBoolean()) {
            return;
        }
        String child = Randomly.fromList(table.getPartitionChildren());
        String bound = getPartitionBound(globalState, child);
        if (bound == null || bound.isBlank()) {
            return;
        }
        ExpectedErrors errors = ExpectedErrors.from("is not a partition of relation", "cannot detach partition",
                "cannot attach partition", "would overlap partition", "default partition constraint",
                "cannot attach a partition", "value out of range: overflow", "integer out of range");
        globalState.executeStatement(new SQLQueryAdapter(
                "ALTER TABLE " + quoteIdentifier(table.getName()) + " DETACH PARTITION " + quoteIdentifier(child),
                errors, true));
        globalState.executeStatement(new SQLQueryAdapter("ALTER TABLE " + quoteIdentifier(table.getName())
                + " ATTACH PARTITION " + quoteIdentifier(child) + " " + bound, errors, true));
        PostgresVerticalPartitioningOracle.recordAdvancedFeature(AdvancedFeature.PARTITION_ATTACH_DETACH);
    }

    private static String getPartitionBound(PostgresGlobalState globalState, String child) throws SQLException {
        String sql = "SELECT pg_get_expr(c.relpartbound, c.oid) FROM pg_class c " + "WHERE c.oid = to_regclass('"
                + child.replace("'", "''") + "')";
        try (Statement statement = globalState.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static SQLQueryAdapter createVerticalPartitioningCandidateMutation(PostgresGlobalState globalState,
            PostgresSchema.PostgresTable table, boolean keepEmpty) throws Exception {
        List<Action> actions = new ArrayList<>(Arrays.asList(Action.INSERT, Action.UPDATE, Action.CREATE_INDEX,
                Action.ALTER_TABLE, Action.ANALYZE, Action.CREATE_STATISTICS, Action.ALTER_STATISTICS, Action.REINDEX,
                Action.VACUUM, Action.CLUSTER));
        if (!globalState.supportsPostgresAtLeast(13)) {
            // ALTER STATISTICS ... SET STATISTICS was added in PostgreSQL 13.
            actions.remove(Action.ALTER_STATISTICS);
        }
        if (keepEmpty) {
            actions.remove(Action.INSERT);
        }
        if (globalState.getDbmsSpecificOptions().verticalPartitioningDeepExploration) {
            actions.add(Action.COMMENT_ON);
        }
        Action action = Randomly.fromList(actions);
        switch (action) {
        case INSERT:
            SQLQueryAdapter insert = PostgresInsertGenerator.insertRows(globalState, table);
            addPostgres10VpMutationErrors(globalState, insert);
            return insert;
        case UPDATE:
            return createCandidateUpdate(globalState, table);
        case CREATE_INDEX:
            return createCandidateIndex(globalState, table);
        case ALTER_TABLE:
            SQLQueryAdapter alter = PostgresAlterTableGenerator.create(table, globalState, generateOnlyKnown,
                    !hasExclusionConstraint(globalState, table));
            addVerticalPartitioningCandidateMutationErrors(alter.getExpectedErrors());
            if (!globalState.supportsPostgresAtLeast(13)) {
                // DROP EXPRESSION was added in PostgreSQL 13. The shared ALTER
                // generator still explores it, so treat this version rejection as
                // a recoverable candidate-mutation miss.
                alter.getExpectedErrors().add("syntax error at or near \"EXPRESSION\"");
            }
            return alter;
        case ANALYZE:
            return createCandidateAnalyze(table);
        case CREATE_STATISTICS:
            return createCandidateStatistics(globalState, table);
        case ALTER_STATISTICS:
            return createCandidateAlterStatistics(table);
        case REINDEX:
            return createCandidateReindex(globalState, table);
        case VACUUM:
            return createCandidateVacuum(table);
        case CLUSTER:
            return createCandidateCluster(table);
        case COMMENT_ON:
            return createCandidateComment(globalState, table);
        default:
            throw new AssertionError(action);
        }
    }

    private static boolean hasExclusionConstraint(PostgresGlobalState globalState, PostgresSchema.PostgresTable table)
            throws SQLException {
        String tableName = table.getName().replace("'", "''");
        String sql = "SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = to_regclass('" + tableName
                + "') AND contype = 'x')";
        try (Statement statement = globalState.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            return rs.next() && rs.getBoolean(1);
        }
    }

    private static SQLQueryAdapter createCandidateUpdate(PostgresGlobalState globalState,
            PostgresSchema.PostgresTable table) {
        if (table.getColumns().isEmpty()) {
            throw new IgnoreMeException();
        }
        ExpectedErrors errors = new ExpectedErrors();
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonInsertUpdateErrors(errors);
        addVerticalPartitioningCandidateMutationErrors(errors);
        errors.add("violates check constraint");
        errors.add("violates foreign key constraint");
        errors.add("violates not-null constraint");
        errors.add("violates unique constraint");
        errors.add("conflicting key value violates exclusion constraint");
        // A random update may move a partition key to NULL or to an
        // uncovered boundary. PostgreSQL rejects that row normally.
        errors.add("no partition of relation");
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(quoteIdentifier(table.getName()));
        sb.append(" SET ");
        List<PostgresColumn> columns = table.getRandomNonEmptyColumnSubset();
        if (globalState.getDbmsSpecificOptions().verticalPartitioningDeepExploration && table.isPartitioned()
                && !table.hasExpressionPartitionKey() && !table.getPartitionKeyColumns().isEmpty()
                && Randomly.getBoolean()) {
            List<PostgresColumn> partitionKeyColumns = table.getColumns().stream()
                    .filter(c -> table.getPartitionKeyColumns().contains(c.getName()))
                    // DEFAULT is the only legal assignment for generated and
                    // GENERATED ALWAYS identity columns. It is not a stable
                    // partition-key mutation because it can route the row to
                    // no existing child, so keep those columns out of this
                    // dedicated mutation family.
                    .filter(c -> !c.isGeneratedOrIdentity()).collect(Collectors.toList());
            if (!partitionKeyColumns.isEmpty()) {
                columns = List.of(Randomly.fromList(partitionKeyColumns));
                PostgresVerticalPartitioningOracle.recordAdvancedFeature(AdvancedFeature.PARTITION_KEY_MUTATION);
            }
        }
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState).setColumns(table.getColumns());
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            PostgresColumn column = columns.get(i);
            sb.append(quoteIdentifier(column.getName()));
            sb.append(" = ");
            // This check must precede every type-specific literal branch.
            // Generated columns and GENERATED ALWAYS identity columns reject
            // ordinary UPDATE values, including otherwise safe integer and
            // partition-boundary literals.
            if (column.isGeneratedOrIdentity()) {
                sb.append("DEFAULT");
                continue;
            }
            boolean partitionKey = table.isPartitioned() && !table.hasExpressionPartitionKey()
                    && table.getPartitionKeyColumns().contains(column.getName());
            if (partitionKey) {
                // DEFAULT is NULL for the synthetic RANGE candidate and would
                // route the row outside all children. Keep key mutations in a
                // legal partition domain, including when this is not the
                // dedicated PARTITION_KEY_MUTATION branch.
                sb.append(generateCandidateBoundaryLiteral(column));
            } else if (PostgresSchema.isNarrowIntegerType(column) && column.getType() == PostgresDataType.INT) {
                sb.append(PostgresSchema.safeIntegerLiteral(column));
            } else if (shouldUseVpBoundaryLiteral(globalState)) {
                PostgresVerticalPartitioningOracle.recordAdvancedFeature(AdvancedFeature.BOUNDARY_MUTATION);
                sb.append(generateCandidateBoundaryLiteral(column));
            } else if (column.getType() == PostgresDataType.BIT) {
                sb.append(generateBitLiteral(column));
            } else if (Randomly.getBoolean()) {
                sb.append(generateCandidateConstant(globalState, column));
            } else if (Randomly.getBoolean()) {
                sb.append("DEFAULT");
            } else {
                sb.append("(");
                sb.append(PostgresVisitor.asString(gen.generateExpression(0, column.getType())));
                sb.append(")");
            }
        }
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(PostgresVisitor.asString(gen.generateExpression(0, PostgresDataType.BOOLEAN)));
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private static void addVerticalPartitioningCandidateMutationErrors(ExpectedErrors errors) {
        errors.add("cannot convert NaN to smallint");
        errors.add("cannot convert NaN to integer");
        errors.add("cannot convert NaN to bigint");
        errors.add("is violated by some row");
        errors.add("but expression is of type");
    }

    private static void addPostgres10VpMutationErrors(PostgresGlobalState globalState, SQLQueryAdapter query) {
        query.getExpectedErrors().add("smallint out of range");
        if (!globalState.supportsPostgresAtLeast(11)) {
            query.getExpectedErrors().add("ON CONFLICT clause is not supported with partitioned tables");
        }
    }

    private static void addPostgres10VpCreateErrors(PostgresGlobalState globalState, SQLQueryAdapter query) {
        if (!globalState.supportsPostgresAtLeast(12)) {
            query.getExpectedErrors().add("syntax error at or near \"GENERATED\"");
        }
    }

    private static String generateCandidateConstant(PostgresGlobalState globalState, PostgresColumn column) {
        switch (column.getType()) {
        case OTHER:
            // The target column supplies the concrete domain/enum/JSON/native type.
            return "DEFAULT";
        case BIT:
            return generateBitLiteral(column);
        case TEXT:
            if (column.getMaximumLength() != null) {
                String value = globalState.getRandomly().getString();
                int maxLength = Math.max(0, column.getMaximumLength());
                if (value.length() > maxLength) {
                    value = value.substring(0, maxLength);
                }
                return "'" + value.replace("'", "''") + "'";
            }
            break;
        default:
            break;
        }
        PostgresExpression constant = PostgresExpressionGenerator.generateConstant(globalState.getRandomly(),
                column.getType());
        return PostgresVisitor.asString(constant);
    }

    private static SQLQueryAdapter createCandidateIndex(PostgresGlobalState globalState,
            PostgresSchema.PostgresTable table) {
        ExpectedErrors errors = new ExpectedErrors();
        PostgresCommon.addCommonExpressionErrors(errors);
        addVerticalPartitioningCandidateMutationErrors(errors);
        errors.add("already exists");
        errors.add("could not create unique index");
        errors.add("access method \"gin\" does not support unique indexes");
        errors.add("access method \"hash\" does not support unique indexes");
        errors.add("data type");
        errors.add("has no default operator class");
        errors.add("functions in index predicate must be marked IMMUTABLE");
        StringBuilder sb = new StringBuilder("CREATE");
        boolean createUnique = canCreateUniqueCandidateIndex(table) && Randomly.getBoolean();
        if (createUnique) {
            sb.append(" UNIQUE");
        }
        sb.append(" INDEX ");
        sb.append(quoteIdentifier("vp_idx_" + Math.abs(globalState.getRandomly().getInteger())));
        sb.append(" ON ");
        sb.append(quoteIdentifier(table.getName()));
        String indexMethod = null;
        if (!createUnique && Randomly.getBooleanWithSmallProbability()) {
            indexMethod = Randomly.fromOptions("BTREE", "HASH");
            sb.append(" USING ");
            sb.append(indexMethod);
        }
        sb.append("(");
        List<PostgresColumn> columns = table.getRandomNonEmptyColumnSubset();
        if ("HASH".equals(indexMethod) && columns.size() > 1) {
            columns = new ArrayList<>(List.of(Randomly.fromList(columns)));
        }
        sb.append(columns.stream().map(c -> quoteIdentifier(c.getName())).collect(Collectors.joining(", ")));
        sb.append(")");
        if (Randomly.getBooleanWithRatherLowProbability()) {
            sb.append(" WHERE ");
            PostgresExpression expr = new PostgresExpressionGenerator(globalState).setColumns(table.getColumns())
                    .generateExpression(0, PostgresDataType.BOOLEAN);
            sb.append(PostgresVisitor.asString(expr));
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private static boolean canCreateUniqueCandidateIndex(PostgresSchema.PostgresTable table) {
        return !table.isPartitioned();
    }

    private static SQLQueryAdapter createCandidateAnalyze(PostgresSchema.PostgresTable table) {
        StringBuilder sb = new StringBuilder("ANALYZE ");
        sb.append(quoteIdentifier(table.getName()));
        if (Randomly.getBoolean() && !table.getColumns().isEmpty()) {
            sb.append("(");
            sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> quoteIdentifier(c.getName()))
                    .collect(Collectors.joining(", ")));
            sb.append(")");
        }
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("deadlock"));
    }

    private static SQLQueryAdapter createCandidateStatistics(PostgresGlobalState globalState,
            PostgresSchema.PostgresTable table) {
        if (table.getColumns().size() < 2) {
            throw new IgnoreMeException();
        }
        List<PostgresColumn> columns = table
                .getRandomNonEmptyColumnSubset(globalState.getRandomly().getInteger(2, table.getColumns().size()));
        StringBuilder sb = new StringBuilder("CREATE STATISTICS ");
        sb.append(quoteIdentifier("vp_stat_" + Math.abs(globalState.getRandomly().getInteger())));
        sb.append(" ON ");
        sb.append(columns.stream().map(c -> quoteIdentifier(c.getName())).collect(Collectors.joining(", ")));
        sb.append(" FROM ");
        sb.append(quoteIdentifier(table.getName()));
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors.from("cannot have more than 8 columns in statistics",
                "statistics object", "already exists"), true);
    }

    private static SQLQueryAdapter createCandidateAlterStatistics(PostgresSchema.PostgresTable table) {
        if (table.getStatistics().isEmpty()) {
            throw new IgnoreMeException();
        }
        String stat = Randomly.fromList(table.getStatistics()).getQualifiedName();
        return new SQLQueryAdapter(
                "ALTER STATISTICS " + stat + " SET STATISTICS " + Randomly.getNotCachedInteger(-1, 10000), true);
    }

    private static SQLQueryAdapter createCandidateReindex(PostgresGlobalState globalState,
            PostgresSchema.PostgresTable table) {
        ExpectedErrors errors = ExpectedErrors.from("could not create unique index",
                "REINDEX is not yet implemented for partitioned indexes",
                "canceling statement due to statement timeout");
        if (Randomly.getBoolean() && !table.getIndexes().isEmpty()) {
            PostgresIndex index = Randomly.fromList(table.getIndexes());
            return new SQLQueryAdapter("REINDEX INDEX " + quoteIdentifier(index.getIndexName()), errors);
        }
        return new SQLQueryAdapter("REINDEX TABLE " + quoteIdentifier(table.getName()), errors);
    }

    private static SQLQueryAdapter createCandidateVacuum(PostgresSchema.PostgresTable table) {
        ExpectedErrors errors = ExpectedErrors.from("VACUUM cannot run inside a transaction block", "deadlock",
                "ANALYZE option must be specified when a column list is provided", "statement timeout");
        StringBuilder sb = new StringBuilder("VACUUM ");
        if (Randomly.getBoolean()) {
            sb.append("(ANALYZE) ");
        }
        sb.append(quoteIdentifier(table.getName()));
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static SQLQueryAdapter createCandidateCluster(PostgresSchema.PostgresTable table) {
        ExpectedErrors errors = ExpectedErrors.from("there is no previously clustered index for table",
                "cannot cluster a partitioned table", "access method does not support clustering",
                "cannot cluster on partial index");
        StringBuilder sb = new StringBuilder("CLUSTER ");
        sb.append(quoteIdentifier(table.getName()));
        if (Randomly.getBoolean() && !table.getIndexes().isEmpty()) {
            sb.append(" USING ");
            sb.append(quoteIdentifier(table.getRandomIndex().getIndexName()));
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static SQLQueryAdapter createCandidateComment(PostgresGlobalState globalState,
            PostgresSchema.PostgresTable table) {
        StringBuilder sb = new StringBuilder("COMMENT ON ");
        String target = Randomly.fromOptions("TABLE", "COLUMN", "INDEX", "STATISTICS");
        switch (target) {
        case "TABLE":
            sb.append("TABLE ");
            sb.append(quoteIdentifier(table.getName()));
            break;
        case "COLUMN":
            sb.append("COLUMN ");
            PostgresColumn column = table.getRandomColumn();
            sb.append(quoteIdentifier(table.getName()));
            sb.append(".");
            sb.append(quoteIdentifier(column.getName()));
            break;
        case "INDEX":
            if (table.getIndexes().isEmpty()) {
                throw new IgnoreMeException();
            }
            sb.append("INDEX ");
            sb.append(quoteIdentifier(table.getRandomIndex().getIndexName()));
            break;
        case "STATISTICS":
            if (table.getStatistics().isEmpty()) {
                throw new IgnoreMeException();
            }
            sb.append("STATISTICS ");
            sb.append(Randomly.fromList(table.getStatistics()).getQualifiedName());
            break;
        default:
            throw new AssertionError(target);
        }
        sb.append(" IS ");
        if (Randomly.getBoolean()) {
            sb.append("NULL");
        } else {
            sb.append("'");
            sb.append(globalState.getRandomly().getString().replace("'", "''"));
            sb.append("'");
        }
        return new SQLQueryAdapter(sb.toString(), true);
    }

    private static boolean shouldUseVpBoundaryLiteral(PostgresGlobalState globalState) {
        return globalState.getDbmsSpecificOptions().verticalPartitioningDeepExploration
                && Randomly.getBooleanWithRatherLowProbability();
    }

    private static String generateCandidateBoundaryLiteral(PostgresColumn column) {
        switch (column.getType()) {
        case INT:
            if (PostgresSchema.isNarrowIntegerType(column)) {
                return PostgresSchema.safeIntegerLiteral(column);
            }
            return Randomly.fromOptions("0", "1", "-1", "9223372036854775807", "-9223372036854775808");
        case BOOLEAN:
            return Randomly.fromOptions("TRUE", "FALSE", "NULL");
        case TEXT:
            return Randomly.fromOptions("''", "' '", "'0'", "'-1'", "'NaN'");
        case DECIMAL:
            return Randomly.fromOptions("0", "-0", "1", "-1", "1000000", "-1000000", "0.000001", "-0.000001");
        case FLOAT:
            return Randomly.fromOptions("0", "-0", "1", "-1", "1000000", "-1000000", "1e-20", "-1e-20");
        case REAL:
            return Randomly.fromOptions("0", "-0", "1", "-1", "1000000", "-1000000", "1e-20", "-1e-20");
        case MONEY:
            return Randomly.fromOptions("'0'::money", "'-1'::money", "'92233720368547758.07'::money");
        case BIT:
            return generateBitLiteral(column);
        case INET:
            return Randomly.fromOptions("'0.0.0.0'::inet", "'255.255.255.255'::inet", "'::1'::inet");
        case UUID:
            return Randomly.fromOptions("'00000000-0000-0000-0000-000000000000'::uuid",
                    "'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid", "'123e4567-e89b-12d3-a456-426614174000'::uuid");
        case BYTEA:
            return Randomly.fromOptions("'\\x'::bytea", "'\\x00'::bytea", "'\\xff'::bytea", "'\\x000102ff'::bytea");
        case MACADDR:
            return Randomly.fromOptions("'00:00:00:00:00:00'::macaddr", "'ff:ff:ff:ff:ff:ff'::macaddr",
                    "'08:00:2b:01:02:03'::macaddr");
        case TSVECTOR:
            return Randomly.fromOptions("to_tsvector('english'::regconfig, '')",
                    "to_tsvector('english'::regconfig, 'alpha beta')",
                    "to_tsvector('english'::regconfig, 'sqlancer postgres')");
        case TSQUERY:
            return Randomly.fromOptions("to_tsquery('english'::regconfig, 'alpha')",
                    "to_tsquery('english'::regconfig, 'alpha & beta')", "to_tsquery('english'::regconfig, '!foo')");
        case RANGE:
            return Randomly.fromOptions("'empty'::int4range", "'[0,1)'::int4range", "'(,)'::int4range");
        default:
            return "NULL";
        }
    }

    private static String generateBitLiteral(PostgresColumn column) {
        Integer maxLength = column.getMaximumLength();
        if (maxLength == null) {
            return Randomly.fromOptions("B'0'", "B'1'", "B''");
        }
        int length = (int) Randomly.getNotCachedInteger(0, Math.max(1, maxLength) + 1);
        StringBuilder bits = new StringBuilder("B'");
        for (int i = 0; i < length; i++) {
            bits.append(Randomly.getBoolean() ? "1" : "0");
        }
        bits.append("'");
        return bits.toString();
    }

    private static PostgresSchema.PostgresTable findVerticalPartitioningBaseTable(PostgresGlobalState globalState,
            String tableName) {
        return globalState.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> !t.isPartitionChild()).filter(t -> !t.getName().startsWith("vp_"))
                .filter(t -> t.getName().equals(tableName)).findFirst().orElse(null);
    }

    private static boolean isValidVerticalPartitioningBaseTable(PostgresGlobalState globalState,
            PostgresSchema.PostgresTable table) {
        if (table.isView() || table.getName().startsWith("vp_") || table.getColumns().isEmpty()) {
            return false;
        }
        try {
            globalState.executeStatement(
                    new SQLQueryAdapter("SELECT COUNT(*) FROM " + quoteIdentifier(table.getName()), true));
            String columns = table.getColumns().stream().map(c -> quoteIdentifier(c.getName()))
                    .collect(Collectors.joining(", "));
            globalState.executeStatement(new SQLQueryAdapter(
                    "SELECT " + columns + " FROM " + quoteIdentifier(table.getName()) + " LIMIT 1", true));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static long countVerticalPartitioningBaseTables(PostgresGlobalState globalState) {
        return globalState.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> !t.isPartitionChild()).filter(t -> !t.getName().startsWith("vp_"))
                .filter(t -> !t.getColumns().isEmpty()).count();
    }

    private static void logVerticalPartitioningReplacementFailure(PostgresGlobalState globalState,
            String oldTableName) {
        globalState.getState().logStatement("-- [vp-warning] could not create replacement for " + oldTableName);
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public SQLConnection createDatabase(PostgresGlobalState globalState) throws SQLException {
        // StateLogger has already initialized and cleaned the provider log
        // directory when this method is entered. Publish the first VP
        // checkpoint before any connection/database setup can be interrupted.
        if (usesVerticalPartitioningOracle(globalState.getDbmsSpecificOptions())) {
            PostgresVerticalPartitioningOracle.initializeStatsLogging();
        }
        if (globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch((o) -> o == PostgresOracleFactory.PQS)) {
            generateOnlyKnown = true;
        }

        username = globalState.getOptions().getUserName();
        password = globalState.getOptions().getPassword();
        host = globalState.getOptions().getHost();
        port = globalState.getOptions().getPort();
        entryPath = "/test";
        entryURL = globalState.getDbmsSpecificOptions().connectionURL;
        // trim URL to exclude "jdbc:"
        if (entryURL.startsWith("jdbc:")) {
            entryURL = entryURL.substring(5);
        }
        String entryDatabaseName = entryPath.substring(1);
        databaseName = globalState.getDatabaseName();

        try {
            URI uri = new URI(entryURL);
            String userInfoURI = uri.getUserInfo();
            String pathURI = uri.getPath();
            if (userInfoURI != null) {
                // username and password specified in URL take precedence
                if (userInfoURI.contains(":")) {
                    String[] userInfo = userInfoURI.split(":", 2);
                    username = userInfo[0];
                    password = userInfo[1];
                } else {
                    username = userInfoURI;
                    password = null;
                }
                int userInfoIndex = entryURL.indexOf(userInfoURI);
                String preUserInfo = entryURL.substring(0, userInfoIndex);
                String postUserInfo = entryURL.substring(userInfoIndex + userInfoURI.length() + 1);
                entryURL = preUserInfo + postUserInfo;
            }
            if (pathURI != null) {
                entryPath = pathURI;
            }
            if (globalState.getDbmsSpecificOptions().getTestOracleFactory().contains(PostgresOracleFactory.ECST)) {
                // The ECST entry database is the URL path, not the legacy hard-coded test database.
                entryDatabaseName = entryPath.length() > 1 ? entryPath.substring(1) : "postgres";
            }
            if (host == null) {
                host = uri.getHost();
            }
            if (port == MainOptions.NO_SET_PORT) {
                port = uri.getPort();
            }
            entryURL = String.format("%s://%s:%d/%s", uri.getScheme(), host, port, entryDatabaseName);
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }
        Connection con = DriverManager.getConnection("jdbc:" + entryURL, username, password);
        globalState.getState().logStatement(String.format("\\c %s;", entryDatabaseName));

        String dropCommand = "DROP DATABASE";
        boolean forceDrop = Randomly.getBoolean();
        if (forceDrop) {
            dropCommand += " FORCE";
        }
        dropCommand += " IF EXISTS " + databaseName;

        globalState.getState().logStatement(dropCommand + ";");
        try (Statement s = con.createStatement()) {
            s.execute(dropCommand);
        } catch (SQLException e) {
            // If force fails, fall back to regular drop
            if (forceDrop) {
                String fallbackDrop = "DROP DATABASE IF EXISTS " + databaseName;
                globalState.getState().logStatement(fallbackDrop + ";");
                try (Statement s = con.createStatement()) {
                    s.execute(fallbackDrop);
                }
            } else {
                throw e;
            }
        }

        // Create database section
        createDatabaseCommand = getCreateDatabaseCommand(globalState);
        globalState.getState().logStatement(createDatabaseCommand + ";");
        try (Statement s = con.createStatement()) {
            s.execute(createDatabaseCommand);
        }
        con.close();
        if (globalState.getDbmsSpecificOptions().getTestOracleFactory().contains(PostgresOracleFactory.ECST)) {
            testURL = entryURL.substring(0, entryURL.lastIndexOf('/') + 1) + databaseName;
        } else {
            int databaseIndex = entryURL.indexOf(entryDatabaseName);
            String preDatabaseName = entryURL.substring(0, databaseIndex);
            String postDatabaseName = entryURL.substring(databaseIndex + entryDatabaseName.length());
            testURL = preDatabaseName + databaseName + postDatabaseName;
        }
        globalState.getState().logStatement(String.format("\\c %s;", databaseName));

        con = DriverManager.getConnection("jdbc:" + testURL, username, password);
        return new SQLConnection(con);
    }

    protected void readFunctions(PostgresGlobalState globalState) throws SQLException {
        SQLQueryAdapter query = new SQLQueryAdapter("SELECT proname, provolatile FROM pg_proc;");
        SQLancerResultSet rs = query.executeAndGet(globalState);
        while (rs.next()) {
            String functionName = rs.getString(1);
            Character functionType = rs.getString(2).charAt(0);
            globalState.addFunctionAndType(functionName, functionType);
        }
    }

    protected void createTables(PostgresGlobalState globalState, int numTables) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < numTables) {
            try {
                String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                SQLQueryAdapter createTable = PostgresTableGenerator.generate(tableName, globalState.getSchema(),
                        generateOnlyKnown, globalState);
                globalState.executeStatement(createTable);
            } catch (IgnoreMeException e) {

            }
        }
    }

    protected void prepareTables(PostgresGlobalState globalState) throws Exception {
        StatementExecutor<PostgresGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                PostgresProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
        globalState.executeStatement(new SQLQueryAdapter("COMMIT", true));
        globalState.executeStatement(new SQLQueryAdapter("SET SESSION statement_timeout = 15000;\n"));
    }

    private String getCreateDatabaseCommand(PostgresGlobalState state) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE DATABASE " + databaseName + " ");
        if (((PostgresOptions) state.getDbmsSpecificOptions()).testCollations) {
            if (Randomly.getBoolean()) {
                if (Randomly.getBoolean()) {
                    sb.append("WITH ENCODING '");
                    sb.append(Randomly.fromOptions("utf8"));
                    sb.append("' ");
                }
                if (Randomly.getBoolean() && !state.getCollates().isEmpty()) {
                    sb.append(String.format(" LOCALE = '%s' ", Randomly.fromList(state.getCollates())));
                } else {
                    for (String lc : Arrays.asList("LC_COLLATE", "LC_CTYPE")) {
                        if (!state.getCollates().isEmpty() && Randomly.getBoolean()) {
                            sb.append(String.format(" %s = '%s'", lc, Randomly.fromList(state.getCollates())));
                        }
                    }
                }
                sb.append(" TEMPLATE template0");
            }
        } else {
            sb.append("WITH ENCODING 'UTF8' TEMPLATE template0");
        }
        return sb.toString();
    }

    @Override
    public String getDBMSName() {
        return "postgres";
    }

    @Override
    public String getQueryPlan(String selectStr, PostgresGlobalState globalState) throws Exception {
        String queryPlan = "";
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(selectStr);
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        SQLQueryAdapter q = new SQLQueryAdapter(PostgresExplainGenerator.explain(selectStr), null);
        try (SQLancerResultSet rs = q.executeAndGet(globalState)) {
            while (rs.next()) {
                queryPlan += rs.getString(1);
            }
        } catch (SQLException | AssertionError e) {
            queryPlan = "";
        }
        return formatQueryPlan(queryPlan);
    }

    @Override
    protected double[] initializeWeightedAverageReward() {
        return new double[PostgresProvider.Action.values().length];
    }

    @Override
    protected void executeMutator(int index, PostgresGlobalState globalState) throws Exception {
        SQLQueryAdapter queryMutateTable = PostgresProvider.Action.values()[index].getQuery(globalState);
        globalState.executeStatement(queryMutateTable);
    }

    @Override
    protected boolean addRowsToAllTables(PostgresGlobalState globalState) throws Exception {
        List<PostgresSchema.PostgresTable> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (PostgresSchema.PostgresTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = PostgresInsertGenerator.insertRows(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }

    public String formatQueryPlan(String queryPlan) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(queryPlan).get(0).get("Plan");
        // Extract nodes using BFS algorithm
        List<String> nodeTypes = extractNodeTypesIterative(root);
        return String.join(" ", nodeTypes);
    }

    // BFS algorithm for traversing the Json Query Plan
    private static List<String> extractNodeTypesIterative(JsonNode root) {
        List<String> result = new ArrayList<>();
        Queue<JsonNode> queue = new LinkedList<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            JsonNode node = queue.poll();
            if (node.has("Node Type")) {
                result.add(node.get("Node Type").asText());
            }
            if (node.has("Plans") && node.get("Plans").isArray()) {
                for (JsonNode plan : node.get("Plans")) {
                    queue.add(plan);
                }
            }
        }
        return result;
    }

}
