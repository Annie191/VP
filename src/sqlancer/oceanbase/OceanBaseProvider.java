package sqlancer.oceanbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

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
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.oceanbase.gen.OceanBaseAlterTable;
import sqlancer.oceanbase.gen.OceanBaseDeleteGenerator;
import sqlancer.oceanbase.gen.OceanBaseExpressionGenerator;
import sqlancer.oceanbase.gen.OceanBaseDropIndex;
import sqlancer.oceanbase.gen.OceanBaseInsertGenerator;
import sqlancer.oceanbase.gen.OceanBaseTableGenerator;
import sqlancer.oceanbase.gen.OceanBaseTruncateTableGenerator;
import sqlancer.oceanbase.gen.OceanBaseUpdateGenerator;
import sqlancer.oceanbase.gen.datadef.OceanBaseIndexGenerator;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseColumn;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseDataType;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;
import sqlancer.oceanbase.oracle.OceanBaseVerticalPartitioningOracle;
import sqlancer.oceanbase.oracle.OceanBaseVerticalPartitioningOracle.AdvancedFeature;

@AutoService(DatabaseProvider.class)
public class OceanBaseProvider extends SQLProviderAdapter<OceanBaseGlobalState, OceanBaseOptions> {

    private static final int VP_BASE_TABLES_PER_BATCH = 2;
    private static final int VP_REPLACEMENT_RETRY_COUNT = 10;
    private static final int VP_SINGLE_COLUMN_PERCENT = 40;
    private static final int VP_INITIAL_INSERTS = 16;

    public OceanBaseProvider() {
        super(OceanBaseGlobalState.class, OceanBaseOptions.class);
    }

    enum Action implements AbstractAction<OceanBaseGlobalState> {
        SHOW_TABLES((g) -> new SQLQueryAdapter("SHOW TABLES")), INSERT(OceanBaseInsertGenerator::insertRow),
        CREATE_INDEX(OceanBaseIndexGenerator::create), ALTER_TABLE(OceanBaseAlterTable::create),
        TRUNCATE_TABLE(OceanBaseTruncateTableGenerator::generate),
        SELECT_INFO((g) -> new SQLQueryAdapter(
                "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '" + g.getDatabaseName()
                        + "'")),
        CREATE_TABLE((g) -> {
            String tableName = DBMSCommon.createTableName(g.getSchema().getDatabaseTables().size());

            return OceanBaseTableGenerator.generate(g, tableName);
        }), DELETE(OceanBaseDeleteGenerator::delete), UPDATE(OceanBaseUpdateGenerator::update),
        DROP_INDEX(OceanBaseDropIndex::generate);

        private final SQLQueryProvider<OceanBaseGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<OceanBaseGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(OceanBaseGlobalState globalState) throws Exception {
            return sqlQueryProvider.getQuery(globalState);
        }
    }

    private static int mapActions(OceanBaseGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        boolean preserveVerticalPartitioningRows = usesVerticalPartitioningOracle(globalState.getDbmsSpecificOptions())
                && !globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes;
        int nrPerformed = 0;
        switch (a) {
        case DROP_INDEX:
            nrPerformed = r.getInteger(0, 2);
            break;
        case SHOW_TABLES:
            nrPerformed = r.getInteger(0, 1);
            break;
        case CREATE_TABLE:
            nrPerformed = r.getInteger(0, 1);
            break;
        case INSERT:
            nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            break;
        case CREATE_INDEX:
            nrPerformed = r.getInteger(0, 5);
            break;
        case UPDATE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case ALTER_TABLE:
            nrPerformed = 0;
            break;
        case TRUNCATE_TABLE:
            nrPerformed = preserveVerticalPartitioningRows ? 0 : r.getInteger(0, 2);
            break;
        case SELECT_INFO:
            nrPerformed = r.getInteger(0, 10);
            break;
        case DELETE:
            nrPerformed = preserveVerticalPartitioningRows ? 0 : r.getInteger(0, 10);
            break;
        default:
            throw new AssertionError(a);
        }
        return nrPerformed;
    }

    @Override
    public void generateDatabase(OceanBaseGlobalState globalState) throws Exception {
        boolean usesVerticalPartitioningOracle = usesVerticalPartitioningOracle(globalState);
        if (usesVerticalPartitioningOracle) {
            createVerticalPartitioningTableBatch(globalState);
        } else {
            while (globalState.getSchema().getDatabaseTables().size() < Randomly.smallNumber() + 1) {
                String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                SQLQueryAdapter createTable = OceanBaseTableGenerator.generate(globalState, tableName);
                globalState.executeStatement(createTable);
            }
        }

        if (!usesVerticalPartitioningOracle) {
            StatementExecutor<OceanBaseGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                    OceanBaseProvider::mapActions, (q) -> {
                        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                            throw new IgnoreMeException();
                        }
                    });
            se.executeStatements();
        }

        if (usesVerticalPartitioningOracle) {
            OceanBaseVerticalPartitioningOracle.logSchemaShapeStats(globalState, "post-generate");
        }
    }

    private boolean usesVerticalPartitioningOracle(OceanBaseGlobalState globalState) {
        return usesVerticalPartitioningOracle(globalState.getDbmsSpecificOptions());
    }

    private static boolean usesVerticalPartitioningOracle(OceanBaseOptions options) {
        return options.getTestOracleFactory().stream()
                .anyMatch(o -> o == OceanBaseOracleFactory.VERTICAL_PARTITIONING || o == OceanBaseOracleFactory.ECST);
    }

    public static void replaceVerticalPartitioningTableAfterSuccessfulCheck(OceanBaseGlobalState globalState,
            String oldTableName) throws Exception {
        OceanBaseTable replacement = createValidVerticalPartitioningBaseTable(globalState);
        if (replacement == null) {
            logVerticalPartitioningReplacementFailure(globalState, oldTableName);
            throw new IgnoreMeException();
        }
        globalState
                .executeStatement(new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(oldTableName), true));
        globalState.updateSchema();
    }

    public static void ensureVerticalPartitioningBaseTables(OceanBaseGlobalState globalState, int minBaseTables)
            throws Exception {
        globalState.updateSchema();
        while (countVerticalPartitioningBaseTables(globalState) < minBaseTables) {
            OceanBaseTable table = createValidVerticalPartitioningBaseTable(globalState);
            if (table == null) {
                throw new IgnoreMeException();
            }
            globalState.updateSchema();
        }
    }

    private static void createVerticalPartitioningTableBatch(OceanBaseGlobalState globalState) throws Exception {
        for (int i = 0; i < VP_BASE_TABLES_PER_BATCH; i++) {
            OceanBaseTable table = createValidVerticalPartitioningBaseTable(globalState);
            if (table == null) {
                throw new IgnoreMeException();
            }
            globalState.updateSchema();
        }
    }

    private static OceanBaseTable createValidVerticalPartitioningBaseTable(OceanBaseGlobalState globalState)
            throws Exception {
        for (int attempt = 0; attempt < VP_REPLACEMENT_RETRY_COUNT; attempt++) {
            String tableName = getFreeTableName(globalState);
            boolean success = globalState.executeStatement(generateVerticalPartitioningTable(globalState, tableName));
            globalState.updateSchema();
            if (!success) {
                continue;
            }
            OceanBaseTable table = findVerticalPartitioningBaseTable(globalState, tableName);
            if (table != null) {
                populateVerticalPartitioningCandidate(globalState, table);
                mutateVerticalPartitioningCandidate(globalState, table);
                globalState.updateSchema();
                table = findVerticalPartitioningBaseTable(globalState, tableName);
                if (table != null && isValidVerticalPartitioningBaseTable(globalState, table)) {
                    return table;
                }
            }
            globalState
                    .executeStatement(new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(tableName), true));
            globalState.updateSchema();
        }
        return null;
    }

    private static void populateVerticalPartitioningCandidate(OceanBaseGlobalState globalState, OceanBaseTable table)
            throws Exception {
        for (int i = 0; i < VP_INITIAL_INSERTS; i++) {
            try {
                globalState.executeStatement(createCandidateInsert(globalState, table));
            } catch (IgnoreMeException ignored) {
            }
        }
    }

    private static void mutateVerticalPartitioningCandidate(OceanBaseGlobalState globalState, OceanBaseTable table)
            throws Exception {
        int actions = Math.max(1, globalState.getOptions().getMaxNumberInserts())
                + globalState.getRandomly().getInteger(1, 11);
        for (int i = 0; i < actions; i++) {
            globalState.updateSchema();
            table = findVerticalPartitioningBaseTable(globalState, table.getName());
            if (table == null) {
                return;
            }
            try {
                globalState.executeStatement(createVerticalPartitioningCandidateMutation(globalState, table));
            } catch (IgnoreMeException ignored) {
            }
        }
    }

    private static SQLQueryAdapter createVerticalPartitioningCandidateMutation(OceanBaseGlobalState globalState,
            OceanBaseTable table) throws Exception {
        List<Function<OceanBaseTable, SQLQueryAdapter>> actions = new ArrayList<>();
        actions.add(t -> createCandidateInsert(globalState, t));
        actions.add(t -> createCandidateUpdate(globalState, t));
        actions.add(t -> createCandidateIndex(globalState, t));
        actions.add(t -> createCandidateAnalyze(t));
        actions.add(t -> createCandidateDropIndex(t));
        actions.add(t -> createCandidateChecksum(t));
        actions.add(t -> createCandidateRepair(t));
        actions.add(t -> createCandidateOptimize(t));
        if (globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes) {
            actions.add(t -> createCandidateDelete(globalState, t));
            actions.add(
                    t -> new SQLQueryAdapter("TRUNCATE TABLE " + quoteIdentifier(t.getName()), commonMutationErrors()));
        }
        return Randomly.fromList(actions).apply(table);
    }

    private static SQLQueryAdapter createCandidateInsert(OceanBaseGlobalState globalState, OceanBaseTable table) {
        List<OceanBaseColumn> columns = table.getRandomNonEmptyColumnSubset();
        StringBuilder sb = new StringBuilder(Randomly.getBoolean() ? "INSERT INTO " : "REPLACE INTO ");
        sb.append(quoteIdentifier(table.getName())).append("(");
        sb.append(columns.stream().map(c -> quoteIdentifier(c.getName())).collect(Collectors.joining(", ")));
        sb.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(generateCandidateValue(globalState, columns.get(i)));
        }
        sb.append(")");
        return new SQLQueryAdapter(sb.toString(), commonMutationErrors());
    }

    private static SQLQueryAdapter createCandidateUpdate(OceanBaseGlobalState globalState, OceanBaseTable table) {
        List<OceanBaseColumn> columns = table.getRandomNonEmptyColumnSubset();
        OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(globalState).setColumns(table.getColumns());
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(quoteIdentifier(table.getName())).append(" SET ");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(quoteIdentifier(columns.get(i).getName())).append("=");
            if (shouldUseVpBoundaryLiteral(globalState)) {
                OceanBaseVerticalPartitioningOracle.recordAdvancedFeature(AdvancedFeature.BOUNDARY_MUTATION);
                sb.append(generateCandidateBoundaryLiteral(columns.get(i)));
            } else if (Randomly.getBoolean()) {
                sb.append(generateCandidateValue(globalState, columns.get(i)));
            } else {
                sb.append(OceanBaseVisitor.asString(gen.generateExpression()));
            }
        }
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ").append(OceanBaseVisitor.asString(gen.generateExpression()));
        }
        return new SQLQueryAdapter(sb.toString(), commonMutationErrors());
    }

    private static SQLQueryAdapter createCandidateDelete(OceanBaseGlobalState globalState, OceanBaseTable table) {
        OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(globalState).setColumns(table.getColumns());
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        sb.append(quoteIdentifier(table.getName()));
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ").append(OceanBaseVisitor.asString(gen.generateExpression()));
        }
        return new SQLQueryAdapter(sb.toString(), commonMutationErrors());
    }

    private static SQLQueryAdapter createCandidateIndex(OceanBaseGlobalState globalState, OceanBaseTable table) {
        List<OceanBaseColumn> indexable = table.getColumns().stream()
                .filter(c -> c.getType() != OceanBaseDataType.VARCHAR).collect(Collectors.toList());
        if (indexable.isEmpty()) {
            throw new IgnoreMeException();
        }
        List<OceanBaseColumn> columns = Randomly.nonEmptySubset(indexable);
        StringBuilder sb = new StringBuilder("CREATE INDEX ");
        sb.append(globalState.getSchema().getFreeIndexName()).append(" ON ").append(quoteIdentifier(table.getName()))
                .append("(");
        for (int i = 0; i < Math.min(columns.size(), 2); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(quoteIdentifier(columns.get(i).getName()));
            if (Randomly.getBoolean()) {
                sb.append(" ").append(Randomly.fromOptions("ASC", "DESC"));
            }
        }
        sb.append(")");
        return new SQLQueryAdapter(sb.toString(), commonMutationErrors(), true);
    }

    private static SQLQueryAdapter createCandidateDropIndex(OceanBaseTable table) {
        if (!table.hasIndexes()) {
            throw new IgnoreMeException();
        }
        StringBuilder sb = new StringBuilder("DROP INDEX ");
        sb.append(table.getRandomIndex().getIndexName()).append(" ON ").append(quoteIdentifier(table.getName()));
        return new SQLQueryAdapter(sb.toString(), commonMutationErrors(), true);
    }

    private static SQLQueryAdapter createCandidateAnalyze(OceanBaseTable table) {
        return new SQLQueryAdapter("ANALYZE TABLE " + quoteIdentifier(table.getName()), commonMutationErrors());
    }

    private static SQLQueryAdapter createCandidateChecksum(OceanBaseTable table) {
        StringBuilder sb = new StringBuilder("CHECKSUM TABLE ");
        sb.append(quoteIdentifier(table.getName()));
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("QUICK", "EXTENDED"));
        }
        return new SQLQueryAdapter(sb.toString(), commonMutationErrors());
    }

    private static SQLQueryAdapter createCandidateRepair(OceanBaseTable table) {
        StringBuilder sb = new StringBuilder("REPAIR TABLE ");
        sb.append(quoteIdentifier(table.getName()));
        if (Randomly.getBoolean()) {
            sb.append(" QUICK");
        }
        if (Randomly.getBoolean()) {
            sb.append(" EXTENDED");
        }
        if (Randomly.getBoolean()) {
            sb.append(" USE_FRM");
        }
        return new SQLQueryAdapter(sb.toString(), commonMutationErrors());
    }

    private static SQLQueryAdapter createCandidateOptimize(OceanBaseTable table) {
        StringBuilder sb = new StringBuilder("OPTIMIZE TABLE ");
        sb.append(quoteIdentifier(table.getName()));
        return new SQLQueryAdapter(sb.toString(), commonMutationErrors());
    }

    private static boolean shouldUseVpBoundaryLiteral(OceanBaseGlobalState globalState) {
        if (globalState.getDbmsSpecificOptions().verticalPartitioningDeepExploration) {
            return globalState.getRandomly().getInteger(0, 100) < 40;
        }
        return false;
    }

    private static String generateCandidateValue(OceanBaseGlobalState globalState, OceanBaseColumn column) {
        if (shouldUseVpBoundaryLiteral(globalState)) {
            OceanBaseVerticalPartitioningOracle.recordAdvancedFeature(AdvancedFeature.BOUNDARY_MUTATION);
            return generateCandidateBoundaryLiteral(column);
        }
        OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(globalState)
                .setColumns(column.getTable().getColumns());
        return OceanBaseVisitor.asString(gen.generateConstant(column));
    }

    private static String generateCandidateBoundaryLiteral(OceanBaseColumn column) {
        switch (column.getType()) {
        case INT:
            return Randomly.fromOptions("0", "-0", "-1", "1", "2147483647", "-2147483648", "9223372036854775807",
                    "-9223372036854775808", "18446744073709551615");
        case FLOAT:
        case DOUBLE:
            return Randomly.fromOptions("0", "-0.0", "1e308", "-1e308", "0.0000000000000001",
                    "0.000000000000000000000000000001");
        case DECIMAL:
            return Randomly.fromOptions("0", "-1", "99999999999999999999.9999999999",
                    "-99999999999999999999.9999999999", "0.0000000001", "0.000000000000000000000000000001");
        case VARCHAR:
            return Randomly.fromOptions("''", "' '", "'  '", "'abc '", "'AaBbCc'", "'0'", "'00'", "'-0'", "'1e309'",
                    "'0000-00-00'", "'1970-01-01'", "'2038-01-19'", "'9999-12-31'", "'2024-02-29'", "'变'", "'é'",
                    "'ß'");
        default:
            throw new AssertionError(column.getType());
        }
    }

    private static sqlancer.common.query.ExpectedErrors commonMutationErrors() {
        sqlancer.common.query.ExpectedErrors errors = new sqlancer.common.query.ExpectedErrors();
        OceanBaseErrors.addExpressionErrors(errors);
        OceanBaseErrors.addInsertErrors(errors);
        errors.add("doesn't have this option");
        errors.add("Duplicate key name");
        errors.add("Specified key was too long");
        errors.add("used in key specification without a key length");
        errors.add("out of range");
        errors.add("Data Too Long");
        errors.add("Unknown column");
        errors.add("No memory or reach tenant memory limit");
        return errors;
    }

    private static void logVerticalPartitioningReplacementFailure(OceanBaseGlobalState globalState,
            String oldTableName) {
        String message = "-- VP replacement failed after " + VP_REPLACEMENT_RETRY_COUNT
                + " attempts; keeping old base table " + oldTableName;
        try {
            globalState.getLogger().writeCurrent(message);
        } catch (UnsupportedOperationException ignored) {
            globalState.getState().logStatement(message);
        }
    }

    private static OceanBaseTable findVerticalPartitioningBaseTable(OceanBaseGlobalState globalState,
            String tableName) {
        return globalState.getSchema().getDatabaseTables().stream().filter(t -> t.getName().equals(tableName))
                .filter(t -> !t.isView()).filter(t -> !t.getName().startsWith("vp_"))
                .filter(t -> t.getColumns().stream().anyMatch(c -> !"vp_rowid".equalsIgnoreCase(c.getName())))
                .findFirst().orElse(null);
    }

    private static boolean isValidVerticalPartitioningBaseTable(OceanBaseGlobalState globalState,
            OceanBaseTable table) {
        if (table.isView() || table.getName().startsWith("vp_") || table.getColumns().isEmpty()) {
            return false;
        }
        List<OceanBaseColumn> dataColumns = table.getColumns().stream()
                .filter(c -> !"vp_rowid".equalsIgnoreCase(c.getName())).collect(Collectors.toList());
        if (dataColumns.isEmpty()) {
            return false;
        }
        String columnList = dataColumns.stream().map(c -> quoteIdentifier(c.getName()))
                .collect(Collectors.joining(", "));
        try (Statement s = globalState.getConnection().createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + quoteIdentifier(table.getName()))) {
                if (!rs.next()) {
                    return false;
                }
            }
            try (ResultSet ignored = s
                    .executeQuery("SELECT " + columnList + " FROM " + quoteIdentifier(table.getName()) + " LIMIT 1")) {
                while (ignored.next()) {
                }
            }
            return true;
        } catch (SQLException ignored) {
            return false;
        }
    }

    private static long countVerticalPartitioningBaseTables(OceanBaseGlobalState globalState) {
        return globalState.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> !t.getName().startsWith("vp_")).filter(t -> !t.getColumns().isEmpty())
                .filter(t -> t.getColumns().stream().anyMatch(c -> !"vp_rowid".equalsIgnoreCase(c.getName()))).count();
    }

    private static SQLQueryAdapter generateVerticalPartitioningTable(OceanBaseGlobalState globalState,
            String tableName) {
        int nrColumns = globalState.getRandomly().getInteger(0, 100) < VP_SINGLE_COLUMN_PERCENT ? 1
                : 2 + Randomly.smallNumber();
        return OceanBaseTableGenerator.generate(globalState, tableName, nrColumns);
    }

    private static String getFreeTableName(OceanBaseGlobalState globalState) {
        for (int i = 0;; i++) {
            String tableName = DBMSCommon.createTableName(i);
            boolean exists = globalState.getSchema().getDatabaseTables().stream()
                    .anyMatch(t -> t.getName().equals(tableName));
            if (!exists) {
                return tableName;
            }
        }
    }

    private static String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    @Override
    public SQLConnection createDatabase(OceanBaseGlobalState globalState) throws Exception, SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = OceanBaseOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = OceanBaseOptions.DEFAULT_PORT;
        }
        if (username.endsWith("sys") || username.equals("root")) {
            throw new OceanBaseUserCheckException(
                    "please don't use sys tenant to test! Firstly create tenant then test");
        }
        String databaseName = globalState.getDatabaseName();
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        globalState.getState().logStatement("USE " + databaseName);
        String url = String.format("jdbc:mysql://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                host, port);
        Connection con = DriverManager.getConnection(url, username, password);

        try (Statement s = con.createStatement()) {
            s.execute("set ob_query_timeout=" + globalState.getDbmsSpecificOptions().queryTimeout);
        }
        try (Statement s = con.createStatement()) {
            s.execute("set ob_trx_timeout=" + globalState.getDbmsSpecificOptions().trxTimeout);
        }
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + databaseName);
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "oceanbase";
    }

}
