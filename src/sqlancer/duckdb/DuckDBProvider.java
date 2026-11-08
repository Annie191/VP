package sqlancer.duckdb;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.gen.DuckDBDeleteGenerator;
import sqlancer.duckdb.gen.DuckDBIndexGenerator;
import sqlancer.duckdb.gen.DuckDBInsertGenerator;
import sqlancer.duckdb.gen.DuckDBRandomQuerySynthesizer;
import sqlancer.duckdb.gen.DuckDBTableGenerator;
import sqlancer.duckdb.gen.DuckDBUpdateGenerator;
import sqlancer.duckdb.gen.DuckDBViewGenerator;

@AutoService(DatabaseProvider.class)
public class DuckDBProvider extends SQLProviderAdapter<DuckDBGlobalState, DuckDBOptions> {

    private static final int VP_BASE_TABLES_PER_BATCH = 2;
    private static final int VP_REPLACEMENT_RETRY_COUNT = 10;

    public DuckDBProvider() {
        super(DuckDBGlobalState.class, DuckDBOptions.class);
    }

    public enum Action implements AbstractAction<DuckDBGlobalState> {

        INSERT(DuckDBInsertGenerator::getQuery), //
        CREATE_INDEX(DuckDBIndexGenerator::getQuery), //
        VACUUM((g) -> new SQLQueryAdapter("VACUUM;")), //
        ANALYZE((g) -> new SQLQueryAdapter("ANALYZE;")), //
        DELETE(DuckDBDeleteGenerator::generate), //
        UPDATE(DuckDBUpdateGenerator::getQuery), //
        CREATE_VIEW(DuckDBViewGenerator::generate), //
        EXPLAIN((g) -> {
            ExpectedErrors errors = new ExpectedErrors();
            DuckDBErrors.addExpressionErrors(errors);
            DuckDBErrors.addGroupByErrors(errors);
            return new SQLQueryAdapter(
                    "EXPLAIN " + DuckDBToStringVisitor
                            .asString(DuckDBRandomQuerySynthesizer.generateSelect(g, Randomly.smallNumber() + 1)),
                    errors);
        });

        private final SQLQueryProvider<DuckDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<DuckDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(DuckDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(DuckDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        boolean preserveVerticalPartitioningRows = usesVerticalPartitioningOracle(globalState)
                && !globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes;
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case CREATE_INDEX:
            if (!globalState.getDbmsSpecificOptions().testIndexes) {
                return 0;
            }
            // fall through
        case UPDATE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumUpdates + 1);
        case VACUUM: // seems to be ignored
        case ANALYZE: // seems to be ignored
        case EXPLAIN:
            return r.getInteger(0, 2);
        case DELETE:
            if (preserveVerticalPartitioningRows) {
                return 0;
            }
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumDeletes + 1);
        case CREATE_VIEW:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumViews + 1);
        default:
            throw new AssertionError(a);
        }
    }

    public static class DuckDBGlobalState extends SQLGlobalState<DuckDBOptions, DuckDBSchema> {

        @Override
        protected DuckDBSchema readSchema() throws SQLException {
            return DuckDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public void generateDatabase(DuckDBGlobalState globalState) throws Exception {
        boolean usesVerticalPartitioningOracle = usesVerticalPartitioningOracle(globalState);
        if (globalState.getDbmsSpecificOptions().oracles.contains(DuckDBOracleFactory.ECST)) {
            DuckDBEcstSession.prepare(globalState.getConnection().getJdbcConnection());
        }
        if (usesVerticalPartitioningOracle) {
            createVerticalPartitioningTableBatch(globalState);
        } else {
            for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
                boolean success;
                do {
                    SQLQueryAdapter qt = new DuckDBTableGenerator().getQuery(globalState);
                    success = globalState.executeStatement(qt);
                } while (!success);
            }
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException(); // TODO
        }
        StatementExecutor<DuckDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                DuckDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
        if (usesVerticalPartitioningOracle
                && !globalState.getDbmsSpecificOptions().oracles.contains(DuckDBOracleFactory.ECST)) {
            DuckDBVerticalPartitioningOracle.logSchemaShapeStats(globalState, "post-generate");
        }
    }

    public static boolean usesVerticalPartitioningOracle(DuckDBGlobalState globalState) {
        return usesVerticalPartitioningOracle(globalState.getDbmsSpecificOptions());
    }

    private static boolean usesVerticalPartitioningOracle(DuckDBOptions options) {
        return options.getTestOracleFactory().stream()
                .anyMatch(o -> o == DuckDBOracleFactory.VERTICAL_PARTITIONING || o == DuckDBOracleFactory.ECST);
    }

    public static void ensureVerticalPartitioningBaseTables(DuckDBGlobalState globalState, int minBaseTables)
            throws Exception {
        globalState.updateSchema();
        while (countVerticalPartitioningBaseTables(globalState) < minBaseTables) {
            DuckDBSchema.DuckDBTable table = createValidVerticalPartitioningBaseTable(globalState);
            if (table == null) {
                throw new IgnoreMeException();
            }
            globalState.updateSchema();
        }
    }

    public static void replaceVerticalPartitioningTableAfterSuccessfulCheck(DuckDBGlobalState globalState,
            String oldTableName) throws Exception {
        DuckDBSchema.DuckDBTable replacement = createValidVerticalPartitioningBaseTable(globalState);
        if (replacement != null) {
            globalState.executeStatement(
                    new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(oldTableName) + " CASCADE", true));
        }
        globalState.updateSchema();
        ensureVerticalPartitioningBaseTables(globalState, VP_BASE_TABLES_PER_BATCH);
    }

    private static void createVerticalPartitioningTableBatch(DuckDBGlobalState globalState) throws Exception {
        for (int i = 0; i < VP_BASE_TABLES_PER_BATCH; i++) {
            DuckDBSchema.DuckDBTable table = createValidVerticalPartitioningBaseTable(globalState);
            if (table == null) {
                throw new IgnoreMeException();
            }
            globalState.updateSchema();
        }
    }

    private static DuckDBSchema.DuckDBTable createValidVerticalPartitioningBaseTable(DuckDBGlobalState globalState)
            throws Exception {
        if (globalState.getDbmsSpecificOptions().verticalPartitioningDirectedData) {
            createDirectedVerticalPartitioningBaseTable(globalState);
            // Directed data is an additional workload. Keep creating and returning a
            // normal randomized table so EMPTY/SINGLE_COLUMN/MULTI_COLUMN coverage
            // remains active when the directed profile is enabled.
        }
        return createValidRandomVerticalPartitioningBaseTable(globalState);
    }

    private static DuckDBSchema.DuckDBTable createValidRandomVerticalPartitioningBaseTable(
            DuckDBGlobalState globalState) throws Exception {
        for (int attempt = 0; attempt < VP_REPLACEMENT_RETRY_COUNT; attempt++) {
            String tableName = globalState.getSchema().getFreeTableName();
            int requestedColumns = globalState.getRandomly().getInteger(0, 9) < 4 ? 1
                    : globalState.getRandomly().getInteger(2, 4);
            boolean success = globalState
                    .executeStatement(new DuckDBTableGenerator().getQuery(globalState, requestedColumns));
            globalState.updateSchema();
            if (!success) {
                continue;
            }
            DuckDBSchema.DuckDBTable table = findLatestVerticalPartitioningBaseTable(globalState);
            tableName = table == null ? tableName : table.getName();
            if (table != null) {
                boolean keepEmpty = globalState.getRandomly().getInteger(0, 9) == 0;
                if (!keepEmpty) {
                    populateVerticalPartitioningCandidate(globalState, table);
                }
                mutateVerticalPartitioningCandidate(globalState, table, keepEmpty);
                globalState.updateSchema();
                table = findVerticalPartitioningBaseTable(globalState, table.getName());
                if (table != null && isValidVerticalPartitioningBaseTable(globalState, table)) {
                    return table;
                }
                tableName = table == null ? tableName : table.getName();
            }
            globalState
                    .executeStatement(new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(tableName), true));
            globalState.updateSchema();
        }
        return null;
    }

    /**
     * Build one deliberately wide, mixed-type table. The values are generated by DuckDB itself so every run gets
     * different NULL/repetition/extreme-value distributions while still crossing vector and row-group boundaries.
     */
    private static DuckDBSchema.DuckDBTable createDirectedVerticalPartitioningBaseTable(DuckDBGlobalState globalState)
            throws Exception {
        int rows = Math.max(1024, globalState.getDbmsSpecificOptions().verticalPartitioningDirectedSourceRows);
        String tableName = globalState.getSchema().getFreeTableName();
        String create = "CREATE TABLE " + quoteIdentifier(tableName) + " ("
                + "c0 BIGINT, c1 INTEGER, c2 DOUBLE, c3 VARCHAR, "
                + "c4 VARCHAR COLLATE NOCASE, c5 BOOLEAN, c6 DECIMAL(18,3), "
                + "c7 TIMESTAMP, c8 INTERVAL, c9 INTEGER[], " + "c10 STRUCT(f0 INTEGER, f1 VARCHAR), "
                + "c11 MAP(VARCHAR, INTEGER))";
        if (!globalState.executeStatement(new SQLQueryAdapter(create, true))) {
            return null;
        }
        String n = Integer.toString(rows);
        String insert = "INSERT INTO " + quoteIdentifier(tableName) + " SELECT CASE WHEN i % 17 = 0 THEN NULL ELSE "
                + "CASE WHEN i % 29 = 0 THEN 9223372036854775807 ELSE CAST(random() * 2000000000 - 1000000000 AS BIGINT) END END, "
                + "CASE WHEN i % 11 = 0 THEN NULL ELSE CAST(random() * 97 AS INTEGER) END, "
                + "CASE WHEN i % 13 = 0 THEN NULL ELSE (random() * 2e9 - 1e9) END, "
                + "CASE WHEN i % 19 = 0 THEN NULL ELSE concat('v-', CAST(random() * 31 AS INTEGER), '-', repeat('x', CAST(random() * 97 AS INTEGER))) END, "
                + "CASE WHEN i % 7 = 0 THEN 'A' WHEN i % 7 = 1 THEN 'a' ELSE concat('k', CAST(random() * 23 AS INTEGER)) END, "
                + "random() < 0.5, CAST(random() * 100000 AS DECIMAL(18,3)), "
                + "TIMESTAMP '2000-01-01' + CAST(random() * 100000 AS INTEGER) * INTERVAL '1 second', "
                + "CAST(random() * 1000 AS INTEGER) * INTERVAL '1 millisecond', "
                + "[CAST(random() * 5 AS INTEGER), NULL, CASE WHEN random() < 0.33 THEN CAST(random() * 10000 AS INTEGER) ELSE NULL END], "
                + "struct_pack(f0 := CAST(random() * 37 AS INTEGER), f1 := CASE WHEN i % 5 = 0 THEN NULL ELSE concat('u', CAST(random() * 17 AS INTEGER)) END), "
                + "map([concat('m', CAST(random() * 9 AS INTEGER))], [CAST(random() * 101 AS INTEGER)]) FROM range(" + n
                + ") t(i)";
        if (!globalState.executeStatement(new SQLQueryAdapter(insert, true))) {
            globalState
                    .executeStatement(new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(tableName), true));
            globalState.updateSchema();
            return null;
        }
        globalState.updateSchema();
        return findVerticalPartitioningBaseTable(globalState, tableName);
    }

    private static void populateVerticalPartitioningCandidate(DuckDBGlobalState globalState,
            DuckDBSchema.DuckDBTable table) throws Exception {
        for (int attempt = 0; attempt < 12 && countRows(globalState, table.getName()) < 8; attempt++) {
            try {
                globalState.executeStatement(DuckDBInsertGenerator.getQuery(globalState, table));
            } catch (IgnoreMeException ignored) {
                // Invalid generated values count as an attempt; keep trying until the cap.
            }
            globalState.updateSchema();
            table = findVerticalPartitioningBaseTable(globalState, table.getName());
            if (table == null) {
                return;
            }
        }
    }

    private static long countRows(DuckDBGlobalState globalState, String tableName) {
        try (Statement statement = globalState.getConnection().createStatement();
                ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM " + quoteIdentifier(tableName))) {
            return result.next() ? result.getLong(1) : 0;
        } catch (SQLException ignored) {
            return 0;
        }
    }

    private static void mutateVerticalPartitioningCandidate(DuckDBGlobalState globalState,
            DuckDBSchema.DuckDBTable table, boolean keepEmpty) throws Exception {
        int actions = Math.max(1, globalState.getOptions().getMaxNumberInserts())
                + globalState.getRandomly().getInteger(1, 11);
        for (int i = 0; i < actions; i++) {
            globalState.updateSchema();
            table = findVerticalPartitioningBaseTable(globalState, table.getName());
            if (table == null) {
                return;
            }
            try {
                globalState.executeStatement(createVerticalPartitioningCandidateMutation(globalState, table, keepEmpty));
            } catch (IgnoreMeException ignored) {
            }
        }
    }

    private static SQLQueryAdapter createVerticalPartitioningCandidateMutation(DuckDBGlobalState globalState,
            DuckDBSchema.DuckDBTable table, boolean keepEmpty) throws Exception {
        List<SQLQueryProvider<DuckDBGlobalState>> actions = new ArrayList<>();
        if (!keepEmpty) {
            actions.add(g -> DuckDBInsertGenerator.getQuery(g, table));
        }
        actions.add(g -> DuckDBUpdateGenerator.getQuery(g, table));
        if (table.getColumns().stream().anyMatch(c -> c.getType().isIndexable())) {
            actions.add(g -> DuckDBIndexGenerator.getQuery(g, table));
        }
        actions.add(g -> new SQLQueryAdapter("ANALYZE;"));
        if (globalState.getDbmsSpecificOptions().verticalPartitioningDeepExploration) {
            actions.add(DuckDBVerticalPartitioningOracle::createBoundaryValueMutation);
        }
        if (globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes) {
            actions.add(DuckDBDeleteGenerator::generate);
        }
        return Randomly.fromList(actions).getQuery(globalState);
    }

    private static long countVerticalPartitioningBaseTables(DuckDBGlobalState globalState) {
        return globalState.getSchema().getDatabaseTables().stream().filter(DuckDBProvider::isPotentialVpBaseTable)
                .count();
    }

    private static DuckDBSchema.DuckDBTable findLatestVerticalPartitioningBaseTable(DuckDBGlobalState globalState) {
        List<DuckDBSchema.DuckDBTable> tables = globalState.getSchema().getDatabaseTables().stream()
                .filter(DuckDBProvider::isPotentialVpBaseTable).collect(Collectors.toList());
        if (tables.isEmpty()) {
            return null;
        }
        return tables.get(tables.size() - 1);
    }

    private static DuckDBSchema.DuckDBTable findVerticalPartitioningBaseTable(DuckDBGlobalState globalState,
            String tableName) {
        return globalState.getSchema().getDatabaseTables().stream().filter(t -> t.getName().equals(tableName))
                .filter(DuckDBProvider::isPotentialVpBaseTable).findFirst().orElse(null);
    }

    private static boolean isPotentialVpBaseTable(DuckDBSchema.DuckDBTable table) {
        return !table.isView() && !table.getName().startsWith("vp_") && !table.getColumns().isEmpty();
    }

    private static boolean isValidVerticalPartitioningBaseTable(DuckDBGlobalState globalState,
            DuckDBSchema.DuckDBTable table) {
        if (!isPotentialVpBaseTable(table)) {
            return false;
        }
        String columnList = table.getColumns().stream().filter(c -> !c.isSyntheticRowId())
                .map(c -> quoteIdentifier(c.getName())).collect(Collectors.joining(", "));
        if (columnList.isEmpty()) {
            return false;
        }
        try (Statement s = globalState.getConnection().createStatement()) {
            try (ResultSet ignored = s.executeQuery("SELECT COUNT(*) FROM " + quoteIdentifier(table.getName()))) {
                while (ignored.next()) {
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

    public static String quoteIdentifier(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    public void tryDeleteFile(String fname) {
        try {
            File f = new File(fname);
            f.delete();
        } catch (Exception e) {
        }
    }

    public void tryDeleteDatabase(String dbpath) {
        if (dbpath.equals("") || dbpath.equals(":memory:")) {
            return;
        }
        tryDeleteFile(dbpath);
        tryDeleteFile(dbpath + ".wal");
    }

    @Override
    public SQLConnection createDatabase(DuckDBGlobalState globalState) throws SQLException {
        String databaseFile = System.getProperty("duckdb.database.file", "");
        String url = "jdbc:duckdb:" + databaseFile;
        tryDeleteDatabase(databaseFile);

        MainOptions options = globalState.getOptions();
        if (!(options.isDefaultUsername() && options.isDefaultPassword())) {
            throw new AssertionError("DuckDB doesn't support credentials (username/password)");
        }

        Connection conn = DriverManager.getConnection(url);
        try (Statement stmt = conn.createStatement()) {
            /*
             * Every SQLancer worker normally opens an in-memory DuckDB database. DuckDB's default spill directory is
             * relative to the process working directory (".tmp"), so concurrent workers otherwise write and remove
             * identically named duckdb_temp_storage_* files. Besides spurious short reads, that can corrupt the
             * execution context of another worker.
             */
            Path tempDirectory = Files.createTempDirectory("sqlancer-duckdb-" + globalState.getDatabaseName() + "-");
            stmt.execute("SET temp_directory='" + tempDirectory.toAbsolutePath().toString().replace("'", "''") + "'");
            if (!globalState.getDbmsSpecificOptions().oracles.contains(DuckDBOracleFactory.ECST)) {
                stmt.execute("PRAGMA checkpoint_threshold='1 byte';");
            }
        } catch (IOException e) {
            try {
                conn.close();
            } catch (SQLException ignored) {
                // Keep the directory-creation failure as the primary exception.
            }
            throw new SQLException("Could not create an isolated DuckDB temporary directory", e);
        }
        return new SQLConnection(conn);
    }

    @Override
    public String getDBMSName() {
        return "duckdb";
    }

}
