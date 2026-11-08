package sqlancer.databend;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.List;

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
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.gen.DatabendDeleteGenerator;
import sqlancer.databend.gen.DatabendInsertGenerator;
import sqlancer.databend.gen.DatabendRandomQuerySynthesizer;
import sqlancer.databend.gen.DatabendTableGenerator;
import sqlancer.databend.gen.DatabendViewGenerator;
import sqlancer.databend.oracle.DatabendVerticalPartitioningOracle;

@AutoService(DatabaseProvider.class)
public class DatabendProvider extends SQLProviderAdapter<DatabendGlobalState, DatabendOptions> {

    private static boolean usesVerticalPartitioningOracle(DatabendGlobalState state) {
        return state.getDbmsSpecificOptions().getTestOracleFactory()
                .stream().anyMatch(o -> o == DatabendOracleFactory.VERTICAL_PARTITIONING
                        || o == DatabendOracleFactory.ECST);
    }

    private static final int VP_BASE_TABLES_PER_BATCH = 2;
    private static final int VP_EMPTY_TABLE_PERCENT = 10;

    public static void ensureVerticalPartitioningBaseTables(DatabendGlobalState state, int minimum) throws Exception {
        state.updateSchema();
        int guard = 0;
        while (countBaseTables(state) < minimum && guard++ < minimum * 10) {
            createVerticalPartitioningBaseTable(state);
            state.updateSchema();
        }
        if (countBaseTables(state) < minimum) {
            throw new IgnoreMeException();
        }
    }

    private static void createVerticalPartitioningBaseTable(DatabendGlobalState state) throws Exception {
        for (int attempt = 0; attempt < 10; attempt++) {
            String tableName = state.getSchema().getFreeTableName();
            int shape = state.getRandomly().getInteger(0, 100);
            int columns = shape < 22 ? 1
                    : (shape < 60 ? state.getRandomly().getInteger(4, 9) : state.getRandomly().getInteger(2, 4));
            SQLQueryAdapter create = new DatabendTableGenerator(columns).getQuery(state, tableName);
            if (state.executeStatement(create)) {
                state.updateSchema();
                DatabendSchema.DatabendTable table = state.getSchema().getDatabaseTables().stream()
                        .filter(t -> t.getName().equals(tableName)).findFirst().orElse(null);
                if (table != null && state.getRandomly().getInteger(0, 100) >= VP_EMPTY_TABLE_PERCENT) {
                    for (int i = 0; i < 12; i++) {
                        Long rows = tryCountRows(state, table);
                        if (rows != null && rows >= 8) {
                            break;
                        }
                        try {
                            state.executeStatement(DatabendInsertGenerator.getQuery(state, table));
                        } catch (IgnoreMeException ignored) {
                            // Failed INSERTs still consume one bounded attempt.
                        }
                    }
                }
                return;
            }
        }
        throw new IgnoreMeException();
    }

    private static Long tryCountRows(DatabendGlobalState state, DatabendSchema.DatabendTable table) {
        try (Statement s = state.getConnection().createStatement();
                ResultSet r = s.executeQuery("SELECT COUNT(*) FROM " + table.getName())) {
            return r.next() ? r.getLong(1) : 0;
        } catch (SQLException e) {
            return null;
        }
    }

    private static void populateVerticalPartitioningBaseTables(DatabendGlobalState state) throws Exception {
        state.updateSchema();
        List<DatabendSchema.DatabendTable> tables = state.getSchema().getDatabaseTables().stream()
                .filter(t -> !t.isView() && !t.getName().startsWith("vp_"))
                .collect(java.util.stream.Collectors.toList());
        if (tables.isEmpty()) {
            throw new IgnoreMeException();
        }
        // Each INSERT is generated randomly by SQLancer; do not mechanically
        // repeat a fixed row count for every table.
        int maxInserts = Math.max(1, state.getOptions().getMaxNumberInserts());
        int minBudget = Math.max(1, tables.size());
        int maxBudget = Math.max(minBudget, maxInserts * tables.size());
        int actionBudget = state.getRandomly().getInteger(minBudget, maxBudget);
        for (int i = 0; i < actionBudget; i++) {
            try {
                DatabendSchema.DatabendTable target = tables.get(state.getRandomly().getInteger(0, tables.size()));
                Long rows = tryCountRows(state, target);
                if (rows != null && rows == 0) {
                    continue;
                }
                state.executeStatement(DatabendInsertGenerator.getQuery(state, target));
            } catch (IgnoreMeException ignored) {
                // Keep subsequent random actions alive when one is inapplicable.
            }
        }
        state.updateSchema();
    }

    private static int countBaseTables(DatabendGlobalState state) {
        return (int) state.getSchema().getDatabaseTables().stream()
                .filter(t -> !t.isView() && !t.getName().startsWith("vp_")).count();
    }

    public static void replaceVerticalPartitioningTableAfterSuccessfulCheck(DatabendGlobalState state, String oldName)
            throws Exception {
        replaceVerticalPartitioningTables(state, List.of(oldName), VP_BASE_TABLES_PER_BATCH);
    }

    public static void replaceVerticalPartitioningTables(DatabendGlobalState state, List<String> oldNames,
            int replacementCount) throws Exception {
        for (String oldName : oldNames) {
            state.executeStatement(new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(oldName), true));
        }
        state.updateSchema();
        ensureVerticalPartitioningBaseTables(state, replacementCount);
        populateVerticalPartitioningBaseTables(state);
        state.updateSchema();
    }

    private static String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    public DatabendProvider() {
        super(DatabendGlobalState.class, DatabendOptions.class);
    }

    public enum Action implements AbstractAction<DatabendGlobalState> {

        INSERT(DatabendInsertGenerator::getQuery), DELETE(DatabendDeleteGenerator::generate),
        // TODO 等待databend实现update
        // UPDATE(DatabendUpdateGenerator::getQuery), //
        CREATE_VIEW(DatabendViewGenerator::generate), EXPLAIN((g) -> {
            ExpectedErrors errors = new ExpectedErrors();
            DatabendErrors.addExpressionErrors(errors);
            DatabendErrors.addGroupByErrors(errors);
            return new SQLQueryAdapter(
                    "EXPLAIN " + DatabendToStringVisitor
                            .asString(DatabendRandomQuerySynthesizer.generateSelect(g, Randomly.smallNumber() + 1)),
                    errors);
        });

        private final SQLQueryProvider<DatabendGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<DatabendGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(DatabendGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(DatabendGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case EXPLAIN:
            return r.getInteger(0, 2);
        // TODO 等待databend实现update && delete
        // case UPDATE:
        // return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumUpdates + 1);
        case DELETE:
            return globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes
                    ? r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumDeletes + 1) : 0;
        case CREATE_VIEW:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumViews + 1);
        default:
            throw new AssertionError(a);
        }
    }

    public static class DatabendGlobalState extends SQLGlobalState<DatabendOptions, DatabendSchema> {

        @Override
        protected DatabendSchema readSchema() throws SQLException {
            return DatabendSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public void generateDatabase(DatabendGlobalState globalState) throws Exception {
        if (globalState.getDbmsSpecificOptions().oracles.contains(DatabendOracleFactory.ECST)) {
            sqlancer.databend.oracle.DatabendEcstSession.prepare(globalState.getConnection().getJdbcConnection());
        }
        if (usesVerticalPartitioningOracle(globalState)) {
            ensureVerticalPartitioningBaseTables(globalState, 2);
            populateVerticalPartitioningBaseTables(globalState);
            if (!globalState.getDbmsSpecificOptions().oracles.contains(DatabendOracleFactory.ECST)) {
                DatabendVerticalPartitioningOracle.logSchemaShapeStats(globalState, "post-generate");
            }
            return;
        }
        for (int i = 0; i < Randomly.fromOptions(3, 4); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new DatabendTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException(); // TODO
        }
        StatementExecutor<DatabendGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                DatabendProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements(); // 增删改一些数据（按权重随机选取算法）
    }

    @Override
    public SQLConnection createDatabase(DatabendGlobalState globalState) throws SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = DatabendOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = DatabendOptions.DEFAULT_PORT;
        }
        String databaseName = globalState.getDatabaseName();
        String url = String.format("jdbc:mysql://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                host, port);
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
            globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
            s.execute("CREATE DATABASE " + databaseName);
            globalState.getState().logStatement("CREATE DATABASE " + databaseName);
            s.execute("USE " + databaseName);
            globalState.getState().logStatement("USE " + databaseName);
        }
        if (DatabendBugs.bug15569) {
            con.close();
            String urlWithRetry = String.format(
                    "jdbc:mysql://%s:%d/%s?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true&autoReconnect=true",
                    host, port, databaseName);
            con = DriverManager.getConnection(urlWithRetry, username, password);
        }

        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "databend"; // 用于DatabendOptions
    }

}
