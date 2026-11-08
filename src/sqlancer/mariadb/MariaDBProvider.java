package sqlancer.mariadb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.auto.service.AutoService;

import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.gen.MariaDBDeleteGenerator;
import sqlancer.mariadb.gen.MariaDBIndexGenerator;
import sqlancer.mariadb.gen.MariaDBInsertGenerator;
import sqlancer.mariadb.gen.MariaDBSetGenerator;
import sqlancer.mariadb.gen.MariaDBTableAdminCommandGenerator;
import sqlancer.mariadb.gen.MariaDBTableGenerator;
import sqlancer.mariadb.gen.MariaDBTruncateGenerator;
import sqlancer.mariadb.gen.MariaDBUpdateGenerator;

@AutoService(DatabaseProvider.class)
public class MariaDBProvider extends SQLProviderAdapter<MariaDBGlobalState, MariaDBOptions> {

    public static final int MAX_EXPRESSION_DEPTH = 3;
    private static final int VP_SINGLE_COLUMN_PERCENT = 40;
    private static final int VP_EMPTY_BASE_TABLE_PERCENT = 10;
    private static final int VP_NON_EMPTY_MIN_ROWS = 8;
    private static final int VP_NON_EMPTY_INSERT_ATTEMPTS = 32;
    private static final int VP_CANDIDATE_RETRY_COUNT = 10;
    /*
     * These objects are probe fixtures, not part of the ordinary random DML surface. In particular, the generic UPDATE
     * generator deliberately uses constants without consulting the destination type, which is appropriate for its
     * expected-error model but not for DATE columns in the period fixture.
     */
    private static final Set<String> VP_MARIADB_FEATURE_OBJECTS = Set.of("vp_system_versioned", "vp_application_period",
            "vp_invisible", "vp_sequence");
    private static final Set<String> VP_MARIADB_FEATURE_TABLES = Set.of("vp_system_versioned", "vp_application_period",
            "vp_invisible");
    private static final AtomicLong VP_CREATION_TOTAL = new AtomicLong();
    private static final AtomicLong VP_CREATION_EMPTY = new AtomicLong();
    private static final AtomicLong VP_CREATION_SINGLE_COLUMN = new AtomicLong();
    private static final AtomicLong VP_CREATION_EMPTY_SINGLE = new AtomicLong();
    private static final AtomicLong VP_CREATION_EMPTY_MULTI = new AtomicLong();
    private static final AtomicLong VP_CREATION_NONEMPTY_SINGLE = new AtomicLong();
    private static final AtomicLong VP_CREATION_NONEMPTY_MULTI = new AtomicLong();
    private static final AtomicLong VP_CREATION_SEED_ATTEMPTS = new AtomicLong();
    private static final AtomicLong VP_CREATION_SEED_SUCCESSES = new AtomicLong();

    private static SQLQueryAdapter generateVerticalPartitioningTable(MariaDBGlobalState state, String name,
            int columns) {
        return MariaDBTableGenerator.generate(name, state.getRandomly(), state.getSchema(), columns);
    }

    public static void ensureVerticalPartitioningBaseTables(MariaDBGlobalState state, int count) throws Exception {
        if (state.getDbmsSpecificOptions().verticalPartitioningMariaDBFeatures) {
            ensureMariaDBFeatureObjects(state);
        }
        while (ordinaryProtectedTableCount(state) < count) {
            String name = state.getSchema().getFreeTableName();
            createValidVerticalPartitioningBaseTable(state, name);
        }
    }

    private static int ordinaryProtectedTableCount(MariaDBGlobalState state) {
        int count = 0;
        for (String table : state.getVerticalPartitioningProtectedTables()) {
            if (!VP_MARIADB_FEATURE_TABLES.contains(table))
                count++;
        }
        return count;
    }

    private static void ensureMariaDBFeatureObjects(MariaDBGlobalState state) throws Exception {
        // Keep feature objects stable across oracle invocations. They are
        // ordinary protected tables for current-state VP checks; temporal
        // history and sequence advancement are exercised by dedicated probes.
        executeFeature(state, "CREATE TABLE IF NOT EXISTS `vp_system_versioned` " + "(`id` INT NOT NULL, `val` INT, "
                + "PRIMARY KEY (`id`)) WITH SYSTEM VERSIONING");
        executeFeature(state,
                "CREATE TABLE IF NOT EXISTS `vp_application_period` "
                        + "(`id` INT NOT NULL, `s` DATE NOT NULL, `e` DATE NOT NULL, "
                        + "PERIOD FOR `app_time` (`s`, `e`), PRIMARY KEY (`id`))");
        executeFeature(state, "CREATE TABLE IF NOT EXISTS `vp_invisible` "
                + "(`id` INT NOT NULL, `visible_val` INT, `hidden_val` INT INVISIBLE, " + "PRIMARY KEY (`id`))");
        executeFeature(state, "CREATE SEQUENCE IF NOT EXISTS `vp_sequence` " + "START WITH 1 INCREMENT BY 1");
        executeFeature(state, "INSERT IGNORE INTO `vp_system_versioned` (`id`, `val`) VALUES (1, 10)");
        executeFeature(state, "INSERT IGNORE INTO `vp_application_period` (`id`, `s`, `e`) "
                + "VALUES (1, '2020-01-01', '2030-01-01')");
        executeFeature(state,
                "INSERT IGNORE INTO `vp_invisible` (`id`, `visible_val`, `hidden_val`) " + "VALUES (1, 10, 20)");
        state.updateSchema();
        state.getVerticalPartitioningProtectedTables().addAll(VP_MARIADB_FEATURE_TABLES);
    }

    private static MariaDBSchema schemaForGeneralActions(MariaDBGlobalState state) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningMariaDBFeatures) {
            return state.getSchema();
        }
        List<MariaDBSchema.MariaDBTable> tables = new ArrayList<>(state.getSchema().getDatabaseTables());
        tables.removeIf(table -> VP_MARIADB_FEATURE_OBJECTS.contains(table.getName()));
        return new MariaDBSchema(tables);
    }

    private static Set<String> excludedFromGeneralInserts(MariaDBGlobalState state) throws SQLException {
        Set<String> excluded = emptyVerticalPartitioningProtectedTables(state);
        if (state.getDbmsSpecificOptions().verticalPartitioningMariaDBFeatures) {
            excluded.addAll(VP_MARIADB_FEATURE_OBJECTS);
        }
        return excluded;
    }

    private static void executeFeature(MariaDBGlobalState state, String sql) throws Exception {
        ExpectedErrors errors = new ExpectedErrors();
        MariaDBErrors.addCommonErrors(errors);
        errors.add("already exists");
        errors.add("Duplicate entry");
        errors.add("not supported");
        state.executeStatement(new SQLQueryAdapter(sql, errors, true));
    }

    private static void createValidVerticalPartitioningBaseTable(MariaDBGlobalState state, String name)
            throws Exception {
        int columns = state.getRandomly().getInteger(0, 100) < VP_SINGLE_COLUMN_PERCENT ? 1
                : 2 + Randomly.smallNumber();
        boolean keepEmpty = state.getRandomly().getInteger(0, 100) < VP_EMPTY_BASE_TABLE_PERCENT;
        for (int attempt = 0; attempt < VP_CANDIDATE_RETRY_COUNT; attempt++) {
            state.executeStatement(generateVerticalPartitioningTable(state, name, columns));
            state.updateSchema();
            MariaDBSchema.MariaDBTable table = findTable(state, name);
            if (table == null)
                continue;
            boolean seeded = keepEmpty || seedNonEmptyVerticalPartitioningTable(state, table);
            if (seeded) {
                recordVerticalPartitioningCreation(table, keepEmpty);
                state.getVerticalPartitioningProtectedTables().add(name);
                return;
            }
            state.executeStatement(new SQLQueryAdapter("DROP TABLE IF EXISTS `" + name.replace("`", "``") + "`",
                    new ExpectedErrors(), true));
            state.updateSchema();
        }
        throw new IgnoreMeException();
    }

    private static MariaDBSchema.MariaDBTable findTable(MariaDBGlobalState state, String name) {
        return state.getSchema().getDatabaseTables().stream().filter(t -> t.getName().equals(name))
                .filter(t -> !t.isView()).filter(t -> !t.getColumns().isEmpty()).findFirst().orElse(null);
    }

    private static boolean seedNonEmptyVerticalPartitioningTable(MariaDBGlobalState state,
            MariaDBSchema.MariaDBTable table) throws Exception {
        long rows = countRows(state, table.getName());
        for (int attempt = 0; attempt < VP_NON_EMPTY_INSERT_ATTEMPTS && rows < VP_NON_EMPTY_MIN_ROWS; attempt++) {
            VP_CREATION_SEED_ATTEMPTS.incrementAndGet();
            try {
                boolean executed = state.executeStatement(MariaDBInsertGenerator.insertVpSeed(table, rows));
                if (executed) {
                    long updatedRows = countRows(state, table.getName());
                    if (updatedRows > rows) {
                        VP_CREATION_SEED_SUCCESSES.addAndGet(updatedRows - rows);
                    }
                    rows = updatedRows;
                }
            } catch (IgnoreMeException ignored) {
                // Retry with a fresh row when a generated row violates a constraint.
            }
        }
        return rows >= VP_NON_EMPTY_MIN_ROWS;
    }

    private static void recordVerticalPartitioningCreation(MariaDBSchema.MariaDBTable table, boolean empty) {
        VP_CREATION_TOTAL.incrementAndGet();
        boolean single = table.getColumns().size() == 1;
        if (empty) {
            VP_CREATION_EMPTY.incrementAndGet();
            (single ? VP_CREATION_EMPTY_SINGLE : VP_CREATION_EMPTY_MULTI).incrementAndGet();
        } else {
            (single ? VP_CREATION_NONEMPTY_SINGLE : VP_CREATION_NONEMPTY_MULTI).incrementAndGet();
        }
        if (single) {
            VP_CREATION_SINGLE_COLUMN.incrementAndGet();
        }
    }

    /** Returns creation-time VP metrics for inclusion in the VP statistics log. */
    public static String verticalPartitioningCreationStats() {
        long total = VP_CREATION_TOTAL.get();
        double emptyRatio = total == 0 ? 0.0 : (double) VP_CREATION_EMPTY.get() / total;
        double singleRatio = total == 0 ? 0.0 : (double) VP_CREATION_SINGLE_COLUMN.get() / total;
        return "-- [vp-stats] creation-shape-counts total=" + total + " empty-single=" + VP_CREATION_EMPTY_SINGLE.get()
                + " empty-multi=" + VP_CREATION_EMPTY_MULTI.get() + " nonempty-single="
                + VP_CREATION_NONEMPTY_SINGLE.get() + " nonempty-multi=" + VP_CREATION_NONEMPTY_MULTI.get() + " empty="
                + VP_CREATION_EMPTY.get() + " nonempty=" + (total - VP_CREATION_EMPTY.get()) + " single-column="
                + VP_CREATION_SINGLE_COLUMN.get() + " multi-column=" + (total - VP_CREATION_SINGLE_COLUMN.get()) + "\n"
                + "-- [vp-stats] creation-empty-ratio=" + String.format(java.util.Locale.ROOT, "%.6f", emptyRatio)
                + " creation-single-column-ratio=" + String.format(java.util.Locale.ROOT, "%.6f", singleRatio) + "\n"
                + "-- [vp-stats] creation-seed-attempts=" + VP_CREATION_SEED_ATTEMPTS.get()
                + " creation-seed-successes=" + VP_CREATION_SEED_SUCCESSES.get();
    }

    private static long countRows(MariaDBGlobalState state, String table) throws SQLException {
        try (Statement statement = state.getConnection().createStatement(); java.sql.ResultSet rs = statement
                .executeQuery("SELECT COUNT(*) FROM `" + table.replace("`", "``") + "`")) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            // The schema cache can lag behind a CREATE OR REPLACE/DROP issued
            // by the preceding mutation batch. A vanished candidate is not a
            // database failure and must not become a database*.log assertion.
            String message = e.getMessage();
            if (message != null && (message.contains("doesn't exist") || message.contains("does not exist"))) {
                throw new IgnoreMeException();
            }
            throw e;
        }
    }

    public static void replaceVerticalPartitioningTableAfterSuccessfulCheck(MariaDBGlobalState state, String oldName)
            throws Exception {
        if (VP_MARIADB_FEATURE_TABLES.contains(oldName)) {
            // Feature fixtures are persistent VP candidates. Replacing them
            // would remove the object that subsequent feature checks depend on.
            return;
        }
        String replacement = state.getSchema().getFreeTableName();
        createValidVerticalPartitioningBaseTable(state, replacement);
        state.executeStatement(new SQLQueryAdapter("DROP TABLE IF EXISTS " + oldName, new ExpectedErrors(), true));
        state.updateSchema();
        state.getVerticalPartitioningProtectedTables().remove(oldName);
        if (usesVerticalPartitioningOracle(state.getDbmsSpecificOptions())) {
            // Keep the next VP batch populated before the next oracle invocation.
            ensureVerticalPartitioningBaseTables(state, 2);
        }
    }

    private static boolean usesVerticalPartitioningOracle(MariaDBOptions options) {
        return options.getTestOracleFactory().stream()
                .anyMatch(o -> o == MariaDBOracleFactory.VERTICAL_PARTITIONING || o == MariaDBOracleFactory.ECST);
    }

    public MariaDBProvider() {
        super(MariaDBGlobalState.class, MariaDBOptions.class);
    }

    enum Action {
        ANALYZE_TABLE, //
        CHECKSUM, //
        CHECK_TABLE, //
        CREATE_INDEX, //
        INSERT, //
        OPTIMIZE, //
        REPAIR_TABLE, //
        SET, //
        TRUNCATE, //
        UPDATE, //
        DELETE,
    }

    @Override
    public void generateDatabase(MariaDBGlobalState globalState) throws Exception {
        MainOptions options = globalState.getOptions();
        if (globalState.getDbmsSpecificOptions().oracles.contains(MariaDBOracleFactory.ECST)) {
            sqlancer.mariadb.oracle.MariaDBEcstSession.prepare(globalState.getConnection().getJdbcConnection());
        }

        if (usesVerticalPartitioningOracle(globalState.getDbmsSpecificOptions())) {
            ensureVerticalPartitioningBaseTables(globalState, 2);
        }
        while (globalState.getSchema().getDatabaseTables().size() < Randomly.getNotCachedInteger(1, 3)) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
            SQLQueryAdapter createTable = MariaDBTableGenerator.generate(tableName, globalState.getRandomly(),
                    globalState.getSchema());
            globalState.executeStatement(createTable);
        }

        int[] nrRemaining = new int[Action.values().length];
        List<Action> actions = new ArrayList<>();
        int total = 0;
        for (int i = 0; i < Action.values().length; i++) {
            Action action = Action.values()[i];
            int nrPerformed = 0;
            switch (action) {
            case CHECKSUM:
            case CHECK_TABLE:
            case TRUNCATE:
            case REPAIR_TABLE:
            case OPTIMIZE:
            case ANALYZE_TABLE:
            case UPDATE:
            case CREATE_INDEX:
                nrPerformed = action == Action.TRUNCATE
                        && usesVerticalPartitioningOracle(globalState.getDbmsSpecificOptions())
                        && !globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes ? 0
                                : globalState.getRandomly().getInteger(0, 2);
                break;
            case DELETE:
                nrPerformed = usesVerticalPartitioningOracle(globalState.getDbmsSpecificOptions())
                        && !globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes ? 0
                                : globalState.getRandomly().getInteger(0, 2);
                break;
            case SET:
                nrPerformed = globalState.getDbmsSpecificOptions().oracles.contains(MariaDBOracleFactory.ECST) ? 0 : 20;
                break;
            case INSERT:
                nrPerformed = globalState.getRandomly().getInteger(0, options.getMaxNumberInserts());
                break;
            default:
                throw new AssertionError(action);
            }
            if (nrPerformed != 0) {
                actions.add(action);
            }
            nrRemaining[action.ordinal()] = nrPerformed;
            total += nrPerformed;
        }
        while (total != 0) {
            Action nextAction = null;
            int selection = globalState.getRandomly().getInteger(0, total);
            int previousRange = 0;
            for (int i = 0; i < nrRemaining.length; i++) {
                if (previousRange <= selection && selection < previousRange + nrRemaining[i]) {
                    nextAction = Action.values()[i];
                    break;
                } else {
                    previousRange += nrRemaining[i];
                }
            }
            assert nextAction != null;
            assert nrRemaining[nextAction.ordinal()] > 0;
            nrRemaining[nextAction.ordinal()]--;
            SQLQueryAdapter query;
            try {
                MariaDBSchema actionSchema = schemaForGeneralActions(globalState);
                switch (nextAction) {
                case CHECKSUM:
                    query = MariaDBTableAdminCommandGenerator.checksumTable(actionSchema);
                    break;
                case CHECK_TABLE:
                    query = MariaDBTableAdminCommandGenerator.checkTable(actionSchema);
                    break;
                case TRUNCATE:
                    query = MariaDBTruncateGenerator.truncate(actionSchema);
                    break;
                case REPAIR_TABLE:
                    query = MariaDBTableAdminCommandGenerator.repairTable(actionSchema);
                    break;
                case INSERT:
                    // VP base tables are part of the mutation surface when they contain
                    // rows. Empty VP bases are deliberate boundary cases and must remain
                    // empty after bootstrap; filter only those tables here.
                    query = MariaDBInsertGenerator.insert(actionSchema, globalState.getRandomly(),
                            excludedFromGeneralInserts(globalState));
                    break;
                case OPTIMIZE:
                    query = MariaDBTableAdminCommandGenerator.optimizeTable(actionSchema);
                    break;
                case ANALYZE_TABLE:
                    query = MariaDBTableAdminCommandGenerator.analyzeTable(actionSchema);
                    break;
                case UPDATE:
                    query = MariaDBUpdateGenerator.update(actionSchema, globalState.getRandomly());
                    break;
                case CREATE_INDEX:
                    query = MariaDBIndexGenerator.generate(actionSchema);
                    break;
                case SET:
                    query = MariaDBSetGenerator.set(globalState.getRandomly(), options);
                    break;
                case DELETE:
                    query = MariaDBDeleteGenerator.delete(actionSchema, globalState.getRandomly());
                    break;
                default:
                    throw new AssertionError(nextAction);
                }
            } catch (IgnoreMeException e) {
                total--;
                continue;
            }
            try {
                globalState.executeStatement(query);
            } catch (Throwable t) {
                System.err.println(query.getQueryString());
                throw t;
            }
            total--;
        }
        if (usesVerticalPartitioningOracle(globalState.getDbmsSpecificOptions())) {
            ensureVerticalPartitioningBaseTables(globalState, 2);
        }
    }

    private static Set<String> emptyVerticalPartitioningProtectedTables(MariaDBGlobalState state) throws SQLException {
        Set<String> empty = new HashSet<>();
        for (String table : state.getVerticalPartitioningProtectedTables()) {
            if (countRows(state, table) == 0) {
                empty.add(table);
            }
        }
        return empty;
    }

    public static class MariaDBGlobalState extends SQLGlobalState<MariaDBOptions, MariaDBSchema> {
        private final Set<String> verticalPartitioningProtectedTables = new HashSet<>();

        public Set<String> getVerticalPartitioningProtectedTables() {
            return verticalPartitioningProtectedTables;
        }

        @Override
        protected MariaDBSchema readSchema() throws SQLException {
            return MariaDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    public static boolean isMariaDBFeatureTable(String name) {
        return VP_MARIADB_FEATURE_TABLES.contains(name);
    }

    @Override
    public SQLConnection createDatabase(MariaDBGlobalState globalState) throws SQLException {
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + globalState.getDatabaseName());
        globalState.getState().logStatement("CREATE DATABASE " + globalState.getDatabaseName());
        globalState.getState().logStatement("USE " + globalState.getDatabaseName());
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = MariaDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = MariaDBOptions.DEFAULT_PORT;
        }
        String url = String.format("jdbc:mariadb://%s:%d", host, port);
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + globalState.getDatabaseName());
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + globalState.getDatabaseName());
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + globalState.getDatabaseName());
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "mariadb";
    }

}
