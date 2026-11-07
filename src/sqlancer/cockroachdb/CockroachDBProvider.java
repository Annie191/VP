package sqlancer.cockroachdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.auto.service.AutoService;

import sqlancer.DatabaseProvider;
import sqlancer.ExecutionTimer;
import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Main.QueryManager;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;
import sqlancer.cockroachdb.gen.CockroachDBCommentOnGenerator;
import sqlancer.cockroachdb.gen.CockroachDBCreateStatisticsGenerator;
import sqlancer.cockroachdb.gen.CockroachDBDeleteGenerator;
import sqlancer.cockroachdb.gen.CockroachDBDropTableGenerator;
import sqlancer.cockroachdb.gen.CockroachDBDropViewGenerator;
import sqlancer.cockroachdb.gen.CockroachDBIndexGenerator;
import sqlancer.cockroachdb.gen.CockroachDBInsertGenerator;
import sqlancer.cockroachdb.gen.CockroachDBRandomQuerySynthesizer;
import sqlancer.cockroachdb.gen.CockroachDBSetClusterSettingGenerator;
import sqlancer.cockroachdb.gen.CockroachDBSetSessionGenerator;
import sqlancer.cockroachdb.gen.CockroachDBShowGenerator;
import sqlancer.cockroachdb.gen.CockroachDBTableGenerator;
import sqlancer.cockroachdb.gen.CockroachDBTruncateGenerator;
import sqlancer.cockroachdb.gen.CockroachDBUpdateGenerator;
import sqlancer.cockroachdb.gen.CockroachDBViewGenerator;
import sqlancer.cockroachdb.oracle.CockroachDBVerticalPartitioningOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.query.SQLancerResultSet;

@AutoService(DatabaseProvider.class)
public class CockroachDBProvider extends SQLProviderAdapter<CockroachDBGlobalState, CockroachDBOptions> {

    private static final int VP_BASE_TABLES_PER_BATCH = 2;
    private static final int VP_REPLACEMENT_RETRY_COUNT = 10;
    private static final int VP_TRANSIENT_CLUSTER_RETRIES = 3;
    private static final int VP_EMPTY_BASE_TABLE_PERCENT = 10;
    private static final int VP_NON_EMPTY_MIN_ROWS = 8;
    private static final int VP_NON_EMPTY_INSERT_ATTEMPTS = 12;
    private static final int VP_BOOTSTRAP_CANDIDATE_MUTATIONS = 3;
    private static final int VP_DATABASE_CREATE_RETRIES = 120;
    private static final int VP_SCHEMA_REFRESH_RETRIES = 12;
    // A one-second TTL races ordinary reads with GC while distributed schema jobs are settling.
    // Ten minutes still reclaims short-lived VP databases quickly without manufacturing stale reads.
    private static final int VP_SCHEMA_GC_TTL_SECONDS = 600;
    private static final int VP_ACTION_QUERY_TIMEOUT_SECONDS = 15;
    // A three-node unlicensed cluster exhausts its transaction budget well before 24 concurrent
    // schema jobs have settled. Keep enough DDL pressure for coverage without starving VP reads.
    private static final int VP_MAX_CONCURRENT_SCHEMA_JOBS = 8;
    private static final int VP_SCHEMA_JOB_BACKPRESSURE_RETRIES = 120;
    private static final int VP_MAX_CONCURRENT_DATABASE_BOOTSTRAPS = 1;
    private static final int VP_MAX_CONCURRENT_ORACLE_RUNS = 4;
    public static final String VP_GATEWAY_CLUSTER_MISMATCH_SQL_STATE = "VP001";
    private static final Semaphore VP_DATABASE_CREATE_PERMITS = new Semaphore(VP_MAX_CONCURRENT_DATABASE_BOOTSTRAPS,
            true);
    private static final Semaphore VP_ORACLE_RUN_PERMITS = new Semaphore(VP_MAX_CONCURRENT_ORACLE_RUNS, true);
    private static final ReentrantLock VP_SCHEMA_CHANGE_LOCK = new ReentrantLock(true);
    private static final AtomicLong VP_VIEW_COUNTER = new AtomicLong();

    public CockroachDBProvider() {
        super(CockroachDBGlobalState.class, CockroachDBOptions.class);
    }

    public enum Action {
        CREATE_TABLE(CockroachDBTableGenerator::generate), CREATE_INDEX(CockroachDBIndexGenerator::create), //
        CREATE_VIEW(CockroachDBViewGenerator::generate), //
        CREATE_STATISTICS(CockroachDBCreateStatisticsGenerator::create), //
        INSERT(CockroachDBInsertGenerator::insert), //
        UPDATE(CockroachDBUpdateGenerator::gen), //
        SET_SESSION(CockroachDBSetSessionGenerator::create), //
        SET_CLUSTER_SETTING(CockroachDBSetClusterSettingGenerator::create), //
        DELETE(CockroachDBDeleteGenerator::delete), //
        TRUNCATE(CockroachDBTruncateGenerator::truncate), //
        DROP_TABLE(CockroachDBDropTableGenerator::drop), //
        DROP_VIEW(CockroachDBDropViewGenerator::drop), //
        COMMENT_ON(CockroachDBCommentOnGenerator::comment), //
        SHOW(CockroachDBShowGenerator::show), //
        TRANSACTION((g) -> {
            String s = Randomly.fromOptions("BEGIN", "ROLLBACK", "COMMIT");
            return new SQLQueryAdapter(s, ExpectedErrors.from("there is no transaction in progress",
                    "there is already a transaction in progress", "current transaction is aborted"));
        }), EXPLAIN((g) -> {
            StringBuilder sb = new StringBuilder("EXPLAIN ");
            ExpectedErrors errors = new ExpectedErrors();
            if (Randomly.getBoolean()) {
                sb.append("(");
                if (isVectorizeEnabled(g)) {
                    sb.append(Randomly.fromOptions("VERBOSE", "TYPES", "OPT", "DISTSQL", "VEC"));
                } else {
                    sb.append(Randomly.fromOptions("VERBOSE", "TYPES", "OPT", "DISTSQL"));
                }
                sb.append(") ");
                errors.add("cannot set EXPLAIN mode more than once");
                errors.add("unable to vectorize execution plan");
                errors.add("unsupported type");
            }
            sb.append(CockroachDBRandomQuerySynthesizer.generate(g, Randomly.smallNumber() + 1));
            CockroachDBErrors.addExpressionErrors(errors);
            return new SQLQueryAdapter(sb.toString(), errors);
        }), //
        SCRUB((g) -> new SQLQueryAdapter(
                "EXPERIMENTAL SCRUB table " + g.getSchema().getRandomTable(t -> !t.isView()).getName(),
                // https://github.com/cockroachdb/cockroach/issues/46401
                ExpectedErrors.from("scrub-fk: column \"t.rowid\" does not exist",
                        "check-constraint: cannot access temporary tables of other sessions" /*
                                                                                              * https:// github. com/
                                                                                              * cockroachdb / cockroach
                                                                                              * /issues/ 47031
                                                                                              */))), //
        SPLIT((g) -> {
            CockroachDBTable randomTable = g.getSchema().getRandomTable();
            return createCandidateSplit(g, randomTable);
        });

        private final SQLQueryProvider<CockroachDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<CockroachDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        public SQLQueryAdapter getQuery(CockroachDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    public static class CockroachDBGlobalState extends SQLGlobalState<CockroachDBOptions, CockroachDBSchema> {

        @Override
        protected CockroachDBSchema readSchema() throws SQLException {
            return CockroachDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    /**
     * A VP worker owns one database and can therefore safely move its JDBC session to another SQL gateway in the same
     * CockroachDB cluster. This is deliberately scoped to VP: normal SQLancer database creation continues to use the
     * command-line gateway.
     */
    public static void switchVerticalPartitioningGateway(CockroachDBGlobalState globalState, String host, int port)
            throws SQLException {
        String databaseName = globalState.getDatabaseName();
        SQLException lastFailure = null;
        for (int attempt = 0; attempt < VP_TRANSIENT_CLUSTER_RETRIES; attempt++) {
            Connection replacementConnection = null;
            try {
                verifyVerticalPartitioningGatewayCluster(globalState, host, port);
                replacementConnection = DriverManager.getConnection(
                        String.format("jdbc:postgresql://%s:%d/%s", host, port, databaseName),
                        globalState.getOptions().getUserName(), globalState.getOptions().getPassword());
                try (Statement statement = replacementConnection.createStatement()) {
                    if (globalState.getDbmsSpecificOptions().testHashIndexes) {
                        statement.execute("SET experimental_enable_hash_sharded_indexes = 'on'");
                    }
                    // Session settings do not follow a JDBC connection to another SQL gateway. VP uses
                    // TEMP_TABLE reconstruction independently of the general temp-table option.
                    statement.execute("SET experimental_enable_temp_tables = 'on'");
                    // Distributed profile probes crdb_internal.node_id() immediately after the
                    // switch. Reapply this on the replacement session; setting it on the old
                    // gateway is not sufficient.
                    statement.execute("SET allow_unsafe_internals = true");
                }
                SQLConnection oldConnection = globalState.getConnection();
                globalState.setConnection(new SQLConnection(replacementConnection));
                if (oldConnection != null) {
                    oldConnection.close();
                }
                return;
            } catch (SQLException e) {
                lastFailure = e;
                if (replacementConnection != null) {
                    replacementConnection.close();
                }
                if ((isCockroachTransientClusterError(getVpActionErrorText(e))
                        || isDatabaseMetadataNotYetVisible(e, databaseName))
                        && attempt + 1 < VP_TRANSIENT_CLUSTER_RETRIES) {
                    try {
                        waitForTransientClusterRecovery(attempt);
                    } catch (IgnoreMeException interrupted) {
                        SQLException interruptedFailure = new SQLException(
                                "Interrupted while switching CockroachDB VP gateway", interrupted);
                        interruptedFailure.addSuppressed(e);
                        throw interruptedFailure;
                    }
                    continue;
                }
                throw e;
            }
        }
        throw lastFailure;
    }

    private static void verifyVerticalPartitioningGatewayCluster(CockroachDBGlobalState globalState, String host,
            int port) throws SQLException {
        String currentClusterId = getCockroachClusterId(globalState.getConnection().getJdbcConnection());
        try (Connection candidate = DriverManager.getConnection(
                String.format("jdbc:postgresql://%s:%d/defaultdb", host, port), globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword())) {
            String candidateClusterId = getCockroachClusterId(candidate);
            if (!currentClusterId.equals(candidateClusterId)) {
                throw new SQLException(
                        "configured VP gateway " + host + ":" + port + " belongs to CockroachDB cluster "
                                + candidateClusterId + ", expected " + currentClusterId,
                        VP_GATEWAY_CLUSTER_MISMATCH_SQL_STATE);
            }
        }
    }

    private static String getCockroachClusterId(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET allow_unsafe_internals = true");
            try (ResultSet result = statement.executeQuery("SELECT crdb_internal.cluster_id()")) {
                if (!result.next()) {
                    throw new SQLException("crdb_internal.cluster_id() returned no row");
                }
                return result.getString(1);
            }
        }
    }

    private static boolean isDatabaseMetadataNotYetVisible(SQLException error, String databaseName) {
        String message = getVpActionErrorText(error).toLowerCase(java.util.Locale.ROOT);
        return message.contains("database \"" + databaseName.toLowerCase(java.util.Locale.ROOT) + "\" does not exist");
    }

    /**
     * CockroachDB schema changes create asynchronous jobs. VP deliberately exercises many DDL paths, but an unlicensed
     * cluster has a low global concurrent-transaction limit. Serialize only VP schema changes and wait for the global
     * job backlog to drain; DML and query pairs remain fully parallel.
     */
    public static void acquireVerticalPartitioningSchemaChangePermit(CockroachDBGlobalState globalState, String sql)
            throws Exception {
        if (!isVerticalPartitioningSchemaChange(sql)) {
            return;
        }
        try {
            int transientFailures = 0;
            int backpressureAttempts = 0;
            while (backpressureAttempts++ < VP_SCHEMA_JOB_BACKPRESSURE_RETRIES) {
                try {
                    if (!hasVerticalPartitioningSchemaCapacity(globalState)) {
                        waitForVerticalPartitioningSchemaCapacity();
                        continue;
                    }
                    if (!VP_SCHEMA_CHANGE_LOCK.tryLock(500L, TimeUnit.MILLISECONDS)) {
                        continue;
                    }
                    boolean permitGranted = false;
                    try {
                        if (hasVerticalPartitioningSchemaCapacity(globalState)) {
                            permitGranted = true;
                            return;
                        }
                    } finally {
                        if (!permitGranted) {
                            VP_SCHEMA_CHANGE_LOCK.unlock();
                        }
                    }
                    waitForVerticalPartitioningSchemaCapacity();
                } catch (SQLException e) {
                    String errorText = getVpActionErrorText(e);
                    if (!isCockroachTransientClusterError(errorText)) {
                        throw e;
                    }
                    transientFailures++;
                    if (transientFailures >= VP_TRANSIENT_CLUSTER_RETRIES) {
                        CockroachDBVerticalPartitioningOracle.recordSkippedError(
                                "cluster-transient-schema-capacity", sql, errorText);
                        throw new IgnoreMeException();
                    }
                    waitForTransientClusterRecovery(transientFailures - 1);
                }
            }
            CockroachDBVerticalPartitioningOracle.recordSkippedError("schema-backpressure", sql,
                    "CockroachDB schema job backlog did not fall below " + VP_MAX_CONCURRENT_SCHEMA_JOBS);
            throw new IgnoreMeException();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IgnoreMeException();
        }
    }

    public static void releaseVerticalPartitioningSchemaChangePermit(String sql) {
        if (VP_SCHEMA_CHANGE_LOCK.isHeldByCurrentThread()) {
            VP_SCHEMA_CHANGE_LOCK.unlock();
        }
    }

    /**
     * A complete VP run creates several historical reads and may overlap with schema jobs. Bound those runs
     * independently of SQLancer's worker count so exhausted CockroachDB transaction slots recover through backpressure
     * rather than turning into skipped comparisons.
     */
    public static void acquireVerticalPartitioningOracleRunPermit() throws IgnoreMeException {
        try {
            VP_ORACLE_RUN_PERMITS.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IgnoreMeException();
        }
    }

    public static void releaseVerticalPartitioningOracleRunPermit() {
        VP_ORACLE_RUN_PERMITS.release();
    }

    private static boolean isVerticalPartitioningSchemaChange(String sql) {
        String normalized = sql.stripLeading().toUpperCase(java.util.Locale.ROOT);
        return normalized.startsWith("CREATE ") || normalized.startsWith("ALTER ") || normalized.startsWith("DROP ")
                || normalized.startsWith("TRUNCATE ");
    }

    private static boolean hasVerticalPartitioningSchemaCapacity(CockroachDBGlobalState globalState)
            throws SQLException {
        final String countSql = "SELECT count(*) FROM [SHOW JOBS] WHERE status IN ('running', 'pending') "
                + "AND job_type IN ('SCHEMA CHANGE', 'NEW SCHEMA CHANGE')";
        try (Statement statement = globalState.getConnection().createStatement();
                ResultSet result = statement.executeQuery(countSql)) {
            result.next();
            return result.getLong(1) < VP_MAX_CONCURRENT_SCHEMA_JOBS;
        } catch (SQLException e) {
            if (isCockroachResourceLimitError(getVpActionErrorText(e))) {
                return false;
            }
            throw e;
        }
    }

    private static void waitForVerticalPartitioningSchemaCapacity() throws InterruptedException {
        Thread.sleep(500L);
    }

    @Override
    public void generateDatabase(CockroachDBGlobalState globalState) throws Exception {
        QueryManager<SQLConnection> manager = globalState.getManager();
        MainOptions options = globalState.getOptions();
        boolean usesVerticalPartitioningOracle = usesVerticalPartitioningOracle(globalState);
        if (globalState.getDbmsSpecificOptions().oracle != CockroachDBOracleFactory.ECST) {
            manager.execute(new SQLQueryAdapter("--Don't send automatic bug reports"));
            executeOptionalClusterSetting(globalState, "debug.panic_on_failed_assertions", "true");
            executeOptionalClusterSetting(globalState, "diagnostics.reporting.enabled", "false");
            executeOptionalClusterSetting(globalState, "diagnostics.reporting.send_crash_reports.enabled", "false");
            manager.execute(
                    new SQLQueryAdapter("-- Disable the collection of metrics and hope that it helps performance"));
            executeOptionalClusterSetting(globalState, "sql.metrics.statement_details.enabled", "false");
            executeOptionalClusterSetting(globalState, "sql.metrics.statement_details.dump_to_logs.enabled", "false");
            executeOptionalClusterSetting(globalState, "sql.metrics.statement_details.gateway_node.enabled", "false");
            executeOptionalClusterSetting(globalState,
                    "sql.metrics.statement_details.index_recommendation_collection.enabled", "false");
            executeOptionalClusterSetting(globalState, "sql.stats.automatic_collection.enabled", "false");
            executeOptionalClusterSetting(globalState, "timeseries.storage.enabled", "false");
        }

        if (globalState.getDbmsSpecificOptions().testHashIndexes) {
            executeOptionalSessionSetting(globalState, "experimental_enable_hash_sharded_indexes", "'on'");
        }
        if (globalState.getDbmsSpecificOptions().testTempTables || usesVerticalPartitioningOracle) {
            executeOptionalSessionSetting(globalState, "experimental_enable_temp_tables", "'on'");
        }

        if (usesVerticalPartitioningOracle) {
            createVerticalPartitioningTableBatch(globalState);
        } else {
            for (int i = 0; i < Randomly.fromOptions(2, 3); i++) {
                boolean success = false;
                do {
                    try {
                        SQLQueryAdapter q = CockroachDBTableGenerator.generate(globalState);
                        success = globalState.executeStatement(q);
                    } catch (IgnoreMeException e) {
                        // continue trying
                    }
                } while (!success);
            }
        }

        if (!usesVerticalPartitioningOracle) {
            executeRandomActions(globalState, options);
        }

        if (usesVerticalPartitioningOracle) {
            CockroachDBVerticalPartitioningOracle.logSchemaShapeStats(globalState, "post-generate");
        }

        if (globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch((o) -> o == CockroachDBOracleFactory.CERT)) {
            // Enfore statistic collected for all tables
            ExpectedErrors errors = new ExpectedErrors();
            CockroachDBErrors.addExpressionErrors(errors);
            for (CockroachDBTable table : globalState.getSchema().getDatabaseTables()) {
                globalState.executeStatement(new SQLQueryAdapter("ANALYZE " + table.getName() + ";", errors));
            }
        }
    }

    private void executeOptionalClusterSetting(CockroachDBGlobalState globalState, String setting, String value)
            throws Exception {
        if (!clusterSettingExists(globalState, setting)) {
            return;
        }
        executeOptionalSetting(globalState, "SET CLUSTER SETTING " + setting + " = " + value);
    }

    private boolean clusterSettingExists(CockroachDBGlobalState globalState, String setting) {
        String sql = "SHOW CLUSTER SETTING " + setting;
        for (int attempt = 0; attempt < VP_TRANSIENT_CLUSTER_RETRIES; attempt++) {
            try (Statement s = globalState.getConnection().createStatement(); ResultSet ignored = s.executeQuery(sql)) {
                return true;
            } catch (SQLException e) {
                if (isCockroachTransientClusterError(getVpActionErrorText(e))
                        && attempt + 1 < VP_TRANSIENT_CLUSTER_RETRIES) {
                    try {
                        waitForTransientClusterRecovery(attempt);
                    } catch (IgnoreMeException ignored) {
                        return false;
                    }
                    continue;
                }
                return false;
            }
        }
        return false;
    }

    private void executeOptionalSessionSetting(CockroachDBGlobalState globalState, String setting, String value)
            throws Exception {
        if (!sessionSettingExists(globalState, setting)) {
            return;
        }
        executeOptionalSetting(globalState, "SET " + setting + " = " + value);
    }

    private boolean sessionSettingExists(CockroachDBGlobalState globalState, String setting) {
        String sql = "SHOW " + setting;
        for (int attempt = 0; attempt < VP_TRANSIENT_CLUSTER_RETRIES; attempt++) {
            try (Statement s = globalState.getConnection().createStatement(); ResultSet ignored = s.executeQuery(sql)) {
                return true;
            } catch (SQLException e) {
                if (isCockroachTransientClusterError(getVpActionErrorText(e))
                        && attempt + 1 < VP_TRANSIENT_CLUSTER_RETRIES) {
                    try {
                        waitForTransientClusterRecovery(attempt);
                    } catch (IgnoreMeException ignored) {
                        return false;
                    }
                    continue;
                }
                return false;
            }
        }
        return false;
    }

    private void executeOptionalSetting(CockroachDBGlobalState globalState, String sql) throws Exception {
        globalState.getState().logStatement(sql);
        for (int attempt = 0; attempt < VP_TRANSIENT_CLUSTER_RETRIES; attempt++) {
            try (Statement s = globalState.getConnection().createStatement()) {
                s.execute(sql);
                return;
            } catch (SQLException e) {
                String errorText = getVpActionErrorText(e);
                if (isCockroachTransientClusterError(errorText) && attempt + 1 < VP_TRANSIENT_CLUSTER_RETRIES) {
                    waitForTransientClusterRecovery(attempt);
                    continue;
                }
                CockroachDBVerticalPartitioningOracle.recordSkippedError("setup", sql, errorText);
                return;
            }
        }
    }

    private void executeRandomActions(CockroachDBGlobalState globalState, MainOptions options) throws Exception {
        int[] nrRemaining = new int[Action.values().length];
        int total = 0;
        for (int i = 0; i < Action.values().length; i++) {
            Action action = Action.values()[i];
            int nrPerformed = mapActions(globalState, options, action, false);
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
            SQLQueryAdapter query = null;
            try {
                boolean success;
                int nrTries = 0;
                do {
                    query = nextAction.getQuery(globalState);
                    success = globalState.executeStatement(query);
                } while (!success && nrTries++ < 1000);
            } catch (IgnoreMeException e) {

            }
            if (query != null && query.couldAffectSchema() && globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
            total--;
        }
    }

    private int mapActions(CockroachDBGlobalState globalState, MainOptions options, Action action,
            boolean usesVerticalPartitioningOracle) {
        boolean preserveVpRows = usesVerticalPartitioningOracle
                && !globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes;
        switch (action) {
        case INSERT:
            return globalState.getRandomly().getInteger(0, options.getMaxNumberInserts());
        case UPDATE:
        case SPLIT:
            return globalState.getRandomly().getInteger(0, 3);
        case EXPLAIN:
            return globalState.getRandomly().getInteger(0, 10);
        case SHOW:
        case CREATE_STATISTICS:
            return globalState.getRandomly().getInteger(0, 2);
        case TRUNCATE:
        case DELETE:
        case DROP_TABLE:
            return preserveVpRows ? 0 : globalState.getRandomly().getInteger(0, 2);
        case CREATE_VIEW:
            return globalState.getRandomly().getInteger(0, 2);
        case SET_SESSION:
        case SET_CLUSTER_SETTING:
            return globalState.getRandomly().getInteger(0, 3);
        case CREATE_INDEX:
            return globalState.getRandomly().getInteger(0, 10);
        case DROP_VIEW:
            return usesVerticalPartitioningOracle ? 0 : 0;
        case COMMENT_ON:
        case SCRUB:
        case TRANSACTION:
        case CREATE_TABLE:
            return 0;
        default:
            throw new AssertionError(action);
        }
    }

    private boolean usesVerticalPartitioningOracle(CockroachDBGlobalState globalState) {
        return usesVerticalPartitioningOracle(globalState.getDbmsSpecificOptions());
    }

    private static boolean usesVerticalPartitioningOracle(CockroachDBOptions options) {
        return options.getTestOracleFactory().stream().anyMatch(
                o -> o == CockroachDBOracleFactory.VERTICAL_PARTITIONING || o == CockroachDBOracleFactory.ECST);
    }

    public static void replaceVerticalPartitioningTableAfterSuccessfulCheck(CockroachDBGlobalState globalState,
            String oldTableName) throws Exception {
        CockroachDBTable replacement = createValidVerticalPartitioningBaseTable(globalState, false);
        if (replacement == null) {
            logVerticalPartitioningReplacementFailure(globalState, oldTableName);
            throw new IgnoreMeException();
        }
        String dropOldBase = "DROP TABLE IF EXISTS " + quoteIdentifier(oldTableName) + " CASCADE";
        try {
            executeVpActionStatement(globalState,
                    new SQLQueryAdapter(dropOldBase, commonCandidateMutationErrors(), true));
            updateSchemaOrIgnore(globalState);
        } catch (AssertionError e) {
            String errorText = getVpActionErrorText(e);
            if (isCockroachSchemaChangerOperationalError(errorText)) {
                CockroachDBVerticalPartitioningOracle.recordSkippedError("setup", dropOldBase, errorText);
                try {
                    updateSchemaOrIgnore(globalState);
                } catch (IgnoreMeException ignored) {
                }
                return;
            }
            throw e;
        }
    }

    public static void ensureVerticalPartitioningBaseTables(CockroachDBGlobalState globalState, int minBaseTables)
            throws Exception {
        updateSchemaOrIgnore(globalState);
        while (countVerticalPartitioningBaseTables(globalState) < minBaseTables) {
            CockroachDBTable table = createValidVerticalPartitioningBaseTable(globalState, true);
            if (table == null) {
                throw new IgnoreMeException();
            }
            updateSchemaOrIgnore(globalState);
        }
    }

    private static void createVerticalPartitioningTableBatch(CockroachDBGlobalState globalState) throws Exception {
        for (int i = 0; i < VP_BASE_TABLES_PER_BATCH; i++) {
            CockroachDBTable table = createValidVerticalPartitioningBaseTable(globalState, true);
            if (table == null) {
                throw new IgnoreMeException();
            }
            updateSchemaOrIgnore(globalState);
        }
        updateSchemaOrIgnore(globalState);
    }

    private static CockroachDBTable createValidVerticalPartitioningBaseTable(CockroachDBGlobalState globalState,
            boolean bootstrapCandidate) throws Exception {
        ExpectedErrors createTableErrors = commonCandidateMutationErrors();
        for (int attempt = 0; attempt < VP_REPLACEMENT_RETRY_COUNT; attempt++) {
            updateSchemaOrIgnore(globalState);
            List<String> before = globalState.getSchema().getDatabaseTables().stream().map(CockroachDBTable::getName)
                    .collect(Collectors.toList());
            boolean directed = globalState.getDbmsSpecificOptions().verticalPartitioningDeepExploration
                    && ((bootstrapCandidate && before.isEmpty()) || globalState.getRandomly().getInteger(0, 3) == 0);
            SQLQueryAdapter createTable = directed ? createDirectedVerticalPartitioningTable(globalState)
                    : CockroachDBTableGenerator.generate(globalState);
            boolean success;
            try {
                success = executeVpActionStatement(globalState, createTable);
            } catch (IgnoreMeException ignored) {
                continue;
            } catch (AssertionError e) {
                if (isAlreadyExistsError(e)) {
                    continue;
                }
                String errorText = getVpActionErrorText(e);
                if (createTableErrors.errorIsExpected(errorText)) {
                    CockroachDBVerticalPartitioningOracle.recordSkippedError("setup", createTable.getQueryString(),
                            errorText);
                    continue;
                }
                throw e;
            }
            updateSchemaOrIgnore(globalState);
            if (!success) {
                continue;
            }
            CockroachDBTable table = globalState.getSchema().getDatabaseTables().stream()
                    .filter(t -> !before.contains(t.getName())).filter(t -> isPotentialVerticalPartitioningBaseTable(t))
                    .findFirst().orElse(null);
            if (table == null) {
                continue;
            }
            boolean keepEmpty = globalState.getRandomly().getInteger(0, 100) < VP_EMPTY_BASE_TABLE_PERCENT;
            if (!keepEmpty && directed) {
                seedDirectedVerticalPartitioningTable(globalState, table);
            }
            if (!keepEmpty && !seedNonEmptyVerticalPartitioningCandidate(globalState, table)) {
                executeVpActionStatement(globalState, new SQLQueryAdapter(
                        "DROP TABLE IF EXISTS " + quoteIdentifier(table.getName()) + " CASCADE", true));
                updateSchemaOrIgnore(globalState);
                continue;
            }
            // Preserve the requested empty-table population. Empty candidates still receive the
            // non-row-producing mutations below, but INSERT must not silently fill them before VP
            // selects the base-table pool.
            mutateVerticalPartitioningCandidate(globalState, table, !keepEmpty, bootstrapCandidate);
            updateSchemaOrIgnore(globalState);
            table = findVerticalPartitioningBaseTable(globalState, table.getName());
            if (table != null && isValidVerticalPartitioningBaseTable(globalState, table)) {
                return table;
            }
            executeVpActionStatement(globalState,
                    new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(table.getName()) + " CASCADE", true));
            updateSchemaOrIgnore(globalState);
        }
        return null;
    }

    private static boolean isAlreadyExistsError(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains("already exists")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static SQLQueryAdapter createDirectedVerticalPartitioningTable(CockroachDBGlobalState globalState) {
        String table = globalState.getSchema().getFreeTableName();
        String enumType = quoteIdentifier(table + "_vp_enum_" + VP_VIEW_COUNTER.incrementAndGet());
        String sql = "CREATE TYPE " + enumType + " AS ENUM ('z_first', 'a_middle', 'm_last'); CREATE TABLE "
                + quoteIdentifier(table) + " (c0 INT8, c1 JSONB, c2 INT8[], c3 STRING, c4 " + enumType
                + ", c5 INT8 AS (c0 % 1000) STORED)";
        return new SQLQueryAdapter(sql, commonCandidateMutationErrors(), true);
    }

    private static void seedDirectedVerticalPartitioningTable(CockroachDBGlobalState globalState,
            CockroachDBTable table) throws Exception {
        String enumType = table.getColumns().stream().filter(c -> c.getName().equals("c4")).findFirst().get()
                .getType().toString();
        String sql = "INSERT INTO " + quoteIdentifier(table.getName()) + " (c0, c1, c2, c3, c4) SELECT i, "
                + "CASE i % 4 WHEN 0 THEN NULL WHEN 1 THEN '{\"vp\":1}'::JSONB "
                + "WHEN 2 THEN '{\"k\":null}'::JSONB ELSE '[1,2]'::JSONB END, "
                + "CASE i % 4 WHEN 0 THEN NULL WHEN 1 THEN ARRAY[]::INT8[] "
                + "WHEN 2 THEN ARRAY[1,NULL]::INT8[] ELSE ARRAY[0,1,1]::INT8[] END, "
                + "CASE i % 4 WHEN 0 THEN NULL WHEN 1 THEN 'Aa' WHEN 2 THEN 'a' ELSE 'b' END, "
                + "(CASE i % 4 WHEN 0 THEN NULL WHEN 1 THEN 'z_first' WHEN 2 THEN 'a_middle' ELSE 'm_last' END)::"
                + enumType + " FROM generate_series(-4, 7) AS g(i)";
        executeVpActionStatement(globalState, new SQLQueryAdapter(sql, commonCandidateMutationErrors()));
    }

    private static void mutateVerticalPartitioningCandidate(CockroachDBGlobalState globalState, CockroachDBTable table,
            boolean allowInsert, boolean bootstrapCandidate) throws Exception {
        int actions = bootstrapCandidate ? VP_BOOTSTRAP_CANDIDATE_MUTATIONS
                : Math.max(1, globalState.getOptions().getMaxNumberInserts())
                        + globalState.getRandomly().getInteger(1, 11);
        for (int i = 0; i < actions; i++) {
            updateSchemaOrIgnore(globalState);
            table = findVerticalPartitioningBaseTable(globalState, table.getName());
            if (table == null) {
                return;
            }
            try {
                SQLQueryAdapter query = createVerticalPartitioningCandidateMutation(globalState, table, allowInsert);
                executeVpActionStatement(globalState, query);
            } catch (IgnoreMeException ignored) {
            }
        }
    }

    private static boolean seedNonEmptyVerticalPartitioningCandidate(CockroachDBGlobalState globalState,
            CockroachDBTable table) throws Exception {
        for (int attempt = 0; attempt < VP_NON_EMPTY_INSERT_ATTEMPTS
                && getVerticalPartitioningRowCount(globalState, table.getName()) < VP_NON_EMPTY_MIN_ROWS; attempt++) {
            try {
                executeVpActionStatement(globalState, CockroachDBInsertGenerator.insert(globalState, table));
            } catch (IgnoreMeException ignored) {
                // Expected insert failures are retried with a newly generated row batch.
            }
        }
        return getVerticalPartitioningRowCount(globalState, table.getName()) >= VP_NON_EMPTY_MIN_ROWS;
    }

    private static long getVerticalPartitioningRowCount(CockroachDBGlobalState globalState, String tableName)
            throws Exception {
        String sql = "SELECT COUNT(*) FROM " + quoteIdentifier(tableName);
        SQLException lastTransientFailure = null;
        for (int attempt = 0; attempt < VP_TRANSIENT_CLUSTER_RETRIES; attempt++) {
            try (Statement s = globalState.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
                if (!rs.next()) {
                    throw new AssertionError("COUNT(*) returned no row for " + tableName);
                }
                return rs.getLong(1);
            } catch (SQLException e) {
                String msg = getVpActionErrorText(e);
                if (!isCockroachTransientClusterError(msg)) {
                    throw e;
                }
                lastTransientFailure = e;
                if (attempt + 1 < VP_TRANSIENT_CLUSTER_RETRIES) {
                    waitForTransientClusterRecovery(attempt);
                }
            }
        }
        CockroachDBVerticalPartitioningOracle.recordSkippedError("cluster-transient-setup", sql,
                getVpActionErrorText(lastTransientFailure));
        throw new IgnoreMeException();
    }

    private static boolean executeVpActionStatement(CockroachDBGlobalState globalState, SQLQueryAdapter query)
            throws Exception {
        ExecutionTimer timer = null;
        boolean logExecutionTime = globalState.getOptions().logExecutionTime();
        if (logExecutionTime) {
            timer = new ExecutionTimer().start();
        }
        if (globalState.getOptions().printAllStatements()) {
            System.out.println(query.getLogString());
        }
        if (globalState.getOptions().logEachSelect()) {
            if (logExecutionTime) {
                globalState.getLogger().writeCurrentNoLineBreak(query.getLogString());
            } else {
                globalState.getLogger().writeCurrent(query.getLogString());
            }
        }
        boolean success = false;
        boolean handled = false;
        boolean schemaPermitAcquired = false;
        try {
            acquireVerticalPartitioningSchemaChangePermit(globalState, query.getQueryString());
            schemaPermitAcquired = isVerticalPartitioningSchemaChange(query.getQueryString());
            for (int attempt = 0; attempt < VP_TRANSIENT_CLUSTER_RETRIES; attempt++) {
                try (Statement s = globalState.getConnection().createStatement()) {
                    s.setQueryTimeout(VP_ACTION_QUERY_TIMEOUT_SECONDS);
                    s.execute(query.getQueryString());
                    success = true;
                    handled = true;
                    return true;
                } catch (SQLException e) {
                    String errorText = getVpActionErrorText(e);
                    if (isCockroachActionTimeout(errorText)) {
                        CockroachDBVerticalPartitioningOracle.recordSkippedError("action", query.getQueryString(),
                                errorText);
                        handled = true;
                        return false;
                    }
                    if (isCockroachTransientClusterError(errorText) && attempt + 1 < VP_TRANSIENT_CLUSTER_RETRIES) {
                        waitForTransientClusterRecovery(attempt);
                        continue;
                    }
                    if (query.getExpectedErrors().errorIsExpected(errorText)
                            || isCockroachTransientClusterError(errorText)) {
                        CockroachDBVerticalPartitioningOracle.recordSkippedError("action", query.getQueryString(),
                                errorText);
                        handled = true;
                        return false;
                    }
                    Main.nrUnsuccessfulActions.addAndGet(1);
                    throw new AssertionError(query.getQueryString(), e);
                }
            }
            throw new AssertionError("CockroachDB VP action retry loop exhausted");
        } finally {
            if (schemaPermitAcquired) {
                releaseVerticalPartitioningSchemaChangePermit(query.getQueryString());
            }
            if (handled) {
                if (success) {
                    Main.nrSuccessfulActions.addAndGet(1);
                } else {
                    Main.nrUnsuccessfulActions.addAndGet(1);
                }
                if (success && globalState.getOptions().printSucceedingStatements()) {
                    System.out.println(query.getQueryString());
                }
                if (logExecutionTime) {
                    globalState.getLogger().writeCurrent(" -- " + timer.end().asString());
                }
                if (success && query.couldAffectSchema()) {
                    updateSchemaOrIgnore(globalState);
                }
            }
        }
    }

    private static String getVpActionErrorText(Throwable throwable) {
        StringBuilder sb = new StringBuilder();
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null) {
                sb.append(current.getMessage()).append('\n');
            }
            current = current.getCause();
        }
        return sb.toString();
    }

    private static SQLQueryAdapter createVerticalPartitioningCandidateMutation(CockroachDBGlobalState globalState,
            CockroachDBTable table, boolean allowInsert) throws Exception {
        List<Function<CockroachDBTable, SQLQueryAdapter>> actions = new ArrayList<>();
        if (allowInsert) {
            actions.add(t -> CockroachDBInsertGenerator.insert(globalState, t));
        }
        actions.add(t -> createCandidateUpdate(globalState, t));
        actions.add(t -> createCandidateIndex(globalState, t));
        actions.add(t -> createCandidateStatistics(globalState, t));
        actions.add(t -> createCandidateSplit(globalState, t));
        actions.add(t -> createCandidateShow(t));
        actions.add(t -> createCandidateExplain(globalState, t));
        actions.add(t -> createCandidateView(globalState, t));
        actions.add(t -> {
            try {
                return CockroachDBSetSessionGenerator.create(globalState);
            } catch (Exception e) {
                throw new IgnoreMeException();
            }
        });
        if (globalState.getDbmsSpecificOptions().oracle != CockroachDBOracleFactory.ECST) {
            actions.add(t -> {
                try {
                    return CockroachDBSetClusterSettingGenerator.create(globalState);
                } catch (Exception e) {
                    throw new IgnoreMeException();
                }
            });
        }
        if (globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes) {
            actions.add(t -> createCandidateDelete(globalState, t));
            actions.add(CockroachDBProvider::createCandidateTruncate);
        }
        return Randomly.fromList(actions).apply(table);
    }

    private static SQLQueryAdapter createVerticalPartitioningCandidateMutation(CockroachDBGlobalState globalState,
            CockroachDBTable table, Action action) throws Exception {
        switch (action) {
        case INSERT:
            return CockroachDBInsertGenerator.insert(globalState, table);
        case UPDATE:
            return createCandidateUpdate(globalState, table);
        case CREATE_INDEX:
            return createCandidateIndex(globalState, table);
        case CREATE_STATISTICS:
            return createCandidateStatistics(globalState, table);
        case SPLIT:
            return createCandidateSplit(globalState, table);
        case SHOW:
            return createCandidateShow(table);
        case EXPLAIN:
            return createCandidateExplain(globalState, table);
        case CREATE_VIEW:
            return createCandidateView(globalState, table);
        case SET_SESSION:
            return CockroachDBSetSessionGenerator.create(globalState);
        case SET_CLUSTER_SETTING:
            if (globalState.getDbmsSpecificOptions().oracle == CockroachDBOracleFactory.ECST) {
                throw new IgnoreMeException();
            }
            return CockroachDBSetClusterSettingGenerator.create(globalState);
        case DELETE:
            if (!globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes) {
                throw new IgnoreMeException();
            }
            return createCandidateDelete(globalState, table);
        case TRUNCATE:
            if (!globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes) {
                throw new IgnoreMeException();
            }
            return createCandidateTruncate(table);
        case CREATE_TABLE:
        case DROP_TABLE:
        case DROP_VIEW:
        case COMMENT_ON:
        case SCRUB:
        case TRANSACTION:
            throw new IgnoreMeException();
        default:
            throw new AssertionError(action);
        }
    }

    private static SQLQueryAdapter createCandidateUpdate(CockroachDBGlobalState globalState, CockroachDBTable table) {
        ExpectedErrors errors = commonCandidateMutationErrors();
        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState)
                .setColumns(table.getColumns());
        List<CockroachDBColumn> writableColumns = table.getColumns().stream().filter(c -> !c.isComputed())
                .collect(Collectors.toList());
        if (writableColumns.isEmpty()) {
            throw new IgnoreMeException();
        }
        List<CockroachDBColumn> columns = Randomly.nonEmptySubset(writableColumns);
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(quoteIdentifier(table.getName()));
        appendCandidateForceIndex(sb, table);
        sb.append(" SET ");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            CockroachDBColumn column = columns.get(i);
            sb.append(quoteIdentifier(column.getName()));
            sb.append("=");
            if (shouldUseVpBoundaryLiteral(globalState)) {
                CockroachDBVerticalPartitioningOracle
                        .recordAdvancedFeature(CockroachDBVerticalPartitioningOracle.AdvancedFeature.BOUNDARY_MUTATION);
                sb.append(generateCandidateBoundaryLiteral(column));
            } else if (Randomly.getBooleanWithRatherLowProbability()) {
                sb.append("DEFAULT");
            } else {
                sb.append(CockroachDBVisitor.asString(gen.generateExpression(column.getType())));
            }
        }
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(CockroachDBVisitor.asString(gen.generateExpression(CockroachDBDataType.BOOL.get())));
        } else {
            errors.add("UPDATE without WHERE clause (sql_safe_updates = true)");
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static void appendCandidateForceIndex(StringBuilder sb, CockroachDBTable table) {
        List<sqlancer.common.schema.TableIndex> forceableIndexes = table.getNonInvertedIndexes();
        if (!forceableIndexes.isEmpty() && Randomly.getBoolean()) {
            sb.append("@{FORCE_INDEX=");
            sb.append(Randomly.fromList(forceableIndexes).getIndexName());
            sb.append("}");
        }
    }

    private static SQLQueryAdapter createCandidateIndex(CockroachDBGlobalState globalState, CockroachDBTable table) {
        if (globalState.getSchema().getIndexCount() >= globalState.getDbmsSpecificOptions().maxNumIndexes) {
            throw new IgnoreMeException();
        }
        ExpectedErrors errors = commonCandidateMutationErrors();
        errors.add("is part of the primary index and therefore implicit in all indexes");
        errors.add("already contains column");
        errors.add("and thus is not indexable");
        errors.add("the following columns are not indexable due to their type");
        errors.add("cannot determine type of empty array. Consider annotating with the desired type");
        errors.add("incompatible IF expression");
        errors.add("overflow during Encode");
        errors.add("of type interval");
        errors.add("null value in column");
        errors.add("cannot create a sharded index on a computed column");

        List<CockroachDBColumn> invertedColumns = table.getColumns().stream()
                .filter(CockroachDBProvider::isCandidateInvertedIndexColumn)
                .collect(Collectors.toCollection(ArrayList::new));
        List<CockroachDBColumn> ordinaryIndexableColumns = table.getColumns().stream()
                .filter(CockroachDBProvider::isCandidateOrdinaryIndexColumn)
                .collect(Collectors.toCollection(ArrayList::new));
        if (!invertedColumns.isEmpty()
                && (ordinaryIndexableColumns.isEmpty() || Randomly.getBooleanWithSmallProbability())) {
            CockroachDBColumn column = Randomly.fromList(invertedColumns);
            String sql = "CREATE INVERTED INDEX " + quoteIdentifier(globalState.getSchema().getFreeIndexName()) + " ON "
                    + quoteIdentifier(table.getName()) + " (" + quoteIdentifier(column.getName()) + ")";
            errors.add("inverted indexes don't support");
            errors.add("is not supported with inverted indexes");
            errors.add("index column cannot have type");
            return new SQLQueryAdapter(sql, errors, true);
        }

        StringBuilder sb = new StringBuilder("CREATE ");
        if (ordinaryIndexableColumns.isEmpty()) {
            throw new IgnoreMeException();
        }
        if (Randomly.getBoolean()) {
            sb.append("UNIQUE ");
        }
        sb.append("INDEX ");
        sb.append(quoteIdentifier(globalState.getSchema().getFreeIndexName()));
        sb.append(" ON ");
        sb.append(quoteIdentifier(table.getName()));
        List<CockroachDBColumn> columns = Randomly.nonEmptySubset(ordinaryIndexableColumns);
        appendColumnList(sb, columns, true);
        boolean hashSharded = globalState.getDbmsSpecificOptions().testHashIndexes
                && Randomly.getBooleanWithSmallProbability();
        if (hashSharded) {
            sb.append(" USING HASH WITH BUCKET_COUNT=");
            sb.append(Randomly.getNotCachedInteger(2, 2048));
        }
        if (Randomly.getBoolean()) {
            List<CockroachDBColumn> storingColumns = new ArrayList<>(table.getRandomNonEmptyColumnSubset());
            storingColumns.removeAll(columns);
            if (!storingColumns.isEmpty()) {
                sb.append(" ");
                sb.append(Randomly.fromOptions("STORING", "COVERING"));
                sb.append(" ");
                appendColumnList(sb, storingColumns, false);
            }
        }
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private static boolean isCandidateInvertedIndexColumn(CockroachDBColumn column) {
        if (column.isPrimaryKey()) {
            // CockroachDB rejects inverted indexes over primary-key columns.
            return false;
        }
        switch (column.getType().getPrimitiveDataType()) {
        case JSONB:
        case ARRAY:
            return true;
        default:
            return false;
        }
    }

    private static boolean isCandidateOrdinaryIndexColumn(CockroachDBColumn column) {
        switch (column.getType().getPrimitiveDataType()) {
        case JSONB:
        case ARRAY:
            return false;
        default:
            return true;
        }
    }

    private static void appendColumnList(StringBuilder sb, List<CockroachDBColumn> columns, boolean allowOrdering) {
        sb.append("(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(quoteIdentifier(columns.get(i).getName()));
            if (allowOrdering && Randomly.getBoolean()) {
                sb.append(" ");
                sb.append(Randomly.fromOptions("ASC", "DESC"));
            }
        }
        sb.append(")");
    }

    private static SQLQueryAdapter createCandidateStatistics(CockroachDBGlobalState globalState,
            CockroachDBTable table) {
        ExpectedErrors errors = commonCandidateMutationErrors();
        errors.add("overflow during Encode");
        StringBuilder sb = new StringBuilder("CREATE STATISTICS ");
        sb.append(quoteIdentifier("vp_s" + Math.abs(globalState.getRandomly().getInteger())));
        if (Randomly.getBoolean()) {
            sb.append(" ON ");
            sb.append(quoteIdentifier(table.getRandomColumn().getName()));
        }
        sb.append(" FROM ");
        sb.append(quoteIdentifier(table.getName()));
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static SQLQueryAdapter createCandidateSplit(CockroachDBGlobalState globalState, CockroachDBTable table) {
        if (table.getIndexes().isEmpty()) {
            throw new IgnoreMeException();
        }
        ExpectedErrors errors = commonCandidateMutationErrors();
        errors.add("must be of type");
        errors.add("has no key columns");
        String indexName = Randomly.fromList(table.getIndexes()).getIndexName();
        String splitValues = splitValuesForIndex(globalState, table, indexName);
        if (splitValues == null) {
            throw new IgnoreMeException();
        }
        StringBuilder sb = new StringBuilder("ALTER INDEX ");
        sb.append(quoteIdentifier(table.getName()));
        sb.append("@");
        sb.append(quoteIdentifier(indexName));
        sb.append(" SPLIT AT VALUES ");
        sb.append(splitValues);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static String splitValuesForIndex(CockroachDBGlobalState globalState, CockroachDBTable table,
            String indexName) {
        CockroachDBColumn firstKeyColumn = getFirstIndexKeyColumn(globalState, table, indexName);
        if (firstKeyColumn == null) {
            return null;
        }
        switch (firstKeyColumn.getType().getPrimitiveDataType()) {
        case BOOL:
            return "(false), (true)";
        case INT:
        case SERIAL:
            return "(0), (1)";
        case FLOAT:
            return "(0.0), (1.0)";
        case DECIMAL:
            return "(0:::DECIMAL), (1:::DECIMAL)";
        case STRING:
            return "('a'), ('b')";
        case BYTES:
            return "(x'00'::BYTES), (x'01'::BYTES)";
        case BIT:
            return "(B'" + "0".repeat(firstKeyColumn.getType().getSize()) + "'), (B'"
                    + "0".repeat(firstKeyColumn.getType().getSize() - 1) + "1')";
        case VARBIT:
            return "(B'0'), (B'1')";
        case TIMESTAMP:
            return "(TIMESTAMP '1970-01-01 00:00:00'), (TIMESTAMP '1970-01-02 00:00:00')";
        case TIMESTAMPTZ:
            return "(TIMESTAMPTZ '1970-01-01 00:00:00+00'), (TIMESTAMPTZ '1970-01-02 00:00:00+00')";
        case TIME:
            return "(TIME '00:00:00'), (TIME '00:00:01')";
        case TIMETZ:
            return "(TIMETZ '00:00:00+00'), (TIMETZ '00:00:01+00')";
        case INTERVAL:
            return "(INTERVAL '0 seconds'), (INTERVAL '1 second')";
        case ARRAY:
        case JSONB:
        default:
            return null;
        }
    }

    private static CockroachDBColumn getFirstIndexKeyColumn(CockroachDBGlobalState globalState, CockroachDBTable table,
            String indexName) {
        try (Statement s = globalState.getConnection().createStatement();
                ResultSet rs = s.executeQuery("SHOW INDEX FROM " + quoteIdentifier(table.getName()))) {
            while (rs.next()) {
                if (!indexName.equals(rs.getString("index_name"))) {
                    continue;
                }
                String columnName = rs.getString("column_name");
                if (columnName == null) {
                    continue;
                }
                boolean storing = getBooleanColumnIfPresent(rs, "storing");
                boolean implicit = getBooleanColumnIfPresent(rs, "implicit");
                if (storing) {
                    continue;
                }
                if (columnName.contains("crdb_internal")) {
                    return new CockroachDBColumn(columnName, CockroachDBDataType.INT.get(), false, false);
                }
                if (implicit) {
                    continue;
                }
                for (CockroachDBColumn column : table.getColumns()) {
                    if (column.getName().equals(columnName)) {
                        return column;
                    }
                }
            }
        } catch (SQLException ignored) {
        }
        return null;
    }

    private static boolean getBooleanColumnIfPresent(ResultSet rs, String columnName) throws SQLException {
        try {
            return rs.getBoolean(columnName);
        } catch (SQLException ignored) {
            return false;
        }
    }

    private static SQLQueryAdapter createCandidateShow(CockroachDBTable table) {
        ExpectedErrors errors = commonCandidateMutationErrors();
        StringBuilder sb = new StringBuilder();
        switch (Randomly.fromOptions("FINGERPRINTS", "RANGES", "COLUMNS", "INDEXES")) {
        case "FINGERPRINTS":
            sb.append("SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE ");
            sb.append(quoteIdentifier(table.getName()));
            errors.add("bytea encoded value ends with incomplete escape sequence");
            errors.add("invalid bytea escape sequence");
            break;
        case "RANGES":
            sb.append("SHOW RANGES FROM TABLE ");
            sb.append(quoteIdentifier(table.getName()));
            break;
        case "COLUMNS":
            sb.append("SHOW COLUMNS FROM ");
            sb.append(quoteIdentifier(table.getName()));
            break;
        case "INDEXES":
            sb.append("SHOW INDEXES FROM ");
            sb.append(quoteIdentifier(table.getName()));
            break;
        default:
            throw new AssertionError();
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static SQLQueryAdapter createCandidateExplain(CockroachDBGlobalState globalState, CockroachDBTable table) {
        ExpectedErrors errors = commonCandidateMutationErrors();
        errors.add("cannot set EXPLAIN mode more than once");
        errors.add("unable to vectorize execution plan");
        errors.add("unsupported type");
        StringBuilder sb = new StringBuilder("EXPLAIN ");
        if (Randomly.getBoolean()) {
            sb.append("(");
            if (isVectorizeEnabled(globalState)) {
                sb.append(Randomly.fromOptions("VERBOSE", "TYPES", "OPT", "DISTSQL", "VEC"));
            } else {
                sb.append(Randomly.fromOptions("VERBOSE", "TYPES", "OPT", "DISTSQL"));
            }
            sb.append(") ");
        }
        sb.append(createCandidateSelect(globalState, table, Math.max(1, Randomly.smallNumber() + 1)));
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static boolean isVectorizeEnabled(CockroachDBGlobalState globalState) {
        String sql = "SELECT value FROM [SHOW SESSION VARIABLES] WHERE variable = 'vectorize'";
        try (Statement s = globalState.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() && !"off".equalsIgnoreCase(rs.getString(1));
        } catch (SQLException ignored) {
            // Avoid emitting EXPLAIN (VEC) without a confirmed vectorized session.
            return false;
        }
    }

    private static SQLQueryAdapter createCandidateView(CockroachDBGlobalState globalState, CockroachDBTable table) {
        // Schema objects can lag asynchronous CockroachDB jobs. Do not generate a view from a
        // cached column list until the live table metadata confirms every source column exists.
        if (!hasLiveTableColumns(globalState, table)) {
            throw new IgnoreMeException();
        }
        ExpectedErrors errors = commonCandidateMutationErrors();
        errors.add("value type unknown cannot be used for table columns");
        errors.add("already exists");
        // A schema change can commit after hasLiveTableColumns() and before CREATE VIEW is
        // planned. The generated view is then stale, rather than evidence of a database bug.
        errors.add("does not exist");
        int nrColumns = Math.max(1, Randomly.smallNumber() + 1);
        StringBuilder sb = new StringBuilder("CREATE VIEW ");
        sb.append(quoteIdentifier(freshVpViewName()));
        sb.append("(");
        for (int i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(quoteIdentifier("c" + i));
        }
        sb.append(") AS ");
        sb.append(createCandidateSelect(globalState, table, nrColumns));
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private static boolean hasLiveTableColumns(CockroachDBGlobalState globalState, CockroachDBTable table) {
        String sql = "SELECT * FROM " + quoteIdentifier(table.getName()) + " LIMIT 0";
        try (Statement statement = globalState.getConnection().createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            java.sql.ResultSetMetaData metadata = result.getMetaData();
            if (metadata.getColumnCount() != table.getColumns().size()) {
                return false;
            }
            for (int i = 0; i < table.getColumns().size(); i++) {
                if (!table.getColumns().get(i).getName().equals(metadata.getColumnName(i + 1))) {
                    return false;
                }
            }
            return true;
        } catch (SQLException ignored) {
            return false;
        }
    }

    private static String freshVpViewName() {
        return "vp_v" + VP_VIEW_COUNTER.incrementAndGet();
    }

    private static String createCandidateSelect(CockroachDBGlobalState globalState, CockroachDBTable table,
            int nrColumns) {
        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState)
                .setColumns(table.getColumns());
        boolean useAggregates = Randomly.getBooleanWithSmallProbability();
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            CockroachDBColumn column = Randomly.fromList(table.getColumns());
            if (useAggregates) {
                sb.append(CockroachDBVisitor.asString(gen.generateAggregate()));
            } else if (Randomly.getBoolean()) {
                sb.append(quoteIdentifier(column.getName()));
            } else {
                sb.append(CockroachDBVisitor.asString(gen.generateExpression(column.getType())));
            }
        }
        sb.append(" FROM ");
        sb.append(quoteIdentifier(table.getName()));
        if (!useAggregates && Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(CockroachDBVisitor.asString(gen.generateExpression(CockroachDBDataType.BOOL.get())));
        }
        if (!useAggregates && Randomly.getBoolean()) {
            sb.append(" ORDER BY ");
            List<CockroachDBColumn> orderColumns = table.getRandomNonEmptyColumnSubset();
            for (int i = 0; i < orderColumns.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(quoteIdentifier(table.getName()));
                sb.append(".");
                sb.append(quoteIdentifier(orderColumns.get(i).getName()));
                if (Randomly.getBoolean()) {
                    sb.append(" ");
                    sb.append(Randomly.fromOptions("ASC", "DESC"));
                }
            }
        }
        if (Randomly.getBoolean()) {
            sb.append(" LIMIT ");
            sb.append(Randomly.getNotCachedInteger(0, 25));
        }
        return sb.toString();
    }

    private static SQLQueryAdapter createCandidateDelete(CockroachDBGlobalState globalState, CockroachDBTable table) {
        ExpectedErrors errors = commonCandidateMutationErrors();
        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState)
                .setColumns(table.getColumns());
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        sb.append(quoteIdentifier(table.getName()));
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(CockroachDBVisitor.asString(gen.generateExpression(CockroachDBDataType.BOOL.get())));
        } else {
            errors.add("rejected: DELETE without WHERE clause (sql_safe_updates = true)");
        }
        errors.add("foreign key violation");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static SQLQueryAdapter createCandidateTruncate(CockroachDBTable table) {
        ExpectedErrors errors = commonCandidateMutationErrors();
        errors.add("is referenced by foreign key");
        errors.add("found in depended-on-by references, no such index in this relation");
        StringBuilder sb = new StringBuilder("TRUNCATE");
        if (Randomly.getBoolean()) {
            sb.append(" TABLE");
        }
        sb.append(" ");
        sb.append(quoteIdentifier(table.getName()));
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("CASCADE", "RESTRICT"));
        }
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static boolean shouldUseVpBoundaryLiteral(CockroachDBGlobalState globalState) {
        if (globalState.getDbmsSpecificOptions().verticalPartitioningDeepExploration) {
            return globalState.getRandomly().getInteger(0, 100) < 40;
        }
        return false;
    }

    private static String generateCandidateBoundaryLiteral(CockroachDBColumn column) {
        switch (column.getType().getPrimitiveDataType()) {
        case INT:
        case SERIAL:
            switch (column.getType().getSize()) {
            case 2:
                return Randomly.fromOptions("0", "-0", "-1", "1", "32767", "-32768");
            case 4:
                return Randomly.fromOptions("0", "-0", "-1", "1", "2147483647", "-2147483648");
            case 8:
            default:
                return Randomly.fromOptions("0", "-0", "-1", "1", "2147483647", "-2147483648", "9223372036854775807",
                        "-9223372036854775808");
            }
        case BOOL:
            return Randomly.fromOptions("true", "false", "NULL");
        case FLOAT:
            return Randomly.fromOptions("0", "-0.0", "1e308", "-1e308", "0.0000000000000001", "'NaN'::FLOAT8",
                    "'Infinity'::FLOAT8", "'-Infinity'::FLOAT8");
        case DECIMAL:
            return Randomly.fromOptions("0", "-1", "99999999999999999999.9999999999",
                    "-99999999999999999999.9999999999", "0.0000000001", "0.000000000000000000000000000001");
        case STRING:
            return Randomly.fromOptions("''", "' '", "'0'", "'NaN'", "'Infinity'", "'AaBbCc'", "'zzzzzzzzzzzzzzzzzzzz'",
                    "'  '", "'abc '", "'00'", "'-0'", "'1e309'", "'变'", "'é'", "'ß'", "'1970-01-01'", "'2038-01-19'",
                    "'9999-12-31'", "'2024-02-29'");
        case BYTES:
            return Randomly.fromOptions("x''::BYTES", "x'00'::BYTES", "x'ff'::BYTES", "x'414243'::BYTES");
        case TIME:
        case TIMETZ:
            return Randomly.fromOptions("'00:00:00'", "'00:00:00.000001'", "'12:34:56'", "'23:59:59'");
        case TIMESTAMP:
        case TIMESTAMPTZ:
            return Randomly.fromOptions("'1970-01-01 00:00:00'", "'2038-01-19 03:14:07'", "'9999-12-31 23:59:59'",
                    "'2024-02-29 12:34:56'");
        case INTERVAL:
            return Randomly.fromOptions("'0 seconds'", "'1 microsecond'", "'-1 day'", "'100 years'");
        case BIT:
            return fixedBitLiteral(column.getType().getSize());
        case VARBIT:
            return Randomly.fromOptions("B'0'", "B'1'", "B'1010'", "B''");
        case JSONB:
            return Randomly.fromOptions("'null'::JSONB", "'{}'::JSONB", "'[]'::JSONB", "'{\"vp\":1}'::JSONB");
        case ENUM:
            // Boundary mutations must retain the enum type. A bare string would be
            // resolved using the column context in some statements but not all.
            List<String> labels = column.getType().getEnumLabels();
            if (labels.isEmpty()) {
                return "NULL";
            }
            String label = Randomly.fromList(labels).replace("'", "''");
            return "'" + label + "'::" + column.getType().toString();
        case ARRAY:
            return "NULL";
        default:
            throw new AssertionError(column.getType().getPrimitiveDataType());
        }
    }

    private static String fixedBitLiteral(int size) {
        StringBuilder sb = new StringBuilder("B'");
        for (int i = 0; i < size; i++) {
            sb.append(Randomly.getBoolean() ? '1' : '0');
        }
        sb.append("'");
        return sb.toString();
    }

    private static ExpectedErrors commonCandidateMutationErrors() {
        ExpectedErrors errors = new ExpectedErrors();
        CockroachDBErrors.addExpressionErrors(errors);
        CockroachDBErrors.addTransactionErrors(errors);
        errors.add("violates unique constraint");
        errors.add("violates not-null constraint");
        errors.add("violates foreign key constraint");
        errors.add("failed to satisfy CHECK constraint");
        errors.add("cannot write directly to computed column");
        errors.add("numeric constant out of int64 range");
        errors.add("integer out of range");
        errors.add("decimal out of range");
        errors.add("invalid cast");
        errors.add("at or near \"null\": syntax error");
        errors.add("invalid input value for enum");
        errors.add("syntax error");
        errors.add("invalid input value for enum");
        errors.add("could not parse");
        errors.add("unsupported type");
        errors.add("schema change statement cannot follow a statement that has written in the same transaction");
        errors.add("relation already exists");
        errors.add("already exists");
        errors.add("polling for queued jobs to complete");
        errors.add("intent_resolver_ir_batcher.sendBatch");
        errors.add("active-schema-leases-by-region");
        errors.add("must be after replica GC threshold");
        errors.add("result is ambiguous: context deadline exceeded");
        errors.add("query execution canceled");
        errors.add("node liveness error");
        errors.add("could not mark as succeeded");
        // CockroachDB rejects asynchronous index backfills once a store crosses its
        // configured free-space watermark. This is an environmental admission
        // failure, not a property of the generated statement.
        errors.add("insufficient remaining capacity to ingest data");
        errors.add("job was paused before it completed with reason: error executing PostCommitPhase");
        return errors;
    }

    private static boolean isCockroachSchemaChangerOperationalError(String errorText) {
        String msg = errorText.toLowerCase();
        return msg.contains("polling for queued jobs to complete")
                || msg.contains("intent_resolver_ir_batcher.sendbatch")
                || msg.contains("active-schema-leases-by-region") || msg.contains("must be after replica gc threshold")
                || msg.contains("result is ambiguous: context deadline exceeded")
                || msg.contains("query execution canceled") || msg.contains("node liveness error")
                || msg.contains("could not mark as succeeded") || isCockroachResourceLimitError(msg);
    }

    public static boolean isCockroachTransientClusterError(String errorText) {
        String msg = errorText.toLowerCase();
        return msg.contains("replica unavailable") || msg.contains("replica has been leaderless")
                || msg.contains("leaderless for") || msg.contains("closed timestamp") || msg.contains("slow proposal")
                || msg.contains("txn exec: context canceled") || msg.contains("txn exec: context cancelled")
                || msg.contains("node liveness error") || msg.contains("must be after replica gc threshold")
                || msg.contains("i/o error occurred while sending to the backend")
                || msg.contains("eofexception") || msg.contains("connection reset")
                || msg.contains("connection closed") || msg.contains("connection has been closed")
                || msg.contains("connection is closed") || msg.contains("terminating connection")
                || isCockroachResourceLimitError(msg);
    }

    private static boolean isCockroachResourceLimitError(String errorText) {
        String msg = errorText.toLowerCase();
        return msg.contains("maximum number of concurrently open transactions")
                || msg.contains("too many open transactions");
    }

    private static boolean isCockroachActionTimeout(String errorText) {
        String msg = errorText.toLowerCase();
        return msg.contains("canceling statement due to user request") || msg.contains("statement timeout")
                || msg.contains("query execution canceled");
    }

    private static void waitForTransientClusterRecovery(int attempt) throws IgnoreMeException {
        try {
            Thread.sleep(250L * (attempt + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IgnoreMeException();
        }
    }

    private static void updateSchemaOrIgnore(CockroachDBGlobalState globalState) throws Exception {
        for (int attempt = 0; attempt < VP_SCHEMA_REFRESH_RETRIES; attempt++) {
            try {
                globalState.updateSchema();
                return;
            } catch (SQLException e) {
                String errorText = getVpActionErrorText(e);
                String msg = errorText.toLowerCase();
                if (msg.contains("database") && msg.contains("does not exist")) {
                    CockroachDBVerticalPartitioningOracle.recordSkippedError("setup", "UPDATE SCHEMA", errorText);
                    throw new IgnoreMeException();
                }
                if (isCockroachTransientClusterError(msg)) {
                    if (attempt + 1 < VP_SCHEMA_REFRESH_RETRIES) {
                        waitForTransientClusterRecovery(attempt);
                        continue;
                    }
                    CockroachDBVerticalPartitioningOracle.recordSkippedError("setup", "UPDATE SCHEMA", errorText);
                    throw new IgnoreMeException();
                }
                throw e;
            }
        }
        throw new IgnoreMeException();
    }

    private static void logVerticalPartitioningReplacementFailure(CockroachDBGlobalState globalState,
            String oldTableName) {
        String message = "-- VP replacement failed after " + VP_REPLACEMENT_RETRY_COUNT
                + " attempts; keeping old base table " + oldTableName;
        try {
            globalState.getLogger().writeCurrent(message);
        } catch (UnsupportedOperationException ignored) {
            globalState.getState().logStatement(message);
        }
    }

    private static CockroachDBTable findVerticalPartitioningBaseTable(CockroachDBGlobalState globalState,
            String tableName) {
        return globalState.getSchema().getDatabaseTables().stream().filter(t -> t.getName().equals(tableName))
                .filter(CockroachDBProvider::isPotentialVerticalPartitioningBaseTable).findFirst().orElse(null);
    }

    private static CockroachDBTable getRandomVerticalPartitioningBaseTable(CockroachDBGlobalState globalState) {
        List<CockroachDBTable> candidates = globalState.getSchema().getDatabaseTables().stream()
                .filter(CockroachDBProvider::isPotentialVerticalPartitioningBaseTable).collect(Collectors.toList());
        if (candidates.isEmpty()) {
            throw new IgnoreMeException();
        }
        return Randomly.fromList(candidates);
    }

    private static boolean isPotentialVerticalPartitioningBaseTable(CockroachDBTable table) {
        return !table.isView() && !table.getName().startsWith("vp_") && !table.getColumns().isEmpty();
    }

    private static boolean isValidVerticalPartitioningBaseTable(CockroachDBGlobalState globalState,
            CockroachDBTable table) {
        if (!isPotentialVerticalPartitioningBaseTable(table)) {
            return false;
        }
        String columnList = table.getColumns().stream().map(c -> quoteIdentifier(c.getName()))
                .collect(Collectors.joining(", "));
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

    private static long countVerticalPartitioningBaseTables(CockroachDBGlobalState globalState) {
        return globalState.getSchema().getDatabaseTables().stream()
                .filter(CockroachDBProvider::isPotentialVerticalPartitioningBaseTable).count();
    }

    private static String quoteIdentifier(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public SQLConnection createDatabase(CockroachDBGlobalState globalState) throws SQLException {
        if (usesVerticalPartitioningOracle(globalState)) {
            return createVerticalPartitioningDatabase(globalState);
        }
        return createDatabaseInternal(globalState);
    }

    private SQLConnection createVerticalPartitioningDatabase(CockroachDBGlobalState globalState) throws SQLException {
        SQLException lastFailure = null;
        for (int attempt = 0; attempt < VP_DATABASE_CREATE_RETRIES; attempt++) {
            try {
                return createVerticalPartitioningDatabaseAttempt(globalState);
            } catch (SQLException e) {
                lastFailure = e;
                if (!isDatabaseBootstrapConcurrencyError(e) || attempt + 1 >= VP_DATABASE_CREATE_RETRIES) {
                    throw e;
                }
                try {
                    Thread.sleep(Math.min(2000L, 250L * (attempt + 1)));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    SQLException interruptedFailure = new SQLException(
                            "Interrupted while retrying CockroachDB VP database bootstrap", interrupted);
                    interruptedFailure.addSuppressed(e);
                    throw interruptedFailure;
                }
            }
        }
        throw lastFailure;
    }

    private SQLConnection createVerticalPartitioningDatabaseAttempt(CockroachDBGlobalState globalState)
            throws SQLException {
        boolean acquired = false;
        try {
            VP_DATABASE_CREATE_PERMITS.acquire();
            acquired = true;
            return createDatabaseInternal(globalState);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while acquiring CockroachDB VP database bootstrap permit", e);
        } finally {
            if (acquired) {
                VP_DATABASE_CREATE_PERMITS.release();
            }
        }
    }

    private static boolean isDatabaseBootstrapConcurrencyError(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String lower = message.toLowerCase();
                if (isCockroachResourceLimitError(lower) || lower.contains("schema change is waiting")
                        || lower.contains("waiting for job") || lower.contains("polling for queued jobs")
                        || lower.contains("too many open transactions")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private SQLConnection createDatabaseInternal(CockroachDBGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = CockroachDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = CockroachDBOptions.DEFAULT_PORT;
        }
        String databaseName = globalState.getDatabaseName();
        String bootstrap = globalState.getDbmsSpecificOptions().oracle == CockroachDBOracleFactory.ECST ? "defaultdb"
                : "test";
        String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, bootstrap);
        Connection con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        globalState.getState().logStatement("USE " + bootstrap);
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName + " CASCADE");
        String createDatabaseCommand = "CREATE DATABASE " + databaseName;
        globalState.getState().logStatement(createDatabaseCommand);
        globalState.getState().logStatement("USE " + databaseName);
        try {
            try (Statement s = con.createStatement()) {
                s.execute("DROP DATABASE IF EXISTS " + databaseName);
            }
            try (Statement s = con.createStatement()) {
                s.execute(createDatabaseCommand);
            }
            if (usesVerticalPartitioningOracle(globalState)
                    && globalState.getDbmsSpecificOptions().oracle != CockroachDBOracleFactory.ECST) {
                configureVerticalPartitioningDatabaseGc(con, databaseName, globalState);
            }
        } finally {
            con.close();
        }
        con = DriverManager.getConnection(String.format("jdbc:postgresql://%s:%d/%s", host, port, databaseName),
                globalState.getOptions().getUserName(), globalState.getOptions().getPassword());
        return new SQLConnection(con);
    }

    private static void configureVerticalPartitioningDatabaseGc(Connection con, String databaseName,
            CockroachDBGlobalState globalState) {
        String sql = "ALTER DATABASE " + quoteIdentifier(databaseName) + " CONFIGURE ZONE USING gc.ttlseconds = "
                + VP_SCHEMA_GC_TTL_SECONDS;
        globalState.getState().logStatement(sql);
        try (Statement s = con.createStatement()) {
            s.execute(sql);
        } catch (SQLException ignored) {
            // Zone configuration is a profile acceleration. Keep VP functional on clusters where
            // the current user is not permitted to configure zones.
        }
    }

    @Override
    public String getDBMSName() {
        return "cockroachdb";
    }

    @Override
    public String getQueryPlan(String selectStr, CockroachDBGlobalState globalState) throws Exception {
        String queryPlan = "";
        String explainQuery = "EXPLAIN (OPT) " + selectStr;
        if (globalState.getOptions().logEachSelect()) {
            globalState.getLogger().writeCurrent(explainQuery);
            try {
                globalState.getLogger().getCurrentFileWriter().flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        SQLQueryAdapter q = new SQLQueryAdapter(explainQuery);
        boolean afterProjection = false; // Remove the concrete expression after each Projection operator
        try (SQLancerResultSet rs = q.executeAndGet(globalState)) {
            if (rs != null) {
                while (rs.next()) {
                    String targetQueryPlan = rs.getString(1).replace("└──", "").replace("├──", "").replace("│", "")
                            .trim() + ";"; // Unify format
                    if (afterProjection) {
                        afterProjection = false;
                        continue;
                    }
                    if (targetQueryPlan.startsWith("projections")) {
                        afterProjection = true;
                    }
                    // Remove all concrete expressions by keywords
                    if (targetQueryPlan.contains(">") || targetQueryPlan.contains("<") || targetQueryPlan.contains("=")
                            || targetQueryPlan.contains("*") || targetQueryPlan.contains("+")
                            || targetQueryPlan.contains("'")) {
                        continue;
                    }
                    queryPlan += targetQueryPlan;
                }
            }
        } catch (AssertionError e) {
            throw new AssertionError("Explain failed: " + explainQuery);
        }

        return queryPlan;
    }

    @Override
    protected double[] initializeWeightedAverageReward() {
        return new double[Action.values().length];
    }

    @Override
    protected void executeMutator(int index, CockroachDBGlobalState globalState) throws Exception {
        Action action = Action.values()[index];
        if (usesVerticalPartitioningOracle(globalState)) {
            if (!globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes
                    && (action == Action.DELETE || action == Action.TRUNCATE || action == Action.DROP_TABLE)) {
                throw new IgnoreMeException();
            }
            if (action == Action.CREATE_TABLE) {
                CockroachDBTable table = createValidVerticalPartitioningBaseTable(globalState, false);
                if (table == null) {
                    throw new IgnoreMeException();
                }
                updateSchemaOrIgnore(globalState);
                return;
            }
            updateSchemaOrIgnore(globalState);
            CockroachDBTable table = getRandomVerticalPartitioningBaseTable(globalState);
            // Empty candidates are intentional VP boundary cases. Keep them empty throughout
            // the provider mutation phase; INSERT remains available for every non-empty base
            // table and is still exercised by the ordinary candidate action pool.
            if (action == Action.INSERT && getVerticalPartitioningRowCount(globalState, table.getName()) == 0) {
                throw new IgnoreMeException();
            }
            SQLQueryAdapter queryMutateTable = createVerticalPartitioningCandidateMutation(globalState, table, action);
            executeVpActionStatement(globalState, queryMutateTable);
            updateSchemaOrIgnore(globalState);
            return;
        }
        // DDL/DML actions can invalidate the cached schema (especially after DROP/ALTER
        // COLUMN). Refresh before selecting the target table so generated VIEW expressions
        // never retain references to columns that no longer exist.
        updateSchemaOrIgnore(globalState);
        SQLQueryAdapter queryMutateTable = action.getQuery(globalState);
        globalState.executeStatement(queryMutateTable);
    }

    @Override
    public boolean addRowsToAllTables(CockroachDBGlobalState globalState) throws Exception {
        List<CockroachDBTable> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (CockroachDBTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = CockroachDBInsertGenerator.insert(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }

}
