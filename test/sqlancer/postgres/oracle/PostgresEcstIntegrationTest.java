package sqlancer.postgres.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import com.beust.jcommander.JCommander;

import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.StateToReproduce;
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.common.oracle.ecst.EcstTransformationConfig;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.ExpressionContext;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.PartitionLayout;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.Reconstruction;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.Representation;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresOptions;
import sqlancer.postgres.PostgresOracleFactory;
import sqlancer.postgres.PostgresProvider;
import sqlancer.postgres.oracle.PostgresVerticalPartitioningOracle.EcstCase;
import sqlancer.postgres.oracle.PostgresVerticalPartitioningOracle.EcstSeed;

/** Isolated databases on the configured single-node server, always dropped after each acceptance test. */
@EnabledIfSystemProperty(named = "postgres.ecst.live", matches = "true")
class PostgresEcstIntegrationTest {
    private Connection connection;
    private String database;
    private PostgresGlobalState state;
    private PostgresVerticalPartitioningOracle vp;

    private static Connection connect(String database) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", System.getProperty("postgres.ecst.user", "postgres"));
        properties.setProperty("password",
                System.getProperty("postgres.ecst.password", System.getenv().getOrDefault("PG_ECST_PASSWORD", "")));
        properties.setProperty("options", System.getProperty("postgres.ecst.options", ""));
        return DriverManager.getConnection("jdbc:postgresql://" + System.getProperty("postgres.ecst.host", "127.0.0.1")
                + ":" + System.getProperty("postgres.ecst.port", "23318") + "/" + database, properties);
    }

    @BeforeEach
    void setup() throws Exception {
        database = "ecst_pg_test_" + Long.toUnsignedString(System.nanoTime(), 36);
        try (Connection admin = connect("postgres"); Statement statement = admin.createStatement()) {
            statement.execute("CREATE DATABASE " + database);
        }
        connection = connect(database);
        PostgresEcstSession.prepare(connection);
        execute("CREATE TABLE base (c0 BIGINT NULL,c1 BIGINT NULL,c2 VARCHAR(50) COLLATE \"C\" NULL)");
        execute("INSERT INTO base VALUES (1,2,'001'),(1,2,'1'),(NULL,2,''),(3,NULL,NULL),(-4,1,'x'),(10,1,'a ')");
        execute("CREATE TABLE base2 AS SELECT * FROM base");
        state = new PostgresGlobalState();
        MainOptions main = new MainOptions();
        JCommander.newBuilder().addObject(main).build().parse("--log-each-select", "false");
        state.setMainOptions(main);
        PostgresOptions options = new PostgresOptions();
        options.oracle = List.of(PostgresOracleFactory.ECST);
        options.ecstQueryPairs = 2;
        state.setDbmsSpecificOptions(options);
        state.setRandomly(new Randomly(37));
        state.setDatabaseName(database);
        state.setConnection(new SQLConnection(connection));
        state.setState(new StateToReproduce(database, new PostgresProvider()));
        state.updateSchema();

        vp = new PostgresVerticalPartitioningOracle(state);
    }

    @AfterEach
    void cleanup() throws SQLException {
        if (connection != null) {
            connection.close();
        }
        if (database != null) {
            try (Connection admin = connect("postgres"); Statement statement = admin.createStatement()) {
                statement.execute("DROP DATABASE IF EXISTS " + database);
            }
        }
    }

    private void execute(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private long scalar(String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(sql)) {
            assertTrue(rows.next());
            return rows.getLong(1);
        }
    }

    private EcstSeed seed() throws Exception {
        return vp.createEcstSeed("base", "coalesce(\"c0\",0)+coalesce(\"c1\",0)", List.of("c0", "c1"),
                ExpressionContext.WHERE, false, false);
    }

    private static EcstTransformationConfig config(PartitionLayout layout, Reconstruction reconstruction,
            ExpressionContext context) {
        return new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED, layout, reconstruction, context);
    }

    private void valid(EcstCase testCase) throws Exception {
        vp.prepareEcstCase(testCase);
        PostgresVerticalPartitioningOracle.EcstSemanticPreconditionResult semantic = vp
                .checkSemanticPreconditions(testCase);
        PostgresVerticalPartitioningOracle.EcstSemanticPreconditionResult data = vp.validateDataPreconditions(testCase);
        PostgresVerticalPartitioningOracle.EcstComparison comparison = vp.compareEcstCase(testCase);
        assertTrue(semantic.isValid(), semantic.toString());
        assertTrue(data.isValid(), data.toString());
        assertTrue(comparison.isEqual(), comparison.getReason());
    }

    @Test
    void all63ConfigurationsKeepTheSourceOperationAndResults() throws Exception {
        int total = 0;
        for (ExpressionContext context : List.of(ExpressionContext.WHERE, ExpressionContext.SELECT,
                ExpressionContext.HAVING)) {
            EcstSeed seed = vp.createEcstSeed("base", "coalesce(\"c0\",0)+coalesce(\"c1\",0)", List.of("c0", "c1"),
                    context, false, false);
            try {
                for (Representation representation : List.of(Representation.VERTICAL_PARTITIONED,
                        Representation.VIEW)) {
                    for (PartitionLayout layout : PartitionLayout.values()) {
                        for (Reconstruction reconstruction : Reconstruction.values()) {
                            for (ExpressionContext embedded : ExpressionContext.values()) {
                                EcstTransformationConfig configuration = new EcstTransformationConfig(representation,
                                        layout, reconstruction, embedded);
                                if (!PostgresEcstCapabilities.isLegal(seed, configuration)) {
                                    continue;
                                }
                                EcstCase testCase = vp.applyEcstTransformation(seed, configuration, null);
                                assertEquals(seed.getOriginalQuery(), testCase.getOriginalQuery());
                                assertEquals(seed.getOperation(), testCase.getOperation());
                                valid(testCase);
                                total++;
                            }
                        }
                    }
                }
            } finally {
                vp.cleanupEcstSeed(seed);
            }
            System.out.println("Postgres ECST verified: " + total + "/63 (" + context + ")");
        }
        assertEquals(63, total);
    }

    @Test
    void corruptStringsDuplicateRowIdsAndMetadataAreRejectedAndRetryRecovers() throws Exception {
        EcstSeed seed = seed();
        try {
            EcstTransformationConfig configuration = config(PartitionLayout.SAME_SIDE, Reconstruction.JOIN,
                    ExpressionContext.WHERE);
            EcstCase first = vp.applyEcstTransformation(seed, configuration, null);
            valid(first);
            String owner = scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema='" + "public"
                    + "' AND table_name='" + first.getLeftTable() + "' AND column_name='c2'") == 1
                            ? first.getLeftTable() : first.getRightTable();
            execute("UPDATE \"" + owner + "\" SET c2='1' WHERE c2='001'");
            assertFalse(vp.validateDataPreconditions(first).isValid());
            EcstCase second = vp.applyEcstTransformation(seed, configuration, null);
            valid(second);
            execute("ALTER TABLE \"" + second.getLeftTable() + "\" DROP CONSTRAINT \"" + second.getLeftTable()
                    + "_pkey\"");
            assertFalse(vp.checkSemanticPreconditions(second).isValid());
            EcstCase third = vp.applyEcstTransformation(seed, configuration, null);
            valid(third);
            execute("ALTER TABLE \"" + third.getLeftTable() + "\" ALTER COLUMN c0 TYPE VARCHAR(100) USING c0::varchar");
            assertFalse(vp.checkSemanticPreconditions(third).isValid());
            valid(vp.applyEcstTransformation(seed, configuration, null));
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void scanPredicateMovesIntoNativeJoinCondition() throws Exception {
        EcstSeed seed = vp.createEcstSeed("base", "\"c0\"+\"c1\"", List.of("c0", "c1"), ExpressionContext.WHERE, false,
                false, "\"c0\">\"c1\"");
        try {
            EcstTarget target = new EcstTarget("operator", EcstContextDimension.OPERATOR, "SCAN_FILTER",
                    "JOIN_CONDITION");
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    config(PartitionLayout.CROSS_SIDE, Reconstruction.JOIN, ExpressionContext.JOIN_ON), target);
            valid(testCase);
            sqlancer.common.oracle.ecst.EcstObservation observation = new PostgresExecutionContextAnalyzer(state)
                    .analyze(testCase, target);
            assertEquals(EcstShiftStatus.SUCCESS, observation.getStatus(),
                    observation.getOriginalPlan() + observation.getTransformedPlan());
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void actualRangeAccessCanChangeInBothDirections() throws Exception {
        execute("CREATE TABLE selective(c0 BIGINT)");
        execute("INSERT INTO selective SELECT CASE WHEN seq=1 THEN 1 ELSE -seq END FROM generate_series(1,4096) seq");
        state.getDbmsSpecificOptions().ecstSnapshotRows = 0;
        for (boolean indexed : List.of(false, true)) {
            EcstSeed seed = vp.createEcstSeed("selective", "\"c0\"", List.of("c0"), ExpressionContext.WHERE, indexed,
                    false);
            try {
                EcstTarget target = new EcstTarget("access", EcstContextDimension.ACCESS_PATH,
                        indexed ? "INDEX_SCAN" : "TABLE_SCAN", indexed ? "TABLE_SCAN" : "INDEX_SCAN");
                EcstCase testCase = vp.applyEcstTransformation(seed,
                        config(PartitionLayout.SAME_SIDE, Reconstruction.JOIN, ExpressionContext.WHERE), target);
                valid(testCase);
                sqlancer.common.oracle.ecst.EcstObservation observation = new PostgresExecutionContextAnalyzer(state)
                        .analyze(testCase, target);
                assertEquals(EcstShiftStatus.SUCCESS, observation.getStatus(),
                        observation.getOriginalPlan() + observation.getTransformedPlan());
                assertEquals(1, vp.compareEcstCase(testCase).getOriginalResultCount());
            } finally {
                vp.cleanupEcstSeed(seed);
            }
        }
    }

    @Test
    void materializedBoundaryCanChangeInBothDirections() throws Exception {
        for (boolean retained : List.of(false, true)) {
            EcstSeed seed = vp.createEcstSeed("base", "\"c0\"+\"c1\"", List.of("c0", "c1"), ExpressionContext.WHERE,
                    false, retained);
            try {
                EcstTarget target = new EcstTarget("boundary", EcstContextDimension.BOUNDARY,
                        retained ? "MATERIALIZED" : "MERGED", retained ? "MERGED" : "MATERIALIZED");
                EcstCase testCase = vp.applyEcstTransformation(seed, config(PartitionLayout.CROSS_SIDE,
                        retained ? Reconstruction.JOIN : Reconstruction.CTE, ExpressionContext.WHERE), target);
                valid(testCase);
                sqlancer.common.oracle.ecst.EcstObservation observation = new PostgresExecutionContextAnalyzer(state)
                        .analyze(testCase, target);
                assertEquals(EcstShiftStatus.SUCCESS, observation.getStatus(),
                        observation.getOriginalPlan() + observation.getTransformedPlan());
            } finally {
                vp.cleanupEcstSeed(seed);
            }
        }
    }

    @Test
    void richReplayIsIndependentOfOriginalTablesAndConnection() throws Exception {
        execute("CREATE TABLE rich(c0 BIGINT,c1 BIGINT,c2 TEXT COLLATE \"C\",c3 BYTEA,c4 BIT(64),c5 JSON,c6 NUMERIC(40,6),c7 TIMESTAMPTZ,c8 INET,c9 UUID,c10 INT[],c11 NUMRANGE,c12 INTERVAL,c13 JSONB)");
        execute("INSERT INTO rich VALUES (1,2,'001''é',decode('00ff','hex'),B'1000000000000000000000000000000000000000000000000000000000000001','{\"z\":1,\"a\":null}',123456789012345678901234567890.123456,'2026-11-09 01:02:03.123456+08','2001:db8::1','550e8400-e29b-41d4-a716-446655440000',ARRAY[1,NULL,2],'[1.1,2.2)','1 month 2 days 03:04:05','{\"b\":2,\"a\":1}')");
        EcstSeed seed = vp.createEcstSeed("rich", "c0+c1", List.of("c0", "c1"), ExpressionContext.SELECT, false, false);
        EcstTransformationConfig configuration = config(PartitionLayout.CROSS_SIDE, Reconstruction.CTE,
                ExpressionContext.INNER_SELECT);
        EcstCase testCase = vp.applyEcstTransformation(seed, configuration, null);
        valid(testCase);
        String owner = scalar("SELECT count(*) FROM information_schema.columns WHERE table_name='"
                + testCase.getLeftTable() + "' AND column_name='c5'") == 1 ? testCase.getLeftTable()
                        : testCase.getRightTable();
        execute("UPDATE \"" + owner + "\" SET c5=(c5::text||' ')::json");
        assertFalse(vp.validateDataPreconditions(testCase).isValid());
        testCase = vp.applyEcstTransformation(seed, configuration, null);
        valid(testCase);
        List<String> replay = vp.getSourceDataReplay(testCase);
        String expected = richValues(seed.getSourceTable());
        vp.cleanupEcstSeed(seed);
        execute("DROP TABLE rich");
        String freshDatabase = database + "_replay";
        try (Connection admin = connect("postgres"); Statement statement = admin.createStatement()) {
            statement.execute("CREATE DATABASE " + freshDatabase);
        }
        try (Connection fresh = connect(freshDatabase); Statement statement = fresh.createStatement()) {
            for (String sql : replay) {
                statement.execute(sql);
            }
            try (ResultSet rows = statement.executeQuery(
                    "SELECT row_to_json(t)::text FROM (SELECT c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13 FROM \""
                            + seed.getSourceTable() + "\") t")) {
                assertTrue(rows.next());
                assertEquals(expected, rows.getString(1));
            }
            try (ResultSet rows = statement.executeQuery(testCase.getOriginalQuery())) {
                assertTrue(rows.next());
                assertEquals(3, rows.getInt("ecst_value"));
            }
            try (ResultSet rows = statement.executeQuery(testCase.getTransformedQuery())) {
                assertTrue(rows.next());
                assertEquals(3, rows.getInt("ecst_value"));
            }
        } finally {
            try (Connection admin = connect("postgres"); Statement statement = admin.createStatement()) {
                statement.execute("DROP DATABASE " + freshDatabase);
            }
        }
    }

    private String richValues(String table) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery(
                        "SELECT row_to_json(t)::text FROM (SELECT c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13 FROM \""
                                + table + "\") t")) {
            assertTrue(rows.next());
            return rows.getString(1);
        }
    }

    @Test
    void emptyResultsStillCompareNativeOutputTypes() throws Exception {
        execute("CREATE TABLE empty_base(c0 BIGINT)");
        EcstSeed seed = vp.createEcstSeed("empty_base", "\"c0\"", List.of("c0"), ExpressionContext.SELECT, false,
                false);
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    config(PartitionLayout.SAME_SIDE, Reconstruction.JOIN, ExpressionContext.SELECT), null);
            valid(testCase);
            assertEquals(0, vp.compareEcstCase(testCase).getOriginalResultCount());
            execute("ALTER TABLE \"" + testCase.getLeftTable()
                    + "\" ALTER COLUMN c0 TYPE VARCHAR(100) USING c0::varchar");
            assertFalse(vp.compareEcstCase(testCase).isEqual());
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void incomingOptimizerSettingsAndUnexpectedErrorsArePreserved() throws Exception {
        execute("SET enable_hashjoin=off");
        EcstSeed seed = seed();
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    config(PartitionLayout.CROSS_SIDE, Reconstruction.JOIN, ExpressionContext.WHERE), null);
            valid(testCase);
            try (Statement statement = connection.createStatement();
                    ResultSet rows = statement.executeQuery("SHOW enable_hashjoin")) {
                assertTrue(rows.next());
                assertEquals("off", rows.getString(1));
            }
            assertThrows(SQLException.class, () -> new PostgresExecutionContextAnalyzer(state)
                    .extractContext("SELECT ecst_no_such_function(c0) FROM base", seed.getOperation(), Set.of("base")));
            EcstSeed bad = vp.createEcstSeed("base", "ecst_no_such_function(\"c0\")", List.of("c0"),
                    ExpressionContext.SELECT, false, false);
            try {
                EcstCase broken = vp.applyEcstTransformation(bad,
                        config(PartitionLayout.SAME_SIDE, Reconstruction.JOIN, ExpressionContext.SELECT), null);
                assertThrows(SQLException.class, () -> vp.compareEcstCase(broken));
            } finally {
                vp.cleanupEcstSeed(bad);
            }
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void unsupportedDdlDependenciesAreRejectedBeforeSnapshotCreation() throws Exception {
        execute("CREATE TYPE ecst_custom_enum AS ENUM ('a','b')");
        execute("CREATE TABLE custom_type(c0 BIGINT,c1 ecst_custom_enum)");
        assertThrows(IgnoreMeException.class,
                () -> vp.createEcstSeed("custom_type", "c0", List.of("c0"), ExpressionContext.WHERE, false, false));
        execute("CREATE COLLATION ecst_custom_coll FROM \"C\"");
        execute("CREATE TABLE custom_collation(c0 BIGINT,c1 TEXT COLLATE ecst_custom_coll)");
        assertThrows(IgnoreMeException.class, () -> vp.createEcstSeed("custom_collation", "c0", List.of("c0"),
                ExpressionContext.WHERE, false, false));
        assertEquals(0, scalar("SELECT count(*) FROM pg_class WHERE relname LIKE 'vp_ecst_pg_%'"));
    }

    @Test
    void fullOracleComparesAndCleansHelpers() throws Exception {
        PostgresECSTOracle oracle = new PostgresECSTOracle(state);
        oracle.check();
        assertEquals(2, oracle.getCompletedCases());
        assertEquals(0, scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema='" + "public"
                + "' AND table_name LIKE 'vp_ecst_pg_%'"));
    }
}
