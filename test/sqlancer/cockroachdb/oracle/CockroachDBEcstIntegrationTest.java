package sqlancer.cockroachdb.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.beust.jcommander.JCommander;

import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.StateToReproduce;
import sqlancer.cockroachdb.CockroachDBOptions;
import sqlancer.cockroachdb.CockroachDBOracleFactory;
import sqlancer.cockroachdb.CockroachDBProvider;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.oracle.CockroachDBVerticalPartitioningOracle.EcstCase;
import sqlancer.cockroachdb.oracle.CockroachDBVerticalPartitioningOracle.EcstSeed;
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.common.oracle.ecst.EcstTransformationConfig;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.ExpressionContext;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.PartitionLayout;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.Reconstruction;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.Representation;

/** Opt-in acceptance on a real single-node instance. Configured connection/SQL errors must fail. */
class CockroachDBEcstIntegrationTest {
    private Connection connection;
    private String database;
    private CockroachDBGlobalState state;
    private CockroachDBVerticalPartitioningOracle vp;

    private static String url() {
        return System.getProperty("cockroachdb.ecst.test.url");
    }

    private static String user() {
        return System.getProperty("cockroachdb.ecst.test.user", "root");
    }

    private static String password() {
        return System.getProperty("cockroachdb.ecst.test.password", "");
    }

    private static Connection admin() throws SQLException {
        return DriverManager.getConnection(url(), user(), password());
    }

    private static String databaseUrl(String name) {
        String url = url();
        int query = url.indexOf('?');
        String options = query < 0 ? "" : url.substring(query);
        String base = query < 0 ? url : url.substring(0, query);
        return base.substring(0, base.lastIndexOf('/') + 1) + name + options;
    }

    @BeforeEach
    void connect() throws Exception {
        Assumptions.assumeTrue(url() != null, "set cockroachdb.ecst.test.url to run single-node acceptance");
        database = "ecst_crdb_test_" + Long.toUnsignedString(System.nanoTime(), 36);
        try (Connection admin = admin(); Statement statement = admin.createStatement()) {
            statement.execute("CREATE DATABASE \"" + database + "\"");
        }
        connection = DriverManager.getConnection(databaseUrl(database), user(), password());
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE base (c0 INT8,c1 INT8,c2 STRING)");
            statement.execute(
                    "INSERT INTO base VALUES (1,2,'001'),(1,2,'1'),(NULL,2,''),(3,NULL,NULL),(-4,1,'x'),(0,0,'0')");
            statement.execute("CREATE TABLE base2 AS SELECT * FROM base");
        }
        MainOptions main = new MainOptions();
        JCommander.newBuilder().addObject(main).build().parse("--log-each-select", "false", "--username", user(),
                "--password", password());
        state = new CockroachDBGlobalState();
        state.setMainOptions(main);
        CockroachDBOptions options = new CockroachDBOptions();
        options.oracle = CockroachDBOracleFactory.ECST;
        options.ecstQueryPairs = 2;
        state.setDbmsSpecificOptions(options);
        state.setRandomly(new Randomly(37));
        state.setDatabaseName(database);
        state.setConnection(new SQLConnection(connection));
        state.setState(new StateToReproduce(database, new CockroachDBProvider()));
        state.updateSchema();
        vp = new CockroachDBVerticalPartitioningOracle(state, false);
    }

    @AfterEach
    void cleanup() throws Exception {
        if (database != null) {
            if (state != null && state.getConnection() != null) {
                state.getConnection().close();
            }
            if (connection != null) {
                connection.close();
            }
            try (Connection admin = admin(); Statement statement = admin.createStatement()) {
                statement.execute("DROP DATABASE IF EXISTS \"" + database + "_replay\" CASCADE");
                statement.execute("DROP DATABASE IF EXISTS \"" + database + "\" CASCADE");
            }
        }
    }

    @Test
    void controlledConfigurationsPreserveNullsDuplicatesGroupingAndOriginalOperation() throws Exception {
        int checked = 0;
        for (ExpressionContext context : List.of(ExpressionContext.WHERE, ExpressionContext.SELECT,
                ExpressionContext.HAVING)) {
            EcstSeed seed = vp.createEcstSeed("base", "COALESCE(\"c0\", 0) + COALESCE(\"c1\", 0)", List.of("c0", "c1"),
                    context, false, false);
            try {
                String original = seed.getOriginalQuery();
                for (Representation representation : List.of(Representation.VERTICAL_PARTITIONED,
                        Representation.VIEW)) {
                    for (PartitionLayout layout : PartitionLayout.values()) {
                        for (Reconstruction reconstruction : Reconstruction.values()) {
                            for (ExpressionContext embedded : ExpressionContext.values()) {
                                EcstTransformationConfig config = new EcstTransformationConfig(representation, layout,
                                        reconstruction, embedded);
                                if (!CockroachDBEcstCapabilities.isLegal(seed, config)) {
                                    continue;
                                }
                                EcstCase testCase = vp.applyEcstTransformation(seed, config, null);
                                vp.prepareEcstCase(testCase);
                                assertTrue(vp.checkSemanticPreconditions(testCase).isValid(), config.toString());
                                assertTrue(vp.validateDataPreconditions(testCase).isValid(), config.toString());
                                assertEquals(original, testCase.getOriginalQuery());
                                assertEquals(seed.getOperation(), testCase.getOperation());
                                assertTrue(vp.compareEcstCase(testCase).isComparable(), config.toString());
                                assertTrue(vp.compareEcstCase(testCase).isEqual(), config.toString());
                                checked++;
                            }
                        }
                    }
                }
            } finally {
                vp.cleanupEcstSeed(seed);
            }
            System.out.println("CockroachDB ECST configurations verified: " + checked + "/63 (" + context + ")");
        }
        assertEquals(63, checked, "expected the full legal CockroachDB controlled configuration set");
        assertEquals(0, helperCount());
    }

    @Test
    void retainedViewBoundaryIsObservedInBothDirections() throws Exception {
        CockroachDBExecutionContextAnalyzer analyzer = new CockroachDBExecutionContextAnalyzer(state);
        for (boolean materialized : List.of(false, true)) {
            EcstSeed seed = vp.createEcstSeed("base", "\"c0\" + \"c1\"", List.of("c0", "c1"), ExpressionContext.WHERE,
                    false, materialized);
            try {
                Reconstruction reconstruction = materialized ? Reconstruction.JOIN : Reconstruction.DERIVED_TABLE;
                EcstCase testCase = vp.applyEcstTransformation(seed,
                        new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED, PartitionLayout.CROSS_SIDE,
                                reconstruction, ExpressionContext.WHERE),
                        null);
                vp.prepareEcstCase(testCase);
                EcstTarget target = new EcstTarget("boundary", EcstContextDimension.BOUNDARY,
                        materialized ? "MATERIALIZED" : "MERGED", materialized ? "MERGED" : "MATERIALIZED");
                EcstObservation observed = analyzer.analyze(testCase, target);
                assertEquals(EcstShiftStatus.SUCCESS, observed.getStatus(),
                        observed.getOriginalPlan() + observed.getTransformedPlan());
                assertTrue(vp.compareEcstCase(testCase).isEqual());
            } finally {
                vp.cleanupEcstSeed(seed);
            }
        }
    }

    @Test
    void detectsTextDifferencesAndRejectsCorruptedMaterialization() throws Exception {
        EcstSeed seed = vp.createEcstSeed("base", "\"c2\"", List.of("c2"), ExpressionContext.SELECT, false, false);
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED, PartitionLayout.SAME_SIDE,
                            Reconstruction.JOIN, ExpressionContext.SELECT),
                    null);
            vp.prepareEcstCase(testCase);
            execute("UPDATE \"" + testCase.getLeftTable() + "\" SET c2='1' WHERE c2='001'");
            assertFalse(vp.validateDataPreconditions(testCase).isValid());
            assertFalse(vp.compareEcstCase(testCase).isEqual(), "text 001 and 1 must remain different");
            execute("ALTER TABLE \"" + testCase.getLeftTable() + "\" ALTER COLUMN c2 SET DATA TYPE VARCHAR(20)");
            assertFalse(vp.checkSemanticPreconditions(testCase).isValid());
            EcstCase recovered = vp.applyEcstTransformation(seed, testCase.getAppliedConfig(), null);
            vp.prepareEcstCase(recovered);
            assertTrue(vp.checkSemanticPreconditions(recovered).isValid());
            assertTrue(vp.validateDataPreconditions(recovered).isValid());
            assertTrue(vp.compareEcstCase(recovered).isEqual(), "retries must rebuild invalid cached partitions");
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void retryReplayUsesFinalObjectsAndRunsInAnIndependentDatabase() throws Exception {
        execute("CREATE TABLE rich (c0 INT8,c1 INT8,c2 STRING,b BIT(3),payload BYTES,a INT8[],j JSONB,t TIMESTAMPTZ,d DECIMAL)");
        execute("INSERT INTO rich SELECT c0,c1,c2,B'001',decode('00ff','hex'),ARRAY[1,NULL,3],'{\"x\":1}'::JSONB,'2026-11-07 00:00:00+00'::TIMESTAMPTZ,'NaN'::DECIMAL FROM base");
        EcstSeed seed = vp.createEcstSeed("rich", "\"c0\"+\"c1\"", List.of("c0", "c1"), ExpressionContext.WHERE, true,
                false);
        try {
            vp.prepareEcstCase(
                    vp.applyEcstTransformation(seed, new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED,
                            PartitionLayout.SAME_SIDE, Reconstruction.VIEW, ExpressionContext.WHERE), null));
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED, PartitionLayout.CROSS_SIDE,
                            Reconstruction.VIEW, ExpressionContext.JOIN_ON),
                    null);
            vp.prepareEcstCase(testCase);
            List<String> replay = vp.getSourceDataReplay(testCase);
            assertEquals(3, replay.stream().filter(sql -> sql.startsWith("CREATE TABLE")).count());
            assertFalse(replay.stream().anyMatch(sql -> sql.contains("FROM \"rich\"")));
            try (Connection admin = admin()) {
                admin.createStatement().execute("CREATE DATABASE \"" + database + "_replay\"");
            }
            vp.cleanupEcstSeed(seed);
            SQLConnection original = state.getConnection();
            try (Connection target = DriverManager.getConnection(databaseUrl(database + "_replay"), user(),
                    password())) {
                state.setConnection(new SQLConnection(target));
                for (String sql : replay) {
                    execute(sql);
                }
                assertTrue(vp.checkSemanticPreconditions(testCase).isValid());
                assertTrue(vp.validateDataPreconditions(testCase).isValid(),
                        "typed array/bit/byte/json/time payload must replay exactly");
                assertTrue(vp.compareEcstCase(testCase).isEqual());
            } finally {
                state.setConnection(original);
            }
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void oracleCompletesComparablePairsAndCleansArtifacts() throws Exception {
        CockroachDBECSTOracle oracle = new CockroachDBECSTOracle(state);
        oracle.check();
        assertEquals(2, oracle.getCompletedCases());
        assertTrue(oracle.getHistory().getTotalTrials() >= 2);
        assertEquals(0, helperCount());
    }

    @Test
    void emptyDataStillProducesAComparablePairAndIncompatibleContextsAreFiltered() throws Exception {
        execute("DELETE FROM base");
        EcstSeed seed = vp.createEcstSeed("base", "\"c0\"", List.of("c0"), ExpressionContext.SELECT, false, false);
        try {
            EcstTransformationConfig invalid = new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED,
                    PartitionLayout.CROSS_SIDE, Reconstruction.JOIN, ExpressionContext.SELECT);
            assertFalse(CockroachDBEcstCapabilities.isLegal(seed, invalid));
            EcstTransformationConfig valid = new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED,
                    PartitionLayout.SAME_SIDE, Reconstruction.CTE, ExpressionContext.INNER_SELECT);
            EcstCase testCase = vp.applyEcstTransformation(seed, valid, null);
            vp.prepareEcstCase(testCase);
            assertTrue(vp.compareEcstCase(testCase).isComparable());
            assertTrue(vp.compareEcstCase(testCase).isEqual());
            assertEquals(0, vp.compareEcstCase(testCase).getOriginalResultCount());
            assertEquals(Set.of(), CockroachDBECSTOracle.candidateSets(seed,
                    new sqlancer.common.oracle.ecst.EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"))
                    .keySet());
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void characterHavingIsDeterministicAndVectorizationRemainsUnchanged() throws Exception {
        execute("INSERT INTO base VALUES (9,9,'a'),(9,9,'a ')");
        String before;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rows = statement.executeQuery("SHOW vectorize")) {
            assertTrue(rows.next());
            before = rows.getString(1);
        }
        EcstSeed seed = vp.createEcstSeed("base", "CHAR_LENGTH(\"c2\")", List.of("c2"), ExpressionContext.HAVING, false,
                false);
        try {
            assertTrue(seed.getOriginalQuery().contains("GROUP BY \"vp_rowid\","));
            EcstCase testCase = vp.applyEcstTransformation(seed, new EcstTransformationConfig(Representation.VIEW,
                    PartitionLayout.SAME_SIDE, Reconstruction.VIEW, ExpressionContext.HAVING), null);
            vp.prepareEcstCase(testCase);
            assertTrue(vp.compareEcstCase(testCase).isEqual());
            try (Statement statement = state.getConnection().createStatement();
                    ResultSet rows = statement.executeQuery("SHOW vectorize")) {
                assertTrue(rows.next());
                assertEquals(before, rows.getString(1));
            }
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void compositePrimaryKeyDoesNotProveUniqueRowIdentity() throws Exception {
        execute("UPDATE base SET c0=COALESCE(c0,0)");
        EcstSeed seed = vp.createEcstSeed("base", "\"c0\"", List.of("c0"), ExpressionContext.WHERE, false, false);
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED, PartitionLayout.SAME_SIDE,
                            Reconstruction.JOIN, ExpressionContext.WHERE),
                    null);
            vp.prepareEcstCase(testCase);
            assertTrue(vp.checkSemanticPreconditions(testCase).isValid());
            execute("DROP TABLE \"" + testCase.getLeftTable() + "\"");
            execute("CREATE TABLE \"" + testCase.getLeftTable()
                    + "\" (vp_rowid BIGINT NOT NULL, c0 BIGINT NOT NULL, c2 VARCHAR(500), PRIMARY KEY(vp_rowid,c0))");
            execute("INSERT INTO \"" + testCase.getLeftTable() + "\" SELECT vp_rowid,c0,c2 FROM \""
                    + seed.getSourceTable() + "\"");
            assertFalse(vp.checkSemanticPreconditions(testCase).isValid());
            assertTrue(vp.validateDataPreconditions(testCase).isValid(),
                    "copied values alone do not prove a rowid-only primary key");
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void singleConnectionExecutesLocallyAndReplayHasNoDistributedSetup() throws Exception {
        SQLConnection original = state.getConnection();
        execute("SET vectorize='off'");
        EcstSeed seed = vp.createEcstSeed("base", "\"c0\"+\"c1\"", List.of("c0", "c1"), ExpressionContext.WHERE, false,
                false);
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED, PartitionLayout.CROSS_SIDE,
                            Reconstruction.JOIN, ExpressionContext.WHERE),
                    null);
            vp.prepareEcstCase(testCase);
            assertSame(original, state.getConnection());
            assertTrue(vp.checkSemanticPreconditions(testCase).isValid());
            assertTrue(vp.validateDataPreconditions(testCase).isValid());
            assertTrue(vp.compareEcstCase(testCase).isEqual());
            try (Statement statement = state.getConnection().createStatement();
                    ResultSet setting = statement.executeQuery("SHOW distsql")) {
                assertTrue(setting.next());
                assertEquals("off", setting.getString(1));
            }
            try (Statement statement = state.getConnection().createStatement();
                    ResultSet setting = statement.executeQuery("SHOW vectorize")) {
                assertTrue(setting.next());
                assertEquals("off", setting.getString(1));
            }
            List<String> replay = vp.getSourceDataReplay(testCase);
            assertTrue(replay.contains("SET distsql = E'off'"));
            assertFalse(replay.stream().anyMatch(sql -> sql.contains("SPLIT AT") || sql.contains("SCATTER")
                    || sql.contains("CLUSTER SETTING") || sql.contains("allow_unsafe_internals")));
            StringBuilder runtime = new StringBuilder();
            try (Statement statement = state.getConnection().createStatement(); ResultSet rows = statement
                    .executeQuery("EXPLAIN ANALYZE (VERBOSE) " + testCase.getTransformedQuery())) {
                while (rows.next()) {
                    runtime.append(rows.getString(1)).append('\n');
                }
            }
            assertTrue(runtime.toString().contains("distribution: local"), runtime.toString());
            System.out.println(
                    "ECST single-node execution verified: same connection, local SQL, no split/scatter replay");
        } finally {
            vp.cleanupEcstSeed(seed);
        }
        assertEquals(0, helperCount());
    }

    private long helperCount() throws Exception {
        try (Statement statement = state.getConnection().createStatement(); ResultSet rows = statement.executeQuery(
                "SELECT count(*) FROM information_schema.tables WHERE table_catalog=current_database() AND table_schema=current_schema() AND table_name LIKE 'vp_ecst_crdb_%'")) {
            assertTrue(rows.next());
            return rows.getLong(1);
        }
    }

    private void execute(String sql) throws Exception {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
        }
    }
}
