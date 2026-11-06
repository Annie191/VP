package sqlancer.oceanbase.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
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
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.common.oracle.ecst.EcstTransformationConfig;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.ExpressionContext;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.PartitionLayout;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.Reconstruction;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.Representation;
import sqlancer.oceanbase.OceanBaseGlobalState;
import sqlancer.oceanbase.OceanBaseOracleFactory;
import sqlancer.oceanbase.OceanBaseOptions;
import sqlancer.oceanbase.OceanBaseProvider;
import sqlancer.oceanbase.oracle.OceanBaseVerticalPartitioningOracle.EcstCase;
import sqlancer.oceanbase.oracle.OceanBaseVerticalPartitioningOracle.EcstSeed;

/** Opt-in live acceptance; when an endpoint is configured, SQL failures must fail the test. */
class OceanBaseEcstIntegrationTest {
    private Connection connection;
    private String database;
    private OceanBaseGlobalState state;
    private OceanBaseVerticalPartitioningOracle vp;

    @BeforeEach
    void connect() throws Exception {
        String url = System.getProperty("oceanbase.ecst.test.url");
        Assumptions.assumeTrue(url != null, "set oceanbase.ecst.test.url to run live OceanBase ECST acceptance");
        connection = DriverManager.getConnection(url, System.getProperty("oceanbase.ecst.test.user", "root@test"),
                System.getProperty("oceanbase.ecst.test.password", ""));
        execute("SET SESSION ob_query_timeout=1000000000");
        execute("SET SESSION ob_trx_timeout=1000000000");
        database = "ecst_ob_test_" + Long.toUnsignedString(System.nanoTime(), 36);
        execute("CREATE DATABASE `" + database + "`");
        connection.setCatalog(database);
        execute("CREATE TABLE base (c0 INT, c1 INT, c2 VARCHAR(100))");
        execute("INSERT INTO base VALUES (1,2,'001'),(1,2,'1'),(NULL,2,''),(3,NULL,NULL),(-4,1,'x'),(0,0,'0')");
        execute("CREATE TABLE base2 LIKE base");
        execute("INSERT INTO base2 SELECT * FROM base");
        MainOptions main = new MainOptions();
        JCommander.newBuilder().addObject(main).build().parse("--log-each-select", "false");
        state = new OceanBaseGlobalState();
        state.setMainOptions(main);
        OceanBaseOptions options = new OceanBaseOptions();
        options.oracles = List.of(OceanBaseOracleFactory.ECST);
        options.ecstQueryPairs = 2;
        state.setDbmsSpecificOptions(options);
        state.setRandomly(new Randomly(37));
        state.setDatabaseName(database);
        state.setConnection(new SQLConnection(connection));
        state.setState(new StateToReproduce(database, new OceanBaseProvider()));
        state.updateSchema();
        vp = new OceanBaseVerticalPartitioningOracle(state, false);
    }

    @AfterEach
    void cleanup() throws Exception {
        if (connection != null) {
            try {
                execute("DROP DATABASE IF EXISTS `" + database + "_replay`");
                execute("DROP DATABASE IF EXISTS `" + database + "`");
            } finally {
                connection.close();
            }
        }
    }

    @Test
    void controlledConfigurationsPreserveNullsDuplicatesGroupingAndOriginalOperation() throws Exception {
        int checked = 0;
        for (ExpressionContext context : List.of(ExpressionContext.WHERE, ExpressionContext.SELECT,
                ExpressionContext.HAVING)) {
            EcstSeed seed = vp.createEcstSeed("base", "COALESCE(`c0`, 0) + COALESCE(`c1`, 0)", List.of("c0", "c1"),
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
                                if (!OceanBaseEcstCapabilities.isLegal(seed, config)) {
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
            System.out.println("OceanBase ECST configurations verified: " + checked + "/63 (" + context + ")");
        }
        assertEquals(63, checked, "expected the full legal OceanBase controlled configuration set");
        assertEquals(0, helperCount());
    }

    @Test
    void retainedViewBoundaryIsObservedInBothDirections() throws Exception {
        OceanBaseExecutionContextAnalyzer analyzer = new OceanBaseExecutionContextAnalyzer(state);
        for (boolean materialized : List.of(false, true)) {
            EcstSeed seed = vp.createEcstSeed("base", "`c0` + `c1`", List.of("c0", "c1"), ExpressionContext.WHERE,
                    false, materialized);
            try {
                Reconstruction reconstruction = materialized ? Reconstruction.JOIN : Reconstruction.DERIVED_TABLE;
                EcstCase testCase = vp.applyEcstTransformation(seed,
                        new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED, PartitionLayout.CROSS_SIDE,
                                reconstruction, ExpressionContext.WHERE),
                        null);
                vp.prepareEcstCase(testCase);
                EcstTarget target = new EcstTarget("boundary", EcstContextDimension.BOUNDARY,
                        materialized ? "RETAINED_VIEW" : "MERGED", materialized ? "MERGED" : "RETAINED_VIEW");
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
        EcstSeed seed = vp.createEcstSeed("base", "`c2`", List.of("c2"), ExpressionContext.SELECT, false, false);
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED, PartitionLayout.SAME_SIDE,
                            Reconstruction.JOIN, ExpressionContext.SELECT),
                    null);
            vp.prepareEcstCase(testCase);
            execute("UPDATE `" + testCase.getLeftTable() + "` SET c2='1' WHERE c2='001'");
            assertFalse(vp.validateDataPreconditions(testCase).isValid());
            assertFalse(vp.compareEcstCase(testCase).isEqual(), "text 001 and 1 must remain different");
            execute("ALTER TABLE `" + testCase.getLeftTable() + "` MODIFY c2 VARCHAR(20) COLLATE utf8mb4_bin");
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
        EcstSeed seed = vp.createEcstSeed("base", "`c0` + `c1`", List.of("c0", "c1"), ExpressionContext.WHERE, true,
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
            assertFalse(replay.stream().anyMatch(sql -> sql.contains("FROM `base`")));
            execute("CREATE DATABASE `" + database + "_replay`");
            vp.cleanupEcstSeed(seed);
            connection.setCatalog(database + "_replay");
            for (String sql : replay) {
                execute(sql);
            }
            assertTrue(vp.compareEcstCase(testCase).isEqual());
            connection.setCatalog(database);
        } finally {
            connection.setCatalog(database);
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void oracleCompletesComparablePairsAndCleansArtifacts() throws Exception {
        OceanBaseECSTOracle oracle = new OceanBaseECSTOracle(state);
        oracle.check();
        assertEquals(2, oracle.getCompletedCases());
        assertTrue(oracle.getHistory().getTotalTrials() >= 2);
        assertEquals(0, helperCount());
    }

    @Test
    void emptyDataStillProducesAComparablePairAndIncompatibleContextsAreFiltered() throws Exception {
        execute("DELETE FROM base");
        EcstSeed seed = vp.createEcstSeed("base", "`c0`", List.of("c0"), ExpressionContext.SELECT, false, false);
        try {
            EcstTransformationConfig invalid = new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED,
                    PartitionLayout.CROSS_SIDE, Reconstruction.JOIN, ExpressionContext.SELECT);
            assertFalse(OceanBaseEcstCapabilities.isLegal(seed, invalid));
            EcstTransformationConfig valid = new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED,
                    PartitionLayout.SAME_SIDE, Reconstruction.CTE, ExpressionContext.INNER_SELECT);
            EcstCase testCase = vp.applyEcstTransformation(seed, valid, null);
            vp.prepareEcstCase(testCase);
            assertTrue(vp.compareEcstCase(testCase).isComparable());
            assertTrue(vp.compareEcstCase(testCase).isEqual());
            assertEquals(0, vp.compareEcstCase(testCase).getOriginalResultCount());
            assertEquals(Set.of(), OceanBaseECSTOracle.candidateSets(seed,
                    new sqlancer.common.oracle.ecst.EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"))
                    .keySet());
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void characterHavingIsDeterministicAndAggregationPushdownRemainsNatural() throws Exception {
        execute("INSERT INTO base VALUES (9,9,'a'),(9,9,'a ')");
        String before;
        try (Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("SELECT @@ob_enable_aggregation_pushdown")) {
            assertTrue(rows.next());
            before = rows.getString(1);
        }
        EcstSeed seed = vp.createEcstSeed("base", "CHAR_LENGTH(`c2`)", List.of("c2"), ExpressionContext.HAVING, false,
                false);
        try {
            assertTrue(seed.getOriginalQuery().contains("GROUP BY `vp_rowid`,"));
            EcstCase testCase = vp.applyEcstTransformation(seed, new EcstTransformationConfig(Representation.VIEW,
                    PartitionLayout.SAME_SIDE, Reconstruction.VIEW, ExpressionContext.HAVING), null);
            vp.prepareEcstCase(testCase);
            assertTrue(vp.compareEcstCase(testCase).isEqual());
            try (Statement statement = connection.createStatement();
                    ResultSet rows = statement.executeQuery("SELECT @@ob_enable_aggregation_pushdown")) {
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
        EcstSeed seed = vp.createEcstSeed("base", "`c0`", List.of("c0"), ExpressionContext.WHERE, false, false);
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED, PartitionLayout.SAME_SIDE,
                            Reconstruction.JOIN, ExpressionContext.WHERE),
                    null);
            vp.prepareEcstCase(testCase);
            assertTrue(vp.checkSemanticPreconditions(testCase).isValid());
            execute("DROP TABLE `" + testCase.getLeftTable() + "`");
            execute("CREATE TABLE `" + testCase.getLeftTable()
                    + "` (vp_rowid BIGINT NOT NULL, c0 BIGINT NOT NULL, c2 VARCHAR(500), PRIMARY KEY(vp_rowid,c0))");
            execute("INSERT INTO `" + testCase.getLeftTable() + "` SELECT vp_rowid,c0,c2 FROM `" + seed.getSourceTable()
                    + "`");
            assertFalse(vp.checkSemanticPreconditions(testCase).isValid());
            assertTrue(vp.validateDataPreconditions(testCase).isValid(),
                    "copied values alone do not prove a rowid-only primary key");
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    private long helperCount() throws Exception {
        try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(
                "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE 'vp_ecst_ob_%'")) {
            assertTrue(rows.next());
            return rows.getLong(1);
        }
    }

    private void execute(String sql) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
