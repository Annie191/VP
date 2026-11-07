package sqlancer.clickhouse.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.beust.jcommander.JCommander;

import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.StateToReproduce;
import sqlancer.clickhouse.ClickHouseOptions;
import sqlancer.clickhouse.ClickHouseOracleFactory;
import sqlancer.clickhouse.ClickHouseProvider;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.oracle.ClickHouseVerticalPartitioningOracle.EcstCase;
import sqlancer.clickhouse.oracle.ClickHouseVerticalPartitioningOracle.EcstSeed;
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.common.oracle.ecst.EcstTransformationConfig;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.ExpressionContext;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.PartitionLayout;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.Reconstruction;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.Representation;

/** Configured endpoints must pass; only an absent endpoint skips live acceptance. */
class ClickHouseEcstIntegrationTest {
    private String database;
    private ClickHouseGlobalState state;
    private ClickHouseVerticalPartitioningOracle vp;

    private static String url() {
        return System.getProperty("clickhouse.ecst.test.url");
    }

    private static String user() {
        return System.getProperty("clickhouse.ecst.test.user", "default");
    }

    private static String password() {
        return System.getProperty("clickhouse.ecst.test.password", "");
    }

    private static Connection connect(String database) throws Exception {
        String url = url();
        int query = url.indexOf('?');
        String options = query < 0 ? "" : url.substring(query);
        String base = query < 0 ? url : url.substring(0, query);
        return DriverManager.getConnection(base.substring(0, base.lastIndexOf('/') + 1) + database + options, user(),
                password());
    }

    @BeforeEach
    void setup() throws Exception {
        Assumptions.assumeTrue(url() != null, "set clickhouse.ecst.test.url for single-node acceptance");
        database = "ecst_ch_test_" + Long.toUnsignedString(System.nanoTime(), 36) + "ECST";
        try (Connection admin = connect("default"); Statement s = admin.createStatement()) {
            s.execute("CREATE DATABASE " + database);
        }
        state = new ClickHouseGlobalState();
        MainOptions main = new MainOptions();
        JCommander.newBuilder().addObject(main).build().parse("--log-each-select", "false", "--username", user(),
                "--password", password());
        state.setMainOptions(main);
        ClickHouseOptions options = new ClickHouseOptions();
        options.oracle = List.of(ClickHouseOracleFactory.ECST);
        options.ecstQueryPairs = 2;
        state.setDbmsSpecificOptions(options);
        state.setClickHouseOptions(options);
        state.setRandomly(new Randomly(37));
        state.setDatabaseName(database.substring(0, database.length() - "ECST".length()));
        state.setConnection(new SQLConnection(connect(database)));
        state.setState(new StateToReproduce(database, new ClickHouseProvider()));
        execute("CREATE TABLE base (c0 Nullable(Int64),c1 Nullable(Int64),c2 Nullable(String)) ENGINE=Memory");
        execute("INSERT INTO base VALUES (1,2,'001'),(1,2,'1'),(NULL,2,''),(3,NULL,NULL),(-4,1,'x'),(0,0,'0')");
        state.updateSchema();
        vp = new ClickHouseVerticalPartitioningOracle(state, false);
    }

    @AfterEach
    void cleanup() throws Exception {
        if (database != null) {
            if (state != null && state.getConnection() != null) {
                state.getConnection().close();
            }
            try (Connection c = connect("default"); Statement s = c.createStatement()) {
                s.execute("DROP DATABASE IF EXISTS " + database + "_replay SYNC");
                s.execute("DROP DATABASE IF EXISTS " + database + " SYNC");
            }
        }
    }

    private void execute(String sql) throws Exception {
        try (Statement s = state.getConnection().createStatement()) {
            s.execute(sql);
        }
    }

    private long scalar(String sql) throws Exception {
        try (Statement s = state.getConnection().createStatement(); ResultSet r = s.executeQuery(sql)) {
            assertTrue(r.next());
            return r.getLong(1);
        }
    }

    private EcstTransformationConfig config(PartitionLayout layout, Reconstruction reconstruction,
            ExpressionContext context) {
        return new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED, layout, reconstruction, context);
    }

    @Test
    void all63ConfigurationsPreserveNullsDuplicatesAndImmutableOriginal() throws Exception {
        int checked = 0;
        for (ExpressionContext context : List.of(ExpressionContext.WHERE, ExpressionContext.SELECT,
                ExpressionContext.HAVING)) {
            EcstSeed seed = vp.createEcstSeed("base", "plus(ifNull(`c0`,0),ifNull(`c1`,0))", List.of("c0", "c1"),
                    context, false, false);
            try {
                String original = seed.getOriginalQuery();
                for (Representation rep : Representation.values()) {
                    for (PartitionLayout layout : PartitionLayout.values()) {
                        for (Reconstruction reconstruction : Reconstruction.values()) {
                            for (ExpressionContext embedded : ExpressionContext.values()) {
                                EcstTransformationConfig config = new EcstTransformationConfig(rep, layout,
                                        reconstruction, embedded);
                                if (!ClickHouseEcstCapabilities.isLegal(seed, config)) {
                                    continue;
                                }
                                EcstCase testCase = vp.applyEcstTransformation(seed, config, null);
                                vp.prepareEcstCase(testCase);
                                if (rep == Representation.VIEW) {
                                    assertTrue(testCase.getTransformedQuery().contains(testCase.getViewName()));
                                    assertEquals(1, scalar(
                                            "SELECT count() FROM system.tables WHERE database=currentDatabase() AND name='"
                                                    + testCase.getViewName() + "' AND engine='View'"));
                                }
                                assertTrue(vp.checkSemanticPreconditions(testCase).isValid(),
                                        config + ":" + vp.checkSemanticPreconditions(testCase));
                                assertTrue(vp.validateDataPreconditions(testCase).isValid(),
                                        config + ":" + vp.validateDataPreconditions(testCase));
                                assertEquals(original, testCase.getOriginalQuery());
                                assertSame(seed.getOperation(), testCase.getOperation());
                                assertTrue(vp.compareEcstCase(testCase).isEqual(),
                                        config + ":" + vp.compareEcstCase(testCase).getReason());
                                checked++;
                            }
                        }
                    }
                }
            } finally {
                vp.cleanupEcstSeed(seed);
            }
            System.out.println("ClickHouse ECST configurations verified: " + checked + "/63 (" + context + ")");
        }
        assertEquals(63, checked);
        assertEquals(0, scalar(
                "SELECT count() FROM system.tables WHERE database=currentDatabase() AND startsWith(name,'vp_ecst_ch_')"));
    }

    @Test
    void nativeBoundaryMigrationWorksInBothDirections() throws Exception {
        ClickHouseExecutionContextAnalyzer analyzer = new ClickHouseExecutionContextAnalyzer(state);
        for (boolean retained : List.of(false, true)) {
            EcstSeed seed = vp.createEcstSeed("base", "plus(`c0`,`c1`)", List.of("c0", "c1"), ExpressionContext.WHERE,
                    false, retained);
            try {
                EcstCase testCase = vp.applyEcstTransformation(seed, config(PartitionLayout.CROSS_SIDE,
                        retained ? Reconstruction.JOIN : Reconstruction.CTE, ExpressionContext.WHERE), null);
                vp.prepareEcstCase(testCase);
                EcstTarget target = new EcstTarget("b", EcstContextDimension.BOUNDARY,
                        retained ? "MATERIALIZED" : "MERGED", retained ? "MERGED" : "MATERIALIZED");
                assertEquals(EcstShiftStatus.SUCCESS, analyzer.analyze(testCase, target).getStatus());
                assertTrue(vp.compareEcstCase(testCase).isEqual());
            } finally {
                vp.cleanupEcstSeed(seed);
            }
        }
    }

    @Test
    void corruptionInvalidatesReuseAndText001DiffersFrom1() throws Exception {
        EcstSeed seed = vp.createEcstSeed("base", "`c2`", List.of("c2"), ExpressionContext.SELECT, false, false);
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    config(PartitionLayout.SAME_SIDE, Reconstruction.JOIN, ExpressionContext.SELECT), null);
            vp.prepareEcstCase(testCase);
            execute("ALTER TABLE `" + testCase.getLeftTable()
                    + "` UPDATE c2='1' WHERE c2='001' SETTINGS mutations_sync=1");
            assertFalse(vp.validateDataPreconditions(testCase).isValid());
            assertFalse(vp.compareEcstCase(testCase).isEqual());
            execute("ALTER TABLE `" + testCase.getLeftTable() + "` MODIFY COLUMN c2 Nullable(FixedString(8))");
            assertFalse(vp.checkSemanticPreconditions(testCase).isValid());
            EcstCase retry = vp.applyEcstTransformation(seed, testCase.getAppliedConfig(), null);
            vp.prepareEcstCase(retry);
            assertTrue(vp.checkSemanticPreconditions(retry).isValid());
            assertTrue(vp.validateDataPreconditions(retry).isValid());
            assertTrue(vp.compareEcstCase(retry).isEqual());
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void duplicateRowIdsAreInvalidDespiteMergeTreePrimaryKey() throws Exception {
        EcstSeed seed = vp.createEcstSeed("base", "`c0`", List.of("c0"), ExpressionContext.WHERE, false, false);
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    config(PartitionLayout.SAME_SIDE, Reconstruction.JOIN, ExpressionContext.WHERE), null);
            vp.prepareEcstCase(testCase);
            execute("INSERT INTO `" + testCase.getLeftTable() + "` SELECT * FROM `" + testCase.getLeftTable()
                    + "` LIMIT 1");
            assertFalse(vp.validateDataPreconditions(testCase).isValid());
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void finalRetryReplayPreservesRichNativeValuesInIndependentDatabase() throws Exception {
        execute("CREATE TABLE rich (c0 Int64,c1 Int64,payload String,f FixedString(4),a Array(Nullable(Int64)),m Map(String,Int64),t Tuple(Int64,String),d Decimal(20,4),ts DateTime64(6,'UTC')) ENGINE=Memory");
        execute("INSERT INTO rich VALUES (1,2,unhex('00ff'),'001',[1,NULL,3],map('k',2),(3,'x'),1.2000,'2026-11-08 00:00:00.123456'),(1,2,'001','x',[],map(),(0,''),0,'2026-11-08 00:00:00')");
        EcstSeed seed = vp.createEcstSeed("rich", "plus(`c0`,`c1`)", List.of("c0", "c1"), ExpressionContext.SELECT,
                true, false);
        try {
            vp.prepareEcstCase(vp.applyEcstTransformation(seed,
                    config(PartitionLayout.SAME_SIDE, Reconstruction.VIEW, ExpressionContext.SELECT), null));
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    config(PartitionLayout.CROSS_SIDE, Reconstruction.VIEW, ExpressionContext.INNER_SELECT), null);
            vp.prepareEcstCase(testCase);
            List<String> replay = vp.getSourceDataReplay(testCase);
            assertEquals(3, replay.stream().filter(s -> s.startsWith("CREATE TABLE")).count());
            assertFalse(replay.stream().anyMatch(
                    s -> s.contains("ON CLUSTER") || s.contains("Distributed(") || s.contains("FROM `rich`")));
            try (Connection c = connect("default"); Statement s = c.createStatement()) {
                s.execute("CREATE DATABASE " + database + "_replay");
            }
            vp.cleanupEcstSeed(seed);
            SQLConnection original = state.getConnection();
            try (Connection c = connect(database + "_replay")) {
                state.setConnection(new SQLConnection(c));
                for (String sql : replay) {
                    execute(sql);
                }
                assertTrue(vp.checkSemanticPreconditions(testCase).isValid(),
                        vp.checkSemanticPreconditions(testCase).toString());
                assertTrue(vp.validateDataPreconditions(testCase).isValid(),
                        vp.validateDataPreconditions(testCase).toString());
                assertTrue(vp.compareEcstCase(testCase).isEqual(), vp.compareEcstCase(testCase).getReason());
            } finally {
                state.setConnection(original);
            }
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void emptyDataStillHasComparableOutputSchema() throws Exception {
        execute("TRUNCATE TABLE base");
        EcstSeed seed = vp.createEcstSeed("base", "`c0`", List.of("c0"), ExpressionContext.SELECT, false, false);
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    config(PartitionLayout.SAME_SIDE, Reconstruction.CTE, ExpressionContext.INNER_SELECT), null);
            vp.prepareEcstCase(testCase);
            assertTrue(vp.checkSemanticPreconditions(testCase).isValid());
            assertTrue(vp.validateDataPreconditions(testCase).isValid());
            assertTrue(vp.compareEcstCase(testCase).isEqual());
            assertEquals(0, vp.compareEcstCase(testCase).getOriginalResultCount());
            execute("ALTER TABLE `" + testCase.getLeftTable() + "` MODIFY COLUMN c0 Nullable(Int32)");
            assertFalse(vp.compareEcstCase(testCase).isEqual(), "empty output still requires equal result types");
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void binaryStringsRemainDistinctEvenWhenUtf8DecodingWouldLoseBytes() throws Exception {
        execute("CREATE TABLE binary_data (c0 String,c1 Int64) ENGINE=Memory");
        execute("INSERT INTO binary_data VALUES (unhex('00ff'),1),(unhex('00fe'),2),('001',3),('1',4)");
        EcstSeed seed = vp.createEcstSeed("binary_data", "`c0`", List.of("c0"), ExpressionContext.SELECT, false, false);
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    config(PartitionLayout.SAME_SIDE, Reconstruction.JOIN, ExpressionContext.SELECT), null);
            vp.prepareEcstCase(testCase);
            assertTrue(vp.compareEcstCase(testCase).isEqual());
            execute("ALTER TABLE `" + testCase.getLeftTable()
                    + "` UPDATE c0=unhex('00fe') WHERE hex(c0)='00FF' SETTINGS mutations_sync=1");
            assertFalse(vp.validateDataPreconditions(testCase).isValid());
            assertFalse(vp.compareEcstCase(testCase).isEqual());
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void unexpectedQueryErrorsBypassLegacyPatternSuppression() throws Exception {
        state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns = true;
        EcstSeed seed = vp.createEcstSeed("base", "unknown_ecst_function(`c0`)", List.of("c0"),
                ExpressionContext.SELECT, false, false);
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    config(PartitionLayout.SAME_SIDE, Reconstruction.JOIN, ExpressionContext.SELECT), null);
            vp.prepareEcstCase(testCase);
            assertThrows(java.sql.SQLException.class, () -> vp.compareEcstCase(testCase));
            assertThrows(java.sql.SQLException.class,
                    () -> new ClickHouseExecutionContextAnalyzer(state).extractContext(seed.getOriginalQuery(),
                            seed.getOperation(), java.util.Set.of(seed.getSourceTable())));
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void fullOracleCompletesOnOneConnectionAndCleansHelpers() throws Exception {
        execute("CREATE TABLE vp_base_one ENGINE=Memory AS SELECT * FROM base");
        execute("CREATE TABLE vp_base_two ENGINE=Memory AS SELECT * FROM base");
        state.updateSchema();
        SQLConnection connection = state.getConnection();
        ClickHouseECSTOracle oracle = new ClickHouseECSTOracle(state);
        oracle.check();
        assertEquals(2, oracle.getCompletedCases());
        assertTrue(oracle.getHistory().getTotalTrials() >= 2);
        assertSame(connection, state.getConnection());
        assertEquals(0, scalar(
                "SELECT count() FROM system.tables WHERE database=currentDatabase() AND startsWith(name,'vp_ecst_ch_')"));
    }
}
