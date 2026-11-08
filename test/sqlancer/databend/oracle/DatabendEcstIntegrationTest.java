package sqlancer.databend.oracle;

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
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import com.beust.jcommander.JCommander;

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
import sqlancer.databend.DatabendOptions;
import sqlancer.databend.DatabendOracleFactory;
import sqlancer.databend.DatabendProvider;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.oracle.DatabendVerticalPartitioningOracle.EcstCase;
import sqlancer.databend.oracle.DatabendVerticalPartitioningOracle.EcstSeed;

/** Isolated databases on the configured single-node server, always dropped after each acceptance test. */
@EnabledIfSystemProperty(named = "databend.ecst.live", matches = "true")
class DatabendEcstIntegrationTest {
    private Connection connection;
    private String database;
    private DatabendGlobalState state;
    private DatabendVerticalPartitioningOracle vp;

    private static Connection connect() throws SQLException {
        String url = "jdbc:mysql://" + System.getProperty("databend.ecst.host", "127.0.0.1") + ":"
                + System.getProperty("databend.ecst.port", "23307")
                + "/?useSSL=false&serverTimezone=UTC&socketTimeout=180000";
        return DriverManager.getConnection(url, System.getProperty("databend.ecst.user", "root"),
                System.getProperty("databend.ecst.password", ""));
    }

    @BeforeEach
    void setup() throws Exception {
        database = "ecst_bend_test_" + Long.toUnsignedString(System.nanoTime(), 36);
        connection = connect();
        DatabendEcstSession.prepare(connection);
        execute("CREATE DATABASE " + database);
        execute("USE " + database);
        execute("CREATE TABLE base (c0 BIGINT NULL,c1 BIGINT NULL,c2 STRING NULL) ENGINE=FUSE");
        execute("INSERT INTO base VALUES (1,2,'001'),(1,2,'1'),(NULL,2,''),(3,NULL,NULL),(-4,1,'x'),(10,1,'a ')");
        execute("CREATE TABLE base2 AS SELECT * FROM base");
        state = new DatabendGlobalState();
        MainOptions main = new MainOptions();
        JCommander.newBuilder().addObject(main).build().parse("--log-each-select", "false");
        state.setMainOptions(main);
        DatabendOptions options = new DatabendOptions();
        options.oracles = List.of(DatabendOracleFactory.ECST);
        options.ecstQueryPairs = 2;
        state.setDbmsSpecificOptions(options);
        state.setRandomly(new Randomly(37));
        state.setDatabaseName(database);
        state.setConnection(new SQLConnection(connection));
        state.setState(new StateToReproduce(database, new DatabendProvider()));
        state.updateSchema();
        vp = new DatabendVerticalPartitioningOracle(state, false);
    }

    @AfterEach
    void cleanup() throws SQLException {
        if (connection != null) {
            try {
                execute("DROP DATABASE IF EXISTS " + database);
            } finally {
                connection.close();
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
        return vp.createEcstSeed("base", "coalesce(`c0`,0)+coalesce(`c1`,0)", List.of("c0", "c1"),
                ExpressionContext.WHERE, false, false);
    }

    private static EcstTransformationConfig config(PartitionLayout layout, Reconstruction reconstruction,
            ExpressionContext context) {
        return new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED, layout, reconstruction, context);
    }

    private void valid(EcstCase testCase) throws Exception {
        vp.prepareEcstCase(testCase);
        DatabendVerticalPartitioningOracle.EcstSemanticPreconditionResult semantic = vp
                .checkSemanticPreconditions(testCase);
        DatabendVerticalPartitioningOracle.EcstSemanticPreconditionResult data = vp.validateDataPreconditions(testCase);
        DatabendVerticalPartitioningOracle.EcstComparison comparison = vp.compareEcstCase(testCase);
        assertTrue(semantic.isValid(), semantic.toString());
        assertTrue(data.isValid(), data.toString());
        assertTrue(comparison.isEqual(), comparison.getReason());
    }

    @Test
    void all63ConfigurationsKeepTheSourceOperationAndResults() throws Exception {
        int total = 0;
        for (ExpressionContext context : List.of(ExpressionContext.WHERE, ExpressionContext.SELECT,
                ExpressionContext.HAVING)) {
            EcstSeed seed = vp.createEcstSeed("base", "coalesce(`c0`,0)+coalesce(`c1`,0)", List.of("c0", "c1"), context,
                    false, false);
            try {
                for (Representation representation : List.of(Representation.VERTICAL_PARTITIONED,
                        Representation.VIEW)) {
                    for (PartitionLayout layout : PartitionLayout.values()) {
                        for (Reconstruction reconstruction : Reconstruction.values()) {
                            for (ExpressionContext embedded : ExpressionContext.values()) {
                                EcstTransformationConfig configuration = new EcstTransformationConfig(representation,
                                        layout, reconstruction, embedded);
                                if (!DatabendEcstCapabilities.isLegal(seed, configuration)) {
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
            System.out.println("Databend ECST verified: " + total + "/63 (" + context + ")");
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
            String owner = scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema='" + database
                    + "' AND table_name='" + first.getLeftTable() + "' AND column_name='c2'") == 1
                            ? first.getLeftTable() : first.getRightTable();
            execute("UPDATE `" + owner + "` SET c2='1' WHERE c2='001'");
            assertFalse(vp.validateDataPreconditions(first).isValid());
            EcstCase second = vp.applyEcstTransformation(seed, configuration, null);
            valid(second);
            execute("UPDATE `" + second.getLeftTable() + "` SET vp_rowid=1");
            assertFalse(vp.validateDataPreconditions(second).isValid());
            EcstCase third = vp.applyEcstTransformation(seed, configuration, null);
            valid(third);
            execute("ALTER TABLE `" + third.getLeftTable() + "` MODIFY COLUMN c0 STRING");
            assertFalse(vp.checkSemanticPreconditions(third).isValid());
            valid(vp.applyEcstTransformation(seed, configuration, null));
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void scanPredicateMovesIntoNativeJoinCondition() throws Exception {
        EcstSeed seed = vp.createEcstSeed("base", "`c0`+`c1`", List.of("c0", "c1"), ExpressionContext.WHERE, false,
                false, "`c0`>`c1`");
        try {
            EcstTarget target = new EcstTarget("operator", EcstContextDimension.OPERATOR, "SCAN_FILTER",
                    "JOIN_CONDITION");
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    config(PartitionLayout.CROSS_SIDE, Reconstruction.JOIN, ExpressionContext.JOIN_ON), target);
            valid(testCase);
            sqlancer.common.oracle.ecst.EcstObservation observation = new DatabendExecutionContextAnalyzer(state)
                    .analyze(testCase, target);
            assertEquals(EcstShiftStatus.SUCCESS, observation.getStatus(),
                    observation.getOriginalPlan() + observation.getTransformedPlan());
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void actualFusePruningCanChangeInBothDirections() throws Exception {
        execute("CREATE TABLE selective AS SELECT CASE WHEN number%2=0 THEN cast(number AS BIGINT) ELSE -cast(number AS BIGINT) END AS c0 FROM numbers(8192)");
        for (boolean sorted : List.of(false, true)) {
            EcstSeed seed = vp.createEcstSeed("selective", "`c0`", List.of("c0"), ExpressionContext.WHERE, sorted,
                    false);
            try {
                EcstTarget target = new EcstTarget("access", EcstContextDimension.ACCESS_PATH,
                        sorted ? "PARTITION_PRUNED" : "TABLE_SCAN", sorted ? "TABLE_SCAN" : "PARTITION_PRUNED");
                EcstCase testCase = vp.applyEcstTransformation(seed,
                        config(PartitionLayout.SAME_SIDE, Reconstruction.JOIN, ExpressionContext.WHERE), target);
                valid(testCase);
                sqlancer.common.oracle.ecst.EcstObservation observation = new DatabendExecutionContextAnalyzer(state)
                        .analyze(testCase, target);
                assertEquals(EcstShiftStatus.SUCCESS, observation.getStatus(),
                        observation.getOriginalPlan() + observation.getTransformedPlan());
                assertEquals(4095, vp.compareEcstCase(testCase).getOriginalResultCount());
            } finally {
                vp.cleanupEcstSeed(seed);
            }
        }
    }

    @Test
    void richReplayIsIndependentOfOriginalTablesAndConnection() throws Exception {
        execute("CREATE TABLE rich (c0 BIGINT,c1 BIGINT,c2 STRING,c3 BINARY,c4 ARRAY(BIGINT),c5 MAP(STRING,BIGINT),c6 TUPLE(BIGINT,STRING),c7 VARIANT,c8 DECIMAL(38,4),c9 TIMESTAMP,c10 BITMAP,c11 GEOMETRY)");
        execute("INSERT INTO rich VALUES (1,2,cast(from_hex('3030310027') AS STRING),from_hex('00ff'),[1,NULL,2],map(['a,b','q=x'],[1,NULL]),tuple(7,'q''x'),parse_json('{\"z\":1,\"a\":null}'),123456789012345678901234567890.1234,'2026-11-08 01:02:03.123456',to_bitmap('1,2'),st_setsrid(to_geometry('POINT(1 2)'),4326))");
        EcstSeed seed = vp.createEcstSeed("rich", "`c0`+`c1`", List.of("c0", "c1"), ExpressionContext.SELECT, false,
                false);
        EcstCase testCase = vp.applyEcstTransformation(seed,
                config(PartitionLayout.CROSS_SIDE, Reconstruction.CTE, ExpressionContext.INNER_SELECT), null);
        valid(testCase);
        List<String> replay = vp.getSourceDataReplay(testCase);
        vp.cleanupEcstSeed(seed);
        execute("DROP TABLE rich");
        String replayDatabase = database + "_replay";
        try (Connection fresh = connect(); Statement statement = fresh.createStatement()) {
            statement.execute("CREATE DATABASE " + replayDatabase);
            statement.execute("USE " + replayDatabase);
            try {
                for (String sql : replay) {
                    statement.execute(sql);
                }
                try (ResultSet rows = statement.executeQuery(
                        "SELECT hex(c2),hex(c3),to_string(to_variant(c4)),to_string(to_variant(c5)),to_string(to_variant(c6)),to_string(c7),to_string(c8),to_string(c9),to_string(c10),st_astext(c11),st_srid(c11) FROM `"
                                + seed.getSourceTable() + "`")) {
                    assertTrue(rows.next());
                    assertEquals("3030310027", rows.getString(1));
                    assertEquals("00ff", rows.getString(2));
                    com.fasterxml.jackson.databind.ObjectMapper json = new com.fasterxml.jackson.databind.ObjectMapper();
                    assertEquals(json.readTree("[1,null,2]"), json.readTree(rows.getString(3)));
                    assertEquals(json.readTree("{\"a,b\":1,\"q=x\":null}"), json.readTree(rows.getString(4)));
                    assertEquals(json.readTree("{\"1\":7,\"2\":\"q'x\"}"), json.readTree(rows.getString(5)));
                    assertEquals(json.readTree("{\"z\":1,\"a\":null}"), json.readTree(rows.getString(6)));
                    assertEquals("123456789012345678901234567890.1234", rows.getString(7));
                    assertEquals("2026-11-08 01:02:03.123456", rows.getString(8));
                    assertEquals("1,2", rows.getString(9));
                    assertEquals("POINT(1 2)", rows.getString(10));
                    assertEquals(4326, rows.getInt(11));
                }
                try (ResultSet original = statement.executeQuery(testCase.getOriginalQuery())) {
                    assertTrue(original.next());
                    assertEquals(3, original.getLong("ecst_value"));
                }
                try (ResultSet transformed = statement.executeQuery(testCase.getTransformedQuery())) {
                    assertTrue(transformed.next());
                    assertEquals(3, transformed.getLong("ecst_value"));
                }
            } finally {
                statement.execute("DROP DATABASE " + replayDatabase);
            }
        }
    }

    @Test
    void emptyResultsStillCompareNativeOutputTypes() throws Exception {
        execute("CREATE TABLE empty_base(c0 BIGINT)");
        EcstSeed seed = vp.createEcstSeed("empty_base", "`c0`", List.of("c0"), ExpressionContext.SELECT, false, false);
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    config(PartitionLayout.SAME_SIDE, Reconstruction.JOIN, ExpressionContext.SELECT), null);
            valid(testCase);
            assertEquals(0, vp.compareEcstCase(testCase).getOriginalResultCount());
            execute("ALTER TABLE `" + testCase.getLeftTable() + "` MODIFY COLUMN c0 STRING");
            assertFalse(vp.compareEcstCase(testCase).isEqual());
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void incomingOptimizerSettingsAndUnexpectedErrorsArePreserved() throws Exception {
        execute("SET disable_join_reorder=1");
        EcstSeed seed = seed();
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    config(PartitionLayout.CROSS_SIDE, Reconstruction.JOIN, ExpressionContext.WHERE), null);
            valid(testCase);
            assertEquals(1, scalar("SELECT to_uint64(value) FROM system.settings WHERE name='disable_join_reorder'"));
            assertThrows(SQLException.class, () -> new DatabendExecutionContextAnalyzer(state)
                    .extractContext("SELECT ecst_no_such_function(c0) FROM base", seed.getOperation(), Set.of("base")));
            EcstSeed bad = vp.createEcstSeed("base", "ecst_no_such_function(`c0`)", List.of("c0"),
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
    void fullOracleComparesAndCleansHelpers() throws Exception {
        DatabendECSTOracle oracle = new DatabendECSTOracle(state);
        oracle.check();
        assertEquals(2, oracle.getCompletedCases());
        assertEquals(0, scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema='" + database
                + "' AND table_name LIKE 'vp_ecst_bend_%'"));
    }
}
