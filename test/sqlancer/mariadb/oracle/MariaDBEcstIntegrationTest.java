package sqlancer.mariadb.oracle;

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
import sqlancer.mariadb.MariaDBOptions;
import sqlancer.mariadb.MariaDBOracleFactory;
import sqlancer.mariadb.MariaDBProvider;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.oracle.MariaDBVerticalPartitioningOracle.EcstCase;
import sqlancer.mariadb.oracle.MariaDBVerticalPartitioningOracle.EcstSeed;

/** Isolated databases on the configured single-node server, always dropped after each acceptance test. */
@EnabledIfSystemProperty(named = "mariadb.ecst.live", matches = "true")
class MariaDBEcstIntegrationTest {
    private Connection connection;
    private String database;
    private MariaDBGlobalState state;
    private MariaDBVerticalPartitioningOracle vp;

    private static Connection connect() throws SQLException {
        String url = "jdbc:mariadb://" + System.getProperty("mariadb.ecst.host", "127.0.0.1") + ":"
                + System.getProperty("mariadb.ecst.port", "24308")
                + "/?useSSL=false&serverTimezone=UTC&socketTimeout=180000";
        return DriverManager.getConnection(url, System.getProperty("mariadb.ecst.user", "root"),
                System.getProperty("mariadb.ecst.password", ""));
    }

    @BeforeEach
    void setup() throws Exception {
        database = "ecst_maria_test_" + Long.toUnsignedString(System.nanoTime(), 36);
        connection = connect();
        MariaDBEcstSession.prepare(connection);
        execute("CREATE DATABASE " + database);
        execute("USE " + database);
        execute("CREATE TABLE base (c0 BIGINT NULL,c1 BIGINT NULL,c2 VARCHAR(50) COLLATE utf8mb4_bin NULL) ENGINE=InnoDB");
        execute("INSERT INTO base VALUES (1,2,'001'),(1,2,'1'),(NULL,2,''),(3,NULL,NULL),(-4,1,'x'),(10,1,'a ')");
        execute("CREATE TABLE base2 AS SELECT * FROM base");
        state = new MariaDBGlobalState();
        MainOptions main = new MainOptions();
        JCommander.newBuilder().addObject(main).build().parse("--log-each-select", "false");
        state.setMainOptions(main);
        MariaDBOptions options = new MariaDBOptions();
        options.oracles = List.of(MariaDBOracleFactory.ECST);
        options.ecstQueryPairs = 2;
        state.setDbmsSpecificOptions(options);
        state.setRandomly(new Randomly(37));
        state.setDatabaseName(database);
        state.setConnection(new SQLConnection(connection));
        state.setState(new StateToReproduce(database, new MariaDBProvider()));
        state.updateSchema();
        state.getVerticalPartitioningProtectedTables().addAll(Set.of("base", "base2"));
        vp = new MariaDBVerticalPartitioningOracle(state);
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
        MariaDBVerticalPartitioningOracle.EcstSemanticPreconditionResult semantic = vp
                .checkSemanticPreconditions(testCase);
        MariaDBVerticalPartitioningOracle.EcstSemanticPreconditionResult data = vp.validateDataPreconditions(testCase);
        MariaDBVerticalPartitioningOracle.EcstComparison comparison = vp.compareEcstCase(testCase);
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
                                if (!MariaDBEcstCapabilities.isLegal(seed, configuration)) {
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
            System.out.println("MariaDB ECST verified: " + total + "/63 (" + context + ")");
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
            execute("ALTER TABLE `" + second.getLeftTable()
                    + "` DROP PRIMARY KEY, ADD UNIQUE KEY ecst_rowid_uq(vp_rowid)");
            assertFalse(vp.checkSemanticPreconditions(second).isValid());
            EcstCase third = vp.applyEcstTransformation(seed, configuration, null);
            valid(third);
            execute("ALTER TABLE `" + third.getLeftTable() + "` MODIFY COLUMN c0 VARCHAR(100)");
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
            sqlancer.common.oracle.ecst.EcstObservation observation = new MariaDBExecutionContextAnalyzer(state)
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
        execute("INSERT INTO selective SELECT CASE WHEN seq=1 THEN 1 ELSE -seq END FROM seq_1_to_4096");
        state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows = 0;
        for (boolean indexed : List.of(false, true)) {
            EcstSeed seed = vp.createEcstSeed("selective", "`c0`", List.of("c0"), ExpressionContext.WHERE, indexed,
                    false);
            try {
                EcstTarget target = new EcstTarget("access", EcstContextDimension.ACCESS_PATH,
                        indexed ? "INDEX_RANGE" : "TABLE_SCAN", indexed ? "TABLE_SCAN" : "INDEX_RANGE");
                EcstCase testCase = vp.applyEcstTransformation(seed,
                        config(PartitionLayout.SAME_SIDE, Reconstruction.JOIN, ExpressionContext.WHERE), target);
                valid(testCase);
                sqlancer.common.oracle.ecst.EcstObservation observation = new MariaDBExecutionContextAnalyzer(state)
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
            EcstSeed seed = vp.createEcstSeed("base", "`c0`+`c1`", List.of("c0", "c1"), ExpressionContext.WHERE, false,
                    retained);
            try {
                EcstTarget target = new EcstTarget("boundary", EcstContextDimension.BOUNDARY,
                        retained ? "MATERIALIZED" : "MERGED", retained ? "MERGED" : "MATERIALIZED");
                EcstCase testCase = vp.applyEcstTransformation(seed, config(PartitionLayout.CROSS_SIDE,
                        retained ? Reconstruction.JOIN : Reconstruction.CTE, ExpressionContext.WHERE), target);
                valid(testCase);
                sqlancer.common.oracle.ecst.EcstObservation observation = new MariaDBExecutionContextAnalyzer(state)
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
        execute("CREATE TABLE rich(c0 BIGINT,c1 BIGINT,c2 VARCHAR(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,c3 VARBINARY(10),c4 BIT(64),c5 JSON,c6 DECIMAL(40,6),c7 DATETIME(6),c8 INET6,c9 UUID,c10 ENUM('a','b'),c11 SET('a','b'),c12 BIGINT UNSIGNED,c13 VARCHAR(10) CHARACTER SET latin1 COLLATE latin1_bin)");
        execute("INSERT INTO rich VALUES (1,2,UNHEX('3030310027'),UNHEX('00ff'),b'1000000000000000000000000000000000000000000000000000000000000001','{\"z\":1,\"a\":null}',123456789012345678901234567890.123456,'2026-11-08 01:02:03.123456','2001:db8::1','550e8400-e29b-41d4-a716-446655440000',2,3,18446744073709551615,UNHEX('e9'))");
        EcstSeed seed = vp.createEcstSeed("rich", "`c0`+`c1`", List.of("c0", "c1"), ExpressionContext.SELECT, false,
                false);
        EcstCase testCase = vp.applyEcstTransformation(seed,
                config(PartitionLayout.CROSS_SIDE, Reconstruction.CTE, ExpressionContext.INNER_SELECT), null);
        valid(testCase);
        String jsonOwner = scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema='" + database
                + "' AND table_name='" + testCase.getLeftTable() + "' AND column_name='c5'") == 1
                        ? testCase.getLeftTable() : testCase.getRightTable();
        execute("UPDATE `" + jsonOwner + "` SET c5=CONCAT(c5,' ')");
        assertFalse(vp.validateDataPreconditions(testCase).isValid());
        testCase = vp.applyEcstTransformation(seed,
                config(PartitionLayout.CROSS_SIDE, Reconstruction.CTE, ExpressionContext.INNER_SELECT), null);
        valid(testCase);
        List<String> replay = vp.getSourceDataReplay(testCase);
        vp.cleanupEcstSeed(seed);
        execute("DROP TABLE rich");
        String freshDatabase = database + "_replay";
        try (Connection fresh = connect(); Statement statement = fresh.createStatement()) {
            statement.execute("CREATE DATABASE " + freshDatabase);
            statement.execute("USE " + freshDatabase);
            try {
                for (String sql : replay) {
                    statement.execute(sql);
                }
                try (ResultSet rows = statement.executeQuery(
                        "SELECT HEX(c2),HEX(c3),HEX(c4),c5,CAST(c6 AS CHAR),CAST(c7 AS CHAR),CAST(c8 AS CHAR),CAST(c9 AS CHAR),CAST(c10 AS UNSIGNED),CAST(c11 AS UNSIGNED),CAST(c12 AS CHAR),HEX(c13) FROM `"
                                + seed.getSourceTable() + "`")) {
                    assertTrue(rows.next());
                    assertEquals("3030310027", rows.getString(1));
                    assertEquals("00FF", rows.getString(2));
                    assertEquals("8000000000000001", rows.getString(3));
                    assertEquals("{\"z\":1,\"a\":null}", rows.getString(4));
                    assertEquals("123456789012345678901234567890.123456", rows.getString(5));
                    assertEquals("2026-11-08 01:02:03.123456", rows.getString(6));
                    assertEquals("2001:db8::1", rows.getString(7));
                    assertEquals("550e8400-e29b-41d4-a716-446655440000", rows.getString(8));
                    assertEquals(2, rows.getInt(9));
                    assertEquals(3, rows.getInt(10));
                    assertEquals("18446744073709551615", rows.getString(11));
                    assertEquals("E9", rows.getString(12));
                }
                try (ResultSet original = statement.executeQuery(testCase.getOriginalQuery())) {
                    assertTrue(original.next());
                    assertEquals(3, original.getInt("ecst_value"));
                }
                try (ResultSet transformed = statement.executeQuery(testCase.getTransformedQuery())) {
                    assertTrue(transformed.next());
                    assertEquals(3, transformed.getInt("ecst_value"));
                }
            } finally {
                statement.execute("DROP DATABASE " + freshDatabase);
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
            execute("ALTER TABLE `" + testCase.getLeftTable() + "` MODIFY COLUMN c0 VARCHAR(100)");
            assertFalse(vp.compareEcstCase(testCase).isEqual());
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void incomingOptimizerSettingsAndUnexpectedErrorsArePreserved() throws Exception {
        execute("SET SESSION optimizer_switch='derived_merge=off'");
        EcstSeed seed = seed();
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    config(PartitionLayout.CROSS_SIDE, Reconstruction.JOIN, ExpressionContext.WHERE), null);
            valid(testCase);
            try (Statement statement = connection.createStatement();
                    ResultSet rows = statement.executeQuery("SELECT @@optimizer_switch")) {
                assertTrue(rows.next());
                assertTrue(rows.getString(1).contains("derived_merge=off"));
            }
            assertThrows(SQLException.class, () -> new MariaDBExecutionContextAnalyzer(state)
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
        MariaDBECSTOracle oracle = new MariaDBECSTOracle(state);
        oracle.check();
        assertEquals(2, oracle.getCompletedCases());
        assertEquals(0, scalar("SELECT count(*) FROM information_schema.tables WHERE table_schema='" + database
                + "' AND table_name LIKE 'vp_ecst_maria_%'"));
    }
}
