package sqlancer.duckdb;

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
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.beust.jcommander.JCommander;

import sqlancer.IgnoreMeException;
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
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBVerticalPartitioningOracle.EcstCase;
import sqlancer.duckdb.DuckDBVerticalPartitioningOracle.EcstSeed;

/** Acceptance against independent embedded databases using the actual JDBC engine. */
class DuckDBEcstIntegrationTest {
    private Connection connection;
    private String database;
    private DuckDBGlobalState state;
    private DuckDBVerticalPartitioningOracle vp;

    @BeforeEach
    void connect() throws Exception {
        database = "ecst_duck_test_" + Long.toUnsignedString(System.nanoTime(), 36);
        connection = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET threads=1");
            statement.execute("CREATE TABLE base (c0 BIGINT,c1 BIGINT,c2 VARCHAR)");
            statement.execute(
                    "INSERT INTO base VALUES (1,2,'001'),(1,2,'1'),(NULL,2,''),(3,NULL,NULL),(-4,1,'x'),(0,0,'0')");
            statement.execute("CREATE TABLE base2 AS SELECT * FROM base");
        }
        MainOptions main = new MainOptions();
        JCommander.newBuilder().addObject(main).build().parse("--log-each-select", "false");
        state = new DuckDBGlobalState();
        state.setMainOptions(main);
        DuckDBOptions options = new DuckDBOptions();
        options.oracles = List.of(DuckDBOracleFactory.ECST);
        options.ecstQueryPairs = 2;
        state.setDbmsSpecificOptions(options);
        state.setRandomly(new Randomly(37));
        state.setDatabaseName(database);
        state.setConnection(new SQLConnection(connection));
        state.setState(new StateToReproduce(database, new DuckDBProvider()));
        state.updateSchema();
        vp = new DuckDBVerticalPartitioningOracle(state, false);
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
        }
    }

    @Test
    void syntheticRowIdsDoNotMakeBlobOrTimestampTablesEligible() throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE base");
            statement.execute("DROP TABLE base2");
            statement.execute("CREATE TABLE blob_only (c0 BLOB)");
            statement.execute("CREATE TABLE time_only (c0 TIMESTAMP)");
            statement.execute("INSERT INTO blob_only VALUES (from_hex('00ff'))");
            statement.execute("INSERT INTO time_only VALUES (TIMESTAMP '2026-11-08 00:00:00')");
        }
        state.updateSchema();
        assertTrue(state.getSchema().getDatabaseTables().stream()
                .allMatch(table -> table.getColumns().stream().anyMatch(column -> column.isSyntheticRowId())));
        assertThrows(IgnoreMeException.class, () -> vp.createEcstSeed());
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
                                if (!DuckDBEcstCapabilities.isLegal(seed, config)) {
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
            System.out.println("DuckDB ECST configurations verified: " + checked + "/63 (" + context + ")");
        }
        assertEquals(63, checked, "expected the full legal DuckDB controlled configuration set");
        assertEquals(0, helperCount());
    }

    @Test
    void retainedViewBoundaryIsObservedInBothDirections() throws Exception {
        DuckDBExecutionContextAnalyzer analyzer = new DuckDBExecutionContextAnalyzer(state);
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
            execute("ALTER TABLE \"" + testCase.getLeftTable() + "\" ALTER COLUMN c2 SET DATA TYPE BLOB");
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
        execute("CREATE TABLE rich (c0 BIGINT,c1 BIGINT,c2 VARCHAR,payload BLOB,a BIGINT[],j JSON,t TIMESTAMP_NS,d DECIMAL(38,4),m MAP(VARCHAR,BIGINT),st STRUCT(x BIGINT,s VARCHAR))");
        execute("INSERT INTO rich SELECT c0,c1,c2,from_hex('00ff'),[1,NULL,3],'{\"x\":1}'::JSON,'2026-11-08 00:00:00.123456789'::TIMESTAMP_NS,1.2000,map(['a,b','q=x'],[2,3]),{'x':1,'s':chr(0)||'q''x'} FROM base");
        state.updateSchema();
        DuckDBSchema.DuckDBTable rich = state.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getName().equals("rich")).findFirst().orElseThrow();
        assertEquals("JSON", rich.getColumns().stream().filter(c -> c.getName().equals("j")).findFirst().orElseThrow()
                .getType().toSQLType());
        assertEquals("TIMESTAMP_NS", rich.getColumns().stream().filter(c -> c.getName().equals("t")).findFirst()
                .orElseThrow().getType().toSQLType());
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
            vp.cleanupEcstSeed(seed);
            SQLConnection original = state.getConnection();
            try (Connection target = DriverManager.getConnection("jdbc:duckdb:")) {
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
        DuckDBECSTOracle oracle = new DuckDBECSTOracle(state);
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
            assertFalse(DuckDBEcstCapabilities.isLegal(seed, invalid));
            EcstTransformationConfig valid = new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED,
                    PartitionLayout.SAME_SIDE, Reconstruction.CTE, ExpressionContext.INNER_SELECT);
            EcstCase testCase = vp.applyEcstTransformation(seed, valid, null);
            vp.prepareEcstCase(testCase);
            assertTrue(vp.compareEcstCase(testCase).isComparable());
            assertTrue(vp.compareEcstCase(testCase).isEqual());
            assertEquals(0, vp.compareEcstCase(testCase).getOriginalResultCount());
            execute("ALTER TABLE \"" + testCase.getLeftTable() + "\" ALTER COLUMN c0 TYPE INTEGER");
            assertFalse(vp.compareEcstCase(testCase).isEqual());
            assertEquals(Set.of(), DuckDBECSTOracle.candidateSets(seed,
                    new sqlancer.common.oracle.ecst.EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"))
                    .keySet());
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void characterHavingIsDeterministicAndOptimizerProfileRemainsUnchanged() throws Exception {
        execute("INSERT INTO base VALUES (9,9,'a'),(9,9,'a ')");
        String before;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rows = statement.executeQuery("SELECT current_setting('disabled_optimizers')")) {
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
                    ResultSet rows = statement.executeQuery("SELECT current_setting('disabled_optimizers')")) {
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
                    + "\" (vp_rowid BIGINT NOT NULL, c0 BIGINT NOT NULL, c2 VARCHAR, PRIMARY KEY(vp_rowid,c0))");
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
    void fixedComparisonCanMigrateFromFilterIntoJoinCondition() throws Exception {
        execute("INSERT INTO base VALUES (10,1,'x'),(11,1,'y')");
        EcstSeed seed = vp.createEcstSeed("base", "\"c0\"+\"c1\"", List.of("c0", "c1"), ExpressionContext.WHERE, false,
                false, "\"c0\">\"c1\"");
        try {
            String original = seed.getOriginalQuery();
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED, PartitionLayout.CROSS_SIDE,
                            Reconstruction.JOIN, ExpressionContext.JOIN_ON),
                    null);
            vp.prepareEcstCase(testCase);
            EcstTarget target = new EcstTarget("operator", EcstContextDimension.OPERATOR, "FILTER", "JOIN_CONDITION");
            EcstObservation observation = new DuckDBExecutionContextAnalyzer(state).analyze(testCase, target);
            assertEquals(EcstShiftStatus.SUCCESS, observation.getStatus(),
                    observation.getOriginalPlan() + observation.getTransformedPlan());
            assertEquals(original, testCase.getOriginalQuery());
            assertTrue(vp.compareEcstCase(testCase).isEqual());
            assertEquals(2, vp.compareEcstCase(testCase).getOriginalResultCount());
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    @Test
    void artIndexMigrationUsesExecutedScanEvidenceInBothDirections() throws Exception {
        execute("CREATE TABLE big AS SELECT CASE WHEN i=17 THEN 1 ELSE -i END AS c0,i%11 AS c1 FROM range(1,4097) r(i)");
        DuckDBExecutionContextAnalyzer analyzer = new DuckDBExecutionContextAnalyzer(state);
        for (boolean indexed : List.of(false, true)) {
            EcstSeed seed = vp.createEcstSeed("big", "\"c0\"", List.of("c0"), ExpressionContext.WHERE, indexed, false);
            try {
                sqlancer.common.oracle.ecst.EcstTarget target = new sqlancer.common.oracle.ecst.EcstTarget("access",
                        EcstContextDimension.ACCESS_PATH, indexed ? "INDEX_SCAN" : "TABLE_SCAN",
                        indexed ? "TABLE_SCAN" : "INDEX_SCAN");
                EcstCase testCase = vp
                        .applyEcstTransformation(seed,
                                new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED,
                                        PartitionLayout.SAME_SIDE, Reconstruction.JOIN, ExpressionContext.WHERE),
                                target);
                vp.prepareEcstCase(testCase);
                sqlancer.common.oracle.ecst.EcstObservation observation = analyzer.analyze(testCase, target);
                assertEquals(EcstShiftStatus.SUCCESS, observation.getStatus(),
                        observation.getOriginalPlan() + observation.getTransformedPlan());
                assertTrue(vp.compareEcstCase(testCase).isEqual());
                assertEquals(1, vp.compareEcstCase(testCase).getOriginalResultCount());
            } finally {
                vp.cleanupEcstSeed(seed);
            }
        }
    }

    @Test
    void optimizerProfileIsSharedAndUnexpectedErrorsAreNeverSuppressed() throws Exception {
        execute("SET disabled_optimizers='join_order'");
        state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns = true;
        SQLConnection original = state.getConnection();
        EcstSeed seed = vp.createEcstSeed("base", "unknown_ecst_function(\"c0\")", List.of("c0"),
                ExpressionContext.SELECT, false, false);
        try {
            EcstCase testCase = vp.applyEcstTransformation(seed,
                    new EcstTransformationConfig(Representation.VERTICAL_PARTITIONED, PartitionLayout.SAME_SIDE,
                            Reconstruction.JOIN, ExpressionContext.SELECT),
                    null);
            vp.prepareEcstCase(testCase);
            assertThrows(java.sql.SQLException.class, () -> vp.compareEcstCase(testCase));
            assertThrows(java.sql.SQLException.class,
                    () -> new DuckDBExecutionContextAnalyzer(state).extractContext(seed.getOriginalQuery(),
                            seed.getOperation(), java.util.Set.of(seed.getSourceTable())));
            assertSame(original, state.getConnection());
            try (Statement s = state.getConnection().createStatement();
                    ResultSet r = s.executeQuery("SELECT current_setting('disabled_optimizers')")) {
                assertTrue(r.next());
                assertEquals("join_order", r.getString(1));
            }
        } finally {
            vp.cleanupEcstSeed(seed);
        }
    }

    private long helperCount() throws Exception {
        try (Statement statement = state.getConnection().createStatement(); ResultSet rows = statement.executeQuery(
                "SELECT count(*) FROM information_schema.tables WHERE table_catalog=current_database() AND table_schema=current_schema() AND table_name LIKE 'vp_ecst_duck_%'")) {
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
