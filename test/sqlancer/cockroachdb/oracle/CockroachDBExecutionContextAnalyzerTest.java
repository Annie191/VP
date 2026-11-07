package sqlancer.cockroachdb.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import org.junit.jupiter.api.Test;

import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstExpressionMatcher;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstOperation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;

class CockroachDBExecutionContextAnalyzerTest {
    private static final Path FIXTURES = Path.of("test/sqlancer/cockroachdb/oracle/fixtures/ecst");

    private static EcstOperation operation(String expression) {
        return new EcstOperation("op", expression, Set.of("c0", "c1"), "test").withExpectedRole("SELECTION_PREDICATE");
    }

    private static String fixture(String name) throws Exception {
        return Files.readString(FIXTURES.resolve(name + ".plan"));
    }

    @Test
    void sessionReplayPreservesIntegerTypesAndQuotesStringValues() throws Exception {
        assertEquals("SET opt_split_scan_limit = 2048",
                CockroachDBEcstSession.sessionSetStatement("opt_split_scan_limit", "2048"));
        assertEquals("SET extra_float_digits = -1",
                CockroachDBEcstSession.sessionSetStatement("extra_float_digits", "-1"));
        assertTrue(CockroachDBEcstSession.sessionSetStatement("timezone", "UTC").contains("'UTC'"));
        assertThrows(java.sql.SQLException.class,
                () -> CockroachDBEcstSession.sessionSetStatement("opt_split_scan_limit", "1; SELECT 1"));
    }

    @Test
    void singleNodeOptionsRejectDistributedVpProfiles() {
        sqlancer.cockroachdb.CockroachDBOptions options = new sqlancer.cockroachdb.CockroachDBOptions();
        CockroachDBEcstSession.validateOptions(options);
        options.verticalPartitioningDistributedGateways = "unreachable.invalid:1";
        assertThrows(IllegalArgumentException.class, () -> CockroachDBEcstSession.validateOptions(options));
        options.verticalPartitioningDistributedGateways = "";
        options.verticalPartitioningDistributedProfile = true;
        assertThrows(IllegalArgumentException.class, () -> CockroachDBEcstSession.validateOptions(options));
        options.verticalPartitioningDistributedProfile = false;
        options.verticalPartitioningRowRangePressureProfile = true;
        assertThrows(IllegalArgumentException.class, () -> CockroachDBEcstSession.validateOptions(options));
    }

    @Test
    void nativePredicateMovesFromFilterToJoinPred() throws Exception {
        CockroachDBExecutionContextAnalyzer.ParsedContext before = CockroachDBExecutionContextAnalyzer
                .parse(fixture("filter"), operation("\"c0\"+\"c1\">0"), Set.of("src"));
        CockroachDBExecutionContextAnalyzer.ParsedContext after = CockroachDBExecutionContextAnalyzer
                .parse(fixture("join"), operation("\"c0\"+\"c1\">0"), Set.of("pl", "pr"));
        assertTrue(before.isOperationMatched());
        assertTrue(after.isOperationMatched());
        EcstObservation observed = CockroachDBExecutionContextAnalyzer.observe(before, after,
                new EcstTarget("location", EcstContextDimension.LOCATION, "PRE_JOIN", "POST_JOIN"));
        assertEquals(EcstShiftStatus.SUCCESS, observed.getStatus());
        assertTrue(observed.getObservedShifts().contains("OPERATOR:FILTER->JOIN_CONDITION"));
    }

    @Test
    void resolvesConsumerFiltersThroughMaterializedBufferLineage() throws Exception {
        CockroachDBExecutionContextAnalyzer.ParsedContext context = CockroachDBExecutionContextAnalyzer
                .parse(fixture("cte"), operation("c0+c1>0"), Set.of("src"));
        assertTrue(context.isOperationMatched());
        assertEquals("MATERIALIZED", context.context().getBoundary());
        assertEquals("TABLE_SCAN", context.context().getAccessPath());
        assertFalse(CockroachDBExecutionContextAnalyzer.parse(fixture("cte"), operation("c0+c1>0"), Set.of("other"))
                .isOperationMatched());
    }

    @Test
    void confirmsBothMaterializationDirections() throws Exception {
        CockroachDBExecutionContextAnalyzer.ParsedContext merged = CockroachDBExecutionContextAnalyzer
                .parse(fixture("filter"), operation("c0+c1>0"), Set.of("src"));
        CockroachDBExecutionContextAnalyzer.ParsedContext retained = CockroachDBExecutionContextAnalyzer
                .parse(fixture("cte"), operation("c0+c1>0"), Set.of("src"));
        assertEquals(EcstShiftStatus.SUCCESS,
                CockroachDBExecutionContextAnalyzer
                        .observe(merged, retained,
                                new EcstTarget("f", EcstContextDimension.BOUNDARY, "MERGED", "MATERIALIZED"))
                        .getStatus());
        assertEquals(EcstShiftStatus.SUCCESS,
                CockroachDBExecutionContextAnalyzer
                        .observe(retained, merged,
                                new EcstTarget("b", EcstContextDimension.BOUNDARY, "MATERIALIZED", "MERGED"))
                        .getStatus());
    }

    @Test
    void explicitRenderIsProjectionEvidence() throws Exception {
        EcstOperation projected = operation("c0+c1").withExpectedRole("PROJECTION_ITEM");
        CockroachDBExecutionContextAnalyzer.ParsedContext context = CockroachDBExecutionContextAnalyzer
                .parse(fixture("projection"), projected, Set.of("src"));
        assertTrue(context.isOperationMatched());
        assertEquals("PROJECTION", context.context().getOperator());
        assertFalse(CockroachDBExecutionContextAnalyzer
                .parse(fixture("projection"), operation("c0+c1>0"), Set.of("src")).isOperationMatched());
    }

    @Test
    void columnsAndOriginalSqlAreNotOperationEvidence() {
        String plan = "• project\n│ columns: (c0+c1)\n│ original sql: SELECT c0+c1>0 FROM src\n└── • scan\n      table: src@src_pkey\n      spans: FULL SCAN\n";
        assertFalse(CockroachDBExecutionContextAnalyzer.parse(plan, operation("c0+c1>0"), Set.of("src"))
                .isOperationMatched());
        assertFalse(CockroachDBExecutionContextAnalyzer
                .parse(plan, operation("c0+c1").withExpectedRole("PROJECTION_ITEM"), Set.of("src"))
                .isOperationMatched());
    }

    @Test
    void rangeEvidenceDoesNotInventAnAbsorbedFilter() throws Exception {
        CockroachDBExecutionContextAnalyzer.ParsedContext context = CockroachDBExecutionContextAnalyzer
                .parse(fixture("index"), operation("c0>0"), Set.of("src"));
        assertEquals("INDEX_RANGE", context.context().getAccessPath());
        assertFalse(context.isOperationMatched());
        assertEquals("UNKNOWN", context.context().getLocation());
    }

    @Test
    void stripsNumericAnnotationsAndQuotedIdentifiersButKeepsStringContents() {
        String normalized = CockroachDBExecutionContextAnalyzer
                .normalizeExpression("COALESCE(\"c0\"::DECIMAL,0:::DECIMAL)>0:::INT8");
        String actual = EcstExpressionMatcher.fingerprint(normalized);
        assertNotNull(actual);
        assertEquals(EcstExpressionMatcher.fingerprint("COALESCE(CAST(c0 AS DECIMAL),0)>0"), actual);
        assertEquals("'\"c0\":::INT8'", CockroachDBExecutionContextAnalyzer.normalizeExpression("'\"c0\":::INT8'"));
    }

    @Test
    void ambiguousPredicatesAndUnrecognizedPlansRemainUnknown() throws Exception {
        String duplicated = fixture("filter").replace("│ filter: (c0 + c1) > 0",
                "│ filter: (c0 + c1) > 0 AND (c0 + c1) > 0");
        assertEquals("UNKNOWN", CockroachDBExecutionContextAnalyzer
                .parse(duplicated, operation("c0+c1>0"), Set.of("src")).context().getLocation());
        assertEquals("UNKNOWN", CockroachDBExecutionContextAnalyzer.parse("no plan", operation("c0>0"), Set.of("src"))
                .context().getBoundary());
    }

    @Test
    void strictEncodingKeepsBitsBytesAndTextDistinct() {
        assertEquals("001", CockroachDBVerticalPartitioningOracle.canonicalizeTypedValue("001", "BIT"));
        assertEquals("001", CockroachDBVerticalPartitioningOracle.canonicalizeTypedValue("001", "varchar"));
        assertEquals("a ", CockroachDBVerticalPartitioningOracle.canonicalizeTypedValue("a ", "text"));
        assertEquals("1", CockroachDBVerticalPartitioningOracle.canonicalizeTypedValue("001.00", "numeric"));
        assertEquals("decode('00ff','hex')",
                CockroachDBVerticalPartitioningOracle.ecstLiteral(new byte[] { 0, (byte) 255 }));
        assertEquals("E'a''b\\\\c'", CockroachDBVerticalPartitioningOracle.ecstLiteral("a'b\\c"));
    }

    @Test
    void internalExplainErrorsPropagateWhileUnavailablePlansRemainUnknown() throws Exception {
        sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState state = new sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState();
        java.util.concurrent.atomic.AtomicReference<String> sqlState = new java.util.concurrent.atomic.AtomicReference<>(
                "XX000");
        java.sql.Statement statement = (java.sql.Statement) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] { java.sql.Statement.class },
                (proxy, method, arguments) -> {
                    if (method.getName().equals("executeQuery")) {
                        throw new java.sql.SQLException("deliberate driver failure", sqlState.get());
                    }
                    return null;
                });
        java.sql.Connection connection = (java.sql.Connection) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[] { java.sql.Connection.class },
                (proxy, method, arguments) -> method.getName().equals("createStatement") ? statement : null);
        state.setConnection(new sqlancer.SQLConnection(connection));
        CockroachDBExecutionContextAnalyzer analyzer = new CockroachDBExecutionContextAnalyzer(state);
        assertThrows(java.sql.SQLException.class,
                () -> analyzer.extractContext("SELECT 1", operation("c0>0"), Set.of("src")));
        sqlState.set("42601");
        assertEquals("UNKNOWN",
                analyzer.extractContext("SELECT 1", operation("c0>0"), Set.of("src")).context().getBoundary());
    }

}
