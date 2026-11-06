package sqlancer.mysql.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;

import org.junit.jupiter.api.Test;

import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstOperation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;

class MySQLExecutionContextAnalyzerTest {
    private static EcstOperation predicate(String expression) {
        return new EcstOperation("operation", expression, Set.of("c0"), "test").withExpectedRole("SELECTION_PREDICATE");
    }

    @Test
    void followsAnExpressionAcrossJoinBoundary() {
        String before = "-> Filter: ((src.c0 + src.c1) > 0)  (cost=1 rows=1)\n"
                + "    -> Table scan on src  (cost=1 rows=3)\n";
        String after = "-> Filter: ((ecst_l.c0 + ecst_r.c1) > 0)\n" + "    -> Nested loop inner join\n"
                + "        -> Table scan on ecst_l\n"
                + "        -> Single-row index lookup on ecst_r using PRIMARY (vp_rowid=ecst_l.vp_rowid)\n";
        EcstOperation operation = predicate("`c0` + `c1` > 0");
        EcstObservation observation = MySQLExecutionContextAnalyzer.observe(
                MySQLExecutionContextAnalyzer.parse(before, operation, Set.of("src")),
                MySQLExecutionContextAnalyzer.parse(after, operation, Set.of("ecst_l", "ecst_r")),
                new EcstTarget("location", EcstContextDimension.LOCATION, "PRE_JOIN", "POST_JOIN"));
        assertEquals(EcstShiftStatus.SUCCESS, observation.getStatus());
        assertTrue(observation.getObservedShifts().contains("LOCATION:PRE_JOIN->POST_JOIN"));
    }

    @Test
    void preservesAstStructureAndStringLiteralCase() {
        assertNotNull(MySQLExecutionContextAnalyzer.fingerprint("(c0 + c1) * c2"));
        assertNotEquals(MySQLExecutionContextAnalyzer.fingerprint("(c0 + c1) * c2"),
                MySQLExecutionContextAnalyzer.fingerprint("c0 + c1 * c2"));
        assertNotEquals(MySQLExecutionContextAnalyzer.fingerprint("c0 = 'A'"),
                MySQLExecutionContextAnalyzer.fingerprint("c0 = 'a'"));
        assertEquals(MySQLExecutionContextAnalyzer.fingerprint("c0 > 0"),
                MySQLExecutionContextAnalyzer.fingerprint("0 < `db`.`src`.`c0`"));
        assertEquals(MySQLExecutionContextAnalyzer.fingerprint("CASE WHEN c0 IS NULL THEN 0 ELSE c0 END"),
                MySQLExecutionContextAnalyzer.fingerprint("CASE WHEN src.c0 IS NULL THEN 0 ELSE src.c0 END"));
        assertNotEquals(MySQLExecutionContextAnalyzer.fingerprint("CASE WHEN c0 IS NULL THEN 0 ELSE c0 END"),
                MySQLExecutionContextAnalyzer.fingerprint("CASE WHEN c0 IS NOT NULL THEN 0 ELSE c0 END"));
    }

    @Test
    void ignoresUnrelatedExpressionsAndAccessPaths() {
        String plan = "-> Nested loop inner join\n" + "    -> Table scan on src\n" + "    -> Filter: (other.c0 > 0)\n"
                + "        -> Index range scan on other using ix over (0 < c0)\n";
        MySQLExecutionContextAnalyzer.ParsedContext parsed = MySQLExecutionContextAnalyzer.parse(plan,
                predicate("c0 > 0"), Set.of("src"));
        assertFalse(parsed.isOperationMatched());
        assertEquals("UNKNOWN", parsed.context().getLocation());
        assertEquals("TABLE_SCAN", parsed.context().getAccessPath());
    }

    @Test
    void reportsConflictingPhysicalInstancesAsUnknown() {
        String plan = "-> Filter: (c0 > 0)\n" + "    -> Nested loop inner join\n" + "        -> Filter: (c0 > 0)\n"
                + "            -> Table scan on src\n" + "        -> Table scan on other\n";
        assertEquals("UNKNOWN",
                MySQLExecutionContextAnalyzer.parse(plan, predicate("c0 > 0"), Set.of("src")).context().getLocation());
    }

    @Test
    void recognizesMaterializationWithoutInventingProjectionEvidence() {
        String plan = "-> Table scan on ecst_outer\n" + "    -> Materialize\n"
                + "        -> Covering index range scan on src using ix over (0 < c0)\n";
        EcstOperation projection = predicate("c0 > 0").withExpectedRole("PROJECTION_ITEM");
        MySQLExecutionContextAnalyzer.ParsedContext parsed = MySQLExecutionContextAnalyzer.parse(plan, projection,
                Set.of("src"));
        assertFalse(parsed.isOperationMatched());
        assertEquals("MATERIALIZED", parsed.context().getBoundary());
        assertEquals("INDEX_RANGE", parsed.context().getAccessPath());
        assertEquals("UNKNOWN", parsed.context().getOperator());
    }

    @Test
    void absorbedRangeCanSupportAccessEvidenceAndUnknownRemainsIndependent() {
        EcstOperation operation = predicate("c0 > 0");
        MySQLExecutionContextAnalyzer.ParsedContext scan = MySQLExecutionContextAnalyzer
                .parse("-> Filter: (src.c0 > 0)\n    -> Table scan on src\n", operation, Set.of("src"));
        MySQLExecutionContextAnalyzer.ParsedContext range = MySQLExecutionContextAnalyzer
                .parse("-> Covering index range scan on src using ix over (0 < c0)\n", operation, Set.of("src"));
        EcstObservation access = MySQLExecutionContextAnalyzer.observe(scan, range,
                new EcstTarget("access", EcstContextDimension.ACCESS_PATH, "TABLE_SCAN", "INDEX_RANGE"));
        assertEquals(EcstShiftStatus.SUCCESS, access.getStatus());
        EcstObservation location = MySQLExecutionContextAnalyzer.observe(scan, range,
                new EcstTarget("location", EcstContextDimension.LOCATION, "PRE_JOIN", "POST_JOIN"));
        assertEquals(EcstShiftStatus.UNKNOWN, location.getStatus());
        assertTrue(location.getObservedShifts().contains("ACCESS_PATH:TABLE_SCAN->INDEX_RANGE"));
    }

    @Test
    void doesNotGuessFromAnEliminatedOrUnknownPlan() {
        MySQLExecutionContextAnalyzer.ParsedContext parsed = MySQLExecutionContextAnalyzer
                .parse("-> Zero rows (Impossible WHERE)\n", predicate("c0 > 0"), Set.of("src"));
        assertEquals("UNKNOWN", parsed.context().getBoundary());
        assertEquals("UNKNOWN", parsed.context().getAccessPath());
    }

    @Test
    void valueComparisonPreservesTypesAndReplayLiteralsDoNotDependOnEscapingMode() {
        assertEquals("001", MySQLVerticalPartitioningOracle.canonicalizeTypedValue("001", "VARCHAR"));
        assertEquals("1", MySQLVerticalPartitioningOracle.canonicalizeTypedValue("001.00", "DECIMAL"));
        assertEquals("1e0", MySQLVerticalPartitioningOracle.canonicalizeTypedValue("1e0", "DOUBLE"));
        assertEquals("X'0027ff'", MySQLVerticalPartitioningOracle.ecstLiteral(new byte[] { 0, 39, -1 }));
        assertEquals("CONVERT(X'275c' USING utf8mb4)", MySQLVerticalPartitioningOracle.ecstLiteral("'\\"));
    }
}
