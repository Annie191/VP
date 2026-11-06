package sqlancer.oceanbase.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstExpressionMatcher;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstOperation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;

class OceanBaseExecutionContextAnalyzerTest {
    private static final Path FIXTURES = Path.of("test/sqlancer/oceanbase/oracle/fixtures/ecst");

    private static EcstOperation predicate(String expression) {
        return new EcstOperation("operation", expression, Set.of("c0", "c1"), "test")
                .withExpectedRole("SELECTION_PREDICATE");
    }

    @Test
    void nativePlansTrackPredicatesAcrossTheJoinAndOperatorBoundaries() throws Exception {
        EcstOperation operation = predicate("c0+c1>0");
        OceanBaseExecutionContextAnalyzer.ParsedContext before = OceanBaseExecutionContextAnalyzer
                .parse(Files.readString(FIXTURES.resolve("scan-filter.plan")), operation, Set.of("src"));
        OceanBaseExecutionContextAnalyzer.ParsedContext after = OceanBaseExecutionContextAnalyzer.parse(
                Files.readString(FIXTURES.resolve("cross-side-join.plan")), operation, Set.of("ecst_l", "ecst_r"));
        assertTrue(before.isOperationMatched());
        assertTrue(after.isOperationMatched());
        EcstObservation observation = OceanBaseExecutionContextAnalyzer.observe(before, after,
                new EcstTarget("location", EcstContextDimension.LOCATION, "PRE_JOIN", "POST_JOIN"));
        assertEquals(EcstShiftStatus.SUCCESS, observation.getStatus());
        assertTrue(observation.getObservedShifts().contains("OPERATOR:FILTER->JOIN_CONDITION"));
    }

    @Test
    void subplanScanIsARetainedViewBoundaryNotMaterializationProof() throws Exception {
        OceanBaseExecutionContextAnalyzer.ParsedContext parsed = OceanBaseExecutionContextAnalyzer
                .parse(Files.readString(FIXTURES.resolve("retained-view.plan")), predicate("c0+c1>0"), Set.of("src"));
        assertEquals("RETAINED_VIEW", parsed.context().getBoundary());
        assertTrue(parsed.isOperationMatched());
        assertEquals("TABLE_SCAN", parsed.context().getAccessPath());
    }

    @Test
    void projectionAndOutlineMentionsDoNotBecomePredicateEvidence() throws Exception {
        String plan = Files.readString(FIXTURES.resolve("projection.plan"))
                + "\n0 - output(nil), filter([src.c0+src.c1>0])\n";
        OceanBaseExecutionContextAnalyzer.ParsedContext parsed = OceanBaseExecutionContextAnalyzer.parse(plan,
                predicate("c0+c1>0"), Set.of("src"));
        assertFalse(parsed.isOperationMatched());
        assertEquals("UNKNOWN", parsed.context().getLocation());
    }

    @Test
    void normalizesInternalCastsWithoutChangingNestedExpressionsOrStringContents() {
        String internal = "coalesce(cast((src.c0 + 1), DECIMAL(65, 30)), 0) > 0";
        String normalized = OceanBaseExecutionContextAnalyzer.normalizeCasts(internal);
        assertEquals(EcstExpressionMatcher.fingerprint("COALESCE(CAST(c0+1 AS DECIMAL(65,30)),0)>0"),
                EcstExpressionMatcher.fingerprint(normalized));
        assertNotNull(EcstExpressionMatcher.fingerprint(normalized));
        assertEquals("'cast(c0, DECIMAL(4,2))'",
                OceanBaseExecutionContextAnalyzer.normalizeCasts("'cast(c0, DECIMAL(4,2))'"));
        assertEquals("CAST(CAST(c0 AS DECIMAL(5,2)) AS DECIMAL(65,30))",
                OceanBaseExecutionContextAnalyzer.normalizeCasts("cast(cast(c0, DECIMAL(5,2)), DECIMAL(65,30))"));
    }

    @Test
    void matchesCastsInTheFilterFieldAndRejectsAnUnrelatedScan() {
        String plan = "|0|TABLE FULL SCAN|src|1|1|\nOutputs & filters:\n"
                + "0 - output([src.c0]), filter([cast(src.c0, DECIMAL(65,30)) > 0])\n";
        EcstOperation operation = predicate("CAST(c0 AS DECIMAL(65,30)) > 0");
        assertTrue(OceanBaseExecutionContextAnalyzer.parse(plan, operation, Set.of("src")).isOperationMatched());
        assertFalse(OceanBaseExecutionContextAnalyzer.parse(plan, operation, Set.of("other")).isOperationMatched());
    }

    @Test
    void readsOnlyNamedFieldsAndKeepsParameterCommasIntact() {
        assertEquals(List.of("coalesce(cast(c0, DECIMAL(65,30)),0)>0", "c1 IS NOT NULL"),
                OceanBaseExecutionContextAnalyzer.expressions(
                        "output([c0]), filter([coalesce(cast(c0, DECIMAL(65,30)),0)>0], [c1 IS NOT NULL]), rowset=16",
                        "filter"));
        assertEquals(List.of(),
                OceanBaseExecutionContextAnalyzer.expressions("filter(nil), filter_before_indexback[false]", "filter"));
        assertEquals(List.of("c0 = ']'"),
                OceanBaseExecutionContextAnalyzer.expressions("filter([c0 = ']'])", "filter"));
    }

    @Test
    void rangeAccessEvidenceIsIndependentOfAnAbsorbedPredicate() throws Exception {
        String plan = "|0|TABLE RANGE SCAN|src(ix)|1|1|\nOutputs & filters:\n"
                + "0 - output([src.vp_rowid]), filter(nil)\nrange_key([src.c0]), range(0;MAX)\n";
        OceanBaseExecutionContextAnalyzer.ParsedContext parsed = OceanBaseExecutionContextAnalyzer.parse(plan,
                predicate("c0>0"), Set.of("src"));
        assertEquals("INDEX_RANGE", parsed.context().getAccessPath());
        assertEquals("MERGED", parsed.context().getBoundary());
        assertEquals("UNKNOWN", parsed.context().getLocation());
        assertFalse(parsed.isOperationMatched());
        OceanBaseExecutionContextAnalyzer.ParsedContext captured = OceanBaseExecutionContextAnalyzer
                .parse(Files.readString(FIXTURES.resolve("index-range.plan")), predicate("c0>0"), Set.of("src"));
        assertEquals("INDEX_RANGE", captured.context().getAccessPath());
        assertFalse(captured.isOperationMatched());
        assertEquals("PRIMARY_GET", OceanBaseExecutionContextAnalyzer
                .parse("|0|TABLE GET|src(PRIMARY)|1|1|", predicate("c0>0"), Set.of("src")).context().getAccessPath());
    }

    @Test
    void conflictingRolesAndMissingPlansRemainUnknown() {
        String plan = "|0|MERGE JOIN||1|1|\n|1|└─TABLE FULL SCAN|src|1|1|\nOutputs & filters:\n"
                + "0 - output([src.c0]), filter([src.c0>0]), other_conds([src.c0>0])\n1 - output([src.c0]), filter(nil)\n";
        assertEquals("UNKNOWN", OceanBaseExecutionContextAnalyzer.parse(plan, predicate("c0>0"), Set.of("src"))
                .context().getOperator());
        assertEquals("UNKNOWN", OceanBaseExecutionContextAnalyzer
                .parse("unrecognized plan", predicate("c0>0"), Set.of("src")).context().getBoundary());
    }

    @Test
    void strictComparisonPreservesCharacterAndFloatingRepresentations() {
        assertEquals("001", OceanBaseVerticalPartitioningOracle.canonicalizeTypedValue("001", "VARCHAR"));
        assertEquals("a ", OceanBaseVerticalPartitioningOracle.canonicalizeTypedValue("a ", "VARCHAR"));
        assertEquals("1", OceanBaseVerticalPartitioningOracle.canonicalizeTypedValue("001.00", "DECIMAL"));
        assertEquals("1e0", OceanBaseVerticalPartitioningOracle.canonicalizeTypedValue("1e0", "DOUBLE"));
        assertEquals("X'0027ff'", OceanBaseVerticalPartitioningOracle.ecstLiteral(new byte[] { 0, 39, -1 }));
    }
}
