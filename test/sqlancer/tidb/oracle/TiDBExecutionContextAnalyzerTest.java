package sqlancer.tidb.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstOperation;

public class TiDBExecutionContextAnalyzerTest {
    private static final String TARGET = "CAST(`a` AS SIGNED)";

    @Test
    void matchesRootSelectionInsteadOfUnrelatedCopNode() {
        String plan = "Projection_5 1 root test.t.a\n"
                + "└─Selection_4 1 root gt(cast(test.t.a, signed), 0)\n"
                + "  └─TableReader_3 1 root data:TableFullScan_2\n"
                + "    └─TableFullScan_2 1 cop[tikv] table:t\n";

        EcstContext context = TiDBExecutionContextAnalyzer.parse(plan, TARGET);

        assertEquals("ROOT", context.getLocation());
        assertEquals("SELECTION", context.getOperator());
        assertEquals("NON_PUSHDOWN", context.getBoundary());
        assertEquals("TABLEFULLSCAN", context.getAccessPath());
    }

    @Test
    void usesMatchedCopSelectionForPushdownEvidence() {
        String plan = "Projection_5 1 root test.t.a\n"
                + "└─TableReader_3 1 root data:Selection_2\n"
                + "  └─Selection_2 1 cop[tikv] gt(cast(test.t.a, signed), 0)\n"
                + "    └─TableFullScan_1 1 cop[tikv] table:t\n";

        EcstContext context = TiDBExecutionContextAnalyzer.parse(plan, TARGET);

        assertEquals("COP_TIKV", context.getLocation());
        assertEquals("SELECTION", context.getOperator());
        assertEquals("PUSHDOWN", context.getBoundary());
    }

    @Test
    void returnsUnknownWhenTargetCannotBeMatched() {
        String plan = "Projection_5 1 root test.t.a\n"
                + "└─Selection_4 1 root gt(cast(test.t.a, signed), 0)\n"
                + "  └─TableFullScan_2 1 cop[tikv] table:t\n";

        EcstContext context = TiDBExecutionContextAnalyzer.parse(plan, "CAST(`missing` AS SIGNED)");

        assertEquals("UNKNOWN", context.getLocation());
        assertEquals("UNKNOWN", context.getBoundary());
        assertEquals("UNKNOWN", context.getOperator());
        assertEquals("UNKNOWN", context.getAccessPath());
    }

    @Test
    void ambiguousTargetMatchIsUnknown() {
        String plan = "Selection_2 1 root gt(cast(test.t.a, signed), 0)\n"
                + "└─Selection_1 1 cop[tikv] gt(cast(test.t.a, signed), 0)\n";

        EcstContext context = TiDBExecutionContextAnalyzer.parse(plan, TARGET);

        assertEquals("UNKNOWN", context.getLocation());
        assertEquals("UNKNOWN", context.getBoundary());
    }

    @Test
    void duplicatedPhysicalInstancesRetainAllCandidatesAndSelectBestMatch() {
        String plan = "Selection_2 1 root gt(cast(test.t.a, signed), 0)\n"
                + "└─Selection_1 1 cop[tikv] gt(cast(test.t.a, signed), 0)\n";

        TiDBExecutionContextAnalyzer.ParsedContext parsed = TiDBExecutionContextAnalyzer.parseWithEvidence(plan, TARGET,
                "UNKNOWN");

        assertEquals(TiDBExecutionContextAnalyzer.MatchStatus.DUPLICATED_SAME_OPERATION, parsed.status());
        assertEquals(2, parsed.candidates().size());
        assertEquals("UNKNOWN", parsed.context().getLocation());
        assertEquals("UNKNOWN", parsed.context().getBoundary());
        assertEquals("SELECTION", parsed.context().getOperator());
        assertEquals(parsed.candidates().get(0).operationFingerprint(), parsed.candidates().get(1).operationFingerprint());
    }

    @Test
    void evaluationEnvironmentRetainsStructuredMetadataAndEvidenceState() {
        TiDBExecutionContextAnalyzer.ParsedContext parsed = TiDBExecutionContextAnalyzer.parseWithEvidence(
                "Selection_2 1 root gt(cast(test.t.a, signed), 0)\n", TARGET,
                "sql_mode=STRICT_TRANS_TABLES;collation=utf8mb4_bin;charset=utf8mb4;targetTypeConversion=CAST");

        assertEquals("STRICT_TRANS_TABLES", parsed.context().getEvaluationEnvironment().getSqlMode());
        assertEquals("utf8mb4_bin", parsed.context().getEvaluationEnvironment().getCollation());
        assertTrue(!parsed.context().getEvaluationEnvironment().isEvidenceAvailable());
    }

    @Test
    void preservesEvaluationEnvironmentOnMatchedNode() {
        String plan = "Selection_2 1 root gt(cast(test.t.a, signed), 0)\n";

        TiDBExecutionContextAnalyzer.ParsedContext parsed = TiDBExecutionContextAnalyzer.parseWithEvidence(plan, TARGET,
                "sql_mode=STRICT_TRANS_TABLES;collation=utf8mb4_bin;charset=utf8mb4;targetTypeConversion=CAST")
                ;

        assertEquals("UNKNOWN", parsed.context().getEvaluation());
        assertTrue(parsed.evidence().contains("sql_mode=STRICT_TRANS_TABLES"));
        assertTrue(parsed.evidence().contains("executorEvaluation=UNOBSERVED"));
    }

    private String fixture(String name) throws java.io.IOException {
        return java.nio.file.Files.readString(java.nio.file.Path.of(
                "test/sqlancer/tidb/oracle/fixtures/ecst", name + ".plan"));
    }

    @Test
    void realProjectionAndSelectionPlansLocateTheCast() throws Exception {
        EcstContext projection = TiDBExecutionContextAnalyzer.parse(fixture("projection"), "CAST(`v` AS SIGNED)");
        EcstContext selection = TiDBExecutionContextAnalyzer.parse(fixture("selection"), "CAST(`v` AS SIGNED)");
        assertEquals("PROJECTION", projection.getOperator());
        assertEquals("ROOT", projection.getLocation());
        assertEquals("SELECTION", selection.getOperator());
        assertEquals("COP_TIKV", selection.getLocation());
    }

    @Test
    void realOptimizerDuplicationAndAliasExpansionAreUnknown() throws Exception {
        for (String name : java.util.List.of("join", "alias", "duplicate")) {
            assertEquals("UNKNOWN", TiDBExecutionContextAnalyzer.parse(fixture(name), "CAST(`v` AS SIGNED)")
                    .getLocation(), name);
        }
    }

    @Test
    void sameColumnWithDifferentOperationDoesNotMatch() throws Exception {
        assertEquals("UNKNOWN", TiDBExecutionContextAnalyzer.parse(fixture("selection"), "ABS(`v`)")
                .getLocation());
        assertEquals("UNKNOWN", TiDBExecutionContextAnalyzer.parse(fixture("selection"), "CAST(`v` AS UNSIGNED)")
                .getLocation());
    }

    @Test
    void matchesTidbInternalArithmeticFingerprintAfterRewrite() {
        String plan = "Projection_1 1 root plus(cast(test.t.a, bigint BINARY), cast(test.t.b, bigint BINARY))\n";
        EcstContext context = TiDBExecutionContextAnalyzer.parse(plan,
                "(CAST(`a` AS SIGNED) + CAST(`b` AS SIGNED))");
        assertEquals("ROOT", context.getLocation());
        assertEquals("PROJECTION", context.getOperator());
    }

    @Test
    void rewrittenFingerprintStillReturnsUnknownWhenTwoNodesMatch() {
        String plan = "Projection_1 1 root plus(cast(test.t.a, bigint BINARY), cast(test.t.b, bigint BINARY))\n"
                + "└─Selection_2 1 cop[tikv] plus(cast(test.t.a, bigint BINARY), cast(test.t.b, bigint BINARY)) > 0\n";
        EcstContext context = TiDBExecutionContextAnalyzer.parse(plan,
                "(CAST(`a` AS SIGNED) + CAST(`b` AS SIGNED))");
        assertEquals("UNKNOWN", context.getLocation());
    }

    @Test
    void accessPathIsResolvedFromMatchedNodeSubtree() {
        String plan = "Selection_5 1 root gt(cast(test.t.a, signed), 0)\n"
                + "  └─IndexLookUp_4 1 root \n"
                + "    ├─IndexRangeScan_2 1 cop[tikv] table:t index:a\n"
                + "    └─TableRowIDScan_3 1 cop[tikv] table:t\n";
        EcstContext context = TiDBExecutionContextAnalyzer.parse(plan, "CAST(`a` AS SIGNED)");
        assertEquals("SELECTION", context.getOperator());
        assertEquals("INDEXLOOKUP", context.getAccessPath());
    }

    @Test
    void realTwoCastArithmeticPlansAreLocatable() throws Exception {
        EcstContext projection = TiDBExecutionContextAnalyzer.parse(fixture("two_cast_projection"),
                "(CAST(`a` AS SIGNED) + CAST(`b` AS SIGNED))");
        EcstContext selection = TiDBExecutionContextAnalyzer.parse(fixture("two_cast_selection"),
                "(CAST(`a` AS SIGNED) + CAST(`b` AS SIGNED))");
        assertEquals("PROJECTION", projection.getOperator());
        assertEquals("ROOT", projection.getLocation());
        assertEquals("SELECTION", selection.getOperator());
        assertEquals("COP_TIKV", selection.getLocation());
        assertEquals("TABLEFULLSCAN", selection.getAccessPath());
    }

    @Test
    void realTwoCastHavingPlanReportsAmbiguousRelocation() throws Exception {
        EcstContext having = TiDBExecutionContextAnalyzer.parse(fixture("two_cast_having"),
                "(CAST(`a` AS SIGNED) + CAST(`b` AS SIGNED))");
        assertEquals("UNKNOWN", having.getLocation());
    }

    @Test
    void matchesJoinOtherConditionAsAnExpressionInstance() {
        String plan = "MergeJoin | 12500.00 | root | | inner join, left key:test.l.vp_rowid, "
                + "right key:test.r.vp_rowid, other cond:gt(plus(cast(test.l.c0, bigint BINARY), "
                + "cast(test.r.c1, bigint BINARY)), 0)\n"
                + "├─TableReader(Build) | 10000.00 | root | | data:TableFullScan\n"
                + "│ └─TableFullScan | 10000.00 | cop[tikv] | table:l\n"
                + "└─TableReader(Probe) | 10000.00 | root | | data:TableFullScan\n"
                + "  └─TableFullScan | 10000.00 | cop[tikv] | table:r\n";

        TiDBExecutionContextAnalyzer.ParsedContext parsed = TiDBExecutionContextAnalyzer.parseWithEvidence(plan,
                "(CAST(`c0` AS SIGNED) + CAST(`c1` AS SIGNED))", "UNKNOWN");

        assertEquals(TiDBExecutionContextAnalyzer.MatchStatus.UNIQUE, parsed.status());
        assertEquals("ROOT", parsed.context().getLocation());
        assertEquals("MERGEJOIN", parsed.context().getOperator());
        assertEquals(TiDBExecutionContextAnalyzer.ExpressionRole.JOIN_OTHER_CONDITION,
                parsed.selectedExpression().role());
        assertTrue(parsed.selectedExpression().expressionText().contains("plus(cast(test.l.c0"));
        assertTrue(parsed.evidence().contains("expressionRole=JOIN_OTHER_CONDITION"));
        assertTrue(parsed.evidence().contains("BUILD=TABLEFULLSCAN"));
        assertTrue(parsed.evidence().contains("PROBE=TABLEFULLSCAN"));
        assertEquals("UNKNOWN", parsed.context().getAccessPath());
    }

    @Test
    void operationLineageResolvesQualifiedPartitionColumns() {
        java.util.Map<String, String> lineage = java.util.Map.of("l.c0", "c0", "r.c1", "c1");
        EcstOperation operation = new EcstOperation("op-1", "(CAST(`c0` AS SIGNED) + CAST(`c1` AS SIGNED))",
                java.util.Set.of("c0", "c1"),
                new sqlancer.common.oracle.ecst.EcstExpressionShape(2, "SIGNED", java.util.Set.of("+"),
                        java.util.Set.of("c0", "c1")), lineage);
        TiDBExecutionContextAnalyzer.ParsedContext parsed = TiDBExecutionContextAnalyzer.parseWithEvidence(
                "MergeJoin | 1.0 | root | | inner join, other cond:gt(plus(cast(l.c0, bigint BINARY), "
                        + "cast(r.c1, bigint BINARY)), 0)\n",
                operation, "UNKNOWN");
        assertEquals(TiDBExecutionContextAnalyzer.MatchStatus.UNIQUE, parsed.status());
        assertEquals("ROOT", parsed.context().getLocation());
        assertEquals(TiDBExecutionContextAnalyzer.ExpressionRole.JOIN_OTHER_CONDITION,
                parsed.selectedExpression().role());
    }

    @Test
    void keepsProjectionAndHavingExpressionsAsSeparateInstances() throws Exception {
        TiDBExecutionContextAnalyzer.ParsedContext projection = TiDBExecutionContextAnalyzer.parseWithEvidence(
                fixture("two_cast_projection"), "(CAST(`a` AS SIGNED) + CAST(`b` AS SIGNED))", "UNKNOWN");
        assertEquals(TiDBExecutionContextAnalyzer.ExpressionRole.PROJECTION_ITEM,
                projection.selectedExpression().role());

        TiDBExecutionContextAnalyzer.ParsedContext having = TiDBExecutionContextAnalyzer.parseWithEvidence(
                fixture("two_cast_having"), "(CAST(`a` AS SIGNED) + CAST(`b` AS SIGNED))", "UNKNOWN");
        assertEquals(TiDBExecutionContextAnalyzer.MatchStatus.AMBIGUOUS, having.status());
        assertTrue(having.candidates().stream().anyMatch(candidate ->
                candidate.expression().role() == TiDBExecutionContextAnalyzer.ExpressionRole.SELECTION_PREDICATE));
        assertTrue(having.candidates().stream().anyMatch(candidate ->
                candidate.expression().role() == TiDBExecutionContextAnalyzer.ExpressionRole.PROJECTION_ITEM));
    }

    @Test
    void projectionItemMatchIgnoresSiblingRowIdentityProjection() {
        String plan = "Projection | 10000.00 | root | | vp_rowid, plus(cast(test.t.c0, bigint BINARY), "
                + "cast(test.t.c1, bigint BINARY))->Column#5\n";
        TiDBExecutionContextAnalyzer.ParsedContext parsed = TiDBExecutionContextAnalyzer.parseWithEvidence(plan,
                "(CAST(`c0` AS SIGNED) + CAST(`c1` AS SIGNED))", "UNKNOWN");
        assertEquals(TiDBExecutionContextAnalyzer.MatchStatus.UNIQUE, parsed.status());
        assertEquals(TiDBExecutionContextAnalyzer.ExpressionRole.PROJECTION_ITEM,
                parsed.selectedExpression().role());
    }

    @Test
    void expectedHavingPredicateRoleDisambiguatesProjectionCopy() throws Exception {
        EcstOperation operation = new EcstOperation("having-op",
                "(CAST(`a` AS SIGNED) + CAST(`b` AS SIGNED))", java.util.Set.of("a", "b"),
                new sqlancer.common.oracle.ecst.EcstExpressionShape(2, "SIGNED", java.util.Set.of("+"),
                        java.util.Set.of("a", "b")), java.util.Map.of(), "SELECTION_PREDICATE");
        TiDBExecutionContextAnalyzer.ParsedContext parsed = TiDBExecutionContextAnalyzer.parseWithEvidence(
                fixture("two_cast_having"), operation, "UNKNOWN");
        assertEquals(TiDBExecutionContextAnalyzer.MatchStatus.UNIQUE, parsed.status());
        assertEquals(TiDBExecutionContextAnalyzer.ExpressionRole.SELECTION_PREDICATE,
                parsed.selectedExpression().role());
    }

    @Test
    void realPlansConfirmOnlyRequestedDirection() throws Exception {
        EcstContext root = TiDBExecutionContextAnalyzer.parse(fixture("projection"), "CAST(`v` AS SIGNED)");
        EcstContext cop = TiDBExecutionContextAnalyzer.parse(fixture("selection"), "CAST(`v` AS SIGNED)");
        sqlancer.common.oracle.ecst.EcstTarget target = new sqlancer.common.oracle.ecst.EcstTarget("direction",
                sqlancer.common.oracle.ecst.EcstContextDimension.LOCATION, "ROOT", "COP_TIKV");
        assertEquals(sqlancer.common.oracle.ecst.EcstShiftStatus.SUCCESS,
                TiDBExecutionContextAnalyzer.matchTarget(root, cop, target));
        assertEquals(sqlancer.common.oracle.ecst.EcstShiftStatus.FAILURE,
                TiDBExecutionContextAnalyzer.matchTarget(cop, root, target));
    }
}
