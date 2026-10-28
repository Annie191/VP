package sqlancer.tidb.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import sqlancer.common.oracle.ecst.EcstContext;

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
