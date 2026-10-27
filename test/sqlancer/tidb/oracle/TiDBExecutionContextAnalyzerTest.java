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
        assertEquals("UNKNOWN", context.getAccessPath());
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

        EcstContext context = TiDBExecutionContextAnalyzer.parseWithEvidence(plan, TARGET,
                "sql_mode=STRICT_TRANS_TABLES;collation=utf8mb4_bin;charset=utf8mb4;targetTypeConversion=CAST")
                .context();

        assertTrue(context.getEvaluation().contains("sql_mode=STRICT_TRANS_TABLES"));
        assertTrue(context.getEvaluation().contains("targetTypeConversion=CAST"));
    }
}
