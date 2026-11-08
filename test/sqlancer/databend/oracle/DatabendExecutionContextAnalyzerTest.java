package sqlancer.databend.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstExpressionShape;
import sqlancer.common.oracle.ecst.EcstOperation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;

class DatabendExecutionContextAnalyzerTest {
    private static String plan(String fixture) throws Exception {
        return Files.readString(Path.of("test/sqlancer/databend/oracle/fixtures/ecst", fixture + ".txt"));
    }

    private static EcstOperation operation(String expression, String role) {
        Set<String> dependencies = Set.of("c0", "c1");
        return new EcstOperation("test", expression, dependencies,
                new EcstExpressionShape(0, "UNKNOWN", Set.of(), dependencies), Map.of(), role);
    }

    @Test
    void scanPredicateAndPostJoinFilterAreMatchedStructurally() throws Exception {
        EcstOperation operation = operation("`c0`+`c1`>0", "SELECTION_PREDICATE");
        DatabendExecutionContextAnalyzer.ParsedContext before = DatabendExecutionContextAnalyzer.parse(plan("filter"),
                operation, Set.of("src"));
        DatabendExecutionContextAnalyzer.ParsedContext after = DatabendExecutionContextAnalyzer.parse(plan("join"),
                operation, Set.of("pl", "pr"));
        assertTrue(before.isOperationMatched(), before.getEvidence());
        assertTrue(after.isOperationMatched(), after.getEvidence());
        assertEquals("SCAN_FILTER", before.context().getOperator());
        assertEquals("PRE_JOIN", before.context().getLocation());
        assertEquals("FILTER", after.context().getOperator());
        assertEquals("POST_JOIN", after.context().getLocation());
    }

    @Test
    void nativeJoinConditionIsDistinctFromOutputColumnsAndRuntimeFilters() throws Exception {
        DatabendExecutionContextAnalyzer.ParsedContext context = DatabendExecutionContextAnalyzer
                .parse(plan("join-condition"), operation("`c0`>`c1`", "SELECTION_PREDICATE"), Set.of("pl", "pr"));
        assertTrue(context.isOperationMatched(), context.getEvidence());
        assertEquals("JOIN_CONDITION", context.context().getOperator());
    }

    @Test
    void nativeEvalScalarCarriesTheProjectionExpression() throws Exception {
        DatabendExecutionContextAnalyzer.ParsedContext context = DatabendExecutionContextAnalyzer
                .parse(plan("projection"), operation("`c0`+`c1`", "PROJECTION_ITEM"), Set.of("src"));
        assertTrue(context.isOperationMatched(), context.getEvidence());
        assertEquals("PROJECTION", context.context().getOperator());
        assertFalse(DatabendExecutionContextAnalyzer
                .parse(plan("projection").replace("src.c0 (#1) + src.c1 (#2)", "ecst_value (#4)"),
                        operation("`c0`+`c1`", "PROJECTION_ITEM"), Set.of("src"))
                .isOperationMatched());
    }

    @Test
    void opaqueCteScanCannotProveSourceLineageOrMaterialization() throws Exception {
        EcstOperation operation = operation("`c0`+`c1`>0", "SELECTION_PREDICATE");
        DatabendExecutionContextAnalyzer.ParsedContext before = DatabendExecutionContextAnalyzer.parse(plan("filter"),
                operation, Set.of("src"));
        DatabendExecutionContextAnalyzer.ParsedContext after = DatabendExecutionContextAnalyzer.parse(plan("cte"),
                operation, Set.of("src"));
        assertEquals("UNKNOWN", after.context().getBoundary());
        assertEquals("UNKNOWN", after.context().getAccessPath());
        assertFalse(after.isOperationMatched());
        assertEquals(EcstShiftStatus.UNKNOWN,
                DatabendExecutionContextAnalyzer
                        .observe(before, after,
                                new EcstTarget("boundary", EcstContextDimension.BOUNDARY, "MERGED", "MATERIALIZED"))
                        .getStatus());
    }

    @Test
    void syntheticPartitionCountsRequireActualReductionAndValidDenominator() throws Exception {
        String nativePlan = plan("scan");
        EcstOperation operation = operation("`c0`>0", "SELECTION_PREDICATE");
        assertEquals("TABLE_SCAN",
                DatabendExecutionContextAnalyzer.parse(nativePlan, operation, Set.of("src")).context().getAccessPath());
        String reduced = nativePlan.replace("partitions total: 1", "partitions total: 4");
        assertEquals("PARTITION_PRUNED",
                DatabendExecutionContextAnalyzer.parse(reduced, operation, Set.of("src")).context().getAccessPath());
        String invalid = nativePlan.replace("partitions total: 1", "partitions total: 0");
        assertEquals("UNKNOWN",
                DatabendExecutionContextAnalyzer.parse(invalid, operation, Set.of("src")).context().getAccessPath());
        assertEquals("UNKNOWN", DatabendExecutionContextAnalyzer.parse(reduced, operation, Set.of("unrelated"))
                .context().getAccessPath());
    }

    @Test
    void ordinalsAreRemovedOnlyFromNamedExpressionsAndLiteralsArePreserved() {
        assertEquals("src.c0 > '(#7)'", DatabendExecutionContextAnalyzer.normalize("src.c0 (#1) > '(#7)'"));
        assertEquals("#1 > 0", DatabendExecutionContextAnalyzer.normalize("#1 > 0"));
    }

    @Test
    void duplicatePlacementAndMissingRelationEvidenceRemainUnknown() throws Exception {
        EcstOperation operation = operation("`c0`+`c1`>0", "SELECTION_PREDICATE");
        String duplicate = plan("filter").replace("TableScan\n",
                "Filter\n├── filters: [src.c0 (#1) + src.c1 (#2) > 0]\n└── TableScan\n");
        assertFalse(DatabendExecutionContextAnalyzer.parse(duplicate, operation, Set.of("src")).isOperationMatched());
        assertFalse(DatabendExecutionContextAnalyzer.parse(plan("join"), operation, Set.of("pl", "missing"))
                .isOperationMatched());
        assertFalse(DatabendExecutionContextAnalyzer.parse("SELECT c0+c1>0", operation, Set.of("src"))
                .isOperationMatched());
    }
}
