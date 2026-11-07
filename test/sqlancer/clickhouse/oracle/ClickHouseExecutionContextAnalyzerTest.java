package sqlancer.clickhouse.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import org.junit.jupiter.api.Test;

import sqlancer.clickhouse.ClickHouseOptions;
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstOperation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;

class ClickHouseExecutionContextAnalyzerTest {
    private String fixture(String name) throws Exception {
        return Files.readString(Path.of("test/sqlancer/clickhouse/oracle/fixtures/ecst", name + ".json"));
    }

    private EcstOperation operation(String expression) {
        return new EcstOperation("op", expression, Set.of("c0", "c1"), "test").withExpectedRole("SELECTION_PREDICATE");
    }

    @Test
    void filterMovesAcrossPhysicalJoinWithoutInventingJoinConditionRole() throws Exception {
        ClickHouseExecutionContextAnalyzer.ParsedContext before = ClickHouseExecutionContextAnalyzer
                .parse(fixture("filter"), operation("greater(plus(c0,c1),0)"), Set.of("src"));
        ClickHouseExecutionContextAnalyzer.ParsedContext after = ClickHouseExecutionContextAnalyzer
                .parse(fixture("join"), operation("greater(plus(c0,c1),0)"), Set.of("pl", "pr"));
        assertTrue(before.isOperationMatched());
        assertTrue(after.isOperationMatched());
        assertEquals("FILTER", after.context().getOperator());
        assertEquals(EcstShiftStatus.SUCCESS,
                ClickHouseExecutionContextAnalyzer
                        .observe(before, after,
                                new EcstTarget("loc", EcstContextDimension.LOCATION, "PRE_JOIN", "POST_JOIN"))
                        .getStatus());
        assertFalse(ClickHouseEcstCapabilities.targets().stream()
                .anyMatch(t -> t.getDimension() == EcstContextDimension.OPERATOR));
    }

    @Test
    void nativeMaterializationWorksInBothDirections() throws Exception {
        ClickHouseExecutionContextAnalyzer.ParsedContext a = ClickHouseExecutionContextAnalyzer.parse(fixture("filter"),
                operation("greater(plus(c0,c1),0)"), Set.of("src"));
        ClickHouseExecutionContextAnalyzer.ParsedContext b = ClickHouseExecutionContextAnalyzer.parse(fixture("cte"),
                operation("greater(plus(c0,c1),0)"), Set.of("src"));
        assertEquals("MATERIALIZED", b.context().getBoundary());
        assertEquals(EcstShiftStatus.SUCCESS,
                ClickHouseExecutionContextAnalyzer
                        .observe(a, b, new EcstTarget("b", EcstContextDimension.BOUNDARY, "MERGED", "MATERIALIZED"))
                        .getStatus());
        assertEquals(EcstShiftStatus.SUCCESS,
                ClickHouseExecutionContextAnalyzer
                        .observe(b, a, new EcstTarget("b", EcstContextDimension.BOUNDARY, "MATERIALIZED", "MERGED"))
                        .getStatus());
    }

    @Test
    void anonymousCteConsumersDoNotInventSourceLineage() throws Exception {
        assertEquals("UNKNOWN", ClickHouseExecutionContextAnalyzer
                .parse(fixture("cte"), operation("greater(plus(c0,c1),0)"), Set.of("src")).context().getLocation());
        assertFalse(ClickHouseExecutionContextAnalyzer
                .parse(fixture("filter"), operation("greater(plus(c0,c1),0)"), Set.of("unrelated"))
                .isOperationMatched());
    }

    @Test
    void functionActionsProveProjectionEvaluation() throws Exception {
        ClickHouseExecutionContextAnalyzer.ParsedContext c = ClickHouseExecutionContextAnalyzer.parse(
                fixture("projection"), operation("plus(c0,c1)").withExpectedRole("PROJECTION_ITEM"), Set.of("src"));
        assertTrue(c.isOperationMatched());
        assertEquals("PROJECTION", c.context().getOperator());
    }

    @Test
    void actualPrimaryKeyGranuleReductionIsRequired() throws Exception {
        assertEquals("PRIMARY_PRUNED", ClickHouseExecutionContextAnalyzer
                .parse(fixture("pruned"), operation("greater(c0,0)"), Set.of("keyed")).context().getAccessPath());
        assertEquals("TABLE_SCAN", ClickHouseExecutionContextAnalyzer
                .parse(fixture("filter"), operation("greater(c0,0)"), Set.of("src")).context().getAccessPath());
    }

    @Test
    void unsupportedJsonAndOutputNamesRemainUnknown() {
        assertFalse(ClickHouseExecutionContextAnalyzer
                .parse("[{\"Plan\":{\"Node Type\":\"Expression\",\"Description\":\"greater(c0,0)\"}}]",
                        operation("greater(c0,0)"), Set.of("src"))
                .isOperationMatched());
        assertEquals("UNKNOWN", ClickHouseExecutionContextAnalyzer
                .parse("not json", operation("greater(c0,0)"), Set.of("src")).context().getBoundary());
    }

    @Test
    void normalizationPreservesLiteralContents() {
        assertEquals("greater(c0,0)", ClickHouseExecutionContextAnalyzer.normalize("greater(c0,0_UInt8)"));
        assertEquals("equals(c2,'0_UInt8')", ClickHouseExecutionContextAnalyzer.normalize("equals(c2,'0_UInt8')"));
    }

    @Test
    void distributedVpOptionsAreRejected() {
        ClickHouseOptions options = new ClickHouseOptions();
        ClickHouseEcstSession.validateOptions(options);
        options.verticalPartitioningMultiGatewayProfile = true;
        assertThrows(IllegalArgumentException.class, () -> ClickHouseEcstSession.validateOptions(options));
    }
}
