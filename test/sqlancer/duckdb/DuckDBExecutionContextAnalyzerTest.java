package sqlancer.duckdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstOperation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;

class DuckDBExecutionContextAnalyzerTest {
    private String fixture(String name) throws Exception {
        return Files.readString(Path.of("test/sqlancer/duckdb/fixtures/ecst", name + ".json"));
    }

    private EcstOperation op(String expression) {
        return new EcstOperation("op", expression, Set.of("c0", "c1"), "DuckDB")
                .withExpectedRole("SELECTION_PREDICATE");
    }

    @Test
    void filterMovesAcrossNativeJoin() throws Exception {
        DuckDBExecutionContextAnalyzer.ParsedContext before = DuckDBExecutionContextAnalyzer.parse(fixture("filter"),
                op("c0+c1>0"), Set.of("src"));
        DuckDBExecutionContextAnalyzer.ParsedContext after = DuckDBExecutionContextAnalyzer.parse(fixture("join"),
                op("c0+c1>0"), Set.of("pl", "pr"));
        assertTrue(before.isOperationMatched());
        assertTrue(after.isOperationMatched());
        assertEquals(EcstShiftStatus.SUCCESS,
                DuckDBExecutionContextAnalyzer
                        .observe(before, after,
                                new EcstTarget("l", EcstContextDimension.LOCATION, "PRE_JOIN", "POST_JOIN"))
                        .getStatus());
    }

    @Test
    void nativeJoinConditionsAreExpressionEvidence() throws Exception {
        DuckDBExecutionContextAnalyzer.ParsedContext parsed = DuckDBExecutionContextAnalyzer
                .parse(fixture("join-condition"), op("c0>c1"), Set.of("pl", "pr"));
        assertTrue(parsed.isOperationMatched());
        assertEquals("JOIN_CONDITION", parsed.context().getOperator());
    }

    @Test
    void quotedIdentifiersAndReversedComparisonsAreMatchedWithoutEditingLiterals() throws Exception {
        String reversed = fixture("join-condition").replace("c0 > c1", "c1 < c0");
        DuckDBExecutionContextAnalyzer.ParsedContext parsed = DuckDBExecutionContextAnalyzer.parse(reversed,
                op("\"c0\">\"c1\""), Set.of("pl", "pr"));
        assertTrue(parsed.isOperationMatched(), parsed.getEvidence());
        assertEquals("JOIN_CONDITION", parsed.context().getOperator());
        assertEquals("contains(`c2`,'\"c0\"')", DuckDBExecutionContextAnalyzer.normalize("contains(\"c2\",'\"c0\"')"));
    }

    @Test
    void physicalCteBoundaryIsObservedInBothDirections() throws Exception {
        DuckDBExecutionContextAnalyzer.ParsedContext a = DuckDBExecutionContextAnalyzer.parse(fixture("filter"),
                op("c0+c1>0"), Set.of("src"));
        DuckDBExecutionContextAnalyzer.ParsedContext b = DuckDBExecutionContextAnalyzer.parse(fixture("cte"),
                op("c0+c1>0"), Set.of("src"));
        assertEquals("MATERIALIZED", b.context().getBoundary());
        assertEquals(EcstShiftStatus.SUCCESS,
                DuckDBExecutionContextAnalyzer
                        .observe(a, b, new EcstTarget("b", EcstContextDimension.BOUNDARY, "MERGED", "MATERIALIZED"))
                        .getStatus());
        assertEquals(EcstShiftStatus.SUCCESS,
                DuckDBExecutionContextAnalyzer
                        .observe(b, a, new EcstTarget("b", EcstContextDimension.BOUNDARY, "MATERIALIZED", "MERGED"))
                        .getStatus());
    }

    @Test
    void cteConsumerIndexResolvesToItsProducer() throws Exception {
        JsonNode tree = new ObjectMapper().readTree(fixture("cte"));
        // A synthetic variant removes the duplicate pushed producer predicate to isolate consumer lineage.
        removeProducerPredicates(tree);
        DuckDBExecutionContextAnalyzer.ParsedContext c = DuckDBExecutionContextAnalyzer.parse(tree.toString(),
                op("c0+c1>0"), Set.of("src"));
        assertTrue(c.isOperationMatched(), c.getEvidence());
        assertEquals("PRE_JOIN", c.context().getLocation());
        assertFalse(DuckDBExecutionContextAnalyzer.parse(tree.toString(), op("c0+c1>0"), Set.of("unrelated"))
                .isOperationMatched());
    }

    private void removeProducerPredicates(JsonNode node) {
        if (node.path("operator_name").asText().trim().equals("CTE")) {
            clearExpressions(node.path("children").get(0));
        }
        node.path("children").forEach(this::removeProducerPredicates);
    }

    private void clearExpressions(JsonNode node) {
        if (node.path("extra_info").isObject()) {
            ((ObjectNode) node.path("extra_info")).remove("Expression");
        }
        node.path("children").forEach(this::clearExpressions);
    }

    @Test
    void runtimeTypeDistinguishesArtFromStaticSeqScanName() throws Exception {
        DuckDBExecutionContextAnalyzer.ParsedContext c = DuckDBExecutionContextAnalyzer.parse(fixture("index"),
                op("c0=17"), Set.of("src"));
        assertEquals("INDEX_SCAN", c.context().getAccessPath());
        assertEquals("SCAN_FILTER", c.context().getOperator());
        String staticPlan = "[{\"name\":\"SEQ_SCAN\",\"extra_info\":{\"Table\":\"src\",\"Type\":\"Sequential Scan\"},\"children\":[]}]";
        assertEquals("TABLE_SCAN",
                DuckDBExecutionContextAnalyzer.parse(staticPlan, op("c0=17"), Set.of("src")).context().getAccessPath());
    }

    @Test
    void aliasesAndOrdinalProjectionsDoNotProveEvaluation() throws Exception {
        assertFalse(DuckDBExecutionContextAnalyzer
                .parse(fixture("projection"), op("c0+c1").withExpectedRole("PROJECTION_ITEM"), Set.of("src"))
                .isOperationMatched());
        String plan = "[{\"name\":\"PROJECTION\",\"extra_info\":{\"Projections\":\"c0+c1\"},\"children\":[{\"name\":\"SEQ_SCAN\",\"extra_info\":{\"Table\":\"src\",\"Type\":\"Sequential Scan\"}}]}]";
        assertFalse(DuckDBExecutionContextAnalyzer
                .parse(plan, op("c0+c1").withExpectedRole("PROJECTION_ITEM"), Set.of("src")).isOperationMatched());
    }

    @Test
    void duplicatedPredicatePlacementIsAmbiguousAndMalformedPlansRemainUnknown() throws Exception {
        assertEquals("UNKNOWN", DuckDBExecutionContextAnalyzer.parse(fixture("cte"), op("c0+c1>0"), Set.of("src"))
                .context().getLocation());
        assertEquals("UNKNOWN",
                DuckDBExecutionContextAnalyzer.parse("not a plan", op("c0>0"), Set.of("src")).context().getBoundary());
    }
}
