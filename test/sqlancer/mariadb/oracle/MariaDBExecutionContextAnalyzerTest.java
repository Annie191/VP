package sqlancer.mariadb.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstExpressionShape;
import sqlancer.common.oracle.ecst.EcstOperation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;

class MariaDBExecutionContextAnalyzerTest {
    private static String plan(String fixture) throws Exception {
        return Files.readString(Path.of("test/sqlancer/mariadb/oracle/fixtures/ecst", fixture + ".json"));
    }

    private static EcstOperation operation(String expression, String role) {
        Set<String> dependencies = Set.of("c0", "c1");
        return new EcstOperation("test", expression, dependencies,
                new EcstExpressionShape(0, "UNKNOWN", Set.of(), dependencies), Map.of(), role);
    }

    @Test
    void scanAndJoinBufferConditionsHaveDistinctRolesAndLocations() throws Exception {
        EcstOperation operation = operation("`c0`+`c1`>0", "SELECTION_PREDICATE");
        MariaDBExecutionContextAnalyzer.ParsedContext before = MariaDBExecutionContextAnalyzer.parse(plan("scan"),
                operation, Set.of("src"));
        MariaDBExecutionContextAnalyzer.ParsedContext after = MariaDBExecutionContextAnalyzer.parse(plan("bnl-join"),
                operation, Set.of("l", "r"));
        assertTrue(before.isOperationMatched(), before.getEvidence());
        assertTrue(after.isOperationMatched(), after.getEvidence());
        assertEquals("SCAN_FILTER", before.context().getOperator());
        assertEquals("PRE_JOIN", before.context().getLocation());
        assertEquals("JOIN_CONDITION", after.context().getOperator());
        assertEquals("POST_JOIN", after.context().getLocation());
        assertEquals(EcstShiftStatus.SUCCESS,
                MariaDBExecutionContextAnalyzer.observe(before, after,
                        new EcstTarget("operator", EcstContextDimension.OPERATOR, "SCAN_FILTER", "JOIN_CONDITION"))
                        .getStatus());
    }

    @Test
    void ordinaryAttachedConditionUsingBothInputsIsAFilterAfterTheJoin() throws Exception {
        ObjectMapper json = new ObjectMapper();
        JsonNode plan = json.readTree(plan("bnl-join"));
        com.fasterxml.jackson.databind.node.ArrayNode loop = (com.fasterxml.jackson.databind.node.ArrayNode) plan
                .path("query_block").path("nested_loop");
        ObjectNode buffer = (ObjectNode) loop.get(1).path("block-nl-join");
        ObjectNode table = (ObjectNode) buffer.path("table");
        table.put("attached_condition", buffer.path("attached_condition").asText());
        ObjectNode entry = json.createObjectNode();
        entry.set("table", table);
        loop.set(1, entry);
        MariaDBExecutionContextAnalyzer.ParsedContext context = MariaDBExecutionContextAnalyzer.parse(plan.toString(),
                operation("`c0`+`c1`>0", "SELECTION_PREDICATE"), Set.of("l", "r"));
        assertTrue(context.isOperationMatched(), context.getEvidence());
        assertEquals("FILTER", context.context().getOperator());
        assertEquals("POST_JOIN", context.context().getLocation());
    }

    @Test
    void materializedProducerLineageMapsAnOuterDerivedCondition() throws Exception {
        EcstOperation operation = operation("`c0`+`c1`>0", "SELECTION_PREDICATE");
        MariaDBExecutionContextAnalyzer.ParsedContext before = MariaDBExecutionContextAnalyzer.parse(plan("scan"),
                operation, Set.of("src"));
        MariaDBExecutionContextAnalyzer.ParsedContext after = MariaDBExecutionContextAnalyzer
                .parse(plan("materialized"), operation, Set.of("src"));
        assertTrue(after.isOperationMatched(), after.getEvidence());
        assertEquals("MATERIALIZED", after.context().getBoundary());
        assertEquals(EcstShiftStatus.SUCCESS,
                MariaDBExecutionContextAnalyzer
                        .observe(before, after,
                                new EcstTarget("boundary", EcstContextDimension.BOUNDARY, "MERGED", "MATERIALIZED"))
                        .getStatus());
    }

    @Test
    void unrelatedMaterializedSubqueryDoesNotEstablishTheSourceBoundary() throws Exception {
        ObjectMapper json = new ObjectMapper();
        JsonNode scan = json.readTree(plan("scan"));
        ((ObjectNode) scan.path("query_block")).set("subqueries",
                json.createArrayNode().add(json.readTree(plan("materialized"))));
        assertEquals("MERGED",
                MariaDBExecutionContextAnalyzer
                        .parse(scan.toString(), operation("`c0`+`c1`>0", "SELECTION_PREDICATE"), Set.of("src"))
                        .context().getBoundary());
    }

    @Test
    void accessTypeRequiresNativeSelectionRatherThanPossibleKeys() throws Exception {
        EcstOperation operation = operation("`c0`+`c1`>0", "SELECTION_PREDICATE");
        ObjectMapper json = new ObjectMapper();
        JsonNode scan = json.readTree(plan("scan"));
        ObjectNode table = (ObjectNode) scan.path("query_block").path("nested_loop").get(0).path("table");
        table.putArray("possible_keys").add("idx");
        assertEquals("TABLE_SCAN", MariaDBExecutionContextAnalyzer.parse(scan.toString(), operation, Set.of("src"))
                .context().getAccessPath());
        for (String[] pair : new String[][] { { "range", "INDEX_RANGE" }, { "ref", "INDEX_LOOKUP" },
                { "eq_ref", "INDEX_LOOKUP" }, { "index", "INDEX_SCAN" } }) {
            table.put("access_type", pair[0]);
            table.put("key", "idx");
            assertEquals(pair[1], MariaDBExecutionContextAnalyzer.parse(scan.toString(), operation, Set.of("src"))
                    .context().getAccessPath());
        }
        assertEquals("UNKNOWN", MariaDBExecutionContextAnalyzer
                .parse(scan.toString(), operation, Set.of("src", "missing")).context().getAccessPath());
    }

    @Test
    void duplicatePlacementsMissingInputsAndProjectionAliasesRemainUnknown() throws Exception {
        EcstOperation operation = operation("`c0`+`c1`>0", "SELECTION_PREDICATE");
        String duplicate = plan("scan").replace("src.c0 + src.c1 > 0", "src.c0 + src.c1 > 0 and src.c0 + src.c1 > 0");
        assertFalse(MariaDBExecutionContextAnalyzer.parse(duplicate, operation, Set.of("src")).isOperationMatched());
        assertFalse(MariaDBExecutionContextAnalyzer.parse(plan("bnl-join"), operation, Set.of("l", "missing"))
                .isOperationMatched());
        assertFalse(MariaDBExecutionContextAnalyzer
                .parse(plan("scan"), operation("ecst_value", "PROJECTION_ITEM"), Set.of("src")).isOperationMatched());
        assertFalse(MariaDBExecutionContextAnalyzer.parse("{}", operation, Set.of("src")).isOperationMatched());
        assertFalse(
                MariaDBExecutionContextAnalyzer.parse("SELECT c0+c1>0", operation, Set.of("src")).isOperationMatched());
    }
}
