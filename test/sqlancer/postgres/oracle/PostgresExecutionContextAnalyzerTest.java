package sqlancer.postgres.oracle;

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

class PostgresExecutionContextAnalyzerTest {
    private static EcstOperation operation(String expression) {
        return new EcstOperation("test", expression, Set.of("c0", "c1"), "test")
                .withExpectedRole("SELECTION_PREDICATE");
    }

    private static String fixture(String name, String version) throws Exception {
        return Files
                .readString(Path.of("test/sqlancer/postgres/oracle/fixtures/ecst/" + name + "-" + version + ".json"));
    }

    @Test
    void nativeScanAndJoinPredicatesOn12And18() throws Exception {
        for (String version : new String[] { "12", "18" }) {
            PostgresExecutionContextAnalyzer.ParsedContext scan = PostgresExecutionContextAnalyzer
                    .parse(fixture("scan", version), operation("c0>c1"), Set.of("source"));
            PostgresExecutionContextAnalyzer.ParsedContext join = PostgresExecutionContextAnalyzer
                    .parse(fixture("join", version), operation("c0>c1"), Set.of("ecst_l", "ecst_r"));
            assertTrue(scan.isOperationMatched(), scan.getEvidence());
            assertEquals("PRE_JOIN", scan.context().getLocation());
            assertTrue(join.isOperationMatched(), join.getEvidence());
            assertEquals("POST_JOIN", join.context().getLocation());
            assertEquals(EcstShiftStatus.SUCCESS,
                    PostgresExecutionContextAnalyzer.observe(scan, join,
                            new EcstTarget("join", EcstContextDimension.OPERATOR, "SCAN_FILTER", "JOIN_CONDITION"))
                            .getStatus());
        }
    }

    @Test
    void actualMaterializedProducerAndMergedInputOnBothVersions() throws Exception {
        for (String version : new String[] { "12", "18" }) {
            assertEquals("MATERIALIZED",
                    PostgresExecutionContextAnalyzer
                            .parse(fixture("materialized", version), operation("c0>c1"), Set.of("source")).context()
                            .getBoundary());
            assertEquals("MERGED", PostgresExecutionContextAnalyzer
                    .parse(fixture("scan", version), operation("c0>c1"), Set.of("source")).context().getBoundary());
        }
    }

    @Test
    void selectedAccessTypesAndMissingRelations() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        for (String[] type : new String[][] { { "Seq Scan", "TABLE_SCAN" }, { "Index Scan", "INDEX_SCAN" },
                { "Index Only Scan", "INDEX_ONLY_SCAN" }, { "Bitmap Heap Scan", "BITMAP_SCAN" } }) {
            JsonNode json = mapper.readTree(fixture("scan", "18"));
            ((ObjectNode) json.get(0).get("Plan")).put("Node Type", type[0]);
            assertEquals(type[1], PostgresExecutionContextAnalyzer
                    .parse(json.toString(), operation("c0>c1"), Set.of("source")).context().getAccessPath());
        }
        assertEquals("UNKNOWN", PostgresExecutionContextAnalyzer
                .parse(fixture("scan", "18"), operation("c0>c1"), Set.of("missing")).context().getAccessPath());
    }

    @Test
    void duplicatesProjectionAndInvalidJsonStayUnknown() throws Exception {
        String plan = fixture("scan", "18");
        JsonNode json = new ObjectMapper().readTree(plan);
        ((ObjectNode) json.get(0).get("Plan")).put("Filter", "(source.c0 > source.c1) AND (source.c0 > source.c1)");
        assertFalse(PostgresExecutionContextAnalyzer.parse(json.toString(), operation("c0>c1"), Set.of("source"))
                .isOperationMatched());
        assertFalse(PostgresExecutionContextAnalyzer
                .parse(plan, operation("c0>c1").withExpectedRole("PROJECTION_ITEM"), Set.of("source"))
                .isOperationMatched());
        assertEquals("UNKNOWN", PostgresExecutionContextAnalyzer.parse("{}", operation("c0>c1"), Set.of("source"))
                .context().getBoundary());
    }

    @Test
    void unrelatedMaterializedSubplanDoesNotMoveBoundary() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode scan = mapper.readTree(fixture("scan", "18"));
        JsonNode subplan = mapper.readTree(fixture("materialized", "18")).get(0).get("Plan");
        ((ObjectNode) subplan).put("Parent Relationship", "SubPlan");
        ((ObjectNode) subplan.get("Plans").get(0)).put("Relation Name", "unrelated").put("Alias", "unrelated");
        ((ObjectNode) scan.get(0).get("Plan")).putArray("Plans").add(subplan);
        assertEquals("MERGED", PostgresExecutionContextAnalyzer
                .parse(scan.toString(), operation("c0>c1"), Set.of("source")).context().getBoundary());
    }
}
