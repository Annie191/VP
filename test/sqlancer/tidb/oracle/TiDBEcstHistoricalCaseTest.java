package sqlancer.tidb.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import sqlancer.common.oracle.ecst.EcstHistoricalRule;

/** Offline acceptance tests for the VP cases represented in ECST vocabulary. */
public class TiDBEcstHistoricalCaseTest {
    private static final Path ROOT = Path.of("test/sqlancer/tidb/oracle/fixtures/ecst/historical_cases");
    private static final Path PLANS = Path.of("test/sqlancer/tidb/oracle/fixtures/ecst/historical-plans.jsonl");

    @Test
    void everyHistoricalVpCaseHasAnEcstDescriptorAndExecutableFixture() {
        List<TiDBEcstHistoricalCase> cases = TiDBEcstHistoricalCase.all();
        assertEquals(16, cases.size());
        Set<String> ids = new HashSet<>();
        for (TiDBEcstHistoricalCase historical : cases) {
            assertTrue(ids.add(historical.getIssue()));
            Path fixture = ROOT.resolve(historical.getIssue());
            assertTrue(Files.isRegularFile(fixture.resolve("setup.sql")), historical.getIssue());
            assertTrue(Files.isRegularFile(fixture.resolve("original.sql")), historical.getIssue());
            assertTrue(Files.isRegularFile(fixture.resolve("vp.sql")), historical.getIssue());
            assertFalse(historical.targetKey().isEmpty());
            assertFalse(historical.configKey().isEmpty());
        }
    }

    @Test
    void historicalPlanReplayContainsAResultDifferenceForEveryCase() throws Exception {
        List<String> lines = Files.readAllLines(PLANS);
        assertEquals(16, lines.size());
        for (String line : lines) {
            assertTrue(line.contains("\"status\":\"DIFFERENT\""), line);
            assertTrue(line.contains("originalPlan"), line);
            assertTrue(line.contains("transformedPlan"), line);
        }
    }

    @Test
    void historicalRulesExposeConcreteBoundaryAndConfigurationEvidence() {
        List<EcstHistoricalRule> rules = TiDBEcstRuleLibrary.historicalRules();
        assertEquals(16, rules.size());
        assertTrue(rules.stream().anyMatch(rule -> rule.getTargetKey().contains("LOCATION_COP_TIKV_TO_ROOT")));
        assertTrue(rules.stream().anyMatch(rule -> rule.getTargetKey().contains("ACCESS_PATH_INDEXREADER_TO_TABLEFULLSCAN")));
        assertTrue(rules.stream().anyMatch(rule -> rule.getConfiguration().contains("VIEW")));
        assertTrue(rules.stream().anyMatch(rule -> rule.getConfiguration().contains("CTE")));
        assertTrue(rules.stream().allMatch(rule -> !rule.getCaseIds().isEmpty() && !rule.getEvidence().isEmpty()));
    }
}
