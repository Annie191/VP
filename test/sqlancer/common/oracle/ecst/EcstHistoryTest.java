package sqlancer.common.oracle.ecst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class EcstHistoryTest {
    @Test
    public void unknownDoesNotBecomeAFalseFailureAndObservedShiftsAreTracked() {
        EcstHistory history = new EcstHistory();
        history.observe("target", "config", EcstShiftStatus.UNKNOWN, Set.of(), 10);
        history.observe("target", "config", EcstShiftStatus.SUCCESS, Set.of("LOCATION:ROOT->COP_TIKV"), 20);

        assertEquals(2, history.getTotalTrials());
        assertTrue(history.hasObservedShift("LOCATION:ROOT->COP_TIKV"));
        assertEquals(1, history.observedShiftCount());
        assertEquals("config", history.chooseTransform("target", List.of("config")));
        assertNotNull(history.formatSummary());
    }

    @Test
    public void targetSelectionKeepsUncoveredTargetsEligible() {
        EcstHistory history = new EcstHistory();
        history.observe("covered", "config", EcstShiftStatus.SUCCESS, Set.of("LOCATION:ROOT->COP_TIKV"), 10);
        String selected = history.chooseTarget(List.of("covered", "uncovered"));
        assertNotNull(selected);
        assertTrue(selected.equals("covered") || selected.equals("uncovered"));
    }

    @Test
    public void coverageCountsOnlyTheTargetMigrationDimension() {
        EcstHistory history = new EcstHistory();
        history.observe("TIDB_LOCATION_SHIFT", "config", EcstShiftStatus.FAILURE,
                Set.of("ACCESS_PATH:TABLEFULLSCAN->INDEXLOOKUP"), 10);

        assertEquals(0, history.getTargetCoverage("TIDB_LOCATION_SHIFT"));
        assertEquals(1, history.getShiftCoverage("ACCESS_PATH:TABLEFULLSCAN->INDEXLOOKUP"));

        history.observe("TIDB_LOCATION_SHIFT", "config", EcstShiftStatus.SUCCESS,
                List.of("LOCATION:ROOT->COP_TIKV", "LOCATION:ROOT->COP_TIKV"), 10);
        assertEquals(1, history.getTargetCoverage("TIDB_LOCATION_SHIFT"));
        assertEquals(1, history.getShiftCoverage("LOCATION:ROOT->COP_TIKV"));
        assertEquals(2, history.getTotalTrials());
    }

    @Test
    public void transformSelectionUsesObservedSuccessRateAndCost() {
        EcstHistory history = new EcstHistory();
        for (int i = 0; i < 12; i++) {
            history.observe("target", "slow-failing", EcstShiftStatus.FAILURE, Set.of(), 10_000_000_000L);
            history.observe("target", "fast-success", EcstShiftStatus.SUCCESS, Set.of("LOCATION"), 1L);
        }
        assertEquals("fast-success", history.chooseTransform("target", List.of("slow-failing", "fast-success")));
    }

    @Test
    public void featureFeedbackUsesAHierarchicalFallback() {
        EcstHistory history = new EcstHistory();
        EcstFeature feature = new EcstFeature("TiDB", "CAST_ARITHMETIC", "WHERE", "NUMERIC_DEPENDENCY");
        for (int i = 0; i < 3; i++) {
            history.observe("target", "config", EcstShiftStatus.SUCCESS, Set.of("LOCATION"), 1L, feature);
        }

        EcstHistory.Decision decision = history.chooseTransformDecision("target", Map.of("config", feature));

        assertNotNull(decision);
        assertTrue(history.formatSummary().contains("feature[target|config|TIDB:CAST_ARITHMETIC:WHERE:NUMERIC_DEPENDENCY]"));
    }
}
