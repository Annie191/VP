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
    public void targetScorePrefersNoveltyWhenReachabilityIsComparable() {
        EcstHistory history = new EcstHistory();
        for (int i = 0; i < 20; i++) {
            history.observe("TIDB_LOCATION_ROOT_TO_COP_TIKV", "config", EcstShiftStatus.SUCCESS,
                    Set.of("LOCATION:ROOT->COP_TIKV"), 1);
            history.observe("uncovered", "config", EcstShiftStatus.SUCCESS,
                    Set.of("ACCESS_PATH:TABLEFULLSCAN->INDEXLOOKUP"), 1);
        }
        EcstHistory.Decision decision = history.chooseTargetDecision(
                Map.of("TIDB_LOCATION_ROOT_TO_COP_TIKV", List.of("config"),
                        "TIDB_LOCATION_SHIFT", List.of("config")));
        assertEquals("TIDB_LOCATION_SHIFT", decision.getKey());
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
    public void directionalTargetsDoNotCountReverseMigrations() {
        EcstHistory history = new EcstHistory();
        String target = "TIDB_LOCATION_ROOT_TO_COP_TIKV";

        history.observe(target, "config", EcstShiftStatus.SUCCESS, Set.of("LOCATION:COP_TIKV->ROOT"), 1);
        assertEquals(0, history.getTargetCoverage(target));

        history.observe(target, "config", EcstShiftStatus.SUCCESS, Set.of("LOCATION:ROOT->COP_TIKV"), 1);
        assertEquals(1, history.getTargetCoverage(target));
    }

    @Test
    public void featureBucketsRemainVisibleAndDoNotCollapseIntoOneKey() {
        EcstHistory history = new EcstHistory();
        EcstFeature where = new EcstFeature("TiDB", "CAST_ARITHMETIC", "WHERE", "NUMERIC_DEPENDENCY");
        EcstFeature select = new EcstFeature("TiDB", "CAST_ARITHMETIC", "SELECT", "NUMERIC_DEPENDENCY");
        for (int i = 0; i < 3; i++) {
            history.observe("target", "config", EcstShiftStatus.SUCCESS, Set.of("LOCATION"), 1, where);
            history.observe("target", "config", EcstShiftStatus.FAILURE, Set.of(), 1, select);
        }

        String summary = history.formatSummary();
        assertTrue(summary.contains(where.key()));
        assertTrue(summary.contains(select.key()));
    }

    @Test
    public void targetReachabilityUsesFeatureConditionedTransformStats() {
        EcstHistory history = new EcstHistory();
        EcstFeature feature = new EcstFeature("TiDB", "CAST_ARITHMETIC", "WHERE", "CROSS_SIDE_DEPENDENCY");
        for (int i = 0; i < 4; i++) {
            history.observe("target-good", "config", EcstShiftStatus.SUCCESS, Set.of("LOCATION"), 1, feature);
            history.observe("target-bad", "config", EcstShiftStatus.FAILURE, Set.of(), 1, feature);
        }
        EcstHistory.Decision decision = history.chooseTargetDecision(
                Map.of("target-good", List.of("config"), "target-bad", List.of("config")), feature);
        assertEquals("target-good", decision.getKey());
    }

    @Test
    public void reachabilityUsesObservedSuccessRateBelowPriorFloor() {
        EcstHistory history = new EcstHistory();
        for (int i = 0; i < 8; i++) {
            history.observe("hard", "theta1", EcstShiftStatus.FAILURE, Set.of(), 1);
            history.observe("slightly-easier", "theta2", EcstShiftStatus.FAILURE, Set.of(), 1);
        }
        history.observe("slightly-easier", "theta2", EcstShiftStatus.SUCCESS, Set.of("LOCATION"), 1);

        EcstHistory.Decision hard = history.chooseTargetDecision(
                Map.of("hard", List.of("theta1")));
        EcstHistory.Decision slightlyEasier = history.chooseTargetDecision(
                Map.of("slightly-easier", List.of("theta2")));
        assertTrue(slightlyEasier.getScore() > hard.getScore());
        assertTrue(hard.getScore() < 2.1, "reachability must not be forced to the 0.5 prior");
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

    @Test
    public void sparseExactFeatureFallsBackToCoarseThenGlobalStatistics() {
        EcstFeature coarse = new EcstFeature("TiDB", "CAST_ARITHMETIC", "WHERE", "NUMERIC_DEPENDENCY");
        EcstFeature sparseExact = new EcstFeature("TiDB", "CAST_ARITHMETIC", "WHERE", "CROSS_SIDE_DEPENDENCY");
        EcstHistory coarseHistory = new EcstHistory();
        for (int i = 0; i < 3; i++) {
            coarseHistory.observe("target", "coarse-good", EcstShiftStatus.SUCCESS, Set.of("LOCATION"), 1, coarse);
            coarseHistory.observe("target", "global-bad", EcstShiftStatus.FAILURE, Set.of(), 1, sparseExact);
        }
        assertEquals("coarse-good", coarseHistory.chooseTransformDecision("target",
                Map.of("coarse-good", sparseExact, "global-bad", sparseExact)).getKey());

        EcstHistory globalHistory = new EcstHistory();
        for (int i = 0; i < 3; i++) {
            globalHistory.observe("target", "global-good", EcstShiftStatus.SUCCESS, Set.of("LOCATION"), 1);
            globalHistory.observe("target", "coarse-bad", EcstShiftStatus.FAILURE, Set.of(), 1, coarse);
        }
        assertEquals("global-good", globalHistory.chooseTransformDecision("target",
                Map.of("global-good", sparseExact, "coarse-bad", sparseExact)).getKey());
    }
}
