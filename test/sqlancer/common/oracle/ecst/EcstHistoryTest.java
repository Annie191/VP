package sqlancer.common.oracle.ecst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
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
    public void transformSelectionUsesObservedSuccessRateAndCost() {
        EcstHistory history = new EcstHistory();
        for (int i = 0; i < 12; i++) {
            history.observe("target", "slow-failing", EcstShiftStatus.FAILURE, Set.of(), 10_000_000_000L);
            history.observe("target", "fast-success", EcstShiftStatus.SUCCESS, Set.of("LOCATION"), 1L);
        }
        assertEquals("fast-success", history.chooseTransform("target", List.of("slow-failing", "fast-success")));
    }
}
