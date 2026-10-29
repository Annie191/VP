package sqlancer.tidb.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import sqlancer.tidb.oracle.TiDBVerticalPartitioningOracle.DependencyLayout;

public class TiDBDependencyLayoutTest {
    @Test
    void sameSideDoesNotReplicateTargetDependency() {
        assertEquals(DependencyLayout.SAME_SIDE,
                TiDBVerticalPartitioningOracle.classifyDependencyLayoutForTest(List.of("a", "b"),
                        List.of("a", "b"), List.of()));
    }

    @Test
    void crossSideRequiresTwoDistinctDependencies() {
        assertEquals(DependencyLayout.CROSS_SIDE,
                TiDBVerticalPartitioningOracle.classifyDependencyLayoutForTest(List.of("a", "b"),
                        List.of("a"), List.of("b")));
        assertEquals(DependencyLayout.SINGLE_SIDE,
                TiDBVerticalPartitioningOracle.classifyDependencyLayoutForTest(List.of("a"), List.of("a"), List.of()));
    }

    @Test
    void overlappedDependencyIsRejectedForCrossSide() {
        assertEquals(DependencyLayout.OVERLAPPED,
                TiDBVerticalPartitioningOracle.classifyDependencyLayoutForTest(List.of("a", "b"),
                        List.of("a", "b"), List.of("b")));
    }
}
