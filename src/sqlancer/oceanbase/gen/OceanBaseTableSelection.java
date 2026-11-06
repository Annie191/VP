package sqlancer.oceanbase.gen;

import java.util.function.Predicate;

import sqlancer.oceanbase.OceanBaseGlobalState;
import sqlancer.oceanbase.OceanBaseOracleFactory;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;

public final class OceanBaseTableSelection {

    private OceanBaseTableSelection() {
    }

    public static OceanBaseTable getRandomTable(OceanBaseGlobalState globalState) {
        return globalState.getSchema().getRandomTable(table -> isVisibleToRandomActions(globalState, table));
    }

    public static OceanBaseTable getRandomTable(OceanBaseGlobalState globalState, Predicate<OceanBaseTable> predicate) {
        return globalState.getSchema()
                .getRandomTable(table -> isVisibleToRandomActions(globalState, table) && predicate.test(table));
    }

    private static boolean isVisibleToRandomActions(OceanBaseGlobalState globalState, OceanBaseTable table) {
        if (!usesVerticalPartitioningOracle(globalState)) {
            return true;
        }
        return !table.getName().startsWith("vp_");
    }

    private static boolean usesVerticalPartitioningOracle(OceanBaseGlobalState globalState) {
        return globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch(o -> o == OceanBaseOracleFactory.VERTICAL_PARTITIONING || o == OceanBaseOracleFactory.ECST);
    }
}
