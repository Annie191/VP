package sqlancer.mariadb.oracle;

import java.util.ArrayList;
import java.util.List;

import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.common.oracle.ecst.EcstTransformationConfig;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.ExpressionContext;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.PartitionLayout;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.Reconstruction;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.Representation;
import sqlancer.mariadb.oracle.MariaDBVerticalPartitioningOracle.EcstSeed;

/** MariaDB capabilities describe inducible transitions, never a guarantee that an optimizer chooses them. */
public final class MariaDBEcstCapabilities {
    private MariaDBEcstCapabilities() {
    }

    public static List<EcstTarget> targets() {
        List<EcstTarget> targets = new ArrayList<>();
        addBoth(targets, EcstContextDimension.LOCATION, "PRE_JOIN", "POST_JOIN");
        addBoth(targets, EcstContextDimension.OPERATOR, "FILTER", "JOIN_CONDITION");
        addBoth(targets, EcstContextDimension.OPERATOR, "SCAN_FILTER", "JOIN_CONDITION");
        addBoth(targets, EcstContextDimension.BOUNDARY, "MERGED", "MATERIALIZED");
        for (String access : List.of("INDEX_RANGE", "INDEX_LOOKUP", "INDEX_SCAN")) {
            addBoth(targets, EcstContextDimension.ACCESS_PATH, "TABLE_SCAN", access);
        }
        return targets;
    }

    private static void addBoth(List<EcstTarget> targets, EcstContextDimension dimension, String first, String second) {
        targets.add(new EcstTarget(dimension + ":" + first + "->" + second, dimension, first, second));
        targets.add(new EcstTarget(dimension + ":" + second + "->" + first, dimension, second, first));
    }

    public static boolean isLegal(EcstSeed seed, EcstTransformationConfig config) {
        if (seed == null || config == null || config.getRepresentation() == Representation.ORIGINAL
                || config.getRepresentation() == Representation.REPLICATED) {
            return false;
        }
        if (config.getPartitionLayout() == PartitionLayout.CROSS_SIDE && (seed.dependencies.size() < 2
                || config.getRepresentation() != Representation.VERTICAL_PARTITIONED)) {
            return false;
        }
        if (config.getRepresentation() == Representation.VIEW && config.getReconstruction() == Reconstruction.JOIN) {
            return false;
        }
        ExpressionContext embedded = config.getExpressionContext();
        return embedded == seed.context
                || seed.context == ExpressionContext.WHERE
                        && (embedded == ExpressionContext.INNER_WHERE || embedded == ExpressionContext.JOIN_ON
                                && config.getRepresentation() == Representation.VERTICAL_PARTITIONED)
                || seed.context == ExpressionContext.SELECT && embedded == ExpressionContext.INNER_SELECT;
    }

    public static List<EcstTransformationConfig> configurations(EcstSeed seed, EcstContext context, EcstTarget target) {
        List<EcstTransformationConfig> result = new ArrayList<>();
        if (!target.getFrom().equals(context.value(target.getDimension()))) {
            return result;
        }
        for (Representation representation : Representation.values()) {
            for (PartitionLayout layout : PartitionLayout.values()) {
                for (Reconstruction reconstruction : Reconstruction.values()) {
                    for (ExpressionContext embedded : ExpressionContext.values()) {
                        EcstTransformationConfig config = new EcstTransformationConfig(representation, layout,
                                reconstruction, embedded);
                        if (!isLegal(seed, config)) {
                            continue;
                        }
                        if ((target.getDimension() == EcstContextDimension.LOCATION
                                || target.getDimension() == EcstContextDimension.OPERATOR)
                                && (seed.context == ExpressionContext.SELECT
                                        || representation != Representation.VERTICAL_PARTITIONED)) {
                            continue;
                        }
                        if (target.getDimension() == EcstContextDimension.OPERATOR
                                && layout != PartitionLayout.CROSS_SIDE) {
                            continue;
                        }
                        if (target.getDimension() == EcstContextDimension.ACCESS_PATH
                                && (representation != Representation.VERTICAL_PARTITIONED
                                        || layout == PartitionLayout.CROSS_SIDE || !seed.isIndexable())) {
                            continue;
                        }
                        result.add(config);
                    }
                }
            }
        }
        return result;
    }
}
