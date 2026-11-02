package sqlancer.tidb.oracle;

import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.common.oracle.ecst.EcstTransformationConfig;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;

import java.util.ArrayList;
import java.util.List;

/**
 * The currently implemented TiDB ECST capability set.  This is deliberately
 * separate from semantic legality: a legal ECST configuration may still be a
 * valid future extension that the TiDB adapter cannot materialize yet.
 */
public final class TiDBEcstCapabilities {
    public static final String DESCRIPTION = "TiDB ECST prototype capability set";
    private final boolean tiflashEnabled;

    public TiDBEcstCapabilities() {
        this.tiflashEnabled = false;
    }

    public TiDBEcstCapabilities(TiDBGlobalState state) {
        this.tiflashEnabled = state != null && state.getDbmsSpecificOptions().tiflash;
    }

    public List<EcstTarget> supportedTargets(TiDBGlobalState state) {
        List<EcstTarget> result = new ArrayList<>();
        for (EcstTarget target : TiDBECSTOracle.allTargets()) {
            if (!tiflashEnabled && target.getKey().contains("TIFLASH")) {
                continue;
            }
            result.add(target);
        }
        return result;
    }

    public boolean supportsRepresentation(EcstTransformationConfig.Representation representation) {
        return representation == EcstTransformationConfig.Representation.VERTICAL_PARTITIONED
                || representation == EcstTransformationConfig.Representation.VIEW;
    }

    public boolean supportsReconstruction(EcstTransformationConfig.Reconstruction reconstruction) {
        return reconstruction == EcstTransformationConfig.Reconstruction.JOIN
                || reconstruction == EcstTransformationConfig.Reconstruction.DERIVED_TABLE
                || reconstruction == EcstTransformationConfig.Reconstruction.CTE
                || reconstruction == EcstTransformationConfig.Reconstruction.VIEW;
    }

    public boolean supportsExpressionContext(EcstTransformationConfig.ExpressionContext context) {
        return context == EcstTransformationConfig.ExpressionContext.WHERE
                || context == EcstTransformationConfig.ExpressionContext.SELECT
                || context == EcstTransformationConfig.ExpressionContext.HAVING
                || context == EcstTransformationConfig.ExpressionContext.JOIN_ON
                || context == EcstTransformationConfig.ExpressionContext.INNER_WHERE
                || context == EcstTransformationConfig.ExpressionContext.INNER_SELECT;
    }

    /**
     * Checks target support that depends on the actual seed context.  TiFlash
     * relocation is kept out of this prototype because the VP materializer
     * does not create a TiFlash helper relation.
     */
    public boolean supportsTarget(EcstTarget target, TiDBVerticalPartitioningOracle.EcstSeed seed,
            EcstContext seedContext) {
        if (target == null || seed == null || seedContext == null) {
            return false;
        }
        if (!target.getFrom().equals(seedContext.value(target.getDimension()))) {
            return false;
        }
        switch (target.getDimension()) {
        case LOCATION:
            return supportsLocation(target);
        case BOUNDARY:
            return isOneOf(target.getFrom(), "NON_PUSHDOWN", "PUSHDOWN")
                    && isOneOf(target.getTo(), "NON_PUSHDOWN", "PUSHDOWN");
        case ACCESS_PATH:
            return seed.getAccessPathPreparation().supports(target, seedContext, seed.getExpressionContext());
        case OPERATOR:
            return isOneOf(target.getFrom(), "SELECTION", "MERGEJOIN")
                    && isOneOf(target.getTo(), "SELECTION", "MERGEJOIN")
                    && !target.getFrom().equals(target.getTo());
        default:
            return false;
        }
    }

    public boolean supports(EcstTransformationConfig config, EcstTarget target,
            TiDBVerticalPartitioningOracle.EcstSeed seed, EcstContext seedContext) {
        boolean representationMatchesReconstruction = config != null && supportsRepresentationReconstruction(config);
        return config != null && representationMatchesReconstruction
                && supportsRepresentation(config.getRepresentation())
                && supportsReconstruction(config.getReconstruction())
                && supportsExpressionContext(config.getExpressionContext())
                && supportsTarget(target, seed, seedContext)
                && (target.getDimension() != sqlancer.common.oracle.ecst.EcstContextDimension.OPERATOR
                        || config.getPartitionLayout() == EcstTransformationConfig.PartitionLayout.CROSS_SIDE);
    }

    private static boolean supportsRepresentationReconstruction(EcstTransformationConfig config) {
        if (config.getRepresentation() == EcstTransformationConfig.Representation.VERTICAL_PARTITIONED) {
            return config.getReconstruction() == EcstTransformationConfig.Reconstruction.JOIN
                    || config.getReconstruction() == EcstTransformationConfig.Reconstruction.DERIVED_TABLE
                    || config.getReconstruction() == EcstTransformationConfig.Reconstruction.CTE
                    || config.getReconstruction() == EcstTransformationConfig.Reconstruction.VIEW;
        }
        if (config.getRepresentation() == EcstTransformationConfig.Representation.VIEW) {
            return config.getReconstruction() == EcstTransformationConfig.Reconstruction.DERIVED_TABLE
                    || config.getReconstruction() == EcstTransformationConfig.Reconstruction.CTE
                    || config.getReconstruction() == EcstTransformationConfig.Reconstruction.VIEW;
        }
        return false;
    }

    public String describe() {
        return DESCRIPTION;
    }

    private boolean supportsLocation(EcstTarget target) {
        boolean sourceSupported = isOneOf(target.getFrom(), "ROOT", "COP_TIKV")
                || tiflashEnabled && "COP_TIFLASH".equals(target.getFrom());
        boolean destinationSupported = isOneOf(target.getTo(), "ROOT", "COP_TIKV")
                || tiflashEnabled && "COP_TIFLASH".equals(target.getTo());
        return sourceSupported && destinationSupported;
    }

    private static boolean isOneOf(String value, String first, String second) {
        return first.equals(value) || second.equals(value);
    }
}
