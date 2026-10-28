package sqlancer.common.oracle.ecst;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/** Target rule envelope; DBMS-specific capability checks happen after this semantic rule filter. */
public final class EcstRuleLibrary {
    private EcstRuleLibrary() {
    }

    public static List<EcstRule> tiDbRules() {
        Set<EcstTransformationConfig.Representation> relationRepresentations = EnumSet.of(
                EcstTransformationConfig.Representation.VERTICAL_PARTITIONED,
                EcstTransformationConfig.Representation.VIEW,
                EcstTransformationConfig.Representation.REPLICATED);
        Set<EcstTransformationConfig.Reconstruction> relationalReconstructions = EnumSet.allOf(
                EcstTransformationConfig.Reconstruction.class);
        return Arrays.asList(
                new EcstRule("Root-Cop relocation", "TIDB_LOCATION_SHIFT", relationRepresentations,
                        relationalReconstructions, EnumSet.allOf(EcstTransformationConfig.ExpressionContext.class)),
                new EcstRule("Predicate pushdown boundary", "TIDB_PUSHDOWN_SHIFT", relationRepresentations,
                        relationalReconstructions, EnumSet.of(EcstTransformationConfig.ExpressionContext.WHERE,
                                EcstTransformationConfig.ExpressionContext.HAVING,
                                EcstTransformationConfig.ExpressionContext.ON)),
                new EcstRule("Access path shift", "TIDB_ACCESS_PATH_SHIFT", relationRepresentations,
                        relationalReconstructions, EnumSet.of(EcstTransformationConfig.ExpressionContext.WHERE,
                                EcstTransformationConfig.ExpressionContext.SELECT,
                                EcstTransformationConfig.ExpressionContext.ON)));
    }
}
