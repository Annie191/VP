package sqlancer.common.oracle.ecst;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/** Initial TiDB-capability rule set. Unsupported rules can be added without changing the oracle loop. */
public final class EcstRuleLibrary {
    private EcstRuleLibrary() {
    }

    public static List<EcstRule> tiDbRules() {
        Set<EcstTransformationConfig.Representation> representations = Set
                .of(EcstTransformationConfig.Representation.VERTICAL_PARTITIONED);
        Set<EcstTransformationConfig.Reconstruction> reconstructions = Set.of(
                EcstTransformationConfig.Reconstruction.JOIN,
                EcstTransformationConfig.Reconstruction.DERIVED_TABLE);
        Set<EcstTransformationConfig.ExpressionContext> contexts = Set
                .of(EcstTransformationConfig.ExpressionContext.WHERE);
        return Arrays.asList(
                new EcstRule("Root-Cop relocation", "TIDB_LOCATION_SHIFT", representations, reconstructions,
                        contexts),
                new EcstRule("Predicate pushdown boundary", "TIDB_PUSHDOWN_SHIFT", representations, reconstructions,
                        contexts),
                new EcstRule("Access path shift", "TIDB_ACCESS_PATH_SHIFT", representations, reconstructions,
                        contexts));
    }
}
