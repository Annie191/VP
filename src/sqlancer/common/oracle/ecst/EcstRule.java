package sqlancer.common.oracle.ecst;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/** A bounded applicability rule for target-driven transformation synthesis. */
public final class EcstRule {
    private final String name;
    private final String targetKey;
    private final Set<EcstTransformationConfig.Representation> representations;
    private final Set<EcstTransformationConfig.Reconstruction> reconstructions;
    private final Set<EcstTransformationConfig.ExpressionContext> expressionContexts;

    public EcstRule(String name, String targetKey, Set<EcstTransformationConfig.Representation> representations,
            Set<EcstTransformationConfig.Reconstruction> reconstructions,
            Set<EcstTransformationConfig.ExpressionContext> expressionContexts) {
        this.name = Objects.requireNonNull(name);
        this.targetKey = Objects.requireNonNull(targetKey);
        this.representations = EnumSet.copyOf(representations);
        this.reconstructions = EnumSet.copyOf(reconstructions);
        this.expressionContexts = EnumSet.copyOf(expressionContexts);
    }

    public String getName() {
        return name;
    }

    public String getTargetKey() {
        return targetKey;
    }

    public boolean supports(EcstTransformationConfig config) {
        return representations.contains(config.getRepresentation()) && reconstructions.contains(config.getReconstruction())
                && expressionContexts.contains(config.getExpressionContext());
    }
}
