package sqlancer.common.oracle.ecst;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/** Stable logical operation identity used when matching plan-node candidates. */
public final class EcstOperation {
    private final String operationId;
    private final String normalizedExpression;
    private final Set<String> dependencies;
    private final EcstExpressionShape shape;

    public EcstOperation(String operationId, String normalizedExpression, Set<String> dependencies, String shape) {
        this(operationId, normalizedExpression, dependencies, new EcstExpressionShape(0, "UNKNOWN", Set.of(),
                dependencies));
    }

    public EcstOperation(String operationId, String normalizedExpression, Set<String> dependencies,
            EcstExpressionShape shape) {
        this.operationId = operationId == null ? "UNKNOWN" : operationId;
        this.normalizedExpression = normalizedExpression == null ? "" : normalizedExpression;
        this.dependencies = Collections.unmodifiableSet(new LinkedHashSet<>(dependencies == null ? Set.of() : dependencies));
        this.shape = shape == null ? new EcstExpressionShape(0, "UNKNOWN", Set.of(), dependencies) : shape;
    }

    public String getOperationId() { return operationId; }
    public String getNormalizedExpression() { return normalizedExpression; }
    public Set<String> getDependencies() { return dependencies; }
    public EcstExpressionShape getShape() { return shape; }
}
