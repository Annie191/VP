package sqlancer.common.oracle.ecst;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/** Structural fingerprint of a target expression used for plan-node matching. */
public final class EcstExpressionShape {
    private final int castCount;
    private final String castTarget;
    private final Set<String> operators;
    private final Set<String> dependencies;

    public EcstExpressionShape(int castCount, String castTarget, Set<String> operators, Set<String> dependencies) {
        this.castCount = castCount;
        this.castTarget = castTarget == null ? "UNKNOWN" : castTarget;
        this.operators = Collections.unmodifiableSet(new LinkedHashSet<>(operators == null ? Set.of() : operators));
        this.dependencies = Collections.unmodifiableSet(new LinkedHashSet<>(dependencies == null ? Set.of() : dependencies));
    }

    public int getCastCount() { return castCount; }
    public String getCastTarget() { return castTarget; }
    public Set<String> getOperators() { return operators; }
    public Set<String> getDependencies() { return dependencies; }
}
