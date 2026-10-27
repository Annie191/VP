package sqlancer.common.oracle.ecst;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/** Result of comparing the plans of an original and transformed query. */
public final class EcstObservation {
    private final EcstContext original;
    private final EcstContext transformed;
    private final EcstShiftStatus status;
    private final Set<String> observedShifts;
    private final String reason;
    private final String originalPlan;
    private final String transformedPlan;
    private final String originalMatchedNode;
    private final String transformedMatchedNode;
    private final String originalEvidence;
    private final String transformedEvidence;

    public EcstObservation(EcstContext original, EcstContext transformed, EcstShiftStatus status,
            Set<String> observedShifts, String reason) {
        this(original, transformed, status, observedShifts, reason, "", "", "", "", "", "");
    }

    public EcstObservation(EcstContext original, EcstContext transformed, EcstShiftStatus status,
            Set<String> observedShifts, String reason, String originalPlan, String transformedPlan,
            String originalMatchedNode, String transformedMatchedNode, String originalEvidence,
            String transformedEvidence) {
        this.original = original;
        this.transformed = transformed;
        this.status = status;
        this.observedShifts = Collections.unmodifiableSet(new LinkedHashSet<>(observedShifts));
        this.reason = reason == null ? "" : reason;
        this.originalPlan = originalPlan == null ? "" : originalPlan;
        this.transformedPlan = transformedPlan == null ? "" : transformedPlan;
        this.originalMatchedNode = originalMatchedNode == null ? "" : originalMatchedNode;
        this.transformedMatchedNode = transformedMatchedNode == null ? "" : transformedMatchedNode;
        this.originalEvidence = originalEvidence == null ? "" : originalEvidence;
        this.transformedEvidence = transformedEvidence == null ? "" : transformedEvidence;
    }

    public EcstContext getOriginal() {
        return original;
    }

    public EcstContext getTransformed() {
        return transformed;
    }

    public EcstShiftStatus getStatus() {
        return status;
    }

    public Set<String> getObservedShifts() {
        return observedShifts;
    }

    public String getReason() {
        return reason;
    }

    public String getOriginalPlan() {
        return originalPlan;
    }

    public String getTransformedPlan() {
        return transformedPlan;
    }

    public String getOriginalMatchedNode() {
        return originalMatchedNode;
    }

    public String getTransformedMatchedNode() {
        return transformedMatchedNode;
    }

    public String getOriginalEvidence() {
        return originalEvidence;
    }

    public String getTransformedEvidence() {
        return transformedEvidence;
    }
}
