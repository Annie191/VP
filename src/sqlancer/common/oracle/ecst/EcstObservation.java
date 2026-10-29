package sqlancer.common.oracle.ecst;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/** Result of comparing the plans of an original and transformed query. */
public final class EcstObservation {
    public enum UnknownReason {
        NONE, UNKNOWN_BECAUSE_PLAN, UNKNOWN_BECAUSE_EVALUATION_EVIDENCE, UNKNOWN_BECAUSE_OPERATION_MAPPING
    }
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
    private final UnknownReason unknownReason;

    public EcstObservation(EcstContext original, EcstContext transformed, EcstShiftStatus status,
            Set<String> observedShifts, String reason) {
        this(original, transformed, status, observedShifts, reason, "", "", "", "", "", "");
    }

    public EcstObservation(EcstContext original, EcstContext transformed, EcstShiftStatus status,
            Set<String> observedShifts, String reason, String originalPlan, String transformedPlan,
            String originalMatchedNode, String transformedMatchedNode, String originalEvidence,
            String transformedEvidence) {
        this(original, transformed, status, observedShifts, reason, originalPlan, transformedPlan, originalMatchedNode,
                transformedMatchedNode, originalEvidence, transformedEvidence, UnknownReason.NONE);
    }

    public EcstObservation(EcstContext original, EcstContext transformed, EcstShiftStatus status,
            Set<String> observedShifts, String reason, String originalPlan, String transformedPlan,
            String originalMatchedNode, String transformedMatchedNode, String originalEvidence,
            String transformedEvidence, UnknownReason unknownReason) {
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
        this.unknownReason = unknownReason == null ? UnknownReason.NONE : unknownReason;
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

    public UnknownReason getUnknownReason() {
        return unknownReason;
    }
}
