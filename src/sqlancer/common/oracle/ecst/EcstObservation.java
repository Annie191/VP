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

    public EcstObservation(EcstContext original, EcstContext transformed, EcstShiftStatus status,
            Set<String> observedShifts, String reason) {
        this.original = original;
        this.transformed = transformed;
        this.status = status;
        this.observedShifts = Collections.unmodifiableSet(new LinkedHashSet<>(observedShifts));
        this.reason = reason == null ? "" : reason;
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
}
