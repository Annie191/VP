package sqlancer.common.oracle.ecst;

import java.util.Collections;
import java.util.List;

/**
 * Evidence-backed rule learned from a previously observed VP/ECST-equivalent
 * pair.  Unlike {@link EcstRule}, this type records the concrete boundary and
 * the fixture IDs that support it; it is useful for seed scheduling and audit
 * output without widening the generic configuration product.
 */
public final class EcstHistoricalRule {
    private final String name;
    private final String family;
    private final String targetKey;
    private final String configuration;
    private final List<String> caseIds;
    private final List<String> evidence;

    public EcstHistoricalRule(String name, String family, String targetKey, String configuration,
            List<String> caseIds, List<String> evidence) {
        this.name = name;
        this.family = family;
        this.targetKey = targetKey;
        this.configuration = configuration;
        this.caseIds = Collections.unmodifiableList(caseIds);
        this.evidence = Collections.unmodifiableList(evidence);
    }

    public String getName() {
        return name;
    }

    public String getFamily() {
        return family;
    }

    public String getTargetKey() {
        return targetKey;
    }

    public String getConfiguration() {
        return configuration;
    }

    public List<String> getCaseIds() {
        return caseIds;
    }

    public List<String> getEvidence() {
        return evidence;
    }
}
