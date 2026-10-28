package sqlancer.common.oracle.ecst;

import java.util.Objects;
import java.util.Locale;

/** Query-side features used to condition ECST feedback without making every combination a separate state. */
public final class EcstFeature {
    private final String dbms;
    private final String expressionType;
    private final String clause;
    private final String dependencyClass;

    public EcstFeature(String dbms, String expressionType, String clause, String dependencyClass) {
        this.dbms = normalize(dbms);
        this.expressionType = normalize(expressionType);
        this.clause = normalize(clause);
        this.dependencyClass = normalize(dependencyClass);
    }

    private static String normalize(String value) {
        return value == null || value.trim().isEmpty() ? "UNKNOWN" : value.trim().toUpperCase(Locale.ROOT);
    }

    public String getDbms() {
        return dbms;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public String getClause() {
        return clause;
    }

    public String getDependencyClass() {
        return dependencyClass;
    }

    /** Full feature bucket. */
    public String key() {
        return dbms + ":" + expressionType + ":" + clause + ":" + dependencyClass;
    }

    /** Coarser fallback bucket used when the full feature has too few samples. */
    public String coarseKey() {
        return dbms + ":" + expressionType + ":" + clause;
    }

    @Override
    public String toString() {
        return key();
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbms, expressionType, clause, dependencyClass);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof EcstFeature)) {
            return false;
        }
        EcstFeature that = (EcstFeature) other;
        return dbms.equals(that.dbms) && expressionType.equals(that.expressionType)
                && clause.equals(that.clause) && dependencyClass.equals(that.dependencyClass);
    }
}
