package sqlancer.common.oracle.ecst;

import java.util.Objects;
import java.util.Locale;

/** Query-side features used to condition ECST feedback without making every combination a separate state. */
public final class EcstFeature {
    private final String dbms;
    private final String expressionType;
    private final String clause;
    private final String dependencyClass;
    private final String queryShape;

    public EcstFeature(String dbms, String expressionType, String clause, String dependencyClass) {
        this(dbms, expressionType, clause, dependencyClass, "UNKNOWN");
    }

    public EcstFeature(String dbms, String expressionType, String clause, String dependencyClass, String queryShape) {
        this.dbms = normalize(dbms);
        this.expressionType = normalize(expressionType);
        this.clause = normalize(clause);
        this.dependencyClass = normalize(dependencyClass);
        this.queryShape = normalize(queryShape);
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

    public String getQueryShape() {
        return queryShape;
    }

    /** Full feature bucket. */
    public String key() {
        return "UNKNOWN".equals(queryShape) ? dbms + ":" + expressionType + ":" + clause + ":" + dependencyClass
                : dbms + ":" + expressionType + ":" + clause + ":" + dependencyClass + ":" + queryShape;
    }

    /** Coarser fallback bucket used when the full feature has too few samples. */
    public String coarseKey() {
        return "UNKNOWN".equals(queryShape) ? dbms + ":" + expressionType + ":" + clause
                : dbms + ":" + expressionType + ":" + clause + ":" + queryShape;
    }

    @Override
    public String toString() {
        return key();
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbms, expressionType, clause, dependencyClass, queryShape);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof EcstFeature)) {
            return false;
        }
        EcstFeature that = (EcstFeature) other;
        return dbms.equals(that.dbms) && expressionType.equals(that.expressionType)
                && clause.equals(that.clause) && dependencyClass.equals(that.dependencyClass)
                && queryShape.equals(that.queryShape);
    }
}
