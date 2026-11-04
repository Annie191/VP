package sqlancer.common.oracle.ecst;

import java.util.Objects;
import java.util.Locale;

/** Query-side features used to condition ECST feedback without duplicating transformation configuration state. */
public final class EcstFeature {
    private final String dbms;
    private final String expressionType;
    private final String clause;
    /** Intrinsic dependency class; SAME_SIDE/CROSS_SIDE placement is carried by the transform key. */
    private final String dependencyClass;
    private final String queryShape;
    private final String indexAvailability;
    private final String dataScale;
    private final String accessPathCondition;

    public EcstFeature(String dbms, String expressionType, String clause, String dependencyClass) {
        this(dbms, expressionType, clause, dependencyClass, "UNKNOWN");
    }

    public EcstFeature(String dbms, String expressionType, String clause, String dependencyClass, String queryShape) {
        this(dbms, expressionType, clause, dependencyClass, queryShape, "UNKNOWN", "UNKNOWN", "UNKNOWN");
    }

    public EcstFeature(String dbms, String expressionType, String clause, String dependencyClass, String queryShape,
            String indexAvailability, String dataScale, String accessPathCondition) {
        this.dbms = normalize(dbms);
        this.expressionType = normalize(expressionType);
        this.clause = normalize(clause);
        this.dependencyClass = normalize(dependencyClass);
        this.queryShape = normalize(queryShape);
        this.indexAvailability = normalize(indexAvailability);
        this.dataScale = normalize(dataScale);
        this.accessPathCondition = normalize(accessPathCondition);
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

    public String getIndexAvailability() { return indexAvailability; }

    public String getDataScale() { return dataScale; }

    public String getAccessPathCondition() { return accessPathCondition; }

    /** Full feature bucket. */
    public String key() {
        String base = "UNKNOWN".equals(queryShape) ? dbms + ":" + expressionType + ":" + clause + ":"
                + dependencyClass : dbms + ":" + expressionType + ":" + clause + ":" + dependencyClass + ":"
                + queryShape;
        return extendedKey(base);
    }

    /** Coarser fallback bucket used when the full feature has too few samples. */
    public String coarseKey() {
        String base = "UNKNOWN".equals(queryShape) ? dbms + ":" + expressionType + ":" + clause
                : dbms + ":" + expressionType + ":" + clause + ":" + queryShape;
        return extendedKey(base);
    }

    private String extendedKey(String base) {
        if ("UNKNOWN".equals(indexAvailability) && "UNKNOWN".equals(dataScale)
                && "UNKNOWN".equals(accessPathCondition)) {
            return base;
        }
        return base + ":INDEX=" + indexAvailability + ":SCALE=" + dataScale + ":ACCESS=" + accessPathCondition;
    }

    @Override
    public String toString() {
        return key();
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbms, expressionType, clause, dependencyClass, queryShape, indexAvailability, dataScale,
                accessPathCondition);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof EcstFeature)) {
            return false;
        }
        EcstFeature that = (EcstFeature) other;
        return dbms.equals(that.dbms) && expressionType.equals(that.expressionType)
                && clause.equals(that.clause) && dependencyClass.equals(that.dependencyClass)
                && queryShape.equals(that.queryShape) && indexAvailability.equals(that.indexAvailability)
                && dataScale.equals(that.dataScale) && accessPathCondition.equals(that.accessPathCondition);
    }
}
