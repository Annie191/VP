package sqlancer.common.oracle.ecst;

/** A normalized, DBMS-adapter-produced execution context. */
public final class EcstContext {
    private final String location;
    private final String operator;
    private final String boundary;
    private final String accessPath;
    private final String evaluation;
    private final EcstEvaluationEnvironment evaluationEnvironment;

    public EcstContext(String location, String operator, String boundary, String accessPath, String evaluation) {
        this(location, operator, boundary, accessPath, evaluation, EcstEvaluationEnvironment.unavailable());
    }

    public EcstContext(String location, String operator, String boundary, String accessPath,
            EcstEvaluationEnvironment evaluationEnvironment) {
        this(location, operator, boundary, accessPath,
                evaluationEnvironment == null || !evaluationEnvironment.isEvidenceAvailable() ? "UNKNOWN"
                        : evaluationEnvironment.toString(),
                evaluationEnvironment);
    }

    private EcstContext(String location, String operator, String boundary, String accessPath, String evaluation,
            EcstEvaluationEnvironment evaluationEnvironment) {
        this.location = normalize(location);
        this.operator = normalize(operator);
        this.boundary = normalize(boundary);
        this.accessPath = normalize(accessPath);
        this.evaluation = normalize(evaluation);
        this.evaluationEnvironment = evaluationEnvironment == null ? EcstEvaluationEnvironment.unavailable()
                : evaluationEnvironment;
    }

    private static String normalize(String value) {
        return value == null || value.trim().isEmpty() ? "UNKNOWN" : value.trim();
    }

    public String getLocation() {
        return location;
    }

    public String getOperator() {
        return operator;
    }

    public String getBoundary() {
        return boundary;
    }

    public String getAccessPath() {
        return accessPath;
    }

    public String getEvaluation() {
        return evaluation;
    }

    public EcstEvaluationEnvironment getEvaluationEnvironment() {
        return evaluationEnvironment;
    }

    public String value(EcstContextDimension dimension) {
        switch (dimension) {
        case LOCATION:
            return location;
        case OPERATOR:
            return operator;
        case BOUNDARY:
            return boundary;
        case ACCESS_PATH:
            return accessPath;
        case EVALUATION:
            return evaluation;
        default:
            throw new AssertionError(dimension);
        }
    }

    @Override
    public String toString() {
        return "location=" + location + ", operator=" + operator + ", boundary=" + boundary + ", accessPath="
                + accessPath + ", evaluation=" + evaluation;
    }
}
