package sqlancer.common.oracle.ecst;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Structured per-phase failure information suitable for JSONL diagnostics. */
public final class EcstFailureRecord {
    private final EcstPhase phase;
    private final String target;
    private final String config;
    private final String baseTable;
    private final String originalQuery;
    private final String transformedQuery;
    private final String targetExpression;
    private final String message;
    private final String sqlState;
    private final int errorCode;
    private final String stackTrace;
    private final List<String> setupStatements;
    private final String generationInfo;

    public EcstFailureRecord(EcstPhase phase, String target, String config, String baseTable, String originalQuery,
            String transformedQuery, String targetExpression, String message, String stackTrace,
            List<String> setupStatements, String generationInfo) {
        this(phase, target, config, baseTable, originalQuery, transformedQuery, targetExpression, message, "", 0,
                stackTrace, setupStatements, generationInfo);
    }

    public EcstFailureRecord(EcstPhase phase, String target, String config, String baseTable, String originalQuery,
            String transformedQuery, String targetExpression, String message, String sqlState, int errorCode,
            String stackTrace, List<String> setupStatements, String generationInfo) {
        this.phase = phase;
        this.target = value(target);
        this.config = value(config);
        this.baseTable = value(baseTable);
        this.originalQuery = value(originalQuery);
        this.transformedQuery = value(transformedQuery);
        this.targetExpression = value(targetExpression);
        this.message = value(message);
        this.sqlState = value(sqlState);
        this.errorCode = errorCode;
        this.stackTrace = value(stackTrace);
        this.setupStatements = Collections.unmodifiableList(new ArrayList<>(setupStatements == null ? List.of() : setupStatements));
        this.generationInfo = value(generationInfo);
    }

    private static String value(String value) {
        return value == null ? "" : value;
    }

    public EcstPhase getPhase() { return phase; }
    public String getTarget() { return target; }
    public String getConfig() { return config; }
    public String getBaseTable() { return baseTable; }
    public String getOriginalQuery() { return originalQuery; }
    public String getTransformedQuery() { return transformedQuery; }
    public String getTargetExpression() { return targetExpression; }
    public String getMessage() { return message; }
    public String getSqlState() { return sqlState; }
    public int getErrorCode() { return errorCode; }
    public String getStackTrace() { return stackTrace; }
    public List<String> getSetupStatements() { return setupStatements; }
    public String getGenerationInfo() { return generationInfo; }

    public String toJsonLine() {
        return "{\"type\":\"failure\",\"phase\":\"" + json(phase == null ? "UNKNOWN" : phase.name())
                + "\",\"target\":\"" + json(target) + "\",\"config\":\"" + json(config)
                + "\",\"baseTable\":\"" + json(baseTable) + "\",\"originalQuery\":\""
                + json(originalQuery) + "\",\"transformedQuery\":\"" + json(transformedQuery)
                + "\",\"targetExpression\":\"" + json(targetExpression) + "\",\"message\":\""
                + json(message) + "\",\"sqlState\":\"" + json(sqlState) + "\",\"errorCode\":"
                + errorCode + ",\"stackTrace\":\"" + json(stackTrace) + "\",\"setup\":"
                + jsonArray(setupStatements) + ",\"generationInfo\":\"" + json(generationInfo) + "\"}";
    }

    public static String json(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder result = new StringBuilder(value.length() + 16);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
            case '\\':
                result.append("\\\\");
                break;
            case '"':
                result.append("\\\"");
                break;
            case '\r':
                result.append("\\r");
                break;
            case '\n':
                result.append("\\n");
                break;
            case '\t':
                result.append("\\t");
                break;
            case '\b':
                result.append("\\b");
                break;
            case '\f':
                result.append("\\f");
                break;
            default:
                if (c < 0x20) {
                    result.append(String.format("\\u%04x", (int) c));
                } else {
                    result.append(c);
                }
            }
        }
        return result.toString();
    }

    public static String jsonArray(List<String> values) {
        StringBuilder result = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) result.append(',');
            result.append('"').append(json(values.get(i))).append('"');
        }
        return result.append(']').toString();
    }
}
