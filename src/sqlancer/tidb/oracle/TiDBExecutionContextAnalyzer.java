package sqlancer.tidb.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;

/** Small TiDB plan adapter used by the first ECST implementation. */
public final class TiDBExecutionContextAnalyzer {
    private final TiDBGlobalState state;

    public TiDBExecutionContextAnalyzer(TiDBGlobalState state) {
        this.state = state;
    }

    public EcstObservation analyze(String originalQuery, String transformedQuery, String targetExpression) {
        return analyze(originalQuery, transformedQuery, targetExpression, null);
    }

    public EcstObservation analyze(String originalQuery, String transformedQuery, String targetExpression,
            EcstTarget target) {
        try {
            String originalPlan = explain(originalQuery);
            String transformedPlan = explain(transformedQuery);
            EcstContext original = parse(originalPlan, targetExpression);
            EcstContext transformed = parse(transformedPlan, targetExpression);
            if (target == null || target.getDimension() == sqlancer.common.oracle.ecst.EcstContextDimension.LOCATION) {
                if ("UNKNOWN".equals(original.getLocation()) || "UNKNOWN".equals(transformed.getLocation())) {
                    return new EcstObservation(original, transformed, EcstShiftStatus.UNKNOWN, Set.of(),
                            "plan did not expose a Root/Coprocessor location");
                }
            }
            Set<String> shifts = new LinkedHashSet<>();
            addKnownShift(shifts, "LOCATION", original.getLocation(), transformed.getLocation());
            addKnownShift(shifts, "BOUNDARY", original.getBoundary(), transformed.getBoundary());
            addKnownShift(shifts, "ACCESS_PATH", original.getAccessPath(), transformed.getAccessPath());
            addKnownShift(shifts, "OPERATOR", original.getOperator(), transformed.getOperator());
            EcstShiftStatus status = target == null ? (shifts.isEmpty() ? EcstShiftStatus.FAILURE
                    : EcstShiftStatus.SUCCESS) : matchTarget(original, transformed, target);
            return new EcstObservation(original, transformed, status, shifts,
                    status == EcstShiftStatus.SUCCESS ? "confirmed target migration"
                            : status == EcstShiftStatus.FAILURE ? "target migration not observed"
                                    : "target migration cannot be determined");
        } catch (SQLException e) {
            return new EcstObservation(new EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"),
                    new EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"),
                    EcstShiftStatus.UNKNOWN, Set.of(), "EXPLAIN failed: " + e.getMessage());
        }
    }

    private EcstShiftStatus matchTarget(EcstContext original, EcstContext transformed, EcstTarget target) {
        String before = original.value(target.getDimension());
        String after = transformed.value(target.getDimension());
        if ("UNKNOWN".equals(before) || "UNKNOWN".equals(after)) {
            return EcstShiftStatus.UNKNOWN;
        }
        boolean fromMatches = "*".equals(target.getFrom()) || target.getFrom().equals(before);
        boolean toMatches = "*".equals(target.getTo()) || target.getTo().equals(after);
        if (!fromMatches || !toMatches || before.equals(after)) {
            return EcstShiftStatus.FAILURE;
        }
        return EcstShiftStatus.SUCCESS;
    }

    private void addKnownShift(Set<String> shifts, String dimension, String before, String after) {
        if (!"UNKNOWN".equals(before) && !"UNKNOWN".equals(after) && !before.equals(after)) {
            shifts.add(dimension + ":" + before + "->" + after);
        }
    }

    public String explain(String query) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery("EXPLAIN FORMAT=brief " + query)) {
            int columns = result.getMetaData().getColumnCount();
            while (result.next()) {
                for (int i = 1; i <= columns; i++) {
                    if (i > 1) {
                        plan.append(" | ");
                    }
                    String value = result.getString(i);
                    plan.append(value == null ? "NULL" : value);
                }
                plan.append(System.lineSeparator());
            }
        }
        return plan.toString();
    }

    private EcstContext parse(String plan, String targetExpression) {
        String lower = plan.toLowerCase(Locale.ROOT);
        String location;
        if (lower.contains("cop[tikv]")) {
            location = "COP_TIKV";
        } else if (lower.contains("cop[tiflash]")) {
            location = "COP_TIFLASH";
        } else if (lower.contains("root")) {
            location = "ROOT";
        } else {
            location = "UNKNOWN";
        }

        String operator = firstOperator(lower);
        String accessPath = firstAccessPath(lower);
        String boundary = lower.contains("cop[tikv]") || lower.contains("cop[tiflash]") ? "PUSHDOWN"
                : "NON_PUSHDOWN";
        if (targetExpression != null && !targetExpression.isEmpty()
                && !lower.contains(targetExpression.toLowerCase(Locale.ROOT).replace("`", ""))) {
            // TiDB often normalizes CAST and identifiers in EXPLAIN. Keep the context but lower confidence by making
            // the location unknown when the plan has no recognizable target expression at all.
            if (!lower.contains("cast") && !lower.contains("selection") && !lower.contains("projection")) {
                location = "UNKNOWN";
            }
        }
        return new EcstContext(location, operator, boundary, accessPath, "UNKNOWN");
    }

    private String firstOperator(String plan) {
        String[] operators = { "selection", "projection", "hashjoin", "indexjoin", "join", "hashagg", "streamagg",
                "aggregation" };
        for (String operator : operators) {
            if (plan.contains(operator)) {
                return operator.toUpperCase(Locale.ROOT);
            }
        }
        return "UNKNOWN";
    }

    private String firstAccessPath(String plan) {
        String[] paths = { "indexlookup", "indexreader", "indexfullscan", "tablefullscan", "tablescan" };
        for (String path : paths) {
            if (plan.contains(path)) {
                return path.toUpperCase(Locale.ROOT);
            }
        }
        return "UNKNOWN";
    }
}
