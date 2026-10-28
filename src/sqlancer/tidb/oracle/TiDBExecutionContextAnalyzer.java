package sqlancer.tidb.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;

/** Resolves an ECST target to one TiDB EXPLAIN node before extracting its execution context. */
public final class TiDBExecutionContextAnalyzer {
    private static final Pattern LOCATION = Pattern.compile("(?i)(root|cop\\[(?:tikv|tiflash)\\]|mpp\\[[^\\]]+\\])");
    private static final Pattern TOKEN = Pattern.compile("(?i)([a-z_][a-z0-9_$]*(?:\\.[a-z_][a-z0-9_$]*)*)");
    private static final Pattern OPERATOR_ID = Pattern.compile("(?i)^([a-z][a-z0-9]*?)(?:_[0-9]+)?$");
    private static final Set<String> STOP_WORDS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "select", "from", "where", "and", "or", "not", "as", "cast", "convert", "binary", "signed",
            "unsigned", "decimal", "char", "character", "date", "datetime", "time", "true", "false", "null",
            "is", "in", "between", "like", "case", "when", "then", "else", "end", "collate", "charset")));
    private static final Set<String> ACCESS_MARKERS = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
            "indexlookup", "indexreader", "indexfullscan", "indexscan", "tablefullscan", "tablescan",
            "table_scan", "table_reader", "index_reader")));
    private static final Set<String> PREDICATE_MARKERS = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
            "selection", "filter", "condition", "predicate", "projection", "aggregation", "hashagg", "streamagg",
            "gt(", "ge(", "lt(", "le(", "eq(", "ne(", "in(", "like(", "isnull", "notnull", ">", "<", "=")));

    private final TiDBGlobalState state;

    public TiDBExecutionContextAnalyzer(TiDBGlobalState state) {
        this.state = state;
    }

    public EcstObservation analyze(String originalQuery, String transformedQuery, String targetExpression) {
        return analyze(originalQuery, transformedQuery, targetExpression, null);
    }

    public EcstObservation analyze(String originalQuery, String transformedQuery, String targetExpression,
            EcstTarget target) {
        String originalPlan = "";
        String transformedPlan = "";
        try {
            originalPlan = explain(originalQuery);
            transformedPlan = explain(transformedQuery);
            String evaluation;
            try {
                evaluation = readEvaluationContext(targetExpression);
            } catch (SQLException ignored) {
                // Session metadata is supplementary evidence; a failed metadata probe must not discard plan matches.
                evaluation = "UNKNOWN";
            }
            ParsedContext original = parsePlan(originalPlan, targetExpression, evaluation);
            ParsedContext transformed = parsePlan(transformedPlan, targetExpression, evaluation);
            if (!original.matched || !transformed.matched) {
                return observation(original, transformed, EcstShiftStatus.UNKNOWN, Set.of(),
                        "target expression was not uniquely matched in both plans");
            }
            Set<String> shifts = new LinkedHashSet<>();
            addShift(shifts, "LOCATION", original.context.getLocation(), transformed.context.getLocation());
            addShift(shifts, "BOUNDARY", original.context.getBoundary(), transformed.context.getBoundary());
            addShift(shifts, "ACCESS_PATH", original.context.getAccessPath(), transformed.context.getAccessPath());
            addShift(shifts, "OPERATOR", original.context.getOperator(), transformed.context.getOperator());
            EcstShiftStatus status = target == null ? (shifts.isEmpty() ? EcstShiftStatus.FAILURE
                    : EcstShiftStatus.SUCCESS) : matchTarget(original.context, transformed.context, target);
            return observation(original, transformed, status, shifts,
                    status == EcstShiftStatus.SUCCESS ? "confirmed target migration"
                            : status == EcstShiftStatus.FAILURE ? "target migration not observed"
                                    : "target migration cannot be determined");
        } catch (SQLException e) {
            ParsedContext original = ParsedContext.unknown(originalPlan, "EXPLAIN failed: " + e.getMessage());
            ParsedContext transformed = ParsedContext.unknown(transformedPlan, "EXPLAIN failed: " + e.getMessage());
            return observation(original, transformed, EcstShiftStatus.UNKNOWN, Set.of(),
                    "EXPLAIN failed: " + e.getMessage());
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

    /** Package-visible parser entry point for focused analyzer tests. */
    static EcstContext parse(String plan, String targetExpression) {
        return parsePlan(plan, targetExpression, "UNKNOWN").context;
    }

    /** Package-visible evidence entry point for focused analyzer tests. */
    static ParsedContext parseWithEvidence(String plan, String targetExpression, String evaluation) {
        return parsePlan(plan, targetExpression, evaluation);
    }

    private EcstObservation observation(ParsedContext original, ParsedContext transformed, EcstShiftStatus status,
            Set<String> shifts, String reason) {
        return new EcstObservation(original.context, transformed.context, status, shifts,
                reason + "; originalEvidence=" + original.evidence + "; transformedEvidence=" + transformed.evidence,
                original.plan, transformed.plan, original.matchedNode, transformed.matchedNode, original.evidence,
                transformed.evidence);
    }

    private EcstShiftStatus matchTarget(EcstContext original, EcstContext transformed, EcstTarget target) {
        String before = original.value(target.getDimension());
        String after = transformed.value(target.getDimension());
        if ("UNKNOWN".equals(before) || "UNKNOWN".equals(after)) {
            return EcstShiftStatus.UNKNOWN;
        }
        boolean fromMatches = "*".equals(target.getFrom()) || target.getFrom().equals(before);
        boolean toMatches = "*".equals(target.getTo()) || target.getTo().equals(after);
        return fromMatches && toMatches && !before.equals(after) ? EcstShiftStatus.SUCCESS : EcstShiftStatus.FAILURE;
    }

    private static void addShift(Set<String> shifts, String dimension, String before, String after) {
        if (!"UNKNOWN".equals(before) && !"UNKNOWN".equals(after) && !before.equals(after)) {
            shifts.add(dimension + ":" + before + "->" + after);
        }
    }

    private static ParsedContext parsePlan(String plan, String targetExpression, String evaluation) {
        List<PlanNode> nodes = parseNodes(plan);
        TargetPattern target = TargetPattern.create(targetExpression);
        if (target == null || nodes.isEmpty()) {
            return ParsedContext.unknown(plan, "no target expression or no explain nodes");
        }
        List<Candidate> candidates = new ArrayList<>();
        for (PlanNode node : nodes) {
            Candidate candidate = score(node, target);
            if (candidate != null) {
                candidates.add(candidate);
            }
        }
        if (candidates.isEmpty()) {
            return ParsedContext.unknown(plan, "target references were not found in a predicate-bearing plan node");
        }
        candidates.sort((a, b) -> Integer.compare(b.score, a.score));
        Candidate best = candidates.get(0);
        if (candidates.size() > 1 && candidates.get(1).score == best.score
                && !candidates.get(1).node.raw.equals(best.node.raw)) {
            String lines = candidates.stream().filter(c -> c.score == best.score)
                    .map(c -> Integer.toString(c.node.line)).collect(Collectors.joining(","));
            return ParsedContext.unknown(plan, "ambiguous target match at plan lines " + lines);
        }
        EcstContext context = contextFor(best.node, evaluation, target);
        String evidence = "line=" + best.node.line + ", score=" + best.score + ", signals="
                + String.join("+", best.signals) + ", normalizedTarget=" + target.normalized + ", node="
                + best.node.raw;
        return new ParsedContext(context, plan, best.node.raw, evidence, true);
    }

    private static List<PlanNode> parseNodes(String plan) {
        if (plan == null || plan.trim().isEmpty()) {
            return List.of();
        }
        List<PlanNode> nodes = new ArrayList<>();
        String[] lines = plan.split("\\R");
        for (int i = 0; i < lines.length; i++) {
            String raw = lines[i].trim();
            if (raw.isEmpty() || raw.startsWith("id ") || raw.equalsIgnoreCase("id")) {
                continue;
            }
            String normalized = raw.replace('│', ' ').replace('├', ' ').replace('└', ' ').replace('─', ' ').trim();
            String nodeId = normalized.split("\\s+", 2)[0];
            if (nodeId.isEmpty() || !Character.isLetter(nodeId.charAt(0))) {
                continue;
            }
            Matcher matcher = LOCATION.matcher(normalized);
            String task = matcher.find() ? matcher.group(1).toLowerCase(Locale.ROOT) : "UNKNOWN";
            nodes.add(new PlanNode(i + 1, raw, nodeId, operatorName(nodeId), task));
        }
        return nodes;
    }

    private static Candidate score(PlanNode node, TargetPattern target) {
        String text = normalize(node.raw);
        Set<String> nodeReferences = references(node.raw);
        List<String> signals = new ArrayList<>();
        int score = 0;
        boolean exact = !target.normalized.isEmpty() && text.contains(target.normalized);
        boolean allReferences = !target.references.isEmpty() && nodeReferences.containsAll(target.references);
        boolean predicate = containsPredicateMarker(text) || isPredicateOperator(node.operator);
        if (exact) {
            score += 100;
            signals.add("exact-expression");
        }
        if (allReferences) {
            score += 40;
            signals.add("all-column-references");
        }
        if (predicate) {
            score += 30;
            signals.add("predicate-node");
        }
        if (text.contains("cast(") || text.contains("convert(")) {
            score += 10;
            signals.add("type-conversion");
        }
        if ((!exact && !allReferences) || (!exact && !predicate)) {
            return null;
        }
        return new Candidate(node, score, signals);
    }

    private static EcstContext contextFor(PlanNode node, String evaluation, TargetPattern target) {
        String location = location(node.task);
        String boundary = "ROOT".equals(location) || "MPP".equals(location) ? "NON_PUSHDOWN"
                : "COP_TIKV".equals(location) || "COP_TIFLASH".equals(location) ? "PUSHDOWN" : "UNKNOWN";
        String nodeEvaluation = evaluation == null || evaluation.isEmpty() ? "UNKNOWN" : evaluation;
        String text = normalize(node.raw);
        if (text.contains("collate") || text.contains("charset") || text.contains("cast(")
                || text.contains("convert(")) {
            nodeEvaluation += ";nodeTypeEvidence=" + (text.contains("collate") ? "COLLATION"
                    : text.contains("charset") ? "CHARSET" : "CAST_OR_CONVERT");
        }
        return new EcstContext(location, node.operator, boundary, accessPath(node), nodeEvaluation);
    }

    private static String location(String task) {
        String lower = task.toLowerCase(Locale.ROOT);
        if ("root".equals(lower)) {
            return "ROOT";
        }
        if ("cop[tikv]".equals(lower)) {
            return "COP_TIKV";
        }
        if ("cop[tiflash]".equals(lower)) {
            return "COP_TIFLASH";
        }
        return lower.startsWith("mpp[") ? "MPP" : "UNKNOWN";
    }

    private static String operatorName(String nodeId) {
        Matcher matcher = OPERATOR_ID.matcher(nodeId);
        String name = matcher.matches() ? matcher.group(1) : nodeId;
        return name.toUpperCase(Locale.ROOT);
    }

    private static boolean isPredicateOperator(String operator) {
        String lower = operator.toLowerCase(Locale.ROOT);
        return lower.contains("selection") || lower.contains("filter") || lower.contains("condition");
    }

    private static boolean containsPredicateMarker(String text) {
        for (String marker : PREDICATE_MARKERS) {
            if (text.contains(marker)) {
                return true;
            }
        }
        return false;
    }

    private static String accessPath(PlanNode node) {
        String operator = node.operator.toLowerCase(Locale.ROOT);
        String text = normalize(node.raw);
        for (String marker : ACCESS_MARKERS) {
            if (operator.contains(marker) || text.contains(marker)) {
                return marker.replace('_', ' ').replace(" ", "_").toUpperCase(Locale.ROOT);
            }
        }
        return "UNKNOWN";
    }

    private static String normalize(String value) {
        if (value == null) {
            return "";
        }
        String normalized = value.toLowerCase(Locale.ROOT).replace("`", "").replace("\"", "");
        normalized = normalized.replaceAll("\\b[a-z_][a-z0-9_$]*\\.", "");
        return normalized.replaceAll("\\s+", "");
    }

    private static Set<String> references(String value) {
        if (value == null) {
            return Set.of();
        }
        String clean = value.toLowerCase(Locale.ROOT).replace("`", "").replace("\"", "");
        Set<String> result = new LinkedHashSet<>();
        Matcher matcher = TOKEN.matcher(clean);
        while (matcher.find()) {
            String token = matcher.group(1);
            String leaf = token.substring(token.lastIndexOf('.') + 1);
            boolean function = matcher.end() < clean.length() && clean.charAt(matcher.end()) == '(';
            if (!STOP_WORDS.contains(leaf) && !function && !leaf.matches("[-+]?\\d+(?:\\.\\d+)?")) {
                result.add(leaf);
            }
        }
        return result;
    }

    private String readEvaluationContext(String targetExpression) throws SQLException {
        try (Statement statement = state.getConnection().createStatement(); ResultSet result = statement.executeQuery(
                "SELECT @@sql_mode, @@collation_connection, @@character_set_connection")) {
            if (!result.next()) {
                return "UNKNOWN";
            }
            String cast = targetExpression != null && normalize(targetExpression).contains("cast(")
                    ? ";targetTypeConversion=CAST" : "";
            return "sql_mode=" + safe(result.getString(1)) + ";collation=" + safe(result.getString(2)) + ";charset="
                    + safe(result.getString(3)) + cast;
        }
    }

    private static String safe(String value) {
        return value == null || value.isEmpty() ? "UNKNOWN" : value;
    }

    static final class ParsedContext {
        private final EcstContext context;
        private final String plan;
        private final String matchedNode;
        private final String evidence;
        private final boolean matched;

        private ParsedContext(EcstContext context, String plan, String matchedNode, String evidence, boolean matched) {
            this.context = context;
            this.plan = plan == null ? "" : plan;
            this.matchedNode = matchedNode == null ? "" : matchedNode;
            this.evidence = evidence == null ? "" : evidence;
            this.matched = matched;
        }

        private static ParsedContext unknown(String plan, String evidence) {
            return new ParsedContext(new EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"),
                    plan, "", evidence, false);
        }

        EcstContext context() {
            return context;
        }
    }

    private static final class PlanNode {
        private final int line;
        private final String raw;
        private final String operator;
        private final String task;

        private PlanNode(int line, String raw, String nodeId, String operator, String task) {
            this.line = line;
            this.raw = raw;
            this.operator = operator;
            this.task = task;
        }
    }

    private static final class Candidate {
        private final PlanNode node;
        private final int score;
        private final List<String> signals;

        private Candidate(PlanNode node, int score, List<String> signals) {
            this.node = node;
            this.score = score;
            this.signals = signals;
        }
    }

    private static final class TargetPattern {
        private final String normalized;
        private final Set<String> references;

        private TargetPattern(String normalized, Set<String> references) {
            this.normalized = normalized;
            this.references = references;
        }

        private static TargetPattern create(String expression) {
            String normalized = normalize(expression);
            return normalized.isEmpty() ? null : new TargetPattern(normalized, references(expression));
        }
    }
}
