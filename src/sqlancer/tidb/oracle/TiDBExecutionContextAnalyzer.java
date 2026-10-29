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

import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstEvaluationEnvironment;
import sqlancer.common.oracle.ecst.EcstObservation.UnknownReason;
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
    private static final Set<String> PLAN_WORDS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "root", "cop", "tikv", "tiflash", "selection", "projection", "table", "reader", "fullscan",
            "indexlookup", "indexrangescan", "tablerowidscan", "scan", "data", "keep", "order", "stats",
            "pseudo", "join", "othercondition", "condition", "column", "bigint")));
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

    /** Extracts the execution context of the untransformed seed query. */
    public EcstContext extractContext(String originalQuery, String targetExpression) {
        try {
            ParsedContext parsed = parsePlan(explain(originalQuery), targetExpression,
                    readEvaluationContext(targetExpression));
            return parsed.context();
        } catch (SQLException ignored) {
            return new EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN");
        }
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
                UnknownReason unknownReason = original.status == MatchStatus.NOT_FOUND
                        || transformed.status == MatchStatus.NOT_FOUND ? UnknownReason.UNKNOWN_BECAUSE_PLAN
                                : UnknownReason.UNKNOWN_BECAUSE_OPERATION_MAPPING;
                return observation(original, transformed, EcstShiftStatus.UNKNOWN, Set.of(),
                        "target expression was not reliably matched in both plans", unknownReason);
            }
            Set<String> shifts = new LinkedHashSet<>();
            addShift(shifts, "LOCATION", original.context.getLocation(), transformed.context.getLocation());
            addShift(shifts, "BOUNDARY", original.context.getBoundary(), transformed.context.getBoundary());
            addShift(shifts, "ACCESS_PATH", original.context.getAccessPath(), transformed.context.getAccessPath());
            addShift(shifts, "OPERATOR", original.context.getOperator(), transformed.context.getOperator());
            EcstShiftStatus status = target == null ? (shifts.isEmpty() ? EcstShiftStatus.FAILURE
                    : EcstShiftStatus.SUCCESS) : matchTarget(original.context, transformed.context, target);
            UnknownReason unknownReason = status == EcstShiftStatus.UNKNOWN
                    ? target != null && target.getDimension() == sqlancer.common.oracle.ecst.EcstContextDimension.EVALUATION
                            ? UnknownReason.UNKNOWN_BECAUSE_EVALUATION_EVIDENCE
                            : UnknownReason.UNKNOWN_BECAUSE_OPERATION_MAPPING
                    : UnknownReason.NONE;
            return observation(original, transformed, status, shifts,
                    status == EcstShiftStatus.SUCCESS ? "confirmed target migration"
                            : status == EcstShiftStatus.FAILURE ? "target migration not observed"
                                    : "target migration cannot be determined", unknownReason);
        } catch (SQLException e) {
            ParsedContext original = ParsedContext.unknown(originalPlan, "EXPLAIN failed: " + e.getMessage());
            ParsedContext transformed = ParsedContext.unknown(transformedPlan, "EXPLAIN failed: " + e.getMessage());
            return observation(original, transformed, EcstShiftStatus.UNKNOWN, Set.of(),
                    "EXPLAIN failed: " + e.getMessage(), UnknownReason.UNKNOWN_BECAUSE_PLAN);
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
        ParsedContext parsed = parsePlan(plan, targetExpression, "UNKNOWN");
        return parsed.status == MatchStatus.UNIQUE ? parsed.context() : ParsedContext.unknown(plan,
                parsed.reason).context();
    }

    /** Package-visible evidence entry point for focused analyzer tests. */
    static ParsedContext parseWithEvidence(String plan, String targetExpression, String evaluation) {
        return parsePlan(plan, targetExpression, evaluation);
    }

    private EcstObservation observation(ParsedContext original, ParsedContext transformed, EcstShiftStatus status,
            Set<String> shifts, String reason, UnknownReason unknownReason) {
        return new EcstObservation(original.context, transformed.context, status, shifts,
                reason + "; originalEvidence=" + original.evidence + "; transformedEvidence=" + transformed.evidence,
                original.plan, transformed.plan, original.matchedNode, transformed.matchedNode, original.evidence,
                transformed.evidence, unknownReason);
    }

    static EcstShiftStatus matchTarget(EcstContext original, EcstContext transformed, EcstTarget target) {
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
            return ParsedContext.unknown(plan, "no target expression or no explain nodes", MatchStatus.NOT_FOUND);
        }
        List<Candidate> candidates = new ArrayList<>();
        for (PlanNode node : nodes) {
            Candidate candidate = score(node, target);
            if (candidate != null) {
                candidates.add(candidate);
            }
        }
        if (candidates.isEmpty()) {
            return ParsedContext.unknown(plan, "target expression fingerprint was not found in a plan node",
                    MatchStatus.NOT_FOUND);
        }
        candidates.sort((a, b) -> Integer.compare(b.score, a.score));
        Candidate best = candidates.get(0);
        if (best.score == Integer.MAX_VALUE) {
            return ParsedContext.unknown(plan, "multiple target occurrences in one node; sessionMetadata=" + evaluation,
                    MatchStatus.AMBIGUOUS, candidates);
        }
        MatchStatus status = MatchStatus.UNIQUE;
        if (candidates.size() > 1) {
            Candidate second = candidates.get(1);
            if (best.score == second.score) {
                status = allCandidatesRepresentSameOperation(candidates, target)
                        ? MatchStatus.DUPLICATED_SAME_OPERATION : MatchStatus.AMBIGUOUS;
            }
        }
        EcstEvaluationEnvironment environment = parseEvaluationEnvironment(evaluation);
        EcstContext context = contextFor(best.node, environment, target);
        String evidence = "sessionMetadata=" + evaluation + "; executorEvaluation=UNOBSERVED; matchStatus=" + status
                + "; candidates=" + candidates.size() + "; line=" + best.node.line + ", score=" + best.score + ", signals="
                + String.join("+", best.signals) + ", normalizedTarget=" + target.normalized + ", node="
                + best.node.raw;
        String reason = status == MatchStatus.DUPLICATED_SAME_OPERATION ? "duplicated physical instances of one operation"
                : status == MatchStatus.AMBIGUOUS ? "equally scored candidates represent different operations" : "unique operation match";
        return new ParsedContext(context, plan, best.node.raw, evidence, status, reason, candidates);
    }

    private static boolean allCandidatesRepresentSameOperation(List<Candidate> candidates, TargetPattern target) {
        return candidates.stream().allMatch(candidate -> candidate.operationFingerprint.equals(target.operationFingerprint));
    }

    private static List<PlanNode> parseNodes(String plan) {
        if (plan == null || plan.trim().isEmpty()) {
            return List.of();
        }
        List<PlanNode> nodes = new ArrayList<>();
        List<PlanNode> parents = new ArrayList<>();
        String[] lines = plan.split("\\R");
        for (int i = 0; i < lines.length; i++) {
            String sourceLine = lines[i];
            String raw = sourceLine.trim();
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
            int depth = treeDepth(sourceLine);
            PlanNode node = new PlanNode(i + 1, raw, nodeId, operatorName(nodeId), task);
            while (parents.size() > depth) {
                parents.remove(parents.size() - 1);
            }
            if (!parents.isEmpty()) {
                parents.get(parents.size() - 1).children.add(node);
            }
            parents.add(node);
            nodes.add(node);
        }
        return nodes;
    }

    private static int treeDepth(String line) {
        String prefix = line.substring(0, Math.max(0, line.length() - line.trim().length()))
                .replace('│', ' ').replace('├', ' ').replace('└', ' ').replace('─', ' ');
        return prefix.length() / 2;
    }

    private static Candidate score(PlanNode node, TargetPattern target) {
        String text = normalize(node.raw);
        Set<String> nodeReferences = references(node.raw);
        List<String> signals = new ArrayList<>();
        int score = 0;
        int first = text.indexOf(target.normalized);
        boolean exact = !target.normalized.isEmpty() && first >= 0;
        boolean exactStructure = exact && nodeReferences.equals(target.references);
        // Multiple occurrences (including distinct table columns with the same leaf name) are ambiguous.
        if (exact && text.indexOf(target.normalized, first + target.normalized.length()) >= 0) {
            return new Candidate(node, Integer.MAX_VALUE, List.of("ambiguous-expression-occurrences"),
                    target.operationFingerprint);
        }
        boolean allReferences = !target.references.isEmpty() && nodeReferences.equals(target.references);
        boolean equivalent = allReferences && equivalentExpressionShape(text, target);
        boolean predicate = containsPredicateMarker(text) || isPredicateOperator(node.operator);
        if (exactStructure) {
            score += 100;
            signals.add("exact-expression");
        }
        if (allReferences) {
            score += 40;
            signals.add("all-column-references");
        }
        if (equivalent && !exact) {
            score += 75;
            signals.add("equivalent-expression-fingerprint");
        }
        if (predicate) {
            score += 30;
            signals.add("predicate-node");
        }
        if (text.contains("cast(") || text.contains("convert(")) {
            score += 10;
            signals.add("type-conversion");
        }
        if (!exactStructure && !equivalent) {
            return null;
        }
        return new Candidate(node, score, signals, target.operationFingerprint);
    }

    private static boolean equivalentExpressionShape(String text, TargetPattern target) {
        if (countOccurrences(text, "cast(") != target.castCount) {
            return false;
        }
        if (target.castMode != null && !text.contains(target.castMode)) {
            return false;
        }
        if (!target.operatorShape.equals(operatorShape(text))) {
            return false;
        }
        if (target.hasFunction && !text.contains(target.functionName + "(")) {
            return false;
        }
        return target.hasCast || target.hasArithmetic || target.hasFunction;
    }

    private static int countOccurrences(String text, String needle) {
        int count = 0;
        for (int index = text.indexOf(needle); index >= 0; index = text.indexOf(needle, index + needle.length())) {
            count++;
        }
        return count;
    }

    private static String operatorShape(String text) {
        String shape = text.replace("->", "").replace("plus(", "+(").replace("minus(", "-(")
                .replace("mul(", "*(").replace("div(", "/(");
        return shape.replaceAll("[^+*/-]", "");
    }

    private static EcstContext contextFor(PlanNode node, EcstEvaluationEnvironment evaluation,
            TargetPattern target) {
        String location = location(node.task);
        String boundary = "ROOT".equals(location) || "MPP".equals(location) ? "NON_PUSHDOWN"
                : "COP_TIKV".equals(location) || "COP_TIFLASH".equals(location) ? "PUSHDOWN" : "UNKNOWN";
        // Session variables and CAST syntax do not establish executor evaluation semantics.
        return new EcstContext(location, node.operator, boundary, accessPath(node), evaluation);
    }

    private static EcstEvaluationEnvironment parseEvaluationEnvironment(String evaluation) {
        if (evaluation == null || evaluation.trim().isEmpty() || "UNKNOWN".equalsIgnoreCase(evaluation)) {
            return EcstEvaluationEnvironment.unavailable();
        }
        String sqlMode = field(evaluation, "sql_mode");
        String collation = field(evaluation, "collation");
        String charset = field(evaluation, "charset");
        String conversion = field(evaluation, "targetTypeConversion");
        return new EcstEvaluationEnvironment(sqlMode, collation, charset, conversion, false);
    }

    private static String field(String metadata, String name) {
        for (String part : metadata.split(";")) {
            int equals = part.indexOf('=');
            if (equals > 0 && name.equalsIgnoreCase(part.substring(0, equals).trim())) {
                return part.substring(equals + 1).trim();
            }
        }
        return "UNKNOWN";
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
        for (PlanNode child : node.children) {
            String path = accessPath(child);
            if (!"UNKNOWN".equals(path)) {
                return path;
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
        normalized = normalized.replaceAll("\\s+", "");
        // Restricted, lossless spelling correspondence for a single-column signed cast.
        // Unsupported expressions/optimizer rewrites remain UNKNOWN instead of matching columns alone.
        normalized = normalized.replaceAll("cast\\(([a-z_][a-z0-9_$]*)assigned\\)", "cast($1,signed)");
        return normalized.replaceAll("cast\\(([a-z_][a-z0-9_$]*),bigintbinary\\)", "cast($1,signed)");
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
            String planLeaf = leaf.replaceFirst("_[0-9]+$", "");
            boolean function = matcher.end() < clean.length() && clean.charAt(matcher.end()) == '(';
            if (!STOP_WORDS.contains(leaf) && !PLAN_WORDS.contains(leaf) && !PLAN_WORDS.contains(planLeaf) && !function
                    && !leaf.matches("[-+]?\\d+(?:\\.\\d+)?")) {
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

    enum MatchStatus {
        UNIQUE, DUPLICATED_SAME_OPERATION, AMBIGUOUS, NOT_FOUND
    }

    static final class ParsedContext {
        private final EcstContext context;
        private final String plan;
        private final String matchedNode;
        private final String evidence;
        private final boolean matched;
        private final MatchStatus status;
        private final String reason;
        private final List<Candidate> candidates;
        private final Candidate selected;

        private ParsedContext(EcstContext context, String plan, String matchedNode, String evidence, boolean matched) {
            this(context, plan, matchedNode, evidence, matched ? MatchStatus.UNIQUE : MatchStatus.NOT_FOUND,
                    evidence, List.of());
        }

        private ParsedContext(EcstContext context, String plan, String matchedNode, String evidence,
                MatchStatus status, String reason, List<Candidate> candidates) {
            this.context = context;
            this.plan = plan == null ? "" : plan;
            this.matchedNode = matchedNode == null ? "" : matchedNode;
            this.evidence = evidence == null ? "" : evidence;
            this.status = status == null ? MatchStatus.NOT_FOUND : status;
            this.reason = reason == null ? "" : reason;
            this.candidates = Collections.unmodifiableList(new ArrayList<>(candidates == null ? List.of() : candidates));
            this.selected = this.candidates.isEmpty() ? null : this.candidates.get(0);
            this.matched = status == MatchStatus.UNIQUE || status == MatchStatus.DUPLICATED_SAME_OPERATION;
        }

        private static ParsedContext unknown(String plan, String evidence) {
            return unknown(plan, evidence, MatchStatus.NOT_FOUND);
        }

        private static ParsedContext unknown(String plan, String evidence, MatchStatus status) {
            return unknown(plan, evidence, status, List.of());
        }

        private static ParsedContext unknown(String plan, String evidence, MatchStatus status,
                List<Candidate> candidates) {
            return new ParsedContext(new EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"),
                    plan, "", evidence, status, evidence, candidates);
        }

        String evidence() {
            return evidence;
        }

        EcstContext context() {
            return context;
        }

        MatchStatus status() {
            return status;
        }

        List<Candidate> candidates() {
            return candidates;
        }

        Candidate selected() {
            return selected;
        }

        String reason() {
            return reason;
        }
    }

    private static final class PlanNode {
        private final int line;
        private final String raw;
        private final String operator;
        private final String task;
        private final List<PlanNode> children = new ArrayList<>();

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
        private final String operationFingerprint;

        private Candidate(PlanNode node, int score, List<String> signals, String operationFingerprint) {
            this.node = node;
            this.score = score;
            this.signals = signals;
            this.operationFingerprint = operationFingerprint;
        }
    }

    private static final class TargetPattern {
        private final String normalized;
        private final Set<String> references;
        private final boolean hasCast;
        private final boolean hasArithmetic;
        private final boolean hasFunction;
        private final String functionName;
        private final String castMode;
        private final int castCount;
        private final String operatorShape;
        private final String operationFingerprint;

        private TargetPattern(String normalized, Set<String> references, boolean hasCast, boolean hasArithmetic,
                boolean hasFunction, String functionName, String castMode, int castCount, String operatorShape) {
            this.normalized = normalized;
            this.references = references;
            this.hasCast = hasCast;
            this.hasArithmetic = hasArithmetic;
            this.hasFunction = hasFunction;
            this.functionName = functionName;
            this.castMode = castMode;
            this.castCount = castCount;
            this.operatorShape = operatorShape;
            this.operationFingerprint = normalized + "|refs=" + references + "|casts=" + castCount + "|ops="
                    + operatorShape + "|castMode=" + castMode;
        }

        private static TargetPattern create(String expression) {
            String normalized = normalize(expression);
            if (normalized.isEmpty()) {
                return null;
            }
            boolean cast = normalized.contains("cast(");
            boolean arithmetic = normalized.contains("+") || normalized.contains("-") || normalized.contains("*");
            String function = "";
            Matcher matcher = Pattern.compile("^([a-z_][a-z0-9_$]*)\\(").matcher(normalized);
            boolean hasFunction = matcher.find() && !"cast".equals(matcher.group(1));
            if (hasFunction) {
                function = matcher.group(1);
            }
            String mode = normalized.contains("unsigned") ? "unsigned" : normalized.contains("signed") ? "signed" : null;
            Set<String> dependencies = references(expression);
            return new TargetPattern(normalized, dependencies, cast, arithmetic, hasFunction, function, mode,
                    countOccurrences(normalized, "cast("), operatorShape(normalized));
        }
    }
}
