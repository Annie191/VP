package sqlancer.tidb.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstEvaluationEnvironment;
import sqlancer.common.oracle.ecst.EcstOperation;
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
            "is", "in", "between", "like", "case", "when", "then", "else", "end", "collate", "charset",
            "bigint", "binary")));
    private static final Pattern JOIN_FIELD = Pattern.compile(
            "(?i)(?:^|,\\s*)(equal|other\\s+cond|left\\s+key|right\\s+key):");
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
    public ParsedContext extractContext(String originalQuery, String targetExpression) {
        return extractContext(originalQuery, operationFor(targetExpression));
    }

    public ParsedContext extractContext(String originalQuery, EcstOperation operation) {
        String plan;
        try {
            plan = explain(originalQuery);
        } catch (SQLException failure) {
            return ParsedContext.explainFailure(originalQuery, failure);
        }
        String evaluation = "UNKNOWN";
        try {
            evaluation = readEvaluationContext(operation == null ? null : operation.getNormalizedExpression());
        } catch (SQLException failure) {
            evaluation = "UNKNOWN; supplementaryEvaluationError=" + sqlError(failure);
        }
        return parsePlan(plan, operation, evaluation);
    }

    /** Compatibility accessor for callers that only need the normalized context. */
    public EcstContext extractEcstContext(String originalQuery, EcstOperation operation) {
        return extractContext(originalQuery, operation).context();
    }

    public EcstObservation analyze(String originalQuery, String transformedQuery, String targetExpression,
            EcstTarget target) {
        return analyze(originalQuery, transformedQuery, operationFor(targetExpression), target);
    }

    public EcstObservation analyze(String originalQuery, String transformedQuery, EcstOperation operation,
            EcstTarget target) {
        String originalPlan = "";
        String transformedPlan = "";
        try {
            originalPlan = explain(originalQuery);
        } catch (SQLException failure) {
            ParsedContext original = ParsedContext.explainFailure(originalQuery, failure);
            ParsedContext transformed = ParsedContext.unknown(transformedPlan, "transformed plan not requested");
            return observation(original, transformed, EcstShiftStatus.UNKNOWN, Set.of(),
                    "original EXPLAIN failed", UnknownReason.UNKNOWN_BECAUSE_PLAN);
        }
        try {
            transformedPlan = explain(transformedQuery);
        } catch (SQLException failure) {
            ParsedContext original = ParsedContext.unknown(originalPlan, "original plan available");
            ParsedContext transformed = ParsedContext.explainFailure(transformedQuery, failure);
            return observation(original, transformed, EcstShiftStatus.UNKNOWN, Set.of(),
                    "transformed EXPLAIN failed", UnknownReason.UNKNOWN_BECAUSE_PLAN);
        }
        String evaluation;
        try {
            evaluation = readEvaluationContext(operation == null ? null : operation.getNormalizedExpression());
        } catch (SQLException failure) {
            evaluation = "UNKNOWN; supplementaryEvaluationError=" + sqlError(failure);
        }
        try {
            ParsedContext original = parsePlan(originalPlan, operation, evaluation);
            ParsedContext transformed = parsePlan(transformedPlan, operation, evaluation);
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
        } catch (RuntimeException e) {
            ParsedContext original = ParsedContext.unknown(originalPlan, "plan parsing failed: " + e.getMessage());
            ParsedContext transformed = ParsedContext.unknown(transformedPlan, "plan parsing failed: " + e.getMessage());
            return observation(original, transformed, EcstShiftStatus.UNKNOWN, Set.of(),
                    "plan parsing failed: " + e.getMessage(), UnknownReason.UNKNOWN_BECAUSE_PLAN);
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
        ParsedContext parsed = parsePlan(plan, operationFor(targetExpression), "UNKNOWN");
        return parsed.status == MatchStatus.UNIQUE ? parsed.context() : ParsedContext.unknown(plan,
                parsed.reason).context();
    }

    /** Package-visible evidence entry point for focused analyzer tests. */
    static ParsedContext parseWithEvidence(String plan, String targetExpression, String evaluation) {
        return parsePlan(plan, operationFor(targetExpression), evaluation);
    }

    static ParsedContext parseWithEvidence(String plan, EcstOperation operation, String evaluation) {
        return parsePlan(plan, operation, evaluation);
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

    private static ParsedContext parsePlan(String plan, EcstOperation operation, String evaluation) {
        List<PlanNode> nodes = parseNodes(plan);
        TargetPattern target = TargetPattern.create(operation);
        if (target == null || nodes.isEmpty()) {
            return ParsedContext.unknown(plan, "no target expression or no explain nodes", MatchStatus.NOT_FOUND);
        }
        List<Candidate> candidates = new ArrayList<>();
        for (PlanNode node : nodes) {
            for (PlanExpression expression : node.expressions) {
                Candidate candidate = score(expression, target);
                if (candidate != null) {
                    candidates.add(candidate);
                }
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
            if (!allCandidatesHaveSameRole(candidates)) {
                // A projection and a predicate can contain the same expression, but
                // they are different operation instances until an AST/source
                // correspondence identifies which one is the target.
                status = MatchStatus.AMBIGUOUS;
            } else if (best.score == second.score) {
                status = allCandidatesRepresentSameOperation(candidates)
                        ? MatchStatus.DUPLICATED_SAME_OPERATION : MatchStatus.AMBIGUOUS;
            }
        }
        EcstEvaluationEnvironment environment = parseEvaluationEnvironment(evaluation);
        EcstContext context = contextForCandidates(candidates, environment, target);
        String evidence = "sessionMetadata=" + evaluation + "; executorEvaluation=UNOBSERVED; matchStatus=" + status
                + "; candidates=" + candidates.size() + "; line=" + best.expression.owner.line + ", score="
                + best.score + ", signals=" + String.join("+", best.signals) + ", normalizedTarget="
                + target.normalized + ", expressionRole=" + best.expression.role + ", expression="
                + best.expression.expressionText + ", nodeRole=" + best.expression.owner.role + ", node="
                + best.expression.owner.raw + ", accessPaths=" + accessPaths(best.expression.owner);
        String reason = status == MatchStatus.DUPLICATED_SAME_OPERATION ? "duplicated physical instances of one operation"
                : status == MatchStatus.AMBIGUOUS ? "equally scored candidates represent different operations" : "unique operation match";
        return new ParsedContext(context, plan, best.expression.owner.raw, evidence, status, reason, candidates);
    }

    private static boolean allCandidatesRepresentSameOperation(List<Candidate> candidates) {
        if (candidates.isEmpty()) {
            return true;
        }
        String fingerprint = candidates.get(0).operationFingerprint;
        return candidates.stream().allMatch(candidate -> candidate.operationFingerprint.equals(fingerprint));
    }

    private static boolean allCandidatesHaveSameRole(List<Candidate> candidates) {
        if (candidates.isEmpty()) {
            return true;
        }
        ExpressionRole role = candidates.get(0).expression.role;
        return candidates.stream().allMatch(candidate -> candidate.expression.role == role);
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
            String nodeToken = normalized.split("\\s+", 2)[0];
            String nodeId = nodeToken;
            String nodeRole = "";
            int roleStart = nodeToken.indexOf('(');
            if (roleStart > 0 && nodeToken.endsWith(")")) {
                nodeId = nodeToken.substring(0, roleStart);
                nodeRole = nodeToken.substring(roleStart + 1, nodeToken.length() - 1).toUpperCase(Locale.ROOT);
            }
            if (nodeId.isEmpty() || !Character.isLetter(nodeId.charAt(0))) {
                continue;
            }
            Matcher matcher = LOCATION.matcher(normalized);
            String task = matcher.find() ? matcher.group(1).toLowerCase(Locale.ROOT) : "UNKNOWN";
            int depth = treeDepth(sourceLine);
            PlanNode node = new PlanNode(i + 1, raw, nodeId, operatorName(nodeId), nodeRole, task);
            while (parents.size() > depth) {
                parents.remove(parents.size() - 1);
            }
            if (!parents.isEmpty()) {
                parents.get(parents.size() - 1).children.add(node);
            }
            parents.add(node);
            node.expressions.addAll(extractExpressions(node));
            nodes.add(node);
        }
        return nodes;
    }

    /**
     * Extracts expression instances from the payload owned by a physical node. The
     * plan node remains the owner of the expression, but matching and dependency
     * extraction are performed on the expression text only.
     */
    private static List<PlanExpression> extractExpressions(PlanNode node) {
        String payload = payload(node.raw);
        if (payload.isEmpty()) {
            return List.of();
        }
        String operator = node.operator.toLowerCase(Locale.ROOT);
        List<PlanExpression> expressions = new ArrayList<>();
        if (operator.contains("selection") || operator.contains("filter")) {
            addExpression(expressions, node, ExpressionRole.SELECTION_PREDICATE, payload);
            return expressions;
        }
        if (operator.contains("projection")) {
            for (String item : splitTopLevel(payload, ',')) {
                String expression = stripProjectionAlias(item);
                addExpression(expressions, node, ExpressionRole.PROJECTION_ITEM, expression);
            }
            return expressions;
        }
        if (operator.contains("join")) {
            extractJoinExpressions(expressions, node, payload);
            return expressions;
        }
        if (operator.contains("agg") || operator.contains("aggregation")) {
            extractAggregationExpressions(expressions, node, payload);
            return expressions;
        }
        return expressions;
    }

    private static void extractJoinExpressions(List<PlanExpression> expressions, PlanNode node, String payload) {
        Matcher matcher = JOIN_FIELD.matcher(payload);
        List<JoinField> fields = new ArrayList<>();
        while (matcher.find()) {
            fields.add(new JoinField(matcher.group(1).toLowerCase(Locale.ROOT).replaceAll("\\s+", "_"),
                    matcher.end()));
        }
        for (int i = 0; i < fields.size(); i++) {
            int end = i + 1 < fields.size() ? fields.get(i + 1).start : payload.length();
            String value = payload.substring(fields.get(i).start, end).trim();
            ExpressionRole role = fields.get(i).name.equals("other_cond") ? ExpressionRole.JOIN_OTHER_CONDITION
                    : ExpressionRole.JOIN_EQUAL_CONDITION;
            if (role == ExpressionRole.JOIN_EQUAL_CONDITION && value.startsWith("[")) {
                value = value.substring(1, value.endsWith("]") ? value.length() - 1 : value.length());
            }
            for (String expression : splitTopLevel(value, ',')) {
                addExpression(expressions, node, role, expression);
            }
        }
    }

    private static void extractAggregationExpressions(List<PlanExpression> expressions, PlanNode node,
            String payload) {
        String groupBy = fieldValue(payload, "group by:", "funcs:");
        for (String expression : splitTopLevel(groupBy, ',')) {
            addExpression(expressions, node, ExpressionRole.GROUP_KEY, expression);
        }
        String funcs = fieldValue(payload, "funcs:");
        for (String expression : splitTopLevel(funcs, ',')) {
            String item = stripProjectionAlias(expression);
            addExpression(expressions, node, ExpressionRole.AGGREGATE_ARGUMENT, item);
            int open = item.indexOf('(');
            int close = item.lastIndexOf(')');
            if (open >= 0 && close > open) {
                addExpression(expressions, node, ExpressionRole.AGGREGATE_ARGUMENT,
                        item.substring(open + 1, close));
            }
        }
    }

    private static String payload(String raw) {
        if (raw == null) {
            return "";
        }
        int separator = raw.lastIndexOf('|');
        if (separator >= 0) {
            return raw.substring(separator + 1).trim();
        }
        Matcher location = LOCATION.matcher(raw);
        return location.find() ? raw.substring(location.end()).trim() : raw.trim();
    }

    private static String fieldValue(String payload, String field, String... nextFields) {
        int start = payload.toLowerCase(Locale.ROOT).indexOf(field.toLowerCase(Locale.ROOT));
        if (start < 0) {
            return "";
        }
        start += field.length();
        int end = payload.length();
        String lower = payload.toLowerCase(Locale.ROOT);
        for (String next : nextFields) {
            int candidate = lower.indexOf(next.toLowerCase(Locale.ROOT), start);
            if (candidate >= 0) {
                end = Math.min(end, candidate);
            }
        }
        return payload.substring(start, end).trim();
    }

    private static String stripProjectionAlias(String value) {
        int arrow = value.indexOf("->");
        return (arrow >= 0 ? value.substring(0, arrow) : value).trim();
    }

    private static void addExpression(List<PlanExpression> expressions, PlanNode owner, ExpressionRole role,
            String expression) {
        String clean = expression == null ? "" : expression.trim();
        if (!clean.isEmpty() && !clean.equals(",")) {
            expressions.add(new PlanExpression(owner, role, clean, normalize(clean)));
        }
    }

    private static List<String> splitTopLevel(String value, char delimiter) {
        if (value == null || value.trim().isEmpty()) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '(' || c == '[') {
                depth++;
            } else if (c == ')' || c == ']') {
                depth = Math.max(0, depth - 1);
            } else if (c == delimiter && depth == 0) {
                result.add(value.substring(start, i).trim());
                start = i + 1;
            }
        }
        result.add(value.substring(start).trim());
        return result;
    }

    private static final class JoinField {
        private final String name;
        private final int start;

        private JoinField(String name, int start) {
            this.name = name;
            this.start = start;
        }
    }

    private static int treeDepth(String line) {
        int prefixLength = 0;
        while (prefixLength < line.length()) {
            char c = line.charAt(prefixLength);
            if (c == ' ' || c == '│' || c == '├' || c == '└' || c == '─') {
                prefixLength++;
            } else {
                break;
            }
        }
        return prefixLength / 2;
    }

    private static Candidate score(PlanExpression expression, TargetPattern target) {
        if (!target.acceptsRole(expression.role)) {
            return null;
        }
        String text = expression.normalized;
        Set<String> nodeReferences = logicalReferences(expression.expressionText, target);
        List<String> signals = new ArrayList<>();
        int score = 0;
        int first = text.indexOf(target.normalized);
        boolean exact = !target.normalized.isEmpty() && first >= 0;
        boolean exactStructure = exact && nodeReferences.equals(target.references);
        // Multiple occurrences (including distinct table columns with the same leaf name) are ambiguous.
        if (exact && text.indexOf(target.normalized, first + target.normalized.length()) >= 0) {
            return new Candidate(expression, Integer.MAX_VALUE, List.of("ambiguous-expression-occurrences"));
        }
        boolean allReferences = !target.references.isEmpty() && nodeReferences.equals(target.references);
        boolean equivalent = allReferences && equivalentExpressionShape(expression.expressionText, target);
        boolean predicate = containsPredicateMarker(text) || expression.role == ExpressionRole.SELECTION_PREDICATE
                || expression.role == ExpressionRole.JOIN_OTHER_CONDITION;
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
        signals.add("role=" + expression.role.name());
        return new Candidate(expression, score, signals);
    }

    private static boolean equivalentExpressionShape(String expression, TargetPattern target) {
        ExprFingerprint candidate = ExprFingerprint.parse(expression);
        return candidate != null && target.fingerprint != null && candidate.contains(target.fingerprint);
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

    private static EcstContext contextForCandidates(List<Candidate> candidates,
            EcstEvaluationEnvironment evaluation, TargetPattern target) {
        List<EcstContext> contexts = new ArrayList<>();
        for (Candidate candidate : candidates) {
            contexts.add(contextFor(candidate.expression.owner, evaluation, target));
        }
        EcstContext first = contexts.get(0);
        return new EcstContext(resolveDimension(contexts, sqlancer.common.oracle.ecst.EcstContextDimension.LOCATION),
                resolveDimension(contexts, sqlancer.common.oracle.ecst.EcstContextDimension.OPERATOR),
                resolveDimension(contexts, sqlancer.common.oracle.ecst.EcstContextDimension.BOUNDARY),
                resolveDimension(contexts, sqlancer.common.oracle.ecst.EcstContextDimension.ACCESS_PATH),
                first.getEvaluationEnvironment());
    }

    private static String resolveDimension(List<EcstContext> contexts,
            sqlancer.common.oracle.ecst.EcstContextDimension dimension) {
        String resolved = null;
        for (EcstContext context : contexts) {
            String value = context.value(dimension);
            if ("UNKNOWN".equals(value)) {
                return "UNKNOWN";
            }
            if (resolved == null) {
                resolved = value;
            } else if (!resolved.equals(value)) {
                return "UNKNOWN";
            }
        }
        return resolved == null ? "UNKNOWN" : resolved;
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

    private static boolean containsPredicateMarker(String text) {
        for (String marker : PREDICATE_MARKERS) {
            if (text.contains(marker)) {
                return true;
            }
        }
        return false;
    }

    private static String accessPath(PlanNode node) {
        Map<String, String> paths = accessPaths(node);
        if (paths.size() != 1) {
            return "UNKNOWN";
        }
        return paths.values().iterator().next();
    }

    private static Map<String, String> accessPaths(PlanNode node) {
        Map<String, String> result = new LinkedHashMap<>();
        if (node.operator.toLowerCase(Locale.ROOT).contains("join")) {
            int ordinal = 0;
            for (PlanNode child : node.children) {
                String key = child.role.isEmpty() ? "INPUT_" + ordinal : child.role;
                String path = firstAccessPath(child);
                result.put(key, path);
                ordinal++;
            }
            return result;
        }
        result.put("INPUT", firstAccessPath(node));
        return result;
    }

    private static String firstAccessPath(PlanNode node) {
        String operator = node.operator.toLowerCase(Locale.ROOT);
        String text = normalize(node.raw);
        for (String marker : ACCESS_MARKERS) {
            if (operator.contains(marker) || text.contains(marker)) {
                return marker.replace('_', ' ').replace(" ", "_").toUpperCase(Locale.ROOT);
            }
        }
        for (PlanNode child : node.children) {
            String path = firstAccessPath(child);
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

    private static String expressionFingerprint(String expression) {
        String normalized = normalize(expression);
        return normalized + "|refs=" + references(expression) + "|casts=" + countOccurrences(normalized, "cast(")
                + "|ops=" + operatorShape(normalized);
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
            if (!STOP_WORDS.contains(leaf) && !function
                    && !leaf.matches("[-+]?\\d+(?:\\.\\d+)?")) {
                result.add(leaf);
            }
        }
        return result;
    }

    private static Set<String> logicalReferences(String value, TargetPattern target) {
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
            if (STOP_WORDS.contains(leaf) || function || leaf.matches("[-+]?\\d+(?:\\.\\d+)?")) {
                continue;
            }
            String mapped = target.lineage.get(token);
            if (mapped == null) {
                mapped = target.lineage.get(token.toLowerCase(Locale.ROOT));
            }
            if (mapped == null && token.indexOf('.') < 0 && target.references.contains(leaf)) {
                mapped = leaf;
            }
            if (mapped == null && token.indexOf('.') >= 0 && target.references.contains(leaf)) {
                // Only collapse a qualifier when the logical target dependency
                // confirms that the leaf belongs to this operation.
                mapped = leaf;
            }
            result.add(mapped == null ? token : mapped);
        }
        return result;
    }

    private String readEvaluationContext(String targetExpression) throws SQLException {
        try (Statement statement = state.getConnection().createStatement(); ResultSet result = statement.executeQuery(
                "SELECT @@sql_mode, @@collation_connection, @@character_set_connection, "
                        + "@@tidb_enable_new_only_full_group_by_check")) {
            if (!result.next()) {
                return "UNKNOWN";
            }
            String cast = targetExpression != null && normalize(targetExpression).contains("cast(")
                    ? ";targetTypeConversion=CAST" : "";
            return "sql_mode=" + safe(result.getString(1)) + ";collation=" + safe(result.getString(2)) + ";charset="
                    + safe(result.getString(3)) + ";tidb_enable_new_only_full_group_by_check=" + safe(result.getString(4))
                    + cast;
        }
    }

    private static String safe(String value) {
        return value == null || value.isEmpty() ? "UNKNOWN" : value;
    }

    private static String sqlError(SQLException failure) {
        return "sqlState=" + safe(failure.getSQLState()) + ",errorCode=" + failure.getErrorCode() + ",message="
                + safe(failure.getMessage());
    }

    private static EcstOperation operationFor(String targetExpression) {
        String normalized = normalize(targetExpression);
        return new EcstOperation("ECST_TARGET:" + normalized, targetExpression, references(targetExpression), "ARITHMETIC");
    }

    enum MatchStatus {
        UNIQUE, DUPLICATED_SAME_OPERATION, AMBIGUOUS, NOT_FOUND
    }

    public enum ExtractionStatus {
        MATCHED, EXPRESSION_NOT_FOUND, AMBIGUOUS, EXPLAIN_ERROR
    }

    public static final class ParsedContext {
        private final EcstContext context;
        private final String plan;
        private final String matchedNode;
        private final String evidence;
        private final boolean matched;
        private final MatchStatus status;
        private final ExtractionStatus extractionStatus;
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
            this.extractionStatus = extractionStatusFor(this.status);
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

        private static ParsedContext explainFailure(String query, SQLException failure) {
            String evidence = "EXPLAIN failed: " + sqlError(failure) + "; query=" + query;
            return new ParsedContext(new EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"),
                    "", "", evidence, ExtractionStatus.EXPLAIN_ERROR, evidence, List.of());
        }

        private ParsedContext(EcstContext context, String plan, String matchedNode, String evidence,
                ExtractionStatus extractionStatus, String reason, List<Candidate> candidates) {
            this.context = context;
            this.plan = plan == null ? "" : plan;
            this.matchedNode = matchedNode == null ? "" : matchedNode;
            this.evidence = evidence == null ? "" : evidence;
            this.status = extractionStatus == ExtractionStatus.AMBIGUOUS ? MatchStatus.AMBIGUOUS
                    : extractionStatus == ExtractionStatus.MATCHED ? MatchStatus.UNIQUE : MatchStatus.NOT_FOUND;
            this.extractionStatus = extractionStatus;
            this.reason = reason == null ? "" : reason;
            this.candidates = Collections.unmodifiableList(new ArrayList<>(candidates == null ? List.of() : candidates));
            this.selected = this.candidates.isEmpty() ? null : this.candidates.get(0);
            this.matched = extractionStatus == ExtractionStatus.MATCHED;
        }

        private static ExtractionStatus extractionStatusFor(MatchStatus status) {
            if (status == MatchStatus.AMBIGUOUS) return ExtractionStatus.AMBIGUOUS;
            if (status == MatchStatus.UNIQUE || status == MatchStatus.DUPLICATED_SAME_OPERATION) {
                return ExtractionStatus.MATCHED;
            }
            return ExtractionStatus.EXPRESSION_NOT_FOUND;
        }

        public String evidence() {
            return evidence;
        }

        public String plan() {
            return plan;
        }

        public List<String> candidateExpressions() {
            List<String> expressions = new ArrayList<>();
            for (Candidate candidate : candidates) {
                expressions.add(candidate.expression.expressionText);
            }
            return expressions;
        }

        public EcstContext context() {
            return context;
        }

        public MatchStatus status() {
            return status;
        }

        public ExtractionStatus extractionStatus() {
            return extractionStatus;
        }

        public List<Candidate> candidates() {
            return candidates;
        }

        public Candidate selected() {
            return selected;
        }

        public PlanExpression selectedExpression() {
            return selected == null ? null : selected.expression;
        }

        public String reason() {
            return reason;
        }
    }

    private static final class PlanNode {
        private final int line;
        private final String raw;
        private final String operator;
        private final String role;
        private final String task;
        private final List<PlanNode> children = new ArrayList<>();
        private final List<PlanExpression> expressions = new ArrayList<>();

        private PlanNode(int line, String raw, String nodeId, String operator, String role, String task) {
            this.line = line;
            this.raw = raw;
            this.operator = operator;
            this.role = role == null ? "" : role;
            this.task = task;
        }
    }

    public enum ExpressionRole {
        SELECTION_PREDICATE,
        PROJECTION_ITEM,
        JOIN_OTHER_CONDITION,
        JOIN_EQUAL_CONDITION,
        GROUP_KEY,
        AGGREGATE_ARGUMENT
    }

    public static final class PlanExpression {
        private final PlanNode owner;
        private final ExpressionRole role;
        private final String expressionText;
        private final String fingerprint;
        private final String normalized;

        private PlanExpression(PlanNode owner, ExpressionRole role, String expressionText, String normalized) {
            this.owner = owner;
            this.role = role;
            this.expressionText = expressionText;
            this.fingerprint = expressionFingerprint(expressionText);
            this.normalized = normalized;
        }

        public ExpressionRole role() {
            return role;
        }

        public String expressionText() {
            return expressionText;
        }

        public String fingerprint() {
            return fingerprint;
        }
    }

    public static final class Candidate {
        private final PlanExpression expression;
        private final int score;
        private final List<String> signals;
        private final String operationFingerprint;

        private Candidate(PlanExpression expression, int score, List<String> signals) {
            this.expression = expression;
            this.score = score;
            this.signals = signals;
            this.operationFingerprint = expression.fingerprint;
        }

        public PlanExpression expression() {
            return expression;
        }

        public String operationFingerprint() {
            return operationFingerprint;
        }
    }

    /** Structural expression fingerprint; unlike operator text, child order and types are significant. */
    public static final class ExprFingerprint {
        private final String kind;
        private final String resultType;
        private final List<ExprFingerprint> children;
        private final String column;
        private final String constant;

        private ExprFingerprint(String kind, String resultType, List<ExprFingerprint> children, String column,
                String constant) {
            this.kind = kind;
            this.resultType = resultType;
            this.children = List.copyOf(children == null ? List.of() : children);
            this.column = column;
            this.constant = constant;
        }

        static ExprFingerprint parse(String expression) {
            ExprFingerprint caseFingerprint = parseSqlCase(expression);
            if (caseFingerprint != null) {
                return caseFingerprint;
            }
            String normalized = normalize(canonicalizeSqlCasts(expression));
            return normalized.isEmpty() ? null : parseNormalized(normalized);
        }

        private static String canonicalizeSqlCasts(String expression) {
            return expression == null ? "" : expression.replaceAll(
                    "(?i)cast\\(\\s*([^(),]+?)\\s+as\\s+char(?:\\s*\\(\\s*(\\d+)\\s*\\))?\\s*\\)",
                    "cast($1,sql_char$2)");
        }

        private static ExprFingerprint parseSqlCase(String expression) {
            if (expression == null) {
                return null;
            }
            Matcher matcher = Pattern.compile("(?is)^\\s*case\\s+when\\s+(.+?)\\s+then\\s+(.+?)\\s+else\\s+(.+?)\\s+end\\s*$")
                    .matcher(expression);
            if (!matcher.matches()) {
                return null;
            }
            return new ExprFingerprint("CASE_WHEN", "UNKNOWN", List.of(parseCaseCondition(matcher.group(1)),
                    parse(matcher.group(2)), parse(matcher.group(3))), null, null);
        }

        private static ExprFingerprint parseCaseCondition(String condition) {
            Matcher isNull = Pattern.compile("(?is)^\\s*(.+?)\\s+is\\s+null\\s*$").matcher(condition);
            if (isNull.matches()) {
                return new ExprFingerprint("IS_NULL", "BOOLEAN", List.of(parse(isNull.group(1))), null, null);
            }
            return parse(condition);
        }

        private static ExprFingerprint parseNormalized(String expression) {
            String value = stripOuterParentheses(expression);
            ComparisonOperator comparison = topLevelComparison(value);
            if (comparison != null) {
                String kind;
                switch (comparison.operator) {
                case ">": kind = "GT"; break;
                case "<": kind = "LT"; break;
                case ">=": kind = "GE"; break;
                case "<=": kind = "LE"; break;
                case "!=": kind = "NE"; break;
                case "<>": kind = "NE"; break;
                case "<=>": kind = "NULL_SAFE_EQ"; break;
                default: kind = "EQ"; break;
                }
                return new ExprFingerprint(kind, "BOOLEAN",
                        List.of(parseNormalized(value.substring(0, comparison.index)),
                                parseNormalized(value.substring(comparison.index + comparison.operator.length()))), null, null);
            }
            return parseAdditive(value);
        }

        private static ExprFingerprint parseAdditive(String value) {
            int operator = topLevelBinaryOperator(value, "+-");
            if (operator > 0) {
                String symbol = String.valueOf(value.charAt(operator));
                return new ExprFingerprint(symbol.equals("+") ? "ADD" : "SUB", "UNKNOWN",
                        List.of(parseAdditive(value.substring(0, operator)),
                                parseMultiplicative(value.substring(operator + 1))), null, null);
            }
            return parseMultiplicative(value);
        }

        private static ExprFingerprint parseMultiplicative(String value) {
            int operator = topLevelBinaryOperator(value, "*/");
            if (operator > 0) {
                String symbol = String.valueOf(value.charAt(operator));
                return new ExprFingerprint(symbol.equals("*") ? "MUL" : "DIV", "UNKNOWN",
                        List.of(parseMultiplicative(value.substring(0, operator)),
                                parseUnary(value.substring(operator + 1))), null, null);
            }
            return parseUnary(value);
        }

        private static ExprFingerprint parseUnary(String value) {
            if (value.startsWith("-") && value.length() > 1) {
                return new ExprFingerprint("UNARY_NEG", "UNKNOWN", List.of(parseUnary(value.substring(1))), null,
                        null);
            }
            return parsePrimary(value);
        }

        private static ExprFingerprint parsePrimary(String value) {
            int open = value.indexOf('(');
            if (open > 0 && value.endsWith(")")) {
                String name = value.substring(0, open);
                List<String> arguments = splitNormalizedArguments(value.substring(open + 1, value.length() - 1));
                if ("cast".equals(name) && !arguments.isEmpty()) {
                    String type = arguments.size() > 1 ? castType(arguments.get(1)) : "UNKNOWN";
                    String length = castLength(arguments.size() > 1 ? arguments.get(1) : "");
                    return new ExprFingerprint("CAST", type, List.of(parseNormalized(arguments.get(0))), null, length);
                }
                String kind;
                switch (name) {
                case "plus":
                    kind = "ADD";
                    break;
                case "minus":
                    kind = "SUB";
                    break;
                case "mul":
                    kind = "MUL";
                    break;
                case "div":
                    kind = "DIV";
                    break;
                case "casewhen":
                case "case":
                    kind = "CASE_WHEN";
                    break;
                case "isnull":
                    kind = "IS_NULL";
                    break;
                case "gt":
                    kind = "GT";
                    break;
                case "ge":
                    kind = "GE";
                    break;
                case "eq":
                    kind = "EQ";
                    break;
                case "ne":
                    kind = "NE";
                    break;
                case "lt":
                    kind = "LT";
                    break;
                case "le":
                    kind = "LE";
                    break;
                case "nulleq":
                    kind = "NULL_SAFE_EQ";
                    break;
                default:
                    kind = name.toUpperCase(Locale.ROOT);
                    break;
                }
                if ("CASE_WHEN".equals(kind) && arguments.size() != 3) {
                    return new ExprFingerprint("UNSUPPORTED_REWRITE", "UNKNOWN", List.of(), null, value);
                }
                List<ExprFingerprint> children = new ArrayList<>();
                for (String argument : arguments) {
                    children.add(parseNormalized(argument));
                }
                boolean booleanResult = "IS_NULL".equals(kind) || "GT".equals(kind) || "GE".equals(kind)
                        || "EQ".equals(kind) || "NE".equals(kind) || "LT".equals(kind) || "LE".equals(kind)
                        || "NULL_SAFE_EQ".equals(kind);
                return new ExprFingerprint(kind, booleanResult ? "BOOLEAN" : "UNKNOWN", children, null, null);
            }
            if (value.matches("[-+]?\\d+(?:\\.\\d+)?")) {
                return new ExprFingerprint("CONSTANT", numericType(value), List.of(), null, value);
            }
            if (value.matches("[a-z_][a-z0-9_$]*(?:\\.[a-z_][a-z0-9_$]*)?")) {
                return new ExprFingerprint("COLUMN", "UNKNOWN", List.of(),
                        value.substring(value.lastIndexOf('.') + 1), null);
            }
            // CASE and unsupported optimizer rewrites are intentionally opaque. They
            // can match only when their normalized structure is exactly preserved.
            return new ExprFingerprint("UNSUPPORTED_REWRITE", "UNKNOWN", List.of(), null, value);
        }

        private static String castType(String type) {
            if (type.startsWith("sql_char") || type.startsWith("var_string")) {
                return "CHAR";
            }
            if (type.contains("unsigned")) {
                return "UNSIGNED";
            }
            if (type.contains("signed") || type.contains("bigintbinary")) {
                return "SIGNED";
            }
            return type.toUpperCase(Locale.ROOT);
        }

        private static String castLength(String type) {
            Matcher matcher = Pattern.compile("(?:sql_char|var_string)(?:\\((\\d+)\\))?").matcher(type);
            return matcher.matches() && matcher.group(1) != null ? matcher.group(1) : "*";
        }

        private static String numericType(String value) {
            return value.contains(".") ? "DECIMAL" : "INTEGER";
        }

        private static String stripOuterParentheses(String value) {
            while (value.startsWith("(") && value.endsWith(")") && matchingOuterParentheses(value)) {
                value = value.substring(1, value.length() - 1);
            }
            return value;
        }

        private static boolean matchingOuterParentheses(String value) {
            int depth = 0;
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (c == '(') depth++;
                if (c == ')' && --depth == 0 && i != value.length() - 1) return false;
            }
            return depth == 0;
        }

        private static int topLevelBinaryOperator(String value, String operators) {
            int depth = 0;
            boolean quoted = false;
            int result = -1;
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (c == '\'' ) quoted = !quoted;
                if (quoted) continue;
                if (c == '(') depth++;
                else if (c == ')') depth--;
                else if (depth == 0 && operators.indexOf(c) >= 0 && i > 0) {
                    char previous = value.charAt(i - 1);
                    if (operators.indexOf(previous) < 0 && previous != '(' && previous != ',') result = i;
                }
            }
            return result;
        }

        private static ComparisonOperator topLevelComparison(String value) {
            int depth = 0;
            boolean quoted = false;
            String[] operators = { "<=>", ">=", "<=", "!=", "<>", ">", "<", "=" };
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (c == '\'') quoted = !quoted;
                if (quoted) continue;
                if (c == '(') depth++;
                else if (c == ')') depth--;
                else if (depth == 0 && i > 0) {
                    for (String operator : operators) {
                        if (value.startsWith(operator, i)) return new ComparisonOperator(i, operator);
                    }
                }
            }
            return null;
        }

        private static final class ComparisonOperator {
            private final int index;
            private final String operator;

            private ComparisonOperator(int index, String operator) {
                this.index = index;
                this.operator = operator;
            }
        }

        private static List<String> splitNormalizedArguments(String value) {
            return splitTopLevel(value, ',');
        }

        private boolean contains(ExprFingerprint target) {
            if (equals(target)) return true;
            for (ExprFingerprint child : children) {
                if (child.contains(target)) return true;
            }
            return false;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ExprFingerprint)) return false;
            ExprFingerprint that = (ExprFingerprint) other;
            boolean castLengthCompatible = !"CAST".equals(kind) || !"CHAR".equals(resultType)
                    || "*".equals(constant) || "*".equals(that.constant) || constant.equals(that.constant);
            return castLengthCompatible && kind.equals(that.kind) && resultType.equals(that.resultType)
                    && children.equals(that.children) && java.util.Objects.equals(column, that.column);
        }

        @Override
        public int hashCode() {
            String hashConstant = "CAST".equals(kind) && "CHAR".equals(resultType) ? "*" : constant;
            return java.util.Objects.hash(kind, resultType, children, column, hashConstant);
        }
    }

    private static final class TargetPattern {
        private final String normalized;
        private final Set<String> references;
        private final Map<String, String> lineage;
        private final String expectedRole;
        private final ExprFingerprint fingerprint;

        private TargetPattern(String normalized, Set<String> references, Map<String, String> lineage,
                String expectedRole, ExprFingerprint fingerprint) {
            this.normalized = normalized;
            this.references = references;
            this.lineage = lineage == null ? Map.of() : lineage;
            this.expectedRole = expectedRole == null ? "UNKNOWN" : expectedRole;
            this.fingerprint = fingerprint;
        }

        private static TargetPattern create(EcstOperation operation) {
            String normalized = normalize(operation == null ? "" : operation.getNormalizedExpression());
            if (normalized.isEmpty()) {
                return null;
            }
            Set<String> dependencies = operation.getDependencies();
            return new TargetPattern(normalized, dependencies, operation.getDependencyLineage(),
                    operation.getExpectedRole(), ExprFingerprint.parse(operation.getNormalizedExpression()));
        }

        private boolean acceptsRole(ExpressionRole role) {
            if ("UNKNOWN".equals(expectedRole)) {
                return true;
            }
            if (expectedRole.equals(role.name())) {
                return true;
            }
            return "SELECTION_PREDICATE".equals(expectedRole) && role == ExpressionRole.JOIN_OTHER_CONDITION;
        }
    }
}
