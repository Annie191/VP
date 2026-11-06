package sqlancer.mysql.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstObservation.UnknownReason;
import sqlancer.common.oracle.ecst.EcstOperation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.mysql.MySQLGlobalState;

/** MySQL TREE evidence anchored to the target operation and its physical input relations. */
public final class MySQLExecutionContextAnalyzer {
    private static final Pattern TREE_NODE = Pattern.compile("^(\\s*)->\\s+(.*)$");
    private static final Pattern RELATION = Pattern.compile("(?i)\\bon\\s+(`?[a-z0-9_$]+`?(?:\\.`?[a-z0-9_$]+`?)*)");
    private final MySQLGlobalState state;

    public MySQLExecutionContextAnalyzer(MySQLGlobalState state) {
        this.state = state;
    }

    public static final class ParsedContext {
        private final EcstContext context;
        private final String plan;
        private final String evidence;
        private final boolean operationMatched;

        private ParsedContext(EcstContext context, String plan, String evidence, boolean matched) {
            this.context = context;
            this.plan = plan;
            this.evidence = evidence;
            this.operationMatched = matched;
        }

        public EcstContext context() {
            return context;
        }

        public String getEvidence() {
            return evidence;
        }

        public boolean isOperationMatched() {
            return operationMatched;
        }
    }

    public ParsedContext extractContext(String query, EcstOperation operation, Set<String> relations) {
        try {
            return parse(explain(query), operation, relations);
        } catch (SQLException failure) {
            return new ParsedContext(unknown(), "", "EXPLAIN failed: " + failure.getSQLState() + ":"
                    + failure.getErrorCode() + ":" + failure.getMessage(), false);
        }
    }

    public EcstObservation analyze(MySQLVerticalPartitioningOracle.EcstCase testCase, EcstTarget target) {
        ParsedContext original = extractContext(testCase.getOriginalQuery(), testCase.getOperation(),
                Set.of(testCase.seed.getSourceTable()));
        ParsedContext transformed = extractContext(testCase.getTransformedQuery(), testCase.getOperation(),
                testCase.getAccessRelations().isEmpty() ? Set.of("ecst_l", "ecst_r") : testCase.getAccessRelations());
        return observe(original, transformed, target);
    }

    static EcstObservation observe(ParsedContext original, ParsedContext transformed, EcstTarget target) {
        Set<String> shifts = new LinkedHashSet<>();
        for (EcstContextDimension dimension : EcstContextDimension.values()) {
            String before = original.context.value(dimension);
            String after = transformed.context.value(dimension);
            if (!"UNKNOWN".equals(before) && !"UNKNOWN".equals(after) && !before.equals(after)) {
                shifts.add(dimension + ":" + before + "->" + after);
            }
        }
        String before = original.context.value(target.getDimension());
        String after = transformed.context.value(target.getDimension());
        EcstShiftStatus status = "UNKNOWN".equals(before) || "UNKNOWN".equals(after) ? EcstShiftStatus.UNKNOWN
                : target.getFrom().equals(before) && target.getTo().equals(after) ? EcstShiftStatus.SUCCESS
                        : EcstShiftStatus.FAILURE;
        return new EcstObservation(original.context, transformed.context, status, shifts,
                "MySQL plan evidence; causal intervention not performed", original.plan, transformed.plan,
                original.operationMatched ? original.evidence : "",
                transformed.operationMatched ? transformed.evidence : "", original.evidence, transformed.evidence,
                status == EcstShiftStatus.UNKNOWN ? UnknownReason.UNKNOWN_BECAUSE_OPERATION_MAPPING
                        : UnknownReason.NONE);
    }

    public String explain(String query) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rows = statement.executeQuery("EXPLAIN FORMAT=TREE " + query)) {
            while (rows.next()) {
                plan.append(rows.getString(1)).append('\n');
            }
        }
        return plan.toString();
    }

    private static final class Node {
        private final int indent;
        private final String text;
        private final Node parent;
        private final List<Node> children = new ArrayList<>();

        private Node(int indent, String text, Node parent) {
            this.indent = indent;
            this.text = text;
            this.parent = parent;
        }

        private boolean join() {
            return text.toLowerCase(Locale.ROOT).contains("join");
        }

        private boolean materialize() {
            return text.toLowerCase(Locale.ROOT).startsWith("materialize");
        }

        private boolean hasJoinBelow() {
            return children.stream().anyMatch(n -> n.join() || n.hasJoinBelow());
        }
    }

    static ParsedContext parse(String plan, EcstOperation operation, Set<String> relations) {
        List<Node> nodes = new ArrayList<>();
        List<Node> stack = new ArrayList<>();
        for (String line : plan.split("\\R")) {
            Matcher matcher = TREE_NODE.matcher(line);
            if (!matcher.matches()) {
                continue;
            }
            int indent = matcher.group(1).length();
            while (!stack.isEmpty() && stack.get(stack.size() - 1).indent >= indent) {
                stack.remove(stack.size() - 1);
            }
            Node parent = stack.isEmpty() ? null : stack.get(stack.size() - 1);
            Node node = new Node(indent, matcher.group(2).replaceFirst("\\s+\\(cost=.*$", ""), parent);
            if (parent != null) {
                parent.children.add(node);
            }
            nodes.add(node);
            stack.add(node);
        }
        Set<String> access = new LinkedHashSet<>();
        Set<String> boundaries = new LinkedHashSet<>();
        Set<String> normalizedRelations = relations.stream().map(r -> r.toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
        for (Node node : nodes) {
            Matcher relation = RELATION.matcher(node.text);
            if (!relation.find()) {
                continue;
            }
            String name = relation.group(1).replace("`", "").toLowerCase(Locale.ROOT);
            name = name.substring(name.lastIndexOf('.') + 1);
            if (!normalizedRelations.contains(name)) {
                continue;
            }
            String path = accessPath(node.text);
            if ("UNKNOWN".equals(path)) {
                continue;
            }
            access.add(path);
            boolean materialized = false;
            for (Node parent = node.parent; parent != null; parent = parent.parent) {
                materialized |= parent.materialize();
            }
            boundaries.add(materialized ? "MATERIALIZED" : "MERGED");
        }
        String target = fingerprint(operation.getNormalizedExpression());
        List<Node> matched = new ArrayList<>();
        boolean duplicatedInNode = false;
        if (!"PROJECTION_ITEM".equals(operation.getExpectedRole()) && target != null) {
            for (Node node : nodes) {
                String expression = null;
                if (node.text.startsWith("Filter: ")) {
                    expression = node.text.substring("Filter: ".length());
                } else if (node.text.contains("extra conditions: ")) {
                    expression = node.text
                            .substring(node.text.indexOf("extra conditions: ") + "extra conditions: ".length());
                }
                if (expression != null && hasRelevantInput(node, normalizedRelations)) {
                    try {
                        int count = countMatches(CCJSqlParserUtil.parseExpression(expression), target);
                        if (count > 0) {
                            matched.add(node);
                        }
                        duplicatedInNode |= count > 1;
                    } catch (JSQLParserException | RuntimeException unavailable) {
                        // An unrecognized expression is missing evidence, never a confirmed non-migration.
                    }
                }
            }
        }
        Set<String> locations = new LinkedHashSet<>();
        Set<String> operators = new LinkedHashSet<>();
        for (Node node : matched) {
            locations.add(node.join() || node.hasJoinBelow() ? "POST_JOIN" : "PRE_JOIN");
            operators.add(node.join() ? "JOIN_CONDITION" : "FILTER");
        }
        boolean reliableMatch = !matched.isEmpty() && !duplicatedInNode && locations.size() == 1
                && operators.size() == 1;
        EcstContext context = new EcstContext(reliableMatch ? only(locations) : "UNKNOWN",
                reliableMatch ? only(operators) : "UNKNOWN", only(boundaries), only(access), "UNKNOWN");
        return new ParsedContext(context, plan,
                "operationMatches=" + matched.size() + ", reliable=" + reliableMatch + ", inputRelations=" + relations
                        + ", relationBoundary=" + only(boundaries) + ", access=" + only(access) + "; "
                        + matched.stream().map(n -> n.text).collect(Collectors.joining("; ")),
                reliableMatch);
    }

    private static boolean hasRelevantInput(Node node, Set<String> relations) {
        Matcher matcher = RELATION.matcher(node.text);
        if (matcher.find()) {
            String name = matcher.group(1).replace("`", "").toLowerCase(Locale.ROOT);
            if (relations.contains(name.substring(name.lastIndexOf('.') + 1))) {
                return true;
            }
        }
        return node.children.stream().anyMatch(child -> hasRelevantInput(child, relations));
    }

    private static String only(Set<String> values) {
        return values.size() == 1 ? values.iterator().next() : "UNKNOWN";
    }

    private static EcstContext unknown() {
        return new EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN");
    }

    private static String accessPath(String text) {
        String lower = text.toLowerCase(Locale.ROOT);
        if (lower.contains("index range scan")) {
            return "INDEX_RANGE";
        }
        if (lower.contains("index lookup")) {
            return "INDEX_LOOKUP";
        }
        if (lower.contains("index scan")) {
            return "INDEX_SCAN";
        }
        if (lower.startsWith("table scan on ")) {
            return "TABLE_SCAN";
        }
        return "UNKNOWN";
    }

    static String fingerprint(String expression) {
        try {
            return key(CCJSqlParserUtil.parseExpression(expression));
        } catch (JSQLParserException | RuntimeException unavailable) {
            return null;
        }
    }

    private static int countMatches(Expression expression, String target) {
        if (expression instanceof Parenthesis) {
            return countMatches(((Parenthesis) expression).getExpression(), target);
        }
        int count = target.equals(key(expression)) ? 1 : 0;
        for (Expression child : children(expression)) {
            count += countMatches(child, target);
        }
        return count;
    }

    private static List<Expression> children(Expression expression) {
        if (expression instanceof BinaryExpression) {
            BinaryExpression binary = (BinaryExpression) expression;
            return List.of(binary.getLeftExpression(), binary.getRightExpression());
        }
        if (expression instanceof CastExpression) {
            return List.of(((CastExpression) expression).getLeftExpression());
        }
        if (expression instanceof SignedExpression) {
            return List.of(((SignedExpression) expression).getExpression());
        }
        if (expression instanceof Parenthesis) {
            return List.of(((Parenthesis) expression).getExpression());
        }
        if (expression instanceof IsNullExpression) {
            return List.of(((IsNullExpression) expression).getLeftExpression());
        }
        if (expression instanceof WhenClause) {
            WhenClause when = (WhenClause) expression;
            return List.of(when.getWhenExpression(), when.getThenExpression());
        }
        if (expression instanceof CaseExpression) {
            CaseExpression caseExpression = (CaseExpression) expression;
            List<Expression> result = new ArrayList<>();
            if (caseExpression.getSwitchExpression() != null) {
                result.add(caseExpression.getSwitchExpression());
            }
            if (caseExpression.getWhenClauses() != null) {
                result.addAll(caseExpression.getWhenClauses());
            }
            if (caseExpression.getElseExpression() != null) {
                result.add(caseExpression.getElseExpression());
            }
            return result;
        }
        if (expression instanceof Function && ((Function) expression).getParameters() != null) {
            return ((Function) expression).getParameters().getExpressions();
        }
        return List.of();
    }

    private static String key(Expression expression) {
        if (expression instanceof Parenthesis) {
            return key(((Parenthesis) expression).getExpression());
        }
        if (expression instanceof Column) {
            return "column:" + ((Column) expression).getColumnName().replace("`", "").toLowerCase(Locale.ROOT);
        }
        if (expression instanceof BinaryExpression) {
            BinaryExpression binary = (BinaryExpression) expression;
            String operator = expression.getClass().getSimpleName();
            String left = key(binary.getLeftExpression());
            String right = key(binary.getRightExpression());
            if ("GreaterThan".equals(operator) || "GreaterThanEquals".equals(operator)) {
                operator = "GreaterThan".equals(operator) ? "MinorThan" : "MinorThanEquals";
                String swap = left;
                left = right;
                right = swap;
            }
            return operator + "[" + left + "," + right + "]";
        }
        if (expression instanceof Function) {
            Function function = (Function) expression;
            return "function:" + function.getName().toLowerCase(Locale.ROOT) + ":" + function.isDistinct() + "["
                    + children(expression).stream().map(MySQLExecutionContextAnalyzer::key)
                            .collect(Collectors.joining(","))
                    + "]";
        }
        if (expression instanceof CastExpression) {
            CastExpression cast = (CastExpression) expression;
            return "cast:" + cast.getType().toString().toLowerCase(Locale.ROOT).replaceAll("\\s+", "") + "["
                    + key(cast.getLeftExpression()) + "]";
        }
        if (expression instanceof SignedExpression) {
            SignedExpression signed = (SignedExpression) expression;
            return signed.getSign() + "[" + key(signed.getExpression()) + "]";
        }
        if (expression instanceof CaseExpression || expression instanceof WhenClause
                || expression instanceof IsNullExpression) {
            String negation = expression instanceof IsNullExpression && ((IsNullExpression) expression).isNot() ? "not"
                    : "";
            return expression.getClass().getSimpleName() + negation + "[" + children(expression).stream()
                    .map(MySQLExecutionContextAnalyzer::key).collect(Collectors.joining(",")) + "]";
        }
        return expression.getClass().getSimpleName() + ":" + expression.toString();
    }
}
