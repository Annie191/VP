package sqlancer.oceanbase.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstExpressionMatcher;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstObservation.UnknownReason;
import sqlancer.common.oracle.ecst.EcstOperation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.oceanbase.OceanBaseGlobalState;

/** OceanBase operator table plus Outputs & filters evidence, separate from outline/hint suggestions. */
public final class OceanBaseExecutionContextAnalyzer {
    private static final Pattern ROW = Pattern.compile("^\\|(\\d+)\\s*\\|([^|]+)\\|([^|]*)\\|.*$");
    private static final Pattern DETAIL = Pattern.compile("^\\s*(\\d+)\\s*-\\s*(.*)$");
    private final OceanBaseGlobalState state;

    public OceanBaseExecutionContextAnalyzer(OceanBaseGlobalState state) {
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

    private static final class Node {
        private final int indent;
        private final String operator;
        private final String relation;
        private final Node parent;
        private final List<Node> children = new ArrayList<>();
        private final StringBuilder detail = new StringBuilder();

        private Node(int indent, String operator, String relation, Node parent) {
            this.indent = indent;
            this.operator = operator;
            this.relation = relation;
            this.parent = parent;
        }

        private boolean join() {
            return operator.contains("JOIN");
        }

        private boolean boundary() {
            return operator.equals("SUBPLAN SCAN") || operator.equals("VIEW");
        }

        private boolean materialize() {
            return operator.contains("MATERIAL") || operator.contains("TEMP TABLE TRANSFORMATION");
        }

        private boolean hasJoinBelow() {
            return children.stream().anyMatch(n -> n.join() || n.hasJoinBelow());
        }
    }

    public ParsedContext extractContext(String query, EcstOperation operation, Set<String> relations) {
        try {
            return parse(explain(query), operation, relations);
        } catch (SQLException failure) {
            return new ParsedContext(unknown(), "", "OceanBase EXPLAIN failed: " + failure.getSQLState() + ":"
                    + failure.getErrorCode() + ":" + failure.getMessage(), false);
        }
    }

    public String explain(String query) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rows = statement.executeQuery("EXPLAIN EXTENDED_NOADDR " + query)) {
            while (rows.next()) {
                for (int i = 1; i <= rows.getMetaData().getColumnCount(); i++) {
                    if (i > 1) {
                        plan.append(" | ");
                    }
                    plan.append(rows.getString(i));
                }
                plan.append('\n');
            }
        }
        return plan.toString();
    }

    public EcstObservation analyze(OceanBaseVerticalPartitioningOracle.EcstCase testCase, EcstTarget target) {
        ParsedContext before = extractContext(testCase.getOriginalQuery(), testCase.getOperation(),
                Set.of(testCase.seed.getSourceTable()));
        ParsedContext after = extractContext(testCase.getTransformedQuery(), testCase.getOperation(),
                testCase.getAccessRelations().isEmpty() ? Set.of("ecst_l", "ecst_r") : testCase.getAccessRelations());
        return observe(before, after, target);
    }

    static EcstObservation observe(ParsedContext before, ParsedContext after, EcstTarget target) {
        Set<String> shifts = new LinkedHashSet<>();
        for (EcstContextDimension dimension : EcstContextDimension.values()) {
            String original = before.context.value(dimension);
            String transformed = after.context.value(dimension);
            if (!"UNKNOWN".equals(original) && !"UNKNOWN".equals(transformed) && !original.equals(transformed)) {
                shifts.add(dimension + ":" + original + "->" + transformed);
            }
        }
        String original = before.context.value(target.getDimension());
        String transformed = after.context.value(target.getDimension());
        EcstShiftStatus status = "UNKNOWN".equals(original) || "UNKNOWN".equals(transformed) ? EcstShiftStatus.UNKNOWN
                : target.getFrom().equals(original) && target.getTo().equals(transformed) ? EcstShiftStatus.SUCCESS
                        : EcstShiftStatus.FAILURE;
        return new EcstObservation(before.context, after.context, status, shifts,
                "OceanBase plan-only evidence; retained SUBPLAN SCAN is not materialization proof", before.plan,
                after.plan, before.operationMatched ? before.evidence : "",
                after.operationMatched ? after.evidence : "", before.evidence, after.evidence,
                status == EcstShiftStatus.UNKNOWN ? UnknownReason.UNKNOWN_BECAUSE_OPERATION_MAPPING
                        : UnknownReason.NONE);
    }

    static ParsedContext parse(String plan, EcstOperation operation, Set<String> relations) {
        Map<Integer, Node> nodes = new LinkedHashMap<>();
        List<Node> stack = new ArrayList<>();
        boolean outputs = false;
        Node current = null;
        for (String line : plan.split("\\R")) {
            Matcher row = ROW.matcher(line);
            if (!outputs && row.matches()) {
                String cell = row.group(2);
                int indent = 0;
                while (indent < cell.length() && !Character.isLetter(cell.charAt(indent))) {
                    indent++;
                }
                String operator = cell.substring(indent).trim().toUpperCase(Locale.ROOT);
                while (!stack.isEmpty() && stack.get(stack.size() - 1).indent >= indent) {
                    stack.remove(stack.size() - 1);
                }
                Node parent = stack.isEmpty() ? null : stack.get(stack.size() - 1);
                Node node = new Node(indent, operator, row.group(3).trim(), parent);
                if (parent != null) {
                    parent.children.add(node);
                }
                nodes.put(Integer.parseInt(row.group(1)), node);
                stack.add(node);
            }
            if (line.startsWith("Outputs & filters:")) {
                outputs = true;
                continue;
            }
            if (outputs && (line.startsWith("Used Hint:") || line.startsWith("Outline Data:")
                    || line.startsWith("Optimization Info:") || line.startsWith("Qb name trace:"))) {
                break;
            }
            if (outputs) {
                Matcher detail = DETAIL.matcher(line);
                if (detail.matches()) {
                    current = nodes.get(Integer.parseInt(detail.group(1)));
                    if (current != null) {
                        current.detail.append(detail.group(2)).append(' ');
                    }
                } else if (current != null) {
                    current.detail.append(line.trim()).append(' ');
                }
            }
        }
        Set<String> normalizedRelations = relations.stream().map(OceanBaseExecutionContextAnalyzer::relationName)
                .collect(Collectors.toSet());
        Set<String> boundaries = new LinkedHashSet<>();
        Set<String> paths = new LinkedHashSet<>();
        for (Node node : nodes.values()) {
            if (!normalizedRelations.contains(relationName(node.relation))) {
                continue;
            }
            String access = accessPath(node);
            if ("UNKNOWN".equals(access)) {
                continue;
            }
            paths.add(access);
            String boundary = "MERGED";
            for (Node parent = node.parent; parent != null; parent = parent.parent) {
                if (parent.materialize()) {
                    boundary = "MATERIALIZED";
                    break;
                }
                if (parent.boundary()) {
                    boundary = "RETAINED_VIEW";
                }
            }
            boundaries.add(boundary);
        }
        String target = EcstExpressionMatcher.fingerprint(operation.getNormalizedExpression());
        List<String> matches = new ArrayList<>();
        Set<String> locations = new LinkedHashSet<>();
        Set<String> operators = new LinkedHashSet<>();
        boolean duplicateInNode = false;
        if (target != null && !"PROJECTION_ITEM".equals(operation.getExpectedRole())) {
            for (Node node : nodes.values()) {
                if (!hasRelevantInput(node, normalizedRelations)) {
                    continue;
                }
                for (String field : List.of("filter", "other_conds")) {
                    int count = 0;
                    for (String expression : expressions(node.detail.toString(), field)) {
                        try {
                            count += EcstExpressionMatcher
                                    .countMatches(CCJSqlParserUtil.parseExpression(normalizeCasts(expression)), target);
                        } catch (JSQLParserException | RuntimeException unavailable) {
                            // Unsupported internal expression syntax is insufficient evidence, not a failed shift.
                        }
                    }
                    if (count > 0) {
                        matches.add(node.operator + " " + node.relation + " " + field + ":" + node.detail);
                        locations.add(node.join() || node.hasJoinBelow() ? "POST_JOIN" : "PRE_JOIN");
                        operators.add(field.equals("other_conds") ? "JOIN_CONDITION" : "FILTER");
                        duplicateInNode |= count > 1;
                    }
                }
            }
        }
        boolean reliable = !matches.isEmpty() && !duplicateInNode && locations.size() == 1 && operators.size() == 1;
        EcstContext context = new EcstContext(reliable ? only(locations) : "UNKNOWN",
                reliable ? only(operators) : "UNKNOWN", only(boundaries), only(paths), "UNKNOWN");
        return new ParsedContext(context, plan,
                "operationMatches=" + matches.size() + ", reliable=" + reliable + ", inputRelations=" + relations
                        + ", boundary=" + only(boundaries) + ", access=" + only(paths) + "; "
                        + String.join("; ", matches),
                reliable);
    }

    private static boolean hasRelevantInput(Node node, Set<String> relations) {
        return relations.contains(relationName(node.relation))
                || node.children.stream().anyMatch(child -> hasRelevantInput(child, relations));
    }

    private static String relationName(String name) {
        String normalized = name.replace("`", "").replace("\"", "").trim().toLowerCase(Locale.ROOT);
        int index = normalized.indexOf('(');
        if (index >= 0) {
            normalized = normalized.substring(0, index).trim();
        }
        return normalized.substring(normalized.lastIndexOf('.') + 1);
    }

    private static String accessPath(Node node) {
        int marker = node.relation.indexOf('(');
        String index = marker < 0 ? "" : node.relation.substring(marker + 1).replace(")", "").trim();
        boolean secondary = marker >= 0 && !index.equalsIgnoreCase("PRIMARY")
                && !index.equalsIgnoreCase(relationName(node.relation));
        if (!node.operator.startsWith("TABLE ")) {
            return "UNKNOWN";
        }
        if (node.operator.contains("GET")) {
            return secondary ? "INDEX_GET" : "PRIMARY_GET";
        }
        if (node.operator.contains("RANGE SCAN")) {
            return secondary ? "INDEX_RANGE" : "PRIMARY_RANGE";
        }
        if (node.operator.contains("FULL SCAN") || node.operator.equals("TABLE SCAN")) {
            return secondary ? "INDEX_SCAN" : "TABLE_SCAN";
        }
        return "UNKNOWN";
    }

    private static String only(Set<String> values) {
        return values.size() == 1 ? values.iterator().next() : "UNKNOWN";
    }

    private static EcstContext unknown() {
        return new EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN");
    }

    /**
     * Reads only a named operator field, excluding output/range/outline mentions of the expression.
     *
     * @param detail
     *            the metadata attached to one operator
     * @param field
     *            the filter or join-condition field
     *
     * @return the bracket-delimited expressions in this field
     */
    static List<String> expressions(String detail, String field) {
        Matcher start = Pattern.compile("(?:^|[,\\s])" + Pattern.quote(field) + "\\(").matcher(detail);
        if (!start.find()) {
            return List.of();
        }
        int open = start.end() - 1;
        int close = closing(detail, open);
        if (close < 0) {
            return List.of();
        }
        String body = detail.substring(open + 1, close).trim();
        if (body.equalsIgnoreCase("nil")) {
            return List.of();
        }
        List<String> expressions = new ArrayList<>();
        for (int i = 0; i < body.length(); i++) {
            if (body.charAt(i) == '[') {
                int end = closing(body, i);
                if (end < 0) {
                    return List.of();
                }
                expressions.add(body.substring(i + 1, end));
                i = end;
            }
        }
        return expressions;
    }

    /**
     * Converts OceanBase's cast(value, DECIMAL(p,s)) display syntax to SQL CAST without changing structure.
     *
     * @param expression
     *            the expression emitted in an operator field
     *
     * @return a SQL expression with the same cast operands and nesting
     */
    static String normalizeCasts(String expression) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < expression.length();) {
            char quote = expression.charAt(i);
            if (quote == '\'' || quote == '"' || quote == '`') {
                int end = quotedEnd(expression, i);
                result.append(expression, i, end);
                i = end;
                continue;
            }
            Matcher cast = Pattern.compile("(?i)cast\\s*\\(").matcher(expression);
            cast.region(i, expression.length());
            if ((i == 0 || !Character.isJavaIdentifierPart(expression.charAt(i - 1))) && cast.lookingAt()) {
                int open = cast.end() - 1;
                int end = closing(expression, open);
                if (end < 0) {
                    return expression;
                }
                String body = expression.substring(open + 1, end);
                int comma = topLevelComma(body);
                if (comma >= 0) {
                    result.append("CAST(").append(normalizeCasts(body.substring(0, comma))).append(" AS ")
                            .append(body.substring(comma + 1).trim()).append(')');
                } else {
                    result.append("CAST(").append(normalizeCasts(body)).append(')');
                }
                i = end + 1;
            } else {
                result.append(expression.charAt(i++));
            }
        }
        return result.toString();
    }

    private static int topLevelComma(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'' || c == '"' || c == '`') {
                i = quotedEnd(text, i) - 1;
            } else if (c == '(') {
                int end = closing(text, i);
                if (end < 0) {
                    return -1;
                }
                i = end;
            } else if (c == ',') {
                return i;
            }
        }
        return -1;
    }

    private static int closing(String text, int open) {
        int depth = 0;
        char opening = text.charAt(open);
        char closing = opening == '[' ? ']' : ')';
        for (int i = open; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'' || c == '"' || c == '`') {
                i = quotedEnd(text, i) - 1;
            } else if (c == opening) {
                depth++;
            } else if (c == closing && --depth == 0) {
                return i;
            }
        }
        return -1;
    }

    private static int quotedEnd(String text, int start) {
        char quote = text.charAt(start);
        for (int i = start + 1; i < text.length(); i++) {
            if (text.charAt(i) == '\\') {
                i++;
            } else if (text.charAt(i) == quote) {
                if (i + 1 < text.length() && text.charAt(i + 1) == quote) {
                    i++;
                } else {
                    return i + 1;
                }
            }
        }
        return text.length();
    }
}
