package sqlancer.cockroachdb.oracle;

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
import java.util.stream.Collectors;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import sqlancer.cockroachdb.CockroachDBProvider;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstExpressionMatcher;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstObservation.UnknownReason;
import sqlancer.common.oracle.ecst.EcstOperation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;

/** Native physical operators and buffer producer/consumer lineage, without treating SQL text as execution evidence. */
public final class CockroachDBExecutionContextAnalyzer {
    private final CockroachDBGlobalState state;

    public CockroachDBExecutionContextAnalyzer(CockroachDBGlobalState state) {
        this.state = state;
    }

    public static final class ParsedContext {
        private final EcstContext context;
        private final String plan;
        private final String evidence;
        private final boolean operationMatched;

        private ParsedContext(EcstContext context, String plan, String evidence, boolean operationMatched) {
            this.context = context;
            this.plan = plan;
            this.evidence = evidence;
            this.operationMatched = operationMatched;
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
        final int indent;
        final String operator;
        final Node parent;
        final List<Node> children = new ArrayList<>();
        final Map<String, List<String>> fields = new LinkedHashMap<>();

        Node(int indent, String operator, Node parent) {
            this.indent = indent;
            this.operator = operator;
            this.parent = parent;
        }

        String field(String name) {
            List<String> values = fields.get(name);
            return values == null || values.isEmpty() ? "" : values.get(0);
        }

        boolean join() {
            return operator.contains("join");
        }
    }

    public String explain(String query) throws SQLException {
        StringBuilder text = new StringBuilder();
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rows = statement.executeQuery("EXPLAIN (VERBOSE) " + query)) {
            while (rows.next()) {
                text.append(rows.getString(1)).append('\n');
            }
        }
        return text.toString();
    }

    public ParsedContext extractContext(String query, EcstOperation operation, Set<String> relations)
            throws SQLException {
        try {
            return parse(explain(query), operation, relations);
        } catch (SQLException error) {
            if (error.getSQLState() != null && error.getSQLState().startsWith("XX")
                    && !CockroachDBProvider.isCockroachTransientClusterError(String.valueOf(error.getMessage()))) {
                throw error;
            }
            return new ParsedContext(unknown(), "",
                    "CockroachDB EXPLAIN failed: " + error.getSQLState() + ":" + error.getMessage(), false);
        }
    }

    public EcstObservation analyze(CockroachDBVerticalPartitioningOracle.EcstCase testCase, EcstTarget target)
            throws SQLException {
        return observe(
                extractContext(testCase.getOriginalQuery(), testCase.getOperation(),
                        Set.of(testCase.seed.getSourceTable())),
                extractContext(testCase.getTransformedQuery(), testCase.getOperation(), testCase.getAccessRelations()),
                target);
    }

    static EcstObservation observe(ParsedContext before, ParsedContext after, EcstTarget target) {
        Set<String> shifts = new LinkedHashSet<>();
        for (EcstContextDimension dimension : EcstContextDimension.values()) {
            String from = before.context.value(dimension);
            String to = after.context.value(dimension);
            if (!"UNKNOWN".equals(from) && !"UNKNOWN".equals(to) && !from.equals(to)) {
                shifts.add(dimension + ":" + from + "->" + to);
            }
        }
        String from = before.context.value(target.getDimension());
        String to = after.context.value(target.getDimension());
        EcstShiftStatus status = "UNKNOWN".equals(from) || "UNKNOWN".equals(to) ? EcstShiftStatus.UNKNOWN
                : target.getFrom().equals(from) && target.getTo().equals(to) ? EcstShiftStatus.SUCCESS
                        : EcstShiftStatus.FAILURE;
        return new EcstObservation(before.context, after.context, status, shifts,
                "CockroachDB native physical-plan evidence for single-node ECST", before.plan, after.plan,
                before.operationMatched ? before.evidence : "", after.operationMatched ? after.evidence : "",
                before.evidence, after.evidence, status == EcstShiftStatus.UNKNOWN
                        ? UnknownReason.UNKNOWN_BECAUSE_OPERATION_MAPPING : UnknownReason.NONE);
    }

    static ParsedContext parse(String plan, EcstOperation operation, Set<String> relations) {
        List<Node> nodes = new ArrayList<>();
        List<Node> stack = new ArrayList<>();
        Node current = null;
        for (String line : plan.split("\\R")) {
            int bullet = line.indexOf('•');
            if (bullet >= 0 && line.substring(0, bullet).matches("[\\s│├└─]*")) {
                while (!stack.isEmpty() && stack.get(stack.size() - 1).indent >= bullet) {
                    stack.remove(stack.size() - 1);
                }
                Node parent = stack.isEmpty() ? null : stack.get(stack.size() - 1);
                current = new Node(bullet, line.substring(bullet + 1).trim().toLowerCase(Locale.ROOT), parent);
                if (parent != null) {
                    parent.children.add(current);
                }
                nodes.add(current);
                stack.add(current);
            } else if (current != null) {
                String field = line.replaceFirst("^[\\s│├└─]+", "");
                int colon = field.indexOf(':');
                if (colon > 0) {
                    current.fields
                            .computeIfAbsent(field.substring(0, colon).toLowerCase(Locale.ROOT), k -> new ArrayList<>())
                            .add(field.substring(colon + 1).trim());
                }
            }
        }
        Map<String, Node> buffers = new LinkedHashMap<>();
        for (Node node : nodes) {
            if (node.operator.equals("buffer")) {
                buffers.put(bufferLabel(node), node);
            }
        }
        Set<String> names = relations.stream().map(CockroachDBExecutionContextAnalyzer::relationName)
                .collect(Collectors.toSet());
        Set<String> paths = new LinkedHashSet<>();
        Set<String> boundaries = new LinkedHashSet<>();
        for (Node node : nodes) {
            if (!names.contains(relationName(node.field("table")))) {
                continue;
            }
            String path = access(node);
            if ("UNKNOWN".equals(path)) {
                continue;
            }
            paths.add(path);
            boolean materialized = false;
            for (Node p = node.parent; p != null; p = p.parent) {
                if (p.operator.equals("buffer")) {
                    materialized = true;
                }
            }
            boundaries.add(materialized ? "MATERIALIZED" : "MERGED");
        }
        String fingerprint = EcstExpressionMatcher
                .fingerprint(normalizeExpression(operation.getNormalizedExpression()));
        Set<String> locations = new LinkedHashSet<>();
        Set<String> operators = new LinkedHashSet<>();
        List<String> matches = new ArrayList<>();
        int count = 0;
        if (fingerprint != null) {
            boolean projection = "PROJECTION_ITEM".equals(operation.getExpectedRole());
            for (Node node : nodes) {
                if (!relevant(node, names, buffers, new LinkedHashSet<>())) {
                    continue;
                }
                for (Map.Entry<String, List<String>> field : node.fields.entrySet()) {
                    String role = projection && field.getKey().startsWith("render ") ? "PROJECTION"
                            : !projection && field.getKey().equals("filter") ? "FILTER"
                                    : !projection && field.getKey().equals("pred") ? "JOIN_CONDITION" : null;
                    if (role == null) {
                        continue;
                    }
                    for (String expression : field.getValue()) {
                        try {
                            int occurrences = EcstExpressionMatcher.countMatches(
                                    CCJSqlParserUtil.parseExpression(normalizeExpression(expression)), fingerprint);
                            if (occurrences > 0) {
                                count += occurrences;
                                matches.add(node.operator + " " + field.getKey() + ": " + expression);
                                operators.add(role);
                                locations.add(hasJoin(node, buffers, new LinkedHashSet<>()) ? "POST_JOIN" : "PRE_JOIN");
                            }
                        } catch (Exception unsupported) {
                            // Unsupported native syntax remains unmapped.
                        }
                    }
                }
            }
        }
        boolean reliable = count == 1 && matches.size() == 1 && locations.size() == 1 && operators.size() == 1;
        EcstContext context = new EcstContext(reliable ? only(locations) : "UNKNOWN",
                reliable ? only(operators) : "UNKNOWN", only(boundaries), only(paths), "UNKNOWN");
        return new ParsedContext(context, plan,
                "operationMatches=" + count + ", reliable=" + reliable + ", relations=" + relations + ", boundary="
                        + only(boundaries) + ", access=" + only(paths) + "; " + String.join("; ", matches),
                reliable);
    }

    private static String bufferLabel(Node node) {
        return node.field("label").replaceFirst("\\s*\\(.*$", "").trim();
    }

    private static List<Node> inputs(Node node, Map<String, Node> buffers) {
        List<Node> result = new ArrayList<>(node.children);
        if (node.operator.equals("scan buffer")) {
            Node producer = buffers.get(bufferLabel(node));
            if (producer != null) {
                result.add(producer);
            }
        }
        return result;
    }

    private static boolean relevant(Node node, Set<String> names, Map<String, Node> buffers, Set<Node> visited) {
        if (!visited.add(node)) {
            return false;
        }
        return names.contains(relationName(node.field("table")))
                || inputs(node, buffers).stream().anyMatch(n -> relevant(n, names, buffers, visited));
    }

    private static boolean hasJoin(Node node, Map<String, Node> buffers, Set<Node> visited) {
        return visited.add(node)
                && (node.join() || inputs(node, buffers).stream().anyMatch(n -> hasJoin(n, buffers, visited)));
    }

    private static String relationName(String name) {
        String table = name.split("@", 2)[0].replace("\"", "").trim().toLowerCase(Locale.ROOT);
        int dot = table.lastIndexOf('.');
        return dot < 0 ? table : table.substring(dot + 1);
    }

    private static String access(Node node) {
        if (!node.operator.equals("scan") && !node.operator.startsWith("scan ")) {
            return "UNKNOWN";
        }
        String table = node.field("table");
        int at = table.indexOf('@');
        if (at < 0 || node.field("spans").isEmpty()) {
            return "UNKNOWN";
        }
        String index = table.substring(at + 1).replace("\"", "").toLowerCase(Locale.ROOT);
        boolean primary = index.equals(relationName(table) + "_pkey") || index.equals("primary");
        boolean full = node.field("spans").equalsIgnoreCase("FULL SCAN");
        return primary ? (full ? "TABLE_SCAN" : "PRIMARY_RANGE") : (full ? "INDEX_SCAN" : "INDEX_RANGE");
    }

    static String normalizeExpression(String expression) {
        StringBuilder result = new StringBuilder();
        StringBuilder code = new StringBuilder();
        for (int i = 0; i < expression.length();) {
            char ch = expression.charAt(i);
            if (ch == '\'') {
                result.append(normalizeCode(code.toString()));
                code.setLength(0);
                int start = i++;
                while (i < expression.length()) {
                    char c = expression.charAt(i++);
                    if (c == '\\' && i < expression.length()) {
                        i++;
                    } else if (c == '\'') {
                        if (i < expression.length() && expression.charAt(i) == '\'') {
                            i++;
                        } else {
                            break;
                        }
                    }
                }
                result.append(expression, start, i);
            } else if (ch == '"') {
                code.append('`');
                i++;
                while (i < expression.length()) {
                    char c = expression.charAt(i++);
                    if (c == '"') {
                        if (i < expression.length() && expression.charAt(i) == '"') {
                            code.append('"');
                            i++;
                        } else {
                            break;
                        }
                    } else {
                        code.append(c);
                    }
                }
                code.append('`');
            } else {
                code.append(ch);
                i++;
            }
        }
        return result.append(normalizeCode(code.toString())).toString();
    }

    private static String normalizeCode(String code) {
        return code.replaceAll("(?i)(?<![\\w.])([+-]?\\d+(?:\\.\\d+)?):::[a-z][a-z0-9_]*(?:\\([0-9, ]+\\))?", "$1")
                .replace(":::", "::");
    }

    private static String only(Set<String> values) {
        return values.size() == 1 ? values.iterator().next() : "UNKNOWN";
    }

    private static EcstContext unknown() {
        return new EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN");
    }
}
