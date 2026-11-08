package sqlancer.databend.oracle;

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
import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstExpressionMatcher;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstOperation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.oracle.DatabendVerticalPartitioningOracle.EcstCase;

/** Databend physical operators and FUSE pruning counts; opaque CTE scans never establish producer lineage. */
public final class DatabendExecutionContextAnalyzer {
    private final DatabendGlobalState state;

    public DatabendExecutionContextAnalyzer(DatabendGlobalState state) {
        this.state = state;
    }

    public static final class ParsedContext {
        private final EcstContext context;
        private final String plan;
        private final String evidence;
        private final boolean matched;

        private ParsedContext(EcstContext context, String plan, String evidence, boolean matched) {
            this.context = context;
            this.plan = plan;
            this.evidence = evidence;
            this.matched = matched;
        }

        public EcstContext context() {
            return context;
        }

        public String getEvidence() {
            return evidence;
        }

        public boolean isOperationMatched() {
            return matched;
        }
    }

    private static final class Node {
        final int indent;
        final String name;
        final List<Node> children = new ArrayList<>();
        final Map<String, String> fields = new LinkedHashMap<>();

        Node(int indent, String name) {
            this.indent = indent;
            this.name = name;
        }

        String field(String key) {
            return fields.getOrDefault(key, "");
        }
    }

    public ParsedContext extractContext(String sql, EcstOperation operation, Set<String> relations)
            throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rows = statement.executeQuery("EXPLAIN " + sql)) {
            while (rows.next()) {
                plan.append(rows.getString(1)).append('\n');
            }
        }
        if (plan.length() == 0) {
            throw new SQLException("Databend EXPLAIN returned no plan");
        }
        return parse(plan.toString(), operation, relations);
    }

    public EcstObservation analyze(EcstCase testCase, EcstTarget target) throws SQLException {
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
            if (!from.equals("UNKNOWN") && !to.equals("UNKNOWN") && !from.equals(to)) {
                shifts.add(dimension + ":" + from + "->" + to);
            }
        }
        String from = before.context.value(target.getDimension());
        String to = after.context.value(target.getDimension());
        EcstShiftStatus status = from.equals("UNKNOWN") || to.equals("UNKNOWN") ? EcstShiftStatus.UNKNOWN
                : target.getFrom().equals(from) && target.getTo().equals(to) ? EcstShiftStatus.SUCCESS
                        : EcstShiftStatus.FAILURE;
        return new EcstObservation(before.context, after.context, status, shifts,
                "Databend native physical plan and FUSE partition counts; no causal confirmation", before.plan,
                after.plan, before.matched ? before.evidence : "", after.matched ? after.evidence : "", before.evidence,
                after.evidence,
                status == EcstShiftStatus.UNKNOWN ? EcstObservation.UnknownReason.UNKNOWN_BECAUSE_OPERATION_MAPPING
                        : EcstObservation.UnknownReason.NONE);
    }

    static ParsedContext parse(String plan, EcstOperation operation, Set<String> relations) {
        List<Node> nodes = new ArrayList<>();
        List<Node> stack = new ArrayList<>();
        for (String line : plan.split("\\R")) {
            String body = line.replaceFirst("^[\\s│├└─]*", "");
            int indent = line.length() - body.length();
            if (body.matches("[A-Z][A-Za-z0-9]*(?:\\((?:Build|Probe)\\))?")) {
                while (!stack.isEmpty() && stack.get(stack.size() - 1).indent >= indent) {
                    stack.remove(stack.size() - 1);
                }
                Node node = new Node(indent, body);
                if (!stack.isEmpty()) {
                    stack.get(stack.size() - 1).children.add(node);
                }
                nodes.add(node);
                stack.add(node);
            } else if (!stack.isEmpty() && body.contains(":")) {
                while (stack.size() > 1 && stack.get(stack.size() - 1).indent >= indent) {
                    stack.remove(stack.size() - 1);
                }
                int colon = body.indexOf(':');
                stack.get(stack.size() - 1).fields.put(body.substring(0, colon).toLowerCase(Locale.ROOT),
                        body.substring(colon + 1).trim());
            }
        }
        Set<String> names = relations.stream().map(DatabendExecutionContextAnalyzer::name).collect(Collectors.toSet());
        Set<String> paths = new LinkedHashSet<>();
        boolean sourceSeen = false;
        for (Node node : nodes) {
            if (!source(node, names)) {
                continue;
            }
            sourceSeen = true;
            try {
                long total = Long.parseLong(node.field("partitions total"));
                long scanned = Long.parseLong(node.field("partitions scanned"));
                if (total > 0 && scanned >= 0 && scanned <= total) {
                    paths.add(scanned < total ? "PARTITION_PRUNED" : "TABLE_SCAN");
                } else {
                    paths.add("UNKNOWN");
                }
            } catch (NumberFormatException unknown) {
                paths.add("UNKNOWN");
            }
        }
        String key = EcstExpressionMatcher.fingerprint(operation.getNormalizedExpression());
        int count = 0;
        Set<String> locations = new LinkedHashSet<>();
        Set<String> roles = new LinkedHashSet<>();
        List<String> evidence = new ArrayList<>();
        for (Node node : nodes) {
            if (key == null || !relevant(node, names)) {
                continue;
            }
            String role;
            String expressions;
            if (operation.getExpectedRole().equals("PROJECTION_ITEM")) {
                if (!node.name.equals("EvalScalar")) {
                    continue;
                }
                role = "PROJECTION";
                expressions = node.field("expressions");
            } else if (node.name.equals("Filter")) {
                role = "FILTER";
                expressions = node.field("filters");
            } else if (node.name.contains("Join")) {
                role = "JOIN_CONDITION";
                expressions = node.field("filters");
            } else if (source(node, names)) {
                role = "SCAN_FILTER";
                String push = node.field("push downs");
                int start = push.indexOf("filters: [");
                int end = push.lastIndexOf("], limit:");
                expressions = start >= 0 && end > start ? push.substring(start + 9, end + 1) : "";
            } else {
                continue;
            }
            for (String expression : expressions(expressions)) {
                String normalized = normalize(expression);
                if (normalized.contains("#")) {
                    continue;
                }
                try {
                    int matches = EcstExpressionMatcher.countMatches(CCJSqlParserUtil.parseExpression(normalized), key);
                    if (matches > 0) {
                        count += matches;
                        roles.add(role);
                        locations.add(hasJoin(node) ? "POST_JOIN" : "PRE_JOIN");
                        evidence.add(node.name + ":" + expression);
                    }
                } catch (Exception unavailable) {
                    // Native forms that cannot be mapped structurally remain unknown.
                }
            }
        }
        boolean matched = count == 1 && roles.size() == 1 && locations.size() == 1;
        return new ParsedContext(
                new EcstContext(matched ? only(locations) : "UNKNOWN", matched ? only(roles) : "UNKNOWN",
                        sourceSeen ? "MERGED" : "UNKNOWN", only(paths), "UNKNOWN"),
                plan, "matches=" + count + "; relations=" + names + "; " + String.join(";", evidence), matched);
    }

    private static String name(String relation) {
        String value = relation.replace("`", "").replace("\"", "");
        return value.substring(value.lastIndexOf('.') + 1).toLowerCase(Locale.ROOT);
    }

    private static boolean source(Node node, Set<String> names) {
        return node.name.startsWith("TableScan") && names.contains(name(node.field("table")));
    }

    private static boolean relevant(Node node, Set<String> names) {
        Set<String> found = new LinkedHashSet<>();
        collectRelations(node, names, found);
        return !names.isEmpty() && found.containsAll(names);
    }

    private static void collectRelations(Node node, Set<String> names, Set<String> found) {
        if (source(node, names)) {
            found.add(name(node.field("table")));
        }
        node.children.forEach(child -> collectRelations(child, names, found));
    }

    private static boolean hasJoin(Node node) {
        return node.name.contains("Join") || node.children.stream().anyMatch(DatabendExecutionContextAnalyzer::hasJoin);
    }

    private static String only(Set<String> values) {
        return values.size() == 1 ? values.iterator().next() : "UNKNOWN";
    }

    static String normalize(String expression) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < expression.length();) {
            char current = expression.charAt(i);
            if (current == '\'' || current == '`' || current == '"') {
                char quote = current;
                result.append(current);
                i++;
                while (i < expression.length()) {
                    current = expression.charAt(i++);
                    result.append(current);
                    if (current == '\\' && i < expression.length()) {
                        result.append(expression.charAt(i++));
                    } else if (current == quote) {
                        if (i < expression.length() && expression.charAt(i) == quote) {
                            result.append(expression.charAt(i++));
                        } else {
                            break;
                        }
                    }
                }
            } else {
                java.util.regex.Matcher ordinal = java.util.regex.Pattern.compile("\\s*\\(#\\d+\\)")
                        .matcher(expression);
                ordinal.region(i, expression.length());
                if (ordinal.lookingAt()) {
                    i = ordinal.end();
                } else {
                    result.append(current);
                    i++;
                }
            }
        }
        return result.toString();
    }

    private static List<String> expressions(String value) {
        List<String> result = new ArrayList<>();
        String text = value.startsWith("[") && value.endsWith("]") ? value.substring(1, value.length() - 1) : value;
        int depth = 0;
        int start = 0;
        char quote = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (quote != 0) {
                if (c == '\\') {
                    i++;
                } else if (c == quote) {
                    if (i + 1 < text.length() && text.charAt(i + 1) == quote) {
                        i++;
                    } else {
                        quote = 0;
                    }
                }
            } else if (c == '\'' || c == '`' || c == '"') {
                quote = c;
            } else if (c == '(' || c == '[') {
                depth++;
            } else if (c == ')' || c == ']') {
                depth--;
            } else if (c == ',' && depth == 0) {
                result.add(text.substring(start, i).trim());
                start = i + 1;
            }
        }
        if (start < text.length()) {
            result.add(text.substring(start).trim());
        }
        return result;
    }
}
