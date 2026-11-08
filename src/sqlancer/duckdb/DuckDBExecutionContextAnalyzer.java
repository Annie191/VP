package sqlancer.duckdb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstExpressionMatcher;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstOperation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBVerticalPartitioningOracle.EcstCase;

/** Executed native JSON plans; CTE consumer indexes resolve to their physical producer inputs. */
public final class DuckDBExecutionContextAnalyzer {
    private final DuckDBGlobalState state;

    public DuckDBExecutionContextAnalyzer(DuckDBGlobalState state) {
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
        private final JsonNode info;
        private final String name;
        private final Node parent;
        private final List<Node> children = new ArrayList<>();

        private Node(JsonNode data, Node parent) {
            this.name = data.path("operator_name").asText(data.path("name").asText()).trim();
            this.info = data.path("extra_info");
            this.parent = parent;
        }
    }

    public ParsedContext extractContext(String sql, EcstOperation operation, Set<String> relations)
            throws SQLException {
        synchronized (DuckDBVerticalPartitioningOracle.ecstExecutionLock()) {
            String plan = null;
            try (Statement s = state.getConnection().createStatement();
                    ResultSet rows = s.executeQuery("EXPLAIN (ANALYZE,FORMAT JSON) " + sql)) {
                while (rows.next()) {
                    plan = rows.getString(2);
                }
            }
            if (plan == null) {
                throw new SQLException("DuckDB EXPLAIN ANALYZE returned no plan");
            }
            return parse(plan, operation, relations);
        }
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
        for (EcstContextDimension d : EcstContextDimension.values()) {
            String from = before.context.value(d);
            String to = after.context.value(d);
            if (!from.equals("UNKNOWN") && !to.equals("UNKNOWN") && !from.equals(to)) {
                shifts.add(d + ":" + from + "->" + to);
            }
        }
        String from = before.context.value(target.getDimension());
        String to = after.context.value(target.getDimension());
        EcstShiftStatus status = from.equals("UNKNOWN") || to.equals("UNKNOWN") ? EcstShiftStatus.UNKNOWN
                : target.getFrom().equals(from) && target.getTo().equals(to) ? EcstShiftStatus.SUCCESS
                        : EcstShiftStatus.FAILURE;
        return new EcstObservation(before.context, after.context, status, shifts,
                "DuckDB executed physical-plan evidence; no causal confirmation", before.plan, after.plan,
                before.matched ? before.evidence : "", after.matched ? after.evidence : "", before.evidence,
                after.evidence,
                status == EcstShiftStatus.UNKNOWN ? EcstObservation.UnknownReason.UNKNOWN_BECAUSE_OPERATION_MAPPING
                        : EcstObservation.UnknownReason.NONE);
    }

    static ParsedContext parse(String plan, EcstOperation operation, Set<String> relations) {
        List<Node> nodes = new ArrayList<>();
        try {
            JsonNode json = new ObjectMapper().readTree(plan);
            if (json.isArray()) {
                for (JsonNode n : json) {
                    build(n, null, nodes);
                }
            } else if (json.isObject() && json.has("children")) {
                build(json, null, nodes);
            } else {
                return unknown(plan, "unrecognized native envelope");
            }
        } catch (Exception unsupported) {
            return unknown(plan, "invalid native JSON");
        }
        Map<String, Node> producers = new HashMap<>();
        Set<String> ambiguous = new HashSet<>();
        for (Node node : nodes) {
            if (node.name.equals("CTE") && node.info.has("Table Index") && !node.children.isEmpty()) {
                String index = node.info.path("Table Index").asText();
                if (producers.putIfAbsent(index, node.children.get(0)) != null) {
                    ambiguous.add(index);
                }
            }
        }
        ambiguous.forEach(producers::remove);
        Set<String> paths = new LinkedHashSet<>();
        Set<String> boundaries = new LinkedHashSet<>();
        for (Node node : nodes) {
            if (!source(node, relations)) {
                continue;
            }
            String type = node.info.path("Type").asText();
            if (type.equals("Sequential Scan")) {
                paths.add("TABLE_SCAN");
            } else if (type.equals("Index Scan")) {
                paths.add("INDEX_SCAN");
            }
            boolean materialized = false;
            for (Node child = node, p = node.parent; p != null; child = p, p = p.parent) {
                if (p.name.equals("CTE") && !p.children.isEmpty() && p.children.get(0) == child) {
                    materialized = true;
                }
            }
            boundaries.add(materialized ? "MATERIALIZED" : "MERGED");
        }
        Set<String> keys = fingerprints(operation.getNormalizedExpression());
        List<String> evidence = new ArrayList<>();
        Set<String> locations = new LinkedHashSet<>();
        Set<String> operators = new LinkedHashSet<>();
        int count = 0;
        if (!keys.isEmpty()) {
            for (Node node : nodes) {
                if (!relevant(node, relations, producers, new HashSet<>())) {
                    continue;
                }
                String field;
                String role;
                if (operation.getExpectedRole().equals("PROJECTION_ITEM")) {
                    // DuckDB 1.5 exposes projection aliases/ordinals, not an expression tree.
                    // Never promote those display names to operation-placement evidence.
                    continue;
                } else if (node.name.equals("FILTER")) {
                    field = "Expression";
                    role = "FILTER";
                } else if (node.name.contains("JOIN")) {
                    field = node.info.has("Condition") ? "Condition" : "Conditions";
                    role = "JOIN_CONDITION";
                } else if (source(node, relations)) {
                    field = "Filters";
                    role = "SCAN_FILTER";
                } else {
                    continue;
                }
                for (String expression : values(node.info.path(field))) {
                    if (role.equals("PROJECTION") && !expression.matches(".*[()+*/-].*")) {
                        continue;
                    }
                    int matches = keys.stream().mapToInt(key -> matches(expression, key)).sum();
                    if (matches > 0) {
                        count += matches;
                        evidence.add(node.name + " " + field + "=" + expression);
                        locations.add(hasJoin(node, producers, new HashSet<>()) ? "POST_JOIN" : "PRE_JOIN");
                        operators.add(role);
                    }
                }
            }
        }
        boolean matched = count == 1 && locations.size() == 1 && operators.size() == 1;
        return new ParsedContext(
                new EcstContext(matched ? only(locations) : "UNKNOWN", matched ? only(operators) : "UNKNOWN",
                        only(boundaries), only(paths), "UNKNOWN"),
                plan, "matches=" + count + "; relations=" + relations + "; " + String.join(";", evidence), matched);
    }

    private static ParsedContext unknown(String plan, String why) {
        return new ParsedContext(new EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"), plan, why,
                false);
    }

    private static void build(JsonNode data, Node parent, List<Node> nodes) {
        Node node = new Node(data, parent);
        nodes.add(node);
        if (parent != null) {
            parent.children.add(node);
        }
        for (JsonNode child : data.path("children")) {
            build(child, node, nodes);
        }
    }

    private static boolean source(Node node, Set<String> relations) {
        if (!node.info.has("Table")) {
            return false;
        }
        String table = node.info.path("Table").asText().replace("\"", "");
        return relations.contains(table.substring(table.lastIndexOf('.') + 1));
    }

    private static boolean relevant(Node node, Set<String> relations, Map<String, Node> producers, Set<Node> seen) {
        if (!seen.add(node)) {
            return false;
        }
        if (source(node, relations)) {
            return true;
        }
        if (node.name.equals("CTE_SCAN")) {
            Node producer = producers.get(node.info.path("CTE Index").asText());
            if (producer != null && relevant(producer, relations, producers, seen)) {
                return true;
            }
        }
        for (Node child : node.children) {
            if (relevant(child, relations, producers, seen)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasJoin(Node node, Map<String, Node> producers, Set<Node> seen) {
        if (!seen.add(node)) {
            return false;
        }
        if (node.name.contains("JOIN")) {
            return true;
        }
        if (node.name.equals("CTE_SCAN")) {
            Node producer = producers.get(node.info.path("CTE Index").asText());
            if (producer != null && hasJoin(producer, producers, seen)) {
                return true;
            }
        }
        for (Node child : node.children) {
            if (hasJoin(child, producers, seen)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> values(JsonNode node) {
        List<String> result = new ArrayList<>();
        if (node.isArray()) {
            node.forEach(n -> result.add(n.asText()));
        } else if (node.isTextual()) {
            result.add(node.asText());
        }
        return result;
    }

    private static int matches(String expression, String key) {
        // Positional references and display aliases do not identify the fixed source operation.
        if (expression.contains("#")) {
            return 0;
        }
        try {
            return EcstExpressionMatcher.countMatches(CCJSqlParserUtil.parseExpression(normalize(expression)), key);
        } catch (Exception unsupported) {
            return 0;
        }
    }

    private static Set<String> fingerprints(String expression) {
        Set<String> keys = new LinkedHashSet<>();
        String normalized = normalize(expression);
        String original = EcstExpressionMatcher.fingerprint(normalized);
        if (original != null) {
            keys.add(original);
        }
        try {
            net.sf.jsqlparser.expression.Expression parsed = CCJSqlParserUtil.parseExpression(normalized);
            while (parsed instanceof net.sf.jsqlparser.expression.Parenthesis) {
                parsed = ((net.sf.jsqlparser.expression.Parenthesis) parsed).getExpression();
            }
            String reversed = null;
            if (parsed instanceof net.sf.jsqlparser.expression.operators.relational.GreaterThan) {
                reversed = "<";
            } else if (parsed instanceof net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals) {
                reversed = "<=";
            } else if (parsed instanceof net.sf.jsqlparser.expression.operators.relational.MinorThan) {
                reversed = ">";
            } else if (parsed instanceof net.sf.jsqlparser.expression.operators.relational.MinorThanEquals) {
                reversed = ">=";
            }
            if (reversed != null) {
                net.sf.jsqlparser.expression.BinaryExpression comparison = (net.sf.jsqlparser.expression.BinaryExpression) parsed;
                String key = EcstExpressionMatcher.fingerprint("(" + comparison.getRightExpression() + ")" + reversed
                        + "(" + comparison.getLeftExpression() + ")");
                if (key != null) {
                    keys.add(key);
                }
            }
        } catch (Exception unsupported) {
            // Unsupported native expression syntax remains unknown.
        }
        return keys;
    }

    static String normalize(String expression) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < expression.length();) {
            char c = expression.charAt(i++);
            if (c == '\'') {
                result.append(c);
                while (i < expression.length()) {
                    char literal = expression.charAt(i++);
                    result.append(literal);
                    if (literal == '\\' && i < expression.length()) {
                        result.append(expression.charAt(i++));
                    } else if (literal == '\'') {
                        if (i < expression.length() && expression.charAt(i) == '\'') {
                            result.append(expression.charAt(i++));
                        } else {
                            break;
                        }
                    }
                }
            } else if (c == '"') {
                result.append('`');
                while (i < expression.length()) {
                    char name = expression.charAt(i++);
                    if (name == '"') {
                        if (i < expression.length() && expression.charAt(i) == '"') {
                            result.append('"');
                            i++;
                        } else {
                            break;
                        }
                    } else {
                        if (name == '`') {
                            result.append('`');
                        }
                        result.append(name);
                    }
                }
                result.append('`');
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    private static String only(Set<String> states) {
        return states.size() == 1 ? states.iterator().next() : "UNKNOWN";
    }
}
