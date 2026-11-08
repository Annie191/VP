package sqlancer.mariadb.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

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
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.oracle.MariaDBVerticalPartitioningOracle.EcstCase;

/** MariaDB JSON physical inputs, attached conditions, join buffers and actual materialized producers. */
public final class MariaDBExecutionContextAnalyzer {
    private final MariaDBGlobalState state;

    public MariaDBExecutionContextAnalyzer(MariaDBGlobalState state) {
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
        final String kind;
        final JsonNode info;
        Node parent;
        final List<Node> children = new ArrayList<>();

        Node(String kind, JsonNode info) {
            this.kind = kind;
            this.info = info;
        }

        void add(Node child) {
            if (child != null) {
                children.add(child);
                child.parent = this;
            }
        }
    }

    public ParsedContext extractContext(String sql, EcstOperation operation, Set<String> relations)
            throws SQLException {
        String plan;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rows = statement.executeQuery("EXPLAIN FORMAT=JSON " + sql)) {
            if (!rows.next()) {
                throw new SQLException("MariaDB EXPLAIN returned no plan");
            }
            plan = rows.getString(1);
        }
        return parse(plan, operation, relations);
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
                "MariaDB native JSON plan evidence; no causal confirmation", before.plan, after.plan,
                before.matched ? before.evidence : "", after.matched ? after.evidence : "", before.evidence,
                after.evidence,
                status == EcstShiftStatus.UNKNOWN ? EcstObservation.UnknownReason.UNKNOWN_BECAUSE_OPERATION_MAPPING
                        : EcstObservation.UnknownReason.NONE);
    }

    static ParsedContext parse(String plan, EcstOperation operation, Set<String> relations) {
        List<Node> nodes = new ArrayList<>();
        try {
            JsonNode json = new ObjectMapper().readTree(plan);
            if (json == null || !json.has("query_block")) {
                return unknown(plan, "missing native query_block");
            }
            Node tree = build(json);
            flatten(tree, nodes);
        } catch (Exception unavailable) {
            return unknown(plan, "invalid native JSON");
        }
        Set<String> names = relations.stream().map(MariaDBExecutionContextAnalyzer::name).collect(Collectors.toSet());
        Set<String> aliases = names.stream().filter(n -> n.equals("ecst_l") || n.equals("ecst_r"))
                .collect(Collectors.toSet());
        if (!aliases.isEmpty()) {
            names = aliases;
        }
        Set<String> paths = new LinkedHashSet<>();
        Set<String> seen = new LinkedHashSet<>();
        Set<String> boundaries = new LinkedHashSet<>();
        for (Node node : nodes) {
            if (!source(node, names)) {
                continue;
            }
            seen.add(name(node.info.path("table_name").asText()));
            switch (node.info.path("access_type").asText()) {
            case "ALL":
                paths.add("TABLE_SCAN");
                break;
            case "range":
                paths.add("INDEX_RANGE");
                break;
            case "ref":
            case "eq_ref":
            case "const":
                paths.add("INDEX_LOOKUP");
                break;
            case "index":
                paths.add("INDEX_SCAN");
                break;
            default:
                paths.add("UNKNOWN");
                break;
            }
            boolean materialized = false;
            for (Node ancestor = node.parent; ancestor != null; ancestor = ancestor.parent) {
                materialized |= ancestor.kind.equals("MATERIALIZED");
            }
            boundaries.add(materialized ? "MATERIALIZED" : "MERGED");
        }
        if (!seen.containsAll(names)) {
            paths.add("UNKNOWN");
            boundaries.add("UNKNOWN");
        }
        String key = EcstExpressionMatcher.fingerprint(operation.getNormalizedExpression());
        int matches = 0;
        Set<String> roles = new LinkedHashSet<>();
        Set<String> locations = new LinkedHashSet<>();
        List<String> evidence = new ArrayList<>();
        if (key != null && !operation.getExpectedRole().equals("PROJECTION_ITEM")) {
            for (Node node : nodes) {
                if (!relevant(node, names)) {
                    continue;
                }
                for (String field : List.of("attached_condition", "index_condition", "having_condition")) {
                    String expression = node.info.path(field).asText();
                    if (expression.isEmpty()) {
                        continue;
                    }
                    try {
                        int count = EcstExpressionMatcher.countMatches(CCJSqlParserUtil.parseExpression(expression),
                                key);
                        if (count > 0) {
                            matches += count;
                            boolean join = node.kind.equals("JOIN_BUFFER");
                            roles.add(join ? "JOIN_CONDITION" : node.kind.equals("SCAN") ? "SCAN_FILTER" : "FILTER");
                            locations.add(
                                    join || hasJoin(node) || multipleInputs(expression) ? "POST_JOIN" : "PRE_JOIN");
                            evidence.add(node.kind + ":" + field + "=" + expression);
                        }
                    } catch (Exception unsupported) {
                        // Native conditions without a unique structural mapping remain unknown.
                    }
                }
            }
        }
        boolean matched = matches == 1 && roles.size() == 1 && locations.size() == 1;
        return new ParsedContext(
                new EcstContext(matched ? only(locations) : "UNKNOWN", matched ? only(roles) : "UNKNOWN",
                        only(boundaries), only(paths), "UNKNOWN"),
                plan, "matches=" + matches + "; relations=" + names + "; " + String.join(";", evidence), matched);
    }

    private static Node build(JsonNode data) {
        if (data.has("query_block")) {
            return build(data.path("query_block"));
        }
        if (data.has("table")) {
            Node scan = new Node("SCAN", data.path("table").deepCopy());
            if (scan.info.has("materialized")) {
                Node producer = new Node("MATERIALIZED", new ObjectMapper().createObjectNode());
                producer.add(build(scan.info.path("materialized")));
                scan.add(producer);
            }
            return scan;
        }
        if (data.has("block-nl-join")) {
            JsonNode buffer = data.path("block-nl-join");
            Node join = new Node("JOIN_BUFFER", buffer);
            join.add(build(buffer));
            return join;
        }
        Node input = null;
        if (data.has("nested_loop")) {
            for (JsonNode entry : data.path("nested_loop")) {
                Node next = build(entry);
                if (input == null) {
                    input = next;
                } else if (next.kind.equals("JOIN_BUFFER")) {
                    next.add(input);
                    input = next;
                } else {
                    Node join = new Node("JOIN", new ObjectMapper().createObjectNode());
                    join.add(input);
                    join.add(next);
                    String attached = next.info.path("attached_condition").asText();
                    if (next.kind.equals("SCAN") && multipleInputs(attached)) {
                        com.fasterxml.jackson.databind.node.ObjectNode filterInfo = new ObjectMapper()
                                .createObjectNode();
                        filterInfo.put("attached_condition", attached);
                        ((com.fasterxml.jackson.databind.node.ObjectNode) next.info).remove("attached_condition");
                        Node filter = new Node("FILTER", filterInfo);
                        filter.add(join);
                        input = filter;
                    } else {
                        input = join;
                    }
                }
            }
        } else {
            for (String wrapper : List.of("filesort", "temporary_table", "read_sorted_file",
                    "window_functions_computation")) {
                if (data.has(wrapper)) {
                    input = build(data.path(wrapper));
                    break;
                }
            }
        }
        Node block = new Node("BLOCK", data);
        block.add(input);
        return block;
    }

    private static void flatten(Node node, List<Node> nodes) {
        nodes.add(node);
        node.children.forEach(child -> flatten(child, nodes));
    }

    private static String name(String relation) {
        String text = relation.replace("`", "").replace("\"", "").toLowerCase(Locale.ROOT);
        return text.substring(text.lastIndexOf('.') + 1);
    }

    private static boolean source(Node node, Set<String> names) {
        return node.kind.equals("SCAN") && names.contains(name(node.info.path("table_name").asText()));
    }

    private static boolean relevant(Node node, Set<String> names) {
        Set<String> found = new LinkedHashSet<>();
        collect(node, names, found);
        return !names.isEmpty() && found.containsAll(names);
    }

    private static void collect(Node node, Set<String> names, Set<String> found) {
        if (source(node, names)) {
            found.add(name(node.info.path("table_name").asText()));
        }
        node.children.forEach(child -> collect(child, names, found));
    }

    private static boolean hasJoin(Node node) {
        return node.kind.startsWith("JOIN")
                || node.children.stream().anyMatch(MariaDBExecutionContextAnalyzer::hasJoin);
    }

    private static boolean multipleInputs(String expression) {
        // Count qualified column references from the native expression tree, preserving quoted literals.
        try {
            Set<String> qualifiers = new LinkedHashSet<>();
            CCJSqlParserUtil.parseExpression(expression)
                    .accept(new net.sf.jsqlparser.expression.ExpressionVisitorAdapter() {
                        @Override
                        public void visit(net.sf.jsqlparser.schema.Column column) {
                            if (column.getTable() != null && column.getTable().getName() != null) {
                                qualifiers.add(name(column.getTable().getName()));
                            }
                        }
                    });
            return qualifiers.size() > 1;
        } catch (Exception unavailable) {
            return false;
        }
    }

    private static String only(Set<String> values) {
        return values.size() == 1 ? values.iterator().next() : "UNKNOWN";
    }

    private static ParsedContext unknown(String plan, String evidence) {
        return new ParsedContext(new EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"), plan, evidence,
                false);
    }
}
