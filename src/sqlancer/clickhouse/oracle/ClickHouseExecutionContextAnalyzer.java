package sqlancer.clickhouse.oracle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.oracle.ClickHouseVerticalPartitioningOracle.EcstCase;
import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstExpressionMatcher;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstOperation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstTarget;

/** Native action DAG expressions and physical MergeTree/CTE steps; SQL text is never execution evidence. */
public final class ClickHouseExecutionContextAnalyzer {
    private final ClickHouseGlobalState state;

    public ClickHouseExecutionContextAnalyzer(ClickHouseGlobalState state) {
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
        private final JsonNode data;
        private final Node parent;
        private final List<Node> children = new ArrayList<>();

        private Node(JsonNode data, Node parent) {
            this.data = data;
            this.parent = parent;
        }

        private String type() {
            return data.path("Node Type").asText();
        }
    }

    public ParsedContext extractContext(String sql, EcstOperation operation, Set<String> relations)
            throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement statement = state.getConnection().createStatement(); ResultSet rows = statement
                .executeQuery("EXPLAIN PLAN json=1,actions=1,indexes=1 " + sql + ClickHouseEcstSession.SETTINGS)) {
            while (rows.next()) {
                plan.append(rows.getString(1));
            }
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
                "ClickHouse native physical-plan evidence", before.plan, after.plan,
                before.matched ? before.evidence : "", after.matched ? after.evidence : "", before.evidence,
                after.evidence,
                status == EcstShiftStatus.UNKNOWN ? EcstObservation.UnknownReason.UNKNOWN_BECAUSE_OPERATION_MAPPING
                        : EcstObservation.UnknownReason.NONE);
    }

    static ParsedContext parse(String plan, EcstOperation operation, Set<String> relations) {
        List<Node> nodes = new ArrayList<>();
        try {
            JsonNode json = new ObjectMapper().readTree(plan);
            if (!json.isArray()) {
                return unknown(plan, "unrecognized native JSON envelope");
            }
            for (JsonNode entry : json) {
                build(entry.path("Plan"), null, nodes);
            }
        } catch (Exception unsupported) {
            return unknown(plan, "invalid native plan JSON");
        }
        Set<String> boundaries = new LinkedHashSet<>();
        Set<String> paths = new LinkedHashSet<>();
        for (Node node : nodes) {
            if (!source(node, relations)) {
                continue;
            }
            boolean materialized = false;
            for (Node p = node.parent; p != null; p = p.parent) {
                materialized |= p.type().equals("MaterializingCTE");
            }
            boundaries.add(materialized ? "MATERIALIZED" : "MERGED");
            if (node.type().equals("ReadFromMergeTree")) {
                String path = "TABLE_SCAN";
                for (JsonNode index : node.data.path("Indexes")) {
                    if (index.path("Type").asText().equals("PrimaryKey") && index.has("Initial Granules")
                            && index.has("Selected Granules")
                            && index.path("Initial Granules").asLong() > index.path("Selected Granules").asLong()
                            && !index.path("Condition").asText().equals("true")) {
                        path = "PRIMARY_PRUNED";
                    }
                }
                paths.add(path);
            }
        }
        String fingerprint = EcstExpressionMatcher.fingerprint(normalize(operation.getNormalizedExpression()));
        Set<String> locations = new LinkedHashSet<>();
        Set<String> operators = new LinkedHashSet<>();
        List<String> evidence = new ArrayList<>();
        int matches = 0;
        if (fingerprint != null) {
            for (Node node : nodes) {
                if (!relevant(node, relations)) {
                    continue;
                }
                if (!operation.getExpectedRole().equals("PROJECTION_ITEM") && node.type().equals("Filter")) {
                    int count = count(node.data.path("Filter Column").asText(), fingerprint);
                    if (count > 0) {
                        matches += count;
                        evidence.add("Filter Column=" + node.data.path("Filter Column").asText());
                        locations.add(hasJoin(node) ? "POST_JOIN" : "PRE_JOIN");
                        operators.add("FILTER");
                    }
                } else if (operation.getExpectedRole().equals("PROJECTION_ITEM") && node.type().equals("Expression")) {
                    for (JsonNode action : node.data.path("Expression").path("Actions")) {
                        if (action.path("Node Type").asText().equals("FUNCTION") && fingerprint.equals(
                                EcstExpressionMatcher.fingerprint(normalize(action.path("Result Name").asText())))) {
                            matches++;
                            evidence.add("FUNCTION=" + action.path("Result Name").asText());
                            locations.add(hasJoin(node) ? "POST_JOIN" : "PRE_JOIN");
                            operators.add("PROJECTION");
                        }
                    }
                }
            }
        }
        boolean reliable = matches == 1 && locations.size() == 1 && operators.size() == 1;
        // Anonymous ReadFromMemoryStorage consumers have no stable CTE/source identity in this JSON format.
        // Their operation position remains UNKNOWN; the producer still proves physical materialization.
        EcstContext context = new EcstContext(reliable ? only(locations) : "UNKNOWN",
                reliable ? only(operators) : "UNKNOWN", only(boundaries), only(paths), "UNKNOWN");
        return new ParsedContext(context, plan,
                "matches=" + matches + "; sources=" + relations + "; " + String.join(";", evidence), reliable);
    }

    private static ParsedContext unknown(String plan, String reason) {
        return new ParsedContext(new EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"), plan, reason,
                false);
    }

    private static void build(JsonNode data, Node parent, List<Node> nodes) {
        if (!data.isObject() || !data.has("Node Type")) {
            return;
        }
        Node node = new Node(data, parent);
        nodes.add(node);
        if (parent != null) {
            parent.children.add(node);
        }
        for (JsonNode child : data.path("Plans")) {
            build(child, node, nodes);
        }
    }

    private static boolean source(Node node, Set<String> relations) {
        if (!node.type().equals("ReadFromMergeTree")) {
            return false;
        }
        String table = node.data.path("Description").asText().replace("`", "").replace("\"", "");
        int dot = table.lastIndexOf('.');
        table = table.substring(dot + 1);
        return relations.contains(table);
    }

    private static boolean relevant(Node node, Set<String> relations) {
        return source(node, relations) || node.children.stream().anyMatch(c -> relevant(c, relations));
    }

    private static boolean hasJoin(Node node) {
        return node.type().contains("Join")
                || node.children.stream().anyMatch(ClickHouseExecutionContextAnalyzer::hasJoin);
    }

    private static String only(Set<String> values) {
        return values.size() == 1 ? values.iterator().next() : "UNKNOWN";
    }

    private static int count(String expression, String fingerprint) {
        try {
            return EcstExpressionMatcher.countMatches(CCJSqlParserUtil.parseExpression(normalize(expression)),
                    fingerprint);
        } catch (Exception unsupported) {
            return 0;
        }
    }

    static String normalize(String expression) {
        StringBuilder result = new StringBuilder();
        StringBuilder code = new StringBuilder();
        for (int i = 0; i < expression.length();) {
            char c = expression.charAt(i);
            if (c == '\'') {
                result.append(normalizeCode(code.toString()));
                code.setLength(0);
                int start = i++;
                while (i < expression.length()) {
                    char ch = expression.charAt(i++);
                    if (ch == '\\' && i < expression.length()) {
                        i++;
                    } else if (ch == '\'') {
                        if (i < expression.length() && expression.charAt(i) == '\'') {
                            i++;
                        } else {
                            break;
                        }
                    }
                }
                result.append(expression, start, i);
            } else {
                code.append(c);
                i++;
            }
        }
        return result.append(normalizeCode(code.toString())).toString();
    }

    private static String normalizeCode(String code) {
        return code.replaceAll("(?<![A-Za-z0-9_])(-?[0-9]+(?:\\.[0-9]+)?)_(?:U?Int[0-9]+|Float[0-9]+)", "$1");
    }
}
