package sqlancer.postgres.oracle;

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
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.oracle.PostgresVerticalPartitioningOracle.EcstCase;

/** PostgreSQL native JSON scans, join predicates and materialized CTE producer lineage. */
public final class PostgresExecutionContextAnalyzer {
    private final PostgresGlobalState state;

    public PostgresExecutionContextAnalyzer(PostgresGlobalState state) {
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
                ResultSet rows = statement.executeQuery("EXPLAIN (VERBOSE, FORMAT JSON) " + sql)) {
            if (!rows.next()) {
                throw new SQLException("Postgres EXPLAIN returned no plan");
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
                "Postgres native JSON plan evidence; no causal confirmation", before.plan, after.plan,
                before.matched ? before.evidence : "", after.matched ? after.evidence : "", before.evidence,
                after.evidence,
                status == EcstShiftStatus.UNKNOWN ? EcstObservation.UnknownReason.UNKNOWN_BECAUSE_OPERATION_MAPPING
                        : EcstObservation.UnknownReason.NONE);
    }

    static ParsedContext parse(String plan, EcstOperation operation, Set<String> relations) {
        List<Node> nodes = new ArrayList<>();
        try {
            JsonNode json = new ObjectMapper().readTree(plan);
            if (json == null || !json.isArray() || json.size() != 1 || !json.get(0).has("Plan")) {
                return unknown(plan, "missing PostgreSQL Plan");
            }
            flatten(build(json.get(0).get("Plan")), nodes);
        } catch (Exception invalid) {
            return unknown(plan, "invalid native JSON");
        }
        Set<String> names = relations.stream().map(PostgresExecutionContextAnalyzer::name).collect(Collectors.toSet());
        Set<String> aliases = names.stream().filter(n -> n.equals("ecst_l") || n.equals("ecst_r"))
                .collect(Collectors.toSet());
        if (!aliases.isEmpty()) {
            names = aliases;
        }
        Set<String> seen = new LinkedHashSet<>();
        Set<String> paths = new LinkedHashSet<>();
        Set<String> boundaries = new LinkedHashSet<>();
        for (Node node : nodes) {
            if (!source(node, names)) {
                continue;
            }
            seen.add(identity(node, names));
            switch (node.kind) {
            case "Seq Scan":
                paths.add("TABLE_SCAN");
                break;
            case "Index Scan":
                paths.add("INDEX_SCAN");
                break;
            case "Index Only Scan":
                paths.add("INDEX_ONLY_SCAN");
                break;
            case "Bitmap Heap Scan":
                paths.add("BITMAP_SCAN");
                break;
            default:
                paths.add("UNKNOWN");
                break;
            }
            String boundary = "MERGED";
            for (Node ancestor = node; ancestor != null; ancestor = ancestor.parent) {
                if (ancestor.info.path("Subplan Name").asText().startsWith("CTE ")) {
                    boundary = "MATERIALIZED";
                    break;
                }
                if (ancestor.kind.equals("Subquery Scan")) {
                    boundary = "UNKNOWN";
                }
            }
            boundaries.add(boundary);
        }
        if (names.isEmpty() || !seen.containsAll(names)) {
            paths.add("UNKNOWN");
            boundaries.add("UNKNOWN");
        }
        String key = EcstExpressionMatcher.fingerprint(normalize(operation.getNormalizedExpression(), operation));
        int matches = 0;
        Set<String> roles = new LinkedHashSet<>();
        Set<String> locations = new LinkedHashSet<>();
        List<String> evidence = new ArrayList<>();
        if (key != null && !operation.getExpectedRole().equals("PROJECTION_ITEM")) {
            for (Node node : nodes) {
                if (!relevant(node, names)) {
                    continue;
                }
                for (String field : List.of("Filter", "Index Cond", "Recheck Cond", "Hash Cond", "Merge Cond",
                        "Join Filter")) {
                    String expression = node.info.path(field).asText();
                    if (expression.isEmpty()) {
                        continue;
                    }
                    // A bitmap Recheck Cond repeats the underlying index operation. The index producer is
                    // counted once at its heap consumer, including lossy-page rechecks.
                    if (node.kind.equals("Bitmap Index Scan")) {
                        continue;
                    }
                    try {
                        int count = EcstExpressionMatcher
                                .countMatches(CCJSqlParserUtil.parseExpression(normalize(expression, operation)), key);
                        if (count > 0) {
                            matches += count;
                            boolean join = field.equals("Hash Cond") || field.equals("Merge Cond")
                                    || field.equals("Join Filter");
                            roles.add(join ? "JOIN_CONDITION" : node.kind.endsWith("Scan") ? "SCAN_FILTER" : "FILTER");
                            locations.add(hasJoin(node) ? "POST_JOIN" : "PRE_JOIN");
                            evidence.add(node.kind + ":" + field + "=" + expression);
                        }
                    } catch (Exception unsupported) {
                        // Native expressions that cannot be mapped structurally remain unknown.
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

    private static String normalize(String expression, EcstOperation operation) {
        String normalized = expression;
        for (String dependency : operation.getDependencies()) {
            normalized = normalized.replace("\"" + dependency.replace("\"", "\"\"") + "\"", dependency);
        }
        // PostgreSQL decorates numeric constants with their resolved numeric input type. Preserve
        // all column/expression casts; only numeric literals lose this planner-added decoration.
        return normalized.replaceAll("'(-?[0-9]+)'::(?:bigint|integer|smallint|numeric|real|double precision)", "$1");
    }

    private static Node build(JsonNode info) {
        Node result = new Node(info.path("Node Type").asText(), info);
        for (JsonNode child : info.path("Plans")) {
            result.add(build(child));
        }
        return result;
    }

    private static void flatten(Node node, List<Node> nodes) {
        nodes.add(node);
        node.children.forEach(child -> flatten(child, nodes));
    }

    private static String name(String value) {
        String text = value.replace("\"", "").toLowerCase(Locale.ROOT);
        return text.substring(text.lastIndexOf('.') + 1);
    }

    private static String identity(Node node, Set<String> names) {
        String alias = name(node.info.path("Alias").asText());
        return names.contains(alias) ? alias : name(node.info.path("Relation Name").asText());
    }

    private static boolean source(Node node, Set<String> names) {
        return node.info.has("Relation Name") && names.contains(identity(node, names));
    }

    private static boolean relevant(Node node, Set<String> names) {
        Set<String> found = new LinkedHashSet<>();
        collect(node, names, found);
        return !names.isEmpty() && found.containsAll(names);
    }

    private static void collect(Node node, Set<String> names, Set<String> found) {
        if (source(node, names)) {
            found.add(identity(node, names));
        }
        for (Node child : node.children) {
            if (!child.info.path("Parent Relationship").asText().equals("SubPlan")) {
                collect(child, names, found);
            }
        }
    }

    private static boolean hasJoin(Node node) {
        if (node.kind.endsWith("Join") || node.kind.equals("Nested Loop")) {
            return true;
        }
        return node.children.stream()
                .filter(child -> !child.info.path("Parent Relationship").asText().equals("SubPlan"))
                .anyMatch(PostgresExecutionContextAnalyzer::hasJoin);
    }

    private static String only(Set<String> values) {
        return values.size() == 1 ? values.iterator().next() : "UNKNOWN";
    }

    private static ParsedContext unknown(String plan, String reason) {
        return new ParsedContext(new EcstContext("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"), plan, reason,
                false);
    }
}
