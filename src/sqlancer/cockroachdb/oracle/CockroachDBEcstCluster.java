package sqlancer.cockroachdb.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sqlancer.cockroachdb.CockroachDBProvider;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.common.oracle.ecst.EcstFailureRecord;
import sqlancer.common.oracle.ecst.EcstTransformationConfig.Representation;

/** Three-node membership, same-cluster gateway rotation, and separately attributed SQL/KV runtime evidence. */
public final class CockroachDBEcstCluster {
    static final List<String> SESSION_VARIABLES = List.of("timezone", "distsql", "vectorize",
            "optimizer_use_histograms", "enable_zigzag_join", "opt_split_scan_limit", "datestyle", "intervalstyle",
            "extra_float_digits", "bytea_output", "default_int_size", "enable_implicit_select_for_update",
            "enable_insert_fast_path", "serial_normalization", "reorder_joins_limit", "sql_safe_updates");
    private final CockroachDBGlobalState state;
    private final List<String> gateways = new ArrayList<>();
    private String clusterId;
    private final Set<String> liveNodes = new LinkedHashSet<>();
    private String gatewayNode;

    public CockroachDBEcstCluster(CockroachDBGlobalState state) {
        this.state = state;
        String configured = state.getDbmsSpecificOptions().verticalPartitioningDistributedGateways;
        if (configured != null) {
            for (String endpoint : configured.split(",")) {
                if (!endpoint.trim().isEmpty()) {
                    gateways.add(endpoint.trim());
                }
            }
        }
    }

    public void validate() throws SQLException {
        Connection connection = state.getConnection().getJdbcConnection();
        String actual = id(connection);
        if (clusterId != null && !clusterId.equals(actual)) {
            throw new SQLException("ECST gateway cluster changed",
                    CockroachDBProvider.VP_GATEWAY_CLUSTER_MISMATCH_SQL_STATE);
        }
        clusterId = actual;
        for (String endpoint : gateways) {
            String[] address = address(endpoint);
            try (Connection candidate = DriverManager.getConnection(
                    "jdbc:postgresql://" + address[0] + ":" + address[1] + "/defaultdb",
                    state.getOptions().getUserName(), state.getOptions().getPassword())) {
                if (!clusterId.equals(id(candidate))) {
                    throw new SQLException("ECST gateway " + endpoint + " belongs to a different cluster",
                            CockroachDBProvider.VP_GATEWAY_CLUSTER_MISMATCH_SQL_STATE);
                }
            }
        }
        liveNodes.clear();
        try (Statement statement = connection.createStatement(); ResultSet rows = statement
                .executeQuery("SELECT node_id FROM crdb_internal.gossip_nodes WHERE is_live")) {
            while (rows.next()) {
                liveNodes.add(rows.getString(1));
            }
        }
        int minimum = Math.max(3, state.getDbmsSpecificOptions().verticalPartitioningDistributedMinLiveNodes);
        if (liveNodes.size() < minimum) {
            throw new SQLException(
                    "CockroachDB ECST requires at least " + minimum + " live nodes; observed " + liveNodes.size(),
                    "EC003");
        }
        gatewayNode = scalar(connection, "SELECT crdb_internal.node_id()");
    }

    public void prepareSeed(long ordinal) throws SQLException {
        if (!gateways.isEmpty()) {
            Map<String, String> settings = new LinkedHashMap<>();
            for (String variable : SESSION_VARIABLES) {
                settings.put(variable, scalar(state.getConnection().getJdbcConnection(), "SHOW " + variable));
            }
            String[] endpoint = address(gateways.get((int) (ordinal % gateways.size())));
            CockroachDBProvider.switchVerticalPartitioningGateway(state, endpoint[0], Integer.parseInt(endpoint[1]));
            try (Statement statement = state.getConnection().createStatement()) {
                for (Map.Entry<String, String> setting : settings.entrySet()) {
                    statement.execute(sessionSetStatement(setting.getKey(), setting.getValue()));
                }
            }
        }
        validateCurrent();
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute("SET TIME ZONE 'UTC'");
            statement.execute("SET distsql = 'always'");
            statement.execute("SET bytea_output = 'hex'");
            statement.execute("SET tracing = off");
        }
    }

    static String sessionSetStatement(String variable, String value) throws SQLException {
        if (!SESSION_VARIABLES.contains(variable)) {
            throw new SQLException("unsupported ECST session variable: " + variable);
        }
        boolean integer = Set
                .of("opt_split_scan_limit", "extra_float_digits", "default_int_size", "reorder_joins_limit")
                .contains(variable);
        if (integer && !value.matches("-?[0-9]+")) {
            throw new SQLException("invalid integer ECST session value for " + variable);
        }
        return "SET " + variable + " = " + (integer ? value : CockroachDBVerticalPartitioningOracle.ecstLiteral(value));
    }

    private void validateCurrent() throws SQLException {
        Connection connection = state.getConnection().getJdbcConnection();
        if (!id(connection).equals(clusterId)) {
            throw new SQLException("ECST gateway is not in the validated cluster",
                    CockroachDBProvider.VP_GATEWAY_CLUSTER_MISMATCH_SQL_STATE);
        }
        gatewayNode = scalar(connection, "SELECT crdb_internal.node_id()");
        liveNodes.clear();
        try (Statement statement = connection.createStatement(); ResultSet rows = statement
                .executeQuery("SELECT node_id FROM crdb_internal.gossip_nodes WHERE is_live")) {
            while (rows.next()) {
                liveNodes.add(rows.getString(1));
            }
        }
        int count = liveNodes.size();
        if (count < Math.max(3, state.getDbmsSpecificOptions().verticalPartitioningDistributedMinLiveNodes)) {
            throw new SQLException("ECST cluster lost required live nodes", "EC003");
        }
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getGatewayNode() {
        return gatewayNode;
    }

    public Set<String> getLiveNodes() {
        return Set.copyOf(liveNodes);
    }

    private static String id(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET allow_unsafe_internals = true");
        }
        return scalar(connection, "SELECT crdb_internal.cluster_id()");
    }

    private static String scalar(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(query)) {
            if (!rows.next()) {
                throw new SQLException("ECST cluster metadata returned no row");
            }
            return rows.getString(1);
        }
    }

    private static String[] address(String endpoint) throws SQLException {
        int colon = endpoint.lastIndexOf(':');
        if (colon < 1) {
            throw new SQLException("invalid ECST gateway endpoint: " + endpoint);
        }
        try {
            int port = Integer.parseInt(endpoint.substring(colon + 1));
            if (port < 1 || port > 65535) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException error) {
            throw new SQLException("invalid ECST gateway port: " + endpoint, error);
        }
        return new String[] { endpoint.substring(0, colon), endpoint.substring(colon + 1) };
    }

    public static final class ExecutionEvidence {
        private final String plan;
        private final Set<String> sqlNodes;
        private final Set<String> kvNodes;
        private final String distribution;
        private final String error;

        private ExecutionEvidence(String plan, Set<String> sqlNodes, Set<String> kvNodes, String distribution,
                String error) {
            this.plan = plan;
            this.sqlNodes = Set.copyOf(sqlNodes);
            this.kvNodes = Set.copyOf(kvNodes);
            this.distribution = distribution;
            this.error = error;
        }

        public Set<String> getSqlNodes() {
            return sqlNodes;
        }

        public Set<String> getKvNodes() {
            return kvNodes;
        }

        public boolean isAvailable() {
            return error.isEmpty();
        }

        public String getPlan() {
            return plan;
        }

        public String toJson() {
            return "{\"available\":" + isAvailable() + ",\"distribution\":" + json(distribution) + ",\"sqlNodes\":"
                    + EcstFailureRecord.jsonArray(new ArrayList<>(sqlNodes)) + ",\"kvNodes\":"
                    + EcstFailureRecord.jsonArray(new ArrayList<>(kvNodes)) + ",\"error\":" + json(error) + ",\"plan\":"
                    + json(plan) + "}";
        }
    }

    static ExecutionEvidence parseExecution(String plan) {
        Set<String> sqlNodes = new LinkedHashSet<>();
        Set<String> kvNodes = new LinkedHashSet<>();
        String distribution = "UNKNOWN";
        for (String line : plan.split("\\R")) {
            String field = line.replaceFirst("^[\\s│├└─]+", "");
            if (field.startsWith("sql nodes:")) {
                addNodes(sqlNodes, field.substring("sql nodes:".length()));
            } else if (field.startsWith("kv nodes:")) {
                addNodes(kvNodes, field.substring("kv nodes:".length()));
            } else if (field.startsWith("distribution:")) {
                distribution = field.substring("distribution:".length()).trim();
            }
        }
        return new ExecutionEvidence(plan, sqlNodes, kvNodes, distribution, "");
    }

    private static void addNodes(Set<String> nodes, String text) {
        Matcher matcher = Pattern.compile("(?<![A-Za-z0-9_])n([0-9]+)(?![A-Za-z0-9_])").matcher(text);
        while (matcher.find()) {
            nodes.add(matcher.group(1));
        }
    }

    public ExecutionEvidence captureExecution(String query) {
        StringBuilder plan = new StringBuilder();
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rows = statement.executeQuery("EXPLAIN ANALYZE (VERBOSE) " + query)) {
            while (rows.next()) {
                plan.append(rows.getString(1)).append('\n');
            }
            return parseExecution(plan.toString());
        } catch (SQLException error) {
            return new ExecutionEvidence(plan.toString(), Set.of(), Set.of(), "UNKNOWN",
                    error.getSQLState() + ":" + error.getMessage());
        }
    }

    public String capturePair(CockroachDBVerticalPartitioningOracle.EcstCase testCase) {
        List<String> placements = new ArrayList<>();
        List<String> relations = new ArrayList<>();
        relations.add(testCase.seed.getSourceTable());
        if (testCase.getAppliedConfig().getRepresentation() == Representation.VERTICAL_PARTITIONED) {
            relations.add(testCase.getLeftTable());
            relations.add(testCase.getRightTable());
        }
        for (String relation : relations) {
            placements.add(placement(relation));
        }
        return "{\"clusterId\":" + json(clusterId) + ",\"gatewayNode\":" + json(gatewayNode) + ",\"liveNodes\":"
                + EcstFailureRecord.jsonArray(new ArrayList<>(liveNodes)) + ",\"ranges\":["
                + String.join(",", placements) + "],\"originalExecution\":"
                + captureExecution(testCase.getOriginalQuery()).toJson() + ",\"transformedExecution\":"
                + captureExecution(testCase.getTransformedQuery()).toJson() + "}";
    }

    public String placement(String table) {
        Set<String> ranges = new LinkedHashSet<>();
        Set<String> replicas = new LinkedHashSet<>();
        Set<String> leaseholders = new LinkedHashSet<>();
        try (Statement statement = state.getConnection().createStatement(); ResultSet rows = statement
                .executeQuery("SHOW RANGES FROM TABLE \"" + table.replace("\"", "\"\"") + "\" WITH DETAILS")) {
            while (rows.next()) {
                ranges.add(rows.getString("range_id"));
                addIntegers(replicas, rows.getString("replicas"));
                String holder = rows.getString("lease_holder");
                if (holder != null && !holder.equals("0")) {
                    leaseholders.add(holder);
                }
            }
            return "{\"table\":" + json(table) + ",\"rangeCount\":" + ranges.size() + ",\"replicas\":"
                    + EcstFailureRecord.jsonArray(new ArrayList<>(replicas)) + ",\"leaseholders\":"
                    + EcstFailureRecord.jsonArray(new ArrayList<>(leaseholders)) + "}";
        } catch (SQLException error) {
            return "{\"table\":" + json(table) + ",\"error\":" + json(error.getSQLState() + ":" + error.getMessage())
                    + "}";
        }
    }

    private static void addIntegers(Set<String> target, String text) {
        if (text != null) {
            Matcher matcher = Pattern.compile("[0-9]+").matcher(text);
            while (matcher.find()) {
                target.add(matcher.group());
            }
        }
    }

    private static String json(String value) {
        return "\"" + EcstFailureRecord.json(value) + "\"";
    }
}
