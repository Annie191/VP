package sqlancer.cockroachdb.oracle;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import sqlancer.cockroachdb.CockroachDBOptions;

/** Local execution settings and typed session replay for single-node ECST. */
final class CockroachDBEcstSession {
    static final List<String> SESSION_VARIABLES = List.of("timezone", "distsql", "vectorize",
            "optimizer_use_histograms", "enable_zigzag_join", "opt_split_scan_limit", "datestyle", "intervalstyle",
            "extra_float_digits", "bytea_output", "default_int_size", "enable_implicit_select_for_update",
            "enable_insert_fast_path", "serial_normalization", "reorder_joins_limit", "sql_safe_updates");

    private CockroachDBEcstSession() {
    }

    static void validateOptions(CockroachDBOptions options) {
        if (options.verticalPartitioningDistributedProfile || options.verticalPartitioningRowRangePressureProfile
                || options.verticalPartitioningDistributedGateways != null
                        && !options.verticalPartitioningDistributedGateways.isBlank()) {
            throw new IllegalArgumentException("CockroachDB ECST uses a single node via --host/--port; "
                    + "VP distributed gateways and distributed/row-range profiles are unsupported");
        }
    }

    static void prepare(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET TIME ZONE 'UTC'");
            statement.execute("SET distsql = 'off'");
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
}
