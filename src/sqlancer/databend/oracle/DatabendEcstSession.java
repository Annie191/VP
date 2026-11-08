package sqlancer.databend.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** One connection profile for both sides; optimizer and pruning switches are preserved. */
public final class DatabendEcstSession {
    private DatabendEcstSession() {
    }

    public static void prepare(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET max_threads=1");
            statement.execute("SET timezone='UTC'");
        }
    }

    static List<String> capture(Connection connection) throws SQLException {
        List<String> replay = new ArrayList<>();
        String query = "SELECT name,value FROM system.settings WHERE name IN ('timezone','sql_dialect',"
                + "'default_order_by_null','numeric_cast_option','enable_cbo','disable_join_reorder',"
                + "'enable_bloom_runtime_filter','enable_join_runtime_filter','enable_query_result_cache','max_threads',"
                + "'enable_materialized_cte','enable_auto_materialize_cte','persist_materialized_cte',"
                + "'enable_distributed_pruning','enable_prune_cache','enable_prune_pipeline') ORDER BY name";
        try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(query)) {
            while (rows.next()) {
                replay.add("SET " + rows.getString(1) + "='"
                        + rows.getString(2).replace("\\", "\\\\").replace("'", "''") + "'");
            }
        }
        return replay;
    }
}
