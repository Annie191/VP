package sqlancer.duckdb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** One shared execution profile; optimizer switches are preserved, never changed only on the transformed side. */
final class DuckDBEcstSession {
    private static final List<String> VARIABLES = List.of("threads", "TimeZone", "disabled_optimizers",
            "default_collation", "index_scan_percentage", "index_scan_max_count");

    private DuckDBEcstSession() {
    }

    static void prepare(Connection connection) throws SQLException {
        try (Statement s = connection.createStatement()) {
            s.execute("SET threads=1");
            s.execute("SET TimeZone='UTC'");
        }
    }

    static List<String> capture(Connection connection) throws SQLException {
        List<String> result = new ArrayList<>();
        for (String name : VARIABLES) {
            try (Statement s = connection.createStatement();
                    ResultSet r = s.executeQuery("SELECT current_setting('" + name + "')")) {
                if (!r.next()) {
                    throw new SQLException("missing session setting: " + name);
                }
                result.add("SET " + name + "='" + r.getString(1).replace("'", "''") + "'");
            }
        }
        return result;
    }
}
