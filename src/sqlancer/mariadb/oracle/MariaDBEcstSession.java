package sqlancer.mariadb.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Identical connection state for both sides, without changing optimizer switches. */
public final class MariaDBEcstSession {
    private MariaDBEcstSession() {
    }

    public static void prepare(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET SESSION time_zone='+00:00'");
        }
    }

    static List<String> capture(Connection connection) throws SQLException {
        List<String> result = new ArrayList<>();
        for (String variable : List.of("sql_mode", "time_zone", "character_set_client", "character_set_connection",
                "character_set_results", "collation_connection", "optimizer_switch",
                "optimizer_use_condition_selectivity", "join_cache_level", "max_statement_time")) {
            try (Statement statement = connection.createStatement();
                    ResultSet rows = statement.executeQuery("SELECT @@SESSION." + variable)) {
                if (!rows.next()) {
                    throw new SQLException("session variable returned no row: " + variable);
                }
                String value = rows.getString(1);
                boolean numeric = List
                        .of("optimizer_use_condition_selectivity", "join_cache_level", "max_statement_time")
                        .contains(variable);
                result.add("SET SESSION " + variable + "="
                        + (value == null ? "NULL" : numeric ? value : "'" + value.replace("'", "''") + "'"));
            }
        }
        return result;
    }
}
