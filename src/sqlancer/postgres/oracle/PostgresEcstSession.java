package sqlancer.postgres.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** One execution and serialization profile shared by both queries; preserves incoming planner settings. */
final class PostgresEcstSession {
    private PostgresEcstSession() {
    }

    static void prepare(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("SET TIME ZONE 'UTC'");
            statement.execute("SET standard_conforming_strings=on");
            statement.execute("SET DateStyle='ISO, YMD'");
            statement.execute("SET IntervalStyle='postgres'");
            statement.execute("SET bytea_output='hex'");
            statement.execute("SET extra_float_digits=3");
        }
    }

    static List<String> capture(Connection connection) throws SQLException {
        List<String> result = new ArrayList<>();
        String sql = "SELECT name,current_setting(name) FROM pg_settings WHERE category LIKE 'Query Tuning%' "
                + "OR name IN ('TimeZone','DateStyle','IntervalStyle','bytea_output','extra_float_digits',"
                + "'standard_conforming_strings','search_path','statement_timeout','lock_timeout','row_security',"
                + "'default_transaction_isolation','max_parallel_workers_per_gather','work_mem','jit') ORDER BY name";
        try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(sql)) {
            while (rows.next()) {
                result.add("SELECT pg_catalog.set_config('" + rows.getString(1) + "','"
                        + rows.getString(2).replace("'", "''") + "',false)");
            }
        }
        return result;
    }
}
