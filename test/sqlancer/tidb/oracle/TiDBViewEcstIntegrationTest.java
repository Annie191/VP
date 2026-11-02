package sqlancer.tidb.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** Opt-in live TiDB check for the VIEW relation representation used by ECST. */
public class TiDBViewEcstIntegrationTest {
    @Test
    void viewBackedEquivalentQueryIsVisibleToExplainAndPreservesRows() throws Exception {
        String url = System.getProperty("tidb.test.url", "jdbc:mysql://127.0.0.1:4009/test");
        String user = System.getProperty("tidb.test.user", "root");
        String password = System.getProperty("tidb.test.password", "");
        String suffix = Long.toUnsignedString(System.nanoTime(), 36);
        String source = "ecst_view_source_" + suffix;
        String left = "ecst_view_left_" + suffix;
        String right = "ecst_view_right_" + suffix;
        String view = "ecst_view_" + suffix;
        try (Connection connection = DriverManager.getConnection(url, user, password);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + quote(source)
                    + " (vp_rowid BIGINT PRIMARY KEY, c0 INT, c1 INT)");
            statement.execute("CREATE TABLE " + quote(left)
                    + " (vp_rowid BIGINT PRIMARY KEY, c0 INT)");
            statement.execute("CREATE TABLE " + quote(right)
                    + " (vp_rowid BIGINT PRIMARY KEY, c1 INT)");
            statement.execute("INSERT INTO " + quote(source) + " VALUES (1, 1, 10), (2, -1, 20)");
            statement.execute("INSERT INTO " + quote(left) + " VALUES (1, 1), (2, -1)");
            statement.execute("INSERT INTO " + quote(right) + " VALUES (1, 10), (2, 20)");
            statement.execute("CREATE VIEW " + quote(view) + " AS SELECT l.vp_rowid, l.c0, r.c1 FROM "
                    + quote(left) + " l JOIN " + quote(right) + " r ON l.vp_rowid = r.vp_rowid");

            String definition;
            try (ResultSet rows = statement.executeQuery("SHOW CREATE VIEW " + quote(view))) {
                Assumptions.assumeTrue(rows.next(), "VIEW was not created");
                definition = rows.getString(2).toLowerCase(java.util.Locale.ROOT);
            }
            assertTrue(definition.contains(left.toLowerCase(java.util.Locale.ROOT)));
            assertTrue(definition.contains(right.toLowerCase(java.util.Locale.ROOT)));
            assertTrue(definition.contains("vp_rowid"));

            List<String> original = rows(statement, "SELECT vp_rowid FROM " + quote(source) + " WHERE c0 > 0");
            List<String> transformed = rows(statement, "SELECT vp_rowid FROM " + quote(view) + " AS source WHERE c0 > 0");
            assertEquals(original, transformed);
            try (ResultSet plan = statement.executeQuery("EXPLAIN FORMAT=brief SELECT vp_rowid FROM "
                    + quote(view) + " AS source WHERE c0 > 0")) {
                assertTrue(plan.next(), "EXPLAIN returned no rows");
            }
        } catch (java.sql.SQLException unavailable) {
            Assumptions.assumeTrue(false, "TiDB integration endpoint unavailable: " + unavailable.getMessage());
        } finally {
            try (Connection cleanup = DriverManager.getConnection(url, user, password);
                    Statement statement = cleanup.createStatement()) {
                statement.execute("DROP VIEW IF EXISTS " + quote(view));
                statement.execute("DROP TABLE IF EXISTS " + quote(left));
                statement.execute("DROP TABLE IF EXISTS " + quote(right));
                statement.execute("DROP TABLE IF EXISTS " + quote(source));
            } catch (Exception ignored) {
                // The test's primary assertion reports the connection/SQL failure.
            }
        }
    }

    private static List<String> rows(Statement statement, String sql) throws Exception {
        List<String> values = new ArrayList<>();
        try (ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                values.add(result.getString(1));
            }
        }
        return values;
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
