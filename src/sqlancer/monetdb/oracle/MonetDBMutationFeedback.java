package sqlancer.monetdb.oracle;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import sqlancer.monetdb.MonetDBProvider.MonetDBGlobalState;

/** Mutation audit independent of the optional SELECT logger. No-op is a successful statement with unchanged data. */
public final class MonetDBMutationFeedback {
    private static final Path LOG = Path.of("logs/monetdb/vp-mutations.jsonl");
    private static final Object LOG_LOCK = new Object();
    private static final Map<String, AtomicLong> COUNTS = new ConcurrentHashMap<>();
    static {
        for (String operation : new String[] { "update", "merge", "partition_insert", "partition_update",
                "partition_routing" }) {
            for (String outcome : new String[] { "attempt", "success", "failure", "no_op", "changed", "change_unknown",
                    "skipped" }) {
                COUNTS.put(operation + "." + outcome, new AtomicLong());
            }
        }
    }
    private final MonetDBGlobalState state;

    public static final class Outcome {
        public final boolean success;
        public final Boolean noOp;
        public final long affectedRows;
        public final String failureClass;

        Outcome(boolean success, Boolean noOp, long affectedRows, String failureClass) {
            this.success = success;
            this.noOp = noOp;
            this.affectedRows = affectedRows;
            this.failureClass = failureClass;
        }
    }

    private static final class Snapshot {
        final Map<String, Object> metrics = new LinkedHashMap<>();
        Map<String, Long> values;
    }

    public MonetDBMutationFeedback(MonetDBGlobalState state) {
        this.state = state;
    }

    public Outcome run(String operation, String table, String column, String dataType, String tableType, String shape,
            String sql) throws SQLException {
        count(operation, "attempt");
        Snapshot before = snapshot(table, column);
        Map<String, Object> event = event(operation, table, column, dataType, tableType, shape, sql);
        event.put("before", before.metrics);
        long affected = -1;
        SQLException failure = null;
        state.getState().logStatement(sql);
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent("-- VP MUTATION: " + sql);
        }
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
            affected = statement.getUpdateCount();
        } catch (SQLException e) {
            failure = e;
        }
        Snapshot after = snapshot(table, column);
        event.put("after", after.metrics);
        if (failure == null) {
            Boolean unchanged = before.values != null && after.values != null ? before.values.equals(after.values)
                    : affected == 0 ? Boolean.TRUE : null;
            count(operation, "success");
            if (Boolean.TRUE.equals(unchanged)) {
                count(operation, "no_op");
            } else if (Boolean.FALSE.equals(unchanged)) {
                count(operation, "changed");
            } else {
                count(operation, "change_unknown");
            }
            event.put("status", "success");
            event.put("no_op", unchanged);
            event.put("affected_rows", affected);
            write(event);
            return new Outcome(true, unchanged, affected, null);
        }
        recordFailure(event, operation, dataType, tableType, shape, failure);
        // Connection/resource failures must still reach the main oracle rather than being treated as random rejects.
        String category = classify(failure);
        if (category.equals("connection") || category.equals("resource")) {
            throw failure;
        }
        return new Outcome(false, null, -1, category);
    }

    public void skipped(String operation, String table, String column, String dataType, String tableType, String shape,
            String reason) {
        count(operation, "skipped");
        Map<String, Object> event = event(operation, table, column, dataType, tableType, shape, null);
        event.put("status", "skipped");
        event.put("reason", reason);
        write(event);
    }

    /** Setup/cleanup failures are distinct from an executed MERGE failure. */
    public void externalFailure(String operation, String table, String column, String dataType, String tableType,
            String shape, String sql, SQLException failure) {
        count(operation, "attempt");
        recordFailure(event(operation, table, column, dataType, tableType, shape, sql), operation, dataType, tableType,
                shape, failure);
    }

    private void recordFailure(Map<String, Object> event, String operation, String dataType, String tableType,
            String shape, SQLException failure) {
        String category = classify(failure);
        count(operation, "failure");
        count(operation, "failure." + category);
        String code = Objects.toString(failure.getSQLState(), "unknown");
        count(operation,
                "failure_detail." + category + "." + dataType.toLowerCase(Locale.ROOT) + "."
                        + tableType.toLowerCase(Locale.ROOT) + "." + shape.toLowerCase(Locale.ROOT) + "." + code + "."
                        + failure.getErrorCode());
        event.put("status", "failure");
        event.put("failure_class", category);
        event.put("capability_failure", category.equals("capability"));
        List<Map<String, Object>> errors = new ArrayList<>();
        for (SQLException error = failure; error != null; error = error.getNextException()) {
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("sql_state", error.getSQLState());
            detail.put("error_code", error.getErrorCode());
            detail.put("message", error.getMessage());
            errors.add(detail);
        }
        event.put("errors", errors);
        write(event);
    }

    public static String classify(SQLException error) {
        String sqlState = Objects.toString(error.getSQLState(), "");
        String message = Objects.toString(error.getMessage(), "").toLowerCase(Locale.ROOT);
        if (sqlState.startsWith("08")) {
            return "connection";
        }
        if (sqlState.startsWith("53") || message.contains("out of memory") || message.contains("disk full")) {
            return "resource";
        }
        if (sqlState.equals("M0M29") || message.contains("violates the partition")
                || message.contains("violates partition")) {
            return "partition_routing";
        }
        if (sqlState.startsWith("23") || message.contains("constraint") || message.contains("duplicate key")) {
            return "constraint";
        }
        if (sqlState.startsWith("22") || message.contains("overflow") || message.contains("out of range")
                || message.contains("conversion") || message.contains("cannot convert")) {
            return "type_boundary";
        }
        if (sqlState.startsWith("0A") || message.contains("not supported") || message.contains("unsupported")
                || message.contains("not implemented")
                || message.contains("update on the partitioned column is not possible at the moment")) {
            return "capability";
        }
        if (sqlState.startsWith("42") || message.contains("syntax error") || message.contains("no such")) {
            return "sql_shape";
        }
        return "other";
    }

    private Snapshot snapshot(String table, String column) {
        Snapshot result = new Snapshot();
        List<String> errors = new ArrayList<>();
        String relation = quote(table);
        try (Statement sql = state.getConnection().createStatement();
                ResultSet rows = sql.executeQuery("SELECT * FROM " + relation)) {
            result.values = new LinkedHashMap<>();
            long total = 0;
            long nulls = 0;
            int index = rows.findColumn(column);
            int columns = rows.getMetaData().getColumnCount();
            java.util.Set<String> distinct = new java.util.HashSet<>();
            while (rows.next()) {
                String value = rows.getString(index);
                if (value == null) {
                    nulls++;
                }
                distinct.add(value);
                List<String> record = new ArrayList<>();
                for (int i = 1; i <= columns; i++) {
                    record.add(rows.getString(i));
                }
                result.values.merge(json(record), 1L, Long::sum);
                total++;
            }
            result.metrics.put("row_count", total);
            result.metrics.put("null_count", nulls);
            result.metrics.put("distinct_count", distinct.size() - (distinct.contains(null) ? 1 : 0));
        } catch (SQLException e) {
            result.values = null;
            errors.add("counts: " + e.getSQLState() + ": " + e.getMessage());
        }
        // Some MonetDB atom types cannot be ordered. Keep counts and explicitly report missing min/max.
        try (Statement sql = state.getConnection().createStatement(); ResultSet bounds = sql
                .executeQuery("SELECT MIN(" + quote(column) + "), MAX(" + quote(column) + ") FROM " + relation)) {
            if (bounds.next()) {
                result.metrics.put("min", bounds.getString(1));
                result.metrics.put("max", bounds.getString(2));
            }
        } catch (SQLException e) {
            errors.add("bounds: " + e.getSQLState() + ": " + e.getMessage());
            result.metrics.put("min", null);
            result.metrics.put("max", null);
        }
        result.metrics.put("metric_errors", errors);
        return result;
    }

    private Map<String, Object> event(String operation, String table, String column, String dataType, String tableType,
            String shape, String sql) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("time_utc", java.time.Instant.now().toString());
        event.put("database", state.getDatabaseName());
        event.put("operation", operation);
        event.put("table", table);
        event.put("column", column);
        event.put("data_type", dataType);
        event.put("table_type", tableType);
        event.put("sql_shape", shape);
        event.put("sql", sql);
        return event;
    }

    private static String quote(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private static void count(String operation, String outcome) {
        COUNTS.computeIfAbsent(operation.toLowerCase(Locale.ROOT) + "." + outcome, key -> new AtomicLong())
                .incrementAndGet();
    }

    static void appendStats(StringBuilder output) {
        COUNTS.keySet().stream().sorted().forEach(
                key -> output.append("mutation.").append(key).append('=').append(COUNTS.get(key).get()).append('\n'));
    }

    private static void write(Map<String, Object> event) {
        synchronized (LOG_LOCK) {
            try {
                Files.createDirectories(LOG.getParent());
                Files.writeString(LOG, json(event) + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot persist MonetDB mutation feedback", e);
            }
        }
    }

    private static String json(Object value) {
        if (value == null || value instanceof Boolean || value instanceof Number) {
            return Objects.toString(value, "null");
        }
        if (value instanceof Map<?, ?>) {
            List<String> items = new ArrayList<>();
            ((Map<?, ?>) value).forEach((key, item) -> items.add(json(key.toString()) + ":" + json(item)));
            return "{" + String.join(",", items) + "}";
        }
        if (value instanceof List<?>) {
            List<String> items = new ArrayList<>();
            ((List<?>) value).forEach(item -> items.add(json(item)));
            return "[" + String.join(",", items) + "]";
        }
        String text = value.toString();
        StringBuilder output = new StringBuilder("\"");
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (ch == '"' || ch == '\\') {
                output.append('\\').append(ch);
            } else if (ch < 32) {
                output.append(String.format(Locale.ROOT, "\\u%04x", (int) ch));
            } else {
                output.append(ch);
            }
        }
        return output.append('"').toString();
    }
}
