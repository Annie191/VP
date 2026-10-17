package sqlancer.monetdb.oracle;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Independent comparison layers. Approximate diagnostics never replace exact value checks. */
public final class MonetDBResultComparison {
    public static final class Snapshot {
        final List<String> metadata = new ArrayList<>();
        final List<List<String>> exact = new ArrayList<>();
        final List<List<String>> normalized = new ArrayList<>();
        final List<List<Boolean>> nulls = new ArrayList<>();
        final List<Integer> types = new ArrayList<>();

        public void metadata(ResultSetMetaData md) throws SQLException {
            for (int i = 1; i <= md.getColumnCount(); i++) {
                types.add(md.getColumnType(i));
                metadata.add(md.getColumnType(i) + ":" + md.getColumnTypeName(i) + ":precision="
                        + md.getPrecision(i) + ":scale=" + md.getScale(i) + ":nullable=" + md.isNullable(i));
            }
        }

        public void row(ResultSet rs) throws SQLException {
            List<String> raw = new ArrayList<>();
            List<String> values = new ArrayList<>();
            List<Boolean> bitmap = new ArrayList<>();
            for (int i = 1; i <= types.size(); i++) {
                Object value = rs.getObject(i);
                bitmap.add(value == null);
                if (value == null) {
                    raw.add(null);
                    values.add(null);
                    continue;
                }
                int type = types.get(i - 1);
                String display = rs.getString(i);
                if (binary(type)) {
                    StringBuilder hex = new StringBuilder();
                    for (byte octet : rs.getBytes(i)) {
                        hex.append(String.format(java.util.Locale.ROOT, "%02x", octet & 255));
                    }
                    display = hex.toString();
                    raw.add("binary:" + display);
                    values.add("binary:" + display);
                } else if (floating(type)) {
                    double number = rs.getDouble(i);
                    raw.add("float:" + Long.toHexString(Double.doubleToRawLongBits(number)) + ":" + display);
                    values.add("float:" + Double.toHexString(number));
                } else {
                    raw.add(type + ":" + value.getClass().getName() + ":" + display);
                    if (numeric(type)) {
                        values.add("number:" + new BigDecimal(display).stripTrailingZeros().toPlainString());
                    } else {
                        // Preserve type and full text for MonetDB atoms; no lossy temporal/network normalization.
                        values.add(type + ":" + display);
                    }
                }
            }
            exact.add(raw);
            normalized.add(values);
            nulls.add(bitmap);
        }
    }

    public static final class Report {
        public final boolean exactBag;
        public final boolean normalizedBag;
        public final boolean exactOrder;
        public final boolean normalizedOrder;
        public final boolean metadata;
        public final boolean nullBag;
        public final boolean nullOrder;
        public final String aggregateError;

        Report(Snapshot left, Snapshot right, boolean scalarAggregate, double absolute, double relative) {
            exactBag = bag(left.exact).equals(bag(right.exact));
            normalizedBag = bag(left.normalized).equals(bag(right.normalized));
            exactOrder = left.exact.equals(right.exact);
            normalizedOrder = left.normalized.equals(right.normalized);
            metadata = left.metadata.equals(right.metadata);
            nullBag = bag(left.nulls).equals(bag(right.nulls));
            nullOrder = left.nulls.equals(right.nulls);
            aggregateError = scalarAggregate ? aggregateError(left, right, absolute, relative) : "not_applicable";
        }

        public boolean fails(boolean ordered, boolean strictRepresentation) {
            return !normalizedBag || !nullBag || ordered && (!normalizedOrder || !nullOrder)
                    || strictRepresentation && (!exactBag || !metadata || ordered && !exactOrder);
        }

        public boolean diagnosticDifference() {
            return !exactBag || !exactOrder || !metadata || !nullBag || !nullOrder;
        }

        @Override
        public String toString() {
            return "exact_bag=" + exactBag + ", normalized_bag=" + normalizedBag + ", exact_order=" + exactOrder
                    + ", normalized_order=" + normalizedOrder + ", metadata=" + metadata + ", null_bag=" + nullBag
                    + ", null_order=" + nullOrder + ", aggregate_error=" + aggregateError;
        }
    }

    private MonetDBResultComparison() {
    }

    public static Report compare(Snapshot left, Snapshot right, boolean scalarAggregate, double absolute, double relative) {
        if (!Double.isFinite(absolute) || !Double.isFinite(relative) || absolute < 0 || relative < 0) {
            throw new IllegalArgumentException("Aggregate diagnostic error bounds must be finite and nonnegative");
        }
        return new Report(left, right, scalarAggregate, absolute, relative);
    }

    public static String evidence(Snapshot left, Snapshot right) {
        Map<List<String>, Integer> onlyLeft = bag(left.exact);
        Map<List<String>, Integer> onlyRight = bag(right.exact);
        Map<List<String>, Integer> rightBag = new LinkedHashMap<>(onlyRight);
        onlyLeft.replaceAll((row, count) -> count - rightBag.getOrDefault(row, 0));
        onlyLeft.entrySet().removeIf(e -> e.getValue() <= 0);
        Map<List<String>, Integer> leftBag = bag(left.exact);
        onlyRight.replaceAll((row, count) -> count - leftBag.getOrDefault(row, 0));
        onlyRight.entrySet().removeIf(e -> e.getValue() <= 0);
        int mismatch = 0;
        int common = Math.min(left.exact.size(), right.exact.size());
        while (mismatch < common && left.exact.get(mismatch).equals(right.exact.get(mismatch))) {
            mismatch++;
        }
        return "left_only=" + onlyLeft.entrySet().stream().limit(8).collect(java.util.stream.Collectors.toList())
                + ", right_only=" + onlyRight.entrySet().stream().limit(8).collect(java.util.stream.Collectors.toList())
                + ", first_order_difference=" + (mismatch == common && left.exact.size() == right.exact.size() ? "none" : mismatch)
                + ", left_at_difference=" + (mismatch < left.exact.size() ? left.exact.get(mismatch) : "end")
                + ", right_at_difference=" + (mismatch < right.exact.size() ? right.exact.get(mismatch) : "end");
    }

    private static String aggregateError(Snapshot left, Snapshot right, double absolute, double relative) {
        if (left.exact.size() != 1 || right.exact.size() != 1 || left.types.size() != right.types.size()) {
            return "requires_single_row";
        }
        List<String> errors = new ArrayList<>();
        for (int i = 0; i < left.types.size(); i++) {
            if (!floating(left.types.get(i)) || !floating(right.types.get(i))) {
                continue;
            }
            String a = left.normalized.get(0).get(i);
            String b = right.normalized.get(0).get(i);
            if (a == null || b == null) {
                errors.add("column=" + (i + 1) + ":null_equal=" + Objects.equals(a, b));
                continue;
            }
            double x = Double.valueOf(a.substring(6));
            double y = Double.valueOf(b.substring(6));
            if (!Double.isFinite(x) || !Double.isFinite(y)) {
                errors.add("column=" + (i + 1) + ":nonfinite_equal=" + Objects.equals(a, b));
                continue;
            }
            BigDecimal delta = BigDecimal.valueOf(x).subtract(BigDecimal.valueOf(y)).abs();
            BigDecimal scale = BigDecimal.valueOf(x).abs().max(BigDecimal.valueOf(y).abs());
            BigDecimal bound = BigDecimal.valueOf(absolute).add(BigDecimal.valueOf(relative).multiply(scale));
            errors.add("column=" + (i + 1) + ":abs_error=" + delta + ":bound=" + bound
                    + ":within_bound=" + (delta.compareTo(bound) <= 0));
        }
        return errors.isEmpty() ? "no_floating_columns" : String.join(";", errors);
    }

    private static <T> Map<T, Integer> bag(List<T> rows) {
        Map<T, Integer> result = new LinkedHashMap<>();
        rows.forEach(row -> result.merge(row, 1, Integer::sum));
        return result;
    }

    private static boolean floating(int type) {
        return type == Types.FLOAT || type == Types.REAL || type == Types.DOUBLE;
    }

    private static boolean numeric(int type) {
        return type == Types.TINYINT || type == Types.SMALLINT || type == Types.INTEGER || type == Types.BIGINT
                || type == Types.NUMERIC || type == Types.DECIMAL;
    }

    private static boolean binary(int type) {
        return type == Types.BINARY || type == Types.VARBINARY || type == Types.LONGVARBINARY || type == Types.BLOB;
    }
}
