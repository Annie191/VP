package sqlancer.duckdb;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** An independent, length-delimited representation of values already returned by JDBC. */
final class DuckDBValueCanonicalizer {
    private static final Pattern ARRAY_SUFFIX = Pattern.compile("\\[\\d*\\]$");
    private static final Pattern INTERVAL_PART = Pattern.compile(
            "([+-]?\\d+) (years?|months?|days?)|([+-]?)(\\d+):(\\d{2}):(\\d{2})(?:\\.(\\d{1,6}))?");
    private static final BigInteger DAY_MICROS = BigInteger.valueOf(86_400_000_000L);

    private DuckDBValueCanonicalizer() {
    }

    static boolean isStructuredType(String type) {
        String upper = type.toUpperCase(Locale.ROOT);
        return ARRAY_SUFFIX.matcher(upper).find() || upper.startsWith("LIST(") || upper.startsWith("ARRAY(")
                || upper.startsWith("STRUCT(") || upper.startsWith("MAP(") || upper.equals("BLOB")
                || upper.equals("INTERVAL") || upper.equals("JSON") || upper.contains("GEOMETRY");
    }

    static String canonicalize(Object value, String type) throws SQLException {
        if (value == null) {
            return "N;";
        }
        String upper = type.toUpperCase(Locale.ROOT);
        if (value instanceof java.sql.Array) {
            java.sql.Array array = (java.sql.Array) value;
            return sequence(array.getArray(), array.getBaseTypeName());
        }
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            List<String> fields = parts(inside(struct.getSQLTypeName()));
            Object[] attributes = struct.getAttributes();
            if (fields.size() != attributes.length) {
                throw new SQLException("JDBC STRUCT field count differs from its type: " + type);
            }
            StringBuilder result = new StringBuilder("STRUCT;");
            for (int i = 0; i < attributes.length; i++) {
                String field = fields.get(i);
                int boundary = fieldTypeBoundary(field);
                result.append(token(field.substring(0, boundary)));
                result.append(token(canonicalize(attributes[i], field.substring(boundary).trim())));
            }
            return result.toString();
        }
        if (value instanceof Map && upper.startsWith("MAP(")) {
            List<String> types = parts(inside(type));
            if (types.size() != 2) {
                throw new SQLException("Invalid JDBC MAP type: " + type);
            }
            StringBuilder result = new StringBuilder("MAP;");
            // DuckDB MAP equality is order-sensitive; do not sort or discard entries.
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                result.append(token(canonicalize(entry.getKey(), types.get(0))));
                result.append(token(canonicalize(entry.getValue(), types.get(1))));
            }
            return result.toString();
        }
        if (value instanceof Blob) {
            Blob blob = (Blob) value;
            return "BLOB:" + HexFormat.of().formatHex(blob.getBytes(1, Math.toIntExact(blob.length())));
        }
        if (value instanceof byte[]) {
            return "BLOB:" + HexFormat.of().formatHex((byte[]) value);
        }
        if (upper.equals("INTERVAL")) {
            return interval(value.toString());
        }
        if (value instanceof Number) {
            try {
                return "NUMBER:" + new BigDecimal(value.toString()).stripTrailingZeros().toPlainString();
            } catch (NumberFormatException nonFinite) {
                return "NUMBER:" + value;
            }
        }
        if (upper.startsWith("STRUCT(") || upper.startsWith("MAP(") || ARRAY_SUFFIX.matcher(upper).find()) {
            throw new SQLException("Unsupported JDBC structured representation: " + type + " / " + value.getClass());
        }
        return "VALUE:" + token(value.toString());
    }

    private static String sequence(Object values, String elementType) throws SQLException {
        StringBuilder result = new StringBuilder("ARRAY;");
        int length = Array.getLength(values);
        result.append(length).append(';');
        for (int i = 0; i < length; i++) {
            result.append(token(canonicalize(Array.get(values, i), elementType)));
        }
        return result.toString();
    }

    static String interval(String value) throws SQLException {
        // DuckDB compares intervals with 30 days/month and 24 hours/day. Implement
        // that arithmetic locally, including signed components and microseconds.
        Matcher matcher = INTERVAL_PART.matcher(value);
        BigInteger micros = BigInteger.ZERO;
        int end = 0;
        while (matcher.find()) {
            if (!value.substring(end, matcher.start()).trim().isEmpty()) {
                throw new SQLException("Unrecognized JDBC INTERVAL: " + value);
            }
            if (matcher.group(1) != null) {
                BigInteger count = new BigInteger(matcher.group(1));
                String unit = matcher.group(2);
                int days = unit.startsWith("year") ? 360 : unit.startsWith("month") ? 30 : 1;
                micros = micros.add(count.multiply(BigInteger.valueOf(days)).multiply(DAY_MICROS));
            } else {
                BigInteger time = new BigInteger(matcher.group(4)).multiply(BigInteger.valueOf(3600))
                        .add(new BigInteger(matcher.group(5)).multiply(BigInteger.valueOf(60)))
                        .add(new BigInteger(matcher.group(6))).multiply(BigInteger.valueOf(1_000_000));
                String fraction = matcher.group(7);
                if (fraction != null) {
                    time = time.add(new BigInteger((fraction + "000000").substring(0, 6)));
                }
                micros = micros.add("-".equals(matcher.group(3)) ? time.negate() : time);
            }
            end = matcher.end();
        }
        if (end == 0 || !value.substring(end).trim().isEmpty()) {
            throw new SQLException("Unrecognized JDBC INTERVAL: " + value);
        }
        BigInteger[] seconds = micros.abs().divideAndRemainder(BigInteger.valueOf(1_000_000));
        BigInteger[] hours = seconds[0].divideAndRemainder(BigInteger.valueOf(3600));
        long remainder = hours[1].longValueExact();
        return (micros.signum() < 0 ? "-" : "") + hours[0]
                + String.format(Locale.ROOT, ":%02d:%02d.%06d", remainder / 60, remainder % 60,
                        seconds[1].longValueExact());
    }

    private static String token(String value) {
        return value.length() + ":" + value + ";";
    }

    private static String inside(String type) {
        return type.substring(type.indexOf('(') + 1, type.lastIndexOf(')'));
    }

    private static int fieldTypeBoundary(String field) throws SQLException {
        boolean quoted = false;
        for (int i = 0; i < field.length(); i++) {
            char c = field.charAt(i);
            if (c == '"') {
                quoted = !quoted;
            } else if (!quoted && Character.isWhitespace(c)) {
                return i;
            }
        }
        throw new SQLException("Invalid JDBC STRUCT field declaration: " + field);
    }

    private static List<String> parts(String value) {
        List<String> result = new ArrayList<>();
        int depth = 0;
        int start = 0;
        boolean quoted = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '"') {
                quoted = !quoted;
            } else if (!quoted) {
                if (c == '(' || c == '[') {
                    depth++;
                } else if (c == ')' || c == ']') {
                    depth--;
                } else if (c == ',' && depth == 0) {
                    result.add(value.substring(start, i).trim());
                    start = i + 1;
                }
            }
        }
        if (start < value.length()) {
            result.add(value.substring(start).trim());
        }
        return result;
    }
}
