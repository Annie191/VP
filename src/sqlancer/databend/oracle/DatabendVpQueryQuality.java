package sqlancer.databend.oracle;

import sqlancer.databend.DatabendSchema.DatabendDataType;

/** SQL contracts shared by witnessed VP templates and their regression controls. */
final class DatabendVpQueryQuality {
    private DatabendVpQueryQuality() {
    }

    static String targetShape(int percentile) {
        if (percentile < 0 || percentile >= 100) {
            throw new IllegalArgumentException("Expected percentile in [0, 100)");
        }
        return percentile < 70 ? "MULTI_COLUMN" : percentile < 90 ? "SINGLE_COLUMN" : "EMPTY";
    }

    static boolean supportsTextRoundTrip(DatabendDataType type) {
        return type == DatabendDataType.INT || type == DatabendDataType.VARCHAR || type == DatabendDataType.BOOLEAN
                || type == DatabendDataType.DATE || type == DatabendDataType.TIMESTAMP
                || type == DatabendDataType.DECIMAL;
    }

    static String quote(String name) {
        return "`" + name.replace("`", "``") + "`";
    }

    static String literal(String value) {
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'";
    }

    static String witnessedTerm(String column, String value) {
        String name = quote(column);
        return value == null ? "(" + name + " IS NULL)"
                : "COALESCE(TO_VARCHAR(" + name + ") = " + literal(value) + ", FALSE)";
    }

    static String businessQuery(String relation, String left, String right, String predicate, boolean aggregate) {
        String l = "TO_VARCHAR(" + quote(left) + ")";
        String r = "TO_VARCHAR(" + quote(right) + ")";
        if (aggregate) {
            // A business-derived, low-cardinality key creates repeated groups without grouping by unique rowid.
            String group = "MOD(LENGTH(COALESCE(" + l + ", '')), 3)";
            return "SELECT " + group + " AS vp_group, MIN(" + l + "), MAX(" + r + "), COUNT(" + quote(right) + ") FROM "
                    + relation + " WHERE " + predicate + " GROUP BY " + group + " ORDER BY vp_group";
        }
        return "SELECT " + quote("vp_rowid") + ", " + quote(left) + ", " + quote(right) + " FROM " + relation
                + " WHERE " + predicate + " ORDER BY " + quote("vp_rowid");
    }

    static String decodedJsonColumn(String column, String sqlType) {
        String key = quote(column);
        return "CASE WHEN CAST(jp.payload:" + quote(column + "_null") + " AS BOOLEAN) THEN CAST(NULL AS " + sqlType
                + ") ELSE CAST(CAST(jp.payload:" + key + " AS VARCHAR) AS " + sqlType + ") END";
    }
}
