package sqlancer.databend.oracle;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Contracts observed in the 950 FUSE plans, with explicit evidence rather than template-based claims. */
final class DatabendVpP1 {
    private DatabendVpP1() {
    }

    static String planEvidence(String plan) {
        if (plan == null) {
            return "plan=unknown";
        }
        String normalized = plan.toUpperCase(Locale.ROOT);
        List<String> scans = new ArrayList<>();
        Matcher columns = Pattern.compile("TABLESCAN.*?OUTPUT COLUMNS:\\s*\\[([^\\]]*)\\]", Pattern.DOTALL)
                .matcher(normalized);
        while (columns.find()) {
            scans.add(columns.group(1));
        }
        boolean reduced = false;
        Matcher partitions = Pattern
                .compile("PARTITIONS TOTAL:\\s*(\\d+).*?PARTITIONS SCANNED:\\s*(\\d+)", Pattern.DOTALL)
                .matcher(normalized);
        while (partitions.find()) {
            reduced |= Long.parseLong(partitions.group(2)) < Long.parseLong(partitions.group(1));
        }
        Matcher fetch = Pattern.compile("COLUMNS TO FETCH:\\s*\\[([^\\]]*)\\]").matcher(normalized);
        List<String> fetched = new ArrayList<>();
        while (fetch.find()) {
            fetched.add(fetch.group(1));
        }
        return "scan_outputs=" + scans + " filter_pushdown=" + normalized.contains("PUSH DOWNS: [FILTERS: [IS_TRUE")
                + " pruning_stats=" + normalized.contains("PRUNING STATS:") + " partitions_reduced=" + reduced
                + " late_materialization=" + (normalized.contains("ROWFETCH") && !fetched.isEmpty()) + " fetch_columns="
                + fetched;
    }

    static String semiDocument(String business) {
        // Every six consecutive rowids include all five boundary cases plus a populated array.
        return "CASE MOD(`vp_rowid`, 6) WHEN 0 THEN CAST(NULL AS VARIANT) WHEN 1 THEN PARSE_JSON('null')"
                + " WHEN 2 THEN PARSE_JSON('{}') WHEN 3 THEN PARSE_JSON('{\"gate\":true,\"items\":[]}')"
                + " WHEN 4 THEN PARSE_JSON('{\"gate\":true,\"items\":[[],[1,2]]}')"
                + " ELSE JSON_OBJECT('gate', TRUE, 'items', ARRAY_CONSTRUCT(COALESCE(TO_VARCHAR(" + business
                + "), 'sql-null'), 'tail')) END";
    }

    static String semiQuery(String relation, String document, String opposite) {
        String rid = "`vp_rowid`";
        String path = document + ":items";
        String kind = "CASE WHEN " + document + " IS NULL THEN 'sql-null' WHEN IS_NULL_VALUE(" + document
                + ") THEN 'json-null' WHEN " + path + " IS NULL THEN 'missing' ELSE 'array' END";
        String summary = "SELECT " + rid + " AS rid, 'boundary' AS kind, " + kind
                + " AS path, CONCAT(COALESCE(JSON_TO_STRING(" + path + "), '<missing>'), '|', COALESCE(TO_VARCHAR("
                + opposite + "), '<sql-null>')) AS value FROM " + relation;
        String expansion = "SELECT " + rid + ", 'flatten', f.path, CONCAT(COALESCE(JSON_TO_STRING(f.value),"
                + " '<json-null>'), '|', COALESCE(TO_VARCHAR(" + opposite + "), '<sql-null>')) FROM " + relation
                + ", LATERAL FLATTEN(input => " + path + ", recursive => TRUE) f WHERE COALESCE(CAST(" + document
                + ":gate AS BOOLEAN), FALSE)";
        return "SELECT rid, kind, path, value FROM (" + summary + " UNION ALL " + expansion
                + ") ORDER BY rid, kind, path, value";
    }
}
