package sqlancer.duckdb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;

public class DuckDBSchema extends AbstractSchema<DuckDBGlobalState, DuckDBTable> {

    public enum DuckDBDataType {

        INT, VARCHAR, BOOLEAN, FLOAT, DATE, TIMESTAMP, DECIMAL, HUGEINT, UUID, BLOB, TIME, INTERVAL, LIST, STRUCT, MAP,
        NULL;

        public static DuckDBDataType getRandomWithoutNull() {
            DuckDBDataType dt;
            do {
                dt = Randomly.fromOptions(values());
            } while (dt == DuckDBDataType.NULL);
            return dt;
        }

    }

    /** Immutable base node for the DuckDB type tree. */
    public static class DuckDBCompositeDataType {

        private final DuckDBDataType dataType;
        private final int size;
        private final String sqlType;
        private final int precision;
        private final int scale;
        private final DuckDBCompositeDataType elementType;
        private final List<DuckDBCompositeDataType> children;
        private final Map<String, DuckDBCompositeDataType> fields;

        public DuckDBCompositeDataType(DuckDBDataType dataType, int size) {
            this(dataType, size, null, -1, -1, null, List.of(), Map.of());
        }

        protected DuckDBCompositeDataType(DuckDBDataType dataType, int size, String sqlType, int precision, int scale,
                DuckDBCompositeDataType elementType, List<DuckDBCompositeDataType> children,
                Map<String, DuckDBCompositeDataType> fields) {
            this.dataType = dataType;
            this.size = size;
            this.sqlType = sqlType;
            this.precision = precision;
            this.scale = scale;
            this.elementType = elementType;
            this.children = List.copyOf(children);
            this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
        }

        public DuckDBDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public int getPrecision() {
            return precision;
        }

        public int getScale() {
            return scale;
        }

        public DuckDBCompositeDataType getElementType() {
            return elementType;
        }

        public List<DuckDBCompositeDataType> getChildren() {
            return children;
        }

        public Map<String, DuckDBCompositeDataType> getFields() {
            return fields;
        }

        public boolean isNested() {
            return dataType == DuckDBDataType.LIST || dataType == DuckDBDataType.STRUCT
                    || dataType == DuckDBDataType.MAP;
        }

        public boolean isScalar() {
            return !isNested();
        }

        public boolean isNumeric() {
            return dataType == DuckDBDataType.INT || dataType == DuckDBDataType.FLOAT
                    || dataType == DuckDBDataType.DECIMAL || dataType == DuckDBDataType.HUGEINT;
        }

        public boolean isTemporal() {
            return dataType == DuckDBDataType.DATE || dataType == DuckDBDataType.TIMESTAMP
                    || dataType == DuckDBDataType.TIME;
        }

        public boolean isCharacterLike() {
            return dataType == DuckDBDataType.VARCHAR;
        }

        public boolean supportsVpExpression() {
            return isScalar() && dataType != DuckDBDataType.BLOB && dataType != DuckDBDataType.UUID
                    && dataType != DuckDBDataType.TIME && dataType != DuckDBDataType.INTERVAL;
        }

        /** DuckDB permits nested keys, but a MAP key itself cannot be NULL. */
        public boolean isValidMapKeyType() {
            return dataType != DuckDBDataType.NULL;
        }

        public boolean isOrderable() {
            return !isNested() && dataType != DuckDBDataType.BLOB && dataType != DuckDBDataType.INTERVAL;
        }

        public boolean isComparable() {
            return dataType != DuckDBDataType.NULL;
        }

        public boolean supportsComparable() {
            return isComparable() && !isNested() && dataType != DuckDBDataType.BLOB;
        }

        public boolean supportsOrderable() {
            return isOrderable();
        }

        public boolean isIndexable() {
            return !isNested() && dataType != DuckDBDataType.BLOB && dataType != DuckDBDataType.INTERVAL;
        }

        public boolean supportsLiteral() {
            return isScalar() && dataType != DuckDBDataType.NULL;
        }

        public boolean supportsStableOrdering() {
            return isOrderable() && dataType != DuckDBDataType.FLOAT;
        }

        public boolean supportsAggregate(String aggregate) {
            String name = aggregate.toUpperCase(Locale.ROOT);
            if (isNested() || dataType == DuckDBDataType.BLOB || dataType == DuckDBDataType.NULL) {
                return name.equals("COUNT") || name.equals("LIST");
            }
            if (name.equals("QUANTILE") || name.equals("HISTOGRAM")) {
                return dataType == DuckDBDataType.INT || dataType == DuckDBDataType.FLOAT
                        || dataType == DuckDBDataType.DECIMAL || dataType == DuckDBDataType.HUGEINT;
            }
            if (name.equals("MODE") || name.equals("ARG_MAX") || name.equals("ARG_MIN") || name.equals("FIRST")
                    || name.equals("LAST")) {
                return supportsComparable() || dataType == DuckDBDataType.INTERVAL;
            }
            return true;
        }

        public boolean supportsParquetRoundTrip() {
            return dataType != DuckDBDataType.NULL;
        }

        public boolean supportsReconstruction(String strategy) {
            if ((dataType == DuckDBDataType.LIST || dataType == DuckDBDataType.STRUCT || dataType == DuckDBDataType.MAP)
                    && ("JSON_PACK".equals(strategy) || "STRING_PACK".equals(strategy))) {
                return false;
            }
            return true;
        }

        public String toSQLType() {
            if (sqlType != null) {
                return sqlType;
            }
            return toString();
        }

        public static DuckDBCompositeDataType getRandomWithoutNull() {
            DuckDBDataType type = DuckDBDataType.getRandomWithoutNull();
            int size = -1;
            switch (type) {
            case INT:
                size = Randomly.fromOptions(1, 2, 4, 8);
                break;
            case FLOAT:
                size = Randomly.fromOptions(4, 8);
                break;
            case BOOLEAN:
            case VARCHAR:
            case DATE:
            case TIMESTAMP:
            case DECIMAL:
            case HUGEINT:
            case UUID:
            case BLOB:
            case TIME:
            case INTERVAL:
                size = 0;
                break;
            case LIST:
            case STRUCT:
            case MAP:
                return randomNestedType(2);
            default:
                throw new AssertionError(type);
            }

            return scalar(type, size, null, -1, -1);
        }

        @Override
        public String toString() {
            if (sqlType != null) {
                return sqlType;
            }
            switch (getPrimitiveDataType()) {
            case INT:
                switch (size) {
                case 8:
                    return "BIGINT";
                case 4:
                    return "INTEGER";
                case 2:
                    return "SMALLINT";
                case 1:
                    return "TINYINT";
                default:
                    throw new AssertionError(size);
                }
            case VARCHAR:
                return "VARCHAR";
            case FLOAT:
                switch (size) {
                case 8:
                    return "DOUBLE";
                case 4:
                    return "REAL";
                default:
                    throw new AssertionError(size);
                }
            case BOOLEAN:
                return "BOOLEAN";
            case TIMESTAMP:
                return "TIMESTAMP";
            case DATE:
                return "DATE";
            case NULL:
                return "NULL";
            case DECIMAL:
                return "DECIMAL(18,3)";
            case HUGEINT:
                return "HUGEINT";
            case UUID:
                return "UUID";
            case BLOB:
                return "BLOB";
            case TIME:
                return "TIME";
            case INTERVAL:
                return "INTERVAL";
            case LIST:
                return elementType + "[]";
            case STRUCT:
                return "STRUCT(" + fields.entrySet().stream().map(e -> e.getKey() + " " + e.getValue())
                        .collect(java.util.stream.Collectors.joining(", ")) + ")";
            case MAP:
                return "MAP(" + children.get(0) + ", " + children.get(1) + ")";
            default:
                throw new AssertionError(getPrimitiveDataType());
            }
        }

        private static DuckDBCompositeDataType randomNestedType(int depth) {
            if (depth <= 0 || Randomly.getBoolean()) {
                DuckDBDataType type = Randomly.fromOptions(DuckDBDataType.INT, DuckDBDataType.VARCHAR,
                        DuckDBDataType.BOOLEAN, DuckDBDataType.DATE, DuckDBDataType.UUID, DuckDBDataType.BLOB,
                        DuckDBDataType.TIME, DuckDBDataType.INTERVAL, DuckDBDataType.DECIMAL, DuckDBDataType.HUGEINT);
                int size = type == DuckDBDataType.INT ? 8 : type == DuckDBDataType.HUGEINT ? 16 : 0;
                return scalar(type, size, null, -1, -1);
            }
            DuckDBCompositeDataType child = randomNestedType(depth - 1);
            switch (Randomly.fromOptions(0, 1, 2)) {
            case 0:
                return listType(child);
            case 1:
                Map<String, DuckDBCompositeDataType> fields = new LinkedHashMap<>();
                fields.put("f0", child);
                fields.put("f1", randomNestedType(depth - 1));
                return structType(fields);
            default:
                return mapType(scalar(DuckDBDataType.VARCHAR, 0, "VARCHAR", -1, -1), child);
            }
        }

        private static DuckDBCompositeDataType listType(DuckDBCompositeDataType element) {
            return new ListType(element, element + "[]");
        }

        private static DuckDBCompositeDataType structType(Map<String, DuckDBCompositeDataType> fields) {
            String sql = "STRUCT(" + fields.entrySet().stream().map(e -> e.getKey() + " " + e.getValue())
                    .collect(java.util.stream.Collectors.joining(", ")) + ")";
            return new StructType(fields, sql);
        }

        private static DuckDBCompositeDataType mapType(DuckDBCompositeDataType key, DuckDBCompositeDataType value) {
            if (!key.isValidMapKeyType()) {
                throw new IllegalArgumentException("DuckDB MAP keys cannot have NULL type: " + key);
            }
            return new MapType(key, value);
        }

        private static DuckDBCompositeDataType scalar(DuckDBDataType type, int size, String sql, int precision,
                int scale) {
            return new ScalarType(type, size, sql, precision, scale);
        }

        public static final class ScalarType extends DuckDBCompositeDataType {
            public ScalarType(DuckDBDataType dataType, int size, String sqlType, int precision, int scale) {
                super(dataType, size, sqlType, precision, scale, null, List.of(), Map.of());
            }
        }

        public static final class ListType extends DuckDBCompositeDataType {
            public ListType(DuckDBCompositeDataType elementType, String sqlType) {
                super(DuckDBDataType.LIST, 0, sqlType, -1, -1, elementType, List.of(elementType), Map.of());
            }
        }

        public static final class StructType extends DuckDBCompositeDataType {
            public StructType(Map<String, DuckDBCompositeDataType> fields, String sqlType) {
                super(DuckDBDataType.STRUCT, 0, sqlType, -1, -1, null, List.copyOf(fields.values()), fields);
            }
        }

        public static final class MapType extends DuckDBCompositeDataType {
            public MapType(DuckDBCompositeDataType keyType, DuckDBCompositeDataType valueType) {
                this(validateKey(keyType), valueType, true);
            }

            private MapType(DuckDBCompositeDataType keyType, DuckDBCompositeDataType valueType, boolean validated) {
                super(DuckDBDataType.MAP, 0, "MAP(" + keyType + ", " + valueType + ")", -1, -1, null,
                        List.of(keyType, valueType), Map.of());
            }

            private static DuckDBCompositeDataType validateKey(DuckDBCompositeDataType keyType) {
                if (!keyType.isValidMapKeyType()) {
                    throw new IllegalArgumentException("DuckDB MAP keys cannot have NULL type: " + keyType);
                }
                return keyType;
            }

            public DuckDBCompositeDataType getKeyType() {
                return getChildren().get(0);
            }

            public DuckDBCompositeDataType getValueType() {
                return getChildren().get(1);
            }
        }

        private static DuckDBCompositeDataType parse(String input) {
            String type = input.trim();
            String upper = type.toUpperCase(Locale.ROOT);
            // Array syntax such as DECIMAL(18,3)[] must be recognized before the
            // scalar DECIMAL prefix; otherwise the element suffix is silently lost.
            if (upper.startsWith("LIST(") || upper.endsWith("[]")) {
                String child = upper.startsWith("LIST(") ? inside(type) : type.substring(0, type.length() - 2);
                DuckDBCompositeDataType element = parse(child);
                return listType(element);
            }
            if (upper.startsWith("DECIMAL") || upper.startsWith("NUMERIC")) {
                int open = type.indexOf('('), close = type.lastIndexOf(')');
                int p = 18, s = 3;
                if (open >= 0 && close > open) {
                    String[] parts = type.substring(open + 1, close).split(",");
                    p = Integer.parseInt(parts[0].trim());
                    if (parts.length > 1)
                        s = Integer.parseInt(parts[1].trim());
                }
                return scalar(DuckDBDataType.DECIMAL, 0, "DECIMAL(" + p + "," + s + ")", p, s);
            }
            if (upper.startsWith("MAP(")) {
                List<String> parts = topLevelParts(inside(type));
                if (parts.size() != 2)
                    throw new AssertionError(type);
                return mapType(parse(parts.get(0)), parse(parts.get(1)));
            }
            if (upper.startsWith("STRUCT(") || upper.startsWith("ROW(")) {
                Map<String, DuckDBCompositeDataType> fields = new LinkedHashMap<>();
                for (String part : topLevelParts(inside(type))) {
                    int space = firstTopLevelSpace(part);
                    if (space < 0)
                        throw new AssertionError(type);
                    fields.put(unquote(part.substring(0, space).trim()), parse(part.substring(space + 1)));
                }
                return structType(fields);
            }
            if (upper.equals("INTEGER") || upper.equals("INT"))
                return scalar(DuckDBDataType.INT, 4, "INTEGER", -1, -1);
            if (upper.equals("SMALLINT"))
                return scalar(DuckDBDataType.INT, 2, "SMALLINT", -1, -1);
            if (upper.equals("BIGINT"))
                return scalar(DuckDBDataType.INT, 8, "BIGINT", -1, -1);
            if (upper.equals("HUGEINT"))
                return scalar(DuckDBDataType.HUGEINT, 16, "HUGEINT", -1, -1);
            if (upper.equals("TINYINT"))
                return scalar(DuckDBDataType.INT, 1, "TINYINT", -1, -1);
            if (upper.equals("VARCHAR") || upper.equals("TEXT"))
                return scalar(DuckDBDataType.VARCHAR, 0, "VARCHAR", -1, -1);
            if (upper.equals("FLOAT") || upper.equals("REAL"))
                return scalar(DuckDBDataType.FLOAT, 4, "REAL", -1, -1);
            if (upper.equals("DOUBLE"))
                return scalar(DuckDBDataType.FLOAT, 8, "DOUBLE", -1, -1);
            if (upper.equals("BOOLEAN"))
                return scalar(DuckDBDataType.BOOLEAN, 0, "BOOLEAN", -1, -1);
            if (upper.equals("DATE"))
                return scalar(DuckDBDataType.DATE, 0, "DATE", -1, -1);
            if (upper.equals("TIMESTAMP") || upper.equals("TIMESTAMP WITHOUT TIME ZONE"))
                return scalar(DuckDBDataType.TIMESTAMP, 0, "TIMESTAMP", -1, -1);
            if (upper.equals("JSON"))
                return scalar(DuckDBDataType.VARCHAR, 0, "JSON", -1, -1);
            if (upper.equals("TIMESTAMP_S") || upper.equals("TIMESTAMP_MS") || upper.equals("TIMESTAMP_NS")
                    || upper.equals("TIMESTAMP WITH TIME ZONE") || upper.equals("TIMESTAMPTZ"))
                return scalar(DuckDBDataType.TIMESTAMP, 0,
                        upper.equals("TIMESTAMPTZ") ? "TIMESTAMP WITH TIME ZONE" : upper, -1, -1);
            if (upper.equals("UUID"))
                return scalar(DuckDBDataType.UUID, 0, "UUID", -1, -1);
            if (upper.equals("BLOB") || upper.equals("BYTEA"))
                return scalar(DuckDBDataType.BLOB, 0, "BLOB", -1, -1);
            if (upper.equals("TIME") || upper.equals("TIME WITHOUT TIME ZONE"))
                return scalar(DuckDBDataType.TIME, 0, "TIME", -1, -1);
            if (upper.equals("INTERVAL"))
                return scalar(DuckDBDataType.INTERVAL, 0, "INTERVAL", -1, -1);
            if (upper.equals("\"NULL\"") || upper.equals("NULL"))
                return scalar(DuckDBDataType.NULL, 0, "VARCHAR", -1, -1);
            throw new AssertionError(input);
        }

        private static String inside(String type) {
            return type.substring(type.indexOf('(') + 1, type.lastIndexOf(')'));
        }

        private static String unquote(String name) {
            return name.replaceAll("^\\\"|\\\"$", "");
        }

        private static int firstTopLevelSpace(String s) {
            int depth = 0;
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '(')
                    depth++;
                else if (c == ')')
                    depth--;
                else if (Character.isWhitespace(c) && depth == 0)
                    return i;
            }
            return -1;
        }

        private static List<String> topLevelParts(String s) {
            List<String> result = new ArrayList<>();
            int depth = 0, start = 0;
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '(')
                    depth++;
                else if (c == ')')
                    depth--;
                else if (c == ',' && depth == 0) {
                    result.add(s.substring(start, i).trim());
                    start = i + 1;
                }
            }
            if (start < s.length())
                result.add(s.substring(start).trim());
            return result;
        }

    }

    public static class DuckDBColumn extends AbstractTableColumn<DuckDBTable, DuckDBCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;
        private final boolean isSyntheticRowId;

        public DuckDBColumn(String name, DuckDBCompositeDataType columnType, boolean isPrimaryKey, boolean isNullable) {
            this(name, columnType, isPrimaryKey, isNullable, false);
        }

        public DuckDBColumn(String name, DuckDBCompositeDataType columnType, boolean isPrimaryKey, boolean isNullable,
                boolean isSyntheticRowId) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
            this.isSyntheticRowId = isSyntheticRowId;
        }

        @Override
        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

        public boolean isSyntheticRowId() {
            return isSyntheticRowId;
        }

    }

    public static class DuckDBTables extends AbstractTables<DuckDBTable, DuckDBColumn> {

        public DuckDBTables(List<DuckDBTable> tables) {
            super(tables);
        }

    }

    public DuckDBSchema(List<DuckDBTable> databaseTables) {
        super(databaseTables);
    }

    public DuckDBTables getRandomTableNonEmptyTables() {
        return new DuckDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    private static DuckDBCompositeDataType getColumnType(String typeString) {
        return DuckDBCompositeDataType.parse(typeString);
    }

    public static class DuckDBTable extends AbstractRelationalTable<DuckDBColumn, TableIndex, DuckDBGlobalState> {

        public DuckDBTable(String tableName, List<DuckDBColumn> columns, List<TableIndex> indexes, boolean isView) {
            super(tableName, columns, indexes, isView);
        }

    }

    public static DuckDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<DuckDBTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            if (DBMSCommon.matchesIndexName(tableName)) {
                continue; // TODO: unexpected?
            }
            List<DuckDBColumn> databaseColumns;
            try {
                databaseColumns = getTableColumns(con, tableName);
            } catch (SQLException e) {
                // DuckDB can briefly return a just-dropped relation from sqlite_master while
                // DROP ... CASCADE is followed by a schema refresh.
                // Random view generation may persist a view whose deferred definition applies
                // COLLATE to a non-VARCHAR expression. DuckDB accepts the CREATE VIEW but
                // rejects pragma_table_info when it binds the definition. Such a view cannot
                // participate in table-based generation, so omit it from this refresh.
                if (isMissingRelation(e) || isInvalidViewCollation(e, tableName)) {
                    continue;
                }
                throw e;
            }
            boolean isView = matchesViewName(tableName);
            List<TableIndex> indexes = getIndexes(con, tableName);
            DuckDBTable t = new DuckDBTable(tableName, databaseColumns, indexes, isView);
            for (DuckDBColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new DuckDBSchema(databaseTables);
    }

    private static boolean isMissingRelation(SQLException exception) {
        String message = exception.getMessage();
        if (message == null) {
            return false;
        }
        String normalized = message.toLowerCase(Locale.ROOT);
        return normalized.contains("table with name") && normalized.contains("does not exist")
                // A view can remain listed by sqlite_master for one refresh after its
                // underlying table is replaced. Introspecting that stale view then
                // reports its old column bindings instead of a missing relation.
                || normalized.contains("does not have a column named") && normalized.contains("candidate bindings");
    }

    static boolean isInvalidViewCollation(SQLException exception, String tableName) {
        String message = exception.getMessage();
        return matchesViewName(tableName) && message != null
                && message.toLowerCase(Locale.ROOT).contains("collations are only supported for type varchar");
    }

    private static List<TableIndex> getIndexes(SQLConnection con, String tableName) throws SQLException {
        List<TableIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT index_name FROM duckdb_indexes() WHERE database_name = current_database() AND table_name = '%s';",
                    tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    indexes.add(TableIndex.create(indexName));
                }
            }
        }
        return indexes;
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT * FROM sqlite_master WHERE type='table' or type='view'")) {
                while (rs.next()) {
                    tableNames.add(rs.getString("name"));
                }
            }
        }
        return tableNames;
    }

    private static List<DuckDBColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<DuckDBColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format("SELECT * FROM pragma_table_info('%s');", tableName))) {
                while (rs.next()) {
                    String columnName = rs.getString("name");
                    String dataType = rs.getString("type");
                    boolean isNullable = rs.getString("notnull").contentEquals("false");
                    boolean isPrimaryKey = rs.getString("pk").contains("true");
                    DuckDBColumn c = new DuckDBColumn(columnName, getColumnType(dataType), isPrimaryKey, isNullable);
                    columns.add(c);
                }
            }
        }
        if (columns.stream().noneMatch(c -> c.isPrimaryKey()) && !AbstractSchema.matchesViewName(tableName)) {
            // https://github.com/cwida/duckdb/issues/589
            // https://github.com/cwida/duckdb/issues/588
            // TODO: implement an option to enable/disable rowids
            columns.add(new DuckDBColumn("rowid",
                    new DuckDBCompositeDataType.ScalarType(DuckDBDataType.INT, 4, "INTEGER", -1, -1), false, false,
                    true));
        }
        return columns;
    }

}
