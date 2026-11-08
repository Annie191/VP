package sqlancer.databend;

import static sqlancer.databend.DatabendSchema.DatabendDataType.INT;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractRowValue;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendTable;
import sqlancer.databend.ast.DatabendConstant;

public class DatabendSchema extends AbstractSchema<DatabendGlobalState, DatabendTable> {

    public enum DatabendDataType {

        INT, VARCHAR, BOOLEAN, FLOAT, NULL, DATE, TIMESTAMP, TEXT, CHAR, BLOB, FLOATING, DECIMAL, NUMERIC, BOOL,
        DATETIME, ARRAY, MAP, TUPLE, VARIANT, BITMAP, GEOMETRY;

        private static final DatabendDataType[] COLUMN_TYPES = { INT, VARCHAR, BOOLEAN, FLOAT, DATE, TIMESTAMP, DECIMAL,
                ARRAY, MAP, TUPLE, VARIANT, BITMAP, GEOMETRY };

        public boolean isNumeric() {
            return this == INT || this == FLOAT || this == FLOATING || this == DECIMAL || this == NUMERIC
                    || this == BOOLEAN || this == BOOL;
        }

        public static DatabendDataType getRandomWithoutNull() {
            return Randomly.fromOptions(COLUMN_TYPES);
        }

        static List<DatabendDataType> getColumnTypes() {
            return List.of(COLUMN_TYPES);
        }

        public static DatabendDataType getRandomExpressionType() {
            return Randomly.fromOptions(INT, VARCHAR, BOOLEAN, FLOAT, DATE, TIMESTAMP);
        }

        public static DatabendDataType getRandomWithoutNullAndVarchar() {
            return Randomly.fromOptions(DatabendDataType.INT, DatabendDataType.BOOLEAN, DatabendDataType.FLOAT,
                    DatabendDataType.DATE, DatabendDataType.TIMESTAMP);
        }

    }

    public static class DatabendCompositeDataType {

        private final DatabendDataType dataType;

        private final int size;

        private final int scale;

        public DatabendCompositeDataType(DatabendDataType dataType, int size) {
            this(dataType, size, 0);
        }

        private DatabendCompositeDataType(DatabendDataType dataType, int size, int scale) {
            this.dataType = dataType;
            this.size = size;
            this.scale = scale;
        }

        public DatabendCompositeDataType(DatabendDataType dataType) {
            this(dataType,
                    dataType == DatabendDataType.INT ? 8
                            : dataType == DatabendDataType.DECIMAL || dataType == DatabendDataType.NUMERIC ? 38 : 0,
                    dataType == DatabendDataType.DECIMAL || dataType == DatabendDataType.NUMERIC ? 10 : 0);
        }

        public static DatabendCompositeDataType getInt(int size) {
            return new DatabendCompositeDataType(DatabendDataType.INT, size);
        }

        public DatabendDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public int getScale() {
            return scale;
        }

        public static DatabendCompositeDataType getRandomWithoutNull() {
            DatabendDataType type = DatabendDataType.getRandomWithoutNull();
            int size = -1;
            switch (type) {
            case INT:
                size = Randomly.fromOptions(1, 2, 4, 8);
                break;
            case FLOAT:
                size = Randomly.fromOptions(4, 8);
                break;
            case DECIMAL:
                int precision = (int) Randomly.getNotCachedInteger(1, 77);
                int scale = (int) Randomly.getNotCachedInteger(0, precision + 1);
                return new DatabendCompositeDataType(type, precision, scale);
            case BOOLEAN:
            case VARCHAR:
            case DATE:
            case TIMESTAMP:
            case ARRAY:
            case MAP:
            case TUPLE:
            case VARIANT:
            case BITMAP:
            case GEOMETRY:
                size = 0;
                break;
            default:
                throw new AssertionError(type);
            }

            return new DatabendCompositeDataType(type, size);
        }

        @Override
        public String toString() {
            switch (getPrimitiveDataType()) {
            case INT:
                switch (size) {
                case 8:
                    return Randomly.fromOptions("BIGINT", "INT64");
                case 4:
                    return Randomly.fromOptions("INT", "INT32");
                case 2:
                    return Randomly.fromOptions("SMALLINT", "INT16");
                case 1:
                    return Randomly.fromOptions("TINYINT", "INT8");
                default:
                    throw new AssertionError(size);
                }
            case VARCHAR:
                return Randomly.fromOptions("VARCHAR");
            case BLOB:
                return "BINARY";
            case FLOAT:
                switch (size) {
                case 8:
                    return Randomly.fromOptions("DOUBLE");
                case 4:
                    return Randomly.fromOptions("FLOAT");
                default:
                    throw new AssertionError(size);
                }
            case BOOLEAN:
                return Randomly.fromOptions("BOOLEAN", "BOOL");
            case DATE:
                return Randomly.fromOptions("DATE");
            case TIMESTAMP:
                return Randomly.fromOptions("TIMESTAMP", "DATETIME");
            case DECIMAL:
                return String.format("DECIMAL(%d, %d)", size, scale);
            case ARRAY:
                return "ARRAY(INT64)";
            case MAP:
                return "MAP(VARCHAR, INT64)";
            case TUPLE:
                return "TUPLE(INT64, VARCHAR)";
            case VARIANT:
                return "VARIANT";
            case BITMAP:
                return "BITMAP";
            case GEOMETRY:
                return "GEOMETRY";
            case NULL:
                return Randomly.fromOptions("NULL");
            default:
                throw new AssertionError(getPrimitiveDataType());
            }
        }

    }

    public static class DatabendColumn extends AbstractTableColumn<DatabendTable, DatabendCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;

        public DatabendColumn(String name, DatabendCompositeDataType columnType, boolean isPrimaryKey,
                boolean isNullable) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
        }

        public DatabendColumn(String name, DatabendCompositeDataType columnType, boolean isPrimaryKey,
                boolean isNullable, boolean ignored) {
            this(name, columnType, isPrimaryKey, isNullable);
        }

        @Override
        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

    }

    public static class DatabendTables extends AbstractTables<DatabendTable, DatabendColumn> {

        public DatabendTables(List<DatabendTable> tables) {
            super(tables);
        }

        public DatabendRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String rowValueQuery = String.format("SELECT %s FROM %s ORDER BY 1 LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    tableNamesAsString());
            Map<DatabendColumn, DatabendConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet rs = s.executeQuery(rowValueQuery);
                if (!rs.next()) {
                    throw new IgnoreMeException();
                    // throw new AssertionError("could not find random row " + rowValueQuery + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    DatabendColumn column = getColumns().get(i);
                    int columnIndex = rs.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    DatabendConstant constant;
                    if (rs.getString(columnIndex) == null) {
                        constant = DatabendConstant.createNullConstant();
                    } else {
                        switch (column.getType().getPrimitiveDataType()) {
                        case INT:
                            constant = DatabendConstant.createIntConstant(rs.getLong(columnIndex));
                            break;
                        case BOOLEAN:
                            constant = DatabendConstant.createBooleanConstant(rs.getBoolean(columnIndex));
                            break;
                        case VARCHAR:
                            constant = DatabendConstant.createStringConstant(rs.getString(columnIndex));
                            break;
                        case DATE:
                            constant = DatabendConstant.createDateConstant(rs.getLong(columnIndex));
                            break;
                        case TIMESTAMP:
                            constant = DatabendConstant.createTimestampConstant(rs.getLong(columnIndex));
                        default:
                            throw new IgnoreMeException();
                        }
                    }
                    values.put(column, constant);
                }
                assert !rs.next();
                return new DatabendRowValue(this, values);
            } catch (SQLException e) {
                throw new IgnoreMeException();
            }
        }

    }

    public static class DatabendRowValue extends AbstractRowValue<DatabendTables, DatabendColumn, DatabendConstant> {

        DatabendRowValue(DatabendTables tables, Map<DatabendColumn, DatabendConstant> values) {
            super(tables, values);
        }

    }

    public DatabendSchema(List<DatabendTable> databaseTables) {
        super(databaseTables);
    }

    public DatabendTables getRandomTableNonEmptyTables() {
        return new DatabendTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public DatabendTables getRandomTableNonEmptyAndViewTables() {
        List<DatabendTable> tables = getDatabaseTables().stream().filter(t -> !t.isView()).collect(Collectors.toList());
        tables = Randomly.nonEmptySubset(tables);
        return new DatabendTables(tables);
    }

    public static DatabendCompositeDataType getColumnType(String typeString) {
        String normalized = typeString.trim().toUpperCase();
        if (normalized.startsWith("NULLABLE(") && normalized.endsWith(")")) {
            normalized = normalized.substring("NULLABLE(".length(), normalized.length() - 1).trim();
        }
        if (normalized.startsWith("DECIMAL(") || normalized.startsWith("NUMERIC(")) {
            int open = normalized.indexOf('(');
            int comma = normalized.indexOf(',', open);
            int close = normalized.lastIndexOf(')');
            int precision = Integer.parseInt(normalized.substring(open + 1, comma).trim());
            int scale = Integer.parseInt(normalized.substring(comma + 1, close).trim());
            return new DatabendCompositeDataType(DatabendDataType.DECIMAL, precision, scale);
        }
        if (normalized.startsWith("ARRAY(")) {
            return new DatabendCompositeDataType(DatabendDataType.ARRAY);
        }
        if (normalized.startsWith("MAP(")) {
            return new DatabendCompositeDataType(DatabendDataType.MAP);
        }
        if (normalized.startsWith("TUPLE(")) {
            return new DatabendCompositeDataType(DatabendDataType.TUPLE);
        }
        return getColumnTypeNormalCases(normalized);
    }

    private static DatabendCompositeDataType getColumnTypeNormalCases(String typeString) {
        DatabendDataType primitiveType;
        int size = -1;
        switch (typeString.toUpperCase()) {
        case "BOOLEAN":
        case "BOOL":
            primitiveType = DatabendDataType.BOOLEAN;
            size = 1;
            break;
        case "TINYINT":
        case "INT8":
            primitiveType = INT;
            size = 1;
            break;
        case "SMALLINT":
        case "INT16":
            primitiveType = INT;
            size = 2;
            break;
        case "INT":
        case "INT32":
            primitiveType = INT;
            size = 4;
            break;
        case "BIGINT":
        case "INT64":
            primitiveType = INT;
            size = 8;
            break;
        case "FLOAT":
        case "FLOAT32":
            primitiveType = DatabendDataType.FLOAT;
            size = 4;
            break;
        case "DOUBLE":
        case "FLOAT64":
            primitiveType = DatabendDataType.FLOAT;
            size = 8;
            break;
        case "DATE":
            primitiveType = DatabendDataType.DATE;
            break;
        case "TIMESTAMP":
            primitiveType = DatabendDataType.TIMESTAMP;
            break;
        case "BINARY":
            primitiveType = DatabendDataType.BLOB;
            break;
        case "VARCHAR":
        case "STRING":
        case "TEXT":
        case "CHAR":
        case "BLOB":
            primitiveType = DatabendDataType.VARCHAR;
            break;
        case "DECIMAL":
        case "NUMERIC":
            primitiveType = DatabendDataType.DECIMAL;
            size = 38;
            break;
        case "VARIANT":
        case "OBJECT":
            primitiveType = DatabendDataType.VARIANT;
            break;
        case "BITMAP":
            primitiveType = DatabendDataType.BITMAP;
            break;
        case "GEOMETRY":
            primitiveType = DatabendDataType.GEOMETRY;
            break;
        case "DATETIME":
            primitiveType = DatabendDataType.TIMESTAMP;
            break;
        case "NULL":
            primitiveType = DatabendDataType.NULL;
            break;
        case "INTERVAL":
            throw new IgnoreMeException();
        // TODO: caused when a view contains a computation like ((TIMESTAMP '1970-01-05 11:26:57')-(TIMESTAMP
        // '1969-12-29 06:50:27'))
        default:
            throw new AssertionError(typeString);
        }
        return new DatabendCompositeDataType(primitiveType, size);
    }

    public static class DatabendTable extends AbstractRelationalTable<DatabendColumn, TableIndex, DatabendGlobalState> {

        public DatabendTable(String tableName, List<DatabendColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

        public DatabendTable(String tableName, List<DatabendColumn> columns, List<?> indexes, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

    }

    public static DatabendSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<DatabendTable> databaseTables = new ArrayList<>();
        Map<String, Boolean> tableViews = getTableNames(con, databaseName);
        for (Map.Entry<String, Boolean> entry : tableViews.entrySet()) {
            String tableName = entry.getKey();
            List<DatabendColumn> databaseColumns = getTableColumns(con, tableName, databaseName);
            boolean isView = entry.getValue();
            DatabendTable t = new DatabendTable(tableName, databaseColumns, isView);
            for (DatabendColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new DatabendSchema(databaseTables);
    }

    private static Map<String, Boolean> getTableNames(SQLConnection con, String databaseName) throws SQLException {
        Map<String, Boolean> tableNames = new java.util.LinkedHashMap<>();

        final String sqlStatement = String.format(
                "SELECT table_name, table_type FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = '%s' ",
                databaseName);
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(sqlStatement)) {
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    String tableType = rs.getString("table_type");
                    tableNames.put(tableName, tableType != null && "VIEW".equalsIgnoreCase(tableType));
                }
            }
        }
        return tableNames;
    }

    private static List<DatabendColumn> getTableColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<DatabendColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s' and table_name ='%s'",
                    databaseName, tableName))) {
                try { // 没有catch的话rs.next()会报SQLException：Not a navigable ResultSet
                    while (rs.next()) {
                        String columnName = rs.getString("column_name");
                        String dataType = rs.getString("data_type");
                        dataType = dataType == null ? "VARCHAR" : dataType.trim();
                        int nullable = dataType.indexOf('(');
                        if (nullable > 0 && dataType.regionMatches(true, 0, "Nullable", 0, 8)) {
                            dataType = dataType.substring(nullable + 1, dataType.lastIndexOf(')'));
                        }
                        boolean isNullable = "YES".equalsIgnoreCase(rs.getString("is_nullable"))
                                || rs.getBoolean("is_nullable");
                        // boolean isPrimaryKey = rs.getString("pk").contains("true");
                        boolean isPrimaryKey = false; // 没找到主键元数据
                        DatabendColumn c = new DatabendColumn(columnName, getColumnType(dataType), isPrimaryKey,
                                isNullable);
                        columns.add(c);
                    }
                } catch (Exception e) {
                    System.out.println("TableColumns->SQLException：Not a navigable ResultSet");
                }
            }
        }

        return columns;
    }

}
