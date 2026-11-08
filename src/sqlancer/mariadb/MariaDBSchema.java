package sqlancer.mariadb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.TableIndex;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable.MariaDBEngine;

public class MariaDBSchema extends AbstractSchema<MariaDBGlobalState, MariaDBTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    public enum MariaDBDataType {
        INT, VARCHAR, REAL, BOOLEAN, DECIMAL, DATE, DATETIME, TIMESTAMP, JSON, BIT, INET6, UUID, ENUM, SET;
    }

    public static class MariaDBColumn extends AbstractTableColumn<MariaDBTable, MariaDBDataType> {

        private final boolean isPrimaryKey;
        private final int precision;
        private final String columnTypeDefinition;
        private final String characterSet;
        private final String collation;
        private final boolean nullable;
        private final String extra;

        public enum CollateSequence {
            NOCASE, RTRIM, BINARY;

            public static CollateSequence random() {
                return Randomly.fromOptions(values());
            }
        }

        public MariaDBColumn(String name, MariaDBDataType columnType, boolean isPrimaryKey, int precision) {
            this(name, columnType, isPrimaryKey, precision, null, null, null, true, null);
        }

        public MariaDBColumn(String name, MariaDBDataType columnType, boolean isPrimaryKey, int precision,
                String columnTypeDefinition, String characterSet, String collation) {
            this(name, columnType, isPrimaryKey, precision, columnTypeDefinition, characterSet, collation, true, null);
        }

        public MariaDBColumn(String name, MariaDBDataType columnType, boolean isPrimaryKey, int precision,
                String columnTypeDefinition, String characterSet, String collation, boolean nullable) {
            this(name, columnType, isPrimaryKey, precision, columnTypeDefinition, characterSet, collation, nullable,
                    null);
        }

        public MariaDBColumn(String name, MariaDBDataType columnType, boolean isPrimaryKey, int precision,
                String columnTypeDefinition, String characterSet, String collation, boolean nullable, String extra) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.precision = precision;
            this.columnTypeDefinition = columnTypeDefinition;
            this.characterSet = characterSet;
            this.collation = collation;
            this.nullable = nullable;
            this.extra = extra;
        }

        public int getPrecision() {
            return precision;
        }

        public String getColumnTypeDefinition() {
            return columnTypeDefinition;
        }

        public String getCharacterSet() {
            return characterSet;
        }

        public String getCollation() {
            return collation;
        }

        public boolean isNullable() {
            return nullable;
        }

        public String getExtra() {
            return extra;
        }

        @Override
        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public static MariaDBColumn createDummy(String name) {
            return new MariaDBColumn(name, MariaDBDataType.INT, false, 1);
        }

    }

    public static class MariaDBTables {
        private final List<MariaDBTable> tables;
        private final List<MariaDBColumn> columns;

        public MariaDBTables(List<MariaDBTable> tables) {
            this.tables = tables;
            columns = new ArrayList<>();
            for (MariaDBTable t : tables) {
                columns.addAll(t.getColumns());
            }
        }

        public String tableNamesAsString() {
            return tables.stream().map(t -> t.getName()).collect(Collectors.joining(", "));
        }

        public List<MariaDBTable> getTables() {
            return tables;
        }

        public List<MariaDBColumn> getColumns() {
            return columns;
        }

        public String columnNamesAsString() {
            return getColumns().stream().map(t -> t.getTable().getName() + "." + t.getName())
                    .collect(Collectors.joining(", "));
        }

        public String columnNamesAsString(Function<MariaDBColumn, String> function) {
            return getColumns().stream().map(function).collect(Collectors.joining(", "));
        }
    }

    private static MariaDBDataType getColumnType(String typeString, String columnType, boolean jsonAlias) {
        if (jsonAlias) {
            return MariaDBDataType.JSON;
        }
        if ("tinyint".equals(typeString) && columnType != null
                && columnType.toLowerCase(Locale.ROOT).matches("tinyint\\s*\\(\\s*1\\s*\\).*")) {
            // BOOLEAN is a TINYINT(1) alias in MariaDB and information_schema
            // exposes both declarations identically. Preserve the logical
            // boolean domain; explicitly declared TINYINT(1) has the same SQL
            // coercion and comparison semantics.
            return MariaDBDataType.BOOLEAN;
        }
        switch (typeString) {
        case "tinyint":
        case "smallint":
        case "mediumint":
        case "int":
        case "bigint":
            return MariaDBDataType.INT;
        case "binary":
        case "varbinary":
        case "tinyblob":
        case "blob":
        case "mediumblob":
        case "longblob":
        case "varchar":
        case "tinytext":
        case "mediumtext":
        case "text":
        case "longtext":
        case "char":
            return MariaDBDataType.VARCHAR;
        case "float":
        case "real":
        case "double":
            return MariaDBDataType.REAL;
        case "decimal":
        case "numeric":
            return MariaDBDataType.DECIMAL;
        case "date":
            return MariaDBDataType.DATE;
        case "datetime":
            return MariaDBDataType.DATETIME;
        case "timestamp":
            return MariaDBDataType.TIMESTAMP;
        case "json":
            return MariaDBDataType.JSON;
        case "bit":
            return MariaDBDataType.BIT;
        case "inet6":
            return MariaDBDataType.INET6;
        case "uuid":
            return MariaDBDataType.UUID;
        case "enum":
            return MariaDBDataType.ENUM;
        case "set":
            return MariaDBDataType.SET;
        default:
            throw new AssertionError(typeString);
        }
    }

    public static class MariaDBTable extends AbstractRelationalTable<MariaDBColumn, MariaDBIndex, MariaDBGlobalState> {

        public enum MariaDBEngine {

            INNO_DB("InnoDB"), MY_ISAM("MyISAM"), ARIA("Aria");

            private String s;

            MariaDBEngine(String s) {
                this.s = s;
            }

            public String getTextRepresentation() {
                return s;
            }

            public static MariaDBEngine get(String val) {
                return Stream.of(values()).filter(engine -> engine.s.equalsIgnoreCase(val)).findFirst().orElse(null);
            }

            public static MariaDBEngine getRandomEngine() {
                return Randomly.fromOptions(MariaDBEngine.values());
            }

        }

        private final MariaDBEngine engine;

        public MariaDBTable(String tableName, List<MariaDBColumn> columns, List<MariaDBIndex> indexes,
                MariaDBEngine engine) {
            super(tableName, columns, indexes, false);
            this.engine = engine;
        }

        public MariaDBEngine getEngine() {
            return engine;
        }

    }

    public static final class MariaDBIndex extends TableIndex {

        private MariaDBIndex(String indexName) {
            super(indexName);
        }

        @Override
        public String getIndexName() {
            if (super.getIndexName().contentEquals("PRIMARY")) {
                return "`PRIMARY`";
            } else {
                return super.getIndexName();
            }
        }

    }

    public static MariaDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        /* the loop is a workaround for https://bugs.MariaDB.com/bug.php?id=95929 */
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<MariaDBTable> databaseTables = new ArrayList<>();
                try (Statement s = con.createStatement()) {
                    try (ResultSet rs = s.executeQuery(
                            "select TABLE_NAME, ENGINE, TABLE_TYPE from information_schema.TABLES where table_schema = '"
                                    + databaseName + "';")) {
                        while (rs.next()) {
                            // Views, temporary tables, and engines outside the MariaDB generator's
                            // supported set are not relational-table candidates for this provider.
                            String tableType = rs.getString("TABLE_TYPE");
                            // MariaDB exposes system-versioned tables as
                            // SYSTEM VERSIONED rather than BASE TABLE.
                            if (!"BASE TABLE".equalsIgnoreCase(tableType)
                                    && !"SYSTEM VERSIONED".equalsIgnoreCase(tableType)) {
                                continue;
                            }
                            String tableName = rs.getString("TABLE_NAME");
                            String tableEngineStr = rs.getString("ENGINE");
                            MariaDBEngine engine = MariaDBEngine.get(tableEngineStr);
                            if (engine == null) {
                                continue;
                            }
                            List<MariaDBColumn> databaseColumns = getTableColumns(con, tableName, databaseName);
                            List<MariaDBIndex> indexes = getIndexes(con, tableName, databaseName);
                            MariaDBTable t = new MariaDBTable(tableName, databaseColumns, indexes, engine);
                            for (MariaDBColumn c : databaseColumns) {
                                c.setTable(t);
                            }
                            databaseTables.add(t);
                        }
                    }
                }
                return new MariaDBSchema(databaseTables);
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    private static List<MariaDBIndex> getIndexes(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<MariaDBIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s';",
                    databaseName, tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    indexes.add(new MariaDBIndex(indexName));
                }
            }
        }
        return indexes;
    }

    private static List<MariaDBColumn> getTableColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<MariaDBColumn> columns = new ArrayList<>();
        Set<String> jsonColumns = getJsonAliasColumns(con, tableName, databaseName);
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from information_schema.columns where table_schema = '"
                    + databaseName + "' AND TABLE_NAME='" + tableName + "'")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    String columnType = rs.getString("COLUMN_TYPE");
                    int precision = rs.getInt("NUMERIC_PRECISION");
                    boolean isPrimaryKey = rs.getString("COLUMN_KEY").equals("PRI");
                    MariaDBColumn c = new MariaDBColumn(columnName,
                            getColumnType(dataType, columnType, jsonColumns.contains(columnName)), isPrimaryKey,
                            precision, columnType, rs.getString("CHARACTER_SET_NAME"), rs.getString("COLLATION_NAME"),
                            "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")), rs.getString("EXTRA"));
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    private static Set<String> getJsonAliasColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        Set<String> columns = new HashSet<>();
        String sql = "SELECT CHECK_CLAUSE FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='"
                + databaseName.replace("'", "''") + "' AND TABLE_NAME='" + tableName.replace("'", "''") + "'";
        Pattern jsonCheck = Pattern.compile("(?i)\\bjson_valid\\s*\\(\\s*(?:`([^`]+)`|([A-Za-z0-9_$]+))\\s*\\)");
        try (Statement statement = con.createStatement(); ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                Matcher matcher = jsonCheck.matcher(rs.getString(1));
                while (matcher.find()) {
                    columns.add(matcher.group(1) != null ? matcher.group(1) : matcher.group(2));
                }
            }
        }
        return columns;
    }

    public MariaDBSchema(List<MariaDBTable> databaseTables) {
        super(databaseTables);
    }

    public MariaDBTables getRandomTableNonEmptyTables() {
        return new MariaDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

}
