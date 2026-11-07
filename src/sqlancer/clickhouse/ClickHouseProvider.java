package sqlancer.clickhouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.gen.ClickHouseCommon;
import sqlancer.clickhouse.gen.ClickHouseExpressionGenerator;
import sqlancer.clickhouse.gen.ClickHouseInsertGenerator;
import sqlancer.clickhouse.gen.ClickHouseTableGenerator;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
import sqlancer.clickhouse.oracle.ClickHouseVerticalPartitioningOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;

@AutoService(DatabaseProvider.class)
public class ClickHouseProvider extends SQLProviderAdapter<ClickHouseGlobalState, ClickHouseOptions> {
    private static final Object DATABASE_DDL_LOCK = new Object();

    private static final int VP_REPLACEMENT_RETRY_COUNT = 20;

    public ClickHouseProvider() {
        super(ClickHouseGlobalState.class, ClickHouseOptions.class);
    }

    public enum Action implements AbstractAction<ClickHouseGlobalState> {

        INSERT(ClickHouseInsertGenerator::getQuery);

        private final SQLQueryProvider<ClickHouseGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<ClickHouseGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(ClickHouseGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(ClickHouseGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        default:
            throw new AssertionError(a);
        }
    }

    public static class ClickHouseGlobalState extends SQLGlobalState<ClickHouseOptions, ClickHouseSchema> {

        private ClickHouseOptions clickHouseOptions;

        public void setClickHouseOptions(ClickHouseOptions clickHouseOptions) {
            this.clickHouseOptions = clickHouseOptions;
        }

        public ClickHouseOptions getClickHouseOptions() {
            return this.clickHouseOptions;
        }

        public String getOracleName() {
            return String.join("_",
                    this.clickHouseOptions.oracle.stream().map(o -> o.toString()).collect(Collectors.toList()));
        }

        @Override
        public String getDatabaseName() {
            return super.getDatabaseName() + this.getOracleName();
        }

        @Override
        protected ClickHouseSchema readSchema() throws SQLException {
            return ClickHouseSchema.fromConnection(getConnection(), getDatabaseName());
        }
    }

    @Override
    public void generateDatabase(ClickHouseGlobalState globalState) throws Exception {
        if (globalState.getDbmsSpecificOptions().oracle.contains(ClickHouseOracleFactory.ECST)) {
            createVerticalPartitioningBaseTables(globalState, 2);
            return;
        }
        if (usesVerticalPartitioningOracle(globalState)) {
            ClickHouseVerticalPartitioningOracle.initializeStatsLogging(globalState);
            if (globalState.getDbmsSpecificOptions().verticalPartitioningIncludeNormalWorkload) {
                generateRegularDatabase(globalState);
            }
            createVerticalPartitioningBaseTables(globalState, 2);
            return;
        }
        generateRegularDatabase(globalState);
    }

    private void generateRegularDatabase(ClickHouseGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2, 3, 4, 5); i++) {
            boolean success;
            do {
                String tableName = ClickHouseCommon.createTableName(i);
                SQLQueryAdapter qt = ClickHouseTableGenerator.createTableStatement(tableName, globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }

        // TODO: add more Actions to populate table
        StatementExecutor<ClickHouseGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                ClickHouseProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    private static boolean usesVerticalPartitioningOracle(ClickHouseGlobalState globalState) {
        return globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch(o -> o == ClickHouseOracleFactory.VERTICAL_PARTITIONING);
    }

    public static void ensureVerticalPartitioningBaseTables(ClickHouseGlobalState globalState, int minimum)
            throws Exception {
        createVerticalPartitioningBaseTables(globalState, minimum);
    }

    public static void replaceVerticalPartitioningTableAfterSuccessfulCheck(ClickHouseGlobalState globalState,
            String oldTableName) throws Exception {
        if (!createValidVerticalPartitioningBaseTable(globalState)) {
            throw new IgnoreMeException();
        }
        // A normal-workload table can be used as the immutable VP source when that mode is enabled. Only rotate
        // synthetic VP base tables; deleting a generated workload table would silently erase the schema state that
        // this option is intended to preserve.
        if (oldTableName.startsWith("vp_base_")) {
            globalState.executeStatement(
                    new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(oldTableName), true));
        }
        globalState.updateSchema();
    }

    private static void createVerticalPartitioningBaseTables(ClickHouseGlobalState globalState, int minimum)
            throws Exception {
        globalState.updateSchema();
        while (getVerticalPartitioningBaseTables(globalState).size() < minimum) {
            if (!createValidVerticalPartitioningBaseTable(globalState)) {
                throw new IgnoreMeException();
            }
        }
    }

    private static boolean createValidVerticalPartitioningBaseTable(ClickHouseGlobalState globalState)
            throws Exception {
        for (int attempt = 0; attempt < VP_REPLACEMENT_RETRY_COUNT; attempt++) {
            String tableName = "vp_base_" + System.nanoTime();
            List<String> types = createVerticalPartitioningColumnTypes(globalState.getRandomly(),
                    globalState.getDbmsSpecificOptions().verticalPartitioningComplexTypes);
            boolean largeData = globalState.getDbmsSpecificOptions().verticalPartitioningLargeDataExploration;
            boolean nativeRelations = globalState.getDbmsSpecificOptions().verticalPartitioningNativeRelations;
            boolean updateMutations = globalState.getDbmsSpecificOptions().verticalPartitioningUpdateMutations;
            String create = createVerticalPartitioningBaseTableStatement(tableName, types, globalState.getRandomly(),
                    largeData, globalState.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine,
                    nativeRelations, updateMutations);
            if (!globalState.executeStatement(new SQLQueryAdapter(create, true))) {
                continue;
            }
            List<String> inserts = createVerticalPartitioningInsertStatements(tableName, types,
                    globalState.getRandomly(), globalState, largeData);
            boolean insertSuccess = true;
            for (String insert : inserts) {
                if (!globalState.executeStatement(new SQLQueryAdapter(insert))) {
                    insertSuccess = false;
                    break;
                }
            }
            if (!insertSuccess) {
                dropVerticalPartitioningCandidate(globalState, tableName);
                continue;
            }
            if (nativeRelations && largeData && !types.get(0).startsWith("Nested(")
                    && create.contains("ENGINE = MergeTree")) {
                String projectionKey = isComplexVpType(types.get(0)) ? "toString(c0)" : "c0";
                String projection = "ALTER TABLE " + quoteIdentifier(tableName)
                        + " ADD PROJECTION vp_projection (SELECT c0 ORDER BY " + projectionKey + ")";
                if (!globalState.executeStatement(new SQLQueryAdapter(projection, true))) {
                    dropVerticalPartitioningCandidate(globalState, tableName);
                    continue;
                }
                String materializeProjection = "ALTER TABLE " + quoteIdentifier(tableName)
                        + " MATERIALIZE PROJECTION vp_projection SETTINGS mutations_sync = 1";
                if (!globalState.executeStatement(new SQLQueryAdapter(materializeProjection, true))) {
                    dropVerticalPartitioningCandidate(globalState, tableName);
                    continue;
                }
            }
            long rowCountBeforeMutation = -1;
            if (updateMutations && isUpdateMutationEngine(create) && hasUpdateMutationColumn(types)) {
                rowCountBeforeMutation = countRows(globalState, tableName);
            }
            if (updateMutations && isUpdateMutationEngine(create) && hasUpdateMutationColumn(types)
                    && !globalState.executeStatement(
                            new SQLQueryAdapter(createVerticalPartitioningUpdateMutation(tableName, types), true))) {
                dropVerticalPartitioningCandidate(globalState, tableName);
                continue;
            }
            if (rowCountBeforeMutation >= 0 && countRows(globalState, tableName) != rowCountBeforeMutation) {
                dropVerticalPartitioningCandidate(globalState, tableName);
                continue;
            }
            if (globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes
                    && !globalState.executeStatement(new SQLQueryAdapter("ALTER TABLE " + quoteIdentifier(tableName)
                            + " DELETE WHERE cityHash64(toString(c0)) % 2 = 0 SETTINGS mutations_sync = 1"))) {
                dropVerticalPartitioningCandidate(globalState, tableName);
                continue;
            }
            globalState.updateSchema();
            if (isValidVerticalPartitioningBaseTable(globalState, tableName)) {
                return true;
            }
            dropVerticalPartitioningCandidate(globalState, tableName);
        }
        return false;
    }

    private static boolean isValidVerticalPartitioningBaseTable(ClickHouseGlobalState globalState, String tableName)
            throws SQLException {
        ClickHouseTable table = getVerticalPartitioningBaseTables(globalState).stream()
                .filter(t -> t.getName().equals(tableName)).findFirst().orElse(null);
        if (table == null || table.getColumns().isEmpty()) {
            return false;
        }
        String columns = table.getColumns().stream().map(c -> quoteIdentifier(c.getName()))
                .collect(Collectors.joining(", "));
        try {
            try (Statement countStatement = globalState.getConnection().createStatement();
                    ResultSet count = countStatement
                            .executeQuery("SELECT count() FROM " + quoteIdentifier(tableName))) {
                if (!count.next()) {
                    return false;
                }
                if (count.getLong(1) == 0) {
                    return true;
                }
            }
            try (Statement sampleStatement = globalState.getConnection().createStatement();
                    ResultSet sample = sampleStatement
                            .executeQuery("SELECT " + columns + " FROM " + quoteIdentifier(tableName) + " LIMIT 1")) {
                return sample.next();
            }
        } catch (SQLException ignored) {
            return false;
        }
    }

    private static long countRows(ClickHouseGlobalState globalState, String tableName) throws SQLException {
        try (Statement statement = globalState.getConnection().createStatement();
                ResultSet result = statement.executeQuery("SELECT count() FROM " + quoteIdentifier(tableName))) {
            if (!result.next()) {
                throw new SQLException("ClickHouse did not return a row count for " + tableName);
            }
            return result.getLong(1);
        }
    }

    private static void dropVerticalPartitioningCandidate(ClickHouseGlobalState globalState, String tableName)
            throws Exception {
        globalState.executeStatement(new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(tableName), true));
        globalState.updateSchema();
    }

    private static List<String> createVerticalPartitioningColumnTypes(Randomly random, boolean complexTypes) {
        int columnCount = random.getInteger(1, 5);
        List<String> types = new ArrayList<>();
        List<String> scalarTypes = new ArrayList<>(List.of("Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16",
                "UInt32", "UInt64", "Float32", "Float64", "String", "FixedString(8)", "Decimal(12, 2)",
                "LowCardinality(String)", "Nullable(Int32)", "Nullable(Float64)", "Nullable(String)"));
        if (complexTypes) {
            scalarTypes.addAll(List.of("Enum8('red' = 1, 'blue' = 2)", "Enum16('small' = 1, 'large' = 2)",
                    "Array(Int32)", "Array(String)", "Map(String, Int32)", "Tuple(Int32, String)",
                    "Nested(n Int32, s String)", "DateTime64(3)", "Decimal256(10)", "UUID", "IPv4", "IPv6"));
        }
        for (int i = 0; i < columnCount; i++) {
            types.add(Randomly.fromList(scalarTypes));
        }
        return types;
    }

    private static String createVerticalPartitioningBaseTableStatement(String tableName, List<String> types,
            Randomly random, boolean largeData, boolean engineDiversity, boolean nativeRelations,
            boolean updateMutations) {
        String engine = updateMutations ? Randomly.fromOptions("MergeTree", "ReplacingMergeTree")
                : engineDiversity ? Randomly.fromOptions("MergeTree", "Memory", "Log") : "MergeTree";
        if (nativeRelations && largeData && engine.equals("MergeTree") && Randomly.getBoolean()) {
            engine = "ReplacingMergeTree";
        }
        List<String> definitions = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            definitions.add("c" + i + " " + types.get(i));
        }
        // An alias/materialized expression over a Nested parent is expanded as a
        // tuple by ClickHouse and is not a valid scalar schema object. Native
        // relations still exercise Nested subcolumns directly.
        if (nativeRelations && types.size() > 0 && !types.get(0).startsWith("Nested(") && Randomly.getBoolean()) {
            definitions.add("vp_alias ALIAS c0");
            definitions.add("vp_materialized MATERIALIZED c0");
        }
        if ((engine.equals("MergeTree") || engine.equals("ReplacingMergeTree")) && isNumericVpType(types.get(0))
                && Randomly.getBoolean()) {
            definitions.add("INDEX vp_idx_c0 c0 TYPE minmax GRANULARITY 1");
        }
        String engineClause;
        if (engine.equals("MergeTree") || engine.equals("ReplacingMergeTree")) {
            String orderBy = largeData ? "tuple(" + verticalPartitioningOrderKey(types.get(0)) + ")" : "tuple()";
            String partitionBy = nativeRelations && largeData && Randomly.getBoolean()
                    ? " PARTITION BY cityHash64(" + verticalPartitioningPartitionKey(types.get(0)) + ") % 4" : "";
            engineClause = "ENGINE = " + engine + partitionBy + " ORDER BY " + orderBy;
        } else {
            engineClause = "ENGINE = " + engine;
        }
        return "CREATE TABLE " + quoteIdentifier(tableName) + " (" + String.join(", ", definitions) + ") "
                + engineClause;
    }

    private static boolean isUpdateMutationEngine(String createStatement) {
        return createStatement.contains("ENGINE = MergeTree")
                || createStatement.contains("ENGINE = ReplacingMergeTree");
    }

    private static boolean hasUpdateMutationColumn(List<String> types) {
        for (int i = 1; i < types.size(); i++) {
            if (isUpdateMutationType(types.get(i))) {
                return true;
            }
        }
        return false;
    }

    private static String createVerticalPartitioningUpdateMutation(String tableName, List<String> types) {
        int columnIndex = -1;
        for (int i = 1; i < types.size(); i++) {
            if (isUpdateMutationType(types.get(i))) {
                columnIndex = i;
                break;
            }
        }
        if (columnIndex < 0) {
            throw new IllegalArgumentException("No row-preserving VP update mutation column");
        }
        String column = "c" + columnIndex;
        String normalized = unwrapNullableAndLowCardinality(types.get(columnIndex));
        String assignment;
        if (normalized.equals("String")) {
            assignment = "concat(ifNull(" + quoteIdentifier(column) + ", ''), '_vp_mut')";
        } else if (normalized.startsWith("Float")) {
            assignment = "if(isNull(" + quoteIdentifier(column) + "), 0., if(" + quoteIdentifier(column)
                    + " = 0., 1., 0.))";
        } else {
            assignment = "if(isNull(" + quoteIdentifier(column) + "), 0, if(" + quoteIdentifier(column)
                    + " = 0, 1, 0))";
        }
        String predicateColumn = types.get(0).startsWith("Nested(") ? "c0.n" : "c0";
        return "ALTER TABLE " + quoteIdentifier(tableName) + " UPDATE " + quoteIdentifier(column) + " = " + assignment
                + " WHERE cityHash64(toString(" + quoteIdentifier(predicateColumn)
                + ")) % 3 = 0 SETTINGS mutations_sync = 1";
    }

    private static boolean isUpdateMutationType(String type) {
        String normalized = unwrapNullableAndLowCardinality(type);
        return normalized.equals("String") || normalized.startsWith("Int") || normalized.startsWith("UInt")
                || normalized.startsWith("Float");
    }

    private static String unwrapNullableAndLowCardinality(String type) {
        String normalized = type.replace(" ", "");
        boolean unwrapped;
        do {
            unwrapped = false;
            if (normalized.startsWith("Nullable(") || normalized.startsWith("LowCardinality(")) {
                int open = normalized.indexOf('(');
                normalized = normalized.substring(open + 1, normalized.length() - 1);
                unwrapped = true;
            }
        } while (unwrapped);
        return normalized;
    }

    private static String verticalPartitioningOrderKey(String type) {
        if (type.startsWith("Nested(")) {
            return "arrayStringConcat(c0.s, ',')";
        }
        if (isComplexVpType(type)) {
            return "toString(c0)";
        }
        if (!type.startsWith("Nullable(")) {
            return "c0";
        }
        String baseType = type.substring("Nullable(".length(), type.length() - 1);
        if (baseType.startsWith("LowCardinality(")) {
            baseType = baseType.substring("LowCardinality(".length(), baseType.length() - 1);
        }
        if (baseType.startsWith("String") || baseType.startsWith("FixedString")) {
            return "ifNull(c0, '')";
        }
        if (baseType.startsWith("Float") || baseType.startsWith("Decimal")) {
            return "ifNull(c0, 0.)";
        }
        return "ifNull(c0, 0)";
    }

    private static String verticalPartitioningPartitionKey(String type) {
        if (type.startsWith("Nested(")) {
            return "arrayStringConcat(c0.s, ',')";
        }
        return "toString(assumeNotNull(c0))";
    }

    private static List<String> createVerticalPartitioningInsertStatements(String tableName, List<String> types,
            Randomly random, ClickHouseGlobalState globalState, boolean largeData) {
        if (!largeData) {
            return List.of(createSmallVerticalPartitioningInsertStatement(tableName, types, random));
        }
        int rowCount = Randomly.getBooleanWithRatherLowProbability() ? 0 : random.getInteger(10_000, 12_001);
        if (rowCount == 0) {
            return List.of(createEmptyInsertStatement(tableName, types));
        }
        ClickHouseExpressionGenerator generator = new ClickHouseExpressionGenerator(globalState);
        List<String> inserts = new ArrayList<>();
        StringBuilder rows = new StringBuilder();
        for (int row = 0; row < rowCount; row++) {
            if (rows.length() > 0) {
                rows.append(", ");
            }
            rows.append("(");
            for (int column = 0; column < types.size(); column++) {
                if (column > 0) {
                    rows.append(", ");
                }
                rows.append(String.join(", ", randomValuesForType(types.get(column), generator, globalState)));
            }
            rows.append(")");
            if ((row + 1) % 512 == 0 || row + 1 == rowCount) {
                inserts.add("INSERT INTO " + quoteIdentifier(tableName) + " (" + insertColumnNames(types) + ") VALUES "
                        + rows);
                rows.setLength(0);
            }
        }
        return inserts;
    }

    private static String randomValueForType(String type, ClickHouseExpressionGenerator generator,
            ClickHouseGlobalState globalState) {
        if (type.startsWith("Nullable(") && Randomly.getBooleanWithRatherLowProbability()) {
            return "NULL";
        }
        String baseType = type.startsWith("Nullable(") ? type.substring("Nullable(".length(), type.length() - 1) : type;
        String complexValue = randomComplexValue(baseType);
        if (complexValue != null) {
            return complexValue;
        }
        if (baseType.startsWith("LowCardinality(")) {
            baseType = baseType.substring("LowCardinality(".length(), baseType.length() - 1);
        }
        if (baseType.startsWith("Decimal")) {
            return Double.toString(globalState.getRandomly().getInteger(-10_000, 10_001) / 100.0);
        }
        switch (baseType) {
        case "Int8":
            return Integer.toString(globalState.getRandomly().getInteger(-128, 128));
        case "UInt8":
            return Integer.toString(globalState.getRandomly().getInteger(0, 256));
        case "Int16":
            return Integer.toString(globalState.getRandomly().getInteger(-32_768, 32_768));
        case "UInt16":
            return Integer.toString(globalState.getRandomly().getInteger(0, 65_536));
        case "Int32":
            return Integer.toString(globalState.getRandomly().getInteger(Integer.MIN_VALUE, Integer.MAX_VALUE));
        case "UInt32":
            return Long.toString(globalState.getRandomly().getLong(0, 4_294_967_296L));
        case "Int64":
            return Long.toString(globalState.getRandomly().getLong(Long.MIN_VALUE, Long.MAX_VALUE));
        case "UInt64":
            return Long.toUnsignedString(globalState.getRandomly().getLong(0, Long.MAX_VALUE));
        default:
            break;
        }
        if (baseType.startsWith("FixedString")) {
            // FixedString is byte-sized. Pad before truncating so arbitrary generated
            // strings remain valid even when the source value is shorter than 8 bytes.
            String value = ClickHouseToStringVisitor
                    .asString(generator.generateConstant(new ClickHouseSchema.ClickHouseLancerDataType("String")));
            return "toFixedString(substring(concat(" + value + ", '        '), 1, 8), 8)";
        }
        return ClickHouseToStringVisitor
                .asString(generator.generateConstant(new ClickHouseSchema.ClickHouseLancerDataType(baseType)));
    }

    private static String createEmptyInsertStatement(String tableName, List<String> types) {
        return "INSERT INTO " + quoteIdentifier(tableName) + " (" + insertColumnNames(types) + ") SELECT "
                + types.stream().flatMap(type -> emptyValuesForType(type).stream()).collect(Collectors.joining(", "))
                + " FROM numbers(0)";
    }

    private static String createSmallVerticalPartitioningInsertStatement(String tableName, List<String> types,
            Randomly random) {
        int rowCount = random.getInteger(0, 13);
        List<String> rows = new ArrayList<>();
        if (rowCount == 0) {
            return "INSERT INTO "
                    + quoteIdentifier(tableName) + " (" + insertColumnNames(types) + ") SELECT " + types.stream()
                            .flatMap(type -> emptyValuesForType(type).stream()).collect(Collectors.joining(", "))
                    + " FROM numbers(0)";
        }
        for (int row = 0; row < rowCount; row++) {
            List<String> values = new ArrayList<>();
            for (String type : types) {
                values.addAll(createVerticalPartitioningValues(type, row, random));
            }
            rows.add("(" + String.join(", ", values) + ")");
        }
        return "INSERT INTO " + quoteIdentifier(tableName) + " (" + insertColumnNames(types) + ") VALUES "
                + String.join(", ", rows);
    }

    private static String insertColumnNames(List<String> types) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            if (types.get(i).startsWith("Nested(")) {
                names.add("c" + i + ".n");
                names.add("c" + i + ".s");
            } else {
                names.add("c" + i);
            }
        }
        return String.join(", ", names);
    }

    private static List<String> emptyValuesForType(String type) {
        if (type.startsWith("Nested(")) {
            return List.of("defaultValueOfTypeName('Array(Int32)')", "defaultValueOfTypeName('Array(String)')");
        }
        return List.of(emptyValueForType(type));
    }

    private static List<String> randomValuesForType(String type, ClickHouseExpressionGenerator generator,
            ClickHouseGlobalState globalState) {
        if (type.startsWith("Nested(")) {
            if (Randomly.getBooleanWithRatherLowProbability()) {
                return List.of("CAST(array(), 'Array(Int32)')", "CAST(array(), 'Array(String)')");
            } else if (Randomly.getBoolean()) {
                return List.of("CAST(array(0), 'Array(Int32)')", "CAST(array('a'), 'Array(String)')");
            }
            return List.of("CAST(array(-1, 1), 'Array(Int32)')", "CAST(array('a', 'b'), 'Array(String)')");
        }
        return List.of(randomValueForType(type, generator, globalState));
    }

    private static List<String> createVerticalPartitioningValues(String type, int row, Randomly random) {
        if (type.startsWith("Nested(")) {
            if (Randomly.getBooleanWithRatherLowProbability()) {
                return List.of("CAST(array(), 'Array(Int32)')", "CAST(array(), 'Array(String)')");
            } else if (Randomly.getBoolean()) {
                return List.of("CAST(array(0), 'Array(Int32)')", "CAST(array('vp_" + row + "'), 'Array(String)')");
            }
            return List.of("CAST(array(-1, 1), 'Array(Int32)')",
                    "CAST(array('vp_" + row + "', 'vp_alt'), 'Array(String)')");
        }
        return List.of(createVerticalPartitioningValue(type, row, random));
    }

    private static String createVerticalPartitioningValue(String type, int row, Randomly random) {
        if (type.startsWith("Nullable(") && Randomly.getBooleanWithRatherLowProbability()) {
            return "NULL";
        }
        String baseType = type.startsWith("Nullable(") ? type.substring("Nullable(".length(), type.length() - 1) : type;
        String complexValue = randomComplexValue(baseType);
        if (complexValue != null) {
            return complexValue;
        }
        if (baseType.startsWith("LowCardinality(")) {
            baseType = baseType.substring("LowCardinality(".length(), baseType.length() - 1);
        }
        if (baseType.startsWith("FixedString")) {
            return String.format("'vp%06d'", random.getInteger(0, 1_000_000));
        }
        if (baseType.equals("String")) {
            return "'vp_" + row + "_" + random.getInteger(0, 1_000_000) + "'";
        }
        if (baseType.startsWith("Float") || baseType.startsWith("Decimal")) {
            return Double.toString(random.getInteger(-10_000, 10_001) / 10.0);
        }
        switch (baseType) {
        case "Int8":
            return Integer.toString(random.getInteger(-128, 128));
        case "UInt8":
            return Integer.toString(random.getInteger(0, 256));
        case "Int16":
            return Integer.toString(random.getInteger(-32_768, 32_768));
        case "UInt16":
            return Integer.toString(random.getInteger(0, 65_536));
        case "UInt32":
        case "UInt64":
            return Integer.toString(random.getInteger(0, 100_001));
        default:
            return Integer.toString(random.getInteger(-100_000, 100_001));
        }
    }

    private static String randomComplexValue(String type) {
        if (type.startsWith("Enum8")) {
            return Randomly.fromOptions("1", "2");
        }
        if (type.startsWith("Enum16")) {
            return Randomly.fromOptions("1", "2");
        }
        if (type.equals("Array(Int32)")) {
            return Randomly.fromOptions("array()", "array(0)", "array(-1, 0, 1)", "array(1, 2, 3)");
        }
        if (type.equals("Array(String)")) {
            return Randomly.fromOptions("array()", "array('a')", "array('a', 'b')", "array('vp', 'value')");
        }
        if (type.equals("Map(String, Int32)")) {
            return Randomly.fromOptions("map()", "map('a', 0)", "map('a', 1, 'b', -1)");
        }
        if (type.equals("Tuple(Int32, String)")) {
            return Randomly.fromOptions("tuple(0, 'a')", "tuple(-1, '')", "tuple(1, 'tuple')");
        }
        if (type.equals("Nested(n Int32, s String)")) {
            return Randomly.fromOptions("(array(), array())", "(array(0), array('a'))",
                    "(array(-1, 1), array('a', 'b'))");
        }
        if (type.startsWith("DateTime64")) {
            return Randomly.fromOptions("toDateTime64('1970-01-01 00:00:00', 3)",
                    "toDateTime64('2020-02-29 12:34:56.123', 3)", "toDateTime64('2038-01-19 03:14:07.999', 3)");
        }
        if (type.startsWith("Decimal256")) {
            return Randomly.fromOptions("toDecimal256('0', 2)", "toDecimal256('1.25', 2)",
                    "toDecimal256('-99999999.99', 2)");
        }
        if (type.equals("UUID")) {
            return Randomly.fromOptions("toUUID('00000000-0000-0000-0000-000000000000')",
                    "toUUID('123e4567-e89b-12d3-a456-426614174000')");
        }
        if (type.equals("IPv4")) {
            return Randomly.fromOptions("toIPv4('0.0.0.0')", "toIPv4('127.0.0.1')", "toIPv4('255.255.255.255')");
        }
        if (type.equals("IPv6")) {
            return Randomly.fromOptions("toIPv6('::1')", "toIPv6('2001:db8::1')", "toIPv6('::')");
        }
        if (type.startsWith("LowCardinality(Enum8")) {
            return Randomly.fromOptions("'on'", "'off'");
        }
        return null;
    }

    private static String emptyValueForType(String type) {
        // The expression is analyzed even when numbers(0) emits no rows. This
        // function returns a correctly typed default for both Nullable and
        // non-Nullable columns, including FixedString.
        return "defaultValueOfTypeName('" + type.replace("'", "''") + "')";
    }

    private static boolean isNumericVpType(String type) {
        String normalized = type.toUpperCase(java.util.Locale.ROOT);
        if (isComplexVpType(type) || normalized.startsWith("DATETIME") || normalized.equals("UUID")
                || normalized.startsWith("IPV")) {
            return false;
        }
        if (normalized.startsWith("NULLABLE(")) {
            normalized = normalized.substring("NULLABLE(".length(), normalized.length() - 1);
        }
        if (normalized.startsWith("LOWCARDINALITY(")) {
            normalized = normalized.substring("LOWCARDINALITY(".length(), normalized.length() - 1);
        }
        return normalized.startsWith("INT") || normalized.startsWith("UINT") || normalized.startsWith("FLOAT")
                || normalized.startsWith("DECIMAL");
    }

    private static boolean isComplexVpType(String type) {
        String normalized = type.toUpperCase(java.util.Locale.ROOT);
        return normalized.startsWith("ARRAY(") || normalized.startsWith("MAP(") || normalized.startsWith("TUPLE(")
                || normalized.startsWith("NESTED(") || normalized.startsWith("ENUM8(")
                || normalized.startsWith("ENUM16(") || normalized.startsWith("DATETIME64(")
                || normalized.startsWith("DECIMAL256(") || normalized.equals("UUID") || normalized.equals("IPV4")
                || normalized.equals("IPV6") || normalized.startsWith("LOWCARDINALITY(ENUM8(");
    }

    private static java.util.List<ClickHouseTable> getVerticalPartitioningBaseTables(
            ClickHouseGlobalState globalState) {
        return globalState.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> t.getName().startsWith("vp_base_")).collect(java.util.stream.Collectors.toList());
    }

    private static String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    @Override
    public SQLConnection createDatabase(ClickHouseGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = ClickHouseOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = ClickHouseOptions.DEFAULT_PORT;
        }

        ClickHouseOptions clickHouseOptions = globalState.getDbmsSpecificOptions();
        globalState.setClickHouseOptions(clickHouseOptions);
        // Keep response compression disabled for reproducible HTTP transport across ClickHouse versions.
        // The current com.clickhouse JDBC driver correctly decodes the uncompressed RowBinary result stream.
        // Database initialization can contend with delayed ON CLUSTER DDL from other worker threads. The
        // bootstrap connection must have bounded network/query waits; otherwise DROP DATABASE can block for
        // hours when a replica or distributed- DDL queue is unhealthy.
        String url = String.format(
                "jdbc:clickhouse://%s:%d/%s?socket_timeout=30000&connect_timeout=10000&compress=false", host, port,
                "default");
        String databaseName = globalState.getDatabaseName();
        Connection con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        String dropDatabaseCommand = "DROP DATABASE IF EXISTS " + databaseName;
        globalState.getState().logStatement(dropDatabaseCommand);
        String createDatabaseCommand = "CREATE DATABASE IF NOT EXISTS " + databaseName;
        globalState.getState().logStatement(createDatabaseCommand);
        String useDatabaseCommand = "USE " + databaseName; // Noop. To reproduce easier.
        globalState.getState().logStatement(useDatabaseCommand);
        synchronized (DATABASE_DDL_LOCK) {
            try (Statement s = con.createStatement()) {
                s.setQueryTimeout(30);
                s.execute(dropDatabaseCommand);
            } catch (SQLException e) {
                // Preserve the database setup error for SQLancer's normal logging/retry path; never leave a worker
                // blocked indefinitely in initialization.
                try {
                    con.close();
                } catch (SQLException ignored) {
                }
                throw e;
            }
            try (Statement s = con.createStatement()) {
                s.setQueryTimeout(30);
                s.execute(createDatabaseCommand);
            }
        }
        con.close();
        con = DriverManager.getConnection(
                String.format("jdbc:clickhouse://%s:%d/%s?socket_timeout=300000&compress=false%s", host, port,
                        databaseName, clickHouseOptions.enableAnalyzer ? "&allow_experimental_analyzer=1" : ""),
                globalState.getOptions().getUserName(), globalState.getOptions().getPassword());
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "clickhouse";
    }
}
