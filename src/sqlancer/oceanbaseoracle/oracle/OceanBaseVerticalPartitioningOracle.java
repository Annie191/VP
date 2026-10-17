package sqlancer.oceanbaseoracle.oracle;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.oceanbaseoracle.OceanBaseErrors;
import sqlancer.oceanbaseoracle.OceanBaseGlobalState;
import sqlancer.oceanbaseoracle.OceanBaseProvider;
import sqlancer.oceanbaseoracle.OceanBaseSchema.OceanBaseColumn;
import sqlancer.oceanbaseoracle.OceanBaseSchema.OceanBaseDataType;
import sqlancer.oceanbaseoracle.OceanBaseSchema.OceanBaseIndex;
import sqlancer.oceanbaseoracle.OceanBaseSchema.OceanBaseTable;
import sqlancer.oceanbaseoracle.OceanBaseVisitor;
import sqlancer.oceanbaseoracle.ast.OceanBaseAggregate;
import sqlancer.oceanbaseoracle.ast.OceanBaseAggregate.OceanBaseAggregateFunction;
import sqlancer.oceanbaseoracle.ast.OceanBaseColumnReference;
import sqlancer.oceanbaseoracle.ast.OceanBaseConstant;
import sqlancer.oceanbaseoracle.ast.OceanBaseExpression;
import sqlancer.oceanbaseoracle.ast.OceanBaseSelect;
import sqlancer.oceanbaseoracle.ast.OceanBaseTableReference;
import sqlancer.oceanbaseoracle.ast.OceanBaseText;
import sqlancer.oceanbaseoracle.gen.OceanBaseExpressionGenerator;

public class OceanBaseVerticalPartitioningOracle implements TestOracle<OceanBaseGlobalState> {

    private static final AtomicLong ORACLE_RUN_ID = new AtomicLong();
    private static final String ROW_ID = "vp_rowid";
    private static final int MAX_DIFF_ROWS = 8;
    private static final int MAX_RESULT_SAMPLE = 12;
    private static final double FLOAT_ABSOLUTE_TOLERANCE = 1e-7;
    private static final double FLOAT_RELATIVE_TOLERANCE = 1e-6;
    private static final double DOUBLE_ABSOLUTE_TOLERANCE = 1e-13;
    private static final double DOUBLE_RELATIVE_TOLERANCE = 1e-10;
    private static final Object STATS_LOCK = new Object();
    private static final Map<SplitStrategy, Integer> SPLIT_STRATEGY_COUNTS = new EnumMap<>(SplitStrategy.class);
    private static final Map<BaseShape, Integer> SELECTED_BASE_SHAPE_COUNTS = new EnumMap<>(BaseShape.class);
    private static final Map<ReconstructionStrategy, Integer> RECONSTRUCTION_STRATEGY_COUNTS = new EnumMap<>(
            ReconstructionStrategy.class);
    private static final Map<KnownVpBug, Integer> KNOWN_BUG_COUNTS = new EnumMap<>(KnownVpBug.class);
    private static final Map<AdvancedFeature, Integer> ADVANCED_FEATURE_COUNTS = new EnumMap<>(AdvancedFeature.class);
    private static final Map<String, Integer> SKIPPED_ERROR_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SCHEMA_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final AtomicLong KNOWN_BUG_LOG_ID = new AtomicLong();
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean();
    private static final File STATS_FILE = new File(new File(Main.LOG_DIRECTORY, "oceanbase-oracle"), "vp-stats.log");
    private static int completedOracleRuns;
    private static int completedQueryPairs;
    private static int completedAttempts;
    private static int schemaShapeSamples;

    private final OceanBaseGlobalState state;
    private final ExpectedErrors errors = new ExpectedErrors();
    private String lastQueryString;

    private enum BaseShape {
        EMPTY, SINGLE_COLUMN, MULTI_COLUMN
    }

    private enum SplitStrategy {
        EMPTY_SPLIT, SINGLE_REPLICATED, SINGLE_RIGHT_ROW_ID_ONLY, SINGLE_LEFT_ROW_ID_ONLY, EVEN_SPLIT, OVERLAPPED_SPLIT,
        SKEWED_SPLIT, TYPE_DIRECTED_SPLIT
    }

    private enum ReconstructionStrategy {
        JOIN, LEFT_RIGHT_JOIN, FULL_OUTER_SIMULATION, SCALAR_SUBQUERY, EXISTS_FILTER, IN_FILTER, NOT_EXISTS_ANTI,
        DERIVED_TABLE, CTE, VIEW, TEMP_TABLE, AGGREGATE_THEN_MERGE, ROW_NUMBER_ALIGNMENT, JSON_PACK, STRING_PACK,
        NATURAL_OR_USING, STRAIGHT_JOIN, LATERAL_DERIVED
    }

    private enum KnownVpBug {
        COUNT_NULL_EXPR_PUSHDOWN_AGGREGATION, AVG_NULL_EXPR_PUSHDOWN_AGGREGATION_COUNT_DENOMINATOR,
        GROUP_BY_IFNULL_PREDICATE_PROJECTION_DROPS_IFNULL, GROUP_BY_NONDETERMINISTIC_REPRESENTATIVE_ROW
    }

    public enum AdvancedFeature {
        ROLLUP, CORRELATED_SUBQUERY, GROUP_CONCAT, ASYMMETRIC_INDEX, CHARSET_COLLATION_COLUMN, BOUNDARY_MUTATION,
        WINDOW_FUNCTION, UNION_ALL, ADVANCED_AGGREGATE, HELPER_INDEX_ATTEMPTED, HELPER_INDEX_CREATED,
        HELPER_INDEX_SKIPPED
    }

    private static final class VpColumn {
        private final String name;
        private final OceanBaseDataType type;

        private VpColumn(String name, OceanBaseDataType type) {
            this.name = name;
            this.type = type;
        }
    }

    private static final class BaseTableInfo {
        private final String tableName;
        private final List<VpColumn> columns;
        private final BaseShape shape;
        private final String stableOrderStrategy;
        private final long rowCount;
        private final String physicalProperties;

        private BaseTableInfo(String tableName, List<VpColumn> columns, BaseShape shape, String stableOrderStrategy,
                long rowCount, String physicalProperties) {
            this.tableName = tableName;
            this.columns = columns;
            this.shape = shape;
            this.stableOrderStrategy = stableOrderStrategy;
            this.rowCount = rowCount;
            this.physicalProperties = physicalProperties;
        }
    }

    private static final class VpSchema {
        private final String prefix;
        private final String baseTable;
        private final String sourceTable;
        private final String leftTable;
        private final String rightTable;
        private final String viewName;
        private final String tempTable;
        private final List<VpColumn> columns;
        private final List<VpColumn> leftColumns;
        private final List<VpColumn> rightColumns;
        private final List<VpColumn> overlapColumns;
        private final BaseShape baseShape;
        private final SplitStrategy splitStrategy;
        private final boolean usesPhysicalForeignKey;
        private final String stableOrderStrategy;
        private final long baseRowCount;
        private final String basePhysicalProperties;
        private final Map<String, String> columnCollationClauses = new LinkedHashMap<>();
        private String sourcePhysicalProperties;
        private String leftPhysicalProperties;
        private String rightPhysicalProperties;
        private final List<String> setupStatements = new ArrayList<>();

        private VpSchema(String prefix, String baseTable, String sourceTable, String leftTable, String rightTable,
                String viewName, String tempTable, List<VpColumn> columns, List<VpColumn> leftColumns,
                List<VpColumn> rightColumns, List<VpColumn> overlapColumns, BaseShape baseShape,
                SplitStrategy splitStrategy, boolean usesPhysicalForeignKey, String stableOrderStrategy,
                long baseRowCount, String baseEngine) {
            this.prefix = prefix;
            this.baseTable = baseTable;
            this.sourceTable = sourceTable;
            this.leftTable = leftTable;
            this.rightTable = rightTable;
            this.viewName = viewName;
            this.tempTable = tempTable;
            this.columns = columns;
            this.leftColumns = leftColumns;
            this.rightColumns = rightColumns;
            this.overlapColumns = overlapColumns;
            this.baseShape = baseShape;
            this.splitStrategy = splitStrategy;
            this.usesPhysicalForeignKey = usesPhysicalForeignKey;
            this.stableOrderStrategy = stableOrderStrategy;
            this.baseRowCount = baseRowCount;
            this.basePhysicalProperties = baseEngine;
            this.sourcePhysicalProperties = "";
            this.leftPhysicalProperties = "";
            this.rightPhysicalProperties = "";
        }
    }

    private static final class VpQueryPair {
        private final VpSchema schema;
        private final ReconstructionStrategy reconstructionStrategy;
        private final String singleQuery;
        private final String splitQuery;
        private final List<String> extraSetupStatements;
        private final String description;
        private final boolean orderedComparisonEnabled;

        private VpQueryPair(VpSchema schema, ReconstructionStrategy reconstructionStrategy, String singleQuery,
                String splitQuery, List<String> extraSetupStatements, String description,
                boolean orderedComparisonEnabled) {
            this.schema = schema;
            this.reconstructionStrategy = reconstructionStrategy;
            this.singleQuery = singleQuery;
            this.splitQuery = splitQuery;
            this.extraSetupStatements = extraSetupStatements;
            this.description = description;
            this.orderedComparisonEnabled = orderedComparisonEnabled;
        }
    }

    private static final class QueryResultMultiset {
        private final Map<String, Integer> rows;
        private final Map<String, List<ApproximateKind>> approximateColumns;
        private final List<String> orderedRows;
        private final List<List<ApproximateKind>> orderedApproximateColumns;

        private QueryResultMultiset(Map<String, Integer> rows, Map<String, List<ApproximateKind>> approximateColumns,
                List<String> orderedRows, List<List<ApproximateKind>> orderedApproximateColumns) {
            this.rows = rows;
            this.approximateColumns = approximateColumns;
            this.orderedRows = orderedRows;
            this.orderedApproximateColumns = orderedApproximateColumns;
        }
    }

    private static final class FlakyLimitZeroResultException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private FlakyLimitZeroResultException(String message) {
            super(message);
        }
    }

    private enum ApproximateKind {
        NONE(0, 0), FLOAT(FLOAT_ABSOLUTE_TOLERANCE, FLOAT_RELATIVE_TOLERANCE),
        DOUBLE(DOUBLE_ABSOLUTE_TOLERANCE, DOUBLE_RELATIVE_TOLERANCE);

        private final double absoluteTolerance;
        private final double relativeTolerance;

        ApproximateKind(double absoluteTolerance, double relativeTolerance) {
            this.absoluteTolerance = absoluteTolerance;
            this.relativeTolerance = relativeTolerance;
        }

        private boolean isApproximate() {
            return this != NONE;
        }

        private static ApproximateKind looser(ApproximateKind left, ApproximateKind right) {
            if (left == FLOAT || right == FLOAT) {
                return FLOAT;
            }
            if (left == DOUBLE || right == DOUBLE) {
                return DOUBLE;
            }
            return NONE;
        }
    }

    public OceanBaseVerticalPartitioningOracle(OceanBaseGlobalState state) {
        this.state = state;
        registerVpStatsShutdownHook();
        OceanBaseErrors.addExpressionErrors(errors);
        OceanBaseErrors.addInsertErrors(errors);
        errors.add("You have an error in your SQL syntax");
        errors.add("not supported");
        errors.add("doesn't yet support");
        errors.add("Incorrect DECIMAL value");
        errors.add("Truncated incorrect");
        errors.add("Data truncation");
        errors.add("Data too long");
        errors.add("Out of range value");
        errors.add("Division by 0");
        errors.add("Invalid JSON");
        errors.add("Unknown collation");
        errors.add("COLLATION");
        errors.add("Unknown character set");
        errors.add("No matching function");
    }

    @Override
    public void check() throws Exception {
        resetSessionForVerticalPartitioningOracle();
        OceanBaseProvider.ensureVerticalPartitioningBaseTables(state, 2);
        BaseTableInfo baseTable = selectBaseTable();
        int queryVariantsPerCombination = Math.max(1,
                state.getDbmsSpecificOptions().verticalPartitioningQueryVariantsPerCombination);
        int checkedPairs = 0;
        int attempts = 0;
        List<SplitStrategy> splitSchedule = createSplitSchedule(baseTable.shape);
        for (SplitStrategy splitStrategy : splitSchedule) {
            VpSchema vp = materializeTable(baseTable, splitStrategy);
            incrementSplitStrategy(vp.splitStrategy);
            boolean cleanup = true;
            try {
                List<ReconstructionStrategy> reconstructionSchedule = createReconstructionSchedule();
                for (ReconstructionStrategy strategy : reconstructionSchedule) {
                    for (int i = 0; i < queryVariantsPerCombination; i++) {
                        attempts++;
                        try {
                            VpQueryPair pair = generateRandomRewritePair(vp, strategy);
                            incrementReconstructionStrategy(pair.reconstructionStrategy);
                            comparePair(pair);
                            checkedPairs++;
                        } catch (IgnoreMeException ignored) {
                        }
                    }
                }
            } catch (AssertionError e) {
                cleanup = false;
                throw e;
            } finally {
                if (cleanup) {
                    cleanupIfNoBug(vp);
                }
            }
        }
        if (checkedPairs > 0) {
            recordCompletedVpRun(checkedPairs, attempts);
        } else {
            throw new IgnoreMeException();
        }
        OceanBaseProvider.replaceVerticalPartitioningTableAfterSuccessfulCheck(state, baseTable.tableName);
    }

    @Override
    public String getLastQueryString() {
        return lastQueryString;
    }

    private void resetSessionForVerticalPartitioningOracle() throws Exception {
        executeOptional("ALTER SESSION SET TIME_ZONE = '+00:00'");
        executeOptional("ALTER SESSION SET NLS_COMP = BINARY");
        executeOptional("ALTER SESSION SET NLS_SORT = BINARY");
        executeOptional("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'");
    }

    private BaseTableInfo selectBaseTable() throws Exception {
        state.updateSchema();
        List<OceanBaseTable> candidates = state.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> !isVpArtifact(t.getName())).filter(t -> !t.getColumns().isEmpty())
                .filter(t -> t.getColumns().stream().anyMatch(c -> !ROW_ID.equalsIgnoreCase(c.getName())))
                .collect(Collectors.toList());
        Collections.shuffle(candidates);
        for (OceanBaseTable candidate : candidates) {
            BaseTableInfo baseTable = createBaseTableInfo(candidate);
            if (baseTable != null) {
                return baseTable;
            }
        }
        state.updateSchema();
        throw new IgnoreMeException();
    }

    private BaseTableInfo createBaseTableInfo(OceanBaseTable base) throws Exception {
        List<VpColumn> sourceColumns = base.getColumns().stream().filter(c -> !ROW_ID.equalsIgnoreCase(c.getName()))
                .map(c -> new VpColumn(c.getName(), c.getType())).collect(Collectors.toList());
        if (sourceColumns.isEmpty()) {
            return null;
        }
        Long rowCount = getReadableBaseTableRowCount(base.getName(), sourceColumns);
        if (rowCount == null) {
            return null;
        }
        BaseShape shape = rowCount == 0 ? BaseShape.EMPTY
                : sourceColumns.size() == 1 ? BaseShape.SINGLE_COLUMN : BaseShape.MULTI_COLUMN;
        incrementSelectedBaseShape(shape);
        String stableOrder = getStableOrder(base, sourceColumns);
        return new BaseTableInfo(base.getName(), sourceColumns, shape, stableOrder, rowCount,
                getTablePhysicalSignature(base.getName()));
    }

    private Long getReadableBaseTableRowCount(String tableName, List<VpColumn> columns) {
        String projection = columns.stream().map(c -> quote(c.name)).collect(Collectors.joining(", "));
        try (Statement s = state.getConnection().createStatement()) {
            long rowCount;
            try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + quote(tableName))) {
                if (!rs.next()) {
                    return null;
                }
                rowCount = rs.getLong(1);
            }
            try (ResultSet ignored = s
                    .executeQuery("SELECT " + projection + " FROM " + quote(tableName) + " WHERE ROWNUM <= 1")) {
                while (ignored.next()) {
                }
            }
            return rowCount;
        } catch (SQLException ignored) {
            return null;
        }
    }

    private List<SplitStrategy> createSplitSchedule(BaseShape shape) {
        List<SplitStrategy> splitStrategies = new ArrayList<>(compatibleSplitStrategies(shape));
        Collections.shuffle(splitStrategies);
        return splitStrategies;
    }

    private List<SplitStrategy> compatibleSplitStrategies(BaseShape shape) {
        switch (shape) {
        case EMPTY:
            return List.of(SplitStrategy.EMPTY_SPLIT);
        case SINGLE_COLUMN:
            return List.of(SplitStrategy.SINGLE_REPLICATED, SplitStrategy.SINGLE_RIGHT_ROW_ID_ONLY,
                    SplitStrategy.SINGLE_LEFT_ROW_ID_ONLY);
        case MULTI_COLUMN:
            return List.of(SplitStrategy.EVEN_SPLIT, SplitStrategy.OVERLAPPED_SPLIT, SplitStrategy.SKEWED_SPLIT,
                    SplitStrategy.TYPE_DIRECTED_SPLIT);
        default:
            throw new AssertionError(shape);
        }
    }

    private List<ReconstructionStrategy> createReconstructionSchedule() {
        List<ReconstructionStrategy> shuffled = new ArrayList<>(List.of(ReconstructionStrategy.values()));
        Collections.shuffle(shuffled);
        return shuffled;
    }

    private VpSchema materializeTable(BaseTableInfo base, SplitStrategy splitStrategy) throws Exception {
        String prefix = "vp_" + sanitizeIdentifier(state.getDatabaseName()) + "_" + ORACLE_RUN_ID.incrementAndGet()
                + "_";
        String source = prefix + "source";
        String left = prefix + "l";
        String right = prefix + "r";
        String view = prefix + "v";
        String temp = prefix + "joined";
        SplitPlan splitPlan = createSplitPlan(base.columns, base.shape, splitStrategy);
        boolean useForeignKey = Randomly.getBoolean();
        VpSchema vp = new VpSchema(prefix, base.tableName, source, left, right, view, temp, base.columns,
                splitPlan.leftColumns, splitPlan.rightColumns, splitPlan.overlapColumns, base.shape, splitStrategy,
                useForeignKey, base.stableOrderStrategy, base.rowCount, base.physicalProperties);
        initializeColumnCollations(vp);
        dropArtifacts(vp);
        createSourceTable(vp, helperPhysicalPropertiesCandidate(base));
        populateSourceTable(vp, base.stableOrderStrategy);
        copyBaseSchemaObjectsToSource(vp);
        createPartitionTables(vp, helperPhysicalPropertiesCandidate(base));
        populatePartitionTables(vp);
        maybeCreateHelperIndexes(vp);
        if (useForeignKey) {
            tryExecuteSetup(vp,
                    "ALTER TABLE " + quote(vp.rightTable) + " ADD CONSTRAINT " + quote(vp.prefix + "fk_r_l")
                            + " FOREIGN KEY (" + quote(ROW_ID) + ") REFERENCES " + quote(vp.leftTable) + "("
                            + quote(ROW_ID) + ")");
        }
        return vp;
    }

    private static final class SplitPlan {
        private final List<VpColumn> leftColumns;
        private final List<VpColumn> rightColumns;
        private final List<VpColumn> overlapColumns;

        private SplitPlan(List<VpColumn> leftColumns, List<VpColumn> rightColumns, List<VpColumn> overlapColumns) {
            this.leftColumns = leftColumns;
            this.rightColumns = rightColumns;
            this.overlapColumns = overlapColumns;
        }
    }

    private SplitPlan createSplitPlan(List<VpColumn> columns, BaseShape shape, SplitStrategy strategy) {
        List<VpColumn> left = new ArrayList<>();
        List<VpColumn> right = new ArrayList<>();
        List<VpColumn> overlap = new ArrayList<>();
        if (shape == BaseShape.EMPTY) {
            splitEven(columns, left, right);
            return new SplitPlan(left, right, overlap);
        }
        if (shape == BaseShape.SINGLE_COLUMN) {
            VpColumn only = columns.get(0);
            switch (strategy) {
            case SINGLE_REPLICATED:
                left.add(only);
                right.add(only);
                overlap.add(only);
                break;
            case SINGLE_RIGHT_ROW_ID_ONLY:
                left.add(only);
                break;
            case SINGLE_LEFT_ROW_ID_ONLY:
                right.add(only);
                break;
            default:
                throw new AssertionError(strategy);
            }
            return new SplitPlan(left, right, overlap);
        }
        switch (strategy) {
        case EVEN_SPLIT:
            splitEven(columns, left, right);
            break;
        case OVERLAPPED_SPLIT:
            splitEven(columns, left, right);
            VpColumn overlapped = Randomly.fromList(columns);
            if (!containsColumn(left, overlapped.name)) {
                left.add(overlapped);
            }
            if (!containsColumn(right, overlapped.name)) {
                right.add(overlapped);
            }
            overlap.add(overlapped);
            break;
        case SKEWED_SPLIT:
            List<VpColumn> shuffled = new ArrayList<>(columns);
            Collections.shuffle(shuffled);
            left.add(shuffled.get(0));
            right.addAll(shuffled.subList(1, shuffled.size()));
            if (right.isEmpty()) {
                right.add(shuffled.get(0));
                overlap.add(shuffled.get(0));
            }
            break;
        case TYPE_DIRECTED_SPLIT:
            for (VpColumn column : columns) {
                if (column.type.isNumeric()) {
                    left.add(column);
                } else {
                    right.add(column);
                }
            }
            if (left.isEmpty() || right.isEmpty()) {
                left.clear();
                right.clear();
                splitEven(columns, left, right);
            }
            break;
        default:
            throw new AssertionError(strategy);
        }
        return new SplitPlan(left, right, overlap);
    }

    private void splitEven(List<VpColumn> columns, List<VpColumn> left, List<VpColumn> right) {
        List<VpColumn> shuffled = new ArrayList<>(columns);
        Collections.shuffle(shuffled);
        for (int i = 0; i < shuffled.size(); i++) {
            if (i % 2 == 0) {
                left.add(shuffled.get(i));
            } else {
                right.add(shuffled.get(i));
            }
        }
        if (right.isEmpty() && !left.isEmpty()) {
            right.add(left.get(0));
        }
    }

    private boolean containsColumn(List<VpColumn> columns, String name) {
        return columns.stream().anyMatch(c -> c.name.equals(name));
    }

    private String getStableOrder(OceanBaseTable base, List<VpColumn> columns) {
        List<String> primary = base.getColumns().stream().filter(OceanBaseColumn::isPrimaryKey)
                .map(c -> quote(c.getName())).collect(Collectors.toList());
        if (!primary.isEmpty()) {
            return String.join(", ", primary);
        }
        List<String> comparable = columns.stream()
                .filter(c -> c.type != OceanBaseDataType.FLOAT && c.type != OceanBaseDataType.DOUBLE)
                .map(c -> quote(c.name)).collect(Collectors.toList());
        if (!comparable.isEmpty()) {
            return String.join(", ", comparable);
        }
        return null;
    }

    private String helperPhysicalPropertiesCandidate(BaseTableInfo base) {
        if (state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine && base.physicalProperties != null
                && !base.physicalProperties.isEmpty()) {
            return base.physicalProperties;
        }
        return "";
    }

    private void createSourceTable(VpSchema vp, String preferredPhysicalProperties) throws Exception {
        vp.sourcePhysicalProperties = createTableWithPhysicalPropertyFallback(vp, vp.sourceTable, sourceColumnDefs(vp),
                preferredPhysicalProperties);
    }

    private void initializeColumnCollations(VpSchema vp) {
        for (VpColumn column : vp.columns) {
            String clause = "";
            if (deepExplorationEnabled() && column.type == OceanBaseDataType.VARCHAR && Randomly.getBoolean()) {
                clause = " COLLATE BINARY";
                recordAdvancedFeature(AdvancedFeature.CHARSET_COLLATION_COLUMN);
            }
            vp.columnCollationClauses.put(column.name, clause);
        }
    }

    private List<String> sourceColumnDefs(VpSchema vp) {
        List<String> columnDefs = new ArrayList<>();
        columnDefs.add(quote(ROW_ID) + " NUMBER(19) NOT NULL PRIMARY KEY");
        for (VpColumn column : vp.columns) {
            columnDefs.add(columnDef(vp, column));
        }
        return columnDefs;
    }

    private void populateSourceTable(VpSchema vp, String stableOrder) throws Exception {
        try {
            executeSetup(vp, populateSourceSQL(vp, stableOrder));
        } catch (AssertionError e) {
            if (!canFallbackHelperPhysicalProperties(vp.sourcePhysicalProperties)) {
                throw e;
            }
            vp.sourcePhysicalProperties = recreateHelperTableWithoutPhysicalProperties(vp, vp.sourceTable,
                    sourceColumnDefs(vp));
            executeSetup(vp, populateSourceSQL(vp, stableOrder));
        }
    }

    private String populateSourceSQL(VpSchema vp, String stableOrder) {
        String columnList = vp.columns.stream().map(c -> quote(c.name)).collect(Collectors.joining(", "));
        String targetColumns = quote(ROW_ID) + (columnList.isEmpty() ? "" : ", " + columnList);
        String sourceOrder = stableOrder != null ? stableOrder : "1";
        String sql = "INSERT INTO " + quote(vp.sourceTable) + " (" + targetColumns + ") SELECT " + targetColumns
                + " FROM (SELECT ROW_NUMBER() OVER (ORDER BY " + sourceOrder + ") AS " + quote(ROW_ID)
                + (columnList.isEmpty() ? "" : ", " + columnList) + " FROM " + quote(vp.baseTable) + ")";
        if (state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows > 0) {
            sql += " WHERE ROWNUM <= " + state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows;
        }
        return sql;
    }

    private void createPartitionTables(VpSchema vp, String preferredPhysicalProperties) throws Exception {
        vp.leftPhysicalProperties = createPartitionTable(vp, vp.leftTable, vp.leftColumns, preferredPhysicalProperties);
        vp.rightPhysicalProperties = createPartitionTable(vp, vp.rightTable, vp.rightColumns,
                preferredPhysicalProperties);
    }

    private String createPartitionTable(VpSchema vp, String tableName, List<VpColumn> columns,
            String preferredPhysicalProperties) throws Exception {
        return createTableWithPhysicalPropertyFallback(vp, tableName, partitionColumnDefs(vp, columns),
                preferredPhysicalProperties);
    }

    private List<String> partitionColumnDefs(VpSchema vp, List<VpColumn> columns) {
        List<String> defs = new ArrayList<>();
        defs.add(quote(ROW_ID) + " NUMBER(19) NOT NULL PRIMARY KEY");
        for (VpColumn column : columns) {
            defs.add(columnDef(vp, column));
        }
        return defs;
    }

    private String columnDef(VpSchema vp, VpColumn column) {
        return quote(column.name) + " " + toSQLType(column.type)
                + vp.columnCollationClauses.getOrDefault(column.name, "") + " NULL";
    }

    private String plainColumnDef(VpColumn column) {
        return quote(column.name) + " " + toSQLType(column.type) + " NULL";
    }

    private String createTableWithPhysicalPropertyFallback(VpSchema vp, String tableName, List<String> columnDefs,
            String preferredPhysicalProperties) throws Exception {
        try {
            executeSetup(vp, createTableSQL(tableName, columnDefs, preferredPhysicalProperties));
            return preferredPhysicalProperties;
        } catch (AssertionError e) {
            if (deepExplorationEnabled() && containsCharsetCollation(columnDefs)) {
                List<String> plainDefs = plainColumnDefs(tableName, vp);
                try {
                    executeSetup(vp, createTableSQL(tableName, plainDefs, preferredPhysicalProperties));
                    return preferredPhysicalProperties;
                } catch (AssertionError ignored) {
                    columnDefs = plainDefs;
                }
            }
            if (!canFallbackHelperPhysicalProperties(preferredPhysicalProperties)) {
                throw e;
            }
            return recreateHelperTableWithoutPhysicalProperties(vp, tableName, columnDefs);
        }
    }

    private boolean containsCharsetCollation(List<String> columnDefs) {
        return columnDefs.stream().anyMatch(def -> def.toUpperCase(Locale.ROOT).contains(" COLLATE "));
    }

    private List<String> plainColumnDefs(String tableName, VpSchema vp) {
        if (tableName.equals(vp.sourceTable)) {
            List<String> defs = new ArrayList<>();
            defs.add(quote(ROW_ID) + " NUMBER(19) NOT NULL PRIMARY KEY");
            vp.columns.stream().map(this::plainColumnDef).forEach(defs::add);
            return defs;
        }
        List<VpColumn> columns = tableName.equals(vp.leftTable) ? vp.leftColumns : vp.rightColumns;
        List<String> defs = new ArrayList<>();
        defs.add(quote(ROW_ID) + " NUMBER(19) NOT NULL PRIMARY KEY");
        columns.stream().map(this::plainColumnDef).forEach(defs::add);
        return defs;
    }

    private String createTableSQL(String tableName, List<String> columnDefs, String physicalProperties) {
        String physicalClause = state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine
                ? physicalProperties : "";
        return "CREATE TABLE " + quote(tableName) + " (" + String.join(", ", columnDefs) + ")"
                + (physicalClause == null || physicalClause.isEmpty() ? "" : " " + physicalClause);
    }

    private boolean canFallbackHelperPhysicalProperties(String physicalProperties) {
        return physicalProperties != null && !physicalProperties.isEmpty();
    }

    private String recreateHelperTableWithoutPhysicalProperties(VpSchema vp, String tableName, List<String> columnDefs)
            throws Exception {
        executeOptional("DROP TABLE " + quote(tableName));
        executeSetup(vp, createTableSQL(tableName, columnDefs, ""));
        return "";
    }

    private void populatePartitionTables(VpSchema vp) throws Exception {
        vp.leftPhysicalProperties = populatePartitionTable(vp, vp.leftTable, vp.leftColumns, vp.leftPhysicalProperties);
        vp.rightPhysicalProperties = populatePartitionTable(vp, vp.rightTable, vp.rightColumns,
                vp.rightPhysicalProperties);
    }

    private String populatePartitionTable(VpSchema vp, String tableName, List<VpColumn> columns,
            String physicalProperties) throws Exception {
        try {
            executeSetup(vp, populatePartitionSQL(tableName, columns, vp.sourceTable));
            return physicalProperties;
        } catch (AssertionError e) {
            if (!canFallbackHelperPhysicalProperties(physicalProperties)) {
                throw e;
            }
            String fallbackPhysicalProperties = recreateHelperTableWithoutPhysicalProperties(vp, tableName,
                    partitionColumnDefs(vp, columns));
            executeSetup(vp, populatePartitionSQL(tableName, columns, vp.sourceTable));
            return fallbackPhysicalProperties;
        }
    }

    private String populatePartitionSQL(String partitionTable, List<VpColumn> columns, String sourceTable) {
        List<String> names = new ArrayList<>();
        names.add(quote(ROW_ID));
        names.addAll(columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        return "INSERT INTO " + quote(partitionTable) + " (" + String.join(", ", names) + ") SELECT "
                + String.join(", ", names) + " FROM " + quote(sourceTable);
    }

    private void maybeCreateHelperIndexes(VpSchema vp) throws Exception {
        if (!advancedFeaturesEnabled()) {
            return;
        }
        maybeCreateAsymmetricHelperIndexes(vp);
        maybeCreateHelperIndex(vp, vp.sourceTable, vp.columns, "src");
        maybeCreateHelperIndex(vp, vp.leftTable, vp.leftColumns, "l");
        maybeCreateHelperIndex(vp, vp.rightTable, vp.rightColumns, "r");
    }

    private void maybeCreateAsymmetricHelperIndexes(VpSchema vp) {
        List<VpColumn> leftIndexable = helperIndexableColumns(vp.leftColumns);
        List<VpColumn> rightIndexable = helperIndexableColumns(vp.rightColumns);
        List<VpColumn> sourceIndexable = helperIndexableColumns(vp.columns);
        if (leftIndexable.isEmpty() && rightIndexable.isEmpty() && sourceIndexable.isEmpty()) {
            return;
        }
        recordAdvancedFeature(AdvancedFeature.ASYMMETRIC_INDEX);
        if (!sourceIndexable.isEmpty() && Randomly.getBoolean()) {
            VpColumn c = Randomly.fromList(sourceIndexable);
            executeOptionalHelperIndexSetup(vp,
                    "CREATE INDEX " + quote(vp.prefix + "src_asym_" + Math.abs(state.getRandomly().getInteger()))
                            + " ON " + quote(vp.sourceTable) + "(" + quote(c.name) + ", " + quote(ROW_ID) + ")");
        }
        if (!leftIndexable.isEmpty() && Randomly.getBoolean()) {
            VpColumn c = Randomly.fromList(leftIndexable);
            executeOptionalHelperIndexSetup(vp,
                    "CREATE INDEX " + quote(vp.prefix + "l_asym_" + Math.abs(state.getRandomly().getInteger())) + " ON "
                            + quote(vp.leftTable) + "(" + quote(ROW_ID) + ", " + quote(c.name) + ")");
        }
        if (!rightIndexable.isEmpty() && Randomly.getBoolean()) {
            VpColumn c = Randomly.fromList(rightIndexable);
            executeOptionalHelperIndexSetup(vp,
                    "CREATE INDEX " + quote(vp.prefix + "r_asym_" + Math.abs(state.getRandomly().getInteger())) + " ON "
                            + quote(vp.rightTable) + "(" + quote(c.name) + ", " + quote(ROW_ID) + ")");
        }
    }

    private List<VpColumn> helperIndexableColumns(List<VpColumn> columns) {
        return columns.stream().filter(c -> c.type != OceanBaseDataType.VARCHAR).collect(Collectors.toList());
    }

    private void maybeCreateHelperIndex(VpSchema vp, String tableName, List<VpColumn> columns, String suffix)
            throws Exception {
        List<VpColumn> indexable = helperIndexableColumns(columns);
        if (indexable.isEmpty() || !Randomly.getBoolean()) {
            return;
        }
        Collections.shuffle(indexable);
        List<String> indexColumns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            indexColumns.add(quote(ROW_ID));
        }
        indexColumns
                .addAll(indexable.stream().limit(state.getRandomly().getInteger(1, Math.min(2, indexable.size()) + 1))
                        .map(c -> quote(c.name)).collect(Collectors.toList()));
        if (!indexColumns.contains(quote(ROW_ID)) && Randomly.getBoolean()) {
            indexColumns.add(quote(ROW_ID));
        }
        String indexName = quote(vp.prefix + "idx_" + suffix + "_" + Math.abs(state.getRandomly().getInteger()));
        executeOptionalHelperIndexSetup(vp,
                "CREATE INDEX " + indexName + " ON " + quote(tableName) + "(" + String.join(", ", indexColumns) + ")");
    }

    private void executeOptionalHelperIndexSetup(VpSchema vp, String sql) {
        recordAdvancedFeature(AdvancedFeature.HELPER_INDEX_ATTEMPTED);
        try {
            executeSetup(vp, sql);
            recordAdvancedFeature(AdvancedFeature.HELPER_INDEX_CREATED);
        } catch (AssertionError ignored) {
            recordAdvancedFeature(AdvancedFeature.HELPER_INDEX_SKIPPED);
            vp.setupStatements.add("-- skipped helper index: " + sql);
        } catch (Exception ignored) {
            recordAdvancedFeature(AdvancedFeature.HELPER_INDEX_SKIPPED);
            vp.setupStatements.add("-- skipped helper index: " + sql);
        }
    }

    private void copyBaseSchemaObjectsToSource(VpSchema vp) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSourceCopySchemaObjects) {
            return;
        }
        copyBaseIndexesToSource(vp);
        copyBaseConstraintsToSource(vp);
    }

    private void copyBaseIndexesToSource(VpSchema vp) {
        String sql = "SELECT index_name, uniqueness FROM user_indexes WHERE table_name = '" + sqlString(vp.baseTable)
                + "' AND index_type = 'NORMAL'";
        int copied = 0;
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                List<String> columns = getIndexColumns(vp.baseTable, indexName);
                if (columns.isEmpty() || !sourceHasColumns(vp, columns)) {
                    continue;
                }
                String targetName = vp.prefix + "src_idx_" + copied++;
                String unique = "UNIQUE".equalsIgnoreCase(rs.getString("UNIQUENESS")) ? "UNIQUE " : "";
                String ddl = "CREATE " + unique + "INDEX " + quote(targetName) + " ON " + quote(vp.sourceTable) + " ("
                        + columns.stream().map(OceanBaseVerticalPartitioningOracle::quote)
                                .collect(Collectors.joining(", "))
                        + ")";
                tryExecuteSetup(vp, ddl);
            }
        } catch (SQLException ignored) {
        }
    }

    private void copyBaseConstraintsToSource(VpSchema vp) {
        String sql = "SELECT constraint_name, constraint_type, search_condition FROM user_constraints WHERE table_name = '"
                + sqlString(vp.baseTable) + "' AND constraint_type IN ('P', 'U', 'C')";
        int copied = 0;
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                String constraintName = rs.getString("CONSTRAINT_NAME");
                String constraintType = rs.getString("CONSTRAINT_TYPE");
                String targetName = vp.prefix + "src_cons_" + copied++;
                if ("C".equalsIgnoreCase(constraintType)) {
                    String condition = rs.getString("SEARCH_CONDITION");
                    if (condition == null || condition.isBlank()) {
                        continue;
                    }
                    tryExecuteSetup(vp, "ALTER TABLE " + quote(vp.sourceTable) + " ADD CONSTRAINT " + quote(targetName)
                            + " CHECK (" + condition + ")");
                } else {
                    List<String> columns = getConstraintColumns(vp.baseTable, constraintName);
                    if (columns.isEmpty() || !sourceHasColumns(vp, columns)) {
                        continue;
                    }
                    tryExecuteSetup(vp,
                            "ALTER TABLE " + quote(vp.sourceTable) + " ADD CONSTRAINT " + quote(targetName)
                                    + " UNIQUE (" + columns.stream().map(OceanBaseVerticalPartitioningOracle::quote)
                                            .collect(Collectors.joining(", "))
                                    + ")");
                }
            }
        } catch (SQLException ignored) {
        }
    }

    private List<String> getIndexColumns(String tableName, String indexName) {
        String sql = "SELECT column_name FROM user_ind_columns WHERE table_name = '" + sqlString(tableName)
                + "' AND index_name = '" + sqlString(indexName) + "' ORDER BY column_position";
        return getColumnNameList(sql);
    }

    private List<String> getConstraintColumns(String tableName, String constraintName) {
        String sql = "SELECT column_name FROM user_cons_columns WHERE table_name = '" + sqlString(tableName)
                + "' AND constraint_name = '" + sqlString(constraintName) + "' ORDER BY position";
        return getColumnNameList(sql);
    }

    private List<String> getColumnNameList(String sql) {
        List<String> columns = new ArrayList<>();
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                columns.add(rs.getString(1));
            }
        } catch (SQLException ignored) {
            return List.of();
        }
        return columns;
    }

    private boolean sourceHasColumns(VpSchema vp, List<String> columns) {
        return columns.stream().allMatch(column -> ROW_ID.equalsIgnoreCase(column)
                || vp.columns.stream().anyMatch(sourceColumn -> sourceColumn.name.equalsIgnoreCase(column)));
    }

    private void dropArtifacts(VpSchema vp) {
        executeOptional("DROP VIEW " + quote(vp.viewName));
        executeOptional("DROP TABLE " + quote(vp.tempTable));
        executeOptional("DROP TABLE " + quote(vp.leftTable));
        executeOptional("DROP TABLE " + quote(vp.rightTable));
        executeOptional("DROP TABLE " + quote(vp.sourceTable));
    }

    private void cleanupIfNoBug(VpSchema vp) {
        dropArtifacts(vp);
    }

    private String buildReconstructionRelation(VpSchema vp, ReconstructionStrategy strategy, List<String> extraSetup)
            throws Exception {
        switch (strategy) {
        case JOIN:
            return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false), " FROM " + quote(vp.leftTable)
                    + " l JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID));
        case LEFT_RIGHT_JOIN:
            if (Randomly.getBoolean()) {
                return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                        " FROM " + quote(vp.leftTable) + " l LEFT JOIN " + quote(vp.rightTable) + " r ON l."
                                + quote(ROW_ID) + " = r." + quote(ROW_ID));
            }
            return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false), " FROM " + quote(vp.leftTable)
                    + " l RIGHT JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID));
        case FULL_OUTER_SIMULATION:
            return "(" + "SELECT " + joinProjection(vp, "l", "r", false) + " FROM " + quote(vp.leftTable)
                    + " l LEFT JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID)
                    + " UNION ALL SELECT " + joinProjection(vp, "l", "r", true) + " FROM " + quote(vp.leftTable)
                    + " l RIGHT JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID)
                    + " WHERE l." + quote(ROW_ID) + " IS NULL) " + quote(vp.sourceTable);
        case SCALAR_SUBQUERY:
            return scalarSubqueryRelation(vp);
        case EXISTS_FILTER:
            return reconstructedSubquery(vp, leftProjectionWithRightLookups(vp),
                    " FROM " + quote(vp.leftTable) + " l WHERE EXISTS (SELECT 1 FROM " + quote(vp.rightTable)
                            + " r WHERE r." + quote(ROW_ID) + " = l." + quote(ROW_ID) + ")");
        case IN_FILTER:
            return reconstructedSubquery(vp, leftProjectionWithRightLookups(vp),
                    " FROM " + quote(vp.leftTable) + " l WHERE l." + quote(ROW_ID) + " IN (SELECT r." + quote(ROW_ID)
                            + " FROM " + quote(vp.rightTable) + " r)");
        case NOT_EXISTS_ANTI:
            return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                    " FROM " + quote(vp.leftTable) + " l JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID)
                            + " = r." + quote(ROW_ID) + " WHERE NOT EXISTS (SELECT 1 FROM " + quote(vp.rightTable)
                            + " rx WHERE rx." + quote(ROW_ID) + " = l." + quote(ROW_ID) + " AND rx." + quote(ROW_ID)
                            + " <> l." + quote(ROW_ID) + ")");
        case DERIVED_TABLE:
            return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                    " FROM (SELECT * FROM " + quote(vp.leftTable) + ") l JOIN (SELECT * FROM " + quote(vp.rightTable)
                            + ") r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID));
        case CTE:
            return cteRelation(vp);
        case VIEW:
            extraSetup.add("DROP VIEW " + quote(vp.viewName));
            extraSetup.add("CREATE VIEW " + quote(vp.viewName) + " AS SELECT " + joinProjection(vp, "l", "r", false)
                    + " FROM " + quote(vp.leftTable) + " l JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID)
                    + " = r." + quote(ROW_ID));
            return quote(vp.viewName) + " " + quote(vp.sourceTable);
        case TEMP_TABLE:
            extraSetup.add("DROP TABLE " + quote(vp.tempTable));
            extraSetup.add(createTemporaryReconstructionTableSQL(vp));
            extraSetup.add("INSERT INTO " + quote(vp.tempTable) + " (" + sourceColumnList(vp) + ") SELECT "
                    + joinProjection(vp, "l", "r", false) + " FROM " + quote(vp.leftTable) + " l JOIN "
                    + quote(vp.rightTable) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID));
            return quote(vp.tempTable) + " " + quote(vp.sourceTable);
        case AGGREGATE_THEN_MERGE:
            return aggregateThenMergeRelation(vp);
        case ROW_NUMBER_ALIGNMENT:
            return rowNumberRelation(vp);
        case JSON_PACK:
            return jsonPackRelation(vp);
        case STRING_PACK:
            return stringPackRelation(vp);
        case NATURAL_OR_USING:
            return reconstructedSubquery(vp, usingProjection(vp), " FROM " + quote(vp.leftTable) + " JOIN "
                    + quote(vp.rightTable) + " USING (" + quote(ROW_ID) + ")");
        case STRAIGHT_JOIN:
            return reconstructedSubquery(vp, "/*+ ORDERED */ " + joinProjection(vp, "l", "r", false),
                    " FROM " + quote(vp.leftTable) + " l JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID)
                            + " = r." + quote(ROW_ID));
        case LATERAL_DERIVED:
            return lateralRelation(vp);
        default:
            throw new AssertionError(strategy);
        }
    }

    private String reconstructedSubquery(VpSchema vp, String projection, String fromAndWhere) {
        return "(SELECT " + projection + fromAndWhere + ") " + quote(vp.sourceTable);
    }

    private String createTemporaryReconstructionTableSQL(VpSchema vp) {
        List<String> defs = new ArrayList<>();
        defs.add(quote(ROW_ID) + " NUMBER(19) NOT NULL PRIMARY KEY");
        for (VpColumn column : vp.columns) {
            defs.add(columnDef(vp, column));
        }
        return "CREATE TABLE " + quote(vp.tempTable) + " (" + String.join(", ", defs) + ")";
    }

    private String sourceColumnList(VpSchema vp) {
        List<String> names = new ArrayList<>();
        names.add(quote(ROW_ID));
        names.addAll(vp.columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        return String.join(", ", names);
    }

    private String joinProjection(VpSchema vp, String leftAlias, String rightAlias, boolean rightBiasedRowId) {
        List<String> parts = new ArrayList<>();
        parts.add((rightBiasedRowId ? rightAlias : leftAlias) + "." + quote(ROW_ID) + " AS " + quote(ROW_ID));
        for (VpColumn column : vp.columns) {
            String alias = columnAlias(vp, column, leftAlias, rightAlias);
            parts.add(alias + "." + quote(column.name) + " AS " + quote(column.name));
        }
        return String.join(", ", parts);
    }

    private String usingProjection(VpSchema vp) {
        List<String> parts = new ArrayList<>();
        parts.add(quote(ROW_ID));
        for (VpColumn column : vp.columns) {
            String table = containsColumn(vp.leftColumns, column.name) ? vp.leftTable : vp.rightTable;
            parts.add(quote(table) + "." + quote(column.name) + " AS " + quote(column.name));
        }
        return String.join(", ", parts);
    }

    private String columnAlias(VpSchema vp, VpColumn column, String leftAlias, String rightAlias) {
        boolean inLeft = containsColumn(vp.leftColumns, column.name);
        boolean inRight = containsColumn(vp.rightColumns, column.name);
        if (inLeft && inRight) {
            return Randomly.getBoolean() ? leftAlias : rightAlias;
        }
        return inLeft ? leftAlias : rightAlias;
    }

    private String leftProjectionWithRightLookups(VpSchema vp) {
        List<String> parts = new ArrayList<>();
        parts.add("l." + quote(ROW_ID) + " AS " + quote(ROW_ID));
        for (VpColumn column : vp.columns) {
            if (containsColumn(vp.leftColumns, column.name)) {
                parts.add("l." + quote(column.name) + " AS " + quote(column.name));
            } else {
                parts.add("(SELECT r." + quote(column.name) + " FROM " + quote(vp.rightTable) + " r WHERE r."
                        + quote(ROW_ID) + " = l." + quote(ROW_ID) + ") AS " + quote(column.name));
            }
        }
        return String.join(", ", parts);
    }

    private String scalarSubqueryRelation(VpSchema vp) {
        return reconstructedSubquery(vp, leftProjectionWithRightLookups(vp), " FROM " + quote(vp.leftTable) + " l");
    }

    private String cteRelation(VpSchema vp) {
        return "(WITH l AS (SELECT * FROM " + quote(vp.leftTable) + "), r AS (SELECT * FROM " + quote(vp.rightTable)
                + ") SELECT " + joinProjection(vp, "l", "r", false) + " FROM l JOIN r ON l." + quote(ROW_ID) + " = r."
                + quote(ROW_ID) + ") " + quote(vp.sourceTable);
    }

    private String aggregateThenMergeRelation(VpSchema vp) {
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN (SELECT " + quote(ROW_ID) + ", COUNT(*) AS vp_count FROM "
                        + quote(vp.rightTable) + " GROUP BY " + quote(ROW_ID) + ") rg ON l." + quote(ROW_ID) + " = rg."
                        + quote(ROW_ID) + " JOIN " + quote(vp.rightTable) + " r ON r." + quote(ROW_ID) + " = rg."
                        + quote(ROW_ID));
    }

    private String rowNumberRelation(VpSchema vp) {
        return "(WITH l AS (SELECT ROW_NUMBER() OVER (ORDER BY " + quote(ROW_ID) + ") AS rn, "
                + rowNumberCteProjection(vp.leftColumns) + " FROM " + quote(vp.leftTable)
                + "), r AS (SELECT ROW_NUMBER() OVER (ORDER BY " + quote(ROW_ID) + ") AS rn, "
                + rowNumberCteProjection(vp.rightColumns) + " FROM " + quote(vp.rightTable) + ") SELECT "
                + joinProjection(vp, "l", "r", false) + " FROM l JOIN r ON l.rn = r.rn) " + quote(vp.sourceTable);
    }

    private String rowNumberCteProjection(List<VpColumn> columns) {
        List<String> parts = new ArrayList<>();
        parts.add(quote(ROW_ID));
        parts.addAll(columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        return String.join(", ", parts);
    }

    private String jsonPackRelation(VpSchema vp) {
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN (SELECT " + quote(ROW_ID) + ", JSON_OBJECT('vp', "
                        + quote(ROW_ID) + ") AS payload FROM " + quote(vp.rightTable) + ") jp ON l." + quote(ROW_ID)
                        + " = jp." + quote(ROW_ID) + " JOIN " + quote(vp.rightTable) + " r ON r." + quote(ROW_ID)
                        + " = jp." + quote(ROW_ID));
    }

    private String stringPackRelation(VpSchema vp) {
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN (SELECT " + quote(ROW_ID) + ", ('vp:' || " + quote(ROW_ID)
                        + ") AS payload FROM " + quote(vp.rightTable) + ") sp ON ('vp:' || l." + quote(ROW_ID)
                        + ") = sp.payload JOIN " + quote(vp.rightTable) + " r ON r." + quote(ROW_ID) + " = sp."
                        + quote(ROW_ID));
    }

    private String lateralRelation(VpSchema vp) {
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l CROSS APPLY (SELECT * FROM " + quote(vp.rightTable)
                        + " rr WHERE rr." + quote(ROW_ID) + " = l." + quote(ROW_ID) + ") r");
    }

    private VpQueryPair generateRandomRewritePair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        SourceAst sourceAst = createSourceAst(vp);
        OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(state).setColumns(sourceAst.columns);
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()
                && strategy != ReconstructionStrategy.TEMP_TABLE) {
            return generateCorrelatedSubqueryPair(vp, strategy, sourceAst);
        }
        OceanBaseSelect select = new OceanBaseSelect();
        select.setSelectType(Randomly.fromOptions(OceanBaseSelect.SelectType.values()));
        int fetchColumns = Math.max(1, Randomly.smallNumber() + 1);
        boolean useAggregate = Randomly.getBoolean();
        boolean hasWindowExpression = false;
        List<OceanBaseExpression> selected = new ArrayList<>();
        List<OceanBaseExpression> nonAggregateExpressions = new ArrayList<>();
        for (int i = 0; i < fetchColumns; i++) {
            OceanBaseExpression expr;
            if (!useAggregate && advancedFeaturesEnabled() && Randomly.getBooleanWithRatherLowProbability()) {
                expr = generateAdvancedSelectExpression(sourceAst);
                hasWindowExpression = true;
            } else if (useAggregate && Randomly.getBoolean()) {
                expr = generateVpAggregate(gen, sourceAst.columns);
            } else {
                expr = gen.generateExpression();
                nonAggregateExpressions.add(expr);
            }
            selected.add(expr);
        }
        if (selected.isEmpty()) {
            selected.add(new OceanBaseColumnReference(Randomly.fromList(sourceAst.columns), null));
        }
        select.setFetchColumns(selected);
        select.setFromList(List.of(new OceanBaseTableReference(sourceAst.table)));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (useAggregate || (!hasWindowExpression && Randomly.getBooleanWithRatherLowProbability())) {
            List<OceanBaseExpression> groupByExpressions = chooseGroupByExpressions(nonAggregateExpressions);
            select.setGroupByExpressions(groupByExpressions);
            if (Randomly.getBoolean() && !groupByExpressions.isEmpty()) {
                select.setHavingClause(generateHavingExpression(gen, sourceAst.columns, groupByExpressions));
            }
            if (advancedFeaturesEnabled() && !hasWindowExpression && !groupByExpressions.isEmpty()
                    && Randomly.getBooleanWithSmallProbability()) {
                recordAdvancedFeature(AdvancedFeature.ROLLUP);
                select.setGroupByExpressions(appendRollup(groupByExpressions));
            }
        }
        if (Randomly.getBoolean()) {
            List<OceanBaseExpression> orderBys = chooseOrderByExpressions(useAggregate, select, nonAggregateExpressions,
                    sourceAst.rowIdColumn);
            if (!orderBys.isEmpty()) {
                select.setOrderByClauses(orderBys);
            }
        }
        boolean orderedComparisonEnabled = isRowPreservingQuery(useAggregate, select)
                && hasVpRowIdTieBreaker(select, sourceAst.rowIdColumn);
        if (Randomly.getBoolean()) {
            boolean rowPreservingQuery = isRowPreservingQuery(useAggregate, select);
            if (select.getOrderByClauses().isEmpty()) {
                if (rowPreservingQuery) {
                    select.setOrderByClauses(List.of(new OceanBaseColumnReference(sourceAst.rowIdColumn, null)));
                } else if (!select.getGroupByExpressions().isEmpty()) {
                    select.setOrderByClauses(orderableGroupByExpressions(select.getGroupByExpressions()));
                }
                orderedComparisonEnabled = rowPreservingQuery;
            } else if (rowPreservingQuery) {
                List<OceanBaseExpression> orderBys = new ArrayList<>(select.getOrderByClauses());
                orderBys.add(new OceanBaseColumnReference(sourceAst.rowIdColumn, null));
                select.setOrderByClauses(orderBys);
                orderedComparisonEnabled = true;
            } else if (!select.getGroupByExpressions().isEmpty()) {
                select.setOrderByClauses(orderableGroupByExpressions(select.getGroupByExpressions()));
                orderedComparisonEnabled = false;
            } else {
                select.setOrderByClauses(List.of());
                orderedComparisonEnabled = false;
            }
            select.setSelectType(OceanBaseSelect.SelectType.ALL);
            select.setLimitClause(OceanBaseConstant.createIntConstant(Randomly.getNotCachedInteger(0, 25)));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                select.setOffsetClause(OceanBaseConstant.createIntConstant(Randomly.getNotCachedInteger(0, 5)));
            }
        }
        String singleQuery = OceanBaseVisitor.asString(select);
        if (advancedFeaturesEnabled() && !useAggregate && select.getOrderByClauses().isEmpty()
                && select.getLimitClause() == null && select.getOffsetClause() == null
                && strategy != ReconstructionStrategy.TEMP_TABLE && Randomly.getBooleanWithSmallProbability()) {
            recordAdvancedFeature(AdvancedFeature.UNION_ALL);
            singleQuery = "(" + singleQuery + ") UNION ALL (" + createUnionAllBranch(sourceAst, gen, selected.size())
                    + ")";
            orderedComparisonEnabled = false;
        }
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        String splitQuery = replaceSourceFrom(singleQuery, vp.sourceTable, relation);
        return new VpQueryPair(vp, strategy, singleQuery, splitQuery, extraSetup,
                "shape=" + vp.baseShape + ", split=" + vp.splitStrategy + ", reconstruction=" + strategy,
                orderedComparisonEnabled);
    }

    private List<OceanBaseExpression> appendRollup(List<OceanBaseExpression> groupByExpressions) {
        String expressions = groupByExpressions.stream().map(OceanBaseVisitor::asString)
                .collect(Collectors.joining(", "));
        return List.of(new OceanBaseText("ROLLUP(" + expressions + ")"));
    }

    private VpQueryPair generateCorrelatedSubqueryPair(VpSchema vp, ReconstructionStrategy strategy,
            SourceAst sourceAst) throws Exception {
        recordAdvancedFeature(AdvancedFeature.CORRELATED_SUBQUERY);
        String sourceAlias = "s";
        String subAlias = "s2";
        String selected = simpleColumnOrRowIdForSQL(sourceAst.columns);
        String outerColumn = qualifySelectedExpression(selected, sourceAlias);
        String predicateColumn = qualifySelectedExpression(simpleColumnOrRowIdForSQL(sourceAst.columns), subAlias);
        String singleQuery = Randomly.fromOptions(
                "SELECT " + outerColumn + " FROM " + quote(vp.sourceTable) + " " + sourceAlias
                        + " WHERE EXISTS (SELECT 1 FROM " + quote(vp.sourceTable) + " " + subAlias + " WHERE "
                        + subAlias + "." + quote(ROW_ID) + " = " + sourceAlias + "." + quote(ROW_ID) + " AND "
                        + predicateColumn + " IS NOT NULL)",
                "SELECT " + outerColumn + " FROM " + quote(vp.sourceTable) + " " + sourceAlias + " WHERE " + sourceAlias
                        + "." + quote(ROW_ID) + " IN (SELECT " + subAlias + "." + quote(ROW_ID) + " FROM "
                        + quote(vp.sourceTable) + " " + subAlias + " WHERE " + predicateColumn + " IS NOT NULL)",
                "SELECT " + outerColumn + ", (SELECT MIN(" + predicateColumn + ") FROM " + quote(vp.sourceTable) + " "
                        + subAlias + " WHERE " + subAlias + "." + quote(ROW_ID) + " <= " + sourceAlias + "."
                        + quote(ROW_ID) + ") FROM " + quote(vp.sourceTable) + " " + sourceAlias);
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        String splitQuery = replaceCorrelatedSourceAliases(singleQuery, vp.sourceTable, relation, sourceAlias,
                subAlias);
        return new VpQueryPair(vp, strategy, singleQuery, splitQuery, extraSetup, "shape=" + vp.baseShape + ", split="
                + vp.splitStrategy + ", reconstruction=" + strategy + ", advanced=correlated-subquery", false);
    }

    private String qualifySelectedExpression(String expression, String alias) {
        return expression.replace(quote(ROW_ID), alias + "." + quote(ROW_ID));
    }

    private String replaceCorrelatedSourceAliases(String singleQuery, String sourceTable, String relation,
            String outerAlias, String subqueryAlias) {
        String splitQuery = singleQuery;
        splitQuery = splitQuery.replace(" FROM " + quote(sourceTable) + " " + outerAlias,
                " FROM " + relationWithAlias(relation, sourceTable, outerAlias));
        splitQuery = splitQuery.replace(" FROM " + quote(sourceTable) + " " + subqueryAlias,
                " FROM " + relationWithAlias(relation, sourceTable, subqueryAlias));
        if (splitQuery.equals(singleQuery)) {
            throw new IgnoreMeException();
        }
        return splitQuery;
    }

    private String relationWithAlias(String relation, String sourceTable, String alias) {
        String suffix = " " + quote(sourceTable);
        if (!relation.endsWith(suffix)) {
            throw new IgnoreMeException();
        }
        return relation.substring(0, relation.length() - suffix.length()) + " " + alias;
    }

    private boolean isGroupByOrdinalLiteral(OceanBaseExpression expression) {
        return expression instanceof OceanBaseConstant && ((OceanBaseConstant) expression).isInt();
    }

    private List<OceanBaseExpression> chooseGroupByExpressions(List<OceanBaseExpression> nonAggregateExpressions) {
        return nonAggregateExpressions.stream().filter(e -> !isGroupByOrdinalLiteral(e)).collect(Collectors.toList());
    }

    private OceanBaseExpression generateHavingExpression(OceanBaseExpressionGenerator gen,
            List<OceanBaseColumn> columns, List<OceanBaseExpression> groupByExpressions) {
        List<String> candidates = new ArrayList<>();
        candidates.add("COUNT(*) > 0");
        candidates.add("SUM(" + numericExpressionForSQL(columns) + ") IS NOT NULL");
        candidates.add("(MIN(" + simpleColumnOrRowIdForSQL(columns) + ") = MAX(" + simpleColumnOrRowIdForSQL(columns)
                + ") OR (MIN(" + simpleColumnOrRowIdForSQL(columns) + ") IS NULL AND MAX("
                + simpleColumnOrRowIdForSQL(columns) + ") IS NULL))");

        if (!groupByExpressions.isEmpty()) {
            String groupBy = sqlWithoutRollup(Randomly.fromList(groupByExpressions));
            if (!groupBy.isBlank()) {
                candidates.add("(" + groupBy + ") IS NOT NULL");
                candidates.add("COUNT(" + groupBy + ") >= 0");
                candidates.add("(MIN(" + groupBy + ") = MAX(" + groupBy + ") OR (MIN(" + groupBy + ") IS NULL AND MAX("
                        + groupBy + ") IS NULL))");
            }
        }

        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            String expression = OceanBaseVisitor.asString(gen.generateExpression());
            candidates.add("COUNT(" + expression + ") >= 0");
            candidates.add("MIN(CAST((" + expression + ") AS VARCHAR2(4000))) IS NOT NULL");
            candidates.add("SUM(CAST((" + expression + ") AS NUMBER)) IS NOT NULL");
        }
        return new OceanBaseText(Randomly.fromList(candidates));
    }

    private List<OceanBaseExpression> chooseOrderByExpressions(boolean useAggregate, OceanBaseSelect select,
            List<OceanBaseExpression> nonAggregateExpressions, OceanBaseColumn rowIdColumn) {
        if (useAggregate || !select.getGroupByExpressions().isEmpty()) {
            if (!select.getGroupByExpressions().isEmpty()) {
                return orderableGroupByExpressions(select.getGroupByExpressions());
            }
            return List.of();
        }
        if (nonAggregateExpressions.isEmpty()) {
            return List.of(new OceanBaseColumnReference(rowIdColumn, null));
        }
        return nonAggregateExpressions;
    }

    private List<OceanBaseExpression> orderableGroupByExpressions(List<OceanBaseExpression> groupByExpressions) {
        List<OceanBaseExpression> orderable = new ArrayList<>();
        for (OceanBaseExpression expression : groupByExpressions) {
            String sql = OceanBaseVisitor.asString(expression);
            sql = removeTrailingRollup(sql);
            if (!sql.isBlank()) {
                orderable.add(new OceanBaseText(sql));
            }
        }
        return orderable;
    }

    private String sqlWithoutRollup(OceanBaseExpression expression) {
        return removeTrailingRollup(OceanBaseVisitor.asString(expression));
    }

    private String removeTrailingRollup(String sql) {
        String stripped = sql.strip();
        String upper = stripped.toUpperCase(Locale.ROOT);
        if (upper.endsWith(" WITH ROLLUP")) {
            return stripped.substring(0, upper.lastIndexOf(" WITH ROLLUP")).strip();
        }
        if (upper.startsWith("ROLLUP(") && stripped.endsWith(")")) {
            return stripped.substring("ROLLUP(".length(), stripped.length() - 1).strip();
        }
        return stripped;
    }

    private OceanBaseExpression generateAdvancedSelectExpression(SourceAst sourceAst) {
        recordAdvancedFeature(AdvancedFeature.WINDOW_FUNCTION);
        String order = simpleOrderExpressionForSQL(sourceAst.columns) + ", " + quote(ROW_ID);
        String value = simpleColumnOrRowIdForSQL(sourceAst.columns);
        String numeric = numericExpressionForSQL(sourceAst.columns);
        return new OceanBaseText(Randomly.fromOptions("ROW_NUMBER() OVER (ORDER BY " + order + ")",
                "RANK() OVER (ORDER BY " + order + ")", "LAG(" + value + ") OVER (ORDER BY " + quote(ROW_ID) + ")",
                "SUM(" + numeric + ") OVER (ORDER BY " + quote(ROW_ID) + ")"));
    }

    private String createUnionAllBranch(SourceAst sourceAst, OceanBaseExpressionGenerator gen, int fetchColumns) {
        OceanBaseSelect right = new OceanBaseSelect();
        right.setSelectType(OceanBaseSelect.SelectType.ALL);
        List<OceanBaseExpression> selected = new ArrayList<>();
        for (int i = 0; i < fetchColumns; i++) {
            selected.add(Randomly.getBooleanWithRatherLowProbability() ? generateAdvancedSelectExpression(sourceAst)
                    : gen.generateExpression());
        }
        right.setFetchColumns(selected);
        right.setFromList(List.of(new OceanBaseTableReference(sourceAst.table)));
        if (Randomly.getBoolean()) {
            right.setWhereClause(gen.generateExpression());
        }
        return OceanBaseVisitor.asString(right);
    }

    private boolean isRowPreservingQuery(boolean useAggregate, OceanBaseSelect select) {
        return !useAggregate && select.getGroupByExpressions().isEmpty()
                && select.getFromOptions() == OceanBaseSelect.SelectType.ALL;
    }

    private boolean hasVpRowIdTieBreaker(OceanBaseSelect select, OceanBaseColumn rowIdColumn) {
        List<OceanBaseExpression> orderBys = select.getOrderByClauses();
        if (orderBys.isEmpty()) {
            return false;
        }
        OceanBaseExpression last = orderBys.get(orderBys.size() - 1);
        return last instanceof OceanBaseColumnReference
                && ((OceanBaseColumnReference) last).getColumn().getName().equals(rowIdColumn.getName());
    }

    private OceanBaseExpression generateVpAggregate(OceanBaseExpressionGenerator gen, List<OceanBaseColumn> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            recordAdvancedFeature(AdvancedFeature.ADVANCED_AGGREGATE);
            return new OceanBaseText(randomAdvancedAggregateSQL(columns));
        }
        OceanBaseAggregateFunction func = Randomly.fromOptions(OceanBaseAggregateFunction.values());
        if (func == OceanBaseAggregateFunction.COUNT && Randomly.getBoolean()) {
            return new OceanBaseText("COUNT(*)");
        }
        if (func == OceanBaseAggregateFunction.SUM) {
            return new OceanBaseAggregate(generateSumInputExpression(gen, columns), func);
        }
        OceanBaseExpression input = func == OceanBaseAggregateFunction.BIT_AND
                || func == OceanBaseAggregateFunction.BIT_OR
                        ? new OceanBaseText("CAST(" + numericExpressionForSQL(columns) + " AS NUMBER)")
                        : aggregateInputExpression(gen, columns);
        return new OceanBaseAggregate(input, func);
    }

    private OceanBaseExpression aggregateInputExpression(OceanBaseExpressionGenerator gen,
            List<OceanBaseColumn> columns) {
        if (!columns.isEmpty() && Randomly.getBoolean()) {
            return new OceanBaseColumnReference(Randomly.fromList(columns), null);
        }
        return gen.generateExpression();
    }

    private OceanBaseExpression generateSumInputExpression(OceanBaseExpressionGenerator gen,
            List<OceanBaseColumn> columns) {
        if (Randomly.getBoolean()) {
            OceanBaseColumn column = Randomly.fromList(columns);
            OceanBaseColumnReference ref = new OceanBaseColumnReference(column, null);
            if (column.getType() == OceanBaseDataType.VARCHAR) {
                if (state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns
                        || advancedFeaturesEnabled()) {
                    if (state.getRandomly().getInteger(0, 100) < 70) {
                        return new OceanBaseText(
                                "CAST(" + OceanBaseVisitor.asString(ref) + " AS " + randomSumCastType() + ")");
                    }
                    return gen.generateExpression();
                }
                return new OceanBaseText("CAST(" + OceanBaseVisitor.asString(ref) + " AS NUMBER)");
            }
            return ref;
        }
        return gen.generateExpression();
    }

    private String randomSumCastType() {
        return Randomly.fromOptions("NUMBER", "BINARY_DOUBLE", "NUMBER(10, 2)", "NUMBER(20, 5)", "NUMBER");
    }

    private String randomAdvancedAggregateSQL(List<OceanBaseColumn> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()) {
            recordAdvancedFeature(AdvancedFeature.GROUP_CONCAT);
            return "LISTAGG(" + groupConcatInputForSQL(columns) + ", ',') WITHIN GROUP (ORDER BY " + quote(ROW_ID)
                    + ")";
        }
        String numeric = numericExpressionForSQL(columns);
        String integer = "CAST(" + numeric + " AS NUMBER)";
        return Randomly.fromOptions("AVG(" + numeric + ")", "MIN(" + integer + ")", "MAX(" + integer + ")",
                "STDDEV(" + numeric + ")", "VAR_SAMP(" + numeric + ")");
    }

    private String groupConcatInputForSQL(List<OceanBaseColumn> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        OceanBaseColumn column = Randomly.fromList(columns);
        if (column.getType() == OceanBaseDataType.VARCHAR || Randomly.getBoolean()) {
            return "CAST(" + quote(column.getName()) + " AS VARCHAR2(4000))";
        }
        return quote(column.getName());
    }

    private String numericExpressionForSQL(List<OceanBaseColumn> columns) {
        List<OceanBaseColumn> numericColumns = columns.stream().filter(c -> c.getType().isNumeric())
                .collect(Collectors.toList());
        if (!numericColumns.isEmpty() && Randomly.getBoolean()) {
            return quote(Randomly.fromList(numericColumns).getName());
        }
        return quote(ROW_ID);
    }

    private String simpleColumnOrRowIdForSQL(List<OceanBaseColumn> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        return quote(Randomly.fromList(columns).getName());
    }

    private String simpleOrderExpressionForSQL(List<OceanBaseColumn> columns) {
        if (columns.isEmpty()) {
            return quote(ROW_ID);
        }
        return simpleColumnOrRowIdForSQL(columns);
    }

    private static final class SourceAst {
        private final OceanBaseTable table;
        private final OceanBaseColumn rowIdColumn;
        private final List<OceanBaseColumn> columns;

        private SourceAst(OceanBaseTable table, OceanBaseColumn rowIdColumn, List<OceanBaseColumn> columns) {
            this.table = table;
            this.rowIdColumn = rowIdColumn;
            this.columns = columns;
        }
    }

    private SourceAst createSourceAst(VpSchema vp) {
        List<OceanBaseColumn> astColumns = new ArrayList<>();
        OceanBaseColumn rowId = new OceanBaseColumn(ROW_ID, OceanBaseDataType.INT, true, 64, false);
        astColumns.add(rowId);
        for (VpColumn column : vp.columns) {
            astColumns.add(new OceanBaseColumn(column.name, column.type, false, 0, false));
        }
        OceanBaseTable table = new OceanBaseTable(vp.sourceTable, astColumns, List.<OceanBaseIndex> of());
        for (OceanBaseColumn column : astColumns) {
            column.setTable(table);
        }
        return new SourceAst(table, rowId, astColumns);
    }

    private String replaceSourceFrom(String singleQuery, String sourceTable, String relation) {
        String needle = " FROM " + sourceTable;
        if (!singleQuery.contains(needle)) {
            throw new IgnoreMeException();
        }
        return singleQuery.replace(needle, " FROM " + relation);
    }

    private void comparePair(VpQueryPair pair) throws Exception {
        for (String setup : pair.extraSetupStatements) {
            if (isDropStatement(setup)) {
                executeOptional(setup);
                pair.schema.setupStatements.add(setup);
            } else {
                executeNoSchemaRefresh(setup);
                pair.schema.setupStatements.add(setup);
            }
        }
        QueryResultMultiset singleRows = getResultMultiset(pair.singleQuery);
        QueryResultMultiset splitRows = getResultMultiset(pair.splitQuery);
        if (!resultMultisetsMatch(singleRows, splitRows)) {
            if (isLimitZeroEmptyNonEmptyDiff(pair, singleRows, splitRows)) {
                QueryResultMultiset confirmedSingleRows = getResultMultiset(pair.singleQuery);
                QueryResultMultiset confirmedSplitRows = getResultMultiset(pair.splitQuery);
                if (resultMultisetsMatch(confirmedSingleRows, confirmedSplitRows)) {
                    recordSkippedError(new FlakyLimitZeroResultException(
                            "flaky LIMIT 0 empty/non-empty VP result disappeared on immediate recheck"));
                    throw new IgnoreMeException();
                }
                singleRows = confirmedSingleRows;
                splitRows = confirmedSplitRows;
            }
            KnownVpBug knownBug = classifyKnownBug(pair, singleRows, splitRows);
            if (suppressKnownBug(pair, knownBug, singleRows, splitRows, false)) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(formatBugReport(pair, singleRows, splitRows));
        }
        if (pair.orderedComparisonEnabled && !orderedRowsMatch(singleRows, splitRows)) {
            KnownVpBug knownBug = classifyKnownOrderedBug(pair, singleRows, splitRows);
            if (suppressKnownBug(pair, knownBug, singleRows, splitRows, true)) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(formatOrderedBugReport(pair, singleRows, splitRows));
        }
    }

    private boolean suppressKnownBug(VpQueryPair pair, KnownVpBug knownBug, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows, boolean orderedReport) throws SQLException {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns || knownBug == null) {
            return false;
        }
        logKnownBug(pair, knownBug, singleRows, splitRows, orderedReport);
        recordKnownBugSuppressed(knownBug);
        return true;
    }

    private KnownVpBug classifyKnownBug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns) {
            return null;
        }
        if (isCountNullExprPushdownAggregationDiff(pair, singleRows, splitRows)) {
            return KnownVpBug.COUNT_NULL_EXPR_PUSHDOWN_AGGREGATION;
        }
        if (isAvgNullExprPushdownAggregationDiff(pair, singleRows, splitRows)) {
            return KnownVpBug.AVG_NULL_EXPR_PUSHDOWN_AGGREGATION_COUNT_DENOMINATOR;
        }
        if (isGroupByIfNullPredicateProjectionDropsIfNull(pair, singleRows, splitRows)) {
            return KnownVpBug.GROUP_BY_IFNULL_PREDICATE_PROJECTION_DROPS_IFNULL;
        }
        if (isGroupByNondeterministicRepresentativeRowDiff(pair, singleRows, splitRows)) {
            return KnownVpBug.GROUP_BY_NONDETERMINISTIC_REPRESENTATIVE_ROW;
        }
        return null;
    }

    private boolean isLimitZeroEmptyNonEmptyDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!hasTopLevelLimitZero(pair.singleQuery) || !hasTopLevelLimitZero(pair.splitQuery)) {
            return false;
        }
        int singleCount = totalRowCount(singleRows);
        int splitCount = totalRowCount(splitRows);
        return (singleCount == 0 && splitCount > 0) || (splitCount == 0 && singleCount > 0);
    }

    private boolean hasTopLevelLimitZero(String sql) {
        String upper = sql.toUpperCase(Locale.ROOT);
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inBacktick = false;
        for (int i = 0; i < upper.length(); i++) {
            char ch = upper.charAt(i);
            if (ch == '\'' && !inDoubleQuote && !inBacktick) {
                inSingleQuote = !inSingleQuote;
            } else if (ch == '"' && !inSingleQuote && !inBacktick) {
                inDoubleQuote = !inDoubleQuote;
            } else if (ch == '`' && !inSingleQuote && !inDoubleQuote) {
                inBacktick = !inBacktick;
            } else if (!inSingleQuote && !inDoubleQuote && !inBacktick) {
                if (ch == '(') {
                    depth++;
                } else if (ch == ')') {
                    depth--;
                } else if (depth == 0 && upper.startsWith("LIMIT", i)
                        && isKeywordBoundary(upper, i, "LIMIT".length())) {
                    int pos = i + "LIMIT".length();
                    while (pos < upper.length() && Character.isWhitespace(upper.charAt(pos))) {
                        pos++;
                    }
                    return pos < upper.length() && upper.charAt(pos) == '0'
                            && (pos + 1 >= upper.length() || !Character.isDigit(upper.charAt(pos + 1)));
                } else if (depth == 0 && upper.startsWith("FETCH", i)
                        && isKeywordBoundary(upper, i, "FETCH".length())) {
                    int pos = i + "FETCH".length();
                    pos = skipKeywordAndWhitespace(upper, pos, "FIRST");
                    if (pos < 0) {
                        return false;
                    }
                    return pos < upper.length() && upper.charAt(pos) == '0'
                            && (pos + 1 >= upper.length() || !Character.isDigit(upper.charAt(pos + 1)));
                }
            }
        }
        return false;
    }

    private int skipKeywordAndWhitespace(String sql, int pos, String keyword) {
        while (pos < sql.length() && Character.isWhitespace(sql.charAt(pos))) {
            pos++;
        }
        if (!sql.startsWith(keyword, pos) || !isKeywordBoundary(sql, pos, keyword.length())) {
            return -1;
        }
        pos += keyword.length();
        while (pos < sql.length() && Character.isWhitespace(sql.charAt(pos))) {
            pos++;
        }
        return pos;
    }

    private boolean isGroupByIfNullPredicateProjectionDropsIfNull(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        String single = normalizeSqlForKnownBug(pair.singleQuery);
        if (!containsAny(single, " GROUP BY IFNULL(", " GROUP BY NVL(") || !containsAny(single, "IFNULL(", "NVL(")
                || !single.contains(" HAVING SUM(")) {
            return false;
        }
        if (!single.contains(" NOT IN ") && !single.contains("!=") && !single.contains("<>")
                && !single.contains(" IS NULL") && !single.contains(" IS NOT NULL")) {
            return false;
        }
        return hasSingleColumnNullVsTrueDiff(singleRows, splitRows)
                && explainShowsIfNullGroupButNullableProjection(pair.singleQuery);
    }

    private int totalRowCount(QueryResultMultiset rows) {
        int total = 0;
        for (int count : rows.rows.values()) {
            total += count;
        }
        return total;
    }

    private boolean hasSingleColumnNullVsTrueDiff(QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        List<String> onlySingle = expandedUnmatchedRows(singleRows, splitRows);
        List<String> onlySplit = expandedUnmatchedRows(splitRows, singleRows);
        if (onlySingle.size() != 1 || onlySplit.size() != 1) {
            return false;
        }
        List<String> singleValues = decodeCanonicalRow(onlySingle.get(0));
        List<String> splitValues = decodeCanonicalRow(onlySplit.get(0));
        if (singleValues.size() != 1 || splitValues.size() != 1) {
            return false;
        }
        return (singleValues.get(0) == null && "1".equals(splitValues.get(0)))
                || ("1".equals(singleValues.get(0)) && splitValues.get(0) == null);
    }

    private boolean explainShowsIfNullGroupButNullableProjection(String query) {
        String explain = explainText(query).toUpperCase(Locale.ROOT);
        return (explain.contains("GROUP([IFNULL(") || explain.contains("GROUP([NVL(")) && explain.contains("OUTPUT([")
                && !explain.contains("OUTPUT([IFNULL(") && !explain.contains("OUTPUT([NVL(");
    }

    private boolean containsAny(String text, String... needles) {
        for (String needle : needles) {
            if (text.contains(needle)) {
                return true;
            }
        }
        return false;
    }

    private String explainText(String query) {
        StringBuilder sb = new StringBuilder();
        try (Statement s = state.getConnection().createStatement();
                ResultSet rs = s.executeQuery("EXPLAIN " + stripTrailingSemicolon(query))) {
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    String value = rs.getString(i);
                    if (value != null) {
                        sb.append(value).append('\n');
                    }
                }
            }
        } catch (SQLException ignored) {
            return "";
        }
        return sb.toString();
    }

    private boolean isKeywordBoundary(String sql, int start, int length) {
        int before = start - 1;
        int after = start + length;
        return (before < 0 || !isIdentifierChar(sql.charAt(before)))
                && (after >= sql.length() || !isIdentifierChar(sql.charAt(after)));
    }

    private boolean isIdentifierChar(char ch) {
        return Character.isLetterOrDigit(ch) || ch == '_';
    }

    private boolean isGroupByNondeterministicRepresentativeRowDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        String single = normalizeSqlForKnownBug(pair.singleQuery);
        if (!single.contains(" GROUP BY ")) {
            return false;
        }
        if (single.contains(" WITH ROLLUP") || single.contains("ROLLUP(")) {
            return false;
        }
        if (containsCountNullPushdownCandidate(single) || containsAvgNullPushdownCandidate(single)) {
            return false;
        }
        return hasRepresentativeRowDiffShape(singleRows, splitRows);
    }

    private boolean containsCountNullPushdownCandidate(String sql) {
        return sql.contains("COUNT(") && containsNullBooleanCountPatternInCountArgument(sql);
    }

    private boolean containsAvgNullPushdownCandidate(String sql) {
        return sql.contains("AVG(") && containsNullBooleanAggregatePattern(sql, "AVG(");
    }

    private boolean hasRepresentativeRowDiffShape(QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        List<String> onlySingle = expandedUnmatchedRows(singleRows, splitRows);
        List<String> onlySplit = expandedUnmatchedRows(splitRows, singleRows);
        if (onlySingle.isEmpty() || onlySplit.isEmpty() || onlySingle.size() != onlySplit.size()
                || onlySingle.size() > MAX_DIFF_ROWS) {
            return false;
        }
        for (String singleRow : onlySingle) {
            List<String> singleValues = decodeCanonicalRow(singleRow);
            boolean matched = false;
            for (String splitRow : onlySplit) {
                List<String> splitValues = decodeCanonicalRow(splitRow);
                if (isRepresentativeRowValueDiff(singleValues, splitValues)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                return false;
            }
        }
        return true;
    }

    private List<String> expandedUnmatchedRows(QueryResultMultiset left, QueryResultMultiset right) {
        Map<String, Integer> unmatched = unmatchedRowsAfterFuzzyMatching(left, right);
        List<String> rows = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : unmatched.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                rows.add(entry.getKey());
            }
        }
        return rows;
    }

    private boolean isRepresentativeRowValueDiff(List<String> left, List<String> right) {
        if (left.size() != right.size() || left.isEmpty()) {
            return false;
        }
        int equal = 0;
        int different = 0;
        for (int i = 0; i < left.size(); i++) {
            if (Objects.equals(left.get(i), right.get(i))) {
                equal++;
            } else {
                different++;
            }
        }
        return equal > 0 && different > 0;
    }

    private boolean isCountNullExprPushdownAggregationDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        String single = normalizeSqlForKnownBug(pair.singleQuery);
        String split = normalizeSqlForKnownBug(pair.splitQuery);
        if (!single.contains("COUNT(") || !split.contains("COUNT(")) {
            return false;
        }
        if (!containsNullBooleanCountPatternInCountArgument(single)) {
            return false;
        }
        if (!hasAggregateColumnNonZeroVsZero(singleRows, splitRows)) {
            return false;
        }
        return explainContains(pair.singleQuery, "pushdown_aggregation")
                && !explainContains(pair.splitQuery, "pushdown_aggregation");
    }

    private boolean isAvgNullExprPushdownAggregationDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        String single = normalizeSqlForKnownBug(pair.singleQuery);
        String split = normalizeSqlForKnownBug(pair.splitQuery);
        if (!single.contains("AVG(") || !split.contains("AVG(")) {
            return false;
        }
        if (!containsNullBooleanAggregatePattern(single, "AVG(")) {
            return false;
        }
        if (!hasSingleNumericAggregateMismatch(singleRows, splitRows)) {
            return false;
        }
        return explainContains(pair.singleQuery, "pushdown_aggregation")
                && !explainContains(pair.splitQuery, "pushdown_aggregation");
    }

    private String normalizeSqlForKnownBug(String sql) {
        return sql.replace('`', ' ').replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
    }

    private boolean containsNullBooleanCountPatternInCountArgument(String sql) {
        return containsNullBooleanAggregatePattern(sql, "COUNT(");
    }

    private boolean containsNullBooleanAggregatePattern(String sql, String aggregate) {
        int start = 0;
        while (true) {
            int aggregateStart = sql.indexOf(aggregate, start);
            if (aggregateStart < 0) {
                return false;
            }
            int argumentStart = aggregateStart + aggregate.length();
            int argumentEnd = findMatchingParenthesis(sql, argumentStart - 1);
            if (argumentEnd > argumentStart) {
                String argument = sql.substring(argumentStart, argumentEnd);
                if (argument.contains("NULL") && containsNullableBooleanPattern(argument)) {
                    return true;
                }
            }
            start = argumentStart;
        }
    }

    private int findMatchingParenthesis(String sql, int openParenIndex) {
        int depth = 0;
        for (int i = openParenIndex; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            if (ch == '(') {
                depth++;
            } else if (ch == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private boolean containsNullableBooleanPattern(String argument) {
        if (argument.contains("!=") || argument.contains("<>") || argument.contains(" > ") || argument.contains(" < ")
                || argument.contains(">=") || argument.contains("<=")) {
            return true;
        }
        if (argument.contains(" IN ") || argument.contains(" NOT IN ")) {
            return true;
        }
        return argument.contains("GREATEST(") || argument.contains("LEAST(") || argument.contains("IS NOT UNKNOWN")
                || argument.contains("IS UNKNOWN") || argument.contains("IS NOT NULL") || argument.contains("IS NULL")
                || argument.contains(" XOR ") || argument.contains("IF(") || argument.contains("CASE WHEN");
    }

    private boolean hasAggregateColumnNonZeroVsZero(QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        List<BigDecimal> single = extractSingleNumericRow(singleRows);
        List<BigDecimal> split = extractSingleNumericRow(splitRows);
        if (single == null || split == null || single.size() != split.size()) {
            return false;
        }
        for (int i = 0; i < single.size(); i++) {
            if (single.get(i).compareTo(BigDecimal.ZERO) > 0 && split.get(i).compareTo(BigDecimal.ZERO) == 0) {
                return true;
            }
        }
        return false;
    }

    private boolean hasSingleNumericAggregateMismatch(QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        List<BigDecimal> single = extractSingleNumericRow(singleRows);
        List<BigDecimal> split = extractSingleNumericRow(splitRows);
        if (single == null || split == null || single.size() != split.size() || single.size() != 1) {
            return false;
        }
        return single.get(0).compareTo(split.get(0)) != 0;
    }

    private List<BigDecimal> extractSingleNumericRow(QueryResultMultiset rows) {
        if (rows.rows.size() != 1) {
            return null;
        }
        Map.Entry<String, Integer> entry = rows.rows.entrySet().iterator().next();
        if (entry.getValue() != 1) {
            return null;
        }
        List<String> values = decodeCanonicalRow(entry.getKey());
        if (values.isEmpty() || values.stream().anyMatch(Objects::isNull)) {
            return null;
        }
        List<BigDecimal> numericValues = new ArrayList<>();
        try {
            for (String value : values) {
                numericValues.add(new BigDecimal(value));
            }
            return numericValues;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private boolean explainContains(String query, String needle) {
        String normalizedNeedle = needle.toLowerCase(Locale.ROOT);
        try (Statement s = state.getConnection().createStatement();
                ResultSet rs = s.executeQuery("EXPLAIN " + stripTrailingSemicolon(query))) {
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    String value = rs.getString(i);
                    if (value != null && value.toLowerCase(Locale.ROOT).contains(normalizedNeedle)) {
                        return true;
                    }
                }
            }
        } catch (SQLException ignored) {
            return false;
        }
        return false;
    }

    private String stripTrailingSemicolon(String query) {
        String stripped = query.trim();
        while (stripped.endsWith(";")) {
            stripped = stripped.substring(0, stripped.length() - 1).trim();
        }
        return stripped;
    }

    private KnownVpBug classifyKnownOrderedBug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        return null;
    }

    private void logKnownBug(VpQueryPair pair, KnownVpBug knownBug, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows, boolean orderedReport) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("Known VP bug: ").append(knownBug).append(System.lineSeparator()).append(System.lineSeparator());
        if (orderedReport) {
            sb.append(formatOrderedBugReport(pair, singleRows, splitRows));
        } else {
            sb.append(formatBugReport(pair, singleRows, splitRows));
        }
        File dir = new File(Main.LOG_DIRECTORY, "oceanbase-oracle");
        try {
            Files.createDirectories(dir.toPath());
            Path target = nextKnownBugLogPath(dir);
            Files.writeString(target, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private Path nextKnownBugLogPath(File dir) {
        Path target;
        do {
            target = new File(dir, "vp-known" + KNOWN_BUG_LOG_ID.incrementAndGet() + ".log").toPath();
        } while (Files.exists(target));
        return target;
    }

    private boolean resultMultisetsMatch(QueryResultMultiset left, QueryResultMultiset right) {
        if (left.rows.equals(right.rows)) {
            return true;
        }
        Map<String, Integer> remaining = new LinkedHashMap<>(right.rows);
        for (Map.Entry<String, Integer> entry : left.rows.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                String match = findApproximateRowMatch(entry.getKey(), left.approximateColumns.get(entry.getKey()),
                        remaining, right.approximateColumns);
                if (match == null) {
                    return false;
                }
                int count = remaining.get(match);
                if (count == 1) {
                    remaining.remove(match);
                } else {
                    remaining.put(match, count - 1);
                }
            }
        }
        return remaining.isEmpty();
    }

    private String findApproximateRowMatch(String row, List<ApproximateKind> leftApproximateColumns,
            Map<String, Integer> candidates, Map<String, List<ApproximateKind>> candidateApproximateColumns) {
        Integer exactCount = candidates.get(row);
        if (exactCount != null && exactCount > 0) {
            return row;
        }
        for (Map.Entry<String, Integer> candidate : candidates.entrySet()) {
            if (candidate.getValue() > 0 && rowsApproximatelyEqual(row, leftApproximateColumns, candidate.getKey(),
                    candidateApproximateColumns.get(candidate.getKey()))) {
                return candidate.getKey();
            }
        }
        return null;
    }

    private boolean rowsApproximatelyEqual(String left, List<ApproximateKind> leftApproximateColumns, String right,
            List<ApproximateKind> rightApproximateColumns) {
        List<String> leftValues = decodeCanonicalRow(left);
        List<String> rightValues = decodeCanonicalRow(right);
        if (leftValues.size() != rightValues.size()) {
            return false;
        }
        for (int i = 0; i < leftValues.size(); i++) {
            String l = leftValues.get(i);
            String r = rightValues.get(i);
            if (Objects.equals(l, r)) {
                continue;
            }
            ApproximateKind approximateKind = ApproximateKind.looser(getApproximateKind(leftApproximateColumns, i),
                    getApproximateKind(rightApproximateColumns, i));
            if (!approximateKind.isApproximate() || l == null || r == null
                    || !numericValuesApproximatelyEqual(l, r, approximateKind)) {
                return false;
            }
        }
        return true;
    }

    private ApproximateKind getApproximateKind(List<ApproximateKind> approximateColumns, int index) {
        if (approximateColumns == null || index >= approximateColumns.size()) {
            return ApproximateKind.NONE;
        }
        return approximateColumns.get(index);
    }

    private List<String> decodeCanonicalRow(String row) {
        List<String> values = new ArrayList<>();
        int idx = 0;
        while (idx < row.length()) {
            if (row.startsWith("N;", idx)) {
                values.add(null);
                idx += 2;
            } else if (row.startsWith("S:", idx)) {
                int lengthStart = idx + 2;
                int lengthEnd = row.indexOf(':', lengthStart);
                if (lengthEnd < 0) {
                    throw new AssertionError("Malformed VP row encoding: " + row);
                }
                int length = Integer.parseInt(row.substring(lengthStart, lengthEnd));
                int valueStart = lengthEnd + 1;
                int valueEnd = valueStart + length;
                if (valueEnd >= row.length() || row.charAt(valueEnd) != ';') {
                    throw new AssertionError("Malformed VP row encoding: " + row);
                }
                values.add(row.substring(valueStart, valueEnd));
                idx = valueEnd + 1;
            } else {
                throw new AssertionError("Malformed VP row encoding: " + row);
            }
        }
        return values;
    }

    private boolean numericValuesApproximatelyEqual(String left, String right, ApproximateKind approximateKind) {
        try {
            double l = Double.parseDouble(left);
            double r = Double.parseDouble(right);
            if (!Double.isFinite(l) || !Double.isFinite(r)) {
                return false;
            }
            double diff = Math.abs(l - r);
            double scale = Math.max(Math.abs(l), Math.abs(r));
            return diff <= approximateKind.absoluteTolerance || diff <= approximateKind.relativeTolerance * scale;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    private QueryResultMultiset getResultMultiset(String query) throws SQLException {
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(query);
        }
        SQLQueryAdapter adapter = new SQLQueryAdapter(query, errors, false, state.getOptions().canonicalizeSqlString());
        Map<String, Integer> rows = new LinkedHashMap<>();
        Map<String, List<ApproximateKind>> approximateColumns = new LinkedHashMap<>();
        List<String> orderedRows = new ArrayList<>();
        List<List<ApproximateKind>> orderedApproximateColumns = new ArrayList<>();
        SQLancerResultSet rs = null;
        boolean completed = false;
        Main.nrQueries.addAndGet(1);
        try {
            rs = adapter.executeAndGet(state);
            if (rs == null) {
                throw new IgnoreMeException();
            }
            int columnCount = rs.getColumnCount();
            while (rs.next()) {
                String row = canonicalizeRow(rs, columnCount);
                List<ApproximateKind> rowApproximateColumns = getApproximateColumns(rs, columnCount);
                orderedRows.add(row);
                orderedApproximateColumns.add(rowApproximateColumns);
                rows.put(row, rows.getOrDefault(row, 0) + 1);
                approximateColumns.putIfAbsent(row, rowApproximateColumns);
            }
            completed = true;
        } catch (AssertionError e) {
            if (isSkippableQueryError(e)) {
                recordSkippedError(e);
                throw new IgnoreMeException();
            }
            throw e;
        } finally {
            if (completed) {
                Main.nrSuccessfulActions.addAndGet(1);
            } else {
                Main.nrUnsuccessfulActions.addAndGet(1);
            }
            if (rs != null && !rs.isClosed()) {
                rs.close();
            }
        }
        return new QueryResultMultiset(rows, approximateColumns, orderedRows, orderedApproximateColumns);
    }

    private List<ApproximateKind> getApproximateColumns(SQLancerResultSet rs, int columnCount) throws SQLException {
        List<ApproximateKind> approximate = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            approximate.add(getApproximateColumnType(rs.getType(i)));
        }
        return approximate;
    }

    private ApproximateKind getApproximateColumnType(String type) {
        if (type == null) {
            return ApproximateKind.NONE;
        }
        String upper = type.toUpperCase(Locale.ROOT);
        if (upper.contains("FLOAT") || upper.contains("REAL")) {
            return ApproximateKind.FLOAT;
        }
        if (upper.contains("DOUBLE")) {
            return ApproximateKind.DOUBLE;
        }
        return ApproximateKind.NONE;
    }

    private boolean isSkippableQueryError(AssertionError e) {
        String msg = getErrorText(e);
        if (msg.isEmpty()) {
            return false;
        }
        return msg.contains("doesn't yet support") || msg.contains("You have an error in your SQL syntax")
                || isUnsupportedWindowError(msg) || msg.contains("Incorrect DECIMAL value")
                || msg.contains("Truncated incorrect") || msg.contains("Data truncation")
                || msg.contains("Invalid JSON") || isUnsupportedLateralError(msg)
                || msg.contains("incompatible with sql_mode=only_full_group_by") || msg.contains("not in SELECT list")
                || msg.contains("Expression #") || msg.contains("Invalid use of group function")
                || isSkippableUnknownColumnError(msg) || isMissingFunctionError(msg)
                || isCantGroupOnGeneratedAliasError(msg) || msg.contains("Can't reopen table")
                || msg.contains("Illegal mix of collations") || msg.contains("Out of sort memory")
                || msg.contains("sort buffer size") || msg.contains("Operand should contain")
                || msg.contains("Subquery returns more than 1 row") || msg.contains("Timeout")
                || msg.contains("not supported") || isSkippableOracleQueryError(msg);
    }

    private void recordSkippedError(Throwable throwable) {
        String errorKind = classifySkippedError(throwable);
        synchronized (STATS_LOCK) {
            SKIPPED_ERROR_COUNTS.put(errorKind, SKIPPED_ERROR_COUNTS.getOrDefault(errorKind, 0) + 1);
        }
    }

    private String classifySkippedError(Throwable throwable) {
        String msg = getSqlExceptionMessage(throwable);
        if (msg == null || msg.isBlank()) {
            msg = firstNonBlankLine(getErrorText(throwable));
        }
        return normalizeSkippedErrorMessage(msg);
    }

    private String getSqlExceptionMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SQLException && current.getMessage() != null && !current.getMessage().isBlank()) {
                return current.getMessage();
            }
            current = current.getCause();
        }
        return null;
    }

    private String firstNonBlankLine(String msg) {
        if (msg == null || msg.isBlank()) {
            return "";
        }
        return msg.lines().filter(line -> !line.isBlank()).findFirst().orElse(msg).strip();
    }

    private String normalizeSkippedErrorMessage(String msg) {
        if (msg == null || msg.isBlank()) {
            return "<empty>";
        }
        String firstLine = firstNonBlankLine(msg);
        firstLine = firstLine.replaceAll("\\[[^\\]]*\\]", "[...]");
        firstLine = protectKnownErrorClauses(firstLine);
        firstLine = firstLine.replaceAll("'[^']*'", "'?'");
        firstLine = firstLine.replaceAll("`[^`]*`", "`?`");
        firstLine = restoreKnownErrorClauses(firstLine);
        firstLine = firstLine.replaceAll("\\bvp_[A-Za-z0-9_]+\\b", "vp_?");
        firstLine = firstLine.replaceAll("\\bdatabase\\d+\\b", "database?");
        firstLine = firstLine.replaceAll("\\bt\\d+\\b", "t?");
        firstLine = firstLine.replaceAll("\\bc\\d+\\b", "c?");
        firstLine = firstLine.replaceAll("\\bi\\d+\\b", "i?");
        firstLine = firstLine.replaceAll("-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?", "?");
        firstLine = firstLine.replaceAll("\\s+", " ").strip();
        if (firstLine.length() > 220) {
            firstLine = firstLine.substring(0, 220) + "...";
        }
        return firstLine;
    }

    private String protectKnownErrorClauses(String msg) {
        return msg.replace("in 'order clause'", "in __VP_ORDER_CLAUSE__")
                .replace("in 'group statement'", "in __VP_GROUP_STATEMENT__")
                .replace("in 'having clause'", "in __VP_HAVING_CLAUSE__")
                .replace("in 'field list'", "in __VP_FIELD_LIST__")
                .replace("in 'where clause'", "in __VP_WHERE_CLAUSE__")
                .replace("in 'EXISTS subquery'", "in __VP_EXISTS_SUBQUERY__");
    }

    private String restoreKnownErrorClauses(String msg) {
        return msg.replace("__VP_ORDER_CLAUSE__", "'order clause'")
                .replace("__VP_GROUP_STATEMENT__", "'group statement'")
                .replace("__VP_HAVING_CLAUSE__", "'having clause'").replace("__VP_FIELD_LIST__", "'field list'")
                .replace("__VP_WHERE_CLAUSE__", "'where clause'")
                .replace("__VP_EXISTS_SUBQUERY__", "'EXISTS subquery'");
    }

    private boolean isUnsupportedWindowError(String msg) {
        String normalized = msg.toLowerCase(Locale.ROOT);
        return normalized.contains("window")
                && (normalized.contains("doesn't yet support") || normalized.contains("not allowed")
                        || normalized.contains("invalid use") || normalized.contains("not supported"));
    }

    private boolean isUnsupportedLateralError(String msg) {
        String normalized = msg.toLowerCase(Locale.ROOT);
        return normalized.contains("lateral")
                && (normalized.contains("syntax") || normalized.contains("doesn't yet support")
                        || normalized.contains("not supported") || normalized.contains("not allowed"));
    }

    private boolean isSkippableUnknownColumnError(String msg) {
        return java.util.regex.Pattern.compile(
                "Unknown column '.*' in '(order clause|group statement|having clause|field list|where clause|EXISTS subquery)'")
                .matcher(msg).find();
    }

    private boolean isCantGroupOnGeneratedAliasError(String msg) {
        return java.util.regex.Pattern.compile("Can't group on 'ref\\d+'").matcher(msg).find();
    }

    private boolean isMissingFunctionError(String msg) {
        return msg.contains("FUNCTION") && msg.contains("does not exist");
    }

    private boolean isSkippableOracleQueryError(String msg) {
        String normalized = msg.toLowerCase(Locale.ROOT);
        return normalized.contains("ora-00904") || normalized.contains("invalid identifier")
                || normalized.contains("ora-00932") || normalized.contains("inconsistent datatypes")
                || normalized.contains("ora-01427")
                || normalized.contains("single-row subquery returns more than one row")
                || normalized.contains("ora-01722") || normalized.contains("invalid number")
                || normalized.contains("ora-01426") || normalized.contains("numeric overflow")
                || normalized.contains("ora-01489") || normalized.contains("result of string concatenation is too long")
                || normalized.contains("ora-30483") || normalized.contains("window functions are not allowed here");
    }

    private String getErrorText(Throwable throwable) {
        StringBuilder sb = new StringBuilder();
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null) {
                sb.append(current.getMessage()).append('\n');
            }
            current = current.getCause();
        }
        return sb.toString();
    }

    private String canonicalizeRow(SQLancerResultSet rs, int columnCount) throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            String value = rs.getString(i);
            if (value == null) {
                sb.append("N;");
            } else {
                String canonical = canonicalizeValue(value);
                sb.append("S:").append(canonical.length()).append(":").append(canonical).append(";");
            }
        }
        return sb.toString();
    }

    private String canonicalizeValue(String value) {
        try {
            BigDecimal decimal = new BigDecimal(value);
            String plain = decimal.stripTrailingZeros().toPlainString();
            return Objects.equals(plain, "-0") ? "0" : plain;
        } catch (NumberFormatException ignored) {
            return value;
        }
    }

    private String formatBugReport(VpQueryPair pair, QueryResultMultiset singleRows, QueryResultMultiset splitRows)
            throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("[VERTICAL PARTITIONING BUG CANDIDATE]").append(System.lineSeparator());
        sb.append("Mode: OCEANBASE_ORACLE_RANDOM_TABLE_REWRITE").append(System.lineSeparator());
        sb.append("Strategy summary: split=").append(pair.schema.splitStrategy).append(", reconstruction=")
                .append(pair.reconstructionStrategy).append(", shape=").append(pair.schema.baseShape)
                .append(", prefix=").append(pair.schema.prefix).append(System.lineSeparator());
        synchronized (STATS_LOCK) {
            sb.append("Strategy counts: split={")
                    .append(formatEnumCounts(SplitStrategy.values(), SPLIT_STRATEGY_COUNTS))
                    .append("}, reconstruction={")
                    .append(formatEnumCounts(ReconstructionStrategy.values(), RECONSTRUCTION_STRATEGY_COUNTS))
                    .append("}").append(System.lineSeparator());
        }
        sb.append("Database: ").append(state.getDatabaseName()).append(System.lineSeparator());
        sb.append("OceanBase version: ").append(getSingleString("SELECT VERSION()")).append(System.lineSeparator());
        sb.append("Base table: ").append(pair.schema.baseTable).append(System.lineSeparator());
        sb.append("Base table DDL: ").append(formatTableDdl(pair.schema.baseTable)).append(System.lineSeparator());
        sb.append("Base table physical properties: ").append(pair.schema.basePhysicalProperties)
                .append(System.lineSeparator());
        sb.append("VP source physical properties: ").append(pair.schema.sourcePhysicalProperties)
                .append(System.lineSeparator());
        sb.append("VP left physical properties: ").append(pair.schema.leftPhysicalProperties)
                .append(System.lineSeparator());
        sb.append("VP right physical properties: ").append(pair.schema.rightPhysicalProperties)
                .append(System.lineSeparator());
        sb.append("Base row count: ").append(pair.schema.baseRowCount).append(System.lineSeparator());
        sb.append("Base shape: ").append(pair.schema.baseShape).append(System.lineSeparator());
        sb.append("Stable row order: ")
                .append(pair.schema.stableOrderStrategy == null ? "NONE" : pair.schema.stableOrderStrategy)
                .append(System.lineSeparator());
        sb.append("Prefix: ").append(pair.schema.prefix).append(System.lineSeparator());
        sb.append("Split strategy: ").append(pair.schema.splitStrategy).append(System.lineSeparator());
        sb.append("Left columns: ").append(columnNames(pair.schema.leftColumns)).append(System.lineSeparator());
        sb.append("Right columns: ").append(columnNames(pair.schema.rightColumns)).append(System.lineSeparator());
        sb.append("Overlap columns: ").append(columnNames(pair.schema.overlapColumns)).append(System.lineSeparator());
        sb.append("Physical foreign key: ").append(pair.schema.usesPhysicalForeignKey).append(System.lineSeparator());
        sb.append("Reconstruction strategy: ").append(pair.reconstructionStrategy).append(System.lineSeparator());
        sb.append("Description: ").append(pair.description).append(System.lineSeparator());
        sb.append("Session vars: ").append(captureSessionVars()).append(System.lineSeparator());
        sb.append("Session SET statements:").append(System.lineSeparator());
        sb.append(captureSessionSetStatements());
        sb.append("Setup statements:").append(System.lineSeparator());
        for (String setup : pair.schema.setupStatements) {
            sb.append(setup).append(";").append(System.lineSeparator());
        }
        sb.append("VP source snapshot:").append(System.lineSeparator());
        sb.append(formatSnapshot(pair.schema.sourceTable, pair.schema.columns));
        sb.append("VP left snapshot:").append(System.lineSeparator());
        sb.append(formatSnapshot(pair.schema.leftTable, pair.schema.leftColumns));
        sb.append("VP right snapshot:").append(System.lineSeparator());
        sb.append(formatSnapshot(pair.schema.rightTable, pair.schema.rightColumns));
        sb.append("Single query: ").append(pair.singleQuery).append(System.lineSeparator());
        sb.append("Split query: ").append(pair.splitQuery).append(System.lineSeparator());
        sb.append("Single query plan:").append(System.lineSeparator());
        sb.append(formatExplain(pair.singleQuery));
        sb.append("Split query plan:").append(System.lineSeparator());
        sb.append(formatExplain(pair.splitQuery));
        sb.append("Single result sample: ").append(sample(singleRows.rows)).append(System.lineSeparator());
        sb.append("Split result sample: ").append(sample(splitRows.rows)).append(System.lineSeparator());
        sb.append("Only in single: ").append(diff(singleRows, splitRows)).append(System.lineSeparator());
        sb.append("Only in split: ").append(diff(splitRows, singleRows)).append(System.lineSeparator());
        lastQueryString = pair.singleQuery + System.lineSeparator() + pair.splitQuery;
        return sb.toString();
    }

    private String formatOrderedBugReport(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("[VERTICAL PARTITIONING ORDER BUG CANDIDATE]").append(System.lineSeparator());
        sb.append(formatBugReport(pair, singleRows, splitRows));
        sb.append("Single ordered result sample: ").append(orderedSample(singleRows)).append(System.lineSeparator());
        sb.append("Split ordered result sample: ").append(orderedSample(splitRows)).append(System.lineSeparator());
        sb.append("First ordered mismatch: ").append(firstOrderedMismatch(singleRows, splitRows))
                .append(System.lineSeparator());
        return sb.toString();
    }

    private String sample(Map<String, Integer> rows) {
        return rows.entrySet().stream().limit(MAX_RESULT_SAMPLE).map(e -> e.getKey() + " x" + e.getValue())
                .collect(Collectors.joining(", "));
    }

    private String orderedSample(QueryResultMultiset rows) {
        return rows.orderedRows.stream().limit(MAX_RESULT_SAMPLE).collect(Collectors.joining(", "));
    }

    private String firstOrderedMismatch(QueryResultMultiset left, QueryResultMultiset right) {
        int size = Math.min(left.orderedRows.size(), right.orderedRows.size());
        for (int i = 0; i < size; i++) {
            if (!orderedRowsApproximatelyEqual(left, i, right, i)) {
                return "index=" + i + ", single=" + left.orderedRows.get(i) + ", split=" + right.orderedRows.get(i);
            }
        }
        if (left.orderedRows.size() != right.orderedRows.size()) {
            return "length single=" + left.orderedRows.size() + ", split=" + right.orderedRows.size();
        }
        return "<none>";
    }

    private String diff(QueryResultMultiset left, QueryResultMultiset right) {
        Map<String, Integer> remaining = unmatchedRowsAfterFuzzyMatching(left, right);
        return remaining.entrySet().stream().filter(e -> e.getValue() > 0).limit(MAX_DIFF_ROWS)
                .map(e -> e.getKey() + " x" + e.getValue()).collect(Collectors.joining(", "));
    }

    private boolean orderedRowsMatch(QueryResultMultiset left, QueryResultMultiset right) {
        if (left.orderedRows.size() != right.orderedRows.size()) {
            return false;
        }
        for (int i = 0; i < left.orderedRows.size(); i++) {
            if (!orderedRowsApproximatelyEqual(left, i, right, i)) {
                return false;
            }
        }
        return true;
    }

    private boolean orderedRowsApproximatelyEqual(QueryResultMultiset left, int leftIndex, QueryResultMultiset right,
            int rightIndex) {
        String leftRow = left.orderedRows.get(leftIndex);
        String rightRow = right.orderedRows.get(rightIndex);
        if (leftRow.equals(rightRow)) {
            return true;
        }
        return rowsApproximatelyEqual(leftRow, getOrderedApproximateColumns(left, leftIndex), rightRow,
                getOrderedApproximateColumns(right, rightIndex));
    }

    private List<ApproximateKind> getOrderedApproximateColumns(QueryResultMultiset rows, int index) {
        if (index >= rows.orderedApproximateColumns.size()) {
            return List.of();
        }
        return rows.orderedApproximateColumns.get(index);
    }

    private Map<String, Integer> unmatchedRowsAfterFuzzyMatching(QueryResultMultiset left, QueryResultMultiset right) {
        Map<String, Integer> remainingLeft = new LinkedHashMap<>(left.rows);
        Map<String, Integer> remainingRight = new LinkedHashMap<>(right.rows);
        for (Map.Entry<String, Integer> entry : new ArrayList<>(remainingLeft.entrySet())) {
            int exactMatches = Math.min(entry.getValue(), remainingRight.getOrDefault(entry.getKey(), 0));
            if (exactMatches == 0) {
                continue;
            }
            decrementRowCount(remainingLeft, entry.getKey(), exactMatches);
            decrementRowCount(remainingRight, entry.getKey(), exactMatches);
        }
        for (Map.Entry<String, Integer> entry : new ArrayList<>(remainingLeft.entrySet())) {
            String row = entry.getKey();
            int unmatchedCount = entry.getValue();
            for (int i = 0; i < unmatchedCount; i++) {
                String match = findApproximateRowMatch(row, left.approximateColumns.get(row), remainingRight,
                        right.approximateColumns);
                if (match == null) {
                    break;
                }
                decrementRowCount(remainingLeft, row, 1);
                decrementRowCount(remainingRight, match, 1);
            }
        }
        return remainingLeft;
    }

    private void decrementRowCount(Map<String, Integer> rows, String row, int amount) {
        int remaining = rows.getOrDefault(row, 0) - amount;
        if (remaining <= 0) {
            rows.remove(row);
        } else {
            rows.put(row, remaining);
        }
    }

    private String formatSnapshot(String tableName, List<VpColumn> columns) throws SQLException {
        List<String> names = new ArrayList<>();
        names.add(quote(ROW_ID));
        names.addAll(columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        String sql = "SELECT " + String.join(", ", names) + " FROM " + quote(tableName) + " ORDER BY " + quote(ROW_ID);
        StringBuilder sb = new StringBuilder();
        sb.append(sql).append(";").append(System.lineSeparator());
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int rows = 0;
            while (rs.next() && rows++ < MAX_RESULT_SAMPLE) {
                List<String> values = new ArrayList<>();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    values.add(meta.getColumnLabel(i) + "=" + rs.getString(i));
                }
                sb.append(values).append(System.lineSeparator());
            }
        }
        return sb.toString();
    }

    private String formatExplain(String query) {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN ").append(query).append(";").append(System.lineSeparator());
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery("EXPLAIN " + query)) {
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                List<String> values = new ArrayList<>();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    values.add(meta.getColumnLabel(i) + "=" + rs.getString(i));
                }
                sb.append(values).append(System.lineSeparator());
            }
        } catch (SQLException e) {
            sb.append("-- EXPLAIN failed: ").append(e.getMessage()).append(System.lineSeparator());
        }
        return sb.toString();
    }

    private String captureSessionVars() {
        return "current_schema=" + getSingleString("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL")
                + ", session_user=" + getSingleString("SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') FROM DUAL")
                + ", current_user=" + getSingleString("SELECT SYS_CONTEXT('USERENV', 'CURRENT_USER') FROM DUAL");
    }

    private String captureSessionSetStatements() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER SESSION SET TIME_ZONE = '+00:00';").append(System.lineSeparator());
        sb.append("ALTER SESSION SET NLS_COMP = BINARY;").append(System.lineSeparator());
        sb.append("ALTER SESSION SET NLS_SORT = BINARY;").append(System.lineSeparator());
        sb.append("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,';").append(System.lineSeparator());
        return sb.toString();
    }

    private String getSingleString(String sql) {
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException ignored) {
        }
        return "<unavailable>";
    }

    private String formatTableDdl(String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(quote(tableName)).append(" (");
        try (Statement s = state.getConnection().createStatement();
                ResultSet rs = s.executeQuery("SELECT column_name, data_type, data_precision, data_scale, nullable "
                        + "FROM user_tab_columns WHERE table_name = '" + sqlString(tableName)
                        + "' ORDER BY column_id")) {
            List<String> defs = new ArrayList<>();
            while (rs.next()) {
                String type = rs.getString("DATA_TYPE");
                int precision = rs.getInt("DATA_PRECISION");
                int scale = rs.getInt("DATA_SCALE");
                String typeWithPrecision = type;
                if (precision > 0 && "NUMBER".equalsIgnoreCase(type)) {
                    typeWithPrecision = type + "(" + precision + ", " + scale + ")";
                }
                defs.add(quote(rs.getString("COLUMN_NAME")) + " " + typeWithPrecision
                        + ("N".equalsIgnoreCase(rs.getString("NULLABLE")) ? " NOT NULL" : ""));
            }
            sb.append(String.join(", ", defs));
        } catch (SQLException e) {
            return "<unavailable: " + e.getMessage() + ">";
        }
        sb.append(")");
        String physicalProperties = getTablePhysicalSignature(tableName);
        if (!physicalProperties.isBlank()) {
            sb.append(" ").append(physicalProperties);
        }
        return sb.toString();
    }

    private String getTablePhysicalSignature(String tableName) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine) {
            return "";
        }
        try (Statement s = state.getConnection().createStatement();
                ResultSet rs = s.executeQuery("SELECT compression FROM user_tables WHERE table_name = '"
                        + tableName.toUpperCase(Locale.ROOT) + "'")) {
            if (rs.next() && "ENABLED".equalsIgnoreCase(rs.getString(1))) {
                return "COMPRESS";
            }
        } catch (SQLException ignored) {
        }
        return "";
    }

    private String columnNames(List<VpColumn> columns) {
        return columns.stream().map(c -> c.name).collect(Collectors.joining(", "));
    }

    private void executeSetup(VpSchema vp, String sql) throws Exception {
        executeNoSchemaRefresh(sql);
        vp.setupStatements.add(sql);
    }

    private void tryExecuteSetup(VpSchema vp, String sql) {
        try {
            executeSetup(vp, sql);
        } catch (Exception | AssertionError ignored) {
            vp.setupStatements.add("-- skipped setup: " + sql);
        }
    }

    private void executeNoSchemaRefresh(String sql) throws Exception {
        SQLQueryAdapter query = new SQLQueryAdapter(sql, errors, couldAffectSchema(sql),
                state.getOptions().canonicalizeSqlString());
        try {
            query.execute(state);
        } catch (AssertionError e) {
            if (isSkippableQueryError(e)) {
                recordSkippedError(e);
                throw new IgnoreMeException();
            }
            throw e;
        }
    }

    private boolean couldAffectSchema(String sql) {
        String trimmed = sql.stripLeading().toUpperCase(Locale.ROOT);
        return trimmed.startsWith("CREATE TABLE") || trimmed.startsWith("CREATE TEMPORARY TABLE")
                || trimmed.startsWith("DROP TABLE") || trimmed.startsWith("DROP TEMPORARY TABLE")
                || trimmed.startsWith("ALTER TABLE") || trimmed.startsWith("CREATE VIEW")
                || trimmed.startsWith("DROP VIEW") || trimmed.startsWith("CREATE INDEX")
                || trimmed.startsWith("DROP INDEX");
    }

    private boolean isDropStatement(String sql) {
        String trimmed = sql.stripLeading().toUpperCase(Locale.ROOT);
        return trimmed.startsWith("DROP TABLE") || trimmed.startsWith("DROP VIEW");
    }

    private void executeOptional(String sql) {
        try {
            executeNoSchemaRefresh(sql);
        } catch (Exception | AssertionError ignored) {
        }
    }

    private String toSQLType(OceanBaseDataType type) {
        switch (type) {
        case INT:
            return "NUMBER(19)";
        case VARCHAR:
            return "VARCHAR2(500)";
        case FLOAT:
            return "BINARY_FLOAT";
        case DOUBLE:
            return "BINARY_DOUBLE";
        case DECIMAL:
            return "NUMBER(38, 10)";
        default:
            throw new AssertionError(type);
        }
    }

    private boolean advancedFeaturesEnabled() {
        return state.getDbmsSpecificOptions().verticalPartitioningAdvancedQueryFeatures || deepExplorationEnabled();
    }

    private boolean deepExplorationEnabled() {
        return state.getDbmsSpecificOptions().verticalPartitioningDeepExploration;
    }

    public static void recordAdvancedFeature(AdvancedFeature feature) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            increment(ADVANCED_FEATURE_COUNTS, feature);
        }
    }

    private static void recordKnownBugSuppressed(KnownVpBug knownBug) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            increment(KNOWN_BUG_COUNTS, knownBug);
        }
    }

    private void incrementSplitStrategy(SplitStrategy value) {
        synchronized (STATS_LOCK) {
            increment(SPLIT_STRATEGY_COUNTS, value);
        }
    }

    private void incrementReconstructionStrategy(ReconstructionStrategy value) {
        synchronized (STATS_LOCK) {
            increment(RECONSTRUCTION_STRATEGY_COUNTS, value);
        }
    }

    private void incrementSelectedBaseShape(BaseShape value) {
        synchronized (STATS_LOCK) {
            increment(SELECTED_BASE_SHAPE_COUNTS, value);
        }
    }

    private static <E extends Enum<E>> void increment(Map<E, Integer> counts, E value) {
        counts.put(value, counts.getOrDefault(value, 0) + 1);
    }

    private static void incrementSchemaShape(String key, int value) {
        SCHEMA_SHAPE_COUNTS.put(key, SCHEMA_SHAPE_COUNTS.getOrDefault(key, 0) + value);
    }

    private static void recordCompletedVpRun(int checkedPairs, int attempts) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            completedOracleRuns++;
            completedQueryPairs += checkedPairs;
            completedAttempts += attempts;
        }
    }

    public static void logSchemaShapeStats(OceanBaseGlobalState state, String phase) throws Exception {
        registerVpStatsShutdownHook();
        int emptySingle = 0;
        int emptyMulti = 0;
        int nonEmptySingle = 0;
        int nonEmptyMulti = 0;
        state.updateSchema();
        for (OceanBaseTable table : state.getSchema().getDatabaseTables()) {
            if (table.isView() || isVpArtifact(table.getName()) || table.getColumns().isEmpty()) {
                continue;
            }
            boolean empty = table.getNrRows(state) == 0;
            boolean singleColumn = table.getColumns().size() == 1;
            if (empty && singleColumn) {
                emptySingle++;
            } else if (empty) {
                emptyMulti++;
            } else if (singleColumn) {
                nonEmptySingle++;
            } else {
                nonEmptyMulti++;
            }
        }
        int empty = emptySingle + emptyMulti;
        int single = emptySingle + nonEmptySingle;
        int multi = emptyMulti + nonEmptyMulti;
        int total = single + multi;
        synchronized (STATS_LOCK) {
            schemaShapeSamples++;
            incrementSchemaShape("schema-total", total);
            incrementSchemaShape("schema-empty", empty);
            incrementSchemaShape("schema-single", single);
            incrementSchemaShape("schema-multi", multi);
            incrementSchemaShape("schema-empty-single", emptySingle);
            incrementSchemaShape("schema-empty-multi", emptyMulti);
            incrementSchemaShape("schema-nonempty-single", nonEmptySingle);
            incrementSchemaShape("schema-nonempty-multi", nonEmptyMulti);
        }
    }

    private static String formatVpStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("-- [vp-stats] event=process-final dbms=oceanbase-oracle");
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] oracle-runs completed=").append(completedOracleRuns);
        sb.append(" checkedPairs=").append(completedQueryPairs);
        sb.append(" attempts=").append(completedAttempts);
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] schema-shape samples=").append(schemaShapeSamples);
        for (Map.Entry<String, Integer> entry : SCHEMA_SHAPE_COUNTS.entrySet()) {
            sb.append(" ").append(entry.getKey()).append("=").append(entry.getValue());
        }
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] split-counts ")
                .append(formatEnumCounts(SplitStrategy.values(), SPLIT_STRATEGY_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] selected-shape-counts ")
                .append(formatEnumCounts(BaseShape.values(), SELECTED_BASE_SHAPE_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] reconstruction-counts ")
                .append(formatEnumCounts(ReconstructionStrategy.values(), RECONSTRUCTION_STRATEGY_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] known-suppressed-counts ")
                .append(formatEnumCounts(KnownVpBug.values(), KNOWN_BUG_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] advanced-feature-counts ")
                .append(formatEnumCounts(AdvancedFeature.values(), ADVANCED_FEATURE_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] skipped-error-counts ").append(formatStringCounts(SKIPPED_ERROR_COUNTS, 30));
        sb.append(System.lineSeparator());
        return sb.toString();
    }

    private static <E extends Enum<E>> String formatEnumCounts(E[] values, Map<E, Integer> counts) {
        return java.util.Arrays.stream(values).map(v -> v.name() + "=" + counts.getOrDefault(v, 0))
                .collect(Collectors.joining(", "));
    }

    private static String formatStringCounts(Map<String, Integer> counts, int limit) {
        return counts.entrySet().stream().sorted((a, b) -> Integer.compare(b.getValue(), a.getValue())).limit(limit)
                .map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(", "));
    }

    private static void registerVpStatsShutdownHook() {
        if (SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(OceanBaseVerticalPartitioningOracle::writeFinalVpStats));
        }
    }

    private static void writeFinalVpStats() {
        String log;
        synchronized (STATS_LOCK) {
            log = formatVpStats();
        }
        File parent = STATS_FILE.getParentFile();
        Path target = STATS_FILE.toPath();
        try {
            if (parent != null) {
                Files.createDirectories(parent.toPath());
            }
            Files.writeString(target, log, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private String sanitizeIdentifier(String value) {
        return value.replaceAll("[^A-Za-z0-9_]", "_");
    }

    private static String quote(String identifier) {
        return identifier;
    }

    private static String sqlString(String identifier) {
        return identifier.toUpperCase(Locale.ROOT).replace("'", "''");
    }

    private static boolean isVpArtifact(String tableName) {
        return tableName.toLowerCase(Locale.ROOT).startsWith("vp_");
    }
}
