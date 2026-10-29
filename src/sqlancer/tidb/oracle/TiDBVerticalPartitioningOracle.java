package sqlancer.tidb.oracle;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.oracle.ecst.EcstFeature;
import sqlancer.common.oracle.ecst.EcstOperation;
import sqlancer.common.oracle.ecst.EcstTransformationConfig;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBExpressionGenerator;
import sqlancer.tidb.TiDBProvider;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBColumn;
import sqlancer.tidb.TiDBSchema.TiDBCompositeDataType;
import sqlancer.tidb.TiDBSchema.TiDBDataType;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.ast.TiDBColumnReference;
import sqlancer.tidb.ast.TiDBConstant;
import sqlancer.tidb.ast.TiDBConstant.TiDBIntConstant;
import sqlancer.tidb.ast.TiDBAggregate;
import sqlancer.tidb.ast.TiDBAggregate.TiDBAggregateFunction;
import sqlancer.tidb.ast.TiDBExpression;
import sqlancer.tidb.ast.TiDBSelect;
import sqlancer.tidb.ast.TiDBTableReference;
import sqlancer.tidb.ast.TiDBText;
import sqlancer.tidb.visitor.TiDBVisitor;

public class TiDBVerticalPartitioningOracle implements TestOracle<TiDBGlobalState> {

    private static final AtomicLong ORACLE_RUN_ID = new AtomicLong();
    private static final String ROW_ID = "vp_rowid";
    private static final int MAX_DIFF_ROWS = 8;
    private static final int MAX_RESULT_SAMPLE = 12;
    private static final double FLOAT_ABSOLUTE_TOLERANCE = 1e-6;
    private static final double FLOAT_RELATIVE_TOLERANCE = 1e-5;
    private static final double DOUBLE_ABSOLUTE_TOLERANCE = 1e-12;
    private static final double DOUBLE_RELATIVE_TOLERANCE = 1e-9;
    private static final Object STATS_LOCK = new Object();
    private static final Map<SplitStrategy, Integer> SPLIT_STRATEGY_COUNTS = new EnumMap<>(SplitStrategy.class);
    private static final Map<BaseShape, Integer> SELECTED_BASE_SHAPE_COUNTS = new EnumMap<>(BaseShape.class);
    private static final Map<ReconstructionStrategy, Integer> RECONSTRUCTION_STRATEGY_COUNTS = new EnumMap<>(
            ReconstructionStrategy.class);
    private static final Map<KnownVpBug, Integer> KNOWN_BUG_COUNTS = new EnumMap<>(KnownVpBug.class);
    private static final Map<AdvancedFeature, Integer> ADVANCED_FEATURE_COUNTS = new EnumMap<>(AdvancedFeature.class);
    private static final Map<String, Integer> SCHEMA_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean();
    private static final AtomicLong KNOWN_BUG_LOG_ID = new AtomicLong();
    private static final File STATS_FILE = new File(new File(Main.LOG_DIRECTORY, "tidb"), "vp-stats.log");
    private static int completedOracleRuns;
    private static int completedQueryPairs;
    private static int completedAttempts;
    private static int schemaShapeSamples;

    private final TiDBGlobalState state;
    private final ExpectedErrors errors = new ExpectedErrors();

    private enum BaseShape {
        EMPTY, SINGLE_COLUMN, MULTI_COLUMN
    }

    private enum SplitStrategy {
        EMPTY_SPLIT, SINGLE_REPLICATED, SINGLE_RIGHT_ROW_ID_ONLY, SINGLE_LEFT_ROW_ID_ONLY, EVEN_SPLIT, OVERLAPPED_SPLIT,
        SKEWED_SPLIT, TYPE_DIRECTED_SPLIT, ECST_SAME_SIDE, ECST_CROSS_SIDE
    }

    private enum ReconstructionStrategy {
        JOIN, LEFT_RIGHT_JOIN, FULL_OUTER_SIMULATION, SCALAR_SUBQUERY, EXISTS_FILTER, IN_FILTER, NOT_EXISTS_ANTI,
        DERIVED_TABLE, CTE, VIEW, TEMP_TABLE, AGGREGATE_THEN_MERGE, ROW_NUMBER_ALIGNMENT, JSON_PACK, STRING_PACK,
        NATURAL_OR_USING, STRAIGHT_JOIN, LATERAL_DERIVED
    }

    private enum KnownVpBug {
        TIDB_MINMAX_BINARY_VP_DIFF, TIDB_SUM_TEXT_DECIMAL_OVERFLOW_DIFF, TIDB_TEMPORAL_CAST_NO_ZERO_DATE_NULL_DIFF,
        TIDB_BINARY_PREDICATE_PUSHDOWN_DIFF, TIDB_ORDER_BY_UNARY_MINUS_DESC_NULL_DIFF,
        TIDB_MIN_TEXT_WHITESPACE_TOPN_DIFF, TIDB_TEMPORAL_CAST_PREDICATE_PUSHDOWN_DIFF
    }

    public enum AdvancedFeature {
        ROLLUP, CORRELATED_SUBQUERY, GROUP_CONCAT, ASYMMETRIC_INDEX, CHARSET_COLLATION_COLUMN, BOUNDARY_MUTATION
    }

    public enum DependencyLayout {
        SINGLE_SIDE, SAME_SIDE, CROSS_SIDE, OVERLAPPED, INVALID
    }

    private static final List<ReconstructionStrategy> SUPPORTED_RECONSTRUCTION_STRATEGIES = List.of(
            ReconstructionStrategy.JOIN, ReconstructionStrategy.LEFT_RIGHT_JOIN,
            ReconstructionStrategy.FULL_OUTER_SIMULATION, ReconstructionStrategy.SCALAR_SUBQUERY,
            ReconstructionStrategy.EXISTS_FILTER, ReconstructionStrategy.IN_FILTER,
            ReconstructionStrategy.NOT_EXISTS_ANTI, ReconstructionStrategy.DERIVED_TABLE, ReconstructionStrategy.CTE,
            ReconstructionStrategy.VIEW, ReconstructionStrategy.TEMP_TABLE, ReconstructionStrategy.AGGREGATE_THEN_MERGE,
            ReconstructionStrategy.ROW_NUMBER_ALIGNMENT, ReconstructionStrategy.JSON_PACK,
            ReconstructionStrategy.STRING_PACK, ReconstructionStrategy.NATURAL_OR_USING,
            ReconstructionStrategy.STRAIGHT_JOIN, ReconstructionStrategy.LATERAL_DERIVED);

    private static final class VpColumn {
        private final String name;
        private final TiDBDataType type;

        private VpColumn(String name, TiDBDataType type) {
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
        private final String engine;

        private BaseTableInfo(String tableName, List<VpColumn> columns, BaseShape shape, String stableOrderStrategy,
                long rowCount, String engine) {
            this.tableName = tableName;
            this.columns = columns;
            this.shape = shape;
            this.stableOrderStrategy = stableOrderStrategy;
            this.rowCount = rowCount;
            this.engine = engine;
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
        private final String baseEngine;
        private final Map<String, String> columnCollationClauses = new LinkedHashMap<>();
        private String sourceEngine;
        private String leftEngine;
        private String rightEngine;
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
            this.baseEngine = baseEngine;
            this.sourceEngine = "InnoDB";
            this.leftEngine = "InnoDB";
            this.rightEngine = "InnoDB";
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

    /** A controlled VP query pair owned by this oracle so the existing VP machinery can be reused by ECST. */
    public static final class EcstCase {
        private final VpQueryPair pair;
        private final EcstOperation operation;
        private final String targetExpression;
        private final String baseTable;
        private final List<String> targetDependencies;
        private final List<String> leftDependencies;
        private final List<String> rightDependencies;
        private final String generationInfo;

        private EcstCase(VpQueryPair pair, EcstOperation operation, String targetExpression, String baseTable,
                List<String> targetDependencies,
                List<String> leftDependencies, List<String> rightDependencies, String generationInfo) {
            this.pair = pair;
            this.operation = operation;
            this.targetExpression = targetExpression;
            this.baseTable = baseTable;
            this.targetDependencies = List.copyOf(targetDependencies);
            this.leftDependencies = List.copyOf(leftDependencies);
            this.rightDependencies = List.copyOf(rightDependencies);
            this.generationInfo = generationInfo == null ? "" : generationInfo;
        }

        public String getOriginalQuery() {
            return pair.singleQuery;
        }

        public String getTransformedQuery() {
            return pair.splitQuery;
        }

        public String getTargetExpression() {
            return targetExpression;
        }

        public EcstOperation getOperation() {
            return operation;
        }

        public String getDescription() {
            return pair.description;
        }

        public String getBaseTable() {
            return baseTable;
        }

        public List<String> getTargetDependencies() {
            return targetDependencies;
        }

        public List<String> getLeftDependencies() {
            return leftDependencies;
        }

        public List<String> getRightDependencies() {
            return rightDependencies;
        }

        public String getGenerationInfo() {
            return generationInfo;
        }

        public List<String> getMaterializationSetup() {
            return List.copyOf(pair.schema.setupStatements);
        }
    }

    /**
     * The untransformed ECST input.  The source relation and its query are created before
     * any target or transformation configuration is selected.  The schema is retained
     * internally so that the selected transformation can reuse the same data snapshot.
     */
    public static final class EcstSeed {
        private final String baseTable;
        private final String sourceTable;
        private final String originalQuery;
        private final String targetExpression;
        private final EcstOperation operation;
        private final List<String> targetDependencies;
        private final TiDBECSTOracle.QueryShape shape;
        private final EcstTransformationConfig.ExpressionContext expressionContext;
        private final BaseTableInfo base;
        private final VpSchema schema;

        private EcstSeed(String baseTable, String sourceTable, String originalQuery, String targetExpression,
                List<String> targetDependencies, TiDBECSTOracle.QueryShape shape,
                EcstTransformationConfig.ExpressionContext expressionContext, BaseTableInfo base, VpSchema schema) {
            this.baseTable = baseTable;
            this.sourceTable = sourceTable;
            this.originalQuery = originalQuery;
            this.targetExpression = targetExpression;
            Map<String, String> lineage = new LinkedHashMap<>();
            for (String dependency : targetDependencies) {
                lineage.put(sourceTable + "." + dependency, dependency);
                lineage.put("l." + dependency, dependency);
                lineage.put("r." + dependency, dependency);
            }
            this.operation = new EcstOperation("TIDB_ECST:" + sourceTable + ":" + targetExpression,
                    targetExpression, new LinkedHashSet<>(targetDependencies),
                    new sqlancer.common.oracle.ecst.EcstExpressionShape(2, "SIGNED", Set.of("+"),
                            new LinkedHashSet<>(targetDependencies)), lineage);
            this.targetDependencies = List.copyOf(targetDependencies);
            this.shape = shape;
            this.expressionContext = expressionContext;
            this.base = base;
            this.schema = schema;
        }

        public String getBaseTable() {
            return baseTable;
        }

        public String getSourceTable() {
            return sourceTable;
        }

        public String getOriginalQuery() {
            return originalQuery;
        }

        public String getTargetExpression() {
            return targetExpression;
        }

        public EcstOperation getOperation() {
            return operation;
        }

        public List<String> getTargetDependencies() {
            return targetDependencies;
        }

        public TiDBECSTOracle.QueryShape getShape() {
            return shape;
        }

        public EcstTransformationConfig.ExpressionContext getExpressionContext() {
            return expressionContext;
        }

        public String getExpressionType() {
            return targetExpression.toUpperCase(Locale.ROOT).contains("CAST(") ? "CAST_ARITHMETIC" : "ARITHMETIC";
        }

        public String getDependencyLayout() {
            return dependencyLayout(targetDependencies, schema.leftColumns, schema.rightColumns).name();
        }

        public String getDependencyLayout(EcstTransformationConfig.PartitionLayout layout) {
            if (layout == EcstTransformationConfig.PartitionLayout.CROSS_SIDE) {
                return targetDependencies.size() >= 2 ? DependencyLayout.CROSS_SIDE.name()
                        : DependencyLayout.INVALID.name();
            }
            return targetDependencies.size() < 2 ? DependencyLayout.SINGLE_SIDE.name()
                    : DependencyLayout.SAME_SIDE.name();
        }

        public String getQueryShape() {
            return shape.toString();
        }

        /** A stable feature vector for target scheduling before a transformed case exists. */
        public EcstFeature features() {
            return new EcstFeature("TiDB", getExpressionType(), expressionContext.name(), getDependencyLayout(),
                    getQueryShape());
        }

    }

    public static final class EcstComparison {
        private final boolean comparable;
        private final boolean equal;
        private final String reason;
        private final String originalSample;
        private final String transformedSample;

        private EcstComparison(boolean comparable, boolean equal, String reason, String originalSample,
                String transformedSample) {
            this.comparable = comparable;
            this.equal = equal;
            this.reason = reason == null ? "" : reason;
            this.originalSample = originalSample;
            this.transformedSample = transformedSample;
        }

        public boolean isComparable() {
            return comparable;
        }

        public boolean isEqual() {
            return equal;
        }

        public String getReason() {
            return reason;
        }

        public String getOriginalSample() {
            return originalSample;
        }

        public String getTransformedSample() {
            return transformedSample;
        }
    }

    public static final class EcstSemanticPreconditionResult {
        private final List<String> violations;

        private EcstSemanticPreconditionResult(List<String> violations) {
            this.violations = List.copyOf(violations);
        }

        public boolean isValid() {
            return violations.isEmpty();
        }

        public List<String> getViolations() {
            return violations;
        }

        @Override
        public String toString() {
            return isValid() ? "valid" : String.join("; ", violations);
        }
    }

    private static final class ControlledSplitPlan {
        private final SplitPlan splitPlan;
        private final SplitStrategy splitStrategy;

        private ControlledSplitPlan(SplitPlan splitPlan, SplitStrategy splitStrategy) {
            this.splitPlan = splitPlan;
            this.splitStrategy = splitStrategy;
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

    public TiDBVerticalPartitioningOracle(TiDBGlobalState state) {
        this(state, true);
    }

    TiDBVerticalPartitioningOracle(TiDBGlobalState state, boolean registerVpStatistics) {
        this.state = state;
        if (registerVpStatistics) {
            registerVpStatsShutdownHook();
        }
        TiDBErrors.addExpressionErrors(errors);
        errors.add("This version of TiDB doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'");
        errors.add("You have an error in your SQL syntax");
        errors.add("doesn't yet support");
        errors.add("Unsupported");
        errors.add("Incorrect DECIMAL value");
        errors.add("Truncated incorrect");
        errors.add("Data truncation");
        errors.add("Data too long");
        errors.add("Out of range value");
        errors.add("Division by 0");
        errors.add("Invalid JSON");
        errors.add("Cannot add foreign key constraint");
        errors.add("Failed to add the foreign key constraint");
        errors.add("Cannot create table");
        errors.add("The used table type doesn't support FULLTEXT indexes");
        errors.add("doesn't support BLOB/TEXT columns");
        errors.add("Invalid default value");
        errors.add("Unknown storage engine");
        errors.add("Unknown system variable");
        errors.add("Unsupported modify column");
        errors.add("Unknown collation");
        errors.add("COLLATION");
        errors.add("Unknown character set");
    }

    @Override
    public void check() throws Exception {
        resetSessionForVerticalPartitioningOracle();
        TiDBProvider.ensureVerticalPartitioningBaseTables(state, 2);
        BaseTableInfo baseTable = selectBaseTable();
        int queryVariantsPerCombination = Math.max(1,
                state.getDbmsSpecificOptions().verticalPartitioningQueryVariantsPerCombination);
        int checkedPairs = 0;
        int attempts = 0;
        List<SplitStrategy> splitSchedule = createSplitSchedule(baseTable.shape);
        for (SplitStrategy splitStrategy : splitSchedule) {
            VpSchema vp = materializeTable(baseTable, splitStrategy);
            incrementSplitStrategy(vp.splitStrategy);
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
            cleanupIfNoBug(vp);
        }
        if (checkedPairs > 0) {
            recordCompletedVpRun(checkedPairs, attempts);
        } else {
            throw new IgnoreMeException();
        }
        TiDBProvider.replaceVerticalPartitioningTableAfterSuccessfulCheck(state, baseTable.tableName);
    }

    private BaseTableInfo selectBaseTable() throws Exception {
        state.updateSchema();
        List<TiDBTable> candidates = state.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> !t.getName().startsWith("vp_")).filter(t -> !t.getColumns().isEmpty())
                .collect(Collectors.toList());
        if (candidates.isEmpty()) {
            throw new IgnoreMeException();
        }
        return createBaseTableInfo(Randomly.fromList(candidates));
    }

    private BaseTableInfo createBaseTableInfo(TiDBTable base) throws Exception {
        List<VpColumn> sourceColumns = base.getColumns().stream()
                .map(c -> new VpColumn(c.getName(), c.getType().getPrimitiveDataType())).collect(Collectors.toList());
        long rowCount = getRowCount(base.getName());
        BaseShape shape = rowCount == 0 ? BaseShape.EMPTY
                : sourceColumns.size() == 1 ? BaseShape.SINGLE_COLUMN : BaseShape.MULTI_COLUMN;
        incrementSelectedBaseShape(shape);
        String stableOrder = getStableOrder(base, sourceColumns);
        return new BaseTableInfo(base.getName(), sourceColumns, shape, stableOrder, rowCount,
                extractEngineFromCreateTable(getCreateTable(base.getName())));
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
        List<ReconstructionStrategy> shuffled = new ArrayList<>(SUPPORTED_RECONSTRUCTION_STRATEGIES);
        Collections.shuffle(shuffled);
        return shuffled;
    }

    private void resetSessionForVerticalPartitioningOracle() throws Exception {
        tryExecuteNoSchemaRefresh("SET SESSION sql_mode = DEFAULT");
        tryExecuteNoSchemaRefresh("SET SESSION time_zone = '+00:00'");
        tryExecuteNoSchemaRefresh("SET SESSION sql_auto_is_null = OFF");
        tryExecuteNoSchemaRefresh("SET SESSION group_concat_max_len = 1048576");
        tryExecuteNoSchemaRefresh("SET SESSION tmp_table_size = DEFAULT");
        tryExecuteNoSchemaRefresh("SET SESSION max_heap_table_size = DEFAULT");
        tryExecuteNoSchemaRefresh("SET SESSION join_buffer_size = DEFAULT");
        tryExecuteNoSchemaRefresh("SET SESSION sort_buffer_size = DEFAULT");
        tryExecuteNoSchemaRefresh("SET SESSION read_buffer_size = DEFAULT");
        tryExecuteNoSchemaRefresh("SET SESSION read_rnd_buffer_size = DEFAULT");
        tryExecuteNoSchemaRefresh("SET SESSION unique_checks = DEFAULT");
        tryExecuteNoSchemaRefresh("SET SESSION foreign_key_checks = DEFAULT");
    }

    private VpSchema materializeTable(BaseTableInfo base, SplitStrategy splitStrategy) throws Exception {
        return materializeTable(base, createSplitPlan(base.columns, base.shape, splitStrategy), splitStrategy,
                Randomly.getBoolean());
    }

    private VpSchema materializeTable(BaseTableInfo base, SplitPlan splitPlan, SplitStrategy splitStrategy,
            boolean useForeignKey) throws Exception {
        String prefix = "vp_" + sanitizeIdentifier(state.getDatabaseName()) + "_" + ORACLE_RUN_ID.incrementAndGet()
                + "_";
        String source = prefix + "source";
        String left = prefix + "l";
        String right = prefix + "r";
        String view = prefix + "v";
        String temp = prefix + "joined";

        VpSchema vp = new VpSchema(prefix, base.tableName, source, left, right, view, temp, base.columns,
                splitPlan.leftColumns, splitPlan.rightColumns, splitPlan.overlapColumns, base.shape, splitStrategy,
                useForeignKey, base.stableOrderStrategy, base.rowCount, base.engine);
        try {
            initializeColumnCollations(vp);
            dropArtifacts(vp);
            createSourceTable(vp, helperEngineCandidate(base));
            populateSourceTable(vp, base.stableOrderStrategy);
            copyBaseSchemaObjectsToSource(vp);
            createPartitionTables(vp, helperEngineCandidate(base));
            populatePartitionTables(vp);
            maybeCreateHelperIndexes(vp);
            if (useForeignKey) {
                tryExecuteSetup(vp,
                        "ALTER TABLE " + quote(vp.rightTable) + " ADD CONSTRAINT " + quote(vp.prefix + "fk_r_l")
                                + " FOREIGN KEY (" + quote(ROW_ID) + ") REFERENCES " + quote(vp.leftTable) + "("
                                + quote(ROW_ID) + ")");
            }
            return vp;
        } catch (Throwable failure) {
            try {
                dropArtifacts(vp);
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw new RuntimeException(failure);
        }
    }

    /** Materializes only the source snapshot used by an ECST seed. */
    private VpSchema materializeSourceSeed(BaseTableInfo base, SplitPlan splitPlan, SplitStrategy splitStrategy)
            throws Exception {
        String prefix = "vp_" + sanitizeIdentifier(state.getDatabaseName()) + "_" + ORACLE_RUN_ID.incrementAndGet()
                + "_";
        VpSchema schema = new VpSchema(prefix, base.tableName, prefix + "source", prefix + "l", prefix + "r",
                prefix + "v", prefix + "joined", base.columns, new ArrayList<>(splitPlan.leftColumns),
                new ArrayList<>(splitPlan.rightColumns), new ArrayList<>(splitPlan.overlapColumns), base.shape,
                splitStrategy, false, base.stableOrderStrategy, base.rowCount, base.engine);
        try {
            initializeColumnCollations(schema);
            dropArtifacts(schema);
            createSourceTable(schema, helperEngineCandidate(base));
            populateSourceTable(schema, base.stableOrderStrategy);
            copyBaseSchemaObjectsToSource(schema);
            return schema;
        } catch (Throwable failure) {
            try {
                dropArtifacts(schema);
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            if (failure instanceof Exception) {
                throw (Exception) failure;
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw new RuntimeException(failure);
        }
    }

    /** Creates the source relation and original ECST query without applying a transformation. */
    public EcstSeed createEcstSeed() throws Exception {
        EcstTransformationConfig.ExpressionContext context = Randomly.fromOptions(
                EcstTransformationConfig.ExpressionContext.WHERE,
                EcstTransformationConfig.ExpressionContext.SELECT,
                EcstTransformationConfig.ExpressionContext.HAVING);
        return createEcstSeed(context);
    }

    private EcstSeed createEcstSeed(EcstTransformationConfig.ExpressionContext context) throws Exception {
        resetSessionForVerticalPartitioningOracle();
        TiDBProvider.ensureVerticalPartitioningBaseTables(state, 2);
        BaseTableInfo base = selectBaseTable();
        List<VpColumn> numeric = base.columns.stream().filter(c -> c.type.isNumeric()).collect(Collectors.toList());
        List<VpColumn> dependencies = numeric.size() >= 2 ? numeric : base.columns;
        if (dependencies.isEmpty()) {
            throw new IgnoreMeException();
        }
        if (dependencies.size() > 2) {
            dependencies = dependencies.subList(0, 2);
        }
        ControlledSplitPlan initial = createControlledSplitPlan(base.columns, dependencies,
                EcstTransformationConfig.PartitionLayout.SAME_SIDE);
        VpSchema schema = materializeSourceSeed(base, initial.splitPlan, initial.splitStrategy);
        String left = quote(dependencies.get(0).name);
        String right = quote(dependencies.size() > 1 ? dependencies.get(1).name : dependencies.get(0).name);
        String targetExpression = "(CAST(" + left + " AS SIGNED) + CAST(" + right + " AS SIGNED))";
        String originalQuery = buildEcstSeedQuery(schema, targetExpression, context);
        TiDBECSTOracle.QueryShape shape = TiDBECSTOracle.queryShapeFromSql(originalQuery, dependencies.size());
        List<String> dependencyNames = dependencies.stream().map(column -> column.name).collect(Collectors.toList());
        return new EcstSeed(base.tableName, schema.sourceTable, originalQuery, targetExpression, dependencyNames, shape,
                context, base, schema);
    }

    private String buildEcstSeedQuery(VpSchema schema, String targetExpression,
            EcstTransformationConfig.ExpressionContext context) {
        String predicate = targetExpression + " > 0";
        switch (context) {
        case WHERE:
            return "SELECT " + quote(ROW_ID) + " FROM " + quote(schema.sourceTable) + " WHERE " + predicate;
        case SELECT:
            return "SELECT " + quote(ROW_ID) + ", " + targetExpression + " AS ecst_value FROM "
                    + quote(schema.sourceTable);
        case HAVING:
            return "SELECT " + quote(ROW_ID) + ", " + targetExpression + " AS ecst_value FROM "
                    + quote(schema.sourceTable) + " GROUP BY " + quote(ROW_ID) + ", " + targetExpression
                    + " HAVING " + predicate;
        default:
            throw new IllegalArgumentException("Unsupported ECST seed context: " + context);
        }
    }

    /** Applies the selected ECST transformation to an existing source seed. */
    public EcstCase applyEcstTransformation(EcstSeed seed, EcstTransformationConfig config) throws Exception {
        Objects.requireNonNull(seed, "seed");
        if (config.getRepresentation() != EcstTransformationConfig.Representation.VERTICAL_PARTITIONED
                || config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.ON) {
            throw new IgnoreMeException();
        }
        if (config.getExpressionContext() != seed.expressionContext) {
            throw new IgnoreMeException();
        }
        ReconstructionStrategy reconstructionStrategy;
        switch (config.getReconstruction()) {
        case JOIN:
            reconstructionStrategy = ReconstructionStrategy.JOIN;
            break;
        case DERIVED_TABLE:
            reconstructionStrategy = ReconstructionStrategy.DERIVED_TABLE;
            break;
        default:
            throw new IgnoreMeException();
        }
        BaseTableInfo base = seed.base;
        List<VpColumn> dependencies = seed.schema.columns.stream()
                .filter(column -> seed.targetDependencies.stream().anyMatch(name -> name.equals(column.name)))
                .collect(Collectors.toList());
        if (config.getPartitionLayout() == EcstTransformationConfig.PartitionLayout.CROSS_SIDE
                && dependencies.size() < 2) {
            throw new IgnoreMeException();
        }

        ControlledSplitPlan controlled = createControlledSplitPlan(base.columns, dependencies,
                config.getPartitionLayout());
        VpSchema vp = reconfigureSeedPartitions(seed.schema, controlled.splitPlan, controlled.splitStrategy);
        String targetExpression = seed.targetExpression;
        String singleQuery = seed.originalQuery;
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, reconstructionStrategy, extraSetup);
        String splitQuery = replaceSourceFrom(singleQuery, vp.sourceTable, relation);
        VpQueryPair pair = new VpQueryPair(vp, reconstructionStrategy, singleQuery, splitQuery, extraSetup,
                "ECST config=" + config.stableKey() + ", target=" + targetExpression, false);
        List<String> dependencyNames = seed.targetDependencies;
        List<String> leftDependencies = dependencies.stream().filter(column -> containsColumn(controlled.splitPlan.leftColumns,
                column.name)).map(column -> column.name).collect(Collectors.toList());
        List<String> rightDependencies = dependencies.stream().filter(column -> containsColumn(controlled.splitPlan.rightColumns,
                column.name)).map(column -> column.name).collect(Collectors.toList());
        String generationInfo = "rowCount=" + base.rowCount + ",engine=" + base.engine + ",stableOrder="
                + base.stableOrderStrategy + ",splitStrategy=" + controlled.splitStrategy + ",leftColumns="
                + columnNames(controlled.splitPlan.leftColumns) + ",rightColumns="
                + columnNames(controlled.splitPlan.rightColumns) + ",overlapColumns="
                + columnNames(controlled.splitPlan.overlapColumns);
        return new EcstCase(pair, seed.operation, targetExpression, base.tableName,
                dependencyNames, leftDependencies, rightDependencies, generationInfo);
    }

    private VpSchema reconfigureSeedPartitions(VpSchema schema, SplitPlan splitPlan, SplitStrategy strategy)
            throws Exception {
        executeNoSchemaRefresh("DROP TABLE IF EXISTS " + quote(schema.rightTable));
        executeNoSchemaRefresh("DROP TABLE IF EXISTS " + quote(schema.leftTable));
        schema.leftColumns.clear();
        schema.leftColumns.addAll(splitPlan.leftColumns);
        schema.rightColumns.clear();
        schema.rightColumns.addAll(splitPlan.rightColumns);
        schema.overlapColumns.clear();
        schema.overlapColumns.addAll(splitPlan.overlapColumns);
        // The schema object is retained by the seed, so the selected split is the one used by the case.
        createPartitionTables(schema, helperEngineCandidate(schemaBaseInfo(schema)));
        populatePartitionTables(schema);
        maybeCreateHelperIndexes(schema);
        schema.setupStatements.add("ECST_RECONFIGURE_SPLIT=" + strategy);
        return schema;
    }

    private BaseTableInfo schemaBaseInfo(VpSchema schema) {
        return new BaseTableInfo(schema.baseTable, schema.columns, schema.baseShape, schema.stableOrderStrategy,
                schema.baseRowCount, schema.baseEngine);
    }

    /** Drops all source and helper objects owned by a seed. */
    public void cleanupEcstSeed(EcstSeed seed) throws Exception {
        if (seed != null && seed.schema != null) {
            dropArtifacts(seed.schema);
        }
    }

    /** Compatibility wrapper for callers that still use the old one-stage API. */
    public EcstCase createEcstCase(EcstTransformationConfig config) throws Exception {
        if (config.getRepresentation() != EcstTransformationConfig.Representation.VERTICAL_PARTITIONED
                || config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.ON) {
            throw new IgnoreMeException();
        }
        EcstSeed seed = createEcstSeed(config.getExpressionContext());
        try {
            return applyEcstTransformation(seed, config);
        } catch (Exception | Error failure) {
            cleanupEcstSeed(seed);
            throw failure;
        }
    }

    public EcstComparison compareEcstCase(EcstCase ecstCase) throws Exception {
        VpQueryPair pair = ecstCase.pair;
        try {
            for (String setup : pair.extraSetupStatements) {
                executeNoSchemaRefresh(setup);
                pair.schema.setupStatements.add(setup);
            }
            RowIdValidation rowIds = validateRowIdBijection(pair.schema);
            if (!rowIds.valid()) {
                return incomparable("row-id bijection failed: " + rowIds.reason);
            }
            QueryResultMultiset original = getResultMultiset(pair.singleQuery);
            QueryResultMultiset transformed = getResultMultiset(pair.splitQuery);
            return comparable(resultMultisetsMatch(original, transformed), sample(original.rows),
                    sample(transformed.rows), "result multisets compared");
        } catch (IgnoreMeException | SQLException failure) {
            return incomparable("result comparison unavailable: " + failure.getMessage());
        }
    }

    private EcstComparison comparable(boolean equal, String originalSample, String transformedSample, String reason) {
        return new EcstComparison(true, equal, reason, originalSample, transformedSample);
    }

    private EcstComparison incomparable(String reason) {
        return new EcstComparison(false, false, reason, "", "");
    }

    private static final class RowIdValidation {
        private final boolean valid;
        private final String reason;

        private RowIdValidation(boolean valid, String reason) {
            this.valid = valid;
            this.reason = reason;
        }

        private boolean valid() {
            return valid;
        }
    }

    private RowIdValidation validateRowIdBijection(VpSchema schema) throws SQLException {
        long sourceCount = countRows(schema.sourceTable);
        long leftCount = countRows(schema.leftTable);
        long rightCount = countRows(schema.rightTable);
        if (sourceCount != leftCount || sourceCount != rightCount) {
            return new RowIdValidation(false, "row counts source=" + sourceCount + ", left=" + leftCount
                    + ", right=" + rightCount);
        }
        long missingRight = countMissingRowIds(schema.leftTable, schema.rightTable);
        if (missingRight != 0) {
            return new RowIdValidation(false, "left-to-right missing row ids=" + missingRight);
        }
        long missingLeft = countMissingRowIds(schema.rightTable, schema.leftTable);
        if (missingLeft != 0) {
            return new RowIdValidation(false, "right-to-left missing row ids=" + missingLeft);
        }
        return new RowIdValidation(true, "row-id bijection verified");
    }

    private long countRows(String table) throws SQLException {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM " + quote(table))) {
            return result.next() ? result.getLong(1) : -1;
        }
    }

    private long countMissingRowIds(String leftTable, String rightTable) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + quote(leftTable) + " l LEFT JOIN " + quote(rightTable)
                + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID) + " WHERE r." + quote(ROW_ID)
                + " IS NULL";
        try (Statement statement = state.getConnection().createStatement(); ResultSet result = statement.executeQuery(sql)) {
            return result.next() ? result.getLong(1) : -1;
        }
    }

    public void cleanupEcstCase(EcstCase ecstCase) throws Exception {
        dropArtifacts(ecstCase.pair.schema);
    }

    /**
     * Checks semantic obligations before plan analysis or result comparison. This is deliberately independent from
     * both the execution-context analyzer and the result oracle.
     */
    public EcstSemanticPreconditionResult checkSemanticPreconditions(EcstCase ecstCase) {
        List<String> violations = new ArrayList<>();
        if (ecstCase == null || ecstCase.pair == null || ecstCase.pair.schema == null) {
            violations.add("missing ECST case or materialized schema");
            return new EcstSemanticPreconditionResult(violations);
        }
        if (ecstCase.targetExpression == null || ecstCase.targetExpression.isEmpty()) {
            violations.add("target expression is empty");
        }
        String normalizedTarget = ecstCase.targetExpression == null ? ""
                : ecstCase.targetExpression.toLowerCase(java.util.Locale.ROOT).replace("`", "");
        if (ecstCase.targetDependencies.isEmpty()) {
            violations.add("target has no dependency columns");
        }
        if (new java.util.HashSet<>(ecstCase.targetDependencies).size() != ecstCase.targetDependencies.size()) {
            violations.add("target dependencies contain duplicates");
        }
        if (ecstCase.pair.singleQuery == null || ecstCase.pair.singleQuery.isEmpty()
                || ecstCase.pair.splitQuery == null || ecstCase.pair.splitQuery.isEmpty()) {
            violations.add("original or transformed query is empty");
        }
        if (ecstCase.pair.singleQuery != null && ecstCase.pair.splitQuery != null
                && ecstCase.pair.singleQuery.equals(ecstCase.pair.splitQuery)) {
            violations.add("transformation did not change the query representation");
        }
        if (ecstCase.pair.singleQuery != null && !ecstCase.pair.singleQuery.contains(quote(ROW_ID))) {
            violations.add("original query does not preserve the row identity projection");
        }
        if (ecstCase.pair.splitQuery != null && !ecstCase.pair.splitQuery.contains(quote(ROW_ID))) {
            violations.add("transformed query does not preserve the row identity projection");
        }
        for (String dependency : ecstCase.targetDependencies) {
            if (!normalizedTarget.contains(dependency.toLowerCase(java.util.Locale.ROOT))) {
                violations.add("target expression does not reference dependency: " + dependency);
            }
            if (!ecstCase.leftDependencies.contains(dependency) && !ecstCase.rightDependencies.contains(dependency)) {
                violations.add("dependency is absent from both partitions: " + dependency);
            }
            VpColumn sourceColumn = findColumn(ecstCase.pair.schema.columns, dependency);
            VpColumn leftColumn = findColumn(ecstCase.pair.schema.leftColumns, dependency);
            VpColumn rightColumn = findColumn(ecstCase.pair.schema.rightColumns, dependency);
            if (sourceColumn == null || leftColumn == null && rightColumn == null) {
                violations.add("dependency has no source/partition column mapping: " + dependency);
            } else if (leftColumn != null && !sameType(sourceColumn, leftColumn)
                    || rightColumn != null && !sameType(sourceColumn, rightColumn)) {
                violations.add("dependency type changed across materialization: " + dependency);
            }
        }
        if (!containsSetupToken(ecstCase.pair.schema.setupStatements, quote(ROW_ID) + " BIGINT NOT NULL PRIMARY KEY")) {
            violations.add("materialized partitions do not prove unique non-null row identity");
        }
        String transformed = ecstCase.pair.splitQuery == null ? ""
                : ecstCase.pair.splitQuery.toLowerCase(java.util.Locale.ROOT);
        if (!transformed.contains(ecstCase.pair.schema.leftTable.toLowerCase(java.util.Locale.ROOT))
                || !transformed.contains(ecstCase.pair.schema.rightTable.toLowerCase(java.util.Locale.ROOT))) {
            violations.add("transformed query does not reference both materialized partitions");
        }
        if (ecstCase.pair.schema.splitStrategy == SplitStrategy.ECST_CROSS_SIDE) {
            if (ecstCase.targetDependencies.size() < 2) {
                violations.add("CROSS_SIDE requires at least two distinct dependencies");
            }
            if (ecstCase.leftDependencies.isEmpty() || ecstCase.rightDependencies.isEmpty()) {
                violations.add("CROSS_SIDE has no dependency on one side");
            }
            for (String dependency : ecstCase.targetDependencies) {
                if (ecstCase.leftDependencies.contains(dependency) && ecstCase.rightDependencies.contains(dependency)) {
                    violations.add("CROSS_SIDE dependency is replicated on both sides: " + dependency);
                }
            }
        }
        return new EcstSemanticPreconditionResult(violations);
    }

    private static VpColumn findColumn(List<VpColumn> columns, String name) {
        for (VpColumn column : columns) {
            if (column.name.equalsIgnoreCase(name)) {
                return column;
            }
        }
        return null;
    }

    private static boolean sameType(VpColumn first, VpColumn second) {
        return first.type == second.type || first.type != null && first.type.equals(second.type);
    }

    private static boolean containsSetupToken(List<String> statements, String token) {
        String expected = token.replace("`", "").replaceAll("\\s+", " ").toLowerCase(java.util.Locale.ROOT);
        for (String statement : statements) {
            String normalized = statement.replace("`", "").replaceAll("\\s+", " ").toLowerCase(java.util.Locale.ROOT);
            if (normalized.contains(expected)) {
                return true;
            }
        }
        return false;
    }

    private ControlledSplitPlan createControlledSplitPlan(List<VpColumn> columns, List<VpColumn> dependencies,
            EcstTransformationConfig.PartitionLayout layout) {
        List<VpColumn> left = new ArrayList<>();
        List<VpColumn> right = new ArrayList<>();
        List<VpColumn> overlap = new ArrayList<>();
        if (layout == EcstTransformationConfig.PartitionLayout.SAME_SIDE) {
            left.addAll(dependencies);
            for (VpColumn column : columns) {
                if (!containsColumn(left, column.name)) {
                    right.add(column);
                }
            }
            return new ControlledSplitPlan(new SplitPlan(left, right, overlap), SplitStrategy.ECST_SAME_SIDE);
        }

        if (dependencies.size() < 2) {
            throw new IgnoreMeException();
        }
        left.add(dependencies.get(0));
        VpColumn second = dependencies.get(1);
        right.add(second);
        for (VpColumn column : columns) {
            if (containsColumn(left, column.name) || containsColumn(right, column.name)) {
                continue;
            }
            if (left.size() <= right.size()) {
                left.add(column);
            } else {
                right.add(column);
            }
        }
        return new ControlledSplitPlan(new SplitPlan(left, right, overlap), SplitStrategy.ECST_CROSS_SIDE);
    }

    private static DependencyLayout dependencyLayout(List<String> dependencies, List<VpColumn> left,
            List<VpColumn> right) {
        if (dependencies == null || dependencies.size() < 2) {
            return DependencyLayout.SINGLE_SIDE;
        }
        boolean allLeft = dependencies.stream().allMatch(dependency -> containsColumnStatic(left, dependency));
        boolean allRight = dependencies.stream().allMatch(dependency -> containsColumnStatic(right, dependency));
        boolean anyBoth = dependencies.stream().anyMatch(dependency -> containsColumnStatic(left, dependency)
                && containsColumnStatic(right, dependency));
        if (anyBoth) {
            return DependencyLayout.OVERLAPPED;
        }
        boolean eachExactlyOne = dependencies.stream().allMatch(dependency ->
                containsColumnStatic(left, dependency) ^ containsColumnStatic(right, dependency));
        boolean hasLeft = dependencies.stream().anyMatch(dependency -> containsColumnStatic(left, dependency));
        boolean hasRight = dependencies.stream().anyMatch(dependency -> containsColumnStatic(right, dependency));
        if (eachExactlyOne && hasLeft && hasRight) {
            return DependencyLayout.CROSS_SIDE;
        }
        if (allLeft && !hasRight || allRight && !hasLeft) {
            return DependencyLayout.SAME_SIDE;
        }
        return DependencyLayout.INVALID;
    }

    private static boolean containsColumnStatic(List<VpColumn> columns, String name) {
        return columns != null && columns.stream().anyMatch(column -> column.name.equalsIgnoreCase(name));
    }

    static DependencyLayout classifyDependencyLayoutForTest(List<String> dependencies, List<String> left,
            List<String> right) {
        List<VpColumn> leftColumns = left.stream().map(name -> new VpColumn(name, null)).collect(Collectors.toList());
        List<VpColumn> rightColumns = right.stream().map(name -> new VpColumn(name, null)).collect(Collectors.toList());
        return dependencyLayout(dependencies, leftColumns, rightColumns);
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

    private String getStableOrder(TiDBTable base, List<VpColumn> columns) {
        List<String> primary = base.getColumns().stream().filter(TiDBColumn::isPrimaryKey).map(c -> quote(c.getName()))
                .collect(Collectors.toList());
        if (!primary.isEmpty()) {
            return String.join(", ", primary);
        }
        List<String> comparable = columns.stream().filter(c -> c.type != TiDBDataType.FLOATING).map(c -> quote(c.name))
                .collect(Collectors.toList());
        if (!comparable.isEmpty()) {
            return String.join(", ", comparable);
        }
        return null;
    }

    private long getRowCount(String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + quote(tableName);
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (!rs.next()) {
                throw new AssertionError("COUNT(*) returned no row for " + tableName);
            }
            return rs.getLong(1);
        }
    }

    private String helperEngineCandidate(BaseTableInfo base) {
        if (state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine) {
            return base.engine;
        }
        return "InnoDB";
    }

    private void createSourceTable(VpSchema vp, String preferredEngine) throws Exception {
        vp.sourceEngine = createTableWithEngineFallback(vp, vp.sourceTable, sourceColumnDefs(vp), preferredEngine);
    }

    private void initializeColumnCollations(VpSchema vp) {
        for (VpColumn column : vp.columns) {
            String clause = "";
            if (deepExplorationEnabled() && isCharacterLike(column.type) && Randomly.getBoolean()) {
                clause = " CHARACTER SET utf8mb4 COLLATE "
                        + Randomly.fromOptions("utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_0900_ai_ci");
                recordAdvancedFeature(AdvancedFeature.CHARSET_COLLATION_COLUMN);
            }
            vp.columnCollationClauses.put(column.name, clause);
        }
    }

    private List<String> sourceColumnDefs(VpSchema vp) {
        List<String> columnDefs = new ArrayList<>();
        columnDefs.add(quote(ROW_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY");
        for (VpColumn column : vp.columns) {
            columnDefs.add(columnDef(vp, column));
        }
        return columnDefs;
    }

    private void populateSourceTable(VpSchema vp, String stableOrder) throws Exception {
        try {
            executeSetup(vp, populateSourceSQL(vp, stableOrder));
        } catch (AssertionError e) {
            if (!canFallbackHelperEngine(vp.sourceEngine)) {
                throw e;
            }
            vp.sourceEngine = recreateHelperTableWithInnoDB(vp, vp.sourceTable, sourceColumnDefs(vp));
            executeSetup(vp, populateSourceSQL(vp, stableOrder));
        }
    }

    private String populateSourceSQL(VpSchema vp, String stableOrder) {
        String columnList = vp.columns.stream().map(c -> quote(c.name)).collect(Collectors.joining(", "));
        String sql = "INSERT INTO " + quote(vp.sourceTable) + " (" + columnList + ") SELECT " + columnList + " FROM "
                + quote(vp.baseTable);
        if (stableOrder != null) {
            sql += " ORDER BY " + stableOrder;
        }
        if (state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows > 0) {
            sql += " LIMIT " + state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows;
        }
        return sql;
    }

    private void createPartitionTables(VpSchema vp, String preferredEngine) throws Exception {
        vp.leftEngine = createPartitionTable(vp, vp.leftTable, vp.leftColumns, preferredEngine);
        vp.rightEngine = createPartitionTable(vp, vp.rightTable, vp.rightColumns, preferredEngine);
    }

    private String createPartitionTable(VpSchema vp, String tableName, List<VpColumn> columns, String preferredEngine)
            throws Exception {
        return createTableWithEngineFallback(vp, tableName, partitionColumnDefs(vp, columns), preferredEngine);
    }

    private List<String> partitionColumnDefs(VpSchema vp, List<VpColumn> columns) {
        List<String> defs = new ArrayList<>();
        defs.add(quote(ROW_ID) + " BIGINT NOT NULL PRIMARY KEY");
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

    private boolean isCharacterLike(TiDBDataType type) {
        return type == TiDBDataType.TEXT || type == TiDBDataType.CHAR;
    }

    private String createTableWithEngineFallback(VpSchema vp, String tableName, List<String> columnDefs,
            String preferredEngine) throws Exception {
        try {
            executeSetup(vp, createTableSQL(tableName, columnDefs, preferredEngine));
            return preferredEngine;
        } catch (AssertionError e) {
            if (deepExplorationEnabled() && containsCharsetCollation(columnDefs)) {
                List<String> plainDefs = plainColumnDefs(tableName, vp);
                try {
                    executeSetup(vp, createTableSQL(tableName, plainDefs, preferredEngine));
                    return preferredEngine;
                } catch (AssertionError ignored) {
                    columnDefs = plainDefs;
                }
            }
            if (!canFallbackHelperEngine(preferredEngine)) {
                throw e;
            }
            return recreateHelperTableWithInnoDB(vp, tableName, columnDefs);
        }
    }

    private boolean containsCharsetCollation(List<String> columnDefs) {
        return columnDefs.stream().anyMatch(def -> def.toUpperCase(Locale.ROOT).contains(" COLLATE "));
    }

    private List<String> plainColumnDefs(String tableName, VpSchema vp) {
        if (tableName.equals(vp.sourceTable)) {
            List<String> defs = new ArrayList<>();
            defs.add(quote(ROW_ID) + " BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY");
            vp.columns.stream().map(this::plainColumnDef).forEach(defs::add);
            return defs;
        }
        List<VpColumn> columns = tableName.equals(vp.leftTable) ? vp.leftColumns : vp.rightColumns;
        List<String> defs = new ArrayList<>();
        defs.add(quote(ROW_ID) + " BIGINT NOT NULL PRIMARY KEY");
        columns.stream().map(this::plainColumnDef).forEach(defs::add);
        return defs;
    }

    private String createTableSQL(String tableName, List<String> columnDefs, String engine) {
        String sql = "CREATE TABLE " + quote(tableName) + " (" + String.join(", ", columnDefs) + ")";
        if (engine != null && !engine.isEmpty()) {
            sql += " ENGINE=" + engine;
        }
        return sql;
    }

    private boolean canFallbackHelperEngine(String engine) {
        return state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine && engine != null
                && !"InnoDB".equalsIgnoreCase(engine);
    }

    private String recreateHelperTableWithInnoDB(VpSchema vp, String tableName, List<String> columnDefs)
            throws Exception {
        executeSetup(vp, "DROP TABLE IF EXISTS " + quote(tableName));
        executeSetup(vp, createTableSQL(tableName, columnDefs, "InnoDB"));
        return "InnoDB";
    }

    private void populatePartitionTables(VpSchema vp) throws Exception {
        vp.leftEngine = populatePartitionTable(vp, vp.leftTable, vp.leftColumns, vp.leftEngine);
        vp.rightEngine = populatePartitionTable(vp, vp.rightTable, vp.rightColumns, vp.rightEngine);
    }

    private String populatePartitionTable(VpSchema vp, String tableName, List<VpColumn> columns, String engine)
            throws Exception {
        try {
            executeSetup(vp, populatePartitionSQL(tableName, columns, vp.sourceTable));
            return engine;
        } catch (AssertionError e) {
            if (!canFallbackHelperEngine(engine)) {
                throw e;
            }
            String fallbackEngine = recreateHelperTableWithInnoDB(vp, tableName, partitionColumnDefs(vp, columns));
            executeSetup(vp, populatePartitionSQL(tableName, columns, vp.sourceTable));
            return fallbackEngine;
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
        return columns.stream().filter(c -> isTiDBHelperIndexable(c.type)).collect(Collectors.toList());
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
        try {
            executeSetup(vp, sql);
        } catch (AssertionError ignored) {
            vp.setupStatements.add("-- skipped helper index: " + sql);
        } catch (Exception ignored) {
            vp.setupStatements.add("-- skipped helper index: " + sql);
        }
    }

    private boolean isTiDBHelperIndexable(TiDBDataType type) {
        return type != TiDBDataType.TEXT && type != TiDBDataType.CHAR && type != TiDBDataType.BLOB;
    }

    private void copyBaseSchemaObjectsToSource(VpSchema vp) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSourceCopySchemaObjects) {
            return;
        }
        List<String> tableElements;
        try {
            tableElements = extractTableElements(getCreateTable(vp.baseTable));
        } catch (SQLException e) {
            return;
        }
        int copied = 0;
        for (String element : tableElements) {
            String normalized = element.trim().toUpperCase(java.util.Locale.ROOT);
            if (!isCopyableSourceSchemaObject(normalized)) {
                continue;
            }
            String rewritten = rewriteSchemaObjectForSource(vp, element, copied++);
            try {
                executeSetup(vp, "ALTER TABLE " + quote(vp.sourceTable) + " ADD " + rewritten);
            } catch (AssertionError ignored) {
            } catch (Exception ignored) {
            }
        }
    }

    private boolean isCopyableSourceSchemaObject(String normalizedElement) {
        return normalizedElement.startsWith("KEY ") || normalizedElement.startsWith("INDEX ")
                || normalizedElement.startsWith("UNIQUE KEY ") || normalizedElement.startsWith("UNIQUE INDEX ")
                || normalizedElement.startsWith("CONSTRAINT ") && !normalizedElement.contains(" FOREIGN KEY ");
    }

    private String rewriteSchemaObjectForSource(VpSchema vp, String element, int ordinal) {
        String rewritten = element.trim();
        rewritten = rewritten.replaceFirst("(?i)^KEY\\s+`[^`]+`", "KEY " + quote(vp.prefix + "src_idx_" + ordinal));
        rewritten = rewritten.replaceFirst("(?i)^INDEX\\s+`[^`]+`", "INDEX " + quote(vp.prefix + "src_idx_" + ordinal));
        rewritten = rewritten.replaceFirst("(?i)^UNIQUE\\s+KEY\\s+`[^`]+`",
                "UNIQUE KEY " + quote(vp.prefix + "src_idx_" + ordinal));
        rewritten = rewritten.replaceFirst("(?i)^UNIQUE\\s+INDEX\\s+`[^`]+`",
                "UNIQUE INDEX " + quote(vp.prefix + "src_idx_" + ordinal));
        rewritten = rewritten.replaceFirst("(?i)^CONSTRAINT\\s+`[^`]+`",
                "CONSTRAINT " + quote(vp.prefix + "src_chk_" + ordinal));
        return rewritten;
    }

    private List<String> extractTableElements(String createTable) {
        int start = createTable.indexOf('(');
        int end = createTable.lastIndexOf(')');
        if (start < 0 || end <= start) {
            return List.of();
        }
        String body = createTable.substring(start + 1, end);
        List<String> elements = splitTopLevelComma(body);
        return elements.stream().map(String::trim).filter(s -> !s.startsWith("`")).collect(Collectors.toList());
    }

    private List<String> splitTopLevelComma(String body) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inBacktick = false;
        int start = 0;
        for (int i = 0; i < body.length(); i++) {
            char ch = body.charAt(i);
            if (ch == '\'' && !inBacktick) {
                inSingleQuote = !inSingleQuote;
            } else if (ch == '`' && !inSingleQuote) {
                inBacktick = !inBacktick;
            } else if (!inSingleQuote && !inBacktick) {
                if (ch == '(') {
                    depth++;
                } else if (ch == ')') {
                    depth--;
                } else if (ch == ',' && depth == 0) {
                    parts.add(body.substring(start, i));
                    start = i + 1;
                }
            }
        }
        parts.add(body.substring(start));
        return parts;
    }

    private VpQueryPair generateRandomRewritePair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        SourceAst sourceAst = createSourceAst(vp);
        TiDBExpressionGenerator gen = new TiDBExpressionGenerator(state).setColumns(sourceAst.columns);
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()) {
            return generateCorrelatedSubqueryPair(vp, strategy, sourceAst);
        }
        TiDBSelect select = new TiDBSelect();
        int fetchColumns = Math.max(1, Randomly.smallNumber() + 1);
        boolean useAggregate = Randomly.getBoolean();
        boolean hasWindowExpression = false;
        List<TiDBExpression> selected = new ArrayList<>();
        List<TiDBExpression> nonAggregateExpressions = new ArrayList<>();
        if (useAggregate) {
            selected.add(generateVpAggregate(gen, sourceAst.columns));
            for (int i = 1; i < fetchColumns; i++) {
                TiDBExpression expr = new TiDBColumnReference(Randomly.fromList(sourceAst.columns));
                selected.add(expr);
                nonAggregateExpressions.add(expr);
            }
        } else {
            for (int i = 0; i < fetchColumns; i++) {
                TiDBExpression expr;
                if (advancedFeaturesEnabled() && Randomly.getBooleanWithRatherLowProbability()) {
                    expr = generateAdvancedSelectExpression(sourceAst, gen);
                    hasWindowExpression = true;
                } else {
                    expr = gen.generateExpression();
                    nonAggregateExpressions.add(expr);
                }
                selected.add(expr);
            }
        }
        if (selected.isEmpty()) {
            selected.add(new TiDBColumnReference(Randomly.fromList(sourceAst.columns)));
        }
        select.setFetchColumns(selected);
        select.setFromList(List.of(new TiDBTableReference(sourceAst.table)));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (useAggregate) {
            List<TiDBExpression> groupByExpressions = chooseGroupByExpressions(nonAggregateExpressions);
            select.setGroupByExpressions(maybeAppendRollup(groupByExpressions, hasWindowExpression,
                    isRollupSafe(nonAggregateExpressions, groupByExpressions)));
        } else if (!hasWindowExpression && Randomly.getBooleanWithRatherLowProbability()) {
            List<TiDBExpression> groupByExpressions = chooseGroupByExpressions(nonAggregateExpressions);
            select.setGroupByExpressions(maybeAppendRollup(groupByExpressions, hasWindowExpression,
                    isRollupSafe(nonAggregateExpressions, groupByExpressions)));
            if (Randomly.getBoolean() && !groupByExpressions.isEmpty()) {
                select.setHavingClause(generateHavingExpression(gen, sourceAst.columns));
            }
        }
        if (Randomly.getBoolean()) {
            List<TiDBExpression> orderBys = nonAggregateExpressions.isEmpty()
                    ? List.of(new TiDBColumnReference(sourceAst.rowIdColumn)) : gen.generateOrderBys();
            select.setOrderByClauses(orderBys);
        }
        boolean orderedComparisonEnabled = isRowPreservingQuery(useAggregate, select)
                && hasVpRowIdTieBreaker(select, sourceAst.rowIdColumn);
        if (Randomly.getBoolean()) {
            boolean rowPreservingQuery = isRowPreservingQuery(useAggregate, select);
            if (select.getOrderByClauses().isEmpty()) {
                select.setOrderByClauses(List.of(new TiDBColumnReference(sourceAst.rowIdColumn)));
                orderedComparisonEnabled = rowPreservingQuery;
            } else if (rowPreservingQuery) {
                List<TiDBExpression> orderBys = new ArrayList<>(select.getOrderByClauses());
                orderBys.add(new TiDBColumnReference(sourceAst.rowIdColumn));
                select.setOrderByClauses(orderBys);
                orderedComparisonEnabled = true;
            } else {
                select.setOrderByClauses(List.of(new TiDBColumnReference(sourceAst.rowIdColumn)));
                orderedComparisonEnabled = false;
            }
            select.setLimitClause(TiDBConstant.createIntConstant(Randomly.getNotCachedInteger(0, 25)));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                select.setOffsetClause(TiDBConstant.createIntConstant(Randomly.getNotCachedInteger(0, 5)));
            }
        }
        String singleQuery = TiDBVisitor.asString(select);
        if (advancedFeaturesEnabled() && !useAggregate && select.getOrderByClauses().isEmpty()
                && select.getLimitClause() == null && select.getOffsetClause() == null
                && Randomly.getBooleanWithSmallProbability()) {
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

    private boolean isRollupSafe(List<TiDBExpression> nonAggregateExpressions,
            List<TiDBExpression> groupByExpressions) {
        if (nonAggregateExpressions.size() != groupByExpressions.size()) {
            return false;
        }
        List<String> groupBySql = groupByExpressions.stream().map(TiDBVisitor::asString).collect(Collectors.toList());
        for (TiDBExpression expression : nonAggregateExpressions) {
            if (!groupBySql.contains(TiDBVisitor.asString(expression))) {
                return false;
            }
        }
        return true;
    }

    private List<TiDBExpression> maybeAppendRollup(List<TiDBExpression> groupByExpressions, boolean hasWindowExpression,
            boolean rollupSafe) {
        if (!advancedFeaturesEnabled() || hasWindowExpression || !rollupSafe || groupByExpressions.isEmpty()
                || !Randomly.getBooleanWithSmallProbability()) {
            return groupByExpressions;
        }
        recordAdvancedFeature(AdvancedFeature.ROLLUP);
        List<TiDBExpression> rollup = new ArrayList<>(groupByExpressions);
        int last = rollup.size() - 1;
        rollup.set(last, new TiDBText(TiDBVisitor.asString(rollup.get(last)) + " WITH ROLLUP"));
        return rollup;
    }

    private VpQueryPair generateCorrelatedSubqueryPair(VpSchema vp, ReconstructionStrategy strategy,
            SourceAst sourceAst) throws Exception {
        if (strategy == ReconstructionStrategy.TEMP_TABLE) {
            throw new IgnoreMeException();
        }
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

    private boolean isGroupByOrdinalLiteral(TiDBExpression expression) {
        return expression instanceof TiDBIntConstant;
    }

    private List<TiDBExpression> chooseGroupByExpressions(List<TiDBExpression> nonAggregateExpressions) {
        List<TiDBExpression> groupable = nonAggregateExpressions.stream().filter(e -> !isGroupByOrdinalLiteral(e))
                .collect(Collectors.toList());
        if (!advancedFeaturesEnabled() || groupable.size() <= 1) {
            return groupable;
        }
        Collections.shuffle(groupable);
        return groupable.subList(0, state.getRandomly().getInteger(1, Math.min(3, groupable.size()) + 1));
    }

    private TiDBExpression generateHavingExpression(TiDBExpressionGenerator gen, List<TiDBColumn> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            return new TiDBText(
                    Randomly.fromOptions("COUNT(*) > 0", "SUM(" + numericExpressionForSQL(columns) + ") IS NOT NULL",
                            "MIN(" + simpleColumnOrRowIdForSQL(columns) + ") <=> MAX("
                                    + simpleColumnOrRowIdForSQL(columns) + ")"));
        }
        return gen.generateExpression();
    }

    private TiDBExpression generateAdvancedSelectExpression(SourceAst sourceAst, TiDBExpressionGenerator gen) {
        String order = simpleOrderExpressionForSQL(sourceAst.columns) + ", " + quote(ROW_ID);
        String value = simpleColumnOrRowIdForSQL(sourceAst.columns);
        String numeric = numericExpressionForSQL(sourceAst.columns);
        return new TiDBText(Randomly.fromOptions("ROW_NUMBER() OVER (ORDER BY " + order + ")",
                "RANK() OVER (ORDER BY " + order + ")", "LAG(" + value + ") OVER (ORDER BY " + quote(ROW_ID) + ")",
                "SUM(" + numeric + ") OVER (ORDER BY " + quote(ROW_ID) + ")"));
    }

    private String createUnionAllBranch(SourceAst sourceAst, TiDBExpressionGenerator gen, int fetchColumns) {
        TiDBSelect right = new TiDBSelect();
        List<TiDBExpression> selected = new ArrayList<>();
        for (int i = 0; i < fetchColumns; i++) {
            selected.add(Randomly.getBooleanWithRatherLowProbability()
                    ? generateAdvancedSelectExpression(sourceAst, gen) : gen.generateExpression());
        }
        right.setFetchColumns(selected);
        right.setFromList(List.of(new TiDBTableReference(sourceAst.table)));
        if (Randomly.getBoolean()) {
            right.setWhereClause(gen.generateExpression());
        }
        return TiDBVisitor.asString(right);
    }

    private boolean isRowPreservingQuery(boolean useAggregate, TiDBSelect select) {
        return !useAggregate && select.getGroupByExpressions().isEmpty();
    }

    private boolean hasVpRowIdTieBreaker(TiDBSelect select, TiDBColumn rowIdColumn) {
        List<TiDBExpression> orderBys = select.getOrderByClauses();
        if (orderBys.isEmpty()) {
            return false;
        }
        TiDBExpression last = orderBys.get(orderBys.size() - 1);
        return last instanceof TiDBColumnReference
                && ((TiDBColumnReference) last).getColumn().getName().equals(rowIdColumn.getName());
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

    private static void recordKnownBugSuppressed(KnownVpBug knownBug) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            increment(KNOWN_BUG_COUNTS, knownBug);
        }
    }

    public static void recordAdvancedFeature(AdvancedFeature feature) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            increment(ADVANCED_FEATURE_COUNTS, feature);
        }
    }

    private static String formatVpStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("-- [vp-stats] event=process-final dbms=tidb");
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
        return sb.toString();
    }

    private boolean advancedFeaturesEnabled() {
        return state.getDbmsSpecificOptions().verticalPartitioningAdvancedQueryFeatures || deepExplorationEnabled();
    }

    private boolean deepExplorationEnabled() {
        return state.getDbmsSpecificOptions().verticalPartitioningDeepExploration;
    }

    private static <E extends Enum<E>> String formatEnumCounts(E[] values, Map<E, Integer> counts) {
        return java.util.Arrays.stream(values).map(v -> v.name() + "=" + counts.getOrDefault(v, 0))
                .collect(Collectors.joining(", "));
    }

    public static void logSchemaShapeStats(TiDBGlobalState state, String phase) throws Exception {
        registerVpStatsShutdownHook();
        int emptySingle = 0;
        int emptyMulti = 0;
        int nonEmptySingle = 0;
        int nonEmptyMulti = 0;
        state.updateSchema();
        for (TiDBTable table : state.getSchema().getDatabaseTables()) {
            if (table.isView() || table.getName().startsWith("vp_") || table.getColumns().isEmpty()) {
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

    private static void registerVpStatsShutdownHook() {
        if (SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(TiDBVerticalPartitioningOracle::writeFinalVpStats));
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

    private TiDBExpression generateVpAggregate(TiDBExpressionGenerator gen, List<TiDBColumn> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            return new TiDBText(randomAdvancedAggregateSQL(columns));
        }
        TiDBAggregateFunction func = Randomly.fromOptions(TiDBAggregateFunction.values());
        if (func == TiDBAggregateFunction.SUM) {
            return new TiDBAggregate(List.of(generateSumInputExpression(gen, columns)), func);
        }
        List<TiDBExpression> args = new ArrayList<>();
        for (int i = 0; i < func.getNrArgs(); i++) {
            args.add(gen.generateExpression());
        }
        return new TiDBAggregate(args, func);
    }

    private TiDBExpression generateSumInputExpression(TiDBExpressionGenerator gen, List<TiDBColumn> columns) {
        if (Randomly.getBoolean()) {
            TiDBColumn column = Randomly.fromList(columns);
            TiDBColumnReference ref = new TiDBColumnReference(column);
            if (column.getType().getPrimitiveDataType() == TiDBDataType.TEXT
                    || column.getType().getPrimitiveDataType() == TiDBDataType.CHAR) {
                if (state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns
                        || advancedFeaturesEnabled()) {
                    if (state.getRandomly().getInteger(0, 100) < 70) {
                        return new TiDBText("CAST(" + TiDBVisitor.asString(ref) + " AS " + randomSumCastType() + ")");
                    }
                    return gen.generateExpression();
                }
                return new TiDBText("CAST(" + TiDBVisitor.asString(ref) + " AS DECIMAL(65, 30))");
            }
            return ref;
        }
        return gen.generateExpression();
    }

    private String randomSumCastType() {
        return Randomly.fromOptions("SIGNED", "UNSIGNED", "DOUBLE", "DECIMAL(10, 2)", "DECIMAL(20, 5)",
                "DECIMAL(65, 30)");
    }

    private String randomAdvancedAggregateSQL(List<TiDBColumn> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()) {
            recordAdvancedFeature(AdvancedFeature.GROUP_CONCAT);
            return "GROUP_CONCAT(" + groupConcatInputForSQL(columns) + " ORDER BY " + quote(ROW_ID) + " SEPARATOR ',')";
        }
        String numeric = numericExpressionForSQL(columns);
        String integer = "CAST(" + numeric + " AS SIGNED)";
        return Randomly.fromOptions("AVG(" + numeric + ")", "BIT_AND(" + integer + ")", "BIT_OR(" + integer + ")",
                "BIT_XOR(" + integer + ")", "STD(" + numeric + ")", "VAR_SAMP(" + numeric + ")");
    }

    private String groupConcatInputForSQL(List<TiDBColumn> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        TiDBColumn column = Randomly.fromList(columns);
        if (isCharacterLike(column.getType().getPrimitiveDataType()) || Randomly.getBoolean()) {
            return "CAST(" + quote(column.getName()) + " AS CHAR)";
        }
        return quote(column.getName());
    }

    private String numericExpressionForSQL(List<TiDBColumn> columns) {
        List<TiDBColumn> numericColumns = columns.stream().filter(c -> c.getType().getPrimitiveDataType().isNumeric())
                .collect(Collectors.toList());
        if (!numericColumns.isEmpty() && Randomly.getBoolean()) {
            return quote(Randomly.fromList(numericColumns).getName());
        }
        return quote(ROW_ID);
    }

    private String simpleColumnOrRowIdForSQL(List<TiDBColumn> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        return quote(Randomly.fromList(columns).getName());
    }

    private String simpleOrderExpressionForSQL(List<TiDBColumn> columns) {
        if (columns.isEmpty()) {
            return quote(ROW_ID);
        }
        return simpleColumnOrRowIdForSQL(columns);
    }

    private static final class SourceAst {
        private final TiDBTable table;
        private final TiDBColumn rowIdColumn;
        private final List<TiDBColumn> columns;

        private SourceAst(TiDBTable table, TiDBColumn rowIdColumn, List<TiDBColumn> columns) {
            this.table = table;
            this.rowIdColumn = rowIdColumn;
            this.columns = columns;
        }
    }

    private SourceAst createSourceAst(VpSchema vp) {
        List<TiDBColumn> astColumns = new ArrayList<>();
        TiDBColumn rowId = new TiDBColumn(ROW_ID, TiDBCompositeDataType.getInt(8), true, false, false);
        astColumns.add(rowId);
        for (VpColumn column : vp.columns) {
            astColumns.add(new TiDBColumn(column.name, new TiDBCompositeDataType(column.type), false, true, false));
        }
        TiDBTable table = new TiDBTable(vp.sourceTable, astColumns, List.of(), false);
        for (TiDBColumn column : astColumns) {
            column.setTable(table);
        }
        return new SourceAst(table, rowId, astColumns);
    }

    private String replaceSourceFrom(String singleQuery, String sourceTable, String relation) {
        String needle = " FROM " + sourceTable;
        if (singleQuery.contains(needle)) {
            return singleQuery.replace(needle, " FROM " + relation);
        }
        String quotedNeedle = " FROM " + quote(sourceTable);
        if (singleQuery.contains(quotedNeedle)) {
            return singleQuery.replace(quotedNeedle, " FROM " + relation);
        }
        throw new IgnoreMeException();
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
            extraSetup.add("DROP VIEW IF EXISTS " + quote(vp.viewName));
            extraSetup.add("CREATE VIEW " + quote(vp.viewName) + " AS SELECT " + joinProjection(vp, "l", "r", false)
                    + " FROM " + quote(vp.leftTable) + " l JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID)
                    + " = r." + quote(ROW_ID));
            return quote(vp.viewName) + " " + quote(vp.sourceTable);
        case TEMP_TABLE:
            extraSetup.add("DROP TEMPORARY TABLE IF EXISTS " + quote(vp.tempTable));
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
            return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                    " FROM " + quote(vp.leftTable) + " l STRAIGHT_JOIN " + quote(vp.rightTable) + " r ON l."
                            + quote(ROW_ID) + " = r." + quote(ROW_ID));
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
        defs.add(quote(ROW_ID) + " BIGINT NOT NULL PRIMARY KEY");
        for (VpColumn column : vp.columns) {
            defs.add(columnDef(vp, column));
        }
        return "CREATE TEMPORARY TABLE " + quote(vp.tempTable) + " (" + String.join(", ", defs) + ")";
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
                " FROM " + quote(vp.leftTable) + " l JOIN (SELECT " + quote(ROW_ID) + ", CONCAT('vp:', " + quote(ROW_ID)
                        + ") AS payload FROM " + quote(vp.rightTable) + ") sp ON CONCAT('vp:', l." + quote(ROW_ID)
                        + ") = sp.payload JOIN " + quote(vp.rightTable) + " r ON r." + quote(ROW_ID) + " = sp."
                        + quote(ROW_ID));
    }

    private String lateralRelation(VpSchema vp) {
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN LATERAL (SELECT * FROM " + quote(vp.rightTable)
                        + " rr WHERE rr." + quote(ROW_ID) + " = l." + quote(ROW_ID) + ") r ON TRUE");
    }

    private void comparePair(VpQueryPair pair) throws Exception {
        for (String setup : pair.extraSetupStatements) {
            executeNoSchemaRefresh(setup);
            pair.schema.setupStatements.add(setup);
        }
        QueryResultMultiset singleRows = getResultMultiset(pair.singleQuery);
        QueryResultMultiset splitRows = getResultMultiset(pair.splitQuery);
        if (!resultMultisetsMatch(singleRows, splitRows)) {
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
        if (isMinMaxBinary(pair.singleQuery) || isMinMaxBinary(pair.splitQuery)) {
            return KnownVpBug.TIDB_MINMAX_BINARY_VP_DIFF;
        }
        if (isSumTextDecimal(pair, pair.singleQuery) || isSumTextDecimal(pair, pair.splitQuery)) {
            return KnownVpBug.TIDB_SUM_TEXT_DECIMAL_OVERFLOW_DIFF;
        }
        if (isNoZeroDateTemporalCastDiff(pair, singleRows, splitRows)) {
            return KnownVpBug.TIDB_TEMPORAL_CAST_NO_ZERO_DATE_NULL_DIFF;
        }
        if (isMinTextWhitespaceDiff(pair, singleRows, splitRows)) {
            return KnownVpBug.TIDB_MIN_TEXT_WHITESPACE_TOPN_DIFF;
        }
        if (isTemporalPredicatePushdownDiff(pair, singleRows, splitRows)) {
            return KnownVpBug.TIDB_TEMPORAL_CAST_PREDICATE_PUSHDOWN_DIFF;
        }
        if ((containsBinaryPredicate(pair.singleQuery) || containsBinaryPredicate(pair.splitQuery))
                && hasNonEmptyDiff(singleRows, splitRows)) {
            return KnownVpBug.TIDB_BINARY_PREDICATE_PUSHDOWN_DIFF;
        }
        return null;
    }

    private KnownVpBug classifyKnownOrderedBug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (isUnaryMinusDescNullOrderDiff(pair, singleRows, splitRows)) {
            return KnownVpBug.TIDB_ORDER_BY_UNARY_MINUS_DESC_NULL_DIFF;
        }
        return null;
    }

    private boolean isMinMaxBinary(String query) {
        String normalized = query.toUpperCase(Locale.ROOT);
        return (normalized.contains("MIN(") || normalized.contains("MAX("))
                && (normalized.contains("BINARY") || normalized.contains(" AS BINARY"));
    }

    private boolean isSumTextDecimal(VpQueryPair pair, String query) {
        String normalized = query.toUpperCase(Locale.ROOT);
        if (!normalized.contains("SUM(") || !normalized.contains(" AS DECIMAL")) {
            return false;
        }
        for (VpColumn column : pair.schema.columns) {
            if (isCharacterLike(column.type) && containsSumDecimalCastOfColumn(query, column.name)) {
                return true;
            }
        }
        return false;
    }

    private boolean containsSumDecimalCastOfColumn(String query, String columnName) {
        String quotedColumn = Pattern.quote(quote(columnName));
        String bareColumn = Pattern.quote(columnName);
        String columnReference = "(?:" + quotedColumn + "|\\b[A-Za-z0-9_]+\\." + quotedColumn + "|\\b" + bareColumn
                + "\\b|\\b[A-Za-z0-9_]+\\." + bareColumn + "\\b)";
        String pattern = "(?is)SUM\\s*\\(\\s*CAST\\s*\\([^)]*" + columnReference + "[^)]*\\s+AS\\s+DECIMAL\\b";
        return Pattern.compile(pattern).matcher(query).find();
    }

    private boolean sessionHasNoZeroDateMode() {
        String sqlMode;
        try {
            sqlMode = getSingleString("SELECT @@sql_mode");
        } catch (SQLException e) {
            return false;
        }
        if (sqlMode == null) {
            return false;
        }
        String normalized = sqlMode.toUpperCase(Locale.ROOT);
        return normalized.contains("NO_ZERO_DATE") || normalized.contains("NO_ZERO_IN_DATE");
    }

    private boolean containsTemporalCast(String query) {
        String normalized = query.toUpperCase(Locale.ROOT);
        if (!normalized.contains("CAST(")) {
            return false;
        }
        return normalized.matches("(?s).*\\bCAST\\s*\\(.*\\s+AS\\s+(DATE|TIME|DATETIME)\\b.*");
    }

    private boolean isTemporalPredicatePushdownDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!hasNonEmptyDiff(singleRows, splitRows)) {
            return false;
        }
        return containsTemporalPredicatePushdownShape(pair.singleQuery)
                || containsTemporalPredicatePushdownShape(pair.splitQuery);
    }

    private boolean containsTemporalPredicatePushdownShape(String query) {
        String normalized = query.toUpperCase(Locale.ROOT);
        int whereIndex = normalized.indexOf(" WHERE ");
        if (whereIndex < 0) {
            return false;
        }
        String predicate = normalized.substring(whereIndex);
        boolean temporalExpression = predicate.matches("(?s).*\\bCAST\\s*\\(.*\\s+AS\\s+(DATE|TIME|DATETIME)\\b.*")
                || predicate.contains("DATE_FORMAT(") || predicate.contains("STR_TO_DATE(");
        if (!temporalExpression) {
            return false;
        }
        return predicate.contains(" IS NOT NULL") || predicate.contains(" IS NULL") || predicate.contains("!=")
                || predicate.contains("<>") || predicate.contains("=") || predicate.contains("<")
                || predicate.contains(">");
    }

    private boolean isNoZeroDateTemporalCastDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!sessionHasNoZeroDateMode()) {
            return false;
        }
        if (!(containsTemporalCast(pair.singleQuery) || containsTemporalCast(pair.splitQuery)
                || containsTemporalComparisonWithZeroValue(pair))) {
            return false;
        }
        if (hasZeroTemporalNullDiff(singleRows, splitRows)) {
            return true;
        }
        return hasNonEmptyDiff(singleRows, splitRows) && schemaHasZeroTemporalValue(pair.schema);
    }

    private boolean containsTemporalComparisonWithZeroValue(VpQueryPair pair) {
        if (!schemaHasZeroTemporalValue(pair.schema)) {
            return false;
        }
        return containsTemporalComparison(pair.singleQuery) || containsTemporalComparison(pair.splitQuery);
    }

    private boolean containsTemporalComparison(String query) {
        String normalized = query.toUpperCase(Locale.ROOT);
        if (!normalized.contains(" WHERE ")) {
            return false;
        }
        return normalized.contains("<") || normalized.contains(">") || normalized.contains("=");
    }

    private boolean schemaHasZeroTemporalValue(VpSchema schema) {
        return tableHasZeroTemporalValue(schema.sourceTable, schema.columns)
                || tableHasZeroTemporalValue(schema.leftTable, schema.leftColumns)
                || tableHasZeroTemporalValue(schema.rightTable, schema.rightColumns);
    }

    private boolean tableHasZeroTemporalValue(String tableName, List<VpColumn> columns) {
        return containsZeroTemporalValue(executeDiagnosticQuery(snapshotQuery(tableName, columns)));
    }

    private boolean hasZeroTemporalNullDiff(QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        Map<String, Integer> onlyInSingle = unmatchedRowsAfterFuzzyMatching(singleRows, splitRows);
        Map<String, Integer> onlyInSplit = unmatchedRowsAfterFuzzyMatching(splitRows, singleRows);
        return hasNullRow(onlyInSingle) && hasZeroTemporalRow(onlyInSplit)
                || hasNullRow(onlyInSplit) && hasZeroTemporalRow(onlyInSingle);
    }

    private boolean hasNullRow(Map<String, Integer> rows) {
        return rows.keySet().stream().anyMatch(row -> row.contains("N;"));
    }

    private boolean hasZeroTemporalRow(Map<String, Integer> rows) {
        return rows.keySet().stream().anyMatch(this::containsZeroTemporalValue);
    }

    private boolean containsZeroTemporalValue(String row) {
        return row.contains("0000-00-00") || row.contains("00:00:00");
    }

    private boolean isMinTextWhitespaceDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!(containsMinAggregate(pair.singleQuery) || containsMinAggregate(pair.splitQuery))) {
            return false;
        }
        Map<String, Integer> onlyInSingle = unmatchedRowsAfterFuzzyMatching(singleRows, splitRows);
        Map<String, Integer> onlyInSplit = unmatchedRowsAfterFuzzyMatching(splitRows, singleRows);
        return onlyWhitespaceStringRows(onlyInSingle) && onlyWhitespaceStringRows(onlyInSplit)
                && hasDifferentWhitespaceStringLength(onlyInSingle, onlyInSplit);
    }

    private boolean containsMinAggregate(String query) {
        return query.toUpperCase(Locale.ROOT).contains("MIN(");
    }

    private boolean onlyWhitespaceStringRows(Map<String, Integer> rows) {
        if (rows.isEmpty()) {
            return false;
        }
        for (String row : rows.keySet()) {
            List<String> values = decodeCanonicalRow(row);
            if (values.size() != 1 || values.get(0) == null || !values.get(0).trim().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private boolean hasDifferentWhitespaceStringLength(Map<String, Integer> leftRows, Map<String, Integer> rightRows) {
        for (String left : leftRows.keySet()) {
            int leftLength = decodeCanonicalRow(left).get(0).length();
            for (String right : rightRows.keySet()) {
                if (leftLength != decodeCanonicalRow(right).get(0).length()) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isUnaryMinusDescNullOrderDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        return (containsUnaryMinusDescOrder(pair.singleQuery) || containsUnaryMinusDescOrder(pair.splitQuery))
                && (hasNullRow(singleRows.rows) || schemaHasNullValue(pair.schema))
                && sameOrderedRowsExceptNullPlacement(singleRows, splitRows);
    }

    private boolean containsUnaryMinusDescOrder(String query) {
        String normalized = query.toUpperCase(Locale.ROOT);
        int orderByIndex = normalized.indexOf(" ORDER BY ");
        if (orderByIndex < 0) {
            return false;
        }
        String orderBy = normalized.substring(orderByIndex);
        return java.util.regex.Pattern.compile("(?s)ORDER\\s+BY\\s*\\(?\\s*-\\s*\\(?[^,]+\\)?\\s*\\)?\\s+DESC")
                .matcher(orderBy).find();
    }

    private boolean sameOrderedRowsExceptNullPlacement(QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        if (singleRows.orderedRows.size() != splitRows.orderedRows.size()) {
            return false;
        }
        List<String> singleNonNull = singleRows.orderedRows.stream().filter(row -> !row.contains("N;"))
                .collect(Collectors.toList());
        List<String> splitNonNull = splitRows.orderedRows.stream().filter(row -> !row.contains("N;"))
                .collect(Collectors.toList());
        if (singleNonNull.size() < singleRows.orderedRows.size()) {
            return singleNonNull.equals(splitNonNull);
        }
        if (singleRows.orderedRows.size() < 2) {
            return false;
        }
        if (isRotation(singleRows.orderedRows, splitRows.orderedRows)) {
            return true;
        }
        return singleRows.orderedRows.get(0).equals(splitRows.orderedRows.get(splitRows.orderedRows.size() - 1))
                && singleRows.orderedRows.subList(1, singleRows.orderedRows.size())
                        .equals(splitRows.orderedRows.subList(0, splitRows.orderedRows.size() - 1));
    }

    private boolean isRotation(List<String> left, List<String> right) {
        if (left.size() != right.size() || left.isEmpty()) {
            return false;
        }
        int size = left.size();
        for (int offset = 1; offset < size; offset++) {
            boolean matches = true;
            for (int i = 0; i < size; i++) {
                if (!Objects.equals(left.get(i), right.get((i + offset) % size))) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                return true;
            }
        }
        return false;
    }

    private boolean schemaHasNullValue(VpSchema schema) {
        return tableHasNullValue(schema.sourceTable, schema.columns)
                || tableHasNullValue(schema.leftTable, schema.leftColumns)
                || tableHasNullValue(schema.rightTable, schema.rightColumns);
    }

    private boolean tableHasNullValue(String tableName, List<VpColumn> columns) {
        if (columns.isEmpty()) {
            return false;
        }
        String predicate = columns.stream().map(column -> quote(column.name) + " IS NULL")
                .collect(Collectors.joining(" OR "));
        String result = executeDiagnosticQuery(
                "SELECT 1 FROM " + quote(tableName) + " WHERE " + predicate + " LIMIT 1");
        return result.trim().equals("1");
    }

    private boolean containsBinaryPredicate(String query) {
        String normalized = query.toUpperCase(Locale.ROOT);
        if (!(normalized.contains("BINARY") || normalized.contains(" AS BINARY"))) {
            return false;
        }
        int whereIndex = normalized.indexOf(" WHERE ");
        if (whereIndex < 0) {
            return false;
        }
        String predicate = normalized.substring(whereIndex);
        if (!(predicate.contains("BINARY") || predicate.contains(" AS BINARY"))) {
            return false;
        }
        return predicate.contains("=") || predicate.contains("<") || predicate.contains(">")
                || predicate.contains(" IS ") || predicate.contains(" LIKE ") || predicate.contains(" IN ")
                || predicate.contains(" AND ") || predicate.contains(" OR ") || predicate.contains("NOT ");
    }

    private boolean hasNonEmptyDiff(QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        return !unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).isEmpty()
                || !unmatchedRowsAfterFuzzyMatching(splitRows, singleRows).isEmpty();
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
        File dir = new File(Main.LOG_DIRECTORY, "tidb");
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
                throw new IgnoreMeException();
            }
            throw e;
        } catch (SQLException e) {
            if (isSkippableResultReadError(e)) {
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
        String upper = type.toUpperCase();
        if (upper.contains("FLOAT") || upper.contains("REAL")) {
            return ApproximateKind.FLOAT;
        }
        if (upper.contains("DOUBLE")) {
            return ApproximateKind.DOUBLE;
        }
        return ApproximateKind.NONE;
    }

    private boolean isSkippableResultReadError(SQLException e) {
        String msg = getErrorText(e);
        return msg.contains("Invalid length") && msg.contains("for type DATE");
    }

    private boolean isSkippableQueryError(AssertionError e) {
        String msg = getErrorText(e);
        if (msg.isEmpty()) {
            return false;
        }
        return msg.contains("doesn't yet support") || msg.contains("You have an error in your SQL syntax")
                || msg.contains("window") || msg.contains("Incorrect DECIMAL value")
                || msg.contains("Truncated incorrect") || msg.contains("Data truncation")
                || msg.contains("Invalid JSON") || msg.contains("LATERAL")
                || msg.contains("incompatible with sql_mode=only_full_group_by") || msg.contains("not in SELECT list")
                || msg.contains("Expression #") || msg.contains("Invalid use of group function")
                || msg.contains("Invalid use of group function") || msg.contains("Unknown column")
                || isMissingFunctionError(msg) || msg.contains("Can't group on")
                || msg.contains("of GROUPING function is not in GROUP BY") || msg.contains("Illegal mix of collations")
                || msg.contains("Operand should contain") || msg.contains("Subquery returns more than 1 row");
    }

    private boolean isMissingFunctionError(String msg) {
        return msg.contains("FUNCTION") && msg.contains("does not exist");
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
        sb.append("Mode: RANDOM_TABLE_REWRITE").append(System.lineSeparator());
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
        sb.append("TiDB version: ").append(getSingleString("SELECT VERSION()")).append(System.lineSeparator());
        sb.append("Base table: ").append(pair.schema.baseTable).append(System.lineSeparator());
        sb.append("Base table DDL: ").append(getCreateTable(pair.schema.baseTable)).append(System.lineSeparator());
        sb.append("Base table engine: ").append(pair.schema.baseEngine).append(System.lineSeparator());
        sb.append("VP source engine: ").append(pair.schema.sourceEngine).append(System.lineSeparator());
        sb.append("VP left engine: ").append(pair.schema.leftEngine).append(System.lineSeparator());
        sb.append("VP right engine: ").append(pair.schema.rightEngine).append(System.lineSeparator());
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
        sb.append(formatExplainTree(pair.singleQuery));
        sb.append("Split query plan:").append(System.lineSeparator());
        sb.append(formatExplainTree(pair.splitQuery));
        sb.append("Single result sample: ").append(sample(singleRows.rows)).append(System.lineSeparator());
        sb.append("Split result sample: ").append(sample(splitRows.rows)).append(System.lineSeparator());
        sb.append("Only in single: ").append(diff(singleRows, splitRows)).append(System.lineSeparator());
        sb.append("Only in split: ").append(diff(splitRows, singleRows)).append(System.lineSeparator());
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

    private void decrementRowCount(Map<String, Integer> rows, String row, int count) {
        int remaining = rows.get(row) - count;
        if (remaining <= 0) {
            rows.remove(row);
        } else {
            rows.put(row, remaining);
        }
    }

    private String getCreateTable(String table) throws SQLException {
        try (Statement s = state.getConnection().createStatement();
                ResultSet rs = s.executeQuery("SHOW CREATE TABLE " + quote(table))) {
            if (rs.next()) {
                return rs.getString(2);
            }
            return "<unavailable>";
        }
    }

    private String extractEngineFromCreateTable(String createTable) {
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("(?i)\\bENGINE\\s*=\\s*([A-Za-z0-9_]+)")
                .matcher(createTable);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "InnoDB";
    }

    private String getSingleString(String sql) throws SQLException {
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getString(1);
            }
            return "<none>";
        }
    }

    private String captureSessionVars() {
        return List
                .of("sql_mode", "time_zone", "character_set_connection", "collation_connection", "optimizer_switch",
                        "big_tables", "internal_tmp_mem_storage_engine")
                .stream().map(this::captureSessionVar).collect(Collectors.joining(", "));
    }

    private String captureSessionVar(String varName) {
        try {
            return varName + "=" + getSingleString("SELECT @@" + varName);
        } catch (SQLException e) {
            return varName + "=<unavailable: " + e.getMessage() + ">";
        }
    }

    private String captureSessionSetStatements() {
        List<String> vars = List.of("sql_mode", "optimizer_switch", "big_tables", "internal_tmp_mem_storage_engine");
        return vars.stream().map(this::captureSessionSetStatement).collect(Collectors.joining());
    }

    private String captureSessionSetStatement(String varName) {
        try {
            String value = getSingleString("SELECT @@" + varName);
            return "SET SESSION " + varName + " = " + sessionVariableLiteral(value) + ";" + System.lineSeparator();
        } catch (SQLException e) {
            return "-- SET SESSION " + varName + " unavailable: " + e.getMessage() + System.lineSeparator();
        }
    }

    private String sessionVariableLiteral(String value) {
        if (value == null) {
            return "NULL";
        }
        if (value.matches("-?\\d+(\\.\\d+)?")) {
            return value;
        }
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'";
    }

    private String formatSnapshot(String tableName, List<VpColumn> columns) {
        String query = snapshotQuery(tableName, columns);
        StringBuilder sb = new StringBuilder();
        sb.append(query).append(";").append(System.lineSeparator());
        sb.append(executeDiagnosticQuery(query));
        return sb.toString();
    }

    private String snapshotQuery(String tableName, List<VpColumn> columns) {
        List<String> selectedColumns = new ArrayList<>();
        selectedColumns.add(quote(ROW_ID));
        selectedColumns.addAll(columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        return "SELECT " + String.join(", ", selectedColumns) + " FROM " + quote(tableName) + " ORDER BY "
                + quote(ROW_ID);
    }

    private String formatExplainTree(String query) {
        String explain = "EXPLAIN " + query;
        StringBuilder sb = new StringBuilder();
        sb.append(explain).append(";").append(System.lineSeparator());
        sb.append(executeDiagnosticQuery(explain));
        return sb.toString();
    }

    private String executeDiagnosticQuery(String query) {
        StringBuilder sb = new StringBuilder();
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(query)) {
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> values = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    values.add(rs.getString(i));
                }
                sb.append(values.stream().map(v -> v == null ? "NULL" : v).collect(Collectors.joining(" | ")))
                        .append(System.lineSeparator());
            }
        } catch (SQLException e) {
            sb.append("<unavailable: ").append(e.getMessage()).append(">").append(System.lineSeparator());
        }
        return sb.toString();
    }

    private String columnNames(List<VpColumn> columns) {
        return columns.stream().map(c -> c.name).collect(Collectors.joining(", ", "{", "}"));
    }

    private void dropArtifacts(VpSchema vp) throws Exception {
        executeNoSchemaRefresh("DROP VIEW IF EXISTS " + quote(vp.viewName));
        executeNoSchemaRefresh("DROP TEMPORARY TABLE IF EXISTS " + quote(vp.tempTable));
        executeNoSchemaRefresh("DROP TABLE IF EXISTS " + quote(vp.rightTable));
        executeNoSchemaRefresh("DROP TABLE IF EXISTS " + quote(vp.leftTable));
        executeNoSchemaRefresh("DROP TABLE IF EXISTS " + quote(vp.sourceTable));
    }

    private void cleanupIfNoBug(VpSchema vp) throws Exception {
        dropArtifacts(vp);
    }

    private void executeSetup(VpSchema vp, String sql) throws Exception {
        state.executeStatement(new SQLQueryAdapter(sql, errors, sql.startsWith("CREATE TABLE")));
        vp.setupStatements.add(sql);
    }

    private void tryExecuteSetup(VpSchema vp, String sql) throws Exception {
        try {
            executeSetup(vp, sql);
        } catch (AssertionError e) {
            if (!isSkippableQueryError(e)) {
                throw e;
            }
        }
    }

    private void executeNoSchemaRefresh(String sql) throws Exception {
        state.executeStatement(new SQLQueryAdapter(sql, errors, false));
    }

    private void tryExecuteNoSchemaRefresh(String sql) throws Exception {
        try {
            executeNoSchemaRefresh(sql);
        } catch (AssertionError e) {
            if (!isSkippableQueryError(e)) {
                throw e;
            }
        }
    }

    private String toSQLType(TiDBDataType type) {
        switch (type) {
        case INT:
            return "BIGINT";
        case TEXT:
        case CHAR:
            return "TEXT";
        case FLOATING:
            return "DOUBLE";
        case DECIMAL:
        case NUMERIC:
            return "DECIMAL(65, 30)";
        case BOOL:
            return "BOOLEAN";
        case BLOB:
            return "BLOB";
        case DATE:
            return "DATE";
        case DATETIME:
            return "DATETIME";
        default:
            throw new AssertionError(type);
        }
    }

    private String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private String sanitizeIdentifier(String identifier) {
        return identifier.replaceAll("[^A-Za-z0-9_]", "_");
    }
}
