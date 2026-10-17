package sqlancer.oceanbase.oracle;

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
import sqlancer.oceanbase.OceanBaseErrors;
import sqlancer.oceanbase.OceanBaseGlobalState;
import sqlancer.oceanbase.OceanBaseProvider;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseColumn;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseDataType;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseIndex;
import sqlancer.oceanbase.OceanBaseSchema.OceanBaseTable;
import sqlancer.oceanbase.OceanBaseVisitor;
import sqlancer.oceanbase.ast.OceanBaseAggregate;
import sqlancer.oceanbase.ast.OceanBaseAggregate.OceanBaseAggregateFunction;
import sqlancer.oceanbase.ast.OceanBaseColumnReference;
import sqlancer.oceanbase.ast.OceanBaseConstant;
import sqlancer.oceanbase.ast.OceanBaseExpression;
import sqlancer.oceanbase.ast.OceanBaseSelect;
import sqlancer.oceanbase.ast.OceanBaseText;
import sqlancer.oceanbase.gen.OceanBaseExpressionGenerator;

public class OceanBaseVerticalPartitioningOracle implements TestOracle<OceanBaseGlobalState> {

    private static final AtomicLong ORACLE_RUN_ID = new AtomicLong();
    private static final String ROW_ID = "vp_rowid";
    private static final String VP_RELATION_PLACEHOLDER = "__SQLANCER_VP_RELATION_PLACEHOLDER__";
    private static final int MAX_DIFF_ROWS = 8;
    private static final int MAX_RESULT_SAMPLE = 12;
    private static final int LIMIT_ZERO_RECHECK_ATTEMPTS = 3;
    private static final int PX_ROLLUP_STABILITY_RECHECKS = 10;
    private static final double FLOAT_ABSOLUTE_TOLERANCE = 1e-7;
    private static final double FLOAT_RELATIVE_TOLERANCE = 1e-6;
    private static final double DOUBLE_ABSOLUTE_TOLERANCE = 1e-13;
    // Parallel partial aggregation can reorder FLOAT/DOUBLE additions before the final merge.
    private static final double DOUBLE_RELATIVE_TOLERANCE = 2e-9;
    private static final double FLOAT_UNIT_ROUNDOFF = Math.scalb(1.0, -24);
    private static final double DOUBLE_UNIT_ROUNDOFF = Math.scalb(1.0, -53);
    private static final double FLOATING_AGGREGATE_ERROR_FACTOR = 64.0;
    // STD's E[x*x] - E[x]*E[x] expansion can produce a tiny negative variance, reported as NULL after sqrt.
    private static final double STDDEV_NULL_NEAR_ZERO_TOLERANCE = 1e-7;
    private static final String VP_PUSHDOWN_MODE_NEUTRALIZE = "neutralize";
    private static final String VP_PUSHDOWN_MODE_OBSERVE = "observe";
    private static final String VP_OB_STORAGE_PROFILE_DEFAULT = "default";
    private static final String VP_OB_STORAGE_PROFILE_MEMTABLE_SSTABLE = "memtable-sstable";
    private static final String VP_OB_PARTITION_PROFILE_NONE = "none";
    private static final String VP_OB_PARTITION_PROFILE_HASH = "hash";
    private static final String VP_OB_COLUMN_STORE_PROFILE_NONE = "none";
    private static final String VP_OB_COLUMN_STORE_PROFILE_ALL = "all";
    private static final String VP_OB_COLUMN_STORE_PROFILE_EACH = "each";
    private static final String VP_OB_COLUMN_STORE_PROFILE_ALL_EACH = "all-each";
    private static final String VP_OB_SQL_SEMANTICS_PROFILE_DEFAULT = "default";
    private static final String VP_OB_SQL_SEMANTICS_PROFILE_FUZZ = "fuzz";
    private static final long STORAGE_PROFILE_FREEZE_TIMEOUT_MS = 180_000;
    private static final long STORAGE_PROFILE_PRE_FREEZE_WAIT_MS = 30_000;
    private static final long STORAGE_PROFILE_FREEZE_POLL_MS = 1_000;
    private static final long STORAGE_PROFILE_FREEZE_COOLDOWN_AFTER_TIMEOUT_MS = 600_000;
    private static final Object STATS_LOCK = new Object();
    private static final Object STORAGE_PROFILE_LOCK = new Object();
    private static long storageProfileNextFreezeAllowedAtMillis;
    private static final Map<SplitStrategy, Integer> SPLIT_STRATEGY_COUNTS = new EnumMap<>(SplitStrategy.class);
    private static final Map<BaseShape, Integer> SELECTED_BASE_SHAPE_COUNTS = new EnumMap<>(BaseShape.class);
    private static final Map<ReconstructionStrategy, Integer> RECONSTRUCTION_STRATEGY_COUNTS = new EnumMap<>(
            ReconstructionStrategy.class);
    private static final Map<KnownVpBug, Integer> KNOWN_BUG_COUNTS = new EnumMap<>(KnownVpBug.class);
    private static final Map<AdvancedFeature, Integer> ADVANCED_FEATURE_COUNTS = new EnumMap<>(AdvancedFeature.class);
    private static final Map<String, Integer> SKIPPED_ERROR_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SCHEMA_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SOURCE_ROW_COUNT_BUCKET_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SELECTED_SEMANTICS_COUNTS = new LinkedHashMap<>();
    private static final AtomicLong KNOWN_BUG_LOG_ID = new AtomicLong();
    private static final AtomicLong FLAKY_ROLLUP_LOG_ID = new AtomicLong();
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean();
    private static final Object STATS_WRITE_LOCK = new Object();
    private static final File STATS_FILE = new File(new File(Main.LOG_DIRECTORY, "oceanbase"), "vp-stats.log");
    private static int completedOracleRuns;
    private static int completedQueryPairs;
    private static int completedAttempts;
    private static int schemaShapeSamples;

    private final OceanBaseGlobalState state;
    private final ExpectedErrors errors = new ExpectedErrors();
    private VpCapabilities capabilities;
    private VpSessionSemantics sessionSemantics = VpSessionSemantics.defaultProfile();
    private boolean currentSessionUsesPadSpaceSemantics;
    private boolean currentSourceHasPadSpaceVarchar;
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
        COUNT_NULL_COMPARISON_PUSHDOWN, COUNT_NULL_CHAINED_COMPARISON_PUSHDOWN, COUNT_NULL_IN_NOT_IN_PUSHDOWN,
        COUNT_NULL_IF_CASE_PUSHDOWN, COUNT_NULL_GREATEST_LEAST_PUSHDOWN, COUNT_NULL_XOR_LOGICAL_PUSHDOWN,
        AVG_NULL_GREATEST_LEAST_COUNT_DENOMINATOR_PUSHDOWN, AVG_NULL_IN_NOT_IN_COUNT_DENOMINATOR_PUSHDOWN,
        GROUP_BY_IFNULL_NOT_IN_PROJECTION_DROPS_IFNULL, GROUP_BY_SELECTS_NON_GROUPED_COLUMN_REPRESENTATIVE_ROW,
        ROLLUP_EXPRESSION_EXPANSION_NULL_RECOMPUTE_DIFF, ROLLUP_NULL_SEMANTIC_GROUP_KEY_SUBQUERY_DIFF,
        ROLLUP_GROUP_EXPRESSION_REPRESENTATIVE_VALUE_DIFF, ROLLUP_BIT_AND_COMPLEX_GROUP_KEY_DIFF,
        EXTREME_FLOATING_AGGREGATE_ORDER, STDDEV_NULL_NEAR_ZERO_PARALLEL_PUSHDOWN
    }

    public enum AdvancedFeature {
        ROLLUP, CORRELATED_SUBQUERY, GROUP_CONCAT, ASYMMETRIC_INDEX, CHARSET_COLLATION_COLUMN, BOUNDARY_MUTATION,
        WINDOW_FUNCTION, UNION_ALL, ADVANCED_AGGREGATE, HELPER_INDEX_ATTEMPTED, HELPER_INDEX_CREATED,
        HELPER_INDEX_SKIPPED
    }

    private enum StoragePhase {
        DEFAULT, MEMTABLE, SSTABLE
    }

    private static final class VpCapabilities {
        private boolean engineClause;
        private boolean createIndex;
        private boolean temporaryTable;
        private boolean foreignKey;
        private boolean checkConstraint;
        private boolean rollup;
        private boolean windowFunction;
        private boolean jsonObject;
        private boolean groupConcat;
        private boolean lateralDerived;
        private boolean hashPartition;
        private boolean columnGroup;
        private boolean columnGroupAll;
        private boolean columnGroupEach;
        private boolean columnGroupAllEach;
        private boolean columnStoreReadHint;
        private boolean columnStoreReadHintAll;
        private boolean columnStoreReadHintEach;
        private boolean columnStoreReadHintAllEach;
        private final List<String> supportedCollations = new ArrayList<>();
        private final Map<String, Boolean> sessionVariables = new LinkedHashMap<>();

        private String summary() {
            return "engineClause=" + engineClause + ", createIndex=" + createIndex + ", temporaryTable="
                    + temporaryTable + ", foreignKey=" + foreignKey + ", checkConstraint=" + checkConstraint
                    + ", rollup=" + rollup + ", windowFunction=" + windowFunction + ", jsonObject=" + jsonObject
                    + ", groupConcat=" + groupConcat + ", lateralDerived=" + lateralDerived + ", hashPartition="
                    + hashPartition + ", columnGroup=" + columnGroup + ", columnGroupAll=" + columnGroupAll
                    + ", columnGroupEach=" + columnGroupEach + ", columnGroupAllEach=" + columnGroupAllEach
                    + ", columnStoreReadHint=" + columnStoreReadHint + ", columnStoreReadHintAll="
                    + columnStoreReadHintAll + ", columnStoreReadHintEach=" + columnStoreReadHintEach
                    + ", columnStoreReadHintAllEach=" + columnStoreReadHintAllEach + ", collations="
                    + supportedCollations + ", sessionVariables=" + sessionVariables;
        }
    }

    private static final class VpSessionSemantics {
        private final String name;
        private final String sqlMode;
        private final String characterSetConnection;
        private final String collationConnection;

        private VpSessionSemantics(String name, String sqlMode, String characterSetConnection,
                String collationConnection) {
            this.name = name;
            this.sqlMode = sqlMode;
            this.characterSetConnection = characterSetConnection;
            this.collationConnection = collationConnection;
        }

        private static VpSessionSemantics defaultProfile() {
            return new VpSessionSemantics("default", "DEFAULT", "DEFAULT", "DEFAULT");
        }

        private String summary() {
            return "name=" + name + ", sqlMode=" + sqlMode + ", characterSetConnection=" + characterSetConnection
                    + ", collationConnection=" + collationConnection;
        }
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
        private boolean extraSetupExecuted;

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

    private static final class RenderedSelectPair {
        private final String singleQuery;
        private final String splitQuery;

        private RenderedSelectPair(String singleQuery, String splitQuery) {
            this.singleQuery = singleQuery;
            this.splitQuery = splitQuery;
        }
    }

    private static final class PairExecutionResult {
        private final QueryResultMultiset singleRows;
        private final QueryResultMultiset splitRows;

        private PairExecutionResult(QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
            this.singleRows = singleRows;
            this.splitRows = splitRows;
        }
    }

    private static final class PxRollupStabilityResult {
        private final QueryResultMultiset singleRows;
        private final QueryResultMultiset splitRows;

        private PxRollupStabilityResult(QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
            this.singleRows = singleRows;
            this.splitRows = splitRows;
        }
    }

    private static final class StorageReusablePair {
        private final VpQueryPair pair;
        private final QueryResultMultiset memtableSingleRows;
        private final QueryResultMultiset memtableSplitRows;

        private StorageReusablePair(VpQueryPair pair, QueryResultMultiset memtableSingleRows,
                QueryResultMultiset memtableSplitRows) {
            this.pair = pair;
            this.memtableSingleRows = memtableSingleRows;
            this.memtableSplitRows = memtableSplitRows;
        }
    }

    private static final class StorageReusableSplit {
        private final VpSchema schema;
        private final List<StorageReusablePair> reusablePairs;

        private StorageReusableSplit(VpSchema schema, List<StorageReusablePair> reusablePairs) {
            this.schema = schema;
            this.reusablePairs = reusablePairs;
        }
    }

    private static final class QueryResultMultiset {
        private final Map<String, Integer> rows;
        private final Map<String, List<ApproximateKind>> approximateColumns;
        private final Map<String, List<Boolean>> trailingSpaceComparableColumns;
        private final List<String> orderedRows;
        private final List<List<ApproximateKind>> orderedApproximateColumns;
        private final List<List<Boolean>> orderedTrailingSpaceComparableColumns;

        private QueryResultMultiset(Map<String, Integer> rows, Map<String, List<ApproximateKind>> approximateColumns,
                Map<String, List<Boolean>> trailingSpaceComparableColumns, List<String> orderedRows,
                List<List<ApproximateKind>> orderedApproximateColumns,
                List<List<Boolean>> orderedTrailingSpaceComparableColumns) {
            this.rows = rows;
            this.approximateColumns = approximateColumns;
            this.trailingSpaceComparableColumns = trailingSpaceComparableColumns;
            this.orderedRows = orderedRows;
            this.orderedApproximateColumns = orderedApproximateColumns;
            this.orderedTrailingSpaceComparableColumns = orderedTrailingSpaceComparableColumns;
        }
    }

    private static final class CheckProgress {
        private int checkedPairs;
        private int attempts;
    }

    private static final class MajorCompactionStatus {
        private final boolean available;
        private final long frozenScn;
        private final long lastScn;
        private final String status;
        private final String isError;
        private final String info;

        private MajorCompactionStatus(boolean available, long frozenScn, long lastScn, String status, String isError,
                String info) {
            this.available = available;
            this.frozenScn = frozenScn;
            this.lastScn = lastScn;
            this.status = status;
            this.isError = isError;
            this.info = info;
        }

        private static MajorCompactionStatus unavailable(String info) {
            return new MajorCompactionStatus(false, -1, -1, "<unavailable>", "<unavailable>", info);
        }

        private boolean finishedForFrozenScn() {
            return available && lastScn >= frozenScn && !"YES".equalsIgnoreCase(isError);
        }

        private boolean compactionActive() {
            return available && (!finishedForFrozenScn() || status.toUpperCase(Locale.ROOT).contains("COMPACTING"));
        }

        @Override
        public String toString() {
            return "available=" + available + ", frozenScn=" + frozenScn + ", lastScn=" + lastScn + ", status=" + status
                    + ", isError=" + isError + ", info=" + info;
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

    private static final class FloatingAggregateScale {
        private final double maxPositive;
        private final double maxNegativeMagnitude;
        private final double errorBudget;
        private final boolean floatInput;

        private FloatingAggregateScale(double maxPositive, double maxNegativeMagnitude, double errorBudget,
                boolean floatInput) {
            this.maxPositive = maxPositive;
            this.maxNegativeMagnitude = maxNegativeMagnitude;
            this.errorBudget = errorBudget;
            this.floatInput = floatInput;
        }

        private boolean hasOppositeExtremeValues() {
            double threshold = floatInput ? 1e18 : 1e200;
            return maxPositive >= threshold && maxNegativeMagnitude >= threshold;
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
        errors.add("Illegal mix of collations");
        errors.add("isn't in GROUP BY");
        errors.add("not functionally dependent");
        errors.add("incompatible with sql_mode=only_full_group_by");
    }

    @Override
    public void check() throws Exception {
        ensureCapabilities();
        validateOceanBaseVpProfiles();
        resetSessionForVerticalPartitioningOracle();
        applySqlSemanticsProfile();
        OceanBaseProvider.ensureVerticalPartitioningBaseTables(state, 2);
        BaseTableInfo baseTable = selectBaseTable();
        recordSourceRowCountBucket(sourceRowCount(baseTable));
        int queryVariantsPerCombination = Math.max(1,
                state.getDbmsSpecificOptions().verticalPartitioningQueryVariantsPerCombination);
        CheckProgress progress = new CheckProgress();
        List<SplitStrategy> splitSchedule = createSplitSchedule(baseTable.shape);
        if (VP_OB_STORAGE_PROFILE_MEMTABLE_SSTABLE.equals(vpOceanBaseStorageProfile())) {
            synchronized (STORAGE_PROFILE_LOCK) {
                checkWithMemtableSstableProfile(baseTable, splitSchedule, queryVariantsPerCombination, progress);
            }
        } else {
            for (SplitStrategy splitStrategy : splitSchedule) {
                checkSplitWithDefaultStorageProfile(baseTable, splitStrategy, queryVariantsPerCombination, progress);
            }
        }
        if (progress.checkedPairs > 0) {
            recordCompletedVpRun(progress.checkedPairs, progress.attempts);
        } else {
            throw new IgnoreMeException();
        }
        OceanBaseProvider.replaceVerticalPartitioningTableAfterSuccessfulCheck(state, baseTable.tableName);
    }

    private void checkSplitWithDefaultStorageProfile(BaseTableInfo baseTable, SplitStrategy splitStrategy,
            int queryVariantsPerCombination, CheckProgress progress) throws Exception {
        VpSchema vp = materializeTable(baseTable, splitStrategy);
        incrementSplitStrategy(vp.splitStrategy);
        boolean cleanup = true;
        try {
            List<ReconstructionStrategy> reconstructionSchedule = createReconstructionSchedule();
            for (ReconstructionStrategy strategy : reconstructionSchedule) {
                for (int i = 0; i < queryVariantsPerCombination; i++) {
                    progress.attempts++;
                    try {
                        VpQueryPair pair = generateRandomRewritePair(vp, strategy);
                        incrementReconstructionStrategy(pair.reconstructionStrategy);
                        comparePair(pair);
                        progress.checkedPairs++;
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

    private void checkWithMemtableSstableProfile(BaseTableInfo baseTable, List<SplitStrategy> splitSchedule,
            int queryVariantsPerCombination, CheckProgress progress) throws Exception {
        List<StorageReusableSplit> reusableSplits = new ArrayList<>();
        boolean cleanup = true;
        try {
            for (SplitStrategy splitStrategy : splitSchedule) {
                VpSchema vp = materializeTable(baseTable, splitStrategy);
                incrementSplitStrategy(vp.splitStrategy);
                List<StorageReusablePair> reusablePairs = new ArrayList<>();
                List<ReconstructionStrategy> reconstructionSchedule = createReconstructionSchedule();
                for (ReconstructionStrategy strategy : reconstructionSchedule) {
                    for (int i = 0; i < queryVariantsPerCombination; i++) {
                        progress.attempts++;
                        try {
                            VpQueryPair pair = generateRandomRewritePair(vp, strategy);
                            incrementReconstructionStrategy(pair.reconstructionStrategy);
                            PairExecutionResult memtableResult = comparePair(pair, StoragePhase.MEMTABLE);
                            reusablePairs.add(
                                    new StorageReusablePair(pair, memtableResult.singleRows, memtableResult.splitRows));
                            progress.checkedPairs++;
                        } catch (IgnoreMeException ignored) {
                        }
                    }
                }
                reusableSplits.add(new StorageReusableSplit(vp, reusablePairs));
            }
            if (reusableSplits.stream().allMatch(split -> split.reusablePairs.isEmpty())) {
                throw new IgnoreMeException();
            }
            freezeAndWaitForSstablePhase(
                    reusableSplits.stream().map(split -> split.schema).collect(Collectors.toList()));
            for (StorageReusableSplit split : reusableSplits) {
                for (StorageReusablePair reusable : split.reusablePairs) {
                    try {
                        PairExecutionResult sstableResult = comparePair(reusable.pair, StoragePhase.SSTABLE);
                        compareStoragePhaseResult(reusable.pair, "source", reusable.memtableSingleRows,
                                sstableResult.singleRows);
                        compareStoragePhaseResult(reusable.pair, "split", reusable.memtableSplitRows,
                                sstableResult.splitRows);
                    } catch (IgnoreMeException ignored) {
                    }
                }
            }
        } catch (AssertionError e) {
            cleanup = false;
            throw e;
        } finally {
            if (cleanup) {
                for (StorageReusableSplit split : reusableSplits) {
                    cleanupIfNoBug(split.schema);
                }
            }
        }
    }

    @Override
    public String getLastQueryString() {
        return lastQueryString;
    }

    private void resetSessionForVerticalPartitioningOracle() throws Exception {
        executeSessionSetIfSupported("sql_mode", "SET SESSION sql_mode = DEFAULT");
        executeSessionSetIfSupported("time_zone", "SET SESSION time_zone = '+00:00'");
        executeSessionSetIfSupported("sql_auto_is_null", "SET SESSION sql_auto_is_null = OFF");
        executeSessionSetIfSupported("character_set_connection", "SET SESSION character_set_connection = DEFAULT");
        executeSessionSetIfSupported("collation_connection", "SET SESSION collation_connection = DEFAULT");
        executeSessionSetIfSupported("group_concat_max_len", "SET SESSION group_concat_max_len = 1048576");
        executeSessionSetIfSupported("tmp_table_size", "SET SESSION tmp_table_size = DEFAULT");
        executeSessionSetIfSupported("max_heap_table_size", "SET SESSION max_heap_table_size = DEFAULT");
        executeSessionSetIfSupported("join_buffer_size", "SET SESSION join_buffer_size = DEFAULT");
        executeSessionSetIfSupported("sort_buffer_size", "SET SESSION sort_buffer_size = DEFAULT");
        executeSessionSetIfSupported("read_buffer_size", "SET SESSION read_buffer_size = DEFAULT");
        executeSessionSetIfSupported("read_rnd_buffer_size", "SET SESSION read_rnd_buffer_size = DEFAULT");
        executeSessionSetIfSupported("optimizer_switch", "SET SESSION optimizer_switch = DEFAULT");
        executeSessionSetIfSupported("unique_checks", "SET SESSION unique_checks = DEFAULT");
        executeSessionSetIfSupported("foreign_key_checks", "SET SESSION foreign_key_checks = DEFAULT");
        if (VP_PUSHDOWN_MODE_NEUTRALIZE.equals(vpPushdownMode())) {
            executeSessionSetIfSupported("ob_enable_aggregation_pushdown",
                    "SET SESSION ob_enable_aggregation_pushdown = OFF");
        }
    }

    private void applySqlSemanticsProfile() throws Exception {
        sessionSemantics = chooseSqlSemantics();
        recordSelectedSemantics(sessionSemantics.name);
        if (!"DEFAULT".equals(sessionSemantics.sqlMode)) {
            executeSessionSetIfSupported("sql_mode",
                    "SET SESSION sql_mode = '" + escapeSingleQuoted(sessionSemantics.sqlMode) + "'");
        }
        if (!"DEFAULT".equals(sessionSemantics.characterSetConnection)
                && !"DEFAULT".equals(sessionSemantics.collationConnection)) {
            executeSessionSetIfSupported("character_set_connection", "SET NAMES "
                    + sessionSemantics.characterSetConnection + " COLLATE " + sessionSemantics.collationConnection);
        }
        currentSessionUsesPadSpaceSemantics = currentSessionTreatsTrailingSpacesAsEqual();
    }

    private boolean currentSessionTreatsTrailingSpacesAsEqual() {
        try (Statement s = state.getConnection().createStatement();
                ResultSet rs = s.executeQuery("SELECT 'sqlancer_vp' = 'sqlancer_vp '")) {
            return rs.next() && rs.getLong(1) == 1;
        } catch (SQLException ignored) {
            // Keep comparisons exact when the session collation cannot be inspected.
            return false;
        }
    }

    private VpSessionSemantics chooseSqlSemantics() {
        if (VP_OB_SQL_SEMANTICS_PROFILE_DEFAULT.equals(vpOceanBaseSqlSemanticsProfile())) {
            return VpSessionSemantics.defaultProfile();
        }
        List<String> collations = new ArrayList<>();
        for (String collation : List.of("utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_0900_ai_ci")) {
            if (capabilities.supportedCollations.contains(collation)) {
                collations.add(collation);
            }
        }
        if (collations.isEmpty()) {
            collations.add("DEFAULT");
        }
        List<VpSessionSemantics> profiles = new ArrayList<>();
        profiles.add(new VpSessionSemantics("pipes-as-concat", "PIPES_AS_CONCAT", "DEFAULT", "DEFAULT"));
        profiles.add(new VpSessionSemantics("only-full-group-by", "ONLY_FULL_GROUP_BY", "DEFAULT", "DEFAULT"));
        profiles.add(new VpSessionSemantics("pipes-and-only-full-group-by", "PIPES_AS_CONCAT,ONLY_FULL_GROUP_BY",
                "DEFAULT", "DEFAULT"));
        profiles.add(new VpSessionSemantics("strict-trans-tables", "STRICT_TRANS_TABLES", "DEFAULT", "DEFAULT"));
        for (String collation : collations) {
            if (!"DEFAULT".equals(collation)) {
                profiles.add(new VpSessionSemantics("collation-" + collation, "DEFAULT", "utf8mb4", collation));
                profiles.add(new VpSessionSemantics("pipes-collation-" + collation, "PIPES_AS_CONCAT", "utf8mb4",
                        collation));
            }
        }
        return Randomly.fromList(profiles);
    }

    private String vpPushdownMode() {
        String mode = state.getDbmsSpecificOptions().verticalPartitioningPushdownMode;
        if (mode == null) {
            return VP_PUSHDOWN_MODE_NEUTRALIZE;
        }
        String normalized = mode.toLowerCase(Locale.ROOT);
        if (VP_PUSHDOWN_MODE_NEUTRALIZE.equals(normalized) || VP_PUSHDOWN_MODE_OBSERVE.equals(normalized)) {
            return normalized;
        }
        throw new IllegalArgumentException(
                "--vp-pushdown-mode must be either 'neutralize' or 'observe', but was '" + mode + "'");
    }

    private String vpOceanBaseStorageProfile() {
        String profile = state.getDbmsSpecificOptions().verticalPartitioningOceanBaseStorageProfile;
        if (profile == null) {
            return VP_OB_STORAGE_PROFILE_DEFAULT;
        }
        String normalized = profile.toLowerCase(Locale.ROOT);
        if (VP_OB_STORAGE_PROFILE_DEFAULT.equals(normalized)
                || VP_OB_STORAGE_PROFILE_MEMTABLE_SSTABLE.equals(normalized)) {
            return normalized;
        }
        throw new IllegalArgumentException("--vp-ob-storage-profile must be either 'default' or 'memtable-sstable', "
                + "but was '" + profile + "'");
    }

    private String vpOceanBasePartitionProfile() {
        String profile = state.getDbmsSpecificOptions().verticalPartitioningOceanBasePartitionProfile;
        if (profile == null) {
            return VP_OB_PARTITION_PROFILE_NONE;
        }
        String normalized = profile.toLowerCase(Locale.ROOT);
        if (VP_OB_PARTITION_PROFILE_NONE.equals(normalized) || VP_OB_PARTITION_PROFILE_HASH.equals(normalized)) {
            return normalized;
        }
        throw new IllegalArgumentException(
                "--vp-ob-partition-profile must be either 'none' or 'hash', but was '" + profile + "'");
    }

    private int vpOceanBasePartitionCount() {
        return Math.max(1, state.getDbmsSpecificOptions().verticalPartitioningOceanBasePartitionCount);
    }

    private int vpOceanBaseParallelDegree() {
        return Math.max(0, state.getDbmsSpecificOptions().verticalPartitioningOceanBaseParallelDegree);
    }

    private String vpOceanBaseColumnStoreProfile() {
        String profile = state.getDbmsSpecificOptions().verticalPartitioningOceanBaseColumnStoreProfile;
        if (profile == null) {
            return VP_OB_COLUMN_STORE_PROFILE_NONE;
        }
        String normalized = profile.toLowerCase(Locale.ROOT);
        if (VP_OB_COLUMN_STORE_PROFILE_NONE.equals(normalized) || VP_OB_COLUMN_STORE_PROFILE_ALL.equals(normalized)
                || VP_OB_COLUMN_STORE_PROFILE_EACH.equals(normalized)
                || VP_OB_COLUMN_STORE_PROFILE_ALL_EACH.equals(normalized)) {
            return normalized;
        }
        throw new IllegalArgumentException("--vp-ob-column-store-profile must be one of 'none', 'all', 'each', "
                + "or 'all-each', but was '" + profile + "'");
    }

    private void validateOceanBaseVpProfiles() {
        if (VP_OB_STORAGE_PROFILE_MEMTABLE_SSTABLE.equals(vpOceanBaseStorageProfile())
                && state.getOptions().getNumberConcurrentThreads() != 1) {
            throw new IllegalArgumentException("--vp-ob-storage-profile=memtable-sstable requires --num-threads 1 "
                    + "because ALTER SYSTEM MAJOR FREEZE is tenant/system scoped");
        }
        if (VP_OB_PARTITION_PROFILE_HASH.equals(vpOceanBasePartitionProfile()) && !capabilities.hashPartition) {
            throw new IgnoreMeException();
        }
        if (!isRequestedColumnStoreSupported()) {
            throw new IgnoreMeException();
        }
        vpOceanBaseSqlSemanticsProfile();
    }

    private boolean isRequestedColumnStoreSupported() {
        switch (vpOceanBaseColumnStoreProfile()) {
        case VP_OB_COLUMN_STORE_PROFILE_NONE:
            return true;
        case VP_OB_COLUMN_STORE_PROFILE_ALL:
            return capabilities.columnGroupAll && capabilities.columnStoreReadHintAll;
        case VP_OB_COLUMN_STORE_PROFILE_EACH:
            return capabilities.columnGroupEach && capabilities.columnStoreReadHintEach;
        case VP_OB_COLUMN_STORE_PROFILE_ALL_EACH:
            return capabilities.columnGroupAllEach && capabilities.columnStoreReadHintAllEach;
        default:
            throw new AssertionError(vpOceanBaseColumnStoreProfile());
        }
    }

    private String vpOceanBaseSqlSemanticsProfile() {
        String profile = state.getDbmsSpecificOptions().verticalPartitioningOceanBaseSqlSemanticsProfile;
        if (profile == null) {
            return VP_OB_SQL_SEMANTICS_PROFILE_DEFAULT;
        }
        String normalized = profile.toLowerCase(Locale.ROOT);
        if (VP_OB_SQL_SEMANTICS_PROFILE_DEFAULT.equals(normalized)
                || VP_OB_SQL_SEMANTICS_PROFILE_FUZZ.equals(normalized)) {
            return normalized;
        }
        throw new IllegalArgumentException(
                "--vp-ob-sql-semantics-profile must be either 'default' or 'fuzz', " + "but was '" + profile + "'");
    }

    private void ensureCapabilities() {
        if (capabilities == null) {
            capabilities = probeCapabilities();
        }
    }

    private VpCapabilities probeCapabilities() {
        VpCapabilities probed = new VpCapabilities();
        String prefix = "vp_cap_" + Math.abs(state.getRandomly().getInteger()) + "_";
        probed.engineClause = probeStatements("DROP TABLE IF EXISTS " + quote(prefix + "engine"),
                "CREATE TABLE " + quote(prefix + "engine") + " (id INT PRIMARY KEY) ENGINE=InnoDB",
                "DROP TABLE IF EXISTS " + quote(prefix + "engine"));
        probed.createIndex = probeStatements("DROP TABLE IF EXISTS " + quote(prefix + "idx"),
                createCapabilityTableSQL(prefix + "idx", probed.engineClause),
                "CREATE INDEX " + quote(prefix + "i") + " ON " + quote(prefix + "idx") + "(id)",
                "DROP TABLE IF EXISTS " + quote(prefix + "idx"));
        probed.temporaryTable = probeStatements("DROP TEMPORARY TABLE IF EXISTS " + quote(prefix + "tmp"),
                "CREATE TEMPORARY TABLE " + quote(prefix + "tmp") + " (id INT PRIMARY KEY)",
                "DROP TEMPORARY TABLE IF EXISTS " + quote(prefix + "tmp"));
        probed.foreignKey = probeStatements("DROP TABLE IF EXISTS " + quote(prefix + "child"),
                "DROP TABLE IF EXISTS " + quote(prefix + "parent"),
                createCapabilityTableSQL(prefix + "parent", probed.engineClause),
                "CREATE TABLE " + quote(prefix + "child") + " (id INT PRIMARY KEY, CONSTRAINT " + quote(prefix + "fk")
                        + " FOREIGN KEY (id) REFERENCES " + quote(prefix + "parent") + "(id))"
                        + engineClause(probed.engineClause),
                "DROP TABLE IF EXISTS " + quote(prefix + "child"), "DROP TABLE IF EXISTS " + quote(prefix + "parent"));
        probed.checkConstraint = probeStatements("DROP TABLE IF EXISTS " + quote(prefix + "chk"),
                "CREATE TABLE " + quote(prefix + "chk") + " (id INT, CONSTRAINT " + quote(prefix + "ck")
                        + " CHECK (id IS NULL OR id >= 0))" + engineClause(probed.engineClause),
                "DROP TABLE IF EXISTS " + quote(prefix + "chk"));
        probed.rollup = probeQuery("SELECT 1 AS v GROUP BY 1 WITH ROLLUP");
        probed.windowFunction = probeQuery("SELECT ROW_NUMBER() OVER (ORDER BY id) FROM (SELECT 1 AS id) x");
        probed.jsonObject = probeQuery("SELECT JSON_OBJECT('vp', 1)");
        probed.groupConcat = probeQuery("SELECT GROUP_CONCAT(v ORDER BY id SEPARATOR ',') FROM "
                + "(SELECT 1 AS id, 'a' AS v UNION ALL SELECT 2 AS id, 'b' AS v) x");
        probed.lateralDerived = probeQuery(
                "SELECT * FROM (SELECT 1 AS id) l JOIN LATERAL " + "(SELECT l.id AS id) r ON TRUE");
        probed.hashPartition = probeStatements("DROP TABLE IF EXISTS " + quote(prefix + "part"),
                "CREATE TABLE " + quote(prefix + "part") + " (id BIGINT NOT NULL PRIMARY KEY)"
                        + engineClause(probed.engineClause) + " PARTITION BY HASH(id) PARTITIONS 2",
                "DROP TABLE IF EXISTS " + quote(prefix + "part"));
        probeColumnStoreCapabilities(probed, prefix);
        for (String collation : List.of("utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_0900_ai_ci")) {
            if (probeStatements("DROP TABLE IF EXISTS " + quote(prefix + sanitizeIdentifier(collation)),
                    "CREATE TABLE " + quote(prefix + sanitizeIdentifier(collation))
                            + " (c VARCHAR(10) CHARACTER SET utf8mb4 COLLATE " + collation + ")"
                            + engineClause(probed.engineClause),
                    "DROP TABLE IF EXISTS " + quote(prefix + sanitizeIdentifier(collation)))) {
                probed.supportedCollations.add(collation);
            }
        }
        probeSessionVariable(probed, "sql_mode", "SET SESSION sql_mode = DEFAULT");
        probeSessionVariable(probed, "time_zone", "SET SESSION time_zone = '+00:00'");
        probeSessionVariable(probed, "sql_auto_is_null", "SET SESSION sql_auto_is_null = OFF");
        probeSessionVariable(probed, "character_set_connection", "SET SESSION character_set_connection = DEFAULT");
        probeSessionVariable(probed, "collation_connection", "SET SESSION collation_connection = DEFAULT");
        probeSessionVariable(probed, "group_concat_max_len", "SET SESSION group_concat_max_len = 1048576");
        probeSessionVariable(probed, "tmp_table_size", "SET SESSION tmp_table_size = DEFAULT");
        probeSessionVariable(probed, "max_heap_table_size", "SET SESSION max_heap_table_size = DEFAULT");
        probeSessionVariable(probed, "join_buffer_size", "SET SESSION join_buffer_size = DEFAULT");
        probeSessionVariable(probed, "sort_buffer_size", "SET SESSION sort_buffer_size = DEFAULT");
        probeSessionVariable(probed, "read_buffer_size", "SET SESSION read_buffer_size = DEFAULT");
        probeSessionVariable(probed, "read_rnd_buffer_size", "SET SESSION read_rnd_buffer_size = DEFAULT");
        probeSessionVariable(probed, "optimizer_switch", "SET SESSION optimizer_switch = DEFAULT");
        probeAggregationPushdownSessionVariable(probed);
        probeSessionVariable(probed, "unique_checks", "SET SESSION unique_checks = DEFAULT");
        probeSessionVariable(probed, "foreign_key_checks", "SET SESSION foreign_key_checks = DEFAULT");
        return probed;
    }

    private void probeColumnStoreCapabilities(VpCapabilities probed, String prefix) {
        probed.columnGroupAll = probeColumnGroupClause(probed, prefix + "cg_all", "all columns");
        probed.columnGroupEach = probeColumnGroupClause(probed, prefix + "cg_each", "each column");
        probed.columnGroupAllEach = probeColumnGroupClause(probed, prefix + "cg_all_each", "all columns, each column");
        probed.columnGroup = probed.columnGroupAll || probed.columnGroupEach || probed.columnGroupAllEach;
        probed.columnStoreReadHintAll = probed.columnGroupAll && probeColumnStoreReadHint(prefix + "cg_all");
        probed.columnStoreReadHintEach = probed.columnGroupEach && probeColumnStoreReadHint(prefix + "cg_each");
        probed.columnStoreReadHintAllEach = probed.columnGroupAllEach
                && probeColumnStoreReadHint(prefix + "cg_all_each");
        probed.columnStoreReadHint = probed.columnStoreReadHintAll || probed.columnStoreReadHintEach
                || probed.columnStoreReadHintAllEach;
        probeStatements("DROP TABLE IF EXISTS " + quote(prefix + "cg_all"));
        probeStatements("DROP TABLE IF EXISTS " + quote(prefix + "cg_each"));
        probeStatements("DROP TABLE IF EXISTS " + quote(prefix + "cg_all_each"));
    }

    private boolean probeColumnGroupClause(VpCapabilities probed, String tableName, String columnGroupClause) {
        return probeStatements("DROP TABLE IF EXISTS " + quote(tableName),
                "CREATE TABLE " + quote(tableName) + " (id BIGINT NOT NULL PRIMARY KEY, c0 INT NULL)"
                        + engineClause(probed.engineClause) + " WITH COLUMN GROUP(" + columnGroupClause + ")",
                "INSERT INTO " + quote(tableName) + " VALUES (1, 10), (2, NULL)");
    }

    private boolean probeColumnStoreReadHint(String tableName) {
        return probeExplainContains(
                "EXPLAIN BASIC SELECT /*+ USE_COLUMN_TABLE(" + tableName + ") */ SUM(c0) FROM " + quote(tableName),
                "COLUMN TABLE");
    }

    private boolean probeExplainContains(String sql, String expected) {
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            StringBuilder text = new StringBuilder();
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    String value = rs.getString(i);
                    if (value != null) {
                        text.append(value).append('\n');
                    }
                }
            }
            return text.toString().toUpperCase(Locale.ROOT).contains(expected.toUpperCase(Locale.ROOT));
        } catch (SQLException ignored) {
            return false;
        }
    }

    private void probeAggregationPushdownSessionVariable(VpCapabilities probed) {
        boolean supported = probeStatements("SET SESSION ob_enable_aggregation_pushdown = OFF");
        if (!probeStatements("SET SESSION ob_enable_aggregation_pushdown = DEFAULT")) {
            probeStatements("SET SESSION ob_enable_aggregation_pushdown = ON");
        }
        probed.sessionVariables.put("ob_enable_aggregation_pushdown", supported);
    }

    private void probeSessionVariable(VpCapabilities probed, String variableName, String... sql) {
        probed.sessionVariables.put(variableName, probeStatements(sql));
    }

    private void executeSessionSetIfSupported(String variableName, String sql) throws Exception {
        ensureCapabilities();
        if (capabilities.sessionVariables.getOrDefault(variableName, false)) {
            executeNoSchemaRefresh(sql);
        }
    }

    private boolean probeQuery(String sql) {
        try (Statement s = state.getConnection().createStatement(); ResultSet ignored = s.executeQuery(sql)) {
            while (ignored.next()) {
            }
            return true;
        } catch (SQLException ignored) {
            return false;
        }
    }

    private boolean probeStatements(String... statements) {
        try (Statement s = state.getConnection().createStatement()) {
            for (String statement : statements) {
                s.execute(statement);
            }
            return true;
        } catch (SQLException ignored) {
            return false;
        }
    }

    private String createCapabilityTableSQL(String tableName, boolean withEngine) {
        return "CREATE TABLE " + quote(tableName) + " (id INT PRIMARY KEY)" + engineClause(withEngine);
    }

    private String engineClause(boolean withEngine) {
        return withEngine ? " ENGINE=InnoDB" : "";
    }

    private BaseTableInfo selectBaseTable() throws Exception {
        state.updateSchema();
        List<OceanBaseTable> candidates = state.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> !t.getName().startsWith("vp_")).filter(t -> !t.getColumns().isEmpty())
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
                getTableEngine(base.getName()));
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
                    .executeQuery("SELECT " + projection + " FROM " + quote(tableName) + " LIMIT 1")) {
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
        ensureCapabilities();
        List<ReconstructionStrategy> shuffled = java.util.Arrays.stream(ReconstructionStrategy.values())
                .filter(this::isReconstructionStrategySupported).collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(shuffled);
        return shuffled;
    }

    private boolean isReconstructionStrategySupported(ReconstructionStrategy strategy) {
        switch (strategy) {
        case TEMP_TABLE:
            return capabilities.temporaryTable
                    && !VP_OB_STORAGE_PROFILE_MEMTABLE_SSTABLE.equals(vpOceanBaseStorageProfile())
                    && VP_OB_COLUMN_STORE_PROFILE_NONE.equals(vpOceanBaseColumnStoreProfile());
        case ROW_NUMBER_ALIGNMENT:
            return capabilities.windowFunction;
        case JSON_PACK:
            return capabilities.jsonObject;
        case LATERAL_DERIVED:
            return capabilities.lateralDerived;
        default:
            return true;
        }
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
        boolean useForeignKey = capabilities.foreignKey && Randomly.getBoolean();
        VpSchema vp = new VpSchema(prefix, base.tableName, source, left, right, view, temp, base.columns,
                splitPlan.leftColumns, splitPlan.rightColumns, splitPlan.overlapColumns, base.shape, splitStrategy,
                useForeignKey, base.stableOrderStrategy, base.rowCount, base.engine);
        initializeColumnCollations(vp);
        dropArtifacts(vp);
        createSourceTable(vp, helperEngineCandidate(base));
        populateSourceTable(vp, base.stableOrderStrategy);
        currentSourceHasPadSpaceVarchar = sourceHasPadSpaceVarchar(vp);
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
    }

    private boolean sourceHasPadSpaceVarchar(VpSchema vp) {
        for (VpColumn column : vp.columns) {
            if (column.type != OceanBaseDataType.VARCHAR) {
                continue;
            }
            String quotedColumn = quote(column.name);
            String sql = "SELECT " + quotedColumn + " = CONCAT(" + quotedColumn + ", ' ') FROM " + quote(vp.sourceTable)
                    + " WHERE " + quotedColumn + " IS NOT NULL LIMIT 1";
            try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
                if (rs.next() && rs.getLong(1) == 1) {
                    return true;
                }
            } catch (SQLException ignored) {
                // Do not relax comparisons when the column collation cannot be inspected.
            }
        }
        return false;
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

    private String helperEngineCandidate(BaseTableInfo base) {
        if (state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine && base.engine != null
                && !base.engine.isEmpty()) {
            return base.engine;
        }
        return "InnoDB";
    }

    private void createSourceTable(VpSchema vp, String preferredEngine) throws Exception {
        vp.sourceEngine = createTableWithEngineFallback(vp, vp.sourceTable, sourceColumnDefs(vp), preferredEngine);
    }

    private void initializeColumnCollations(VpSchema vp) {
        ensureCapabilities();
        for (VpColumn column : vp.columns) {
            String clause = "";
            if (deepExplorationEnabled() && column.type == OceanBaseDataType.VARCHAR && Randomly.getBoolean()
                    && !capabilities.supportedCollations.isEmpty()) {
                clause = " CHARACTER SET utf8mb4 COLLATE " + Randomly.fromList(capabilities.supportedCollations);
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
        return "CREATE TABLE " + quote(tableName) + " (" + String.join(", ", columnDefs) + ")"
                + (capabilities.engineClause ? " ENGINE=" + engine : "") + helperPartitionClause()
                + helperColumnGroupClause();
    }

    private String helperPartitionClause() {
        if (VP_OB_PARTITION_PROFILE_NONE.equals(vpOceanBasePartitionProfile())) {
            return "";
        }
        if (!capabilities.hashPartition) {
            throw new IgnoreMeException();
        }
        return " PARTITION BY HASH(" + quote(ROW_ID) + ") PARTITIONS " + vpOceanBasePartitionCount();
    }

    private String helperColumnGroupClause() {
        switch (vpOceanBaseColumnStoreProfile()) {
        case VP_OB_COLUMN_STORE_PROFILE_NONE:
            return "";
        case VP_OB_COLUMN_STORE_PROFILE_ALL:
            return " WITH COLUMN GROUP(all columns)";
        case VP_OB_COLUMN_STORE_PROFILE_EACH:
            return " WITH COLUMN GROUP(each column)";
        case VP_OB_COLUMN_STORE_PROFILE_ALL_EACH:
            return " WITH COLUMN GROUP(all columns, each column)";
        default:
            throw new AssertionError(vpOceanBaseColumnStoreProfile());
        }
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
        if (!advancedFeaturesEnabled() || !capabilities.createIndex) {
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
        if (!state.getDbmsSpecificOptions().verticalPartitioningSourceCopySchemaObjects
                || (!capabilities.createIndex && !capabilities.checkConstraint)) {
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
            String normalized = element.trim().toUpperCase(Locale.ROOT);
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
        boolean index = normalizedElement.startsWith("KEY ") || normalizedElement.startsWith("INDEX ")
                || normalizedElement.startsWith("UNIQUE KEY ") || normalizedElement.startsWith("UNIQUE INDEX ");
        boolean constraint = normalizedElement.startsWith("CONSTRAINT ")
                && !normalizedElement.contains(" FOREIGN KEY ");
        return capabilities.createIndex && index || capabilities.checkConstraint && constraint;
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

    private void dropArtifacts(VpSchema vp) {
        executeOptional("DROP VIEW IF EXISTS " + quote(vp.viewName));
        executeOptional("DROP TEMPORARY TABLE IF EXISTS " + quote(vp.tempTable));
        executeOptional("DROP TABLE IF EXISTS " + quote(vp.leftTable));
        executeOptional("DROP TABLE IF EXISTS " + quote(vp.rightTable));
        executeOptional("DROP TABLE IF EXISTS " + quote(vp.sourceTable));
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
            return "(" + "SELECT " + reconstructionSelectPrefix("l", "r") + joinProjection(vp, "l", "r", false)
                    + " FROM " + quote(vp.leftTable) + " l LEFT JOIN " + quote(vp.rightTable) + " r ON l."
                    + quote(ROW_ID) + " = r." + quote(ROW_ID) + " UNION ALL SELECT "
                    + reconstructionSelectPrefix("l", "r") + joinProjection(vp, "l", "r", true) + " FROM "
                    + quote(vp.leftTable) + " l RIGHT JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID)
                    + " = r." + quote(ROW_ID) + " WHERE l." + quote(ROW_ID) + " IS NULL) " + quote(vp.sourceTable);
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
            extraSetup.add("CREATE VIEW " + quote(vp.viewName) + " AS SELECT " + reconstructionSelectPrefix("l", "r")
                    + joinProjection(vp, "l", "r", false) + " FROM " + quote(vp.leftTable) + " l JOIN "
                    + quote(vp.rightTable) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID));
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
        return "(SELECT " + reconstructionSelectPrefix("l", "r") + projection + fromAndWhere + ") "
                + quote(vp.sourceTable);
    }

    private String reconstructionSelectPrefix(String... aliases) {
        String hint = columnTableHints(aliases);
        return hint.isBlank() ? "" : "/*+ " + hint + " */ ";
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
                parts.add("(SELECT " + reconstructionSelectPrefix("r") + "r." + quote(column.name) + " FROM "
                        + quote(vp.rightTable) + " r WHERE r." + quote(ROW_ID) + " = l." + quote(ROW_ID) + ") AS "
                        + quote(column.name));
            }
        }
        return String.join(", ", parts);
    }

    private String scalarSubqueryRelation(VpSchema vp) {
        return reconstructedSubquery(vp, leftProjectionWithRightLookups(vp), " FROM " + quote(vp.leftTable) + " l");
    }

    private String cteRelation(VpSchema vp) {
        return "(WITH l AS (SELECT " + reconstructionSelectPrefix(vp.leftTable) + "* FROM " + quote(vp.leftTable)
                + "), r AS (SELECT " + reconstructionSelectPrefix(vp.rightTable) + "* FROM " + quote(vp.rightTable)
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
        return "(WITH l AS (SELECT " + reconstructionSelectPrefix(vp.leftTable) + "ROW_NUMBER() OVER (ORDER BY "
                + quote(ROW_ID) + ") AS rn, " + rowNumberCteProjection(vp.leftColumns) + " FROM " + quote(vp.leftTable)
                + "), r AS (SELECT " + reconstructionSelectPrefix(vp.rightTable) + "ROW_NUMBER() OVER (ORDER BY "
                + quote(ROW_ID) + ") AS rn, " + rowNumberCteProjection(vp.rightColumns) + " FROM "
                + quote(vp.rightTable) + ") SELECT " + joinProjection(vp, "l", "r", false)
                + " FROM l JOIN r ON l.rn = r.rn) " + quote(vp.sourceTable);
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

    private VpQueryPair generateRandomRewritePair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        SourceAst sourceAst = createSourceAst(vp);
        OceanBaseExpressionGenerator gen = new OceanBaseExpressionGenerator(state).setColumns(sourceAst.columns);
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()
                && strategy != ReconstructionStrategy.TEMP_TABLE) {
            return generateCorrelatedSubqueryPair(vp, strategy, sourceAst);
        }
        OceanBaseSelect select = new OceanBaseSelect();
        select.setSelectType(Randomly.fromOptions(OceanBaseSelect.SelectType.values()));
        select.setForceExplicitAll(true);
        int fetchColumns = Math.max(1, Randomly.smallNumber() + 1);
        boolean useAggregate = Randomly.getBoolean();
        boolean hasWindowExpression = false;
        List<OceanBaseExpression> selected = new ArrayList<>();
        List<OceanBaseExpression> nonAggregateExpressions = new ArrayList<>();
        for (int i = 0; i < fetchColumns; i++) {
            OceanBaseExpression expr;
            if (!useAggregate && advancedFeaturesEnabled() && capabilities.windowFunction
                    && Randomly.getBooleanWithRatherLowProbability()) {
                expr = generateAdvancedSelectExpression(sourceAst);
                hasWindowExpression = true;
            } else if (useAggregate && Randomly.getBoolean()) {
                expr = generateVpAggregate(gen, sourceAst.columns);
            } else {
                expr = stableExpression(gen.generateExpression());
                nonAggregateExpressions.add(expr);
            }
            selected.add(expr);
        }
        if (selected.isEmpty()) {
            selected.add(new OceanBaseColumnReference(Randomly.fromList(sourceAst.columns), null));
        }
        select.setFetchColumns(selected);
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (useAggregate || (!hasWindowExpression && Randomly.getBooleanWithRatherLowProbability())) {
            List<OceanBaseExpression> groupByExpressions = chooseGroupByExpressions(nonAggregateExpressions);
            boolean useRollup = advancedFeaturesEnabled() && capabilities.rollup && !hasWindowExpression
                    && isRollupSafeGroupBy(groupByExpressions, sourceAst.columns)
                    && Randomly.getBooleanWithSmallProbability();
            if (useRollup) {
                recordAdvancedFeature(AdvancedFeature.ROLLUP);
                select.setGroupByExpressions(appendRollup(groupByExpressions));
            } else {
                select.setGroupByExpressions(groupByExpressions);
            }
            if (Randomly.getBoolean() && !groupByExpressions.isEmpty()) {
                select.setHavingClause(useRollup ? generateRollupHavingExpression(sourceAst.columns, groupByExpressions)
                        : generateHavingExpression(gen, sourceAst.columns, groupByExpressions));
            }
        }
        if (Randomly.getBoolean()) {
            List<OceanBaseExpression> orderBys = chooseOrderByExpressions(useAggregate, select, nonAggregateExpressions,
                    sourceAst.rowIdColumn);
            if (!orderBys.isEmpty()) {
                select.setOrderByClauses(orderBys);
            }
        }
        boolean rowPreservingQuery = isRowPreservingQuery(useAggregate, select);
        boolean orderedComparisonEnabled = rowPreservingQuery && hasVpRowIdTieBreaker(select, sourceAst.rowIdColumn);
        if (Randomly.getBoolean() && shouldGenerateLimitForQuery(hasWindowExpression, rowPreservingQuery, select,
                sourceAst.rowIdColumn)) {
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
            } else if (hasWindowExpression && hasDeterministicOuterOrder(select, sourceAst.rowIdColumn)) {
                orderedComparisonEnabled = false;
            } else if (!select.getGroupByExpressions().isEmpty()) {
                select.setOrderByClauses(orderableGroupByExpressions(select.getGroupByExpressions()));
                orderedComparisonEnabled = false;
            } else {
                select.setOrderByClauses(List.of());
                orderedComparisonEnabled = false;
            }
            if (!hasWindowExpression || rowPreservingQuery) {
                select.setSelectType(OceanBaseSelect.SelectType.ALL);
            }
            select.setLimitClause(OceanBaseConstant.createIntConstant(Randomly.getNotCachedInteger(1, 25)));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                select.setOffsetClause(OceanBaseConstant.createIntConstant(Randomly.getNotCachedInteger(0, 5)));
            }
        }
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        String sourceRelation = quote(vp.sourceTable);
        RenderedSelectPair renderedPair = renderSelectPairWithStableExpressions(select, sourceRelation,
                sourceColumnStoreHint(vp), relation, "");
        String singleQuery = renderedPair.singleQuery;
        String splitQuery = renderedPair.splitQuery;
        if (advancedFeaturesEnabled() && !useAggregate && select.getOrderByClauses().isEmpty()
                && select.getLimitClause() == null && select.getOffsetClause() == null
                && strategy != ReconstructionStrategy.TEMP_TABLE && Randomly.getBooleanWithSmallProbability()) {
            recordAdvancedFeature(AdvancedFeature.UNION_ALL);
            OceanBaseSelect unionBranch = createUnionAllBranch(sourceAst, gen, selected.size());
            RenderedSelectPair renderedUnionBranch = renderSelectPairWithStableExpressions(unionBranch, sourceRelation,
                    sourceColumnStoreHint(vp), relation, "");
            singleQuery = "(" + singleQuery + ") UNION ALL (" + renderedUnionBranch.singleQuery + ")";
            splitQuery = "(" + splitQuery + ") UNION ALL (" + renderedUnionBranch.splitQuery + ")";
            orderedComparisonEnabled = false;
        }
        rejectUnorderedTopLevelLimit(singleQuery, splitQuery);
        return new VpQueryPair(vp, strategy, singleQuery, splitQuery, extraSetup,
                "shape=" + vp.baseShape + ", split=" + vp.splitStrategy + ", reconstruction=" + strategy,
                orderedComparisonEnabled);
    }

    private RenderedSelectPair renderSelectPairWithStableExpressions(OceanBaseSelect select, String singleRelation,
            String singleExtraHint, String splitRelation, String splitExtraHint) {
        select.setHint(null);
        select.setFromList(List.of(new OceanBaseText(VP_RELATION_PLACEHOLDER)));
        String template = OceanBaseVisitor.asString(select);
        return new RenderedSelectPair(renderSelectFromTemplate(template, singleRelation, singleExtraHint),
                renderSelectFromTemplate(template, splitRelation, splitExtraHint));
    }

    private String renderSelectFromTemplate(String template, String relation, String extraHint) {
        String sql = template.replace(VP_RELATION_PLACEHOLDER, relation);
        String hint = buildVpSelectHint(extraHint);
        if (hint.isBlank()) {
            return sql;
        }
        String selectPrefix = "SELECT ";
        if (!sql.startsWith(selectPrefix)) {
            throw new AssertionError(sql);
        }
        return selectPrefix + "/*+ " + hint + " */ " + sql.substring(selectPrefix.length());
    }

    private String buildVpSelectHint(String extraHint) {
        List<String> hints = new ArrayList<>();
        if (extraHint != null && !extraHint.isBlank()) {
            hints.add(extraHint);
        }
        int parallelDegree = vpOceanBaseParallelDegree();
        if (parallelDegree > 0) {
            hints.add("PARALLEL(" + parallelDegree + ")");
        }
        return String.join(" ", hints);
    }

    private String sourceColumnStoreHint(VpSchema vp) {
        if (VP_OB_COLUMN_STORE_PROFILE_NONE.equals(vpOceanBaseColumnStoreProfile())) {
            return "";
        }
        return columnTableHints(vp.sourceTable);
    }

    private String columnTableHints(String... tableAliases) {
        if (VP_OB_COLUMN_STORE_PROFILE_NONE.equals(vpOceanBaseColumnStoreProfile())) {
            return "";
        }
        List<String> hints = new ArrayList<>();
        for (String alias : tableAliases) {
            hints.add("USE_COLUMN_TABLE(" + alias + ")");
        }
        return String.join(" ", hints);
    }

    private List<OceanBaseExpression> appendRollup(List<OceanBaseExpression> groupByExpressions) {
        List<OceanBaseExpression> rollup = new ArrayList<>(groupByExpressions);
        int last = rollup.size() - 1;
        rollup.set(last, new OceanBaseText(OceanBaseVisitor.asString(rollup.get(last)) + " WITH ROLLUP"));
        return rollup;
    }

    private VpQueryPair generateCorrelatedSubqueryPair(VpSchema vp, ReconstructionStrategy strategy,
            SourceAst sourceAst) throws Exception {
        recordAdvancedFeature(AdvancedFeature.CORRELATED_SUBQUERY);
        String sourceAlias = "s";
        String subAlias = "s2";
        String selected = simpleColumnOrRowIdForSQL(sourceAst.columns);
        String outerColumn = qualifySelectedExpression(selected, sourceAlias);
        String predicateColumn = qualifySelectedExpression(simpleColumnOrRowIdForSQL(sourceAst.columns), subAlias);
        int template = state.getRandomly().getInteger(0, 3);
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        String singleQuery = buildCorrelatedQuery(template, outerColumn, predicateColumn,
                quote(vp.sourceTable) + " " + sourceAlias, quote(vp.sourceTable) + " " + subAlias, sourceAlias,
                subAlias);
        String splitQuery = buildCorrelatedQuery(template, outerColumn, predicateColumn,
                relationWithAlias(relation, vp.sourceTable, sourceAlias),
                relationWithAlias(relation, vp.sourceTable, subAlias), sourceAlias, subAlias);
        rejectUnorderedTopLevelLimit(singleQuery, splitQuery);
        return new VpQueryPair(vp, strategy, singleQuery, splitQuery, extraSetup, "shape=" + vp.baseShape + ", split="
                + vp.splitStrategy + ", reconstruction=" + strategy + ", advanced=correlated-subquery", false);
    }

    private String buildCorrelatedQuery(int template, String outerColumn, String predicateColumn, String outerRelation,
            String subqueryRelation, String sourceAlias, String subAlias) {
        switch (template) {
        case 0:
            return "SELECT " + outerColumn + " FROM " + outerRelation + " WHERE EXISTS (SELECT 1 FROM "
                    + subqueryRelation + " WHERE " + subAlias + "." + quote(ROW_ID) + " = " + sourceAlias + "."
                    + quote(ROW_ID) + " AND " + predicateColumn + " IS NOT NULL)";
        case 1:
            return "SELECT " + outerColumn + " FROM " + outerRelation + " WHERE " + sourceAlias + "." + quote(ROW_ID)
                    + " IN (SELECT " + subAlias + "." + quote(ROW_ID) + " FROM " + subqueryRelation + " WHERE "
                    + predicateColumn + " IS NOT NULL)";
        case 2:
            return "SELECT " + outerColumn + ", (SELECT MIN(" + predicateColumn + ") FROM " + subqueryRelation
                    + " WHERE " + subAlias + "." + quote(ROW_ID) + " <= " + sourceAlias + "." + quote(ROW_ID)
                    + ") FROM " + outerRelation;
        default:
            throw new AssertionError(template);
        }
    }

    private String qualifySelectedExpression(String expression, String alias) {
        return expression.replace(quote(ROW_ID), alias + "." + quote(ROW_ID));
    }

    private String relationWithAlias(String relation, String sourceTable, String alias) {
        String suffix = " " + quote(sourceTable);
        if (!relation.endsWith(suffix)) {
            throw new IgnoreMeException();
        }
        return relation.substring(0, relation.length() - suffix.length()) + " " + alias;
    }

    private boolean isGroupByOrdinalLiteral(OceanBaseExpression expression) {
        if (expression instanceof OceanBaseConstant && ((OceanBaseConstant) expression).isInt()) {
            return true;
        }
        // Non-aggregate expressions are stabilized before group-by selection. A bare
        // integer is then an OceanBaseText, but MySQL interprets it as a select-list ordinal.
        return OceanBaseVisitor.asString(expression).strip().matches("[0-9]+");
    }

    private List<OceanBaseExpression> chooseGroupByExpressions(List<OceanBaseExpression> nonAggregateExpressions) {
        return nonAggregateExpressions.stream().filter(e -> !isGroupByOrdinalLiteral(e)).map(this::stableExpression)
                .collect(Collectors.toList());
    }

    private OceanBaseExpression stableExpression(OceanBaseExpression expression) {
        if (expression instanceof OceanBaseText) {
            return expression;
        }
        return new OceanBaseText(OceanBaseVisitor.asString(expression));
    }

    private boolean isRollupSafeGroupBy(List<OceanBaseExpression> groupByExpressions, List<OceanBaseColumn> columns) {
        return !groupByExpressions.isEmpty()
                && groupByExpressions.stream().allMatch(e -> referencesSourceColumn(sqlWithoutRollup(e), columns));
    }

    private boolean referencesSourceColumn(String expression, List<OceanBaseColumn> columns) {
        String normalized = expression.toLowerCase(Locale.ROOT);
        for (OceanBaseColumn column : columns) {
            String name = column.getName().toLowerCase(Locale.ROOT);
            if (normalized.contains("." + quote(name).toLowerCase(Locale.ROOT))
                    || normalized.contains(quote(name).toLowerCase(Locale.ROOT))
                    || normalized.matches(".*\\b" + java.util.regex.Pattern.quote(name) + "\\b.*")) {
                return true;
            }
        }
        return false;
    }

    private OceanBaseExpression generateHavingExpression(OceanBaseExpressionGenerator gen,
            List<OceanBaseColumn> columns, List<OceanBaseExpression> groupByExpressions) {
        List<String> candidates = new ArrayList<>();
        candidates.add("COUNT(*) > 0");
        candidates.add("SUM(" + numericExpressionForSQL(columns) + ") IS NOT NULL");
        candidates.add(
                "MIN(" + simpleColumnOrRowIdForSQL(columns) + ") <=> MAX(" + simpleColumnOrRowIdForSQL(columns) + ")");

        if (!groupByExpressions.isEmpty()) {
            String groupBy = sqlWithoutRollup(Randomly.fromList(groupByExpressions));
            if (!groupBy.isBlank()) {
                candidates.add("(" + groupBy + ") IS NOT NULL");
                candidates.add("COUNT(" + groupBy + ") >= 0");
                candidates.add("MIN(" + groupBy + ") <=> MAX(" + groupBy + ")");
            }
        }

        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            String expression = OceanBaseVisitor.asString(gen.generateExpression());
            candidates.add("COUNT(" + expression + ") >= 0");
            candidates.add("MIN(CAST((" + expression + ") AS CHAR)) IS NOT NULL");
            candidates.add("SUM(CAST((" + expression + ") AS SIGNED)) IS NOT NULL");
        }
        return new OceanBaseText(Randomly.fromList(candidates));
    }

    private OceanBaseExpression generateRollupHavingExpression(List<OceanBaseColumn> columns,
            List<OceanBaseExpression> groupByExpressions) {
        List<String> candidates = new ArrayList<>();
        candidates.add("COUNT(*) > 0");
        candidates.add("SUM(" + numericExpressionForSQL(columns) + ") IS NOT NULL");
        candidates.add(
                "MIN(" + simpleColumnOrRowIdForSQL(columns) + ") <=> MAX(" + simpleColumnOrRowIdForSQL(columns) + ")");
        if (!groupByExpressions.isEmpty()) {
            String groupBy = sqlWithoutRollup(Randomly.fromList(groupByExpressions));
            if (!groupBy.isBlank()) {
                candidates.add("(" + groupBy + ") IS NOT NULL");
                candidates.add("COUNT(" + groupBy + ") >= 0");
            }
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

    private boolean shouldGenerateLimitForQuery(boolean hasWindowExpression, boolean rowPreservingQuery,
            OceanBaseSelect select, OceanBaseColumn rowIdColumn) {
        if (!hasWindowExpression) {
            return true;
        }
        if (rowPreservingQuery) {
            return true;
        }
        return hasDeterministicOuterOrder(select, rowIdColumn);
    }

    private boolean hasDeterministicOuterOrder(OceanBaseSelect select, OceanBaseColumn rowIdColumn) {
        return hasVpRowIdTieBreaker(select, rowIdColumn);
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

    private OceanBaseSelect createUnionAllBranch(SourceAst sourceAst, OceanBaseExpressionGenerator gen,
            int fetchColumns) {
        OceanBaseSelect right = new OceanBaseSelect();
        right.setSelectType(OceanBaseSelect.SelectType.ALL);
        right.setForceExplicitAll(true);
        List<OceanBaseExpression> selected = new ArrayList<>();
        for (int i = 0; i < fetchColumns; i++) {
            selected.add(capabilities.windowFunction && Randomly.getBooleanWithRatherLowProbability()
                    ? generateAdvancedSelectExpression(sourceAst) : gen.generateExpression());
        }
        right.setFetchColumns(selected);
        if (Randomly.getBoolean()) {
            right.setWhereClause(gen.generateExpression());
        }
        return right;
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
                        ? new OceanBaseText("CAST(" + numericExpressionForSQL(columns) + " AS SIGNED)")
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
                return new OceanBaseText("CAST(" + OceanBaseVisitor.asString(ref) + " AS DECIMAL(65, 30))");
            }
            return ref;
        }
        return gen.generateExpression();
    }

    private String randomSumCastType() {
        return Randomly.fromOptions("SIGNED", "UNSIGNED", "DOUBLE", "DECIMAL(10, 2)", "DECIMAL(20, 5)",
                "DECIMAL(65, 30)");
    }

    private String randomAdvancedAggregateSQL(List<OceanBaseColumn> columns) {
        if (advancedFeaturesEnabled() && capabilities.groupConcat && Randomly.getBooleanWithSmallProbability()) {
            recordAdvancedFeature(AdvancedFeature.GROUP_CONCAT);
            return "GROUP_CONCAT(" + groupConcatInputForSQL(columns) + " ORDER BY " + quote(ROW_ID) + " SEPARATOR ',')";
        }
        String numeric = numericExpressionForSQL(columns);
        String integer = "CAST(" + numeric + " AS SIGNED)";
        return Randomly.fromOptions("AVG(" + numeric + ")", "BIT_AND(" + integer + ")", "BIT_OR(" + integer + ")",
                "BIT_XOR(" + integer + ")", "STD(" + numeric + ")", "VAR_SAMP(" + numeric + ")");
    }

    private String groupConcatInputForSQL(List<OceanBaseColumn> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        OceanBaseColumn column = Randomly.fromList(columns);
        if (column.getType() == OceanBaseDataType.VARCHAR || Randomly.getBoolean()) {
            return "CAST(" + quote(column.getName()) + " AS CHAR)";
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
        private final OceanBaseColumn rowIdColumn;
        private final List<OceanBaseColumn> columns;

        private SourceAst(OceanBaseColumn rowIdColumn, List<OceanBaseColumn> columns) {
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
        return new SourceAst(rowId, astColumns);
    }

    private PairExecutionResult comparePair(VpQueryPair pair) throws Exception {
        return comparePair(pair, StoragePhase.DEFAULT);
    }

    private PairExecutionResult comparePair(VpQueryPair pair, StoragePhase storagePhase) throws Exception {
        executeExtraSetupOnce(pair);
        QueryResultMultiset singleRows = getResultMultiset(pair.singleQuery);
        QueryResultMultiset splitRows = getResultMultiset(pair.splitQuery);
        if (handleTopLevelLimitZeroResult(pair, singleRows, splitRows)) {
            return new PairExecutionResult(singleRows, splitRows);
        }
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
            if (isPxRollupExpansionCandidate(pair)) {
                PxRollupStabilityResult stableResult = stabilizePxRollupMismatch(pair, singleRows, splitRows);
                if (stableResult == null) {
                    recordSkippedErrorKind("Flaky PX ROLLUP EXPANSION result");
                    throw new IgnoreMeException();
                }
                singleRows = stableResult.singleRows;
                splitRows = stableResult.splitRows;
            }
            throw new AssertionError(formatBugReport(pair, singleRows, splitRows, storagePhase));
        }
        if (pair.orderedComparisonEnabled && !orderedRowsMatch(singleRows, splitRows)) {
            KnownVpBug knownBug = classifyKnownOrderedBug(pair, singleRows, splitRows);
            if (suppressKnownBug(pair, knownBug, singleRows, splitRows, true)) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(formatOrderedBugReport(pair, singleRows, splitRows, storagePhase));
        }
        return new PairExecutionResult(singleRows, splitRows);
    }

    private void executeExtraSetupOnce(VpQueryPair pair) throws Exception {
        if (pair.extraSetupExecuted) {
            return;
        }
        for (String setup : pair.extraSetupStatements) {
            executeNoSchemaRefresh(setup);
            pair.schema.setupStatements.add(setup);
        }
        pair.extraSetupExecuted = true;
    }

    private void compareStoragePhaseResult(VpQueryPair pair, String side, QueryResultMultiset memtableRows,
            QueryResultMultiset sstableRows) throws SQLException {
        if (!resultMultisetsMatch(memtableRows, sstableRows)) {
            throw new AssertionError(formatStoragePhaseBugReport(pair, side, memtableRows, sstableRows));
        }
        if (pair.orderedComparisonEnabled && !orderedRowsMatch(memtableRows, sstableRows)) {
            throw new AssertionError(formatStoragePhaseOrderedBugReport(pair, side, memtableRows, sstableRows));
        }
    }

    private void freezeAndWaitForSstablePhase(List<VpSchema> schemas) {
        ensureStorageFreezeCooldownOpen(schemas);
        waitForCompactionIdleBeforeFreeze(schemas);
        MajorCompactionStatus before = readMajorCompactionStatus();
        appendStorageSetup(schemas,
                "-- storage-profile=" + vpOceanBaseStorageProfile() + " before major freeze: " + before);
        if (!before.available) {
            recordSkippedError(new RuntimeException("OceanBase major compaction status unavailable: " + before.info));
            throw new IgnoreMeException();
        }
        try (Statement s = state.getConnection().createStatement()) {
            s.execute("ALTER SYSTEM MAJOR FREEZE");
        } catch (SQLException e) {
            recordSkippedError(e);
            throw new IgnoreMeException();
        }
        appendStorageSetup(schemas, "ALTER SYSTEM MAJOR FREEZE");
        MajorCompactionStatus target = readMajorCompactionStatus();
        appendStorageSetup(schemas,
                "-- storage-profile=" + vpOceanBaseStorageProfile() + " after major freeze request: " + target);
        if (!target.available) {
            recordSkippedError(
                    new RuntimeException("OceanBase major compaction status unavailable after freeze: " + target.info));
            throw new IgnoreMeException();
        }
        long deadline = System.currentTimeMillis() + STORAGE_PROFILE_FREEZE_TIMEOUT_MS;
        MajorCompactionStatus current = target;
        while (System.currentTimeMillis() < deadline) {
            current = readMajorCompactionStatus();
            if (!current.available) {
                break;
            }
            if ("YES".equalsIgnoreCase(current.isError)) {
                appendStorageSetup(schemas,
                        "-- storage-profile=" + vpOceanBaseStorageProfile() + " major compaction failed: " + current);
                recordSkippedError(new RuntimeException("OceanBase major compaction entered error state: " + current));
                throw new IgnoreMeException();
            }
            if (!current.compactionActive()) {
                appendStorageSetup(schemas,
                        "-- storage-profile=" + vpOceanBaseStorageProfile() + " major compaction finished: " + current);
                return;
            }
            sleepForStorageProfilePoll();
        }
        enterStorageFreezeCooldown();
        appendStorageSetup(schemas,
                "-- storage-profile=" + vpOceanBaseStorageProfile() + " major compaction wait timed out: " + current);
        recordSkippedError(new RuntimeException("OceanBase major compaction did not finish within "
                + STORAGE_PROFILE_FREEZE_TIMEOUT_MS + "ms: " + current));
        throw new IgnoreMeException();
    }

    private void ensureStorageFreezeCooldownOpen(List<VpSchema> schemas) {
        long now = System.currentTimeMillis();
        if (now >= storageProfileNextFreezeAllowedAtMillis) {
            return;
        }
        long remainingMs = storageProfileNextFreezeAllowedAtMillis - now;
        appendStorageSetup(schemas, "-- storage-profile=" + vpOceanBaseStorageProfile()
                + " major freeze cooldown active, remainingMs=" + remainingMs);
        recordSkippedError(new RuntimeException(
                "OceanBase major compaction cooldown active for " + remainingMs + "ms after a previous timeout"));
        throw new IgnoreMeException();
    }

    private void waitForCompactionIdleBeforeFreeze(List<VpSchema> schemas) {
        long deadline = System.currentTimeMillis() + STORAGE_PROFILE_PRE_FREEZE_WAIT_MS;
        MajorCompactionStatus current = readMajorCompactionStatus();
        appendStorageSetup(schemas,
                "-- storage-profile=" + vpOceanBaseStorageProfile() + " pre-freeze compaction status: " + current);
        while (current.available && current.compactionActive() && System.currentTimeMillis() < deadline) {
            if ("YES".equalsIgnoreCase(current.isError)) {
                appendStorageSetup(schemas, "-- storage-profile=" + vpOceanBaseStorageProfile()
                        + " pre-freeze major compaction failed: " + current);
                recordSkippedError(new RuntimeException(
                        "OceanBase major compaction entered error state before freeze: " + current));
                throw new IgnoreMeException();
            }
            sleepForStorageProfilePoll();
            current = readMajorCompactionStatus();
        }
        if (!current.available) {
            recordSkippedError(new RuntimeException(
                    "OceanBase major compaction status unavailable before freeze: " + current.info));
            throw new IgnoreMeException();
        }
        if (current.compactionActive()) {
            enterStorageFreezeCooldown();
            appendStorageSetup(schemas, "-- storage-profile=" + vpOceanBaseStorageProfile()
                    + " pre-freeze compaction still active, skipping new major freeze: " + current);
            recordSkippedError(new RuntimeException("OceanBase major compaction still active before freeze after "
                    + STORAGE_PROFILE_PRE_FREEZE_WAIT_MS + "ms: " + current));
            throw new IgnoreMeException();
        }
    }

    private void enterStorageFreezeCooldown() {
        storageProfileNextFreezeAllowedAtMillis = System.currentTimeMillis()
                + STORAGE_PROFILE_FREEZE_COOLDOWN_AFTER_TIMEOUT_MS;
    }

    private void appendStorageSetup(List<VpSchema> schemas, String setup) {
        for (VpSchema schema : schemas) {
            schema.setupStatements.add(setup);
        }
    }

    private void sleepForStorageProfilePoll() {
        try {
            Thread.sleep(STORAGE_PROFILE_FREEZE_POLL_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IgnoreMeException();
        }
    }

    private MajorCompactionStatus readMajorCompactionStatus() {
        String sql = "SELECT FROZEN_SCN, LAST_SCN, STATUS, IS_ERROR, INFO FROM oceanbase.DBA_OB_MAJOR_COMPACTION";
        String tenant = getSingleString("SELECT effective_tenant_id()");
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            long maxFrozenScn = -1;
            long minLastScn = Long.MAX_VALUE;
            boolean anyRow = false;
            boolean anyError = false;
            List<String> statuses = new ArrayList<>();
            List<String> infos = new ArrayList<>();
            while (rs.next()) {
                anyRow = true;
                maxFrozenScn = Math.max(maxFrozenScn, rs.getLong(1));
                minLastScn = Math.min(minLastScn, rs.getLong(2));
                String status = rs.getString(3);
                String isError = rs.getString(4);
                String info = rs.getString(5);
                if (status != null && !statuses.contains(status)) {
                    statuses.add(status);
                }
                if ("YES".equalsIgnoreCase(isError)) {
                    anyError = true;
                }
                if (info != null && !info.isBlank()) {
                    infos.add(info);
                }
            }
            if (!anyRow) {
                return MajorCompactionStatus.unavailable("DBA_OB_MAJOR_COMPACTION returned no rows");
            }
            String status = statuses.isEmpty() ? "<empty>" : String.join("|", statuses);
            String info = "rows=" + (infos.isEmpty() ? "0" : infos.size()) + ", tenant=" + tenant + ", info="
                    + String.join("|", infos);
            return new MajorCompactionStatus(true, maxFrozenScn, minLastScn, status, anyError ? "YES" : "NO", info);
        } catch (SQLException e) {
            return MajorCompactionStatus.unavailable(e.getMessage());
        }
    }

    private boolean handleTopLevelLimitZeroResult(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) throws SQLException {
        if (!hasTopLevelLimitZero(pair.singleQuery) || !hasTopLevelLimitZero(pair.splitQuery)) {
            return false;
        }
        if (totalRowCount(singleRows) == 0 && totalRowCount(splitRows) == 0) {
            return true;
        }
        for (int i = 0; i < LIMIT_ZERO_RECHECK_ATTEMPTS; i++) {
            QueryResultMultiset confirmedSingleRows = getResultMultiset(pair.singleQuery);
            QueryResultMultiset confirmedSplitRows = getResultMultiset(pair.splitQuery);
            if (totalRowCount(confirmedSingleRows) == 0 && totalRowCount(confirmedSplitRows) == 0) {
                recordSkippedError(new FlakyLimitZeroResultException(
                        "top-level LIMIT 0 VP result returned rows, then became empty on recheck"));
                throw new IgnoreMeException();
            }
        }
        recordSkippedError(new FlakyLimitZeroResultException(
                "top-level LIMIT 0 VP result returned rows despite deterministic empty semantics"));
        throw new IgnoreMeException();
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
        KnownVpBug countNullBug = classifyCountNullPushdownAggregationDiff(pair, singleRows, splitRows);
        if (countNullBug != null) {
            return countNullBug;
        }
        KnownVpBug avgNullBug = classifyAvgNullPushdownAggregationDiff(pair, singleRows, splitRows);
        if (avgNullBug != null) {
            return avgNullBug;
        }
        if (isStddevNullNearZeroParallelPushdownDiff(pair, singleRows, splitRows)) {
            return KnownVpBug.STDDEV_NULL_NEAR_ZERO_PARALLEL_PUSHDOWN;
        }
        if (isExtremeFloatingAggregateOrderDiff(pair, singleRows, splitRows)) {
            return KnownVpBug.EXTREME_FLOATING_AGGREGATE_ORDER;
        }
        if (isGroupByIfNullPredicateProjectionDropsIfNull(pair, singleRows, splitRows)) {
            return KnownVpBug.GROUP_BY_IFNULL_NOT_IN_PROJECTION_DROPS_IFNULL;
        }
        if (isRollupExpressionExpansionNullRecomputeDiff(pair, singleRows, splitRows)) {
            return KnownVpBug.ROLLUP_EXPRESSION_EXPANSION_NULL_RECOMPUTE_DIFF;
        }
        if (isRollupNullSemanticGroupKeySubqueryDiff(pair, singleRows, splitRows)) {
            return KnownVpBug.ROLLUP_NULL_SEMANTIC_GROUP_KEY_SUBQUERY_DIFF;
        }
        if (isRollupGroupExpressionRepresentativeValueDiff(pair, singleRows, splitRows)) {
            return KnownVpBug.ROLLUP_GROUP_EXPRESSION_REPRESENTATIVE_VALUE_DIFF;
        }
        if (isRollupBitAndComplexGroupKeyDiff(pair, singleRows, splitRows)) {
            return KnownVpBug.ROLLUP_BIT_AND_COMPLEX_GROUP_KEY_DIFF;
        }
        if (isGroupByNondeterministicRepresentativeRowDiff(pair, singleRows, splitRows)) {
            return KnownVpBug.GROUP_BY_SELECTS_NON_GROUPED_COLUMN_REPRESENTATIVE_ROW;
        }
        return null;
    }

    private boolean isStddevNullNearZeroParallelPushdownDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (vpOceanBaseParallelDegree() <= 0 || pair.schema.baseRowCount < 2
                || !isStandaloneStddevQuery(pair.singleQuery) || !isStandaloneStddevQuery(pair.splitQuery)
                || !queryHasPushdownAggregation(pair.singleQuery) || queryHasPushdownAggregation(pair.splitQuery)) {
            return false;
        }
        List<String> onlySingle = expandedUnmatchedRows(singleRows, splitRows);
        List<String> onlySplit = expandedUnmatchedRows(splitRows, singleRows);
        if (onlySingle.size() != 1 || onlySplit.size() != 1 || totalRowCount(singleRows) != 1
                || totalRowCount(splitRows) != 1) {
            return false;
        }
        List<String> singleValues = decodeCanonicalRow(onlySingle.get(0));
        List<String> splitValues = decodeCanonicalRow(onlySplit.get(0));
        if (singleValues.size() != 1 || splitValues.size() != 1 || ApproximateKind.looser(
                getApproximateKind(singleRows.approximateColumns.get(onlySingle.get(0)), 0),
                getApproximateKind(splitRows.approximateColumns.get(onlySplit.get(0)), 0)) != ApproximateKind.DOUBLE) {
            return false;
        }
        String singleValue = singleValues.get(0);
        String splitValue = splitValues.get(0);
        return (singleValue == null && isFiniteNearZero(splitValue))
                || (splitValue == null && isFiniteNearZero(singleValue))
                || (isFiniteNearZero(singleValue) && isFiniteNearZero(splitValue));
    }

    private boolean isExtremeFloatingAggregateOrderDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (vpOceanBaseParallelDegree() <= 0 || pair.schema.baseRowCount < 2
                || !hasFloatingOrderSensitiveAggregate(pair.singleQuery)
                || !hasFloatingOrderSensitiveAggregate(pair.splitQuery)) {
            return false;
        }
        FloatingAggregateScale scale = floatingAggregateScale(pair.schema, pair.singleQuery);
        if (scale == null) {
            return false;
        }
        List<String> onlySingle = expandedUnmatchedRows(singleRows, splitRows);
        List<String> onlySplit = expandedUnmatchedRows(splitRows, singleRows);
        if (onlySingle.isEmpty() || onlySingle.size() != onlySplit.size() || onlySingle.size() > MAX_DIFF_ROWS) {
            return false;
        }
        boolean[] used = new boolean[onlySplit.size()];
        for (String singleRow : onlySingle) {
            boolean matched = false;
            for (int i = 0; i < onlySplit.size(); i++) {
                if (!used[i] && rowsMatchWithinFloatingAggregateBudget(singleRow, singleRows, onlySplit.get(i),
                        splitRows, scale.errorBudget)) {
                    used[i] = true;
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

    private boolean hasFloatingOrderSensitiveAggregate(String query) {
        String sql = normalizeSqlForKnownBug(query);
        return sql.contains("AVG(") || sql.contains("SUM(") || sql.contains("VAR_") || sql.contains("STD(");
    }

    private FloatingAggregateScale floatingAggregateScale(VpSchema schema, String query) {
        for (VpColumn column : schema.columns) {
            if (column.type != OceanBaseDataType.FLOAT && column.type != OceanBaseDataType.DOUBLE
                    || !queryContainsFloatingAggregateOfColumn(query, column.name)) {
                continue;
            }
            FloatingAggregateScale scale = readFloatingAggregateScale(schema.sourceTable, column);
            if (scale != null && scale.hasOppositeExtremeValues()) {
                return scale;
            }
        }
        return null;
    }

    private boolean queryContainsFloatingAggregateOfColumn(String query, String columnName) {
        for (String aggregate : List.of("AVG(", "SUM(", "VAR_SAMP(", "VAR_POP(", "STD(")) {
            for (String argument : aggregateArguments(normalizeSqlForKnownBug(query), aggregate)) {
                if (aggregateArgumentUsesColumn(argument, columnName)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean aggregateArgumentUsesColumn(String argument, String columnName) {
        String normalized = argument.replace('`', ' ').replaceAll("\\s+", " ").trim().toUpperCase(Locale.ROOT);
        String unwrapped = stripBalancedOuterParentheses(normalized);
        if (isColumnReference(unwrapped, columnName)) {
            return true;
        }
        String compact = unwrapped.replace(" ", "");
        String concatSuffix = ",''" + ")";
        if (compact.startsWith("CONCAT(") && compact.endsWith(concatSuffix)) {
            return isColumnReference(compact.substring("CONCAT(".length(), compact.length() - concatSuffix.length()),
                    columnName);
        }
        if (unwrapped.startsWith("CAST(") && unwrapped.endsWith(")")) {
            int asIndex = unwrapped.lastIndexOf(" AS ");
            if (asIndex > "CAST(".length()) {
                return aggregateArgumentUsesColumn(unwrapped.substring("CAST(".length(), asIndex), columnName);
            }
        }
        return false;
    }

    private String stripBalancedOuterParentheses(String expression) {
        String result = expression;
        while (result.startsWith("(") && result.endsWith(")")
                && findMatchingParenthesis(result, 0) == result.length() - 1) {
            result = result.substring(1, result.length() - 1).trim();
        }
        return result;
    }

    private boolean isColumnReference(String expression, String columnName) {
        String compact = expression.replaceAll("\\s+", "");
        String column = columnName.toUpperCase(Locale.ROOT);
        return compact.equals(column) || compact.endsWith("." + column);
    }

    private FloatingAggregateScale readFloatingAggregateScale(String sourceTable, VpColumn column) {
        double maxPositive = 0;
        double maxNegativeMagnitude = 0;
        double maxMagnitude = 0;
        double scaledAbsoluteSum = 0;
        List<Double> values = new ArrayList<>();
        String sql = "SELECT " + quote(column.name) + " FROM " + quote(sourceTable) + " WHERE " + quote(column.name)
                + " IS NOT NULL";
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                double value = Double.parseDouble(rs.getString(1));
                if (!Double.isFinite(value)) {
                    return null;
                }
                values.add(value);
                maxMagnitude = Math.max(maxMagnitude, Math.abs(value));
                if (value > 0) {
                    maxPositive = Math.max(maxPositive, value);
                } else if (value < 0) {
                    maxNegativeMagnitude = Math.max(maxNegativeMagnitude, -value);
                }
            }
        } catch (SQLException | NumberFormatException ignored) {
            return null;
        }
        if (values.isEmpty() || maxMagnitude == 0) {
            return null;
        }
        for (double value : values) {
            scaledAbsoluteSum += Math.abs(value) / maxMagnitude;
        }
        double unitRoundoff = column.type == OceanBaseDataType.FLOAT ? FLOAT_UNIT_ROUNDOFF : DOUBLE_UNIT_ROUNDOFF;
        double errorBudget = FLOATING_AGGREGATE_ERROR_FACTOR * unitRoundoff * maxMagnitude * scaledAbsoluteSum
                / values.size();
        return new FloatingAggregateScale(maxPositive, maxNegativeMagnitude, errorBudget,
                column.type == OceanBaseDataType.FLOAT);
    }

    private boolean rowsMatchWithinFloatingAggregateBudget(String leftRow, QueryResultMultiset leftRows,
            String rightRow, QueryResultMultiset rightRows, double errorBudget) {
        List<String> leftValues = decodeCanonicalRow(leftRow);
        List<String> rightValues = decodeCanonicalRow(rightRow);
        if (leftValues.size() != rightValues.size() || leftValues.isEmpty()) {
            return false;
        }
        boolean sawFloatingDifference = false;
        for (int i = 0; i < leftValues.size(); i++) {
            String left = leftValues.get(i);
            String right = rightValues.get(i);
            if (Objects.equals(left, right)) {
                continue;
            }
            ApproximateKind kind = ApproximateKind.looser(
                    getApproximateKind(leftRows.approximateColumns.get(leftRow), i),
                    getApproximateKind(rightRows.approximateColumns.get(rightRow), i));
            if (!kind.isApproximate() || left == null || right == null
                    || !withinAbsoluteErrorBudget(left, right, errorBudget)) {
                return false;
            }
            sawFloatingDifference = true;
        }
        return sawFloatingDifference;
    }

    private boolean withinAbsoluteErrorBudget(String left, String right, double errorBudget) {
        try {
            double difference = Math.abs(Double.parseDouble(left) - Double.parseDouble(right));
            return Double.isFinite(difference) && difference <= errorBudget;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    private boolean isRollupGroupExpressionRepresentativeValueDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        String single = normalizeSqlForKnownBug(pair.singleQuery);
        if (!isParallelRollupExpansionWithSplitJoin(pair, single) || single.contains("BIT_AND(")) {
            return false;
        }
        boolean ifNullGreatest = single.contains("IFNULL(GREATEST(");
        boolean bitOrBooleanKey = single.contains("BIT_OR(") && single.contains(" IS FALSE");
        if (ifNullGreatest || bitOrBooleanKey) {
            return hasRepresentativeRowDiffShape(singleRows, splitRows)
                    || hasRollupSubtotalNullPresenceDiff(singleRows, splitRows);
        }
        if (!rollupGroupByContains(single, "COALESCE(") || !single.contains("VP_ROWID")) {
            return false;
        }
        if (hasRepresentativeRowDiffShape(singleRows, splitRows)) {
            return true;
        }
        return !containsAggregateFunction(single) && single.contains(" NOT IN ") && single.contains(" IN (")
                && hasTopLevelClause(pair.singleQuery, "ORDER") && hasTopLevelClause(pair.singleQuery, "LIMIT")
                && hasSingleColumnBooleanRepresentativeDiff(singleRows, splitRows);
    }

    private boolean rollupGroupByContains(String sql, String fragment) {
        int groupBy = sql.indexOf(" GROUP BY ");
        int rollup = sql.indexOf(" WITH ROLLUP", groupBy);
        return groupBy >= 0 && rollup > groupBy && sql.substring(groupBy, rollup).contains(fragment);
    }

    private boolean hasSingleColumnBooleanRepresentativeDiff(QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        List<String> onlySingle = expandedUnmatchedRows(singleRows, splitRows);
        List<String> onlySplit = expandedUnmatchedRows(splitRows, singleRows);
        if (onlySingle.size() != 1 || onlySplit.size() != 1) {
            return false;
        }
        List<String> singleValues = decodeCanonicalRow(onlySingle.get(0));
        List<String> splitValues = decodeCanonicalRow(onlySplit.get(0));
        return singleValues.size() == 1 && splitValues.size() == 1 && isBooleanCanonicalValue(singleValues.get(0))
                && isBooleanCanonicalValue(splitValues.get(0))
                && !Objects.equals(singleValues.get(0), splitValues.get(0));
    }

    private boolean isRollupBitAndComplexGroupKeyDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        String single = normalizeSqlForKnownBug(pair.singleQuery);
        if (!isParallelRollupExpansionWithSplitJoin(pair, single) || !single.contains("BIT_AND(")
                || !single.contains("GREATEST(") || !single.contains(" IS FALSE")) {
            return false;
        }
        List<String> onlySingle = expandedUnmatchedRows(singleRows, splitRows);
        List<String> onlySplit = expandedUnmatchedRows(splitRows, singleRows);
        return onlySingle.size() == 1 && onlySplit.size() == 1
                && containsNullValue(decodeCanonicalRow(onlySingle.get(0)))
                && containsNullValue(decodeCanonicalRow(onlySplit.get(0))) && isRepresentativeRowValueDiff(
                        decodeCanonicalRow(onlySingle.get(0)), decodeCanonicalRow(onlySplit.get(0)));
    }

    private boolean isParallelRollupExpansionWithSplitJoin(VpQueryPair pair, String single) {
        if (vpOceanBaseParallelDegree() <= 0 || !single.contains(" WITH ROLLUP") || !single.contains(" GROUP BY ")) {
            return false;
        }
        String split = normalizeSqlForKnownBug(pair.splitQuery);
        if (!split.contains(" WITH ROLLUP")) {
            return false;
        }
        String splitExplain = explainText(pair.splitQuery).toUpperCase(Locale.ROOT);
        return explainContains(pair.singleQuery, "EXPANSION") && splitExplain.contains("EXPANSION")
                && (splitExplain.contains("SUBPLAN SCAN") || splitExplain.contains("JOIN")
                        || pair.reconstructionStrategy == ReconstructionStrategy.TEMP_TABLE);
    }

    private boolean hasRollupSubtotalNullPresenceDiff(QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        List<String> onlySingle = expandedUnmatchedRows(singleRows, splitRows);
        List<String> onlySplit = expandedUnmatchedRows(splitRows, singleRows);
        if (onlySingle.isEmpty() && onlySplit.size() == 1) {
            return containsNullValue(decodeCanonicalRow(onlySplit.get(0)));
        }
        if (onlySplit.isEmpty() && onlySingle.size() == 1) {
            return containsNullValue(decodeCanonicalRow(onlySingle.get(0)));
        }
        if (onlySingle.isEmpty() || onlySingle.size() != onlySplit.size() || onlySingle.size() > MAX_DIFF_ROWS) {
            return false;
        }
        for (String singleRow : onlySingle) {
            for (String splitRow : onlySplit) {
                if (containsNullValue(decodeCanonicalRow(singleRow)) != containsNullValue(
                        decodeCanonicalRow(splitRow))) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isStandaloneStddevQuery(String query) {
        String normalized = normalizeSqlForKnownBug(query);
        int firstStd = normalized.indexOf("STD(");
        return firstStd >= 0 && normalized.indexOf("STD(", firstStd + 1) < 0 && !hasTopLevelClause(query, "GROUP")
                && !hasTopLevelClause(query, "HAVING") && !hasTopLevelClause(query, "ORDER")
                && !hasTopLevelClause(query, "LIMIT") && !normalized.contains(" WITH ROLLUP")
                && !normalized.contains(" OVER ");
    }

    private boolean isFiniteNearZero(String value) {
        if (value == null) {
            return false;
        }
        try {
            double numeric = Double.parseDouble(value);
            return Double.isFinite(numeric) && Math.abs(numeric) <= STDDEV_NULL_NEAR_ZERO_TOLERANCE;
        } catch (NumberFormatException ignored) {
            return false;
        }
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
                }
            }
        }
        return false;
    }

    private void rejectUnorderedTopLevelLimit(String singleQuery, String splitQuery) {
        if ((hasTopLevelClause(singleQuery, "LIMIT") && !hasTopLevelOrderBy(singleQuery))
                || (hasTopLevelClause(splitQuery, "LIMIT") && !hasTopLevelOrderBy(splitQuery))) {
            throw new IgnoreMeException();
        }
    }

    private boolean hasTopLevelClause(String sql, String clause) {
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
                } else if (depth == 0 && upper.startsWith(clause, i) && isKeywordBoundary(upper, i, clause.length())) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasTopLevelOrderBy(String sql) {
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
                } else if (depth == 0 && upper.startsWith("ORDER", i)
                        && isKeywordBoundary(upper, i, "ORDER".length())) {
                    int by = i + "ORDER".length();
                    while (by < upper.length() && Character.isWhitespace(upper.charAt(by))) {
                        by++;
                    }
                    if (upper.startsWith("BY", by) && isKeywordBoundary(upper, by, "BY".length())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean isPxRollupExpansionCandidate(VpQueryPair pair) {
        String single = normalizeSqlForKnownBug(pair.singleQuery);
        String split = normalizeSqlForKnownBug(pair.splitQuery);
        if (!single.contains(" WITH ROLLUP") || !split.contains(" WITH ROLLUP")) {
            return false;
        }
        String singleExplain = explainText(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitExplain = explainText(pair.splitQuery).toUpperCase(Locale.ROOT);
        return singleExplain.contains("PX COORDINATOR") && splitExplain.contains("PX COORDINATOR")
                && singleExplain.contains("EXPANSION") && splitExplain.contains("EXPANSION");
    }

    private PxRollupStabilityResult stabilizePxRollupMismatch(VpQueryPair pair, QueryResultMultiset initialSingleRows,
            QueryResultMultiset initialSplitRows) throws Exception {
        Map<String, Integer> mismatchCounts = new LinkedHashMap<>();
        Map<String, PairExecutionResult> mismatchResults = new LinkedHashMap<>();
        StringBuilder trace = new StringBuilder();
        recordPxRollupStabilityRound(trace, "parallel-" + vpOceanBaseParallelDegree() + "-initial", pair.singleQuery,
                pair.splitQuery, initialSingleRows, initialSplitRows, mismatchCounts, mismatchResults);

        for (int round = 1; round <= PX_ROLLUP_STABILITY_RECHECKS; round++) {
            QueryResultMultiset singleRows = getResultMultiset(pair.singleQuery);
            QueryResultMultiset splitRows = getResultMultiset(pair.splitQuery);
            recordPxRollupStabilityRound(trace, "parallel-" + vpOceanBaseParallelDegree() + "-recheck-" + round,
                    pair.singleQuery, pair.splitQuery, singleRows, splitRows, mismatchCounts, mismatchResults);
        }

        if (isPxRollupTopNExpansionCandidate(pair)) {
            String initialSignature = resultSignature(initialSingleRows) + "|" + resultSignature(initialSplitRows);
            if (mismatchCounts.size() == 1
                    && mismatchCounts.getOrDefault(initialSignature, 0) == PX_ROLLUP_STABILITY_RECHECKS + 1) {
                return new PxRollupStabilityResult(initialSingleRows, initialSplitRows);
            }
            logFlakyPxRollupCandidate(pair, initialSingleRows, initialSplitRows, trace.toString());
            return null;
        }

        String serialSingleQuery = withParallelDegree(pair.singleQuery, 1);
        String serialSplitQuery = withParallelDegree(pair.splitQuery, 1);
        try {
            QueryResultMultiset serialSingleRows = getResultMultiset(serialSingleQuery);
            QueryResultMultiset serialSplitRows = getResultMultiset(serialSplitQuery);
            recordPxRollupStabilityRound(trace, "parallel-1", serialSingleQuery, serialSplitQuery, serialSingleRows,
                    serialSplitRows, new LinkedHashMap<>(), new LinkedHashMap<>());
        } catch (Exception | AssertionError e) {
            trace.append("parallel-1 error=").append(firstNonBlankLine(getErrorText(e))).append(System.lineSeparator());
        }

        for (Map.Entry<String, Integer> entry : mismatchCounts.entrySet()) {
            if (entry.getValue() >= 2) {
                PairExecutionResult stable = mismatchResults.get(entry.getKey());
                return new PxRollupStabilityResult(stable.singleRows, stable.splitRows);
            }
        }
        logFlakyPxRollupCandidate(pair, initialSingleRows, initialSplitRows, trace.toString());
        return null;
    }

    private boolean isPxRollupTopNExpansionCandidate(VpQueryPair pair) {
        if (!hasTopLevelClause(pair.singleQuery, "ORDER") || !hasTopLevelClause(pair.singleQuery, "LIMIT")) {
            return false;
        }
        return explainContains(pair.singleQuery, "TOP-N SORT") && explainContains(pair.splitQuery, "TOP-N SORT");
    }

    private void recordPxRollupStabilityRound(StringBuilder trace, String label, String singleQuery, String splitQuery,
            QueryResultMultiset singleRows, QueryResultMultiset splitRows, Map<String, Integer> mismatchCounts,
            Map<String, PairExecutionResult> mismatchResults) {
        boolean mismatch = !resultMultisetsMatch(singleRows, splitRows);
        trace.append(label).append(" mismatch=").append(mismatch).append(" sourceHash=").append(resultHash(singleRows))
                .append(" splitHash=").append(resultHash(splitRows)).append(System.lineSeparator());
        trace.append("Source plan [").append(label).append("]:").append(System.lineSeparator());
        trace.append(formatExplain(singleQuery));
        trace.append("Split plan [").append(label).append("]:").append(System.lineSeparator());
        trace.append(formatExplain(splitQuery));
        if (mismatch) {
            String signature = resultSignature(singleRows) + "|" + resultSignature(splitRows);
            mismatchCounts.put(signature, mismatchCounts.getOrDefault(signature, 0) + 1);
            mismatchResults.putIfAbsent(signature, new PairExecutionResult(singleRows, splitRows));
        }
    }

    private String withParallelDegree(String query, int degree) {
        int currentDegree = vpOceanBaseParallelDegree();
        if (currentDegree <= 0) {
            return query;
        }
        return query.replace("PARALLEL(" + currentDegree + ")", "PARALLEL(" + degree + ")");
    }

    private String resultSignature(QueryResultMultiset rows) {
        return rows.rows.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .map(entry -> entry.getKey() + "x" + entry.getValue()).collect(Collectors.joining(","));
    }

    private String resultHash(QueryResultMultiset rows) {
        return Integer.toUnsignedString(resultSignature(rows).hashCode(), 16);
    }

    private String trimTrailingSpaces(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == ' ') {
            end--;
        }
        return end == value.length() ? value : value.substring(0, end);
    }

    private boolean isGroupByIfNullPredicateProjectionDropsIfNull(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        String single = normalizeSqlForKnownBug(pair.singleQuery);
        if (!single.contains(" GROUP BY IFNULL(") || !single.contains("IFNULL(") || !single.contains(" HAVING SUM(")) {
            return false;
        }
        if (!single.contains(" NOT IN ")) {
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
        return explain.contains("GROUP([IFNULL(") && explain.contains("OUTPUT([")
                && !explain.contains("OUTPUT([IFNULL(");
    }

    private boolean isRollupExpressionExpansionNullRecomputeDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        String single = normalizeSqlForKnownBug(pair.singleQuery);
        String split = normalizeSqlForKnownBug(pair.splitQuery);
        if (!single.contains(" WITH ROLLUP") || !split.contains(" WITH ROLLUP")) {
            return false;
        }
        if (!single.contains(" GROUP BY ") || !hasRollupNullableExpression(single)) {
            return false;
        }
        // Older reproducers used a literal NULL. Newer ones reach the same EXPANSION
        // recomputation defect through a nullable base column, so accept either result shape.
        if (!hasNullVsBooleanDiff(singleRows, splitRows) && !hasRollupNullToValueRecomputeDiff(singleRows, splitRows)) {
            return false;
        }
        return explainContains(pair.singleQuery, "EXPANSION") && explainContains(pair.splitQuery, "EXPANSION")
                && (explainContains(pair.splitQuery, "SUBPLAN SCAN") || explainContains(pair.splitQuery, "JOIN"));
    }

    private boolean hasRollupNullableExpression(String sql) {
        if ((sql.contains("GREATEST(") || sql.contains("LEAST("))
                && (sql.contains(" IS TRUE") || sql.contains(" IS FALSE") || sql.contains(" IS UNKNOWN"))) {
            return true;
        }
        if (rollupGroupByContains(sql, " IS NOT TRUE") && sql.contains("VP_ROWID")) {
            return true;
        }
        return sql.contains("NULL") && (sql.contains("&&") || sql.contains("CAST(") || sql.contains("CONCAT("));
    }

    private boolean hasRollupNullToValueRecomputeDiff(QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        List<String> onlySingle = expandedUnmatchedRows(singleRows, splitRows);
        List<String> onlySplit = expandedUnmatchedRows(splitRows, singleRows);
        if (onlySingle.isEmpty() || onlySplit.isEmpty() || onlySingle.size() != onlySplit.size()
                || onlySingle.size() > MAX_DIFF_ROWS) {
            return false;
        }
        boolean[] matchedSplitRows = new boolean[onlySplit.size()];
        for (String singleRow : onlySingle) {
            List<String> singleValues = decodeCanonicalRow(singleRow);
            boolean matched = false;
            for (int i = 0; i < onlySplit.size(); i++) {
                if (matchedSplitRows[i]) {
                    continue;
                }
                if (rowsDifferInOneNullToValueColumn(singleValues, decodeCanonicalRow(onlySplit.get(i)))) {
                    matchedSplitRows[i] = true;
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

    private boolean isRollupNullSemanticGroupKeySubqueryDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        String single = normalizeSqlForKnownBug(pair.singleQuery);
        String split = normalizeSqlForKnownBug(pair.splitQuery);
        if (!single.contains(" WITH ROLLUP") || !split.contains(" WITH ROLLUP") || !single.contains(" GROUP BY ")) {
            return false;
        }
        if (!hasRollupNullSensitiveGroupKey(single)) {
            return false;
        }
        if (isRollupTopNNullableGroupKeyDiff(pair, singleRows, splitRows)) {
            return true;
        }
        if (!hasNullVsBooleanDiff(singleRows, splitRows) || !hasRollupNullBooleanCountShift(singleRows, splitRows)) {
            return false;
        }
        String splitExplain = explainText(pair.splitQuery).toUpperCase(Locale.ROOT);
        return explainContains(pair.singleQuery, "EXPANSION") && splitExplain.contains("EXPANSION")
                && (splitExplain.contains("SUBPLAN") || splitExplain.contains("SCALAR SUBQUERY")
                        || splitExplain.contains("DISTRIBUTED TABLE GET") || splitExplain.contains("JOIN"));
    }

    private boolean isRollupTopNNullableGroupKeyDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        String single = normalizeSqlForKnownBug(pair.singleQuery);
        if (!hasTopLevelClause(pair.singleQuery, "LIMIT") || !single.contains(" HAVING COUNT(*)")
                || !single.contains(" ORDER BY ")) {
            return false;
        }
        if (!explainContains(pair.singleQuery, "TOP-N SORT") || !explainContains(pair.splitQuery, "TOP-N SORT")
                || !explainContains(pair.singleQuery, "EXPANSION") || !explainContains(pair.splitQuery, "EXPANSION")) {
            return false;
        }
        String splitExplain = explainText(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!splitExplain.contains("SUBPLAN SCAN") && !splitExplain.contains("JOIN")) {
            return false;
        }
        List<String> onlySingle = expandedUnmatchedRows(singleRows, splitRows);
        List<String> onlySplit = expandedUnmatchedRows(splitRows, singleRows);
        if (onlySingle.size() != 1 || onlySplit.size() != 1) {
            return false;
        }
        List<String> singleValues = decodeCanonicalRow(onlySingle.get(0));
        List<String> splitValues = decodeCanonicalRow(onlySplit.get(0));
        return singleValues.size() == splitValues.size() && containsNullValue(singleValues)
                && !containsNullValue(splitValues);
    }

    private boolean containsNullValue(List<String> values) {
        for (String value : values) {
            if (value == null) {
                return true;
            }
        }
        return false;
    }

    private boolean hasRollupNullSensitiveGroupKey(String sql) {
        if (sql.contains(" IN ") || sql.contains(" NOT IN ") || sql.contains(" IS FALSE") || sql.contains(" IS TRUE")
                || sql.contains(" IS UNKNOWN")) {
            return true;
        }
        if (!sql.contains("EXISTS")) {
            return false;
        }
        return sql.contains("GREATEST(") || sql.contains("LEAST(") || sql.contains("CONCAT(") || sql.contains("CAST(");
    }

    private boolean hasRollupNullBooleanCountShift(QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        List<String> onlySingle = expandedUnmatchedRows(singleRows, splitRows);
        List<String> onlySplit = expandedUnmatchedRows(splitRows, singleRows);
        if (onlySingle.isEmpty() || onlySplit.isEmpty() || onlySingle.size() != onlySplit.size()
                || onlySingle.size() > MAX_DIFF_ROWS) {
            return false;
        }
        boolean[] matchedSplitRows = new boolean[onlySplit.size()];
        for (String singleRow : onlySingle) {
            List<String> singleValues = decodeCanonicalRow(singleRow);
            boolean matched = false;
            for (int i = 0; i < onlySplit.size(); i++) {
                if (matchedSplitRows[i]) {
                    continue;
                }
                List<String> splitValues = decodeCanonicalRow(onlySplit.get(i));
                if (rowsDifferInOneNullBooleanColumn(singleValues, splitValues)) {
                    matchedSplitRows[i] = true;
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

    private boolean hasNullVsBooleanDiff(QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        List<String> onlySingle = expandedUnmatchedRows(singleRows, splitRows);
        List<String> onlySplit = expandedUnmatchedRows(splitRows, singleRows);
        if (onlySingle.isEmpty() || onlySplit.isEmpty() || onlySingle.size() > MAX_DIFF_ROWS
                || onlySplit.size() > MAX_DIFF_ROWS) {
            return false;
        }
        for (String singleRow : onlySingle) {
            for (String splitRow : onlySplit) {
                if (rowsDifferOnlyByNullAndBoolean(decodeCanonicalRow(singleRow), decodeCanonicalRow(splitRow))) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean rowsDifferInOneNullBooleanColumn(List<String> left, List<String> right) {
        if (left.size() != right.size() || left.isEmpty()) {
            return false;
        }
        int nullBooleanDiffs = 0;
        for (int i = 0; i < left.size(); i++) {
            String leftValue = left.get(i);
            String rightValue = right.get(i);
            if (Objects.equals(leftValue, rightValue)) {
                continue;
            }
            if ((leftValue == null && isBooleanCanonicalValue(rightValue))
                    || (rightValue == null && isBooleanCanonicalValue(leftValue))) {
                nullBooleanDiffs++;
                continue;
            }
            return false;
        }
        return nullBooleanDiffs == 1;
    }

    private boolean rowsDifferInOneNullToValueColumn(List<String> left, List<String> right) {
        if (left.size() != right.size() || left.isEmpty()) {
            return false;
        }
        int nullToValueDiffs = 0;
        for (int i = 0; i < left.size(); i++) {
            String leftValue = left.get(i);
            String rightValue = right.get(i);
            if (Objects.equals(leftValue, rightValue)) {
                continue;
            }
            if (leftValue == null || rightValue == null) {
                nullToValueDiffs++;
                continue;
            }
            return false;
        }
        return nullToValueDiffs == 1;
    }

    private boolean rowsDifferOnlyByNullAndBoolean(List<String> left, List<String> right) {
        if (left.size() != right.size() || left.isEmpty()) {
            return false;
        }
        boolean sawNullBooleanDiff = false;
        for (int i = 0; i < left.size(); i++) {
            String leftValue = left.get(i);
            String rightValue = right.get(i);
            if (Objects.equals(leftValue, rightValue)) {
                continue;
            }
            if ((leftValue == null && isBooleanCanonicalValue(rightValue))
                    || (rightValue == null && isBooleanCanonicalValue(leftValue))) {
                sawNullBooleanDiff = true;
                continue;
            }
            return false;
        }
        return sawNullBooleanDiff;
    }

    private boolean isBooleanCanonicalValue(String value) {
        return "0".equals(value) || "1".equals(value);
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
        if (single.contains(" WITH ROLLUP")) {
            return false;
        }
        if (containsAggregateFunction(single)) {
            return false;
        }
        if (containsCountNullPushdownCandidate(single) || containsAvgNullPushdownCandidate(single)) {
            return false;
        }
        return hasRepresentativeRowDiffShape(singleRows, splitRows);
    }

    private boolean containsAggregateFunction(String sql) {
        return sql.contains("COUNT(") || sql.contains("SUM(") || sql.contains("AVG(") || sql.contains("MIN(")
                || sql.contains("MAX(") || sql.contains("STD(") || sql.contains("VAR_SAMP(")
                || sql.contains("GROUP_CONCAT(");
    }

    private boolean containsCountNullPushdownCandidate(String sql) {
        for (String argument : aggregateArguments(sql, "COUNT(")) {
            if (classifyCountNullArgument(argument) != null) {
                return true;
            }
        }
        return false;
    }

    private boolean containsAvgNullPushdownCandidate(String sql) {
        String argument = firstAggregateArgument(sql, "AVG(");
        return argument != null && classifyAvgNullArgument(argument) != null;
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

    private KnownVpBug classifyCountNullPushdownAggregationDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        String single = normalizeSqlForKnownBug(pair.singleQuery);
        String split = normalizeSqlForKnownBug(pair.splitQuery);
        if (!single.contains("COUNT(") || !split.contains("COUNT(")) {
            return null;
        }
        if (!hasAggregateColumnSingleGreaterThanSplit(singleRows, splitRows)) {
            return null;
        }
        if (!explainContains(pair.singleQuery, "pushdown_aggregation")) {
            return null;
        }
        for (String argument : aggregateArguments(single, "COUNT(")) {
            KnownVpBug knownBug = classifyCountNullArgument(argument);
            if (knownBug != null) {
                return knownBug;
            }
        }
        return null;
    }

    private KnownVpBug classifyAvgNullPushdownAggregationDiff(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        String single = normalizeSqlForKnownBug(pair.singleQuery);
        String split = normalizeSqlForKnownBug(pair.splitQuery);
        if (!single.contains("AVG(") || !split.contains("AVG(")) {
            return null;
        }
        String argument = firstAggregateArgument(single, "AVG(");
        if (argument == null || !argument.contains("NULL")) {
            return null;
        }
        if (!hasSingleNumericAggregateMismatch(singleRows, splitRows)) {
            return null;
        }
        if (!explainContains(pair.singleQuery, "pushdown_aggregation")
                || explainContains(pair.splitQuery, "pushdown_aggregation")) {
            return null;
        }
        return classifyAvgNullArgument(argument);
    }

    private KnownVpBug classifyCountNullArgument(String argument) {
        String structuralArgument = normalizeStructuralArgumentForKnownBug(argument);
        String flatArgument = normalizeFlatArgumentForKnownBug(argument);
        if (!flatArgument.contains("NULL")) {
            return null;
        }
        if (structuralArgument.contains("IF(") || structuralArgument.contains("CASE WHEN")) {
            return KnownVpBug.COUNT_NULL_IF_CASE_PUSHDOWN;
        }
        if (structuralArgument.contains("GREATEST(") || structuralArgument.contains("LEAST(")) {
            return KnownVpBug.COUNT_NULL_GREATEST_LEAST_PUSHDOWN;
        }
        if (containsChainedNullComparisonPattern(flatArgument)) {
            return KnownVpBug.COUNT_NULL_CHAINED_COMPARISON_PUSHDOWN;
        }
        if (flatArgument.contains(" NOT IN ") || flatArgument.contains(" IN ")) {
            return KnownVpBug.COUNT_NULL_IN_NOT_IN_PUSHDOWN;
        }
        if (flatArgument.contains(" XOR ") || flatArgument.contains(" && ") || flatArgument.contains(" || ")) {
            return KnownVpBug.COUNT_NULL_XOR_LOGICAL_PUSHDOWN;
        }
        if (containsNullComparisonPattern(flatArgument)) {
            return KnownVpBug.COUNT_NULL_COMPARISON_PUSHDOWN;
        }
        return null;
    }

    private KnownVpBug classifyAvgNullArgument(String argument) {
        String structuralArgument = normalizeStructuralArgumentForKnownBug(argument);
        String flatArgument = normalizeFlatArgumentForKnownBug(argument);
        if (!flatArgument.contains("NULL")) {
            return null;
        }
        if (structuralArgument.contains("GREATEST(") || structuralArgument.contains("LEAST(")) {
            return KnownVpBug.AVG_NULL_GREATEST_LEAST_COUNT_DENOMINATOR_PUSHDOWN;
        }
        if (flatArgument.contains(" NOT IN ") || flatArgument.contains(" IN ")) {
            return KnownVpBug.AVG_NULL_IN_NOT_IN_COUNT_DENOMINATOR_PUSHDOWN;
        }
        return null;
    }

    private boolean containsNullComparisonPattern(String argument) {
        if (argument.contains("= NULL") || argument.contains("NULL =") || argument.contains("!= NULL")
                || argument.contains("NULL !=") || argument.contains("<> NULL") || argument.contains("NULL <>")
                || argument.contains(" > NULL") || argument.contains("NULL >") || argument.contains(" < NULL")
                || argument.contains("NULL <") || argument.contains(">= NULL") || argument.contains("NULL >=")
                || argument.contains("<= NULL") || argument.contains("NULL <=")) {
            return true;
        }
        String nullableOperand = "(?:CAST\\s+)?[+\\-!]*\\s*NULL(?:\\s+AS\\s+[A-Z0-9_]+)?";
        String operator = "(?:=|!=|<>|>=|<=|>|<)";
        return argument.matches(".*" + operator + "\\s+" + nullableOperand + ".*")
                || argument.matches(".*(?:^|\\s)" + nullableOperand + "\\s*" + operator + ".*");
    }

    private boolean containsChainedNullComparisonPattern(String argument) {
        String nullableOperand = "(?:CAST\\s+)?[+\\-!]*\\s*NULL(?:\\s+AS\\s+[A-Z0-9_]+)?";
        String operator = "(?:=|!=|<>|>=|<=|>|<)";
        return argument.matches(".*" + nullableOperand + "\\s*" + operator + "\\s+\\S+\\s*" + operator + ".*")
                || argument.matches(".*\\S+\\s*" + operator + "\\s+" + nullableOperand + "\\s*" + operator + ".*");
    }

    private String normalizeFlatArgumentForKnownBug(String argument) {
        return argument.replace('(', ' ').replace(')', ' ').replaceAll("\\s+", " ").trim();
    }

    private String normalizeStructuralArgumentForKnownBug(String argument) {
        return argument.replaceAll("\\s+", " ").trim();
    }

    private String firstAggregateArgument(String sql, String aggregate) {
        List<String> arguments = aggregateArguments(sql, aggregate);
        return arguments.isEmpty() ? null : arguments.get(0);
    }

    private List<String> aggregateArguments(String sql, String aggregate) {
        List<String> arguments = new ArrayList<>();
        int searchFrom = 0;
        while (searchFrom < sql.length()) {
            int aggregateStart = sql.indexOf(aggregate, searchFrom);
            if (aggregateStart < 0) {
                break;
            }
            int argumentStart = aggregateStart + aggregate.length();
            int argumentEnd = findMatchingParenthesis(sql, argumentStart - 1);
            if (argumentEnd <= argumentStart) {
                searchFrom = argumentStart;
                continue;
            }
            arguments.add(sql.substring(argumentStart, argumentEnd));
            searchFrom = argumentEnd + 1;
        }
        return arguments;
    }

    private String normalizeSqlForKnownBug(String sql) {
        return sql.replace('`', ' ').replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
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

    private boolean hasAggregateColumnSingleGreaterThanSplit(QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        List<BigDecimal> single = extractSingleNumericRowAllowingNulls(singleRows);
        List<BigDecimal> split = extractSingleNumericRowAllowingNulls(splitRows);
        if (single == null || split == null || single.size() != split.size()) {
            return false;
        }
        for (int i = 0; i < single.size(); i++) {
            if (single.get(i) != null && split.get(i) != null && single.get(i).compareTo(split.get(i)) > 0) {
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
        List<BigDecimal> numericValues = extractSingleNumericRowAllowingNulls(rows);
        if (numericValues == null || numericValues.stream().anyMatch(Objects::isNull)) {
            return null;
        }
        return numericValues;
    }

    private List<BigDecimal> extractSingleNumericRowAllowingNulls(QueryResultMultiset rows) {
        if (rows.rows.size() != 1) {
            return null;
        }
        Map.Entry<String, Integer> entry = rows.rows.entrySet().iterator().next();
        if (entry.getValue() != 1) {
            return null;
        }
        List<String> values = decodeCanonicalRow(entry.getKey());
        if (values.isEmpty()) {
            return null;
        }
        List<BigDecimal> numericValues = new ArrayList<>();
        try {
            for (String value : values) {
                numericValues.add(value == null ? null : new BigDecimal(value));
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
        File dir = new File(Main.LOG_DIRECTORY, "oceanbase");
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

    private void logFlakyPxRollupCandidate(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows, String trace) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("[FLAKY PX ROLLUP EXPANSION CANDIDATE]").append(System.lineSeparator());
        sb.append("The mismatch did not repeat twice with the same result signature; skipped.")
                .append(System.lineSeparator()).append(System.lineSeparator());
        sb.append(formatBugReport(pair, singleRows, splitRows));
        sb.append("PX ROLLUP stability trace:").append(System.lineSeparator());
        sb.append(trace);
        File dir = new File(Main.LOG_DIRECTORY, "oceanbase");
        try {
            Files.createDirectories(dir.toPath());
            Path target = nextFlakyRollupLogPath(dir);
            Files.writeString(target, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private Path nextFlakyRollupLogPath(File dir) {
        Path target;
        do {
            target = new File(dir, "vp-flaky" + FLAKY_ROLLUP_LOG_ID.incrementAndGet() + ".log").toPath();
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
                        left.trailingSpaceComparableColumns.get(entry.getKey()), remaining, right.approximateColumns,
                        right.trailingSpaceComparableColumns);
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
            List<Boolean> leftTrailingSpaceComparableColumns, Map<String, Integer> candidates,
            Map<String, List<ApproximateKind>> candidateApproximateColumns,
            Map<String, List<Boolean>> candidateTrailingSpaceComparableColumns) {
        Integer exactCount = candidates.get(row);
        if (exactCount != null && exactCount > 0) {
            return row;
        }
        for (Map.Entry<String, Integer> candidate : candidates.entrySet()) {
            if (candidate.getValue() > 0
                    && rowsApproximatelyEqual(row, leftApproximateColumns, leftTrailingSpaceComparableColumns,
                            candidate.getKey(), candidateApproximateColumns.get(candidate.getKey()),
                            candidateTrailingSpaceComparableColumns.get(candidate.getKey()))) {
                return candidate.getKey();
            }
        }
        return null;
    }

    private boolean rowsApproximatelyEqual(String left, List<ApproximateKind> leftApproximateColumns,
            List<Boolean> leftTrailingSpaceComparableColumns, String right,
            List<ApproximateKind> rightApproximateColumns, List<Boolean> rightTrailingSpaceComparableColumns) {
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
            if (isTrailingSpaceEquivalent(l, r, leftTrailingSpaceComparableColumns, rightTrailingSpaceComparableColumns,
                    i)) {
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

    private boolean isTrailingSpaceEquivalent(String left, String right, List<Boolean> leftComparableColumns,
            List<Boolean> rightComparableColumns, int index) {
        return left != null && right != null && isTrailingSpaceComparable(leftComparableColumns, index)
                && isTrailingSpaceComparable(rightComparableColumns, index)
                && trimTrailingSpaces(left).equals(trimTrailingSpaces(right));
    }

    private boolean isTrailingSpaceComparable(List<Boolean> columns, int index) {
        return columns != null && index < columns.size() && columns.get(index);
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
        Map<String, List<Boolean>> trailingSpaceComparableColumns = new LinkedHashMap<>();
        List<String> orderedRows = new ArrayList<>();
        List<List<ApproximateKind>> orderedApproximateColumns = new ArrayList<>();
        List<List<Boolean>> orderedTrailingSpaceComparableColumns = new ArrayList<>();
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
                List<Boolean> rowTrailingSpaceComparableColumns = getTrailingSpaceComparableColumns(rs, columnCount);
                orderedRows.add(row);
                orderedApproximateColumns.add(rowApproximateColumns);
                orderedTrailingSpaceComparableColumns.add(rowTrailingSpaceComparableColumns);
                rows.put(row, rows.getOrDefault(row, 0) + 1);
                approximateColumns.putIfAbsent(row, rowApproximateColumns);
                trailingSpaceComparableColumns.putIfAbsent(row, rowTrailingSpaceComparableColumns);
            }
            completed = true;
        } catch (AssertionError e) {
            if (isSkippableQueryError(e, query)) {
                if (isRollupInternalError(e, query)) {
                    recordSkippedErrorKind("Internal error in WITH ROLLUP query");
                } else {
                    recordSkippedError(e);
                }
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
        return new QueryResultMultiset(rows, approximateColumns, trailingSpaceComparableColumns, orderedRows,
                orderedApproximateColumns, orderedTrailingSpaceComparableColumns);
    }

    private List<ApproximateKind> getApproximateColumns(SQLancerResultSet rs, int columnCount) throws SQLException {
        List<ApproximateKind> approximate = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            approximate.add(getApproximateColumnType(rs.getType(i)));
        }
        return approximate;
    }

    private List<Boolean> getTrailingSpaceComparableColumns(SQLancerResultSet rs, int columnCount) throws SQLException {
        List<Boolean> comparable = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            String type = rs.getType(i);
            String upper = type == null ? "" : type.toUpperCase(Locale.ROOT);
            comparable.add((currentSessionUsesPadSpaceSemantics || currentSourceHasPadSpaceVarchar)
                    && (upper.contains("CHAR") || upper.contains("TEXT") || upper.contains("STRING")));
        }
        return comparable;
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

    private boolean isSkippableQueryError(AssertionError e, String query) {
        String msg = getErrorText(e);
        if (msg.isEmpty()) {
            return false;
        }
        return isRollupInternalError(e, query) || msg.contains("doesn't yet support")
                || msg.contains("You have an error in your SQL syntax") || isUnsupportedWindowError(msg)
                || msg.contains("Incorrect DECIMAL value") || msg.contains("Truncated incorrect")
                || msg.contains("Data truncation") || msg.contains("Invalid JSON") || isUnsupportedLateralError(msg)
                || msg.contains("incompatible with sql_mode=only_full_group_by") || msg.contains("not in SELECT list")
                || msg.contains("Expression #") || msg.contains("Invalid use of group function")
                || isSkippableUnknownColumnError(msg) || isMissingFunctionError(msg)
                || isCantGroupOnGeneratedAliasError(msg) || msg.contains("Can't reopen table")
                || msg.contains("Illegal mix of collations") || msg.contains("Out of sort memory")
                || msg.contains("sort buffer size") || msg.contains("No memory or reach tenant memory limit")
                || msg.contains("Operand should contain") || msg.contains("Subquery returns more than 1 row")
                || msg.contains("Timeout") || msg.contains("not supported");
    }

    private boolean isRollupInternalError(Throwable throwable, String query) {
        if (query == null || !query.toUpperCase(Locale.ROOT).contains(" WITH ROLLUP")) {
            return false;
        }
        String msg = getErrorText(throwable);
        return msg.contains("Internal error");
    }

    private void recordSkippedError(Throwable throwable) {
        String errorKind = classifySkippedError(throwable);
        recordSkippedErrorKind(errorKind);
    }

    private void recordSkippedErrorKind(String errorKind) {
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
        return formatBugReport(pair, singleRows, splitRows, StoragePhase.DEFAULT);
    }

    private String formatBugReport(VpQueryPair pair, QueryResultMultiset singleRows, QueryResultMultiset splitRows,
            StoragePhase storagePhase) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("[VERTICAL PARTITIONING BUG CANDIDATE]").append(System.lineSeparator());
        sb.append("Mode: OCEANBASE_MYSQL_RANDOM_TABLE_REWRITE").append(System.lineSeparator());
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
        sb.append("OceanBase VP capabilities: ").append(capabilities == null ? "<unprobed>" : capabilities.summary())
                .append(System.lineSeparator());
        sb.append("VP pushdown mode: ").append(vpPushdownMode()).append(System.lineSeparator());
        sb.append("OB storage profile: ").append(vpOceanBaseStorageProfile()).append(System.lineSeparator());
        sb.append("OB storage phase: ").append(storagePhase).append(System.lineSeparator());
        sb.append("OB partition profile: ").append(vpOceanBasePartitionProfile()).append(System.lineSeparator());
        sb.append("OB partition count: ").append(vpOceanBasePartitionCount()).append(System.lineSeparator());
        sb.append("OB parallel degree: ").append(vpOceanBaseParallelDegree()).append(System.lineSeparator());
        sb.append("OB column store profile: ").append(vpOceanBaseColumnStoreProfile()).append(System.lineSeparator());
        sb.append("OB column store source hint: ").append(sourceColumnStoreHint(pair.schema))
                .append(System.lineSeparator());
        sb.append("OB column store split inner hint: ").append(columnTableHints("l", "r"))
                .append(System.lineSeparator());
        sb.append("OB SQL semantics profile: ").append(vpOceanBaseSqlSemanticsProfile()).append(System.lineSeparator());
        sb.append("OB SQL semantics selected: ").append(sessionSemantics.summary()).append(System.lineSeparator());
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
        sb.append("Pushdown aggregation labels: single=")
                .append(queryHasPushdownAggregation(pair.singleQuery) ? "present" : "absent").append(", split=")
                .append(queryHasPushdownAggregation(pair.splitQuery) ? "present" : "absent")
                .append(System.lineSeparator());
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

    private String formatStoragePhaseBugReport(VpQueryPair pair, String side, QueryResultMultiset memtableRows,
            QueryResultMultiset sstableRows) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("[OCEANBASE VP STORAGE PHASE BUG CANDIDATE]").append(System.lineSeparator());
        appendStoragePhaseReportHeader(sb, pair, side);
        sb.append("Query: ").append("source".equals(side) ? pair.singleQuery : pair.splitQuery)
                .append(System.lineSeparator());
        sb.append("Query plan after sstable phase:").append(System.lineSeparator());
        sb.append(formatExplain("source".equals(side) ? pair.singleQuery : pair.splitQuery));
        sb.append("Memtable result sample: ").append(sample(memtableRows.rows)).append(System.lineSeparator());
        sb.append("Sstable result sample: ").append(sample(sstableRows.rows)).append(System.lineSeparator());
        sb.append("Only in memtable: ").append(diff(memtableRows, sstableRows)).append(System.lineSeparator());
        sb.append("Only in sstable: ").append(diff(sstableRows, memtableRows)).append(System.lineSeparator());
        lastQueryString = "source".equals(side) ? pair.singleQuery : pair.splitQuery;
        return sb.toString();
    }

    private String formatStoragePhaseOrderedBugReport(VpQueryPair pair, String side, QueryResultMultiset memtableRows,
            QueryResultMultiset sstableRows) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("[OCEANBASE VP STORAGE PHASE ORDER BUG CANDIDATE]").append(System.lineSeparator());
        appendStoragePhaseReportHeader(sb, pair, side);
        sb.append("Query: ").append("source".equals(side) ? pair.singleQuery : pair.splitQuery)
                .append(System.lineSeparator());
        sb.append("Memtable ordered result sample: ").append(orderedSample(memtableRows))
                .append(System.lineSeparator());
        sb.append("Sstable ordered result sample: ").append(orderedSample(sstableRows)).append(System.lineSeparator());
        sb.append("First ordered mismatch: ").append(firstOrderedMismatch(memtableRows, sstableRows))
                .append(System.lineSeparator());
        lastQueryString = "source".equals(side) ? pair.singleQuery : pair.splitQuery;
        return sb.toString();
    }

    private void appendStoragePhaseReportHeader(StringBuilder sb, VpQueryPair pair, String side) throws SQLException {
        sb.append("Mode: OCEANBASE_MYSQL_VP_MEMTABLE_SSTABLE").append(System.lineSeparator());
        sb.append("Storage side: ").append(side).append(System.lineSeparator());
        sb.append("Database: ").append(state.getDatabaseName()).append(System.lineSeparator());
        sb.append("OceanBase version: ").append(getSingleString("SELECT VERSION()")).append(System.lineSeparator());
        sb.append("Strategy summary: split=").append(pair.schema.splitStrategy).append(", reconstruction=")
                .append(pair.reconstructionStrategy).append(", shape=").append(pair.schema.baseShape)
                .append(", prefix=").append(pair.schema.prefix).append(System.lineSeparator());
        sb.append("OB storage profile: ").append(vpOceanBaseStorageProfile()).append(System.lineSeparator());
        sb.append("OB compaction status: ").append(readMajorCompactionStatus()).append(System.lineSeparator());
        sb.append("VP pushdown mode: ").append(vpPushdownMode()).append(System.lineSeparator());
        sb.append("OB partition profile: ").append(vpOceanBasePartitionProfile()).append(System.lineSeparator());
        sb.append("OB parallel degree: ").append(vpOceanBaseParallelDegree()).append(System.lineSeparator());
        sb.append("OB column store profile: ").append(vpOceanBaseColumnStoreProfile()).append(System.lineSeparator());
        sb.append("OB column store source hint: ").append(sourceColumnStoreHint(pair.schema))
                .append(System.lineSeparator());
        sb.append("OB column store split inner hint: ").append(columnTableHints("l", "r"))
                .append(System.lineSeparator());
        sb.append("OB SQL semantics selected: ").append(sessionSemantics.summary()).append(System.lineSeparator());
        sb.append("Session vars: ").append(captureSessionVars()).append(System.lineSeparator());
        sb.append("Session SET statements:").append(System.lineSeparator());
        sb.append(captureSessionSetStatements());
        sb.append("Setup statements:").append(System.lineSeparator());
        for (String setup : pair.schema.setupStatements) {
            sb.append(setup).append(";").append(System.lineSeparator());
        }
    }

    private String formatOrderedBugReport(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) throws SQLException {
        return formatOrderedBugReport(pair, singleRows, splitRows, StoragePhase.DEFAULT);
    }

    private String formatOrderedBugReport(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows, StoragePhase storagePhase) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("[VERTICAL PARTITIONING ORDER BUG CANDIDATE]").append(System.lineSeparator());
        sb.append(formatBugReport(pair, singleRows, splitRows, storagePhase));
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
        return rowsApproximatelyEqual(leftRow, getOrderedApproximateColumns(left, leftIndex),
                getOrderedTrailingSpaceComparableColumns(left, leftIndex), rightRow,
                getOrderedApproximateColumns(right, rightIndex),
                getOrderedTrailingSpaceComparableColumns(right, rightIndex));
    }

    private List<ApproximateKind> getOrderedApproximateColumns(QueryResultMultiset rows, int index) {
        if (index >= rows.orderedApproximateColumns.size()) {
            return List.of();
        }
        return rows.orderedApproximateColumns.get(index);
    }

    private List<Boolean> getOrderedTrailingSpaceComparableColumns(QueryResultMultiset rows, int index) {
        if (index >= rows.orderedTrailingSpaceComparableColumns.size()) {
            return List.of();
        }
        return rows.orderedTrailingSpaceComparableColumns.get(index);
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
                String match = findApproximateRowMatch(row, left.approximateColumns.get(row),
                        left.trailingSpaceComparableColumns.get(row), remainingRight, right.approximateColumns,
                        right.trailingSpaceComparableColumns);
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
        return "sql_mode=" + getSingleString("SELECT @@SESSION.sql_mode") + ", time_zone="
                + getSingleString("SELECT @@SESSION.time_zone") + ", optimizer_switch="
                + getSingleString("SELECT @@SESSION.optimizer_switch") + ", ob_enable_aggregation_pushdown="
                + getSingleString("SELECT @@SESSION.ob_enable_aggregation_pushdown") + ", character_set_connection="
                + getSingleString("SELECT @@SESSION.character_set_connection") + ", collation_connection="
                + getSingleString("SELECT @@SESSION.collation_connection") + ", group_concat_max_len="
                + getSingleString("SELECT @@SESSION.group_concat_max_len");
    }

    private String captureSessionSetStatements() {
        StringBuilder sb = new StringBuilder();
        sb.append("SET SESSION sql_mode = '").append(escapeSingleQuoted(getSingleString("SELECT @@SESSION.sql_mode")))
                .append("';").append(System.lineSeparator());
        sb.append("SET SESSION time_zone = '").append(escapeSingleQuoted(getSingleString("SELECT @@SESSION.time_zone")))
                .append("';").append(System.lineSeparator());
        String characterSetConnection = getSingleString("SELECT @@SESSION.character_set_connection");
        String collationConnection = getSingleString("SELECT @@SESSION.collation_connection");
        if (!"<unavailable>".equals(characterSetConnection) && !"<unavailable>".equals(collationConnection)) {
            sb.append("SET NAMES ").append(characterSetConnection).append(" COLLATE ").append(collationConnection)
                    .append(";").append(System.lineSeparator());
        }
        sb.append("SET SESSION optimizer_switch = '")
                .append(escapeSingleQuoted(getSingleString("SELECT @@SESSION.optimizer_switch"))).append("';")
                .append(System.lineSeparator());
        String aggregationPushdown = getSingleString("SELECT @@SESSION.ob_enable_aggregation_pushdown");
        if (!"<unavailable>".equals(aggregationPushdown)) {
            sb.append("SET SESSION ob_enable_aggregation_pushdown = ").append(aggregationPushdown).append(";")
                    .append(System.lineSeparator());
        }
        sb.append("SET SESSION group_concat_max_len = ")
                .append(getSingleString("SELECT @@SESSION.group_concat_max_len")).append(";")
                .append(System.lineSeparator());
        return sb.toString();
    }

    private boolean queryHasPushdownAggregation(String query) {
        return explainContains(query, "pushdown_aggregation");
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

    private String escapeSingleQuoted(String value) {
        if (value == null || "<unavailable>".equals(value)) {
            return "";
        }
        return value.replace("'", "''");
    }

    private String getCreateTable(String tableName) throws SQLException {
        try (Statement s = state.getConnection().createStatement();
                ResultSet rs = s.executeQuery("SHOW CREATE TABLE " + quote(tableName))) {
            if (!rs.next()) {
                throw new SQLException("SHOW CREATE TABLE returned no row for " + tableName);
            }
            return rs.getString(2);
        }
    }

    private String getTableEngine(String tableName) {
        try (Statement s = state.getConnection().createStatement();
                ResultSet rs = s.executeQuery("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = '"
                        + state.getDatabaseName() + "' AND TABLE_NAME = '" + tableName + "'")) {
            if (rs.next()) {
                String engine = rs.getString(1);
                if (engine != null && !engine.isEmpty()) {
                    return engine;
                }
            }
        } catch (SQLException ignored) {
        }
        return "InnoDB";
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
            if (isSkippableQueryError(e, sql)) {
                if (isRollupInternalError(e, sql)) {
                    recordSkippedErrorKind("Internal error in WITH ROLLUP query");
                } else {
                    recordSkippedError(e);
                }
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

    private void executeOptional(String sql) {
        try {
            executeNoSchemaRefresh(sql);
        } catch (Exception | AssertionError ignored) {
        }
    }

    private String toSQLType(OceanBaseDataType type) {
        switch (type) {
        case INT:
            return "BIGINT";
        case VARCHAR:
            return "VARCHAR(500)";
        case FLOAT:
            return "FLOAT";
        case DOUBLE:
            return "DOUBLE";
        case DECIMAL:
            return "DECIMAL(65,30)";
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

    private long sourceRowCount(BaseTableInfo baseTable) {
        int maxSourceRows = state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows;
        return maxSourceRows > 0 ? Math.min(baseTable.rowCount, maxSourceRows) : baseTable.rowCount;
    }

    private static void recordSourceRowCountBucket(long rowCount) {
        String bucket;
        if (rowCount == 0) {
            bucket = "0";
        } else if (rowCount == 1) {
            bucket = "1";
        } else if (rowCount <= 4) {
            bucket = "2-4";
        } else if (rowCount <= 16) {
            bucket = "5-16";
        } else if (rowCount <= 64) {
            bucket = "17-64";
        } else if (rowCount <= 256) {
            bucket = "65-256";
        } else {
            bucket = "257+";
        }
        synchronized (STATS_LOCK) {
            SOURCE_ROW_COUNT_BUCKET_COUNTS.put(bucket, SOURCE_ROW_COUNT_BUCKET_COUNTS.getOrDefault(bucket, 0) + 1);
        }
    }

    private static void recordSelectedSemantics(String semanticsName) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            SELECTED_SEMANTICS_COUNTS.put(semanticsName, SELECTED_SEMANTICS_COUNTS.getOrDefault(semanticsName, 0) + 1);
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
        writeVpStatsSnapshot();
    }

    public static void logSchemaShapeStats(OceanBaseGlobalState state, String phase) throws Exception {
        registerVpStatsShutdownHook();
        int emptySingle = 0;
        int emptyMulti = 0;
        int nonEmptySingle = 0;
        int nonEmptyMulti = 0;
        state.updateSchema();
        for (OceanBaseTable table : state.getSchema().getDatabaseTables()) {
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

    private static String formatVpStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("-- [vp-stats] event=process-final dbms=oceanbase");
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
        sb.append("-- [vp-stats] source-row-count-buckets ")
                .append(formatBucketCountsWithPercent(SOURCE_ROW_COUNT_BUCKET_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] selected-semantics-counts ").append(formatStringCounts(SELECTED_SEMANTICS_COUNTS, 30));
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

    private static String formatBucketCountsWithPercent(Map<String, Integer> counts) {
        int total = counts.values().stream().mapToInt(Integer::intValue).sum();
        if (total == 0) {
            return "samples=0";
        }
        List<String> buckets = List.of("0", "1", "2-4", "5-16", "17-64", "65-256", "257+");
        return "samples=" + total + " " + buckets.stream().map(bucket -> {
            int count = counts.getOrDefault(bucket, 0);
            return bucket + "=" + count + " (" + String.format(Locale.ROOT, "%.1f%%", 100.0 * count / total) + ")";
        }).collect(Collectors.joining(", "));
    }

    private static void registerVpStatsShutdownHook() {
        if (SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(OceanBaseVerticalPartitioningOracle::writeVpStatsSnapshot));
        }
    }

    private static void writeVpStatsSnapshot() {
        synchronized (STATS_WRITE_LOCK) {
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
    }

    private String sanitizeIdentifier(String value) {
        return value.replaceAll("[^A-Za-z0-9_]", "_");
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
