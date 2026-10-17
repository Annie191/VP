package sqlancer.cockroachdb.oracle;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.ExecutionTimer;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBColumn;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBCompositeDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTable;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBConstant;
import sqlancer.cockroachdb.ast.CockroachDBAggregate;
import sqlancer.cockroachdb.ast.CockroachDBAggregate.CockroachDBAggregateFunction;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.ast.CockroachDBText;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;

public class CockroachDBVerticalPartitioningOracle implements TestOracle<CockroachDBGlobalState> {

    private static final AtomicLong ORACLE_RUN_ID = new AtomicLong();
    private static final String ROW_ID = "vp_rowid";
    private static final int MAX_DIFF_ROWS = 8;
    private static final int MAX_RESULT_SAMPLE = 12;
    private static final double FLOAT_ABSOLUTE_TOLERANCE = 1e-6;
    private static final double FLOAT_RELATIVE_TOLERANCE = 1e-5;
    private static final double DOUBLE_ABSOLUTE_TOLERANCE = 1e-12;
    private static final double DOUBLE_RELATIVE_TOLERANCE = 1e-9;
    private static final Object STATS_LOCK = new Object();
    private static final Object STATS_WRITE_LOCK = new Object();
    private static final Map<SplitStrategy, Integer> SPLIT_STRATEGY_COUNTS = new EnumMap<>(SplitStrategy.class);
    private static final Map<BaseShape, Integer> SELECTED_BASE_SHAPE_COUNTS = new EnumMap<>(BaseShape.class);
    private static final Map<ReconstructionStrategy, Integer> RECONSTRUCTION_STRATEGY_COUNTS = new EnumMap<>(
            ReconstructionStrategy.class);
    private static final Map<ReconstructionStrategy, EnumMap<PairStage, Integer>> PAIR_STAGE_COUNTS = new EnumMap<>(
            ReconstructionStrategy.class);
    private static final Map<KnownVpBug, Integer> KNOWN_BUG_COUNTS = new EnumMap<>(KnownVpBug.class);
    private static final Map<AdvancedFeature, Integer> ADVANCED_FEATURE_COUNTS = new EnumMap<>(AdvancedFeature.class);
    private static final Map<AdvancedFeature, FeatureCoverage> QUERY_FEATURE_COUNTS = new EnumMap<>(
            AdvancedFeature.class);
    private static final Map<String, Integer> SCHEMA_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SKIPPED_ERROR_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SKIPPED_STAGE_COUNTS = new LinkedHashMap<>();
    private static final Map<SkippedErrorCategory, Integer> SKIPPED_CATEGORY_COUNTS = new EnumMap<>(
            SkippedErrorCategory.class);
    private static final Map<String, Integer> SOURCE_ROW_COUNT_BUCKETS = new LinkedHashMap<>();
    private static final Map<String, Integer> PLAN_FINGERPRINT_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> OPTIMIZER_PROFILE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> READ_COMMITTED_PROBE_COUNTS = new LinkedHashMap<>();
    private static final Set<String> UNSUPPORTED_OPTIMIZER_PROFILES = new LinkedHashSet<>();
    private static final Map<String, Integer> PLAN_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> TIME_TRAVEL_PROFILE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> DISTRIBUTED_PROFILE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, List<SkippedErrorSample>> SKIPPED_ERROR_SAMPLES = new LinkedHashMap<>();
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean();
    private static final AtomicLong KNOWN_BUG_LOG_ID = new AtomicLong();
    private static final AtomicLong UNEXPECTED_ERROR_LOG_ID = new AtomicLong();
    private static final AtomicLong STATS_DIRTY_EVENTS = new AtomicLong();
    private static final File STATS_FILE = new File(new File(Main.LOG_DIRECTORY, "cockroachdb"), "vp-stats.log");
    private static final long STATS_FLUSH_INTERVAL = 64;
    private static final int MAX_SKIPPED_ERROR_COUNT_GROUPS = 200;
    private static final int MAX_SKIPPED_ERROR_SAMPLE_GROUPS = 50;
    private static final int MAX_SKIPPED_ERROR_SAMPLES_PER_GROUP = 3;
    private static final int MAX_PRIORITY_SKIPPED_ERROR_SAMPLES_PER_GROUP = 3;
    private static final int MAX_PLAN_FINGERPRINTS = 128;
    private static final int MAX_DISTRIBUTED_PLAN_SAMPLES = 64;
    private static final int TRANSIENT_CLUSTER_RETRIES = 3;
    private static final int RESOURCE_READ_RETRIES = 12;
    private static final List<Integer> ROW_RANGE_PRESSURE_TARGETS = List.of(256, 1024, 4096);
    private static final String DISTINCT_ORDER_BY_NOT_IN_SELECT_LIST_ERROR = "for SELECT DISTINCT, ORDER BY expressions must appear in select list";
    private static int completedOracleRuns;
    private static int completedQueryPairs;
    private static int completedAttempts;
    private static int schemaShapeSamples;
    private static int planFingerprintSamples;
    private static int distributedPlanSamples;

    private static final class SkippedErrorSample {
        private final String stage;
        private final String sql;

        private SkippedErrorSample(String stage, String sql) {
            this.stage = stage;
            this.sql = sql;
        }
    }

    private static final class FeatureCoverage {
        private int generated;
        private int compared;
    }

    private static final class PlanCapture {
        private final String fingerprint;
        private final Set<String> shapes;

        private PlanCapture(String fingerprint, Set<String> shapes) {
            this.fingerprint = fingerprint;
            this.shapes = shapes;
        }
    }

    private static final class DistributedRangeSnapshot {
        private final int rangeCount;
        private final Set<String> leaseholders;
        private final Set<String> replicas;

        private DistributedRangeSnapshot(int rangeCount, Set<String> leaseholders, Set<String> replicas) {
            this.rangeCount = rangeCount;
            this.leaseholders = leaseholders;
            this.replicas = replicas;
        }

        private boolean hasSamePlacementAs(DistributedRangeSnapshot other) {
            return rangeCount == other.rangeCount && leaseholders.equals(other.leaseholders)
                    && replicas.equals(other.replicas);
        }
    }

    private final CockroachDBGlobalState state;
    private int directedFamilySequence;
    private int directedQuerySequence;
    private final ExpectedErrors errors = new ExpectedErrors();

    private enum BaseShape {
        EMPTY, SINGLE_COLUMN, MULTI_COLUMN
    }

    private enum SplitStrategy {
        EMPTY_SPLIT, SINGLE_REPLICATED, SINGLE_RIGHT_ROW_ID_ONLY, SINGLE_LEFT_ROW_ID_ONLY, EVEN_SPLIT, OVERLAPPED_SPLIT,
        SKEWED_SPLIT, TYPE_DIRECTED_SPLIT
    }

    private enum ReconstructionStrategy {
        JOIN, LEFT_RIGHT_JOIN, FULL_OUTER_SIMULATION, OUTER_NULL_EXTENSION, SCALAR_SUBQUERY, EXISTS_FILTER, IN_FILTER,
        NOT_EXISTS_ANTI, DERIVED_TABLE, CTE, VIEW, TEMP_TABLE, AGGREGATE_THEN_MERGE, ROW_NUMBER_ALIGNMENT, JSON_PACK,
        STRING_PACK, NATURAL_OR_USING, STRAIGHT_JOIN, LATERAL_DERIVED, ZIGZAG_JOIN
    }

    private enum PairStage {
        GENERATED, EXTRA_SETUP_OK, SINGLE_OK, SPLIT_OK, COMPARED, MISMATCH
    }

    private enum SkippedErrorCategory {
        RESOURCE_LIMIT, CLUSTER_TRANSIENT, EXPECTED_QUERY_REJECTION, EXPECTED_GENERATION_OR_SETUP, SETUP_OR_CLEANUP,
        OBSERVABILITY, INTERNAL_DATABASE_ERROR, OTHER
    }

    private enum KnownVpBug {
        NONE,
        // https://github.com/cockroachdb/cockroach/issues/172870
        VECTORIZED_REPLACECOL_BIT_OR_VARBIT,
        // https://github.com/cockroachdb/cockroach/issues/173046
        INVERTED_INDEX_NULL_ARRAY_CONTAINMENT,
        // A LIMIT/OFFSET lookup join can incorrectly treat a right-side partial-index predicate as a filter.
        PARTIAL_INDEX_NULL_FILTER_WITH_LIMIT_OFFSET, VARIANCE_NUMERIC_PLAN_INSTABILITY, STDDEV_NUMERIC_PLAN_INSTABILITY,
        SQRDIFF_NUMERIC_PLAN_INSTABILITY,
        // A single projection may contain multiple unstable numeric aggregates.
        MIXED_NUMERIC_PLAN_INSTABILITY,
        // VARIANCE on one huge, repeated DECIMAL value can produce distinct negative results for equivalent plans.
        VARIANCE_REPEATED_LARGE_DECIMAL,
        // SQRDIFF has the same large repeated-DECIMAL instability, but is a separate aggregate implementation.
        SQRDIFF_REPEATED_LARGE_DECIMAL
    }

    public enum AdvancedFeature {
        ROLLUP, CORRELATED_SUBQUERY, GROUP_CONCAT, ASYMMETRIC_INDEX, CHARSET_COLLATION_COLUMN, BOUNDARY_MUTATION,
        UNION_ALL, SET_OPERATION, COLLATION_PREDICATE, JSONB_PREDICATE, ARRAY_PREDICATE, COMPUTED_COLUMN, PARTIAL_INDEX,
        HASH_SHARDED_INDEX, COVERING_INDEX, EXPRESSION_INDEX, INVERTED_INDEX, COLUMN_FAMILY, REAL_KEY_RECONSTRUCTION,
        ENUM
    }

    private enum KeyMode {
        VP_ROWID, PRIMARY_KEY, NULLABLE_UNIQUE
    }

    private enum OptimizerProfile {
        DEFAULT(List.of()),
        VECTORIZED_HISTOGRAMS(List.of("SET vectorize = 'on'", "SET optimizer_use_histograms = 'on'")),
        VECTORIZED_NO_HISTOGRAMS(List.of("SET vectorize = 'on'", "SET optimizer_use_histograms = 'off'")),
        ROW_EXECUTION(List.of("SET vectorize = 'off'")),
        READ_COMMITTED(List.of("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED")),
        ZIGZAG_ON(List.of("SET enable_zigzag_join = 'on'")), ZIGZAG_OFF(List.of("SET enable_zigzag_join = 'off'")),
        HISTOGRAMS_OFF(List.of("SET optimizer_use_histograms = 'off'")),
        DISTSQL_PLANNING_ON(List.of("SET distsql = 'on'")), DISTSQL_PLANNING_OFF(List.of("SET distsql = 'off'")),
        COST_MODEL_PAIRING(List.of("SET opt_split_scan_limit = 16"));

        private final List<String> statements;

        OptimizerProfile(List<String> statements) {
            this.statements = statements;
        }
    }

    private static final class VpColumn {
        private final String name;
        private final CockroachDBCompositeDataType type;
        private final String computedExpression;

        private VpColumn(String name, CockroachDBCompositeDataType type) {
            this(name, type, null);
        }

        private VpColumn(String name, CockroachDBCompositeDataType type, String computedExpression) {
            this.name = name;
            this.type = type;
            this.computedExpression = computedExpression;
        }
    }

    private static final class BaseTableInfo {
        private final String tableName;
        private final List<VpColumn> columns;
        private final BaseShape shape;
        private final String stableOrderStrategy;
        private final long rowCount;
        private final String engine;
        private final List<VpColumn> primaryKeyColumns;
        private final List<VpColumn> nullableKeyCandidates;

        private BaseTableInfo(String tableName, List<VpColumn> columns, BaseShape shape, String stableOrderStrategy,
                long rowCount, String engine, List<VpColumn> primaryKeyColumns, List<VpColumn> nullableKeyCandidates) {
            this.tableName = tableName;
            this.columns = columns;
            this.shape = shape;
            this.stableOrderStrategy = stableOrderStrategy;
            this.rowCount = rowCount;
            this.engine = engine;
            this.primaryKeyColumns = primaryKeyColumns;
            this.nullableKeyCandidates = nullableKeyCandidates;
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
        private final OptimizerProfile optimizerProfile;
        private KeyMode keyMode;
        private List<VpColumn> reconstructionKeyColumns;
        private final Map<String, String> columnCollationClauses = new LinkedHashMap<>();
        private String sourceEngine;
        private String leftEngine;
        private String rightEngine;
        private String activeViewName;
        private final Set<String> viewNames = new LinkedHashSet<>();
        private boolean tempTableMaterialized;
        private String timeTravelTimestamp;
        private String distributedGatewayNode;
        private final Set<String> distributedLeaseholders = new LinkedHashSet<>();
        private final Set<String> distributedReplicas = new LinkedHashSet<>();
        private long sourceRowCount;
        private final List<String> setupStatements = new ArrayList<>();

        private VpSchema(String prefix, String baseTable, String sourceTable, String leftTable, String rightTable,
                String viewName, String tempTable, List<VpColumn> columns, List<VpColumn> leftColumns,
                List<VpColumn> rightColumns, List<VpColumn> overlapColumns, BaseShape baseShape,
                SplitStrategy splitStrategy, boolean usesPhysicalForeignKey, String stableOrderStrategy,
                long baseRowCount, String baseEngine, OptimizerProfile optimizerProfile, KeyMode keyMode,
                List<VpColumn> reconstructionKeyColumns) {
            this.prefix = prefix;
            this.baseTable = baseTable;
            this.sourceTable = sourceTable;
            this.leftTable = leftTable;
            this.rightTable = rightTable;
            this.viewName = viewName;
            this.tempTable = tempTable;
            this.columns = new ArrayList<>(columns);
            this.leftColumns = leftColumns;
            this.rightColumns = rightColumns;
            this.overlapColumns = overlapColumns;
            this.baseShape = baseShape;
            this.splitStrategy = splitStrategy;
            this.usesPhysicalForeignKey = usesPhysicalForeignKey;
            this.stableOrderStrategy = stableOrderStrategy;
            this.baseRowCount = baseRowCount;
            this.baseEngine = baseEngine;
            this.optimizerProfile = optimizerProfile;
            this.keyMode = keyMode;
            this.reconstructionKeyColumns = reconstructionKeyColumns;
            this.sourceEngine = "N/A";
            this.leftEngine = "N/A";
            this.rightEngine = "N/A";
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
        private final Set<AdvancedFeature> queryFeatures;
        private final OptimizerProfile optimizerProfile;
        private final String timeTravelTimestamp;

        private VpQueryPair(VpSchema schema, ReconstructionStrategy reconstructionStrategy, String singleQuery,
                String splitQuery, List<String> extraSetupStatements, String description,
                boolean orderedComparisonEnabled, Set<AdvancedFeature> queryFeatures,
                OptimizerProfile optimizerProfile) {
            this.schema = schema;
            this.reconstructionStrategy = reconstructionStrategy;
            this.singleQuery = singleQuery;
            this.splitQuery = splitQuery;
            this.extraSetupStatements = extraSetupStatements;
            this.description = description;
            this.orderedComparisonEnabled = orderedComparisonEnabled;
            this.queryFeatures = queryFeatures.isEmpty() ? EnumSet.noneOf(AdvancedFeature.class)
                    : EnumSet.copyOf(queryFeatures);
            this.optimizerProfile = optimizerProfile;
            this.timeTravelTimestamp = schema.timeTravelTimestamp;
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

    private static final class NumericRowDifference {
        private final List<String> singleValues;
        private final List<String> splitValues;

        private NumericRowDifference(List<String> singleValues, List<String> splitValues) {
            this.singleValues = singleValues;
            this.splitValues = splitValues;
        }
    }

    private static final class NumericRowDifferences {
        private final List<NumericRowDifference> differences;

        private NumericRowDifferences(List<NumericRowDifference> differences) {
            this.differences = differences;
        }
    }

    private enum ApproximateKind {
        NONE(0, 0), DOUBLE(DOUBLE_ABSOLUTE_TOLERANCE, DOUBLE_RELATIVE_TOLERANCE),
        FLOAT(FLOAT_ABSOLUTE_TOLERANCE, FLOAT_RELATIVE_TOLERANCE);

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

    public CockroachDBVerticalPartitioningOracle(CockroachDBGlobalState state) {
        this.state = state;
        registerVpStatsShutdownHook();
        CockroachDBErrors.addExpressionErrors(errors);
        CockroachDBErrors.addTransactionErrors(errors);
        errors.add("unsupported comparison operator");
        errors.add("unsupported binary operator");
        errors.add("cannot be used in this context");
        errors.add("is not in select list");
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("cannot order by");
        errors.add("can't order by column type jsonb");
        errors.add("invalid cast");
        errors.add("integer out of range");
        errors.add("decimal out of range");
        errors.add("division by zero");
        errors.add("could not parse");
        errors.add("violates foreign key constraint");
        errors.add("violates unique constraint");
        errors.add("failed to satisfy CHECK constraint");
        errors.add("field position must not be zero");
    }

    @Override
    public void check() throws Exception {
        CockroachDBProvider.acquireVerticalPartitioningOracleRunPermit();
        try {
            checkWithOracleRunPermit();
        } finally {
            CockroachDBProvider.releaseVerticalPartitioningOracleRunPermit();
        }
    }

    private void checkWithOracleRunPermit() throws Exception {
        prepareDistributedProfileRun();
        resetSessionForVerticalPartitioningOracle();
        OptimizerProfile optimizerProfile = chooseOptimizerProfile();
        if (!applyOptimizerProfile(optimizerProfile)) {
            throw new IgnoreMeException();
        }
        recordOptimizerProfile(optimizerProfile);
        CockroachDBProvider.ensureVerticalPartitioningBaseTables(state, 2);
        BaseTableInfo baseTable = selectBaseTable();
        int queryVariantsPerCombination = Math.max(1,
                state.getDbmsSpecificOptions().verticalPartitioningQueryVariantsPerCombination);
        int checkedPairs = 0;
        int attempts = 0;
        List<SplitStrategy> splitSchedule = createSplitSchedule(baseTable.shape);
        for (SplitStrategy splitStrategy : splitSchedule) {
            VpSchema vp = null;
            boolean cleanupArtifacts = true;
            try {
                vp = materializeTable(baseTable, splitStrategy, optimizerProfile);
                if (optimizerProfile == OptimizerProfile.READ_COMMITTED) {
                    runReadCommittedConcurrencyProbe(vp);
                }
                if (distributedProfileEnabled()) {
                    prepareDistributedStorageProfile(vp);
                }
                incrementSplitStrategy(vp.splitStrategy);
                List<ReconstructionStrategy> reconstructionSchedule = createReconstructionSchedule();
                for (ReconstructionStrategy strategy : reconstructionSchedule) {
                    try {
                        // CockroachDB does not support historical reads under READ
                        // COMMITTED. Keep that profile as a normal isolation test.
                        if (timeTravelProfileEnabled() && optimizerProfile != OptimizerProfile.READ_COMMITTED
                                && strategy != ReconstructionStrategy.VIEW) {
                            prepareTimeTravelStrategy(vp, strategy);
                        } else if (timeTravelProfileEnabled() && strategy == ReconstructionStrategy.VIEW) {
                            // A variant view is created immediately before its query pair. It cannot
                            // participate in AOST because it did not exist at the captured timestamp.
                            recordTimeTravelProfileEvent("view-dynamic-skipped");
                        }
                        for (int i = 0; i < queryVariantsPerCombination; i++) {
                            attempts++;
                            try {
                                VpQueryPair pair = generateRandomRewritePair(vp, strategy);
                                incrementReconstructionStrategy(pair.reconstructionStrategy);
                                recordPairGenerated(pair);
                                comparePair(pair);
                                checkedPairs++;
                            } catch (IgnoreMeException ignored) {
                            }
                        }
                    } catch (IgnoreMeException ignored) {
                        // A reconstruction that cannot be read at a historical timestamp is a
                        // profile-specific capability skip; normal VP still exercises it.
                    } finally {
                        vp.timeTravelTimestamp = null;
                    }
                }
            } catch (IgnoreMeException ignored) {
                // Split-level capability skips, including distributed SPLIT/SCATTER skips,
                // should not leave VP artifacts behind.
            } catch (Exception | AssertionError e) {
                cleanupArtifacts = false;
                throw e;
            } finally {
                if (vp != null && cleanupArtifacts) {
                    cleanupIfNoBug(vp);
                }
            }
        }
        if (checkedPairs > 0) {
            recordCompletedVpRun(checkedPairs, attempts);
        } else {
            throw new IgnoreMeException();
        }
        CockroachDBProvider.replaceVerticalPartitioningTableAfterSuccessfulCheck(state, baseTable.tableName);
    }

    private BaseTableInfo selectBaseTable() throws Exception {
        refreshSchemaOrSkipTransient("base-selection");
        List<CockroachDBTable> candidates = state.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> !t.getName().startsWith("vp_")).filter(t -> !t.getColumns().isEmpty())
                .filter(t -> t.getColumns().stream().noneMatch(c -> c.getName().equalsIgnoreCase(ROW_ID)))
                .collect(Collectors.toList());
        if (candidates.isEmpty()) {
            throw new IgnoreMeException();
        }
        return createBaseTableInfo(Randomly.fromList(candidates));
    }

    private BaseTableInfo createBaseTableInfo(CockroachDBTable base) throws Exception {
        Map<String, String> computedExpressions = computedExpressions(base.getName());
        List<VpColumn> sourceColumns = base.getColumns().stream()
                .map(c -> new VpColumn(c.getName(), c.getType(), computedExpressions.get(c.getName())))
                .collect(Collectors.toList());
        if (sourceColumns.stream().anyMatch(c -> c.type.getPrimitiveDataType() == CockroachDBDataType.ENUM)) {
            recordAdvancedFeature(AdvancedFeature.ENUM);
        }
        long rowCount = getRowCount(base.getName());
        BaseShape shape = rowCount == 0 ? BaseShape.EMPTY
                : sourceColumns.size() == 1 ? BaseShape.SINGLE_COLUMN : BaseShape.MULTI_COLUMN;
        incrementSelectedBaseShape(shape);
        String stableOrder = getStableOrder(base, sourceColumns);
        List<VpColumn> primaryKeyColumns = base.getColumns().stream().filter(CockroachDBColumn::isPrimaryKey)
                .map(c -> findColumn(sourceColumns, c.getName())).filter(Objects::nonNull)
                .filter(c -> c.computedExpression == null && isStableReconstructionKeyType(c.type))
                .collect(Collectors.toList());
        List<VpColumn> nullableKeyCandidates = base.getColumns().stream().filter(CockroachDBColumn::isNullable)
                .map(c -> findColumn(sourceColumns, c.getName())).filter(Objects::nonNull)
                .filter(c -> c.computedExpression == null && isStableReconstructionKeyType(c.type))
                .collect(Collectors.toList());
        return new BaseTableInfo(base.getName(), sourceColumns, shape, stableOrder, rowCount, "N/A", primaryKeyColumns,
                nullableKeyCandidates);
    }

    private VpColumn findColumn(List<VpColumn> columns, String name) {
        return columns.stream().filter(c -> c.name.equals(name)).findFirst().orElse(null);
    }

    private Map<String, String> computedExpressions(String tableName) throws SQLException {
        Map<String, String> expressions = new LinkedHashMap<>();
        for (String element : extractTableElements(getCreateTable(tableName))) {
            String upper = element.toUpperCase(Locale.ROOT);
            int asStart = upper.indexOf(" AS (");
            if (asStart < 0 || (!upper.endsWith(") STORED") && !upper.endsWith(") VIRTUAL"))) {
                continue;
            }
            String columnPart = element.substring(0, asStart).trim();
            int nameEnd = columnPart.indexOf(' ');
            if (nameEnd <= 0) {
                continue;
            }
            String columnName = columnPart.substring(0, nameEnd).trim();
            if (columnName.startsWith("\"") && columnName.endsWith("\"")) {
                columnName = columnName.substring(1, columnName.length() - 1).replace("\"\"", "\"");
            }
            int expressionStart = asStart + " AS (".length();
            int expressionEnd = upper.endsWith(") STORED") ? element.length() - ") STORED".length()
                    : element.length() - ") VIRTUAL".length();
            expressions.put(columnName, element.substring(expressionStart, expressionEnd));
        }
        if (!expressions.isEmpty()) {
            recordAdvancedFeature(AdvancedFeature.COMPUTED_COLUMN);
        }
        return expressions;
    }

    private List<SplitStrategy> createSplitSchedule(BaseShape shape) {
        List<SplitStrategy> splitStrategies = new ArrayList<>(compatibleSplitStrategies(shape));
        shuffleWithSqlancerRandom(splitStrategies);
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
        shuffleWithSqlancerRandom(shuffled);
        return shuffled;
    }

    private static <T> void shuffleWithSqlancerRandom(List<T> values) {
        for (int i = values.size() - 1; i > 0; i--) {
            int swapIndex = (int) Randomly.getNotCachedInteger(0, i + 1);
            T value = values.get(i);
            values.set(i, values.get(swapIndex));
            values.set(swapIndex, value);
        }
    }

    private void resetSessionForVerticalPartitioningOracle() throws Exception {
        executeOptional("ROLLBACK");
        executeOptional("SET TIME ZONE 'UTC'");
        executeOptional("RESET statement_timeout");
        executeOptional("RESET optimizer_use_histograms");
        executeOptional("RESET enable_zigzag_join");
        executeOptional("RESET vectorize");
        executeOptional("RESET distsql");
        executeOptional("RESET opt_split_scan_limit");
    }

    private boolean timeTravelProfileEnabled() {
        return state.getDbmsSpecificOptions().verticalPartitioningTimeTravelProfile;
    }

    private boolean distributedProfileEnabled() {
        return state.getDbmsSpecificOptions().verticalPartitioningDistributedProfile;
    }

    private boolean rowRangePressureProfileEnabled() {
        return state.getDbmsSpecificOptions().verticalPartitioningRowRangePressureProfile;
    }

    private void prepareDistributedProfileRun() throws Exception {
        if (!distributedProfileEnabled()) {
            return;
        }
        switchToSampledDistributedGateway();
        int liveNodes = countLiveNodes();
        int minimum = Math.max(3, state.getDbmsSpecificOptions().verticalPartitioningDistributedMinLiveNodes);
        recordDistributedProfileEvent("live-nodes=" + liveNodes);
        recordDistributedProfileEvent("minimum-live-nodes=" + minimum);
        recordDistributedClusterShape();
        if (liveNodes < minimum) {
            recordDistributedProfileEvent("cluster-skipped-insufficient-live-nodes");
            throw new IgnoreMeException();
        }
        recordDistributedProfileEvent("cluster-ready");
    }

    private void switchToSampledDistributedGateway() throws Exception {
        List<String> configured = parseDistributedGateways();
        if (configured.isEmpty()) {
            recordDistributedProfileEvent("gateway-config=initial");
        } else {
            String endpoint = chooseLeastCoveredGateway(configured);
            int separator = endpoint.lastIndexOf(':');
            if (separator <= 0 || separator == endpoint.length() - 1) {
                throw new IgnoreMeException();
            }
            String host = endpoint.substring(0, separator);
            int port;
            try {
                port = Integer.parseInt(endpoint.substring(separator + 1));
            } catch (NumberFormatException ignored) {
                throw new IgnoreMeException();
            }
            try {
                CockroachDBProvider.switchVerticalPartitioningGateway(state, host, port);
            } catch (SQLException e) {
                if (CockroachDBProvider.VP_GATEWAY_CLUSTER_MISMATCH_SQL_STATE.equals(e.getSQLState())) {
                    recordSkippedError("distributed-gateway-cluster-mismatch", endpoint, getErrorText(e));
                    recordDistributedProfileEvent("gateway-cluster-mismatch");
                    throw new IgnoreMeException();
                }
                if (isTransientClusterAvailabilityError(e)) {
                    recordSkippedError("cluster-transient-distributed-gateway", endpoint, getErrorText(e));
                    recordDistributedProfileEvent("gateway-switch-skipped");
                    throw new IgnoreMeException();
                }
                throw e;
            }
            recordDistributedProfileEvent("gateway-endpoint=" + endpoint);
        }
        executeOptional("SET allow_unsafe_internals = true");
        String gatewayNode = currentGatewayNode();
        recordDistributedProfileEvent("gateway-node=" + gatewayNode);
    }

    private List<String> parseDistributedGateways() {
        String configured = state.getDbmsSpecificOptions().verticalPartitioningDistributedGateways;
        if (configured == null || configured.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> endpoints = new ArrayList<>();
        for (String endpoint : configured.split(",")) {
            String trimmed = endpoint.trim();
            if (!trimmed.isEmpty()) {
                endpoints.add(trimmed);
            }
        }
        return endpoints;
    }

    private String chooseLeastCoveredGateway(List<String> gateways) {
        int minimum = Integer.MAX_VALUE;
        List<String> leastCovered = new ArrayList<>();
        synchronized (STATS_LOCK) {
            for (String gateway : gateways) {
                int count = DISTRIBUTED_PROFILE_COUNTS.getOrDefault("gateway-endpoint=" + gateway, 0);
                if (count < minimum) {
                    minimum = count;
                    leastCovered.clear();
                }
                if (count == minimum) {
                    leastCovered.add(gateway);
                }
            }
        }
        return Randomly.fromList(leastCovered);
    }

    private int countLiveNodes() throws SQLException {
        int liveNodes = 0;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("SELECT is_live FROM crdb_internal.gossip_nodes")) {
            while (rs.next()) {
                if (getBooleanByLabelOrSuffix(rs, true, "is_live", "live")) {
                    liveNodes++;
                }
            }
        }
        return liveNodes;
    }

    private void recordDistributedClusterShape() {
        String sql = "SELECT node_id, sql_address, locality, is_live, ranges, leases FROM crdb_internal.gossip_nodes";
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            int rows = 0;
            Set<String> localities = new LinkedHashSet<>();
            Set<String> sqlAddresses = new LinkedHashSet<>();
            while (rs.next()) {
                rows++;
                String locality = getStringByLabelOrSuffix(rs, "locality", "locality_name");
                if (locality != null && !locality.isEmpty()) {
                    localities.add(locality);
                }
                String sqlAddress = getStringByLabelOrSuffix(rs, "sql_address", "address");
                if (sqlAddress != null && !sqlAddress.isEmpty()) {
                    sqlAddresses.add(sqlAddress);
                }
            }
            recordDistributedProfileEvent("node-rows=" + rows);
            recordDistributedProfileEvent("node-locality-count=" + localities.size());
            recordDistributedProfileEvent("node-sql-address-count=" + sqlAddresses.size());
            for (String locality : localities) {
                recordDistributedProfileEvent("node-locality=" + statsSafeToken(locality));
            }
        } catch (SQLException e) {
            recordSkippedError("distributed-topology", sql, getErrorText(e));
            recordDistributedProfileEvent("topology-skipped");
        }
        recordDistributedZoneConfigShape();
    }

    private void recordDistributedZoneConfigShape() {
        String sql = "SHOW ZONE CONFIGURATION FOR DATABASE " + quote(state.getDatabaseName());
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            StringBuilder config = new StringBuilder();
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int column = 1; column <= columns; column++) {
                    String value = rs.getString(column);
                    if (value != null) {
                        config.append(value).append(' ');
                    }
                }
            }
            String lower = config.toString().toLowerCase(Locale.ROOT);
            recordDistributedProfileEvent("zone-config-readable");
            recordDistributedProfileEvent(lower.contains("lease_preferences") || lower.contains("lease preferences")
                    ? "lease-preference=present" : "lease-preference=absent");
            recordDistributedProfileEvent(
                    lower.contains("constraints") ? "zone-constraints=present" : "zone-constraints=absent");
        } catch (SQLException e) {
            recordSkippedError("distributed-zone-config", sql, getErrorText(e));
            recordDistributedProfileEvent("zone-config-skipped");
        }
    }

    private String currentGatewayNode() throws Exception {
        String sql = "SELECT crdb_internal.node_id()";
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            if (!rs.next()) {
                throw new SQLException("crdb_internal.node_id() returned no row");
            }
            return rs.getString(1);
        } catch (SQLException e) {
            if (isTransientClusterAvailabilityError(e)) {
                recordSkippedError("cluster-transient-distributed-gateway-node", sql, getErrorText(e));
                recordDistributedProfileEvent("gateway-node-skipped");
                throw new IgnoreMeException();
            }
            throw e;
        }
    }

    private static void recordDistributedProfileEvent(String event) {
        synchronized (STATS_LOCK) {
            incrementString(DISTRIBUTED_PROFILE_COUNTS, event);
        }
        markVpStatsDirty();
    }

    private void prepareTimeTravelStrategy(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        List<String> setupStatements = new ArrayList<>();
        buildReconstructionRelation(vp, strategy, setupStatements);
        for (String setup : setupStatements) {
            if (!executeInternalStatement("time-travel-setup", setup, false)) {
                recordTimeTravelProfileEvent("setup-skipped");
                throw new IgnoreMeException();
            }
            vp.setupStatements.add(setup);
        }
        if (!setupStatements.isEmpty()) {
            switch (strategy) {
            case TEMP_TABLE:
                vp.tempTableMaterialized = true;
                break;
            default:
                break;
            }
        }
        vp.timeTravelTimestamp = captureTimeTravelTimestamp();
        SourceRelation relation = buildReconstructionRelation(vp, strategy, new ArrayList<>());
        String probe = "SELECT 1 FROM " + relation.withAlias("vp_time_travel_probe") + " LIMIT 0";
        for (int attempt = 0; attempt < RESOURCE_READ_RETRIES; attempt++) {
            try (Statement statement = state.getConnection().createStatement()) {
                statement.executeQuery(probe).close();
                recordTimeTravelProfileEvent("capability-ok");
                recordTimeTravelProfileEvent("capability-recovered-after-retry=" + attempt);
                break;
            } catch (SQLException e) {
                if (isTransientClusterAvailabilityError(e) && attempt + 1 < RESOURCE_READ_RETRIES) {
                    waitForTransientClusterRecovery(attempt);
                    continue;
                }
                recordSkippedError("time-travel-capability", probe, getErrorText(e));
                recordTimeTravelProfileEvent("capability-skipped-" + strategy.name());
                if (isTransientClusterAvailabilityError(e)) {
                    recordSkippedError("cluster-transient-time-travel-capability", probe, getErrorText(e));
                    throw new IgnoreMeException();
                }
                if (isTimeTravelCapabilityError(e)) {
                    throw new IgnoreMeException();
                }
                throw e;
            }
        }
        recordTimeTravelProfileEvent("snapshot-created");
    }

    private String captureTimeTravelTimestamp() throws Exception {
        String sql = "SELECT cluster_logical_timestamp()";
        for (int attempt = 0; attempt < RESOURCE_READ_RETRIES; attempt++) {
            try (Statement statement = state.getConnection().createStatement();
                    ResultSet rs = statement.executeQuery(sql)) {
                if (!rs.next()) {
                    throw new SQLException("cluster_logical_timestamp() returned no row");
                }
                recordTimeTravelProfileEvent("timestamp-recovered-after-retry=" + attempt);
                return rs.getString(1);
            } catch (SQLException e) {
                if (isTransientClusterAvailabilityError(e) && attempt + 1 < RESOURCE_READ_RETRIES) {
                    waitForTransientClusterRecovery(attempt);
                    continue;
                }
                if (isTransientClusterAvailabilityError(e)) {
                    recordSkippedError("cluster-transient-time-travel-timestamp", sql, getErrorText(e));
                    recordTimeTravelProfileEvent("timestamp-skipped");
                    throw new IgnoreMeException();
                }
                throw e;
            }
        }
        throw new AssertionError("CockroachDB VP timestamp retry loop exhausted");
    }

    private boolean isTimeTravelCapabilityError(SQLException error) {
        String message = getErrorText(error).toLowerCase(Locale.ROOT);
        return message.contains("as of system time") || message.contains("historical")
                || message.contains("temporary tables") || message.contains("relation \"");
    }

    private static void recordTimeTravelProfileEvent(String event) {
        synchronized (STATS_LOCK) {
            incrementString(TIME_TRAVEL_PROFILE_COUNTS, event);
        }
        markVpStatsDirty();
    }

    private OptimizerProfile chooseOptimizerProfile() {
        if (!advancedFeaturesEnabled()) {
            return OptimizerProfile.DEFAULT;
        }
        if (rowRangePressureProfileEnabled()) {
            return chooseRowRangePressureOptimizerProfile();
        }
        List<OptimizerProfile> leastCovered = new ArrayList<>();
        int min = Integer.MAX_VALUE;
        synchronized (STATS_LOCK) {
            for (OptimizerProfile profile : OptimizerProfile.values()) {
                if (UNSUPPORTED_OPTIMIZER_PROFILES.contains(profile.name())) {
                    continue;
                }
                int count = OPTIMIZER_PROFILE_COUNTS.getOrDefault(profile.name(), 0);
                if (count < min) {
                    leastCovered.clear();
                    min = count;
                }
                if (count == min) {
                    leastCovered.add(profile);
                }
            }
        }
        return leastCovered.isEmpty() ? OptimizerProfile.DEFAULT : Randomly.fromList(leastCovered);
    }

    private OptimizerProfile chooseRowRangePressureOptimizerProfile() {
        List<OptimizerProfile> pressureProfiles = List.of(OptimizerProfile.DISTSQL_PLANNING_ON,
                OptimizerProfile.DISTSQL_PLANNING_OFF, OptimizerProfile.COST_MODEL_PAIRING, OptimizerProfile.DEFAULT);
        List<OptimizerProfile> leastCovered = new ArrayList<>();
        int min = Integer.MAX_VALUE;
        synchronized (STATS_LOCK) {
            for (OptimizerProfile profile : pressureProfiles) {
                if (UNSUPPORTED_OPTIMIZER_PROFILES.contains(profile.name())) {
                    continue;
                }
                int count = OPTIMIZER_PROFILE_COUNTS.getOrDefault(profile.name(), 0);
                count += DISTRIBUTED_PROFILE_COUNTS
                        .getOrDefault("distsql-plan-diff-profile=" + profile.name() + ":local:local", 0);
                if (count < min) {
                    leastCovered.clear();
                    min = count;
                }
                if (count == min) {
                    leastCovered.add(profile);
                }
            }
        }
        recordDistributedProfileEvent("row-pressure-optimizer-bias");
        return leastCovered.isEmpty() ? OptimizerProfile.DEFAULT : Randomly.fromList(leastCovered);
    }

    private boolean applyOptimizerProfile(OptimizerProfile profile) {
        boolean supported = true;
        for (String statement : profile.statements) {
            supported &= executeOptional(statement);
        }
        if (!supported) {
            synchronized (STATS_LOCK) {
                UNSUPPORTED_OPTIMIZER_PROFILES.add(profile.name());
            }
            recordSkippedError("optimizer-profile", String.join("; ", profile.statements),
                    "unsupported optimizer profile setting");
        }
        return supported;
    }

    private static void recordOptimizerProfile(OptimizerProfile profile) {
        synchronized (STATS_LOCK) {
            incrementString(OPTIMIZER_PROFILE_COUNTS, profile.name());
        }
        markVpStatsDirty();
    }

    private VpSchema materializeTable(BaseTableInfo base, SplitStrategy splitStrategy,
            OptimizerProfile optimizerProfile) throws Exception {
        String prefix = "vp_" + sanitizeIdentifier(state.getDatabaseName()) + "_" + ORACLE_RUN_ID.incrementAndGet()
                + "_";
        String source = prefix + "source";
        String left = prefix + "l";
        String right = prefix + "r";
        String view = prefix + "v";
        String temp = prefix + "joined";

        SplitPlan splitPlan = createSplitPlan(base.columns, base.shape, splitStrategy);
        KeyMode keyMode = chooseKeyMode(base);
        List<VpColumn> reconstructionKeyColumns = initialReconstructionKeyColumns(base, keyMode);
        if (keyMode != KeyMode.VP_ROWID) {
            addReconstructionKeyOverlap(splitPlan, reconstructionKeyColumns);
            recordAdvancedFeature(AdvancedFeature.REAL_KEY_RECONSTRUCTION);
        }
        boolean useForeignKey = Randomly.getBoolean();

        VpSchema vp = new VpSchema(prefix, base.tableName, source, left, right, view, temp, base.columns,
                splitPlan.leftColumns, splitPlan.rightColumns, splitPlan.overlapColumns, base.shape, splitStrategy,
                useForeignKey, base.stableOrderStrategy, base.rowCount, base.engine, optimizerProfile, keyMode,
                reconstructionKeyColumns);
        configureVpComputedColumn(vp);
        initializeColumnCollations(vp);
        dropArtifacts(vp);
        createVpTablesAtomically(vp, helperEngineCandidate(base));
        populateSourceTable(vp, base.stableOrderStrategy);
        vp.sourceRowCount = getRowCount(vp.sourceTable);
        maybeAmplifySourceRows(vp);
        vp.sourceRowCount = getRowCount(vp.sourceTable);
        validateOrFallbackReconstructionKey(vp);
        recordSourceRowCount(vp.sourceRowCount);
        copyBaseSchemaObjectsToSource(vp);
        populatePartitionTables(vp);
        runControlledFragmentOracle(vp);
        maybeCreateHelperIndexes(vp);
        if (useForeignKey) {
            tryExecuteSetup(vp,
                    "ALTER TABLE " + quote(vp.rightTable) + " ADD CONSTRAINT " + quote(vp.prefix + "fk_r_l")
                            + " FOREIGN KEY (" + quote(ROW_ID) + ") REFERENCES " + quote(vp.leftTable) + "("
                            + quote(ROW_ID) + ")");
        }
        return vp;
    }

    private KeyMode chooseKeyMode(BaseTableInfo base) {
        if (rowRangePressureProfileEnabled()) {
            return KeyMode.VP_ROWID;
        }
        if (!advancedFeaturesEnabled() || Randomly.getBoolean()) {
            return KeyMode.VP_ROWID;
        }
        List<KeyMode> candidates = new ArrayList<>();
        if (!base.primaryKeyColumns.isEmpty()) {
            candidates.add(KeyMode.PRIMARY_KEY);
        }
        if (!base.nullableKeyCandidates.isEmpty()) {
            candidates.add(KeyMode.NULLABLE_UNIQUE);
        }
        return candidates.isEmpty() ? KeyMode.VP_ROWID : Randomly.fromList(candidates);
    }

    private List<VpColumn> initialReconstructionKeyColumns(BaseTableInfo base, KeyMode keyMode) {
        switch (keyMode) {
        case PRIMARY_KEY:
            return new ArrayList<>(base.primaryKeyColumns);
        case NULLABLE_UNIQUE:
            return List.of(Randomly.fromList(base.nullableKeyCandidates));
        case VP_ROWID:
            return List.of();
        default:
            throw new AssertionError(keyMode);
        }
    }

    private void validateOrFallbackReconstructionKey(VpSchema vp) throws Exception {
        if (vp.keyMode != KeyMode.NULLABLE_UNIQUE) {
            return;
        }
        if (vp.reconstructionKeyColumns.size() != 1
                || !sourceColumnIsNullEqualUnique(vp, vp.reconstructionKeyColumns.get(0))) {
            vp.keyMode = KeyMode.VP_ROWID;
            vp.reconstructionKeyColumns = List.of();
        }
    }

    private boolean sourceColumnIsNullEqualUnique(VpSchema vp, VpColumn column) throws Exception {
        String columnName = quote(column.name);
        String sql = "SELECT COUNT(*) FROM (SELECT " + columnName + " FROM " + quote(vp.sourceTable) + " GROUP BY "
                + columnName + " HAVING COUNT(*) > 1) duplicates";
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            if (!rs.next()) {
                return false;
            }
            return rs.getLong(1) == 0;
        }
    }

    private void addReconstructionKeyOverlap(SplitPlan splitPlan, List<VpColumn> keyColumns) {
        for (VpColumn column : keyColumns) {
            if (!containsColumn(splitPlan.leftColumns, column.name)) {
                splitPlan.leftColumns.add(column);
            }
            if (!containsColumn(splitPlan.rightColumns, column.name)) {
                splitPlan.rightColumns.add(column);
            }
            if (!containsColumn(splitPlan.overlapColumns, column.name)) {
                splitPlan.overlapColumns.add(column);
            }
        }
    }

    /**
     * Exercises missing, duplicate, and NULL-extension semantics independently of the normal primary-key-backed
     * fragments, which are intentionally complete copies of the source.
     */
    private void runControlledFragmentOracle(VpSchema vp) throws Exception {
        if (vp.sourceRowCount < 2) {
            return;
        }
        String left = vp.prefix + "controlled_l";
        String right = vp.prefix + "controlled_r";
        try (Statement s = state.getConnection().createStatement()) {
            // Use regular uniquely named tables rather than temporary tables. CockroachDB can
            // commit the CREATE TEMP TABLE DDL on a different session during schema changes,
            // making the immediately following INSERT report the table as missing.
            s.execute("CREATE TABLE " + quote(left) + " AS SELECT * FROM " + quote(vp.leftTable) + " LIMIT 0");
            s.execute("CREATE TABLE " + quote(right) + " AS SELECT * FROM " + quote(vp.rightTable) + " LIMIT 0");
            s.execute("INSERT INTO " + quote(left) + " SELECT * FROM " + quote(vp.leftTable));
            s.execute("INSERT INTO " + quote(right) + " SELECT * FROM " + quote(vp.rightTable));
            long missing = scalarLong(s, "SELECT MIN(" + quote(ROW_ID) + ") FROM " + quote(right));
            long duplicate = scalarLong(s, "SELECT MAX(" + quote(ROW_ID) + ") FROM " + quote(right));
            s.executeUpdate("DELETE FROM " + quote(right) + " WHERE " + quote(ROW_ID) + " = " + missing);
            s.executeUpdate("INSERT INTO " + quote(right) + " SELECT * FROM " + quote(right) + " WHERE " + quote(ROW_ID)
                    + " = " + duplicate);
            long leftCount = scalarLong(s, "SELECT COUNT(*) FROM " + quote(left));
            long fullCount = scalarLong(s, "SELECT COUNT(*) FROM " + quote(left) + " l FULL OUTER JOIN " + quote(right)
                    + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID));
            long nullExtensions = scalarLong(s, "SELECT COUNT(*) FROM " + quote(left) + " l LEFT JOIN " + quote(right)
                    + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID) + " WHERE r." + quote(ROW_ID) + " IS NULL");
            if (fullCount != leftCount + 1 || nullExtensions != 1) {
                throw new AssertionError("Controlled VP fragment semantics failed: expected full=" + (leftCount + 1)
                        + ", nullExtensions=1, got full=" + fullCount + ", nullExtensions=" + nullExtensions);
            }
        } finally {
            try (Statement s = state.getConnection().createStatement()) {
                s.execute("DROP TABLE IF EXISTS " + quote(left));
                s.execute("DROP TABLE IF EXISTS " + quote(right));
            }
        }
    }

    private long scalarLong(Statement statement, String sql) throws SQLException {
        try (ResultSet rs = statement.executeQuery(sql)) {
            if (!rs.next()) {
                throw new AssertionError("Scalar query returned no row: " + sql);
            }
            return rs.getLong(1);
        }
    }

    private void runReadCommittedConcurrencyProbe(VpSchema vp) throws Exception {
        String host = state.getOptions().getHost();
        int port = state.getOptions().getPort();
        String database = state.getDatabaseName();
        String url = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
        Connection reader = null;
        Connection writer = null;
        long insertedRowId = Long.MIN_VALUE;
        boolean writerCommitted = false;
        try {
            reader = DriverManager.getConnection(url, state.getOptions().getUserName(),
                    state.getOptions().getPassword());
            writer = DriverManager.getConnection(url, state.getOptions().getUserName(),
                    state.getOptions().getPassword());
            reader.setAutoCommit(false);
            writer.setAutoCommit(false);
            executeConnectionStatement(reader, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
            long before = connectionCount(reader, vp.sourceTable);
            insertedRowId = nextProbeRowId(writer, vp.sourceTable);
            try (Statement statement = writer.createStatement()) {
                statement.executeUpdate("INSERT INTO " + quote(vp.sourceTable) + " (" + quote(ROW_ID) + ") VALUES ("
                        + insertedRowId + ")");
            }
            long uncommitted = connectionCount(reader, vp.sourceTable);
            writer.commit();
            writerCommitted = true;
            long committed = connectionCount(reader, vp.sourceTable);
            if (uncommitted != before || committed != before + 1) {
                throw new AssertionError("READ COMMITTED visibility probe failed: before=" + before + ", uncommitted="
                        + uncommitted + ", committed=" + committed);
            }
            deleteProbeRow(writer, vp.sourceTable, insertedRowId);
            insertedRowId = Long.MIN_VALUE;
            recordReadCommittedProbe("passed");
        } catch (AssertionError e) {
            recordReadCommittedProbe("mismatch");
            throw e;
        } catch (SQLException e) {
            recordReadCommittedProbe("skipped");
            recordSkippedError("read-committed-probe", vp.sourceTable, getErrorText(e));
        } finally {
            if (writer != null) {
                try {
                    if (!writerCommitted) {
                        writer.rollback();
                    } else if (insertedRowId != Long.MIN_VALUE) {
                        // The visibility assertion can fail after commit. Retry cleanup in a
                        // fresh transaction so a failed probe never leaves a permanent row.
                        deleteProbeRow(writer, vp.sourceTable, insertedRowId);
                    }
                } catch (SQLException ignored) {
                }
            }
            closeQuietly(reader);
            closeQuietly(writer);
        }
    }

    private long connectionCount(Connection connection, String tableName) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM " + quote(tableName))) {
            result.next();
            return result.getLong(1);
        }
    }

    private long nextProbeRowId(Connection connection, String tableName) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet result = statement
                .executeQuery("SELECT COALESCE(MAX(" + quote(ROW_ID) + "), 0) + 1 FROM " + quote(tableName))) {
            result.next();
            return result.getLong(1);
        }
    }

    private void deleteProbeRow(Connection connection, String tableName, long rowId) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM " + quote(tableName) + " WHERE " + quote(ROW_ID) + " = " + rowId);
            connection.commit();
        }
    }

    private void executeConnectionStatement(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
            }
        }
    }

    private static void recordReadCommittedProbe(String event) {
        synchronized (STATS_LOCK) {
            incrementString(READ_COMMITTED_PROBE_COUNTS, event);
        }
        markVpStatsDirty();
    }

    private void prepareDistributedStorageProfile(VpSchema vp) throws Exception {
        vp.distributedGatewayNode = currentGatewayNode();
        recordDistributedProfileEvent("pair-gateway-node=" + vp.distributedGatewayNode);
        List<String> relations = List.of(vp.sourceTable, vp.leftTable, vp.rightTable);
        for (String relation : relations) {
            if (!splitAndScatterRelation(vp, relation)) {
                recordDistributedProfileEvent("storage-skipped");
                throw new IgnoreMeException();
            }
        }
        recordDistributedProfileEvent("storage-ready");
    }

    private boolean splitAndScatterRelation(VpSchema vp, String relation) throws Exception {
        List<Long> splitPoints = distributedSplitPoints(vp.sourceRowCount);
        if (splitPoints.isEmpty()) {
            recordDistributedProfileEvent("range-split-skipped-small-or-empty-source");
            return true;
        }
        String values = splitPoints.stream().map(value -> "(" + value + ")").collect(Collectors.joining(", "));
        String split = "ALTER TABLE " + quote(relation) + " SPLIT AT VALUES " + values;
        if (!executeInternalStatement("distributed-split", split, true)) {
            recordDistributedProfileEvent("range-split-skipped");
            return false;
        }
        String scatter = "ALTER TABLE " + quote(relation) + " SCATTER";
        if (!executeInternalStatement("distributed-scatter", scatter, false)) {
            recordDistributedProfileEvent("range-scatter-skipped");
            return false;
        }
        DistributedRangeSnapshot snapshot = waitForStableRanges(relation);
        vp.distributedLeaseholders.addAll(snapshot.leaseholders);
        vp.distributedReplicas.addAll(snapshot.replicas);
        String relationRole = distributedRelationRole(vp, relation);
        recordDistributedProfileEvent("relation-ranges=" + relationRole + ":" + snapshot.rangeCount);
        recordDistributedProfileEvent("relation-replica-coverage=" + relationRole + ":" + snapshot.replicas.size());
        recordDistributedProfileEvent(
                "relation-leaseholder-coverage=" + relationRole + ":" + snapshot.leaseholders.size());
        recordDistributedProfileEvent("ranges=" + snapshot.rangeCount);
        recordDistributedProfileEvent("replica-node-coverage=" + snapshot.replicas.size());
        recordDistributedProfileEvent("leaseholder-node-coverage=" + snapshot.leaseholders.size());
        for (String node : snapshot.leaseholders) {
            recordDistributedProfileEvent("leaseholder-node=" + node);
            recordDistributedProfileEvent("gateway-leaseholder=" + vp.distributedGatewayNode + ":" + node);
        }
        for (String node : snapshot.replicas) {
            recordDistributedProfileEvent("replica-node=" + node);
            recordDistributedProfileEvent("gateway-replica=" + vp.distributedGatewayNode + ":" + node);
        }
        return true;
    }

    private static String distributedRelationRole(VpSchema vp, String relation) {
        if (relation.equals(vp.sourceTable)) {
            return "source";
        }
        if (relation.equals(vp.leftTable)) {
            return "left";
        }
        if (relation.equals(vp.rightTable)) {
            return "right";
        }
        return "unknown";
    }

    private List<Long> distributedSplitPoints(long rowCount) {
        if (rowCount < 3) {
            return Collections.emptyList();
        }
        if (rowRangePressureProfileEnabled()) {
            return rowRangePressureSplitPoints(rowCount);
        }
        Set<Long> points = new LinkedHashSet<>();
        points.add(Math.max(2L, rowCount / 4));
        points.add(Math.max(2L, rowCount / 2));
        points.add(Math.max(2L, (rowCount * 3) / 4));
        points.removeIf(point -> point >= rowCount);
        return new ArrayList<>(points);
    }

    private List<Long> rowRangePressureSplitPoints(long rowCount) {
        int desiredRanges;
        if (rowCount >= 4096) {
            desiredRanges = 32;
        } else if (rowCount >= 1024) {
            desiredRanges = 16;
        } else {
            desiredRanges = 8;
        }
        int splitCount = (int) Math.min(desiredRanges - 1L, Math.max(1L, rowCount - 1L));
        Set<Long> points = new LinkedHashSet<>();
        for (int i = 1; i <= splitCount; i++) {
            long point = Math.max(2L, (rowCount * i) / desiredRanges);
            if (point < rowCount) {
                points.add(point);
            }
        }
        return new ArrayList<>(points);
    }

    private DistributedRangeSnapshot waitForStableRanges(String relation) throws Exception {
        DistributedRangeSnapshot previous = null;
        for (int attempt = 0; attempt < 8; attempt++) {
            DistributedRangeSnapshot current;
            try {
                current = readRanges(relation);
            } catch (SQLException e) {
                if (!isTransientClusterAvailabilityError(e)) {
                    throw e;
                }
                recordSkippedError("distributed-range", "SHOW RANGES FROM TABLE " + quote(relation), getErrorText(e));
                waitForTransientClusterRecovery(attempt);
                continue;
            }
            if (previous != null && current.hasSamePlacementAs(previous)) {
                recordDistributedProfileEvent("ranges-stable");
                return current;
            }
            previous = current;
            Thread.sleep(250L);
        }
        recordDistributedProfileEvent("ranges-not-stable-before-timeout");
        if (previous == null) {
            recordDistributedProfileEvent("ranges-unavailable-after-retries");
            throw new IgnoreMeException();
        }
        return previous;
    }

    private DistributedRangeSnapshot readRanges(String relation) throws SQLException {
        int rangeCount = 0;
        Set<String> leaseholders = new LinkedHashSet<>();
        Set<String> replicas = new LinkedHashSet<>();
        Set<String> rangeIds = new LinkedHashSet<>();
        String sql = "SHOW RANGES FROM TABLE " + quote(relation);
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                rangeCount++;
                addNodeIds(rangeIds, getStringByLabelOrSuffix(rs, "range_id"));
                addNodeIds(leaseholders, getStringByLabelOrSuffix(rs, "lease_holder", "leaseholder"));
                addNodeIds(replicas, getStringByLabelOrSuffix(rs, "replicas", "voting_replicas", "non_voting_replicas",
                        "replica_localities"));
            }
        }
        if (!rangeIds.isEmpty()) {
            readInternalRangePlacement(rangeIds, leaseholders, replicas);
        }
        return new DistributedRangeSnapshot(rangeCount, leaseholders, replicas);
    }

    private void readInternalRangePlacement(Set<String> rangeIds, Set<String> leaseholders, Set<String> replicas)
            throws SQLException {
        String ids = rangeIds.stream().filter(id -> id.matches("\\d+")).collect(Collectors.joining(", "));
        if (ids.isEmpty()) {
            return;
        }
        String sql = "SELECT lease_holder, replicas, voting_replicas, non_voting_replicas FROM crdb_internal.ranges "
                + "WHERE range_id IN (" + ids + ")";
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                addNodeIds(leaseholders, getStringByLabelOrSuffix(rs, "lease_holder"));
                addNodeIds(replicas,
                        getStringByLabelOrSuffix(rs, "replicas", "voting_replicas", "non_voting_replicas"));
            }
        } catch (SQLException e) {
            recordSkippedError("distributed-ranges-internal", sql, getErrorText(e));
            recordDistributedProfileEvent("internal-range-placement-skipped");
        }
    }

    private static String getStringByLabelOrSuffix(ResultSet rs, String... labels) throws SQLException {
        for (String label : labels) {
            try {
                return rs.getString(label);
            } catch (SQLException ignored) {
            }
        }
        int columns = rs.getMetaData().getColumnCount();
        for (String label : labels) {
            String normalizedLabel = label.toLowerCase(Locale.ROOT);
            for (int column = 1; column <= columns; column++) {
                String columnName = rs.getMetaData().getColumnLabel(column).toLowerCase(Locale.ROOT);
                if (columnName.equals(normalizedLabel) || columnName.endsWith("_" + normalizedLabel)
                        || columnName.contains(normalizedLabel)) {
                    return rs.getString(column);
                }
            }
        }
        return null;
    }

    private static boolean getBooleanByLabelOrSuffix(ResultSet rs, boolean defaultValue, String... labels)
            throws SQLException {
        for (String label : labels) {
            try {
                return rs.getBoolean(label);
            } catch (SQLException ignored) {
            }
        }
        int columns = rs.getMetaData().getColumnCount();
        for (String label : labels) {
            String normalizedLabel = label.toLowerCase(Locale.ROOT);
            for (int column = 1; column <= columns; column++) {
                String columnName = rs.getMetaData().getColumnLabel(column).toLowerCase(Locale.ROOT);
                if (columnName.equals(normalizedLabel) || columnName.endsWith("_" + normalizedLabel)
                        || columnName.contains(normalizedLabel)) {
                    return rs.getBoolean(column);
                }
            }
        }
        return defaultValue;
    }

    private static String statsSafeToken(String value) {
        String compact = value.replaceAll("\\s+", "_");
        if (compact.length() > 80) {
            return compact.substring(0, 80);
        }
        return compact;
    }

    private static void addNodeIds(Set<String> nodes, String value) {
        if (value == null) {
            return;
        }
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("\\d+").matcher(value);
        while (matcher.find()) {
            nodes.add(matcher.group());
        }
    }

    private void createVpTablesAtomically(VpSchema vp, String preferredEngine) throws Exception {
        int setupStatementCount = vp.setupStatements.size();
        try {
            createSourceTable(vp, preferredEngine);
            createPartitionTables(vp, preferredEngine);
        } catch (IgnoreMeException e) {
            if (vp.columnCollationClauses.values().stream().noneMatch(clause -> !clause.isEmpty())) {
                throw e;
            }
            // A locale fallback applies to the whole reconstructed schema. Rebuilding from
            // scratch prevents a collated source from being compared with plain partitions.
            dropArtifacts(vp);
            vp.setupStatements.subList(setupStatementCount, vp.setupStatements.size()).clear();
            vp.columnCollationClauses.replaceAll((column, ignored) -> "");
            createSourceTable(vp, preferredEngine);
            createPartitionTables(vp, preferredEngine);
        }
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
            shuffleWithSqlancerRandom(shuffled);
            left.add(shuffled.get(0));
            right.addAll(shuffled.subList(1, shuffled.size()));
            if (right.isEmpty()) {
                right.add(shuffled.get(0));
                overlap.add(shuffled.get(0));
            }
            break;
        case TYPE_DIRECTED_SPLIT:
            for (VpColumn column : columns) {
                if (isNumericType(column.type)) {
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
        shuffleWithSqlancerRandom(shuffled);
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

    private String getStableOrder(CockroachDBTable base, List<VpColumn> columns) {
        List<String> primary = base.getColumns().stream().filter(CockroachDBColumn::isPrimaryKey)
                .map(c -> quote(c.getName())).collect(Collectors.toList());
        if (!primary.isEmpty()) {
            return String.join(", ", primary);
        }
        List<String> comparable = columns.stream().filter(c -> isStableOrderType(c.type)).map(c -> quote(c.name))
                .collect(Collectors.toList());
        if (!comparable.isEmpty()) {
            return String.join(", ", comparable);
        }
        return null;
    }

    private long getRowCount(String tableName) throws Exception {
        String sql = "SELECT COUNT(*) FROM " + quote(tableName);
        for (int attempt = 0; attempt < RESOURCE_READ_RETRIES; attempt++) {
            try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
                if (!rs.next()) {
                    throw new AssertionError("COUNT(*) returned no row for " + tableName);
                }
                return rs.getLong(1);
            } catch (SQLException e) {
                if (isTransientClusterAvailabilityError(e) && attempt + 1 < RESOURCE_READ_RETRIES) {
                    waitForTransientClusterRecovery(attempt);
                    continue;
                }
                if (isTransientClusterAvailabilityError(e)) {
                    recordSkippedError("cluster-transient-row-count", sql, getErrorText(e));
                    throw new IgnoreMeException();
                }
                throw e;
            }
        }
        throw new AssertionError("CockroachDB VP row-count retry loop exhausted");
    }

    private boolean isStableOrderType(CockroachDBCompositeDataType type) {
        switch (type.getPrimitiveDataType()) {
        case FLOAT:
        case JSONB:
        case ARRAY:
            return false;
        default:
            return true;
        }
    }

    private boolean isStableReconstructionKeyType(CockroachDBCompositeDataType type) {
        switch (type.getPrimitiveDataType()) {
        case FLOAT:
        case JSONB:
        case ARRAY:
            return false;
        default:
            return true;
        }
    }

    private boolean isNumericType(CockroachDBCompositeDataType type) {
        switch (type.getPrimitiveDataType()) {
        case INT:
        case SERIAL:
        case FLOAT:
        case DECIMAL:
            return true;
        default:
            return false;
        }
    }

    private boolean isIndexableType(CockroachDBCompositeDataType type) {
        switch (type.getPrimitiveDataType()) {
        case ARRAY:
        case JSONB:
            return false;
        default:
            return true;
        }
    }

    private boolean isInvertedIndexableType(VpColumn column) {
        switch (column.type.getPrimitiveDataType()) {
        case ARRAY:
        case JSONB:
            return column.computedExpression == null;
        default:
            return false;
        }
    }

    private String helperEngineCandidate(BaseTableInfo base) {
        return "N/A";
    }

    private void createSourceTable(VpSchema vp, String preferredEngine) throws Exception {
        vp.sourceEngine = createTableWithEngineFallback(vp, vp.sourceTable, sourceColumnDefs(vp), preferredEngine);
    }

    private void initializeColumnCollations(VpSchema vp) {
        for (VpColumn column : vp.columns) {
            String clause = "";
            if (deepExplorationEnabled() && column.computedExpression == null
                    && column.type.getPrimitiveDataType() == CockroachDBDataType.STRING
                    && (column.name.equals("c3") || Randomly.getBooleanWithRatherLowProbability())) {
                clause = " COLLATE " + Randomly.fromOptions("en", "de", "fr", "und");
                recordAdvancedFeature(AdvancedFeature.CHARSET_COLLATION_COLUMN);
            }
            vp.columnCollationClauses.put(column.name, clause);
        }
    }

    private List<String> sourceColumnDefs(VpSchema vp) {
        if (vp.columns.stream().anyMatch(column -> column.name.equalsIgnoreCase(ROW_ID))) {
            throw new IgnoreMeException();
        }
        List<String> columnDefs = new ArrayList<>();
        columnDefs.add(quote(ROW_ID) + " INT8 NOT NULL PRIMARY KEY");
        for (VpColumn column : vp.columns) {
            columnDefs.add(sourceColumnDef(vp, column));
        }
        columnDefs.addAll(columnFamilyDefs(vp, vp.columns));
        return columnDefs;
    }

    private void populateSourceTable(VpSchema vp, String stableOrder) throws Exception {
        if (!executeSetup(vp, populateSourceSQL(vp, stableOrder))) {
            throw new IgnoreMeException();
        }
    }

    private String populateSourceSQL(VpSchema vp, String stableOrder) {
        List<VpColumn> writableColumns = vp.columns.stream().filter(c -> c.computedExpression == null)
                .collect(Collectors.toList());
        String columnList = writableColumns.stream().map(c -> quote(c.name)).collect(Collectors.joining(", "));
        String order = stableOrder == null ? "" : " ORDER BY " + stableOrder;
        String sql = "INSERT INTO " + quote(vp.sourceTable) + " (" + quote(ROW_ID)
                + (columnList.isEmpty() ? "" : ", " + columnList) + ") SELECT ROW_NUMBER() OVER (" + order + ") AS "
                + quote(ROW_ID) + (columnList.isEmpty() ? "" : ", " + columnList) + " FROM " + quote(vp.baseTable);
        if (state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows > 0) {
            sql += " LIMIT " + state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows;
        }
        return sql;
    }

    private void maybeAmplifySourceRows(VpSchema vp) throws Exception {
        if (!rowRangePressureProfileEnabled()) {
            return;
        }
        if (vp.keyMode != KeyMode.VP_ROWID) {
            recordDistributedProfileEvent("row-pressure-skipped-real-key");
            return;
        }
        long currentRows = getRowCount(vp.sourceTable);
        if (currentRows <= 0) {
            recordDistributedProfileEvent("row-pressure-skipped-empty-source");
            return;
        }
        long targetRows = chooseRowRangePressureTarget(currentRows);
        if (currentRows >= targetRows) {
            recordDistributedProfileEvent("row-pressure-already-large");
            recordDistributedProfileEvent("row-pressure-target=" + targetRows);
            return;
        }
        List<String> writableNames = new ArrayList<>();
        writableNames.add(quote(ROW_ID));
        writableNames.addAll(vp.columns.stream().filter(c -> c.computedExpression == null).map(c -> quote(c.name))
                .collect(Collectors.toList()));
        recordDistributedProfileEvent("row-pressure-target=" + targetRows);
        recordDistributedProfileEvent("row-pressure-before=" + currentRows);
        while (currentRows > 0 && currentRows < targetRows) {
            long maxRowId = maxVpRowId(vp.sourceTable);
            long rowsToAdd = targetRows - currentRows;
            String sql = "INSERT INTO " + quote(vp.sourceTable) + " (" + String.join(", ", writableNames) + ") SELECT "
                    + quote(ROW_ID) + " + " + maxRowId
                    + (writableNames.size() == 1 ? ""
                            : ", " + writableNames.subList(1, writableNames.size()).stream()
                                    .collect(Collectors.joining(", ")))
                    + " FROM " + quote(vp.sourceTable) + " ORDER BY " + quote(ROW_ID) + " LIMIT " + rowsToAdd;
            if (!executeSetup(vp, sql)) {
                recordDistributedProfileEvent("row-pressure-amplify-skipped");
                throw new IgnoreMeException();
            }
            long nextRows = getRowCount(vp.sourceTable);
            if (nextRows <= currentRows) {
                recordDistributedProfileEvent("row-pressure-no-progress");
                break;
            }
            currentRows = nextRows;
        }
        vp.sourceRowCount = currentRows;
        recordDistributedProfileEvent("row-pressure-after=" + currentRows);
    }

    private long chooseRowRangePressureTarget(long currentRows) {
        long cappedMax = Math.min(
                Math.max(1, state.getDbmsSpecificOptions().verticalPartitioningRowRangePressureMaxSourceRows), 4096);
        List<Integer> candidates = ROW_RANGE_PRESSURE_TARGETS.stream().filter(target -> target <= cappedMax)
                .collect(Collectors.toList());
        if (candidates.isEmpty()) {
            return Math.max(currentRows, cappedMax);
        }
        List<Integer> useful = candidates.stream().filter(target -> target > currentRows).collect(Collectors.toList());
        List<Integer> selectable = useful.isEmpty() ? candidates : useful;
        return Randomly.fromList(selectable);
    }

    private long maxVpRowId(String tableName) throws SQLException {
        String sql = "SELECT COALESCE(MAX(" + quote(ROW_ID) + "), 0) FROM " + quote(tableName);
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            if (!rs.next()) {
                return 0;
            }
            return rs.getLong(1);
        }
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
        defs.addAll(columnFamilyDefs(vp, columns));
        return defs;
    }

    private List<String> columnFamilyDefs(VpSchema vp, List<VpColumn> columns) {
        if (!deepExplorationEnabled() || columns.isEmpty() || !Randomly.getBoolean()) {
            return List.of();
        }
        List<String> defs = new ArrayList<>();
        defs.add("FAMILY " + quote("primary") + " (" + quote(ROW_ID) + ")");
        List<String> payloadColumns = columns.stream().map(c -> quote(c.name)).collect(Collectors.toList());
        if (payloadColumns.size() == 1 || Randomly.getBoolean()) {
            defs.add("FAMILY " + quote(vp.prefix + "payload") + " (" + String.join(", ", payloadColumns) + ")");
        } else {
            List<String> first = new ArrayList<>();
            List<String> second = new ArrayList<>();
            for (int i = 0; i < payloadColumns.size(); i++) {
                (i % 2 == 0 ? first : second).add(payloadColumns.get(i));
            }
            if (!first.isEmpty()) {
                defs.add("FAMILY " + quote(vp.prefix + "payload_a") + " (" + String.join(", ", first) + ")");
            }
            if (!second.isEmpty()) {
                defs.add("FAMILY " + quote(vp.prefix + "payload_b") + " (" + String.join(", ", second) + ")");
            }
        }
        recordAdvancedFeature(AdvancedFeature.COLUMN_FAMILY);
        return defs;
    }

    private String columnDef(VpSchema vp, VpColumn column) {
        if ("vp_computed".equals(column.name) && column.computedExpression != null) {
            return quote(column.name) + " " + toSQLType(column.type) + " AS (" + column.computedExpression + ") STORED";
        }
        return quote(column.name) + " " + toSQLType(column.type)
                + vp.columnCollationClauses.getOrDefault(column.name, "") + " NULL";
    }

    private String sourceColumnDef(VpSchema vp, VpColumn column) {
        if (column.computedExpression != null) {
            return quote(column.name) + " " + toSQLType(column.type) + " AS (" + column.computedExpression + ") STORED";
        }
        return columnDef(vp, column);
    }

    private void configureVpComputedColumn(VpSchema vp) {
        if (!advancedFeaturesEnabled() || vp.columns.stream().anyMatch(c -> c.name.equals("vp_computed"))) {
            return;
        }
        VpColumn computed = new VpColumn("vp_computed", CockroachDBCompositeDataType.getInt(8), quote(ROW_ID) + " * 2");
        vp.columns.add(computed);
        (Randomly.getBoolean() ? vp.leftColumns : vp.rightColumns).add(computed);
        vp.columnCollationClauses.put(computed.name, "");
        recordAdvancedFeature(AdvancedFeature.COMPUTED_COLUMN);
    }

    private String createTableWithEngineFallback(VpSchema vp, String tableName, List<String> columnDefs,
            String preferredEngine) throws Exception {
        if (executeSetup(vp, createTableSQL(tableName, columnDefs, preferredEngine))) {
            return preferredEngine;
        }
        throw new IgnoreMeException();
    }

    private String createTableSQL(String tableName, List<String> columnDefs, String engine) {
        return "CREATE TABLE " + quote(tableName) + " (" + String.join(", ", columnDefs) + ")";
    }

    private void populatePartitionTables(VpSchema vp) throws Exception {
        vp.leftEngine = populatePartitionTable(vp, vp.leftTable, vp.leftColumns, vp.leftEngine);
        vp.rightEngine = populatePartitionTable(vp, vp.rightTable, vp.rightColumns, vp.rightEngine);
    }

    private String populatePartitionTable(VpSchema vp, String tableName, List<VpColumn> columns, String engine)
            throws Exception {
        if (executeSetup(vp, populatePartitionSQL(tableName, columns, vp.sourceTable))) {
            return engine;
        }
        throw new IgnoreMeException();
    }

    private String populatePartitionSQL(String partitionTable, List<VpColumn> columns, String sourceTable) {
        List<String> names = new ArrayList<>();
        names.add(quote(ROW_ID));
        names.addAll(columns.stream().filter(c -> !"vp_computed".equals(c.name)).map(c -> quote(c.name))
                .collect(Collectors.toList()));
        return "INSERT INTO " + quote(partitionTable) + " (" + String.join(", ", names) + ") SELECT "
                + String.join(", ", names) + " FROM " + quote(sourceTable);
    }

    private void maybeCreateHelperIndexes(VpSchema vp) throws Exception {
        if (!advancedFeaturesEnabled()) {
            return;
        }
        maybeCreateAsymmetricHelperIndexes(vp);
        maybeCreatePartialHelperIndexes(vp);
        maybeCreateInvertedHelperIndexes(vp);
        maybeCreateReconstructionKeyIndexes(vp);
        maybeCreateComputedColumnIndexes(vp);
        maybeCreateDirectedStringIndexes(vp);
        maybeCreateHelperIndex(vp, vp.sourceTable, vp.columns, "src");
        maybeCreateHelperIndex(vp, vp.leftTable, vp.leftColumns, "l");
        maybeCreateHelperIndex(vp, vp.rightTable, vp.rightColumns, "r");
    }

    private void maybeCreateReconstructionKeyIndexes(VpSchema vp) {
        if (vp.keyMode == KeyMode.VP_ROWID || vp.reconstructionKeyColumns.isEmpty()) {
            return;
        }
        createReconstructionKeyIndex(vp, vp.sourceTable, "src_key");
        createReconstructionKeyIndex(vp, vp.leftTable, "l_key");
        createReconstructionKeyIndex(vp, vp.rightTable, "r_key");
    }

    private void maybeCreateDirectedStringIndexes(VpSchema vp) {
        if (!deepExplorationEnabled()) {
            return;
        }
        for (String table : List.of(vp.sourceTable, vp.leftTable, vp.rightTable)) {
            List<VpColumn> columns = table.equals(vp.sourceTable) ? vp.columns
                    : table.equals(vp.leftTable) ? vp.leftColumns : vp.rightColumns;
            VpColumn column = columns.stream()
                    .filter(c -> c.type.getPrimitiveDataType() == CockroachDBDataType.STRING).findFirst().orElse(null);
            if (column == null) {
                continue;
            }
            String suffix = table.substring(vp.prefix.length());
            executeOptionalHelperIndexSetup(vp, "CREATE INDEX " + quote(vp.prefix + "str_" + suffix) + " ON "
                    + quote(table) + " (" + quote(column.name) + ", " + quote(ROW_ID) + ")");
            String expressionIndex = "CREATE INDEX " + quote(vp.prefix + "expr_" + suffix) + " ON " + quote(table)
                    + " (lower(" + quote(column.name) + "::STRING))";
            if (executeDirectedIndexSetup(vp, expressionIndex)) {
                recordAdvancedFeature(AdvancedFeature.EXPRESSION_INDEX);
            }
        }
    }

    private boolean executeDirectedIndexSetup(VpSchema vp, String sql) {
        executeOptionalHelperIndexSetup(vp, sql);
        return vp.setupStatements.contains(sql);
    }

    private void maybeCreateComputedColumnIndexes(VpSchema vp) {
        if (!deepExplorationEnabled()) {
            return;
        }
        List<String> indexedTables = new ArrayList<>();
        if (vp.columns.stream().anyMatch(c -> c.computedExpression != null)) {
            indexedTables.add(vp.sourceTable);
        }
        if (vp.leftColumns.stream().anyMatch(c -> c.computedExpression != null)) {
            indexedTables.add(vp.leftTable);
        }
        if (vp.rightColumns.stream().anyMatch(c -> c.computedExpression != null)) {
            indexedTables.add(vp.rightTable);
        }
        if (indexedTables.isEmpty()) {
            return;
        }
        for (String table : indexedTables) {
            List<VpColumn> columns = table.equals(vp.sourceTable) ? vp.columns
                    : table.equals(vp.leftTable) ? vp.leftColumns : vp.rightColumns;
            VpColumn computed = columns.stream().filter(c -> c.computedExpression != null).findFirst().orElse(null);
            if (computed == null) {
                continue;
            }
            String sql = "CREATE INDEX " + quote(vp.prefix + "computed_" + table.substring(vp.prefix.length())) + " ON "
                    + quote(table) + " (" + quote(computed.name) + ", " + quote(ROW_ID) + ")";
            executeOptionalHelperIndexSetup(vp, sql);
            recordAdvancedFeature(AdvancedFeature.COMPUTED_COLUMN);
        }
    }

    private void createReconstructionKeyIndex(VpSchema vp, String tableName, String suffix) {
        List<String> keyParts = new ArrayList<>();
        for (VpColumn column : vp.reconstructionKeyColumns) {
            keyParts.add(quote(column.name) + (Randomly.getBoolean() ? " DESC" : " ASC"));
        }
        if (Randomly.getBoolean()) {
            keyParts.add(quote(ROW_ID) + (Randomly.getBoolean() ? " DESC" : " ASC"));
        }
        StringBuilder sql = new StringBuilder("CREATE INDEX ").append(quote(vp.prefix + suffix)).append(" ON ")
                .append(quote(tableName)).append(" (").append(String.join(", ", keyParts)).append(")");
        if (Randomly.getBooleanWithSmallProbability()) {
            sql.append(" USING HASH WITH BUCKET_COUNT=").append(Randomly.getNotCachedInteger(2, 32));
            recordAdvancedFeature(AdvancedFeature.HASH_SHARDED_INDEX);
        }
        recordAdvancedFeature(AdvancedFeature.REAL_KEY_RECONSTRUCTION);
        executeOptionalHelperIndexSetup(vp, sql.toString());
    }

    private void maybeCreateInvertedHelperIndexes(VpSchema vp) {
        createInvertedHelperIndex(vp, vp.sourceTable, vp.columns, "src_inv");
        createInvertedHelperIndex(vp, vp.leftTable, vp.leftColumns, "l_inv");
        createInvertedHelperIndex(vp, vp.rightTable, vp.rightColumns, "r_inv");
    }

    private void createInvertedHelperIndex(VpSchema vp, String tableName, List<VpColumn> columns, String suffix) {
        List<VpColumn> inverted = columns.stream().filter(this::isInvertedIndexableType)
                .collect(Collectors.toCollection(ArrayList::new));
        if (inverted.isEmpty()) {
            return;
        }
        shuffleWithSqlancerRandom(inverted);
        for (VpColumn column : inverted.subList(0, Math.min(2, inverted.size()))) {
            String sql = "CREATE INVERTED INDEX " + quote(vp.prefix + suffix + "_" + column.name) + " ON "
                    + quote(tableName) + " (" + quote(column.name) + ")";
            if (executeDirectedIndexSetup(vp, sql)) {
                recordAdvancedFeature(AdvancedFeature.INVERTED_INDEX);
            }
        }
    }

    private void maybeCreatePartialHelperIndexes(VpSchema vp) {
        createPartialHelperIndex(vp, vp.leftTable, vp.leftColumns, "partial_l");
        createPartialHelperIndex(vp, vp.rightTable, vp.rightColumns, "partial_r");
    }

    private void createPartialHelperIndex(VpSchema vp, String tableName, List<VpColumn> columns, String suffix) {
        List<VpColumn> indexable = helperIndexableColumns(columns);
        if (indexable.isEmpty()) {
            return;
        }
        VpColumn column = indexable.get(0);
        String sql = "CREATE INDEX " + quote(vp.prefix + suffix) + " ON " + quote(tableName) + " (" + quote(column.name)
                + ") WHERE " + quote(column.name) + " IS NOT NULL";
        recordAdvancedFeature(AdvancedFeature.PARTIAL_INDEX);
        executeOptionalHelperIndexSetup(vp, sql);
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
        return columns.stream().filter(c -> isIndexableType(c.type)).collect(Collectors.toList());
    }

    private void maybeCreateHelperIndex(VpSchema vp, String tableName, List<VpColumn> columns, String suffix)
            throws Exception {
        List<VpColumn> indexable = helperIndexableColumns(columns);
        if (indexable.isEmpty() || !Randomly.getBoolean()) {
            return;
        }
        shuffleWithSqlancerRandom(indexable);
        List<String> indexColumns = new ArrayList<>();
        if (Randomly.getBoolean()) {
            indexColumns.add(quote(ROW_ID));
        }
        List<VpColumn> selected = indexable.stream()
                .limit(state.getRandomly().getInteger(1, Math.min(2, indexable.size()) + 1))
                .collect(Collectors.toList());
        boolean expressionIndex = deepExplorationEnabled() && Randomly.getBooleanWithRatherLowProbability()
                && selected.stream().anyMatch(c -> c.type.getPrimitiveDataType() == CockroachDBDataType.STRING);
        if (expressionIndex) {
            VpColumn expressionColumn = selected.stream()
                    .filter(c -> c.type.getPrimitiveDataType() == CockroachDBDataType.STRING).findFirst().get();
            indexColumns.add("lower(" + quote(expressionColumn.name) + ")");
            recordAdvancedFeature(AdvancedFeature.EXPRESSION_INDEX);
        } else {
            indexColumns.addAll(selected.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        }
        if (!indexColumns.contains(quote(ROW_ID)) && Randomly.getBoolean()) {
            indexColumns.add(quote(ROW_ID));
        }
        String indexName = quote(vp.prefix + "idx_" + suffix + "_" + Math.abs(state.getRandomly().getInteger()));
        StringBuilder sql = new StringBuilder("CREATE INDEX ").append(indexName).append(" ON ").append(quote(tableName))
                .append("(").append(String.join(", ", indexColumns)).append(")");
        if (!expressionIndex && Randomly.getBooleanWithRatherLowProbability()) {
            sql.append(" USING HASH WITH BUCKET_COUNT=").append(Randomly.getNotCachedInteger(2, 32));
            recordAdvancedFeature(AdvancedFeature.HASH_SHARDED_INDEX);
        }
        if (!expressionIndex && Randomly.getBooleanWithRatherLowProbability()) {
            List<VpColumn> storing = indexable.stream().filter(c -> !selected.contains(c)).limit(1)
                    .collect(Collectors.toList());
            if (!storing.isEmpty()) {
                sql.append(" STORING (").append(quote(storing.get(0).name)).append(")");
                recordAdvancedFeature(AdvancedFeature.COVERING_INDEX);
            }
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            VpColumn predicateColumn = selected.get(0);
            sql.append(" WHERE ").append(quote(predicateColumn.name)).append(" IS NOT NULL");
            recordAdvancedFeature(AdvancedFeature.PARTIAL_INDEX);
        }
        executeOptionalHelperIndexSetup(vp, sql.toString());
    }

    private void executeOptionalHelperIndexSetup(VpSchema vp, String sql) {
        try {
            if (!executeSetup(vp, sql)) {
                vp.setupStatements.add("-- skipped helper index: " + sql);
            }
        } catch (AssertionError ignored) {
            vp.setupStatements.add("-- skipped helper index: " + sql);
        } catch (Exception ignored) {
            vp.setupStatements.add("-- skipped helper index: " + sql);
        }
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
            String rewritten = rewriteSchemaObjectForSource(vp, element, copied);
            String sql = schemaObjectCopySQL(vp, rewritten);
            copied++;
            try {
                executeSetup(vp, sql);
            } catch (AssertionError ignored) {
            } catch (Exception ignored) {
            }
        }
    }

    private boolean isCopyableSourceSchemaObject(String normalizedElement) {
        return normalizedElement.startsWith("INDEX ") || normalizedElement.startsWith("UNIQUE INDEX ")
                || normalizedElement.startsWith("INVERTED INDEX ")
                || normalizedElement.startsWith("CONSTRAINT ") && !normalizedElement.contains(" FOREIGN KEY ");
    }

    private String rewriteSchemaObjectForSource(VpSchema vp, String element, int ordinal) {
        String rewritten = element.trim();
        String indexName = quote(vp.prefix + "src_idx_" + ordinal);
        String constraintName = quote(vp.prefix + "src_chk_" + ordinal);
        String normalized = rewritten.toUpperCase(Locale.ROOT);
        if (normalized.startsWith("CONSTRAINT ") && normalized.contains(" PRIMARY KEY ")) {
            return rewritten.replaceFirst("(?i)^CONSTRAINT\\s+(?:\"[^\"]+\"|\\S+)\\s+PRIMARY\\s+KEY",
                    "UNIQUE INDEX " + indexName);
        }
        rewritten = rewritten.replaceFirst("(?i)^INDEX\\s+(?:\"[^\"]+\"|\\S+)", "INDEX " + indexName);
        rewritten = rewritten.replaceFirst("(?i)^UNIQUE\\s+INDEX\\s+(?:\"[^\"]+\"|\\S+)", "UNIQUE INDEX " + indexName);
        rewritten = rewritten.replaceFirst("(?i)^INVERTED\\s+INDEX\\s+(?:\"[^\"]+\"|\\S+)",
                "INVERTED INDEX " + indexName);
        rewritten = rewritten.replaceFirst("(?i)^CONSTRAINT\\s+(?:\"[^\"]+\"|\\S+)", "CONSTRAINT " + constraintName);
        return rewritten;
    }

    private String schemaObjectCopySQL(VpSchema vp, String rewritten) {
        String normalized = rewritten.trim().toUpperCase(Locale.ROOT);
        if (normalized.startsWith("INDEX ") || normalized.startsWith("UNIQUE INDEX ")
                || normalized.startsWith("INVERTED INDEX ")) {
            int columnsStart = rewritten.indexOf('(');
            if (columnsStart < 0) {
                throw new IgnoreMeException();
            }
            String indexPrefix = rewritten.substring(0, columnsStart).trim();
            String columns = rewritten.substring(columnsStart).trim();
            return "CREATE " + indexPrefix + " ON " + quote(vp.sourceTable) + " " + columns;
        }
        return "ALTER TABLE " + quote(vp.sourceTable) + " ADD " + rewritten;
    }

    private List<String> extractTableElements(String createTable) {
        int start = createTable.indexOf('(');
        int end = findMatchingParen(createTable, start);
        if (start < 0 || end <= start) {
            return List.of();
        }
        String body = createTable.substring(start + 1, end);
        List<String> elements = splitTopLevelComma(body);
        return elements.stream().map(String::trim).filter(s -> !s.startsWith("\"")).collect(Collectors.toList());
    }

    private int findMatchingParen(String text, int openIndex) {
        if (openIndex < 0) {
            return -1;
        }
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inBacktick = false;
        boolean inDoubleQuote = false;
        for (int i = openIndex; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (ch == '\'' && !inBacktick && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            } else if (ch == '`' && !inSingleQuote && !inDoubleQuote) {
                inBacktick = !inBacktick;
            } else if (ch == '"' && !inSingleQuote && !inBacktick) {
                if (inDoubleQuote && i + 1 < text.length() && text.charAt(i + 1) == '"') {
                    i++;
                } else {
                    inDoubleQuote = !inDoubleQuote;
                }
            } else if (!inSingleQuote && !inBacktick && !inDoubleQuote) {
                if (ch == '(') {
                    depth++;
                } else if (ch == ')') {
                    depth--;
                    if (depth == 0) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }

    private List<String> splitTopLevelComma(String body) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inBacktick = false;
        boolean inDoubleQuote = false;
        int start = 0;
        for (int i = 0; i < body.length(); i++) {
            char ch = body.charAt(i);
            if (ch == '\'' && !inBacktick && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            } else if (ch == '`' && !inSingleQuote && !inDoubleQuote) {
                inBacktick = !inBacktick;
            } else if (ch == '"' && !inSingleQuote && !inBacktick) {
                if (inDoubleQuote && i + 1 < body.length() && body.charAt(i + 1) == '"') {
                    i++;
                } else {
                    inDoubleQuote = !inDoubleQuote;
                }
            } else if (!inSingleQuote && !inBacktick && !inDoubleQuote) {
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
        if (strategy == ReconstructionStrategy.VIEW) {
            vp.activeViewName = vp.prefix + "v_" + ORACLE_RUN_ID.incrementAndGet();
            vp.viewNames.add(vp.activeViewName);
        }
        SourceAst sourceAst = createSourceAst(vp);
        if (deepExplorationEnabled() && directedQuerySequence++ % 2 == 0) {
            return generateDirectedRewritePair(vp, strategy, sourceAst);
        }
        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(state).setTypeAwareCasts(true)
                .setColumns(sourceAst.expressionColumns);
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()) {
            return generateCorrelatedSubqueryPair(vp, strategy, sourceAst);
        }
        CockroachDBSelect select = new CockroachDBSelect();
        select.setDistinct(Randomly.getBoolean());
        int fetchColumns = Math.max(1, Randomly.smallNumber() + 1);
        boolean useAggregate = Randomly.getBoolean();
        boolean hasWindowExpression = false;
        boolean useSyntheticRollup = false;
        Set<AdvancedFeature> queryFeatures = EnumSet.noneOf(AdvancedFeature.class);
        CockroachDBColumn computedColumn = sourceAst.expressionColumns.stream()
                .filter(c -> c.getName().equals("vp_computed")).findFirst().orElse(null);
        boolean forceComputedColumn = computedColumn != null && strategy == ReconstructionStrategy.JOIN
                && !useAggregate;
        List<CockroachDBExpression> selected = new ArrayList<>();
        List<CockroachDBExpression> nonAggregateExpressions = new ArrayList<>();
        List<CockroachDBExpression> aggregateExpressions = new ArrayList<>();
        List<CockroachDBExpression> literalExpressions = new ArrayList<>();
        boolean hasRealColumnReference = false;
        for (int i = 0; i < fetchColumns; i++) {
            CockroachDBExpression expr;
            if (i == 0 && !sourceAst.columnCollationLocales.isEmpty() && Randomly.getBoolean()) {
                expr = generateCollatedSelectExpression(sourceAst, useAggregate);
                queryFeatures.add(AdvancedFeature.COLLATION_PREDICATE);
                if (useAggregate) {
                    aggregateExpressions.add(expr);
                } else {
                    nonAggregateExpressions.add(expr);
                    hasRealColumnReference = true;
                }
            } else if (i == 0 && forceComputedColumn) {
                expr = new CockroachDBColumnReference(computedColumn);
                queryFeatures.add(AdvancedFeature.COMPUTED_COLUMN);
            } else if (!useAggregate && advancedFeaturesEnabled() && Randomly.getBooleanWithRatherLowProbability()) {
                expr = generateAdvancedSelectExpression(sourceAst, gen);
                hasWindowExpression = true;
            } else if (useAggregate && Randomly.getBoolean()) {
                expr = generateVpAggregate(sourceAst.expressionColumns);
                aggregateExpressions.add(expr);
            } else {
                // A projection made solely of generated constants is not a
                // meaningful VP check. Ensure every non-aggregate query has
                // at least one reference to a column from the source.
                if (!useAggregate && !hasRealColumnReference && i == fetchColumns - 1) {
                    expr = new CockroachDBColumnReference(Randomly.fromList(sourceAst.expressionColumns));
                } else {
                    expr = randomExpression(gen, sourceAst.expressionColumns);
                }
                if (isLiteralExpression(expr)) {
                    literalExpressions.add(expr);
                } else {
                    nonAggregateExpressions.add(expr);
                }
                hasRealColumnReference |= expr instanceof CockroachDBColumnReference;
            }
            selected.add(expr);
        }
        if (!useAggregate && !hasRealColumnReference && !selected.isEmpty()) {
            CockroachDBExpression expr = new CockroachDBColumnReference(Randomly.fromList(sourceAst.expressionColumns));
            selected.set(selected.size() - 1, expr);
            nonAggregateExpressions.add(expr);
            hasRealColumnReference = true;
        }
        if (selected.isEmpty()) {
            CockroachDBExpression expr = new CockroachDBColumnReference(Randomly.fromList(sourceAst.expressionColumns));
            selected.add(expr);
            nonAggregateExpressions.add(expr);
        }
        select.setFromList(List.of(new CockroachDBTableReference(sourceAst.table)));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(CockroachDBDataType.BOOL.get()));
        }
        if (useAggregate || (!hasWindowExpression && Randomly.getBooleanWithRatherLowProbability())) {
            List<CockroachDBExpression> groupByExpressions = freezeExpressions(
                    chooseGroupByExpressions(nonAggregateExpressions));
            select.setGroupByExpressions(groupByExpressions);
            selected = buildGroupedFetchColumns(groupByExpressions, aggregateExpressions, literalExpressions);
            if (useAggregate && groupByExpressions.isEmpty() && aggregateExpressions.isEmpty()) {
                selected.add(freezeExpression(generateVpAggregate(sourceAst.expressionColumns)));
            }
            if (selected.isEmpty()) {
                if (!groupByExpressions.isEmpty()) {
                    selected.add(Randomly.fromList(groupByExpressions));
                } else {
                    selected.add(freezeExpression(generateVpAggregate(sourceAst.expressionColumns)));
                }
            }
            if (Randomly.getBoolean() && !groupByExpressions.isEmpty()) {
                select.setHavingClause(
                        generateGroupByHavingExpression(groupByExpressions, sourceAst.expressionColumns));
            }
            if (advancedFeaturesEnabled() && !hasWindowExpression && !groupByExpressions.isEmpty()
                    && Randomly.getBooleanWithSmallProbability()) {
                recordAdvancedFeature(AdvancedFeature.ROLLUP);
                queryFeatures.add(AdvancedFeature.ROLLUP);
                useSyntheticRollup = true;
            }
        } else if (select.isDistinct()) {
            selected = freezeExpressions(selected);
        } else if (Randomly.getBooleanWithRatherLowProbability()) {
            selected = freezeExpressions(selected);
        }
        select.setFetchColumns(selected);
        CockroachDBExpression capabilityPredicate = maybeGenerateCapabilityPredicate(sourceAst, queryFeatures);
        if (capabilityPredicate != null) {
            select.setWhereClause(andPredicates(select.getWhereClause(), capabilityPredicate));
        }
        boolean orderedComparisonEnabled = isRowPreservingQuery(useAggregate, select)
                && hasVpRowIdTieBreaker(select, sourceAst.rowIdColumn);
        if (Randomly.getBoolean()) {
            select.setOrderByClauses(chooseLegalOrderBys(select, nonAggregateExpressions, sourceAst.rowIdColumn,
                    isRowPreservingQuery(useAggregate, select)));
            orderedComparisonEnabled = isRowPreservingQuery(useAggregate, select)
                    && hasVpRowIdTieBreaker(select, sourceAst.rowIdColumn);
        }
        if (Randomly.getBoolean()) {
            boolean rowPreservingQuery = isRowPreservingQuery(useAggregate, select);
            List<CockroachDBExpression> stableOrderBys = chooseStableLimitOrderBys(select, sourceAst.rowIdColumn,
                    rowPreservingQuery, useAggregate);
            if (!stableOrderBys.isEmpty() || isSingleRowAggregate(useAggregate, select)) {
                select.setOrderByClauses(stableOrderBys);
                orderedComparisonEnabled = rowPreservingQuery && hasVpRowIdTieBreaker(select, sourceAst.rowIdColumn);
                select.setLimitClause(CockroachDBConstant.createIntConstant(Randomly.getNotCachedInteger(0, 25)));
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    select.setOffsetClause(CockroachDBConstant.createIntConstant(Randomly.getNotCachedInteger(0, 5)));
                }
            }
        }
        boolean useUnionAll = advancedFeaturesEnabled() && !useAggregate && select.getOrderByClauses().isEmpty()
                && select.getLimitClause() == null && select.getOffsetClause() == null
                && strategy != ReconstructionStrategy.TEMP_TABLE && Randomly.getBooleanWithSmallProbability();
        String setOperation = advancedFeaturesEnabled() && !useAggregate && !useUnionAll
                && select.getOrderByClauses().isEmpty() && select.getLimitClause() == null
                && select.getOffsetClause() == null && Randomly.getBooleanWithSmallProbability()
                        ? Randomly.fromOptions("EXCEPT", "INTERSECT") : null;
        boolean unionAllCopiesWhere = useUnionAll && Randomly.getBoolean();
        String singleQuery = renderOuterQuery(select, sourceAst, sourceTableRelation(vp, strategy), selected,
                useSyntheticRollup, useUnionAll, unionAllCopiesWhere, setOperation);
        if (useSyntheticRollup) {
            orderedComparisonEnabled = false;
        }
        if (useUnionAll) {
            recordAdvancedFeature(AdvancedFeature.UNION_ALL);
            queryFeatures.add(AdvancedFeature.UNION_ALL);
            orderedComparisonEnabled = false;
        }
        if (setOperation != null) {
            recordAdvancedFeature(AdvancedFeature.SET_OPERATION);
            queryFeatures.add(AdvancedFeature.SET_OPERATION);
            orderedComparisonEnabled = false;
        }
        String normalizedQuery = singleQuery.toUpperCase(Locale.ROOT);
        if (normalizedQuery.contains("STRING_AGG(") || normalizedQuery.contains("CONCAT_AGG(")) {
            queryFeatures.add(AdvancedFeature.GROUP_CONCAT);
        }
        List<String> extraSetup = new ArrayList<>();
        SourceRelation relation = buildReconstructionRelation(vp, strategy, extraSetup);
        String splitQuery = renderOuterQuery(select, sourceAst, relation, selected, useSyntheticRollup, useUnionAll,
                unionAllCopiesWhere, setOperation);
        return new VpQueryPair(vp, strategy, singleQuery, splitQuery, extraSetup,
                "shape=" + vp.baseShape + ", split=" + vp.splitStrategy + ", reconstruction=" + strategy,
                orderedComparisonEnabled, queryFeatures, vp.optimizerProfile);
    }

    private VpQueryPair generateDirectedRewritePair(VpSchema vp, ReconstructionStrategy strategy, SourceAst sourceAst)
            throws Exception {
        List<AdvancedFeature> families = new ArrayList<>();
        if (jsonbPredicate(sourceAst) != null) {
            families.add(AdvancedFeature.JSONB_PREDICATE);
        }
        if (arrayPredicate(sourceAst) != null) {
            families.add(AdvancedFeature.ARRAY_PREDICATE);
        }
        if (!sourceAst.columnCollationLocales.isEmpty()) {
            families.add(AdvancedFeature.COLLATION_PREDICATE);
        }
        if (vp.columns.stream().anyMatch(c -> c.type.getPrimitiveDataType() == CockroachDBDataType.ENUM)) {
            families.add(AdvancedFeature.ENUM);
        }
        if (vp.columns.stream().anyMatch(c -> c.computedExpression != null)) {
            families.add(AdvancedFeature.COMPUTED_COLUMN);
        }
        if (vp.timeTravelTimestamp == null) {
            families.add(AdvancedFeature.SET_OPERATION);
        }
        if (families.isEmpty()) {
            throw new IgnoreMeException();
        }
        AdvancedFeature family = families.get(Math.floorMod(directedFamilySequence++, families.size()));
        CockroachDBSelect select = new CockroachDBSelect();
        select.setDistinct(false);
        select.setFromList(List.of(new CockroachDBTableReference(sourceAst.table)));
        List<CockroachDBExpression> selected = new ArrayList<>();
        String operation = null;
        boolean unionAll = false;
        if (family == AdvancedFeature.JSONB_PREDICATE || family == AdvancedFeature.ARRAY_PREDICATE) {
            CockroachDBDataType type = family == AdvancedFeature.JSONB_PREDICATE ? CockroachDBDataType.JSONB
                    : CockroachDBDataType.ARRAY;
            VpColumn column = Randomly.fromList(vp.columns.stream()
                    .filter(c -> c.type.getPrimitiveDataType() == type)
                    .filter(c -> type != CockroachDBDataType.ARRAY || isArrayContainmentType(c.type.getElementType()))
                    .collect(Collectors.toList()));
            String name = quote(column.name);
            selected.add(new CockroachDBText(name));
            if (type == CockroachDBDataType.JSONB) {
                select.setWhereClause(new CockroachDBText(Randomly.fromOptions(name + " @> '{\"vp\":1}'::JSONB",
                        "'{}'::JSONB <@ " + name, name + " ? 'k'", name + " ?| ARRAY['k', 'vp']",
                        name + " ?& ARRAY['vp']")));
            } else {
                String probe = "ARRAY[]::" + arraySQLType(column.type);
                if (column.type.getElementType().getPrimitiveDataType() == CockroachDBDataType.INT) {
                    probe = "ARRAY[" + Randomly.fromOptions("0", "1", "-1") + "]::" + arraySQLType(column.type);
                }
                select.setWhereClause(new CockroachDBText(Randomly.fromOptions(name + " @> " + probe,
                        name + " <@ " + probe, name + " && " + probe)));
            }
        } else if (family == AdvancedFeature.SET_OPERATION) {
            List<VpColumn> payloads = vp.columns.stream()
                    .filter(c -> c.type.getPrimitiveDataType() == CockroachDBDataType.INT
                            || c.type.getPrimitiveDataType() == CockroachDBDataType.STRING
                            || c.type.getPrimitiveDataType() == CockroachDBDataType.ENUM)
                    .collect(Collectors.toList());
            if (payloads.isEmpty() || Randomly.getBoolean()) {
                selected.add(new CockroachDBColumnReference(sourceAst.rowIdColumn));
            }
            if (!payloads.isEmpty()) {
                selected.add(new CockroachDBText(quote(Randomly.fromList(payloads).name)));
            }
            String choice = Randomly.fromOptions("UNION", "UNION ALL", "INTERSECT", "EXCEPT", "INTERSECT ALL",
                    "EXCEPT ALL");
            unionAll = "UNION ALL".equals(choice);
            operation = unionAll ? null : choice;
        } else {
            VpColumn column;
            if (family == AdvancedFeature.COLLATION_PREDICATE) {
                String name = Randomly.fromList(new ArrayList<>(sourceAst.columnCollationLocales.keySet()));
                column = findColumn(vp.columns, name);
                select.setWhereClause(Randomly.getBoolean() ? collatedColumnPredicate(sourceAst)
                        : new CockroachDBText("lower(" + quote(name) + "::STRING) >= 'a'"));
            } else if (family == AdvancedFeature.ENUM) {
                column = Randomly.fromList(vp.columns.stream()
                        .filter(c -> c.type.getPrimitiveDataType() == CockroachDBDataType.ENUM)
                        .collect(Collectors.toList()));
                String label = Randomly.fromList(column.type.getEnumLabels()).replace("'", "''");
                select.setWhereClause(new CockroachDBText(quote(column.name)
                        + Randomly.fromOptions(" = ", " < ", " >= ", " IS NOT DISTINCT FROM ") + "'" + label + "'::"
                        + toSQLType(column.type)));
            } else {
                column = Randomly.fromList(vp.columns.stream().filter(c -> c.computedExpression != null)
                        .collect(Collectors.toList()));
                CockroachDBDataType type = column.type.getPrimitiveDataType();
                boolean numeric = type == CockroachDBDataType.INT || type == CockroachDBDataType.FLOAT
                        || type == CockroachDBDataType.DECIMAL;
                select.setWhereClause(new CockroachDBText(quote(column.name) + (numeric ? " >= 0" : " IS NOT NULL")));
            }
            CockroachDBText key = new CockroachDBText(quote(column.name));
            selected.add(key);
            if (Randomly.getBoolean()) {
                selected.add(new CockroachDBText("COUNT(*)"));
                select.setGroupByExpressions(List.of(key));
            } else {
                selected.add(new CockroachDBColumnReference(sourceAst.rowIdColumn));
            }
            select.setOrderByClauses(List.of(key));
        }
        select.setFetchColumns(selected);
        List<String> extraSetup = new ArrayList<>();
        SourceRelation reconstructed = buildReconstructionRelation(vp, strategy, extraSetup);
        String single = renderOuterQuery(select, sourceAst, sourceTableRelation(vp, strategy), selected, false, unionAll,
                false, operation);
        String split = renderOuterQuery(select, sourceAst, reconstructed, selected, false, unionAll, false, operation);
        AdvancedFeature feature = unionAll ? AdvancedFeature.UNION_ALL : family;
        recordAdvancedFeature(feature);
        return new VpQueryPair(vp, strategy, single, split, extraSetup, "directed-family=" + family, false,
                EnumSet.of(feature), vp.optimizerProfile);
    }

    private String renderOuterQuery(CockroachDBSelect select, SourceAst sourceAst, SourceRelation relation,
            List<CockroachDBExpression> selected, boolean useSyntheticRollup, boolean useUnionAll,
            boolean unionAllCopiesWhere, String setOperation) {
        List<CockroachDBExpression> originalFrom = select.getFromList();
        // CockroachDB rejects AS OF SYSTEM TIME attached to individual set-operation branches.
        // Such combinations are gated below when a historical timestamp is active.
        boolean setOperationQuery = useSyntheticRollup || useUnionAll || setOperation != null;
        if (setOperationQuery && relation.hasTimeTravelTimestamp()) {
            // CockroachDB rejects AS OF SYSTEM TIME on UNION branches. The outer
            // derived-table workaround does not prove that every branch was read
            // at the captured timestamp, so do not count this pair as time-travel
            // coverage. Plain VIEW/TEMP_TABLE relations still use AOST directly.
            recordTimeTravelProfileEvent("set-operation-skipped-no-branch-aost");
            throw new IgnoreMeException();
        }
        select.setFromList(
                List.of(new CockroachDBText(relation.withAlias(quote(sourceAst.table.getName()), !setOperationQuery))));
        try {
            String rendered = CockroachDBVisitor.asString(select);
            if (useSyntheticRollup) {
                rendered = "(" + rendered + ") UNION ALL (" + createSyntheticRollupBranches(sourceAst, selected,
                        select.getGroupByExpressions(), select.getWhereClause(), relation) + ")";
            }
            if (useUnionAll) {
                rendered = "(" + rendered + ") UNION ALL ("
                        + createUnionAllBranch(sourceAst, selected, select, relation, unionAllCopiesWhere) + ")";
            }
            if (setOperation != null) {
                rendered = "(" + rendered + ") " + setOperation + " ("
                        + createSetOperationBranch(sourceAst, selected, select, relation, setOperation) + ")";
            }
            return rendered;
        } finally {
            select.setFromList(originalFrom);
        }
    }

    private String createSetOperationBranch(SourceAst sourceAst, List<CockroachDBExpression> selected,
            CockroachDBSelect leftSelect, SourceRelation relation, String operation) {
        CockroachDBSelect right = new CockroachDBSelect();
        right.setDistinct(false);
        right.setFetchColumns(selected);
        right.setFromList(List.of(new CockroachDBText(relation.withAlias(quote(sourceAst.table.getName()), false))));
        if ("EXCEPT".equals(operation)) {
            // Keep the right operand non-empty and different from the left. This
            // exercises real EXCEPT semantics instead of A EXCEPT empty-set.
            right.setWhereClause(setOperationSubsetPredicate(sourceAst.table.getName()));
        } else {
            // INTERSECT with a deterministic row-id subset exercises filtering
            // inside the set operator while preserving single/split equivalence.
            right.setWhereClause(setOperationSubsetPredicate(sourceAst.table.getName()));
        }
        return CockroachDBVisitor.asString(right);
    }

    private CockroachDBExpression setOperationSubsetPredicate(String tableName) {
        return new CockroachDBText("(" + quote(tableName) + "." + quote(ROW_ID) + " % 2::INT8 = 0)");
    }

    private String createSyntheticRollupBranches(SourceAst sourceAst, List<CockroachDBExpression> selected,
            List<CockroachDBExpression> groupByExpressions, CockroachDBExpression whereClause,
            SourceRelation relation) {
        List<String> branches = new ArrayList<>();
        for (int prefixLength = groupByExpressions.size() - 1; prefixLength >= 0; prefixLength--) {
            branches.add(createSyntheticRollupBranch(sourceAst, selected, groupByExpressions, prefixLength, whereClause,
                    relation));
        }
        return branches.stream().map(branch -> "(" + branch + ")").collect(Collectors.joining(" UNION ALL "));
    }

    private String createSyntheticRollupBranch(SourceAst sourceAst, List<CockroachDBExpression> selected,
            List<CockroachDBExpression> groupByExpressions, int prefixLength, CockroachDBExpression whereClause,
            SourceRelation relation) {
        List<String> retainedGroupKeys = groupByExpressions.subList(0, prefixLength).stream()
                .map(CockroachDBVisitor::asString).collect(Collectors.toList());
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < selected.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(syntheticRollupExpression(selected.get(i), retainedGroupKeys));
        }
        sb.append(" FROM ");
        sb.append(relation.withAlias(quote(sourceAst.table.getName()), false));
        if (whereClause != null) {
            sb.append(" WHERE ");
            sb.append(CockroachDBVisitor.asString(whereClause));
        }
        if (!retainedGroupKeys.isEmpty()) {
            sb.append(" GROUP BY ");
            sb.append(String.join(", ", retainedGroupKeys));
        }
        return sb.toString();
    }

    private String syntheticRollupExpression(CockroachDBExpression expression, List<String> retainedGroupKeys) {
        String sql = CockroachDBVisitor.asString(expression);
        if (retainedGroupKeys.contains(sql)) {
            return sql;
        }
        if (isLiteralExpression(expression)) {
            return sql;
        }
        if (isAggregateExpression(expression)) {
            return sql;
        }
        return "NULL";
    }

    private boolean isAggregateExpression(CockroachDBExpression expression) {
        if (expression instanceof CockroachDBAggregate) {
            return true;
        }
        String sql = CockroachDBVisitor.asString(expression);
        String normalized = sql.stripLeading().toUpperCase(Locale.ROOT);
        return normalized.startsWith("COUNT(") || normalized.startsWith("COUNT_ROWS(") || normalized.startsWith("SUM(")
                || normalized.startsWith("SUM_INT(") || normalized.startsWith("AVG(") || normalized.startsWith("MIN(")
                || normalized.startsWith("MAX(") || normalized.startsWith("BIT_AND(")
                || normalized.startsWith("BIT_OR(") || normalized.startsWith("XOR_AGG(")
                || normalized.startsWith("BOOL_AND(") || normalized.startsWith("BOOL_OR(")
                || normalized.startsWith("STDDEV(") || normalized.startsWith("VARIANCE(")
                || normalized.startsWith("SQRDIFF(") || normalized.startsWith("STRING_AGG(")
                || normalized.startsWith("CONCAT_AGG(");
    }

    private VpQueryPair generateCorrelatedSubqueryPair(VpSchema vp, ReconstructionStrategy strategy,
            SourceAst sourceAst) throws Exception {
        if (strategy == ReconstructionStrategy.TEMP_TABLE) {
            throw new IgnoreMeException();
        }
        recordAdvancedFeature(AdvancedFeature.CORRELATED_SUBQUERY);
        String sourceAlias = "s";
        String subAlias = "s2";
        String selected = simpleColumnOrRowIdForSQL(sourceAst.expressionColumns);
        String outerColumn = qualifySelectedExpression(selected, sourceAlias);
        String predicateColumn = qualifySelectedExpression(simpleColumnOrRowIdForSQL(sourceAst.expressionColumns),
                subAlias);
        int correlatedVariant = state.getRandomly().getInteger(0, 3);
        String singleQuery = buildCorrelatedSubquery(correlatedVariant, outerColumn, predicateColumn, sourceAlias,
                subAlias, sourceTableRelation(vp, strategy));
        List<String> extraSetup = new ArrayList<>();
        SourceRelation relation = buildReconstructionRelation(vp, strategy, extraSetup);
        String splitQuery = buildCorrelatedSubquery(correlatedVariant, outerColumn, predicateColumn, sourceAlias,
                subAlias, relation);
        return new VpQueryPair(vp, strategy, singleQuery, splitQuery, extraSetup,
                "shape=" + vp.baseShape + ", split=" + vp.splitStrategy + ", reconstruction=" + strategy
                        + ", advanced=correlated-subquery",
                false, EnumSet.of(AdvancedFeature.CORRELATED_SUBQUERY), vp.optimizerProfile);
    }

    private String qualifySelectedExpression(String expression, String alias) {
        return expression.replace(quote(ROW_ID), alias + "." + quote(ROW_ID));
    }

    private String buildCorrelatedSubquery(int variant, String outerColumn, String predicateColumn, String sourceAlias,
            String subAlias, SourceRelation relation) {
        switch (variant) {
        case 0:
            return "SELECT " + outerColumn + " FROM " + relation.withAlias(sourceAlias)
                    + " WHERE EXISTS (SELECT 1 FROM " + relation.withAlias(subAlias) + " WHERE " + subAlias + "."
                    + quote(ROW_ID) + " = " + sourceAlias + "." + quote(ROW_ID) + " AND " + predicateColumn
                    + " IS NOT NULL)";
        case 1:
            return "SELECT " + outerColumn + " FROM " + relation.withAlias(sourceAlias) + " WHERE " + sourceAlias + "."
                    + quote(ROW_ID) + " IN (SELECT " + subAlias + "." + quote(ROW_ID) + " FROM "
                    + relation.withAlias(subAlias) + " WHERE " + predicateColumn + " IS NOT NULL)";
        case 2:
            return "SELECT " + outerColumn + ", (SELECT MIN(" + predicateColumn + ") FROM "
                    + relation.withAlias(subAlias) + " WHERE " + subAlias + "." + quote(ROW_ID) + " <= " + sourceAlias
                    + "." + quote(ROW_ID) + ") FROM " + relation.withAlias(sourceAlias);
        default:
            throw new AssertionError(variant);
        }
    }

    private boolean isLiteralExpression(CockroachDBExpression expression) {
        return expression instanceof CockroachDBConstant;
    }

    private CockroachDBExpression freezeExpression(CockroachDBExpression expression) {
        if (isLiteralExpression(expression)) {
            return expression;
        }
        if (expression instanceof CockroachDBColumnReference) {
            return expression;
        }
        if (expression instanceof CockroachDBText) {
            return expression;
        }
        return new CockroachDBText(CockroachDBVisitor.asString(expression));
    }

    private List<CockroachDBExpression> freezeExpressions(List<CockroachDBExpression> expressions) {
        return expressions.stream().map(this::freezeExpression).collect(Collectors.toCollection(ArrayList::new));
    }

    private List<CockroachDBExpression> buildGroupedFetchColumns(List<CockroachDBExpression> groupByExpressions,
            List<CockroachDBExpression> aggregateExpressions, List<CockroachDBExpression> literalExpressions) {
        List<CockroachDBExpression> grouped = new ArrayList<>();
        grouped.addAll(groupByExpressions);
        grouped.addAll(freezeExpressions(aggregateExpressions));
        grouped.addAll(literalExpressions);
        return grouped;
    }

    private CockroachDBExpression randomExpression(CockroachDBExpressionGenerator gen,
            List<CockroachDBColumn> columns) {
        // Bias generation toward real source columns. The generator may still
        // produce constants and expressions of unsupported types, preserving
        // type/expression diversity, but a standalone random expression no
        // longer systematically degenerates to a literal when no matching
        // column type exists.
        if (!columns.isEmpty() && Randomly.getBoolean()) {
            return new CockroachDBColumnReference(Randomly.fromList(columns));
        }
        return gen.generateExpression(CockroachDBCompositeDataType.getRandom());
    }

    private CockroachDBExpression maybeGenerateCapabilityPredicate(SourceAst sourceAst,
            Set<AdvancedFeature> queryFeatures) {
        if (!advancedFeaturesEnabled() || Randomly.getBooleanWithRatherLowProbability()) {
            return null;
        }
        List<CockroachDBText> predicates = new ArrayList<>();
        List<AdvancedFeature> features = new ArrayList<>();
        CockroachDBText collationPredicate = collatedColumnPredicate(sourceAst);
        if (collationPredicate != null) {
            predicates.add(collationPredicate);
            features.add(AdvancedFeature.COLLATION_PREDICATE);
        }
        CockroachDBText jsonbPredicate = jsonbPredicate(sourceAst);
        if (jsonbPredicate != null) {
            predicates.add(jsonbPredicate);
            features.add(AdvancedFeature.JSONB_PREDICATE);
        }
        CockroachDBText arrayPredicate = arrayPredicate(sourceAst);
        if (arrayPredicate != null) {
            predicates.add(arrayPredicate);
            features.add(AdvancedFeature.ARRAY_PREDICATE);
        }
        CockroachDBText enumPredicate = enumPredicate(sourceAst);
        if (enumPredicate != null) {
            predicates.add(enumPredicate);
            features.add(AdvancedFeature.ENUM);
        }
        if (predicates.isEmpty()) {
            return null;
        }
        int index = state.getRandomly().getInteger(0, predicates.size());
        AdvancedFeature feature = features.get(index);
        recordAdvancedFeature(feature);
        queryFeatures.add(feature);
        return predicates.get(index);
    }

    private CockroachDBText enumPredicate(SourceAst sourceAst) {
        List<CockroachDBColumn> enumColumns = sourceAst.columns.stream()
                .filter(c -> c.getType().getPrimitiveDataType() == CockroachDBDataType.ENUM)
                .collect(Collectors.toList());
        if (enumColumns.isEmpty()) {
            return null;
        }
        CockroachDBColumn selected = Randomly.fromList(enumColumns);
        String column = quote(selected.getName());
        String label = Randomly.fromList(selected.getType().getEnumLabels()).replace("'", "''");
        return new CockroachDBText(column + Randomly.fromOptions(" = ", " < ", " >= ", " IS NOT DISTINCT FROM ")
                + "'" + label + "'::" + toSQLType(selected.getType()));
    }

    private CockroachDBExpression andPredicates(CockroachDBExpression left, CockroachDBExpression right) {
        if (left == null) {
            return right;
        }
        return new CockroachDBText(
                "(" + CockroachDBVisitor.asString(left) + ") AND (" + CockroachDBVisitor.asString(right) + ")");
    }

    private CockroachDBText collatedColumnPredicate(SourceAst sourceAst) {
        if (sourceAst.columnCollationLocales.isEmpty()) {
            return null;
        }
        List<Map.Entry<String, String>> entries = new ArrayList<>(sourceAst.columnCollationLocales.entrySet());
        Map.Entry<String, String> entry = Randomly.fromList(entries);
        String column = quote(entry.getKey());
        String locale = entry.getValue();
        String plainColumn = "(" + column + ")::STRING";
        List<String> sameLocaleColumns = entries.stream().filter(e -> e.getValue().equals(locale))
                .map(e -> quote(e.getKey())).collect(Collectors.toList());
        String peer = Randomly.fromList(sameLocaleColumns);
        return new CockroachDBText(
                Randomly.fromOptions("(" + column + " IS NULL OR " + column + " >= ('' COLLATE " + locale + "))",
                        "(" + plainColumn + " LIKE 'a%')",
                        "(" + plainColumn + " ILIKE '%a%')",
                        "(" + plainColumn + " ~ '^.*$')",
                        "(" + plainColumn + " ~* '^a.*$')",
                        "(" + column + " IS NULL OR " + peer + " IS NULL OR " + column + " = " + peer + ")"));
    }

    private CockroachDBExpression generateCollatedSelectExpression(SourceAst sourceAst, boolean aggregate) {
        List<Map.Entry<String, String>> entries = new ArrayList<>(sourceAst.columnCollationLocales.entrySet());
        Map.Entry<String, String> entry = Randomly.fromList(entries);
        String column = quote(entry.getKey());
        String locale = entry.getValue();
        List<String> sameLocaleColumns = entries.stream().filter(e -> e.getValue().equals(locale))
                .map(e -> quote(e.getKey())).collect(Collectors.toList());
        String peer = Randomly.fromList(sameLocaleColumns);
        recordAdvancedFeature(AdvancedFeature.COLLATION_PREDICATE);
        if (aggregate) {
            return new CockroachDBText(Randomly.fromOptions("MIN(" + column + ")", "MAX(" + column + ")",
                    "COUNT(" + column + ")", "STRING_AGG((" + column + ")::STRING, ',' ORDER BY " + column + ")"));
        }
        return new CockroachDBText(Randomly.fromOptions(column, "(" + column + " = " + peer + ")",
                "((" + column + ")::STRING LIKE 'a%')", "upper((" + column + ")::STRING)",
                "((" + column + ")::STRING ILIKE '%a%')",
                "((" + column + ")::STRING ~ '^.*$')",
                "concat((" + column + ")::STRING, (" + peer + ")::STRING)"));
    }

    private CockroachDBText jsonbPredicate(SourceAst sourceAst) {
        List<CockroachDBColumn> jsonbColumns = sourceAst.columns.stream()
                .filter(c -> c.getType().getPrimitiveDataType() == CockroachDBDataType.JSONB)
                .collect(Collectors.toList());
        if (jsonbColumns.isEmpty()) {
            return null;
        }
        String column = quote(Randomly.fromList(jsonbColumns).getName());
        return new CockroachDBText(Randomly.fromOptions("(" + column + " IS NULL OR " + column + " @> '{}'::JSONB)",
                "(" + column + " IS NULL OR '{}'::JSONB <@ " + column + ")",
                "(" + column + " IS NULL OR " + column + " ? 'k')",
                "(" + column + " IS NULL OR " + column + " ?| ARRAY['k', 'vp_missing_key'])",
                "(" + column + " IS NULL OR " + column + " ?& ARRAY['k'])",
                "(jsonb_typeof(" + column + ") IS NOT NULL)"));
    }

    private CockroachDBText arrayPredicate(SourceAst sourceAst) {
        List<CockroachDBColumn> arrayColumns = sourceAst.columns.stream()
                .filter(c -> c.getType().getPrimitiveDataType() == CockroachDBDataType.ARRAY)
                .filter(c -> isArrayContainmentType(c.getType().getElementType())).collect(Collectors.toList());
        if (arrayColumns.isEmpty()) {
            return null;
        }
        CockroachDBColumn column = Randomly.fromList(arrayColumns);
        String columnSql = quote(column.getName());
        String emptyArray = "ARRAY[]::" + arraySQLType(column.getType());
        return new CockroachDBText(
                Randomly.fromOptions("(" + columnSql + " IS NULL OR " + columnSql + " @> " + emptyArray + ")",
                        "(" + columnSql + " IS NULL OR " + emptyArray + " <@ " + columnSql + ")",
                        "(" + columnSql + " IS NULL OR " + columnSql + " && " + emptyArray + ")",
                        "(" + columnSql + " IS NULL OR array_length(" + columnSql + ", 1) IS NULL OR array_length("
                                + columnSql + ", 1) >= 0)"));
    }

    private boolean isArrayContainmentType(CockroachDBCompositeDataType elementType) {
        if (elementType == null) {
            return false;
        }
        switch (elementType.getPrimitiveDataType()) {
        case BIT:
        case VARBIT:
        case ARRAY:
        case JSONB:
            return false;
        default:
            return true;
        }
    }

    private String arraySQLType(CockroachDBCompositeDataType arrayType) {
        return stableSQLType(arrayType.getElementType()) + "[]";
    }

    private String stableSQLType(CockroachDBCompositeDataType type) {
        switch (type.getPrimitiveDataType()) {
        case INT:
        case SERIAL:
            return "INT8";
        case BOOL:
            return "BOOL";
        case STRING:
            return "STRING";
        case FLOAT:
            return "FLOAT8";
        case DECIMAL:
            return "DECIMAL";
        case BYTES:
            return "BYTES";
        case INTERVAL:
            return "INTERVAL";
        case TIMESTAMP:
            return "TIMESTAMP";
        case TIMESTAMPTZ:
            return "TIMESTAMPTZ";
        case TIME:
            return "TIME";
        case TIMETZ:
            return "TIMETZ";
        default:
            return type.toString();
        }
    }

    private List<CockroachDBExpression> chooseGroupByExpressions(List<CockroachDBExpression> nonAggregateExpressions) {
        List<CockroachDBExpression> groupable = nonAggregateExpressions.stream().filter(e -> !isLiteralExpression(e))
                .collect(Collectors.toList());
        if (!advancedFeaturesEnabled() || groupable.size() <= 1) {
            return groupable;
        }
        shuffleWithSqlancerRandom(groupable);
        return groupable.subList(0, state.getRandomly().getInteger(1, Math.min(3, groupable.size()) + 1));
    }

    private List<CockroachDBExpression> chooseLegalOrderBys(CockroachDBSelect select,
            List<CockroachDBExpression> nonAggregateExpressions, CockroachDBColumn rowIdColumn,
            boolean rowPreservingQuery) {
        if (!select.getGroupByExpressions().isEmpty()) {
            return stableGroupByOrderBys(select);
        }
        if (select.isDistinct()) {
            return stableSelectedOrderBys(select);
        }
        if (rowPreservingQuery) {
            List<CockroachDBExpression> orderBys = nonAggregateExpressions.stream().filter(e -> !isLiteralExpression(e))
                    .collect(Collectors.toCollection(ArrayList::new));
            if (orderBys.isEmpty()) {
                orderBys.add(new CockroachDBColumnReference(rowIdColumn));
            }
            appendVpRowIdTieBreaker(orderBys, rowIdColumn);
            return orderBys;
        }
        return List.of();
    }

    private List<CockroachDBExpression> chooseStableLimitOrderBys(CockroachDBSelect select,
            CockroachDBColumn rowIdColumn, boolean rowPreservingQuery, boolean useAggregate) {
        if (isSingleRowAggregate(useAggregate, select)) {
            return List.of();
        }
        if (!select.getGroupByExpressions().isEmpty()) {
            return stableGroupByOrderBys(select);
        }
        if (select.isDistinct()) {
            return stableSelectedOrderBys(select);
        }
        if (rowPreservingQuery) {
            List<CockroachDBExpression> orderBys = new ArrayList<>(select.getOrderByClauses());
            if (orderBys.isEmpty()) {
                orderBys.add(new CockroachDBColumnReference(rowIdColumn));
            }
            appendVpRowIdTieBreaker(orderBys, rowIdColumn);
            return orderBys;
        }
        return List.of();
    }

    private List<CockroachDBExpression> stableSelectedOrderBys(CockroachDBSelect select) {
        List<CockroachDBExpression> orderBys = new ArrayList<>();
        List<CockroachDBExpression> fetchColumns = select.getFetchColumns();
        for (int i = 0; i < fetchColumns.size(); i++) {
            orderBys.add(new CockroachDBText(Integer.toString(i + 1)));
        }
        return orderBys;
    }

    private List<CockroachDBExpression> stableGroupByOrderBys(CockroachDBSelect select) {
        return select.getGroupByExpressions().stream().filter(e -> !isLiteralExpression(e))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private void appendVpRowIdTieBreaker(List<CockroachDBExpression> orderBys, CockroachDBColumn rowIdColumn) {
        if (!hasVpRowIdTieBreaker(orderBys, rowIdColumn)) {
            orderBys.add(new CockroachDBColumnReference(rowIdColumn));
        }
    }

    private CockroachDBExpression generateGroupByHavingExpression(List<CockroachDBExpression> groupByExpressions,
            List<CockroachDBColumn> columns) {
        List<String> predicates = new ArrayList<>();
        predicates.add("COUNT(*) > 0");
        predicates.add("SUM(" + numericExpressionForSQL(columns) + ") IS NOT NULL");
        String minMaxInput = minMaxInputForSQL(columns);
        predicates.add("MIN(" + minMaxInput + ") IS NOT DISTINCT FROM MAX(" + minMaxInput + ")");
        for (CockroachDBExpression groupByExpression : groupByExpressions) {
            predicates.add("(" + CockroachDBVisitor.asString(groupByExpression) + ") IS NOT NULL");
        }
        return new CockroachDBText(Randomly.fromList(predicates));
    }

    private CockroachDBExpression generateAdvancedSelectExpression(SourceAst sourceAst,
            CockroachDBExpressionGenerator gen) {
        String order = simpleOrderExpressionForSQL(sourceAst.columns) + ", " + quote(ROW_ID);
        String value = windowValueExpressionForSQL(sourceAst.expressionColumns);
        String numeric = numericExpressionForSQL(sourceAst.columns);
        return new CockroachDBText(Randomly.fromOptions("ROW_NUMBER() OVER (ORDER BY " + order + ")",
                "RANK() OVER (ORDER BY " + order + ")", "LAG(" + value + ") OVER (ORDER BY " + quote(ROW_ID) + ")",
                "SUM(" + numeric + ") OVER (ORDER BY " + quote(ROW_ID) + ")"));
    }

    private String createUnionAllBranch(SourceAst sourceAst, List<CockroachDBExpression> selected,
            CockroachDBSelect leftSelect, SourceRelation relation, boolean unionAllCopiesWhere) {
        CockroachDBSelect right = new CockroachDBSelect();
        right.setDistinct(false);
        right.setFetchColumns(selected);
        right.setFromList(List.of(new CockroachDBText(relation.withAlias(quote(sourceAst.table.getName()), false))));
        if (unionAllCopiesWhere) {
            right.setWhereClause(leftSelect.getWhereClause());
        }
        return CockroachDBVisitor.asString(right);
    }

    private boolean isRowPreservingQuery(boolean useAggregate, CockroachDBSelect select) {
        return !useAggregate && select.getGroupByExpressions().isEmpty() && !select.isDistinct();
    }

    private boolean isSingleRowAggregate(boolean useAggregate, CockroachDBSelect select) {
        return useAggregate && select.getGroupByExpressions().isEmpty();
    }

    private boolean hasVpRowIdTieBreaker(CockroachDBSelect select, CockroachDBColumn rowIdColumn) {
        return hasVpRowIdTieBreaker(select.getOrderByClauses(), rowIdColumn);
    }

    private boolean hasVpRowIdTieBreaker(List<CockroachDBExpression> orderBys, CockroachDBColumn rowIdColumn) {
        if (orderBys.isEmpty()) {
            return false;
        }
        CockroachDBExpression last = orderBys.get(orderBys.size() - 1);
        return last instanceof CockroachDBColumnReference
                && ((CockroachDBColumnReference) last).getColumn().getName().equals(rowIdColumn.getName());
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

    private static void recordPairGenerated(VpQueryPair pair) {
        synchronized (STATS_LOCK) {
            incrementPairStage(pair.reconstructionStrategy, PairStage.GENERATED);
            for (AdvancedFeature feature : pair.queryFeatures) {
                QUERY_FEATURE_COUNTS.computeIfAbsent(feature, ignored -> new FeatureCoverage()).generated++;
            }
        }
    }

    private static void recordPairStage(VpQueryPair pair, PairStage stage) {
        synchronized (STATS_LOCK) {
            incrementPairStage(pair.reconstructionStrategy, stage);
        }
    }

    private static void incrementPairStage(ReconstructionStrategy strategy, PairStage stage) {
        EnumMap<PairStage, Integer> stages = PAIR_STAGE_COUNTS.computeIfAbsent(strategy,
                ignored -> new EnumMap<>(PairStage.class));
        increment(stages, stage);
    }

    private static void recordComparedFeatures(VpQueryPair pair) {
        synchronized (STATS_LOCK) {
            for (AdvancedFeature feature : pair.queryFeatures) {
                QUERY_FEATURE_COUNTS.computeIfAbsent(feature, ignored -> new FeatureCoverage()).compared++;
            }
        }
    }

    private static void recordSourceRowCount(long rowCount) {
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
            incrementString(SOURCE_ROW_COUNT_BUCKETS, bucket);
        }
        markVpStatsDirty();
    }

    private void incrementSelectedBaseShape(BaseShape value) {
        synchronized (STATS_LOCK) {
            increment(SELECTED_BASE_SHAPE_COUNTS, value);
        }
    }

    private static <E extends Enum<E>> void increment(Map<E, Integer> counts, E value) {
        counts.put(value, counts.getOrDefault(value, 0) + 1);
    }

    private static void incrementString(Map<String, Integer> counts, String value) {
        counts.put(value, counts.getOrDefault(value, 0) + 1);
    }

    private static void incrementSchemaShape(String key, int value) {
        SCHEMA_SHAPE_COUNTS.put(key, SCHEMA_SHAPE_COUNTS.getOrDefault(key, 0) + value);
    }

    public static void recordSkippedError(String stage, String sql, String error) {
        registerVpStatsShutdownHook();
        String normalized = normalizeSkippedError(error);
        String normalizedStage = normalizeSkippedStage(stage);
        SkippedErrorCategory category = classifySkippedError(normalizedStage, error);
        synchronized (STATS_LOCK) {
            SKIPPED_ERROR_COUNTS.put(normalized, SKIPPED_ERROR_COUNTS.getOrDefault(normalized, 0) + 1);
            incrementString(SKIPPED_STAGE_COUNTS, normalizedStage);
            increment(SKIPPED_CATEGORY_COUNTS, category);
            List<SkippedErrorSample> samples = SKIPPED_ERROR_SAMPLES.get(normalized);
            boolean prioritySample = isPrioritySkippedError(normalized);
            if (samples == null && (prioritySample || SKIPPED_ERROR_SAMPLES.size() < MAX_SKIPPED_ERROR_SAMPLE_GROUPS)) {
                samples = new ArrayList<>();
                SKIPPED_ERROR_SAMPLES.put(normalized, samples);
            }
            int sampleLimit = prioritySample ? MAX_PRIORITY_SKIPPED_ERROR_SAMPLES_PER_GROUP
                    : MAX_SKIPPED_ERROR_SAMPLES_PER_GROUP;
            if (samples != null && samples.size() < sampleLimit) {
                samples.add(new SkippedErrorSample(normalizedStage, normalizeSkippedSql(sql)));
            }
        }
        markVpStatsDirty();
    }

    private static SkippedErrorCategory classifySkippedError(String stage, String error) {
        String message = error == null ? "" : error.toLowerCase(Locale.ROOT);
        if (isResourceLimitError(message)) {
            return SkippedErrorCategory.RESOURCE_LIMIT;
        }
        if (isInternalDatabaseError(message)) {
            return SkippedErrorCategory.INTERNAL_DATABASE_ERROR;
        }
        if (stage.startsWith("cluster-transient-") || isTransientClusterError(message)) {
            return SkippedErrorCategory.CLUSTER_TRANSIENT;
        }
        if ("plan-fingerprint".equals(stage) || "distributed-plan".equals(stage)) {
            return SkippedErrorCategory.OBSERVABILITY;
        }
        if ("schema-backpressure".equals(stage)) {
            return SkippedErrorCategory.RESOURCE_LIMIT;
        }
        if ("setup".equals(stage) || "extra-setup".equals(stage) || "action".equals(stage)) {
            return SkippedErrorCategory.EXPECTED_GENERATION_OR_SETUP;
        }
        if ("cleanup".equals(stage)) {
            return SkippedErrorCategory.SETUP_OR_CLEANUP;
        }
        if ("single-query".equals(stage) || "split-query".equals(stage)) {
            return SkippedErrorCategory.EXPECTED_QUERY_REJECTION;
        }
        return SkippedErrorCategory.OTHER;
    }

    private static boolean isResourceLimitError(String message) {
        return message.contains("maximum number of concurrently open transactions")
                || message.contains("too many open transactions") || message.contains("no license")
                || message.contains("insufficient remaining capacity to ingest data")
                || message.contains("statement timeout") || message.contains("canceling statement due to user request")
                || message.contains("query execution canceled") || message.contains("query execution cancelled");
    }

    private static boolean isTransientClusterError(String message) {
        return message.contains("replica unavailable") || message.contains("replica has been leaderless")
                || message.contains("leaderless for") || message.contains("closed timestamp")
                || message.contains("slow proposal") || message.contains("txn exec: context canceled")
                || message.contains("txn exec: context cancelled") || message.contains("node liveness error")
                || message.contains("must be after replica gc threshold")
                || message.contains("i/o error occurred while sending to the backend")
                || message.contains("eofexception") || message.contains("connection reset")
                || message.contains("connection closed") || message.contains("terminating connection")
                || message.contains("result is ambiguous: context deadline exceeded");
    }

    private static boolean isInternalDatabaseError(String message) {
        return message.contains("internal error") || message.contains("panic:") || message.contains("assertion failed")
                || message.contains("unexpected error from the vectorized runtime");
    }

    private static boolean isPrioritySkippedError(String normalizedError) {
        return DISTINCT_ORDER_BY_NOT_IN_SELECT_LIST_ERROR.equals(normalizedError);
    }

    private static String normalizeSkippedStage(String stage) {
        return stage == null || stage.isBlank() ? "unknown" : stage.replaceAll("\\s+", "-").trim();
    }

    private static String normalizeSkippedSql(String sql) {
        if (sql == null || sql.isBlank()) {
            return "<none>";
        }
        String normalized = sql.replaceAll("\\s+", " ").trim();
        return normalized.length() > 500 ? normalized.substring(0, 497) + "..." : normalized;
    }

    private static String normalizeSkippedError(String error) {
        if (error == null || error.isBlank()) {
            return "<empty>";
        }
        String normalized = error.replaceAll("\\s+", " ").trim();
        if (normalized.startsWith("ERROR: ")) {
            normalized = normalized.substring("ERROR: ".length()).trim();
        }
        int detail = normalized.indexOf(" Detail:");
        if (detail >= 0) {
            normalized = normalized.substring(0, detail).trim();
        }
        int hint = normalized.indexOf(" Hint:");
        if (hint >= 0) {
            normalized = normalized.substring(0, hint).trim();
        }
        int where = normalized.indexOf(" Where:");
        if (where >= 0) {
            normalized = normalized.substring(0, where).trim();
        }
        return normalized.length() > 240 ? normalized.substring(0, 237) + "..." : normalized;
    }

    private static String statsQuote(String value) {
        return "\"" + statsSafe(value).replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static String statsSafe(String value) {
        if (value == null) {
            return "<null>";
        }
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (Character.isHighSurrogate(ch)) {
                if (i + 1 < value.length() && Character.isLowSurrogate(value.charAt(i + 1))) {
                    sb.append(ch);
                    sb.append(value.charAt(++i));
                } else {
                    appendUnicodeEscape(sb, ch);
                }
            } else if (Character.isLowSurrogate(ch)) {
                appendUnicodeEscape(sb, ch);
            } else if (ch == '\n') {
                sb.append("\\n");
            } else if (ch == '\r') {
                sb.append("\\r");
            } else if (ch == '\t') {
                sb.append("\\t");
            } else if (Character.isISOControl(ch)) {
                appendUnicodeEscape(sb, ch);
            } else {
                sb.append(ch);
            }
        }
        return sb.toString();
    }

    private static void appendUnicodeEscape(StringBuilder sb, char ch) {
        sb.append("\\u");
        String hex = Integer.toHexString(ch);
        for (int i = hex.length(); i < 4; i++) {
            sb.append('0');
        }
        sb.append(hex);
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

    private static void recordKnownBugSuppressed(KnownVpBug knownBug) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            increment(KNOWN_BUG_COUNTS, knownBug);
        }
        markVpStatsDirty();
    }

    public static void recordAdvancedFeature(AdvancedFeature feature) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            increment(ADVANCED_FEATURE_COUNTS, feature);
        }
        markVpStatsDirty();
    }

    private static String formatVpStats(String event) {
        StringBuilder sb = new StringBuilder();
        sb.append("-- [vp-stats] event=").append(event).append(" dbms=cockroachdb");
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
        sb.append("-- [vp-stats] reconstruction-stage-counts ").append(formatPairStageCounts());
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] known-suppressed-counts ")
                .append(formatEnumCounts(KnownVpBug.values(), KNOWN_BUG_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] advanced-feature-counts ")
                .append(formatEnumCounts(AdvancedFeature.values(), ADVANCED_FEATURE_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] query-feature-counts ").append(formatFeatureCoverage());
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] source-row-count-buckets ")
                .append(formatStringCounts(SOURCE_ROW_COUNT_BUCKETS, SOURCE_ROW_COUNT_BUCKETS.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] skipped-stage-counts ")
                .append(formatStringCounts(SKIPPED_STAGE_COUNTS, SKIPPED_STAGE_COUNTS.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] skipped-category-counts ")
                .append(formatEnumCounts(SkippedErrorCategory.values(), SKIPPED_CATEGORY_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] optimizer-profile-counts ")
                .append(formatStringCounts(OPTIMIZER_PROFILE_COUNTS, OPTIMIZER_PROFILE_COUNTS.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] read-committed-probe-counts ")
                .append(formatStringCounts(READ_COMMITTED_PROBE_COUNTS, READ_COMMITTED_PROBE_COUNTS.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] time-travel-profile-counts ")
                .append(formatStringCounts(TIME_TRAVEL_PROFILE_COUNTS, TIME_TRAVEL_PROFILE_COUNTS.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] distributed-profile-counts ")
                .append(formatStringCounts(DISTRIBUTED_PROFILE_COUNTS, DISTRIBUTED_PROFILE_COUNTS.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] plan-shape-counts ")
                .append(formatStringCounts(PLAN_SHAPE_COUNTS, PLAN_SHAPE_COUNTS.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] plan-fingerprints ")
                .append(formatStringCounts(PLAN_FINGERPRINT_COUNTS, MAX_PLAN_FINGERPRINTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] skipped-error-counts ")
                .append(formatStringCounts(SKIPPED_ERROR_COUNTS, MAX_SKIPPED_ERROR_COUNT_GROUPS));
        sb.append(System.lineSeparator());
        if (SKIPPED_ERROR_COUNTS.size() > MAX_SKIPPED_ERROR_COUNT_GROUPS) {
            sb.append("-- [vp-stats] skipped-error-counts-truncated distinctErrors=");
            sb.append(SKIPPED_ERROR_COUNTS.size());
            sb.append(" shown=").append(MAX_SKIPPED_ERROR_COUNT_GROUPS);
            sb.append(System.lineSeparator());
        }
        for (Map.Entry<String, List<SkippedErrorSample>> entry : prioritizedSkippedErrorSamples()) {
            for (SkippedErrorSample sample : entry.getValue()) {
                sb.append("-- [vp-stats] skipped-error-sample error=").append(statsQuote(entry.getKey()));
                sb.append(" stage=").append(sample.stage);
                sb.append(" sql=").append(statsQuote(sample.sql));
                sb.append(System.lineSeparator());
            }
        }
        if (SKIPPED_ERROR_COUNTS.size() > SKIPPED_ERROR_SAMPLES.size()) {
            sb.append("-- [vp-stats] skipped-error-samples-truncated distinctErrors=");
            sb.append(SKIPPED_ERROR_COUNTS.size());
            sb.append(" sampledErrors=").append(SKIPPED_ERROR_SAMPLES.size());
            sb.append(" maxSamplesPerError=").append(MAX_SKIPPED_ERROR_SAMPLES_PER_GROUP);
            sb.append(" priorityMaxSamplesPerError=").append(MAX_PRIORITY_SKIPPED_ERROR_SAMPLES_PER_GROUP);
            sb.append(System.lineSeparator());
        }
        return sb.toString();
    }

    private static List<Map.Entry<String, List<SkippedErrorSample>>> prioritizedSkippedErrorSamples() {
        List<Map.Entry<String, List<SkippedErrorSample>>> entries = new ArrayList<>(SKIPPED_ERROR_SAMPLES.entrySet());
        entries.sort((left, right) -> Boolean.compare(isPrioritySkippedError(right.getKey()),
                isPrioritySkippedError(left.getKey())));
        return entries;
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

    private static String formatPairStageCounts() {
        return java.util.Arrays.stream(ReconstructionStrategy.values()).map(strategy -> {
            Map<PairStage, Integer> stages = PAIR_STAGE_COUNTS.getOrDefault(strategy, new EnumMap<>(PairStage.class));
            return strategy.name() + "{" + formatEnumCounts(PairStage.values(), stages) + "}";
        }).collect(Collectors.joining(", "));
    }

    private static String formatFeatureCoverage() {
        if (QUERY_FEATURE_COUNTS.isEmpty()) {
            return "NONE=0";
        }
        return java.util.Arrays.stream(AdvancedFeature.values()).filter(QUERY_FEATURE_COUNTS::containsKey)
                .map(feature -> {
                    FeatureCoverage coverage = QUERY_FEATURE_COUNTS.get(feature);
                    return feature.name() + "{generated=" + coverage.generated + ",compared=" + coverage.compared + "}";
                }).collect(Collectors.joining(", "));
    }

    private static String formatStringCounts(Map<String, Integer> counts, int limit) {
        if (counts.isEmpty()) {
            return "NONE=0";
        }
        return counts.entrySet().stream().sorted((l, r) -> Integer.compare(r.getValue(), l.getValue())).limit(limit)
                .map(e -> quoteStatsKey(e.getKey()) + "=" + e.getValue()).collect(Collectors.joining(", "));
    }

    private static String quoteStatsKey(String key) {
        return statsQuote(key);
    }

    public static void logSchemaShapeStats(CockroachDBGlobalState state, String phase) throws Exception {
        registerVpStatsShutdownHook();
        int emptySingle = 0;
        int emptyMulti = 0;
        int nonEmptySingle = 0;
        int nonEmptyMulti = 0;
        try {
            state.updateSchema();
        } catch (SQLException e) {
            String errorText = getStaticErrorText(e);
            if (isStaticTransientClusterAvailabilityError(errorText)) {
                for (int attempt = 0; attempt < RESOURCE_READ_RETRIES; attempt++) {
                    try {
                        sleepStaticTransientClusterRecovery(attempt);
                        state.updateSchema();
                        break;
                    } catch (SQLException retryError) {
                        errorText = getStaticErrorText(retryError);
                        if (!isStaticTransientClusterAvailabilityError(errorText)) {
                            throw retryError;
                        }
                        if (attempt + 1 == RESOURCE_READ_RETRIES) {
                            recordSkippedError("cluster-transient-schema-shape", "UPDATE SCHEMA", errorText);
                            return;
                        }
                    }
                }
            } else {
                throw e;
            }
        }
        for (CockroachDBTable table : state.getSchema().getDatabaseTables()) {
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
        markVpStatsDirty();
    }

    private static boolean isStaticTransientClusterAvailabilityError(String errorText) {
        String message = errorText.toLowerCase(Locale.ROOT);
        return message.contains("replica unavailable") || message.contains("replica has been leaderless")
                || message.contains("leaderless for") || message.contains("closed timestamp")
                || message.contains("slow proposal") || message.contains("txn exec: context canceled")
                || message.contains("txn exec: context cancelled") || message.contains("node liveness error")
                || message.contains("maximum number of concurrently open transactions")
                || message.contains("too many open transactions");
    }

    private static void sleepStaticTransientClusterRecovery(int attempt) throws IgnoreMeException {
        try {
            Thread.sleep(250L * (attempt + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IgnoreMeException();
        }
    }

    private static String getStaticErrorText(Throwable throwable) {
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

    private static void registerVpStatsShutdownHook() {
        if (SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(CockroachDBVerticalPartitioningOracle::writeFinalVpStats));
        }
    }

    private static void writeFinalVpStats() {
        writeVpStats("process-final");
    }

    private static void markVpStatsDirty() {
        long dirtyEvents = STATS_DIRTY_EVENTS.incrementAndGet();
        if (dirtyEvents == 1 || dirtyEvents % STATS_FLUSH_INTERVAL == 0) {
            writeVpStatsSnapshot();
        }
    }

    private static void writeVpStatsSnapshot() {
        writeVpStats("process-checkpoint");
    }

    private static void writeVpStats(String event) {
        String log;
        synchronized (STATS_LOCK) {
            log = formatVpStats(event);
        }
        File parent = STATS_FILE.getParentFile();
        Path target = STATS_FILE.toPath();
        synchronized (STATS_WRITE_LOCK) {
            Path temporary = null;
            try {
                if (parent != null) {
                    Files.createDirectories(parent.toPath());
                }
                temporary = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
                Files.write(temporary, log.getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING);
                try {
                    Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                    Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException e) {
                System.err.println("Could not write CockroachDB VP stats: " + e.getMessage());
            } finally {
                if (temporary != null) {
                    try {
                        Files.deleteIfExists(temporary);
                    } catch (IOException ignored) {
                        // A later run can safely remove an abandoned temporary snapshot.
                    }
                }
            }
        }
    }

    private CockroachDBExpression generateVpAggregate(List<CockroachDBColumn> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            return new CockroachDBText(randomAdvancedAggregateSQL(columns));
        }
        CockroachDBAggregateFunction func = Randomly.fromOptions(CockroachDBAggregateFunction.values());
        if (func == CockroachDBAggregateFunction.CONCAT_AGG) {
            return new CockroachDBText(
                    "CONCAT_AGG((" + groupConcatInputForSQL(columns) + ")::STRING ORDER BY " + quote(ROW_ID) + ")");
        }
        if (func == CockroachDBAggregateFunction.STRING_AGG) {
            return new CockroachDBText("STRING_AGG((" + groupConcatInputForSQL(columns) + ")::STRING, ',' ORDER BY "
                    + quote(ROW_ID) + ")");
        }
        String numeric = numericExpressionForSQL(columns);
        String statisticalNumeric = statisticalAggregateInputForSQL(columns);
        String integer = "CAST(" + numeric + " AS INT8)";
        switch (func) {
        case SUM:
            return new CockroachDBText("SUM(" + numeric + ")");
        case SUM_INT:
            return new CockroachDBText("SUM_INT(" + integer + ")");
        case AVG:
            return new CockroachDBText("AVG(" + numeric + ")");
        case SQRDIFF:
            return new CockroachDBText("SQRDIFF(" + statisticalNumeric + ")");
        case STDDEV:
            return new CockroachDBText("STDDEV(" + statisticalNumeric + ")");
        case VARIANCE:
            return new CockroachDBText("VARIANCE(" + statisticalNumeric + ")");
        case XOR_AGG:
            return new CockroachDBText("XOR_AGG(" + integer + ")");
        case BIT_AND:
            return new CockroachDBText("BIT_AND(" + integer + ")");
        case BIT_OR:
            return new CockroachDBText("BIT_OR(" + integer + ")");
        case BOOL_AND:
            return new CockroachDBText("BOOL_AND(" + booleanAggregateInputForSQL(columns) + ")");
        case BOOL_OR:
            return new CockroachDBText("BOOL_OR(" + booleanAggregateInputForSQL(columns) + ")");
        case COUNT:
            return new CockroachDBText("COUNT(" + simpleColumnOrRowIdForSQL(columns) + ")");
        case COUNT_ROWS:
            return new CockroachDBText("COUNT_ROWS()");
        case MIN:
            return new CockroachDBText("MIN(" + minMaxInputForSQL(columns) + ")");
        case MAX:
            return new CockroachDBText("MAX(" + minMaxInputForSQL(columns) + ")");
        default:
            throw new AssertionError(func);
        }
    }

    private String randomAdvancedAggregateSQL(List<CockroachDBColumn> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()) {
            recordAdvancedFeature(AdvancedFeature.GROUP_CONCAT);
            return "STRING_AGG((" + groupConcatInputForSQL(columns) + ")::STRING, ',' ORDER BY " + quote(ROW_ID) + ")";
        }
        String numeric = numericExpressionForSQL(columns);
        String statisticalNumeric = statisticalAggregateInputForSQL(columns);
        String integer = "CAST(" + numeric + " AS INT8)";
        return Randomly.fromOptions("AVG(" + numeric + ")", "BIT_AND(" + integer + ")", "BIT_OR(" + integer + ")",
                "XOR_AGG(" + integer + ")", "STDDEV(" + statisticalNumeric + ")",
                "VARIANCE(" + statisticalNumeric + ")");
    }

    private String groupConcatInputForSQL(List<CockroachDBColumn> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        CockroachDBColumn column = Randomly.fromList(columns);
        if (column.getType().getPrimitiveDataType() == CockroachDBDataType.STRING || Randomly.getBoolean()) {
            return "CAST(" + quote(column.getName()) + " AS STRING)";
        }
        return quote(column.getName());
    }

    private String numericExpressionForSQL(List<CockroachDBColumn> columns) {
        List<CockroachDBColumn> numericColumns = columns.stream().filter(c -> isNumericType(c.getType()))
                .collect(Collectors.toList());
        if (!numericColumns.isEmpty() && Randomly.getBoolean()) {
            return quote(Randomly.fromList(numericColumns).getName());
        }
        return quote(ROW_ID);
    }

    private String statisticalAggregateInputForSQL(List<CockroachDBColumn> columns) {
        List<CockroachDBColumn> numericColumns = columns.stream().filter(c -> isNumericType(c.getType()))
                .collect(Collectors.toList());
        if (numericColumns.isEmpty() || !Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        CockroachDBColumn column = Randomly.fromList(numericColumns);
        String expression = quote(column.getName());
        if (column.getType().getPrimitiveDataType() != CockroachDBDataType.FLOAT) {
            return expression;
        }
        // Floating-point moment aggregates are order-sensitive after overflow. Keep FLOAT coverage while excluding
        // values whose square cannot remain finite, so an equivalent reconstruction cannot differ only by reduction
        // order.
        return "CASE WHEN " + expression + " BETWEEN -1e150::FLOAT8 AND 1e150::FLOAT8 THEN " + expression
                + " ELSE NULL END";
    }

    private String simpleColumnOrRowIdForSQL(List<CockroachDBColumn> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        return quote(Randomly.fromList(columns).getName());
    }

    private String simpleOrderExpressionForSQL(List<CockroachDBColumn> columns) {
        if (columns.isEmpty()) {
            return quote(ROW_ID);
        }
        return simpleColumnOrRowIdForSQL(columns);
    }

    private String windowValueExpressionForSQL(List<CockroachDBColumn> columns) {
        List<CockroachDBColumn> windowCompatibleColumns = columns.stream().filter(column -> {
            switch (column.getType().getPrimitiveDataType()) {
            case INT:
            case SERIAL:
            case BOOL:
            case FLOAT:
            case DECIMAL:
            case TIMESTAMP:
            case TIMESTAMPTZ:
            case TIME:
            case TIMETZ:
            case INTERVAL:
                return true;
            default:
                return false;
            }
        }).collect(Collectors.toList());
        if (windowCompatibleColumns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        return quote(Randomly.fromList(windowCompatibleColumns).getName());
    }

    private String booleanAggregateInputForSQL(List<CockroachDBColumn> columns) {
        List<CockroachDBColumn> boolColumns = columns.stream()
                .filter(c -> c.getType().getPrimitiveDataType() == CockroachDBDataType.BOOL)
                .collect(Collectors.toList());
        if (!boolColumns.isEmpty() && Randomly.getBoolean()) {
            return quote(Randomly.fromList(boolColumns).getName());
        }
        return quote(ROW_ID) + " IS NOT NULL";
    }

    private String minMaxInputForSQL(List<CockroachDBColumn> columns) {
        List<CockroachDBColumn> supportedColumns = columns.stream().filter(c -> {
            switch (c.getType().getPrimitiveDataType()) {
            case ARRAY:
            case JSONB:
                return false;
            default:
                return true;
            }
        }).collect(Collectors.toList());
        if (!supportedColumns.isEmpty() && Randomly.getBoolean()) {
            return quote(Randomly.fromList(supportedColumns).getName());
        }
        return quote(ROW_ID);
    }

    private static final class SourceAst {
        private final CockroachDBTable table;
        private final CockroachDBColumn rowIdColumn;
        private final List<CockroachDBColumn> columns;
        private final List<CockroachDBColumn> expressionColumns;
        private final Map<String, String> columnCollationLocales;

        private SourceAst(CockroachDBTable table, CockroachDBColumn rowIdColumn, List<CockroachDBColumn> columns,
                List<CockroachDBColumn> expressionColumns, Map<String, String> columnCollationLocales) {
            this.table = table;
            this.rowIdColumn = rowIdColumn;
            this.columns = columns;
            this.expressionColumns = expressionColumns;
            this.columnCollationLocales = columnCollationLocales;
        }
    }

    private static final class SourceRelation {
        private final String relationBody;
        private final String timeTravelTimestamp;

        private SourceRelation(String relationBody) {
            this(relationBody, null);
        }

        private SourceRelation(String relationBody, String timeTravelTimestamp) {
            this.relationBody = relationBody;
            this.timeTravelTimestamp = timeTravelTimestamp;
        }

        private String withAlias(String alias) {
            return withAlias(alias, true);
        }

        private String withAlias(String alias, boolean includeTimeTravel) {
            String relation = relationBody + " AS " + alias;
            if (includeTimeTravel && timeTravelTimestamp != null) {
                relation += " AS OF SYSTEM TIME " + sessionVariableLiteral(timeTravelTimestamp);
            }
            return relation;
        }

        private boolean hasTimeTravelTimestamp() {
            return timeTravelTimestamp != null;
        }

    }

    private SourceAst createSourceAst(VpSchema vp) {
        List<CockroachDBColumn> astColumns = new ArrayList<>();
        List<CockroachDBColumn> expressionColumns = new ArrayList<>();
        Map<String, String> columnCollationLocales = new LinkedHashMap<>();
        CockroachDBColumn rowId = new CockroachDBColumn(ROW_ID, CockroachDBCompositeDataType.getInt(8), true, false);
        astColumns.add(rowId);
        expressionColumns.add(rowId);
        for (VpColumn column : vp.columns) {
            CockroachDBColumn astColumn = new CockroachDBColumn(column.name, column.type, false, true);
            astColumns.add(astColumn);
            // Keep collated STRING columns in the normal expression pool. Dedicated collation
            // expressions below bias the first projection toward same-locale operations, while
            // CockroachDB's expected-error filtering handles combinations that cross collations
            // or are not supported by a particular operator/function.
            String collationClause = vp.columnCollationClauses.getOrDefault(column.name, "");
            expressionColumns.add(astColumn);
            if (!collationClause.isEmpty()) {
                columnCollationLocales.put(column.name, collationClause.replace(" COLLATE ", "").trim());
            }
        }
        CockroachDBTable table = new CockroachDBTable(vp.sourceTable, astColumns, List.of(), false);
        for (CockroachDBColumn column : astColumns) {
            column.setTable(table);
        }
        return new SourceAst(table, rowId, astColumns, expressionColumns, columnCollationLocales);
    }

    private SourceRelation sourceTableRelation(VpSchema vp) {
        return new SourceRelation(quote(vp.sourceTable), vp.timeTravelTimestamp);
    }

    private SourceRelation sourceTableRelation(VpSchema vp, ReconstructionStrategy strategy) {
        if (strategy == ReconstructionStrategy.NOT_EXISTS_ANTI) {
            String rowId = quote(ROW_ID);
            return new SourceRelation("(SELECT s.* FROM " + quote(vp.sourceTable)
                    + " s WHERE NOT EXISTS (SELECT 1 FROM " + quote(vp.rightTable) + " rx WHERE rx." + rowId + " = s."
                    + rowId + " AND rx." + rowId + " % 2::INT8 = 0))", vp.timeTravelTimestamp);
        }
        if (strategy == ReconstructionStrategy.ZIGZAG_JOIN) {
            return new SourceRelation(
                    "(SELECT s.* FROM " + quote(vp.sourceTable) + " s WHERE " + zigzagSourcePredicate(vp, "s") + ")",
                    vp.timeTravelTimestamp);
        }
        return sourceTableRelation(vp);
    }

    private SourceRelation buildReconstructionRelation(VpSchema vp, ReconstructionStrategy strategy,
            List<String> extraSetup) throws Exception {
        switch (strategy) {
        case JOIN:
            return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false), " FROM " + quote(vp.leftTable)
                    + " l JOIN " + quote(vp.rightTable) + " r ON " + reconstructionJoinCondition(vp, "l", "r"));
        case LEFT_RIGHT_JOIN:
            if (Randomly.getBoolean()) {
                return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                        " FROM " + quote(vp.leftTable) + " l LEFT JOIN " + quote(vp.rightTable) + " r ON "
                                + reconstructionJoinCondition(vp, "l", "r"));
            }
            return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false), " FROM " + quote(vp.leftTable)
                    + " l RIGHT JOIN " + quote(vp.rightTable) + " r ON " + reconstructionJoinCondition(vp, "l", "r"));
        case FULL_OUTER_SIMULATION:
            return reconstructedSubquery(vp, fullOuterProjection(vp),
                    " FROM " + quote(vp.leftTable) + " l FULL OUTER JOIN " + quote(vp.rightTable) + " r ON "
                            + reconstructionJoinCondition(vp, "l", "r"));
        case OUTER_NULL_EXTENSION:
            return outerNullExtensionRelation(vp);
        case SCALAR_SUBQUERY:
            return scalarSubqueryRelation(vp);
        case EXISTS_FILTER:
            return reconstructedSubquery(vp, leftProjectionWithRightLookups(vp),
                    " FROM " + quote(vp.leftTable) + " l WHERE EXISTS (SELECT 1 FROM " + quote(vp.rightTable)
                            + " r WHERE " + reconstructionJoinCondition(vp, "l", "r") + ")");
        case IN_FILTER:
            if (vp.keyMode == KeyMode.PRIMARY_KEY) {
                return reconstructedSubquery(vp, leftProjectionWithRightLookups(vp),
                        " FROM " + quote(vp.leftTable) + " l WHERE EXISTS (SELECT 1 FROM " + quote(vp.rightTable)
                                + " r WHERE " + reconstructionJoinCondition(vp, "l", "r") + ")");
            }
            return reconstructedSubquery(vp, leftProjectionWithRightLookups(vp),
                    " FROM " + quote(vp.leftTable) + " l WHERE l." + quote(ROW_ID) + " IN (SELECT r." + quote(ROW_ID)
                            + " FROM " + quote(vp.rightTable) + " r)");
        case NOT_EXISTS_ANTI:
            // Drive from the left partition so the anti-join does not silently discard
            // rows whose right-side payload is absent in a sparse split.
            return reconstructedSubquery(vp, leftProjectionWithRightLookups(vp),
                    " FROM " + quote(vp.leftTable) + " l WHERE NOT EXISTS (SELECT 1 FROM " + quote(vp.rightTable)
                            + " rx WHERE " + reconstructionJoinCondition(vp, "l", "rx") + " AND rx." + quote(ROW_ID)
                            + " % 2::INT8 = 0)");
        case DERIVED_TABLE:
            return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                    " FROM (SELECT * FROM " + quote(vp.leftTable) + ") l JOIN (SELECT * FROM " + quote(vp.rightTable)
                            + ") r ON " + reconstructionJoinCondition(vp, "l", "r"));
        case CTE:
            return cteRelation(vp);
        case VIEW:
            if (vp.activeViewName == null) {
                vp.activeViewName = vp.prefix + "v_" + ORACLE_RUN_ID.incrementAndGet();
                vp.viewNames.add(vp.activeViewName);
            }
            String viewProjection = joinProjection(vp, "l", "r", false);
            String viewFrom;
            switch (state.getRandomly().getInteger(0, 4)) {
            case 0:
                viewFrom = " FROM " + quote(vp.leftTable) + " l JOIN " + quote(vp.rightTable) + " r ON "
                        + reconstructionJoinCondition(vp, "l", "r");
                break;
            case 1:
                viewFrom = " FROM (SELECT * FROM " + quote(vp.leftTable) + ") l JOIN (SELECT * FROM "
                        + quote(vp.rightTable) + ") r ON " + reconstructionJoinCondition(vp, "l", "r");
                break;
            case 2:
                viewFrom = " FROM " + quote(vp.leftTable) + " l JOIN LATERAL (SELECT * FROM " + quote(vp.rightTable)
                        + " rr WHERE " + reconstructionJoinCondition(vp, "l", "rr") + ") r ON TRUE";
                break;
            default:
                viewFrom = " FROM (WITH l0 AS (SELECT * FROM " + quote(vp.leftTable) + "), r0 AS (SELECT * FROM "
                        + quote(vp.rightTable) + ") SELECT " + joinProjection(vp, "l", "r", false)
                        + " FROM l0 l JOIN r0 r ON " + reconstructionJoinCondition(vp, "l", "r") + ") x";
                // The CTE branch already produces the projected row shape.
                viewProjection = "x.*";
                break;
            }
            extraSetup.add("DROP VIEW IF EXISTS " + quote(vp.activeViewName));
            extraSetup.add("CREATE VIEW " + quote(vp.activeViewName) + " AS SELECT " + viewProjection + viewFrom);
            return new SourceRelation(quote(vp.activeViewName), vp.timeTravelTimestamp);
        case TEMP_TABLE:
            if (!vp.tempTableMaterialized) {
                extraSetup.add("DROP TABLE IF EXISTS " + quote(vp.tempTable));
                extraSetup.add(createTemporaryReconstructionTableSQL(vp));
                extraSetup.add("INSERT INTO " + quote(vp.tempTable) + " (" + sourceColumnList(vp) + ") SELECT "
                        + joinProjection(vp, "l", "r", false) + " FROM " + quote(vp.leftTable) + " l JOIN "
                        + quote(vp.rightTable) + " r ON " + reconstructionJoinCondition(vp, "l", "r"));
            }
            return new SourceRelation(quote(vp.tempTable), vp.timeTravelTimestamp);
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
            return hintedJoinRelation(vp);
        case LATERAL_DERIVED:
            return lateralRelation(vp);
        case ZIGZAG_JOIN:
            return zigzagRelation(vp, extraSetup);
        default:
            throw new AssertionError(strategy);
        }
    }

    private SourceRelation reconstructedSubquery(VpSchema vp, String projection, String fromAndWhere) {
        return new SourceRelation("(SELECT " + projection + fromAndWhere + ")", vp.timeTravelTimestamp);
    }

    private String reconstructionJoinCondition(VpSchema vp, String leftAlias, String rightAlias) {
        if (vp.keyMode == KeyMode.VP_ROWID || vp.reconstructionKeyColumns.isEmpty()) {
            return keyEquality(vp, leftAlias, rightAlias, quote(ROW_ID));
        }
        return vp.reconstructionKeyColumns.stream().map(c -> keyEquality(vp, leftAlias, rightAlias, quote(c.name)))
                .collect(Collectors.joining(" AND "));
    }

    private String keyEquality(VpSchema vp, String leftAlias, String rightAlias, String quotedColumnName) {
        String operator = vp.keyMode == KeyMode.NULLABLE_UNIQUE ? " IS NOT DISTINCT FROM " : " = ";
        return leftAlias + "." + quotedColumnName + operator + rightAlias + "." + quotedColumnName;
    }

    private List<String> reconstructionKeyColumnNames(VpSchema vp) {
        if (vp.keyMode == KeyMode.VP_ROWID || vp.reconstructionKeyColumns.isEmpty()) {
            return List.of(quote(ROW_ID));
        }
        return vp.reconstructionKeyColumns.stream().map(c -> quote(c.name)).collect(Collectors.toList());
    }

    private String createTemporaryReconstructionTableSQL(VpSchema vp) {
        List<String> defs = new ArrayList<>();
        defs.add(quote(ROW_ID) + " INT8 NOT NULL PRIMARY KEY");
        for (VpColumn column : vp.columns) {
            // Materialized reconstruction tables must preserve generated/computed values from
            // the fragments. Defining those columns as generated again and omitting them from
            // INSERT loses values for generated base columns (e.g. constant VARBIT columns).
            defs.add(quote(column.name) + " " + toSQLType(column.type)
                    + vp.columnCollationClauses.getOrDefault(column.name, "") + " NULL");
        }
        return "CREATE TEMP TABLE " + quote(vp.tempTable) + " (" + String.join(", ", defs) + ")";
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
            parts.add(projectColumn(vp, column, leftAlias, rightAlias, false));
        }
        return String.join(", ", parts);
    }

    private String fullOuterProjection(VpSchema vp) {
        List<String> parts = new ArrayList<>();
        parts.add("COALESCE(l." + quote(ROW_ID) + ", r." + quote(ROW_ID) + ") AS " + quote(ROW_ID));
        for (VpColumn column : vp.columns) {
            boolean inLeft = containsColumn(vp.leftColumns, column.name);
            boolean inRight = containsColumn(vp.rightColumns, column.name);
            if (inLeft && inRight) {
                parts.add("COALESCE(l." + quote(column.name) + ", r." + quote(column.name) + ") AS "
                        + quote(column.name));
            } else if (inLeft) {
                parts.add("l." + quote(column.name) + " AS " + quote(column.name));
            } else if (inRight) {
                parts.add("r." + quote(column.name) + " AS " + quote(column.name));
            } else {
                parts.add(computedOrNullExpression(vp, column,
                        "COALESCE(l." + quote(ROW_ID) + ", r." + quote(ROW_ID) + ")") + " AS " + quote(column.name));
            }
        }
        return String.join(", ", parts);
    }

    private String usingProjection(VpSchema vp) {
        List<String> parts = new ArrayList<>();
        parts.add(quote(ROW_ID));
        for (VpColumn column : vp.columns) {
            if (containsColumn(vp.leftColumns, column.name)) {
                parts.add(quote(vp.leftTable) + "." + quote(column.name) + " AS " + quote(column.name));
            } else if (containsColumn(vp.rightColumns, column.name)) {
                parts.add(quote(vp.rightTable) + "." + quote(column.name) + " AS " + quote(column.name));
            } else {
                parts.add(computedOrNullExpression(vp, column, quote(ROW_ID)) + " AS " + quote(column.name));
            }
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

    private String projectColumn(VpSchema vp, VpColumn column, String leftAlias, String rightAlias,
            boolean rightBiasedRowId) {
        String alias = columnAlias(vp, column, leftAlias, rightAlias);
        if (containsColumn(vp.leftColumns, column.name) || containsColumn(vp.rightColumns, column.name)) {
            return alias + "." + quote(column.name) + " AS " + quote(column.name);
        }
        String rowId = (rightBiasedRowId ? rightAlias : leftAlias) + "." + quote(ROW_ID);
        return computedOrNullExpression(vp, column, rowId) + " AS " + quote(column.name);
    }

    private String computedOrNullExpression(VpSchema vp, VpColumn column, String rowIdExpression) {
        if (column.computedExpression == null) {
            return "NULL";
        }
        return column.computedExpression.replace(quote(ROW_ID), rowIdExpression);
    }

    private String leftProjectionWithRightLookups(VpSchema vp) {
        List<String> parts = new ArrayList<>();
        parts.add("l." + quote(ROW_ID) + " AS " + quote(ROW_ID));
        for (VpColumn column : vp.columns) {
            if (containsColumn(vp.leftColumns, column.name)) {
                parts.add("l." + quote(column.name) + " AS " + quote(column.name));
            } else if (!containsColumn(vp.rightColumns, column.name) && column.computedExpression != null) {
                parts.add(computedOrNullExpression(vp, column, "l." + quote(ROW_ID)) + " AS " + quote(column.name));
            } else {
                parts.add("(SELECT r." + quote(column.name) + " FROM " + quote(vp.rightTable) + " r WHERE "
                        + reconstructionJoinCondition(vp, "l", "r") + ") AS " + quote(column.name));
            }
        }
        return String.join(", ", parts);
    }

    private SourceRelation scalarSubqueryRelation(VpSchema vp) {
        return reconstructedSubquery(vp, leftProjectionWithRightLookups(vp), " FROM " + quote(vp.leftTable) + " l");
    }

    private SourceRelation cteRelation(VpSchema vp) {
        return new SourceRelation("(WITH l AS (SELECT * FROM " + quote(vp.leftTable) + "), r AS (SELECT * FROM "
                + quote(vp.rightTable) + ") SELECT " + joinProjection(vp, "l", "r", false) + " FROM l JOIN r ON "
                + reconstructionJoinCondition(vp, "l", "r") + ")", vp.timeTravelTimestamp);
    }

    private SourceRelation aggregateThenMergeRelation(VpSchema vp) {
        List<String> keyColumns = reconstructionKeyColumnNames(vp);
        String keyList = String.join(", ", keyColumns);
        String lToRg = keyColumns.stream().map(c -> keyEquality(vp, "l", "rg", c)).collect(Collectors.joining(" AND "));
        String rToRg = keyColumns.stream().map(c -> keyEquality(vp, "r", "rg", c)).collect(Collectors.joining(" AND "));
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN (SELECT " + keyList + ", COUNT(*) AS vp_count FROM "
                        + quote(vp.rightTable) + " GROUP BY " + keyList + ") rg ON " + lToRg
                        + " AND rg.vp_count = 1 JOIN " + quote(vp.rightTable) + " r ON " + rToRg);
    }

    private SourceRelation rowNumberRelation(VpSchema vp) {
        String orderBy = stableRowNumberOrderBy(vp);
        return new SourceRelation(
                "(WITH l AS (SELECT ROW_NUMBER() OVER (" + orderBy + ") AS rn, "
                        + rowNumberCteProjection(vp.leftColumns) + " FROM " + quote(vp.leftTable)
                        + "), r AS (SELECT ROW_NUMBER() OVER (" + orderBy + ") AS rn, "
                        + rowNumberCteProjection(vp.rightColumns) + " FROM " + quote(vp.rightTable) + ") SELECT "
                        + joinProjection(vp, "l", "r", false) + " FROM l JOIN r ON l.rn = r.rn)",
                vp.timeTravelTimestamp);
    }

    private String stableRowNumberOrderBy(VpSchema vp) {
        /*
         * vp_rowid is created as INT8 NOT NULL PRIMARY KEY on the VP source and copied into every split.
         * ROW_NUMBER_ALIGNMENT therefore orders by a unique, non-null key: NULL ordering is irrelevant and ties cannot
         * occur unless CockroachDB violates the primary-key invariant.
         */
        return "ORDER BY " + quote(ROW_ID) + " ASC";
    }

    private String rowNumberCteProjection(List<VpColumn> columns) {
        List<String> parts = new ArrayList<>();
        parts.add(quote(ROW_ID));
        parts.addAll(columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        return String.join(", ", parts);
    }

    private SourceRelation jsonPackRelation(VpSchema vp) {
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN (SELECT " + quote(ROW_ID) + ", jsonb_build_object('vp', "
                        + quote(ROW_ID) + ") AS payload FROM " + quote(vp.rightTable) + ") jp ON l." + quote(ROW_ID)
                        + " = jp." + quote(ROW_ID) + " AND jp.payload = jsonb_build_object('vp', l." + quote(ROW_ID)
                        + ") JOIN " + quote(vp.rightTable) + " r ON r." + quote(ROW_ID) + " = jp." + quote(ROW_ID));
    }

    private SourceRelation stringPackRelation(VpSchema vp) {
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN (SELECT " + quote(ROW_ID) + ", concat('vp:', " + quote(ROW_ID)
                        + "::STRING) AS payload FROM " + quote(vp.rightTable) + ") sp ON concat('vp:', l."
                        + quote(ROW_ID) + "::STRING) = sp.payload JOIN " + quote(vp.rightTable) + " r ON r."
                        + quote(ROW_ID) + " = sp." + quote(ROW_ID));
    }

    private SourceRelation outerNullExtensionRelation(VpSchema vp) {
        String rowId = quote(ROW_ID);
        String outerProbe = "(SELECT COALESCE(MAX(CASE WHEN lp." + rowId + " IS NULL OR rp." + rowId
                + " IS NULL THEN 1 ELSE 0 END), 0) AS has_outer_null FROM " + quote(vp.leftTable)
                + " lp FULL OUTER JOIN " + quote(vp.rightTable) + " rp ON "
                + reconstructionJoinCondition(vp, "lp", "rp") + ") outer_probe";
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN " + quote(vp.rightTable) + " r ON "
                        + reconstructionJoinCondition(vp, "l", "r") + " CROSS JOIN " + outerProbe
                        + " WHERE outer_probe.has_outer_null = 0");
    }

    private SourceRelation hintedJoinRelation(VpSchema vp) {
        String hint = chooseJoinHintForCoverage();
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l INNER " + hint + " JOIN " + quote(vp.rightTable) + " r ON "
                        + reconstructionJoinCondition(vp, "l", "r"));
    }

    private SourceRelation zigzagRelation(VpSchema vp, List<String> extraSetup) {
        List<VpColumn> leftIndexColumns = helperIndexableColumns(vp.leftColumns);
        List<VpColumn> rightIndexColumns = helperIndexableColumns(vp.rightColumns);
        if (leftIndexColumns.size() < 2 || rightIndexColumns.size() < 2) {
            throw new IgnoreMeException();
        }
        String leftIndexA = vp.prefix + "zig_l_a";
        String leftIndexB = vp.prefix + "zig_l_b";
        String rightIndexA = vp.prefix + "zig_r_a";
        String rightIndexB = vp.prefix + "zig_r_b";
        extraSetup
                .add(createZigzagIndexSQL(leftIndexA, vp.leftTable, leftIndexColumns.get(0), leftIndexColumns.get(1)));
        extraSetup
                .add(createZigzagIndexSQL(leftIndexB, vp.leftTable, leftIndexColumns.get(1), leftIndexColumns.get(0)));
        extraSetup.add(
                createZigzagIndexSQL(rightIndexA, vp.rightTable, rightIndexColumns.get(0), rightIndexColumns.get(1)));
        extraSetup.add(
                createZigzagIndexSQL(rightIndexB, vp.rightTable, rightIndexColumns.get(1), rightIndexColumns.get(0)));
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN " + quote(vp.rightTable) + " r ON "
                        + reconstructionJoinCondition(vp, "l", "r") + " WHERE "
                        + zigzagPartitionPredicate(vp, "l", true) + " AND " + zigzagPartitionPredicate(vp, "r", false));
    }

    private String createZigzagIndexSQL(String indexName, String tableName, VpColumn first, VpColumn second) {
        return "CREATE INDEX IF NOT EXISTS " + quote(indexName) + " ON " + quote(tableName) + " (" + quote(first.name)
                + ", " + quote(second.name) + ", " + quote(ROW_ID) + ")";
    }

    private String zigzagSourcePredicate(VpSchema vp, String alias) {
        List<VpColumn> leftIndexColumns = helperIndexableColumns(vp.leftColumns);
        List<VpColumn> rightIndexColumns = helperIndexableColumns(vp.rightColumns);
        if (leftIndexColumns.isEmpty() || rightIndexColumns.isEmpty()) {
            throw new IgnoreMeException();
        }
        return zigzagPartitionPredicate(vp, alias, true) + " AND " + zigzagPartitionPredicate(vp, alias, false);
    }

    private String zigzagPartitionPredicate(VpSchema vp, String alias, boolean left) {
        List<VpColumn> columns = helperIndexableColumns(left ? vp.leftColumns : vp.rightColumns);
        if (columns.isEmpty()) {
            throw new IgnoreMeException();
        }
        return alias + "." + quote(columns.get(0).name) + " IS NOT NULL";
    }

    private String chooseJoinHintForCoverage() {
        if (!advancedFeaturesEnabled()) {
            return Randomly.fromOptions("HASH", "MERGE", "LOOKUP");
        }
        Map<String, String> hintToShape = new LinkedHashMap<>();
        hintToShape.put("HASH", "hash-join");
        hintToShape.put("MERGE", "merge-join");
        hintToShape.put("LOOKUP", "lookup-join");
        int min = Integer.MAX_VALUE;
        List<String> leastCoveredHints = new ArrayList<>();
        synchronized (STATS_LOCK) {
            for (Map.Entry<String, String> entry : hintToShape.entrySet()) {
                int count = PLAN_SHAPE_COUNTS.getOrDefault("split:" + entry.getValue(), 0)
                        + PLAN_SHAPE_COUNTS.getOrDefault("single:" + entry.getValue(), 0);
                if (count < min) {
                    leastCoveredHints.clear();
                    min = count;
                }
                if (count == min) {
                    leastCoveredHints.add(entry.getKey());
                }
            }
        }
        return Randomly.fromList(leastCoveredHints);
    }

    private SourceRelation lateralRelation(VpSchema vp) {
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN LATERAL (SELECT * FROM " + quote(vp.rightTable) + " rr WHERE "
                        + reconstructionJoinCondition(vp, "l", "rr") + ") r ON TRUE");
    }

    private void comparePair(VpQueryPair pair) throws Exception {
        applyOptimizerProfile(pair.optimizerProfile);
        for (String setup : pair.extraSetupStatements) {
            try {
                if (!executeInternalStatement("extra-setup", setup, false)) {
                    throw new IgnoreMeException();
                }
                pair.schema.setupStatements.add(setup);
            } catch (SQLException | AssertionError e) {
                if (isExpectedSetupOrCleanupError(e)) {
                    recordSkippedError("extra-setup", setup, getErrorText(e));
                    throw new IgnoreMeException();
                }
                if (suppressKnownUnexpectedQueryError(pair, "extra-setup", setup, e)) {
                    throw new IgnoreMeException();
                }
                logUnexpectedQueryError(pair, "extra-setup", setup, e);
                throw e;
            }
        }
        markReconstructionMaterialized(pair);
        recordPairStage(pair, PairStage.EXTRA_SETUP_OK);
        QueryResultMultiset singleRows;
        try {
            singleRows = getResultMultiset("single-query", pair.singleQuery);
        } catch (SQLException | AssertionError e) {
            if (suppressKnownUnexpectedQueryError(pair, "single-query", pair.singleQuery, e)) {
                throw new IgnoreMeException();
            }
            logUnexpectedQueryError(pair, "single-query", pair.singleQuery, e);
            throw e;
        }
        recordPairStage(pair, PairStage.SINGLE_OK);
        QueryResultMultiset splitRows;
        try {
            splitRows = getResultMultiset("split-query", pair.splitQuery);
        } catch (SQLException | AssertionError e) {
            if (suppressKnownUnexpectedQueryError(pair, "split-query", pair.splitQuery, e)) {
                throw new IgnoreMeException();
            }
            logUnexpectedQueryError(pair, "split-query", pair.splitQuery, e);
            throw e;
        }
        recordPairStage(pair, PairStage.SPLIT_OK);
        if (!resultMultisetsMatch(singleRows, splitRows)) {
            recordPairStage(pair, PairStage.MISMATCH);
            KnownVpBug knownBug = classifyKnownBug(pair, singleRows, splitRows);
            if (state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns && knownBug != null) {
                logKnownBug(pair, knownBug, singleRows, splitRows);
                recordKnownBugSuppressed(knownBug);
                throw new IgnoreMeException();
            }
            throw new AssertionError(formatBugReport(pair, singleRows, splitRows));
        }
        if (pair.orderedComparisonEnabled && !orderedRowsMatch(singleRows, splitRows)) {
            recordPairStage(pair, PairStage.MISMATCH);
            throw new AssertionError(formatOrderedBugReport(pair, singleRows, splitRows));
        }
        recordPairStage(pair, PairStage.COMPARED);
        recordComparedFeatures(pair);
        capturePlanFingerprints(pair);
    }

    private void markReconstructionMaterialized(VpQueryPair pair) {
        if (pair.extraSetupStatements.isEmpty()) {
            return;
        }
        switch (pair.reconstructionStrategy) {
        case TEMP_TABLE:
            pair.schema.tempTableMaterialized = true;
            break;
        default:
            break;
        }
    }

    private void capturePlanFingerprints(VpQueryPair pair) {
        captureDistributedPlan(pair);
        if (!reservePlanFingerprintSample()) {
            return;
        }
        try {
            PlanCapture sourcePlan = capturePlan(pair.singleQuery, pair.schema.prefix);
            PlanCapture splitPlan = capturePlan(pair.splitQuery, pair.schema.prefix);
            String key = "strategy=" + pair.reconstructionStrategy + " source=" + sourcePlan.fingerprint + " split="
                    + splitPlan.fingerprint;
            synchronized (STATS_LOCK) {
                incrementString(PLAN_FINGERPRINT_COUNTS, key);
                recordPlanShapes("single", sourcePlan.shapes);
                recordPlanShapes("split", splitPlan.shapes);
            }
            markVpStatsDirty();
        } catch (SQLException e) {
            recordSkippedError("plan-fingerprint", "EXPLAIN (OPT) " + pair.singleQuery, getErrorText(e));
        } catch (Exception | AssertionError e) {
            recordSkippedError("plan-fingerprint", "EXPLAIN (OPT) " + pair.singleQuery, getErrorText(e));
        }
    }

    private void captureDistributedPlan(VpQueryPair pair) {
        if (!distributedProfileEnabled() || !reserveDistributedPlanSample()) {
            return;
        }
        try {
            String singleDistribution = classifyDistributedPlan(explainDistributed(pair.singleQuery));
            String splitDistribution = classifyDistributedPlan(explainDistributed(pair.splitQuery));
            String gateway = pair.schema.distributedGatewayNode == null ? "unknown"
                    : pair.schema.distributedGatewayNode;
            recordDistributedPlanDistributions(pair.optimizerProfile, gateway, singleDistribution, splitDistribution);
            captureDistributedPlansFromOtherGateways(pair);
        } catch (SQLException e) {
            recordSkippedError("distributed-plan", "EXPLAIN (DISTSQL) " + pair.splitQuery, getErrorText(e));
            recordDistributedProfileEvent("distsql-plan-skipped");
        }
    }

    private static void recordDistributedPlanDistributions(OptimizerProfile profile, String gateway,
            String singleDistribution, String splitDistribution) {
        recordDistributedProfileEvent("single-distsql-plan=" + singleDistribution);
        recordDistributedProfileEvent("split-distsql-plan=" + splitDistribution);
        recordDistributedProfileEvent("distsql-plan-diff=" + singleDistribution + ":" + splitDistribution);
        recordDistributedProfileEvent(
                "distsql-plan-diff-profile=" + profile.name() + ":" + singleDistribution + ":" + splitDistribution);
        recordDistributedProfileEvent("gateway-single-distsql=" + gateway + ":" + singleDistribution);
        recordDistributedProfileEvent("gateway-split-distsql=" + gateway + ":" + splitDistribution);
        if (!singleDistribution.equals(splitDistribution)) {
            recordDistributedProfileEvent("distsql-plan-locality-divergence");
        }
    }

    private void captureDistributedPlansFromOtherGateways(VpQueryPair pair) {
        if (!rowRangePressureProfileEnabled()) {
            return;
        }
        List<String> gateways = parseDistributedGateways();
        if (gateways.size() <= 1) {
            return;
        }
        List<String> sampledGateways = Randomly.nonEmptySubset(gateways, Math.min(2, gateways.size()));
        for (String endpoint : sampledGateways) {
            try (Connection connection = openGatewayConnection(endpoint)) {
                applyOptimizerProfile(connection, pair.optimizerProfile);
                String singleDistribution = classifyDistributedPlan(explainDistributed(connection, pair.singleQuery));
                String splitDistribution = classifyDistributedPlan(explainDistributed(connection, pair.splitQuery));
                recordDistributedProfileEvent("cross-gateway-endpoint=" + endpoint);
                recordDistributedPlanDistributions(pair.optimizerProfile, endpoint, singleDistribution,
                        splitDistribution);
            } catch (SQLException e) {
                recordSkippedError("distributed-plan-gateway", endpoint + " EXPLAIN (DISTSQL) " + pair.splitQuery,
                        getErrorText(e));
                recordDistributedProfileEvent("cross-gateway-distsql-plan-skipped");
            }
        }
    }

    private Connection openGatewayConnection(String endpoint) throws SQLException {
        int colon = endpoint.lastIndexOf(':');
        if (colon <= 0 || colon + 1 >= endpoint.length()) {
            throw new SQLException("invalid gateway endpoint: " + endpoint);
        }
        String host = endpoint.substring(0, colon);
        int port;
        try {
            port = Integer.parseInt(endpoint.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new SQLException("invalid gateway endpoint port: " + endpoint, e);
        }
        Connection connection = DriverManager.getConnection(
                String.format("jdbc:postgresql://%s:%d/%s", host, port, state.getDatabaseName()),
                state.getOptions().getUserName(), state.getOptions().getPassword());
        try (Statement statement = connection.createStatement()) {
            if (state.getDbmsSpecificOptions().testHashIndexes) {
                statement.execute("SET experimental_enable_hash_sharded_indexes = 'on'");
            }
            statement.execute("SET experimental_enable_temp_tables = 'on'");
            statement.execute("SET allow_unsafe_internals = true");
        }
        return connection;
    }

    private static void applyOptimizerProfile(Connection connection, OptimizerProfile profile) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (String sql : profile.statements) {
                statement.execute(sql);
            }
        }
    }

    private static boolean reserveDistributedPlanSample() {
        synchronized (STATS_LOCK) {
            if (distributedPlanSamples >= MAX_DISTRIBUTED_PLAN_SAMPLES) {
                return false;
            }
            distributedPlanSamples++;
            return true;
        }
    }

    private String explainDistributed(String query) throws SQLException {
        return explainDistributed(state.getConnection().getJdbcConnection(), query);
    }

    private String explainDistributed(Connection connection, String query) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("EXPLAIN (DISTSQL) " + query)) {
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int column = 1; column <= columns; column++) {
                    String value = rs.getString(column);
                    if (value != null) {
                        plan.append(value).append('\n');
                    }
                }
            }
        }
        return plan.toString();
    }

    private static String classifyDistributedPlan(String plan) {
        String lower = plan.toLowerCase(Locale.ROOT);
        if (lower.contains("distribution: full")) {
            return "full";
        }
        if (lower.contains("distribution: local") || lower.contains("no distribution")) {
            return "local";
        }
        return "unknown";
    }

    private static void recordPlanShapes(String side, Set<String> shapes) {
        if (shapes.isEmpty()) {
            incrementString(PLAN_SHAPE_COUNTS, side + ":unknown");
            return;
        }
        for (String shape : shapes) {
            incrementString(PLAN_SHAPE_COUNTS, side + ":" + shape);
        }
    }

    private static boolean reservePlanFingerprintSample() {
        synchronized (STATS_LOCK) {
            if (planFingerprintSamples >= MAX_PLAN_FINGERPRINTS) {
                return false;
            }
            planFingerprintSamples++;
            return true;
        }
    }

    private PlanCapture capturePlan(String query, String vpPrefix) throws SQLException {
        String planText = explainOptText(query);
        String normalized = planText.replace(vpPrefix, "vp_").replaceAll("[0-9]+", "#").replaceAll("\\s+", " ").trim();
        return new PlanCapture(Integer.toUnsignedString(normalized.hashCode(), 16), classifyPlanShapes(planText));
    }

    private String explainOptText(String query) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("EXPLAIN (OPT) " + query)) {
            while (rs.next()) {
                plan.append(rs.getString(1)).append('\n');
            }
        }
        return plan.toString();
    }

    private Set<String> classifyPlanShapes(String planText) {
        String lower = planText.toLowerCase(Locale.ROOT).replace('-', ' ').replace('_', ' ');
        Set<String> shapes = new java.util.LinkedHashSet<>();
        addShapeIfPresent(shapes, lower, "lookup join", "lookup-join");
        addShapeIfPresent(shapes, lower, "hash join", "hash-join");
        addShapeIfPresent(shapes, lower, "merge join", "merge-join");
        addShapeIfPresent(shapes, lower, "zigzag join", "zigzag-join");
        addShapeIfPresent(shapes, lower, "inverted", "inverted");
        addShapeIfPresent(shapes, lower, "index join", "index-join");
        addShapeIfPresent(shapes, lower, "scan", "scan");
        addShapeIfPresent(shapes, lower, "group", "grouping");
        addShapeIfPresent(shapes, lower, "distinct", "distinct");
        addShapeIfPresent(shapes, lower, "sort", "sort");
        addShapeIfPresent(shapes, lower, "distribution", "distribution");
        addShapeIfPresent(shapes, lower, "union", "union");
        addShapeIfPresent(shapes, lower, "window", "window");
        return shapes;
    }

    private static void addShapeIfPresent(Set<String> shapes, String planText, String needle, String shape) {
        if (planText.contains(needle)) {
            shapes.add(shape);
        }
    }

    private KnownVpBug classifyKnownBug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (isPartialIndexNullFilterWithLimitOffset(pair, singleRows, splitRows)) {
            return KnownVpBug.PARTIAL_INDEX_NULL_FILTER_WITH_LIMIT_OFFSET;
        }
        String aggregate = planDependentNumericAggregate(pair, singleRows, splitRows);
        if (aggregate != null) {
            switch (aggregate) {
            case "variance":
                return KnownVpBug.VARIANCE_NUMERIC_PLAN_INSTABILITY;
            case "stddev":
                return KnownVpBug.STDDEV_NUMERIC_PLAN_INSTABILITY;
            case "sqrdiff":
                return KnownVpBug.SQRDIFF_NUMERIC_PLAN_INSTABILITY;
            case "mixed":
                return KnownVpBug.MIXED_NUMERIC_PLAN_INSTABILITY;
            default:
                throw new AssertionError("unexpected numeric aggregate: " + aggregate);
            }
        }

        aggregate = repeatedLargeDecimalAggregate(pair);
        if (aggregate == null || !onlyNumericValuesDifferInAggregateColumns(pair, singleRows, splitRows, aggregate)) {
            return null;
        }
        for (VpColumn column : pair.schema.columns) {
            if (column.type.getPrimitiveDataType() != CockroachDBDataType.DECIMAL
                    || !usesAggregateOnColumn(pair.singleQuery, aggregate, column.name)
                    || !usesAggregateOnColumn(pair.splitQuery, aggregate, column.name)) {
                continue;
            }
            if (hasOneRepeatedLargeDecimalValue(pair.schema, column.name)) {
                return "variance".equals(aggregate) ? KnownVpBug.VARIANCE_REPEATED_LARGE_DECIMAL
                        : KnownVpBug.SQRDIFF_REPEATED_LARGE_DECIMAL;
            }
        }
        return null;
    }

    private boolean isPartialIndexNullFilterWithLimitOffset(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.JOIN || !hasOrderLimitAndOffset(pair.singleQuery)
                || !hasOrderLimitAndOffset(pair.splitQuery)) {
            return false;
        }
        List<String> singleProjections = topLevelSelectProjections(pair.singleQuery);
        List<String> splitProjections = topLevelSelectProjections(pair.splitQuery);
        if (singleProjections.size() != 1 || !sameProjectionShape(singleProjections, splitProjections)) {
            return false;
        }
        String projectedColumn = directlyProjectedVpColumn(pair.schema, singleProjections.get(0));
        if (projectedColumn == null || !containsColumn(pair.schema.rightColumns, projectedColumn)
                || !onlyAllNullRowsAreMissing(singleRows, splitRows)) {
            return false;
        }

        String partialIndexName = pair.schema.prefix + "partial_r";
        if (!hasRightNotNullPartialIndex(pair, partialIndexName, projectedColumn)) {
            return false;
        }
        try {
            String splitPlan = explainOptText(pair.splitQuery).toLowerCase(Locale.ROOT);
            String predicate = "r." + projectedColumn.toLowerCase(Locale.ROOT) + " is not null";
            return splitPlan.contains("partial index predicates")
                    && splitPlan.contains(partialIndexName.toLowerCase(Locale.ROOT))
                    && splitPlan.replace('"', ' ').replaceAll("\\s+", " ").contains(predicate);
        } catch (SQLException ignored) {
            // If the exact optimizer signature cannot be proven, keep reporting the mismatch.
            return false;
        }
    }

    private static boolean hasOrderLimitAndOffset(String query) {
        String normalized = query.toLowerCase(Locale.ROOT);
        return normalized.contains(" order by ") && normalized.contains(" limit ") && normalized.contains(" offset ");
    }

    private String directlyProjectedVpColumn(VpSchema schema, String projection) {
        String normalized = projection.trim().toLowerCase(Locale.ROOT).replace("\"", "");
        for (VpColumn column : schema.columns) {
            String columnName = column.name.toLowerCase(Locale.ROOT);
            if (normalized.equals(columnName) || normalized.endsWith("." + columnName)) {
                return column.name;
            }
        }
        return null;
    }

    private boolean onlyAllNullRowsAreMissing(QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        boolean foundMissingNullRow = false;
        for (Map.Entry<String, Integer> entry : splitRows.rows.entrySet()) {
            if (entry.getValue() > singleRows.rows.getOrDefault(entry.getKey(), 0)) {
                return false;
            }
        }
        for (Map.Entry<String, Integer> entry : singleRows.rows.entrySet()) {
            int missing = entry.getValue() - splitRows.rows.getOrDefault(entry.getKey(), 0);
            if (missing <= 0) {
                continue;
            }
            List<String> values = decodeCanonicalRow(entry.getKey());
            if (values.size() != 1 || values.get(0) != null) {
                return false;
            }
            foundMissingNullRow = true;
        }
        return foundMissingNullRow;
    }

    private boolean hasRightNotNullPartialIndex(VpQueryPair pair, String indexName, String columnName) {
        String expectedTable = quote(pair.schema.rightTable).toLowerCase(Locale.ROOT);
        String expectedIndex = quote(indexName).toLowerCase(Locale.ROOT);
        String expectedColumn = quote(columnName).toLowerCase(Locale.ROOT);
        for (String setup : pair.schema.setupStatements) {
            String normalized = setup.toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");
            if (normalized.startsWith("create index " + expectedIndex + " on " + expectedTable)
                    && normalized.contains("(" + expectedColumn + ")")
                    && normalized.contains("where " + expectedColumn + " is not null")) {
                return true;
            }
        }
        return false;
    }

    private String planDependentNumericAggregate(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        List<String> singleAggregates = numericAggregateKinds(pair.singleQuery);
        List<String> splitAggregates = numericAggregateKinds(pair.splitQuery);
        if (!singleAggregates.equals(splitAggregates) || singleAggregates.isEmpty()) {
            return null;
        }
        // A mixed query is safe to classify when all changed projections belong
        // to exactly one aggregate implementation. If both implementations
        // change, retain the mismatch for investigation rather than guessing.
        Set<String> changedAggregates = changedNumericAggregateKinds(pair, singleRows, splitRows);
        if (changedAggregates.isEmpty()) {
            return null;
        }
        // SUM on floating point input can expose the same plan-dependent
        // accumulation order as variance, but SUM alone remains reportable.
        if (changedAggregates.stream().allMatch("sum"::equals)) {
            return null;
        }
        // Mixed classification is deliberately limited to the known unstable
        // aggregate family and still requires every changed projection to be
        // one of those aggregates (checked by changedNumericAggregateKinds).
        return changedAggregates.size() == 1 ? changedAggregates.iterator().next() : "mixed";
    }

    private List<String> numericAggregateKinds(String query) {
        List<String> aggregates = new ArrayList<>();
        String normalized = query.toLowerCase(Locale.ROOT);
        for (int offset = 0; offset < normalized.length();) {
            int next = -1;
            String nextAggregate = null;
            for (String aggregate : List.of("variance", "stddev", "sqrdiff", "sum")) {
                int candidate = normalized.indexOf(aggregate + "(", offset);
                if (candidate >= 0 && (next < 0 || candidate < next)) {
                    next = candidate;
                    nextAggregate = aggregate;
                }
            }
            if (next < 0) {
                break;
            }
            aggregates.add(nextAggregate);
            offset = next + nextAggregate.length() + 1;
        }
        return aggregates;
    }

    private NumericRowDifferences findNumericRowDifferences(QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        List<List<String>> left = expandRows(singleRows.rows);
        List<List<String>> right = expandRows(splitRows.rows);
        if (left.size() != right.size()) {
            return null;
        }
        List<NumericRowDifference> differences = new ArrayList<>();
        boolean[] used = new boolean[right.size()];
        for (List<String> l : left) {
            int match = -1;
            int fewestDifferences = Integer.MAX_VALUE;
            for (int j = 0; j < right.size(); j++) {
                if (used[j] || !compatibleNumericRow(l, right.get(j))) {
                    continue;
                }
                int differenceCount = numericDifferenceCount(l, right.get(j));
                if (differenceCount < fewestDifferences) {
                    match = j;
                    fewestDifferences = differenceCount;
                    if (differenceCount == 0) {
                        break;
                    }
                }
            }
            if (match < 0) {
                return null;
            }
            used[match] = true;
            List<String> r = right.get(match);
            if (!l.equals(r)) {
                differences.add(new NumericRowDifference(l, r));
            }
        }
        return differences.isEmpty() ? null : new NumericRowDifferences(differences);
    }

    private List<List<String>> expandRows(Map<String, Integer> rows) {
        List<List<String>> result = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : rows.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                result.add(decodeCanonicalRow(entry.getKey()));
            }
        }
        return result;
    }

    private boolean compatibleNumericRow(List<String> left, List<String> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (Objects.equals(left.get(i), right.get(i))) {
                continue;
            }
            if (!isBigDecimal(left.get(i)) || !isBigDecimal(right.get(i))) {
                return false;
            }
        }
        return true;
    }

    private int numericDifferenceCount(List<String> left, List<String> right) {
        int differences = 0;
        for (int i = 0; i < left.size(); i++) {
            if (!Objects.equals(left.get(i), right.get(i))) {
                differences++;
            }
        }
        return differences;
    }

    private boolean onlyNumericValuesDifferInAggregateColumns(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows, String aggregate) {
        NumericRowDifferences allDifferences = findNumericRowDifferences(singleRows, splitRows);
        if (allDifferences == null) {
            return false;
        }
        List<String> singleProjections = topLevelSelectProjections(pair.singleQuery);
        List<String> splitProjections = topLevelSelectProjections(pair.splitQuery);
        if (!sameProjectionShape(singleProjections, splitProjections)) {
            return false;
        }
        for (NumericRowDifference difference : allDifferences.differences) {
            if (singleProjections.size() != difference.singleValues.size()) {
                return false;
            }
            for (int i = 0; i < difference.singleValues.size(); i++) {
                if (!Objects.equals(difference.singleValues.get(i), difference.splitValues.get(i))
                        && !singleProjections.get(i).toLowerCase(Locale.ROOT).contains(aggregate + "(")) {
                    return false;
                }
            }
        }
        return true;
    }

    private Set<String> changedNumericAggregateKinds(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        NumericRowDifferences allDifferences = findNumericRowDifferences(singleRows, splitRows);
        if (allDifferences == null) {
            return Set.of();
        }
        List<String> projections = topLevelSelectProjections(pair.singleQuery);
        List<String> splitProjections = topLevelSelectProjections(pair.splitQuery);
        if (!sameProjectionShape(projections, splitProjections)) {
            return Set.of();
        }
        Set<String> changed = new LinkedHashSet<>();
        boolean hasEstablishedUnstableAggregate = false;
        for (NumericRowDifference difference : allDifferences.differences) {
            if (projections.size() != difference.singleValues.size()) {
                return Set.of();
            }
            for (int i = 0; i < difference.singleValues.size(); i++) {
                if (Objects.equals(difference.singleValues.get(i), difference.splitValues.get(i))) {
                    continue;
                }
                String projection = projections.get(i);
                boolean aggregateProjection = false;
                for (String aggregate : List.of("variance", "stddev", "sqrdiff", "sum")) {
                    if (projection.contains(aggregate + "(")) {
                        if ("sum".equals(aggregate) && !isDirectFloatingPointSum(pair.schema, projection)) {
                            return Set.of();
                        }
                        changed.add(aggregate);
                        if (!"sum".equals(aggregate)) {
                            hasEstablishedUnstableAggregate = true;
                        }
                        aggregateProjection = true;
                        break;
                    }
                }
                if (!aggregateProjection) {
                    return Set.of();
                }
            }
        }
        return hasEstablishedUnstableAggregate ? changed : Set.of();
    }

    private boolean isDirectFloatingPointSum(VpSchema schema, String projection) {
        for (VpColumn column : schema.columns) {
            if (column.type.getPrimitiveDataType() == CockroachDBDataType.FLOAT
                    && usesAggregateOnColumn(projection, "sum", column.name)) {
                return true;
            }
        }
        return false;
    }

    private List<String> topLevelSelectProjections(String query) {
        String normalized = query.toLowerCase(Locale.ROOT);
        int select = normalized.indexOf("select ");
        int start = select < 0 ? -1 : select + "select ".length();
        if (start < 0) {
            return List.of();
        }
        if (normalized.startsWith("distinct ", start)) {
            start += "distinct ".length();
        } else if (normalized.startsWith("all ", start)) {
            start += "all ".length();
        }
        int depth = 0;
        boolean quoted = false;
        List<String> projections = new ArrayList<>();
        int expressionStart = start;
        for (int i = start; i < query.length(); i++) {
            char ch = query.charAt(i);
            if (ch == '\'') {
                if (quoted && i + 1 < query.length() && query.charAt(i + 1) == '\'') {
                    i++;
                } else {
                    quoted = !quoted;
                }
            } else if (!quoted && ch == '(') {
                depth++;
            } else if (!quoted && ch == ')') {
                depth--;
            } else if (!quoted && depth == 0 && ch == ',') {
                projections.add(normalized.substring(expressionStart, i).trim());
                expressionStart = i + 1;
            } else if (!quoted && depth == 0 && normalized.startsWith(" from ", i)) {
                projections.add(normalized.substring(expressionStart, i).trim());
                return projections;
            }
        }
        return List.of();
    }

    private boolean isBigDecimal(String value) {
        if (value == null) {
            return false;
        }
        try {
            new BigDecimal(value);
            return true;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    /**
     * Reconstruction strategies may rename the generated source relation or introduce an equivalent derived-table
     * alias. That textual difference must not prevent the narrow known numeric-aggregate signature from being
     * recognized.
     */
    private boolean sameProjectionShape(List<String> left, List<String> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            String normalizedLeft = normalizeProjectionRelationNames(left.get(i));
            String normalizedRight = normalizeProjectionRelationNames(right.get(i));
            if (!normalizedLeft.equals(normalizedRight)) {
                return false;
            }
        }
        return true;
    }

    private String normalizeProjectionRelationNames(String projection) {
        return projection.replaceAll("(?i)\\\"vp_[a-z0-9_]+_source\\\"", "\\\"vp_source\\\"")
                .replaceAll("(?i)\\bvp_[a-z0-9_]+_source\\b", "vp_source");
    }

    private String repeatedLargeDecimalAggregate(VpQueryPair pair) {
        for (String aggregate : List.of("variance", "sqrdiff")) {
            if (containsExactlyOneAggregate(pair.singleQuery, aggregate)
                    && containsExactlyOneAggregate(pair.splitQuery, aggregate)) {
                return aggregate;
            }
        }
        return null;
    }

    private boolean containsExactlyOneAggregate(String query, String aggregate) {
        String normalized = query.toLowerCase(Locale.ROOT);
        String invocation = aggregate + "(";
        int first = normalized.indexOf(invocation);
        return first >= 0 && normalized.indexOf(invocation, first + 1) < 0;
    }

    private boolean usesAggregateOnColumn(String query, String aggregate, String columnName) {
        return query.toLowerCase(Locale.ROOT)
                .contains(aggregate + "(" + quote(columnName).toLowerCase(Locale.ROOT) + ")");
    }

    private boolean hasOneRepeatedLargeDecimalValue(VpSchema vp, String columnName) {
        String alias = "known_aggregate_source";
        String column = alias + "." + quote(columnName);
        String sql = "SELECT count(" + column + "), count(DISTINCT " + column + "), max(abs(" + column + ")) FROM "
                + sourceTableRelation(vp).withAlias(alias);
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            if (!rs.next() || rs.getLong(1) == 0 || rs.getLong(2) != 1) {
                return false;
            }
            BigDecimal magnitude = rs.getBigDecimal(3);
            return magnitude != null && magnitude.compareTo(new BigDecimal("1000000000000000")) >= 0;
        } catch (SQLException ignored) {
            // Failure to prove this narrow signature must leave the mismatch reportable.
            return false;
        }
    }

    private boolean suppressKnownUnexpectedQueryError(VpQueryPair pair, String stage, String failingSql,
            Throwable error) throws SQLException {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns) {
            return false;
        }
        KnownVpBug knownBug = classifyKnownUnexpectedQueryError(error);
        if (knownBug == null) {
            return false;
        }
        logKnownUnexpectedQueryError(pair, knownBug, stage, failingSql, error);
        recordKnownBugSuppressed(knownBug);
        return true;
    }

    private KnownVpBug classifyKnownUnexpectedQueryError(Throwable error) {
        String message = getErrorText(error).toLowerCase(Locale.ROOT);
        if (message.contains("internal error: unexpected error from the vectorized runtime")
                && message.contains("unexpected replacement: original vector is")
                && message.contains("whereas the replacement is")
                && (message.contains("original vector is bit") || message.contains("original vector is varbit"))) {
            return KnownVpBug.VECTORIZED_REPLACECOL_BIT_OR_VARBIT;
        }
        if (message.contains("internal error: expected inverted index scan to have a constraint")) {
            return KnownVpBug.INVERTED_INDEX_NULL_ARRAY_CONTAINMENT;
        }
        return null;
    }

    private void logKnownBug(VpQueryPair pair, KnownVpBug knownBug, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("Known VP bug: ").append(knownBug).append(System.lineSeparator()).append(System.lineSeparator());
        sb.append(formatBugReport(pair, singleRows, splitRows));
        File dir = new File(Main.LOG_DIRECTORY, "cockroachdb");
        try {
            Files.createDirectories(dir.toPath());
            Path target = nextKnownBugLogPath(dir);
            Files.writeString(target, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void logKnownUnexpectedQueryError(VpQueryPair pair, KnownVpBug knownBug, String stage, String failingSql,
            Throwable error) throws SQLException {
        String report = "Known CockroachDB VP bug: " + knownBug + System.lineSeparator() + System.lineSeparator()
                + formatUnexpectedQueryErrorReport(pair, stage, failingSql, error);
        File dir = new File(Main.LOG_DIRECTORY, "cockroachdb");
        try {
            Files.createDirectories(dir.toPath());
            Files.writeString(nextKnownBugLogPath(dir), report, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
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

    private void logUnexpectedQueryError(VpQueryPair pair, String stage, String failingSql, Throwable error) {
        String report;
        try {
            report = formatUnexpectedQueryErrorReport(pair, stage, failingSql, error);
        } catch (Throwable formattingError) {
            report = "[VERTICAL PARTITIONING UNEXPECTED QUERY ERROR]" + System.lineSeparator() + "Stage: " + stage
                    + System.lineSeparator() + "Failing SQL: " + failingSql + System.lineSeparator() + "Original error:"
                    + System.lineSeparator() + stackTrace(error) + System.lineSeparator() + "Report formatting error:"
                    + System.lineSeparator() + stackTrace(formattingError);
        }
        File dir = new File(Main.LOG_DIRECTORY, "cockroachdb");
        try {
            Files.createDirectories(dir.toPath());
            Path target = nextUnexpectedErrorLogPath(dir);
            Files.writeString(target, report, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
            if (state.getOptions().logEachSelect()) {
                state.getLogger()
                        .writeCurrent("-- CockroachDB VP unexpected query error dumped to " + target.toAbsolutePath());
            }
        } catch (IOException e) {
            System.err.println("Could not write CockroachDB VP unexpected query error: " + e.getMessage());
        }
    }

    private Path nextUnexpectedErrorLogPath(File dir) {
        Path target;
        do {
            target = new File(dir, "vp-unexpected" + UNEXPECTED_ERROR_LOG_ID.incrementAndGet() + ".log").toPath();
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
            if (row.startsWith("N:", idx) || row.startsWith("S:", idx)) {
                char kind = row.charAt(idx);
                int typeLengthStart = idx + 2;
                int typeLengthEnd = row.indexOf(':', typeLengthStart);
                if (typeLengthEnd < 0) {
                    throw new AssertionError("Malformed VP row encoding: " + row);
                }
                int typeLength = Integer.parseInt(row.substring(typeLengthStart, typeLengthEnd));
                int typeStart = typeLengthEnd + 1;
                int typeEnd = typeStart + typeLength;
                if (typeEnd >= row.length() || row.charAt(typeEnd) != ';') {
                    throw new AssertionError("Malformed VP row encoding: " + row);
                }
                idx = typeEnd + 1;
                if (kind == 'N') {
                    values.add(null);
                    continue;
                }
                int lengthStart = idx;
                if (!row.startsWith("S:", idx)) {
                    throw new AssertionError("Malformed VP row encoding: " + row);
                }
                lengthStart = idx + 2;
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

    private QueryResultMultiset getResultMultiset(String stage, String query) throws Exception {
        return getResultMultiset(stage, query, 0);
    }

    private QueryResultMultiset getResultMultiset(String stage, String query, int transientAttempt) throws Exception {
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(query);
        }
        Map<String, Integer> rows = new LinkedHashMap<>();
        Map<String, List<ApproximateKind>> approximateColumns = new LinkedHashMap<>();
        List<String> orderedRows = new ArrayList<>();
        List<List<ApproximateKind>> orderedApproximateColumns = new ArrayList<>();
        boolean completed = false;
        Main.nrQueries.addAndGet(1);
        try (Statement s = state.getConnection().createStatement();
                SQLancerResultSet rs = new SQLancerResultSet(s.executeQuery(query))) {
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
        } catch (SQLException e) {
            if (isTransientClusterAvailabilityError(e) && transientAttempt + 1 < TRANSIENT_CLUSTER_RETRIES) {
                waitForTransientClusterRecovery(transientAttempt);
                return getResultMultiset(stage, query, transientAttempt + 1);
            }
            if (isTransientClusterAvailabilityError(e)) {
                recordSkippedError("cluster-transient-" + stage, query, getErrorText(e));
                throw new IgnoreMeException();
            }
            if (isExpectedQueryError(e)) {
                recordSkippedError(stage, query, getErrorText(e));
                throw new IgnoreMeException();
            }
            throw e;
        } catch (AssertionError e) {
            if (isSkippableQueryError(e)) {
                recordSkippedError(stage, query, getErrorText(e));
                throw new IgnoreMeException();
            }
            throw e;
        } finally {
            if (completed) {
                Main.nrSuccessfulActions.addAndGet(1);
            } else {
                Main.nrUnsuccessfulActions.addAndGet(1);
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
                || msg.contains("Invalid use of group function") || isSkippableUnknownColumnError(msg)
                || isMissingFunctionError(msg) || isCantGroupOnGeneratedAliasError(msg)
                || msg.contains("Can't reopen table") || msg.contains("Illegal mix of collations")
                || msg.contains("invalid input value for enum") || msg.contains("syntax error")
                || msg.contains("Out of sort memory") || msg.contains("sort buffer size")
                || msg.contains("Operand should contain") || msg.contains("Subquery returns more than 1 row");
    }

    private boolean isExpectedQueryError(Throwable throwable) {
        if (isInternalDatabaseError(getErrorText(throwable).toLowerCase(Locale.ROOT))) {
            return false;
        }
        String errorText = getErrorText(throwable).toLowerCase(Locale.ROOT);
        if (errorText.contains("invalid input value for enum")
                || errorText.contains("must be after replica gc threshold")) {
            return true;
        }
        Throwable current = throwable;
        while (current != null) {
            String msg = current.getMessage();
            if (msg != null && errors.errorIsExpected(msg)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean isExpectedSetupOrCleanupError(Throwable throwable) {
        return isExpectedSetupOrCleanupError("setup", throwable);
    }

    private boolean isExpectedSetupOrCleanupError(String stage, Throwable throwable) {
        if (!("setup".equals(stage) || "extra-setup".equals(stage) || "action".equals(stage)
                || "cleanup".equals(stage))) {
            return false;
        }
        String msg = getErrorText(throwable).toLowerCase(Locale.ROOT);
        return msg.contains("polling for queued jobs to complete")
                || msg.contains("intent_resolver_ir_batcher.sendbatch")
                || msg.contains("active-schema-leases-by-region") || msg.contains("must be after replica gc threshold")
                || msg.contains("result is ambiguous: context deadline exceeded");
    }

    private boolean isUnsupportedWindowError(String msg) {
        String normalized = msg.toLowerCase(Locale.ROOT);
        return normalized.contains("window") && (normalized.contains("doesn't yet support")
                || normalized.contains("not allowed") || normalized.contains("invalid use"));
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
            String type = rs.getType(i);
            if (value == null) {
                sb.append("N:").append(type.length()).append(":").append(type).append(";");
            } else {
                String canonical = canonicalizeValue(value, type);
                sb.append("S:").append(type.length()).append(":").append(type).append(";").append("S:")
                        .append(canonical.length()).append(":").append(canonical).append(";");
            }
        }
        return sb.toString();
    }

    private String canonicalizeValue(String value, String type) {
        if (isBooleanType(type) && "true".equalsIgnoreCase(value)) {
            return "true";
        }
        if (isBooleanType(type) && "false".equalsIgnoreCase(value)) {
            return "false";
        }
        if (!isNumericType(type)) {
            return value;
        }
        try {
            BigDecimal decimal = new BigDecimal(value);
            String plain = decimal.stripTrailingZeros().toPlainString();
            return Objects.equals(plain, "-0") ? "0" : plain;
        } catch (NumberFormatException ignored) {
            return value;
        }
    }

    private boolean isBooleanType(String type) {
        return type != null && type.toUpperCase(Locale.ROOT).contains("BOOL");
    }

    private boolean isNumericType(String type) {
        if (type == null) {
            return false;
        }
        String upper = type.toUpperCase(Locale.ROOT);
        return upper.contains("INT") || upper.contains("DECIMAL") || upper.contains("NUMERIC") || upper.contains("REAL")
                || upper.contains("FLOAT") || upper.contains("DOUBLE") || upper.contains("SERIAL")
                || upper.contains("BIT");
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
        sb.append("CockroachDB version: ").append(getSingleString("SELECT version()")).append(System.lineSeparator());
        sb.append("Base table: ").append(pair.schema.baseTable).append(System.lineSeparator());
        sb.append("Base table DDL: ").append(getCreateTable(pair.schema.baseTable)).append(System.lineSeparator());
        sb.append("Base table engine: ").append(pair.schema.baseEngine).append(System.lineSeparator());
        sb.append("VP source engine: ").append(pair.schema.sourceEngine).append(System.lineSeparator());
        sb.append("VP left engine: ").append(pair.schema.leftEngine).append(System.lineSeparator());
        sb.append("VP right engine: ").append(pair.schema.rightEngine).append(System.lineSeparator());
        sb.append("Base row count: ").append(pair.schema.baseRowCount).append(System.lineSeparator());
        sb.append("VP source row count: ").append(pair.schema.sourceRowCount).append(System.lineSeparator());
        sb.append("Base shape: ").append(pair.schema.baseShape).append(System.lineSeparator());
        sb.append("Stable row order: ")
                .append(pair.schema.stableOrderStrategy == null ? "NONE" : pair.schema.stableOrderStrategy)
                .append(System.lineSeparator());
        sb.append("Prefix: ").append(pair.schema.prefix).append(System.lineSeparator());
        sb.append("Split strategy: ").append(pair.schema.splitStrategy).append(System.lineSeparator());
        sb.append("Left columns: ").append(columnNames(pair.schema.leftColumns)).append(System.lineSeparator());
        sb.append("Right columns: ").append(columnNames(pair.schema.rightColumns)).append(System.lineSeparator());
        sb.append("Overlap columns: ").append(columnNames(pair.schema.overlapColumns)).append(System.lineSeparator());
        sb.append("Reconstruction key mode: ").append(pair.schema.keyMode).append(System.lineSeparator());
        sb.append("Reconstruction key columns: ").append(columnNames(pair.schema.reconstructionKeyColumns))
                .append(System.lineSeparator());
        sb.append("Physical foreign key: ").append(pair.schema.usesPhysicalForeignKey).append(System.lineSeparator());
        sb.append("Reconstruction strategy: ").append(pair.reconstructionStrategy).append(System.lineSeparator());
        sb.append("Description: ").append(pair.description).append(System.lineSeparator());
        sb.append("Optimizer profile: ").append(pair.optimizerProfile).append(System.lineSeparator());
        if (pair.timeTravelTimestamp != null) {
            sb.append("AS OF SYSTEM TIME: ").append(pair.timeTravelTimestamp).append(System.lineSeparator());
        }
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

    private String formatUnexpectedQueryErrorReport(VpQueryPair pair, String stage, String failingSql, Throwable error)
            throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("[VERTICAL PARTITIONING UNEXPECTED QUERY ERROR]").append(System.lineSeparator());
        sb.append("Mode: RANDOM_TABLE_REWRITE").append(System.lineSeparator());
        sb.append("Stage: ").append(stage).append(System.lineSeparator());
        sb.append("Error text:").append(System.lineSeparator());
        sb.append(getErrorText(error));
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
        sb.append("CockroachDB version: ").append(getSingleString("SELECT version()")).append(System.lineSeparator());
        sb.append("Base table: ").append(pair.schema.baseTable).append(System.lineSeparator());
        sb.append("Base table DDL: ").append(getCreateTable(pair.schema.baseTable)).append(System.lineSeparator());
        sb.append("Base table engine: ").append(pair.schema.baseEngine).append(System.lineSeparator());
        sb.append("VP source engine: ").append(pair.schema.sourceEngine).append(System.lineSeparator());
        sb.append("VP left engine: ").append(pair.schema.leftEngine).append(System.lineSeparator());
        sb.append("VP right engine: ").append(pair.schema.rightEngine).append(System.lineSeparator());
        sb.append("Base row count: ").append(pair.schema.baseRowCount).append(System.lineSeparator());
        sb.append("VP source row count: ").append(pair.schema.sourceRowCount).append(System.lineSeparator());
        sb.append("Base shape: ").append(pair.schema.baseShape).append(System.lineSeparator());
        sb.append("Stable row order: ")
                .append(pair.schema.stableOrderStrategy == null ? "NONE" : pair.schema.stableOrderStrategy)
                .append(System.lineSeparator());
        sb.append("Prefix: ").append(pair.schema.prefix).append(System.lineSeparator());
        sb.append("Split strategy: ").append(pair.schema.splitStrategy).append(System.lineSeparator());
        sb.append("Left columns: ").append(columnNames(pair.schema.leftColumns)).append(System.lineSeparator());
        sb.append("Right columns: ").append(columnNames(pair.schema.rightColumns)).append(System.lineSeparator());
        sb.append("Overlap columns: ").append(columnNames(pair.schema.overlapColumns)).append(System.lineSeparator());
        sb.append("Reconstruction key mode: ").append(pair.schema.keyMode).append(System.lineSeparator());
        sb.append("Reconstruction key columns: ").append(columnNames(pair.schema.reconstructionKeyColumns))
                .append(System.lineSeparator());
        sb.append("Physical foreign key: ").append(pair.schema.usesPhysicalForeignKey).append(System.lineSeparator());
        sb.append("Reconstruction strategy: ").append(pair.reconstructionStrategy).append(System.lineSeparator());
        sb.append("Description: ").append(pair.description).append(System.lineSeparator());
        sb.append("Optimizer profile: ").append(pair.optimizerProfile).append(System.lineSeparator());
        if (pair.timeTravelTimestamp != null) {
            sb.append("AS OF SYSTEM TIME: ").append(pair.timeTravelTimestamp).append(System.lineSeparator());
        }
        sb.append("Session vars: ").append(captureSessionVars()).append(System.lineSeparator());
        sb.append("Session SET statements:").append(System.lineSeparator());
        sb.append(captureSessionSetStatements());
        sb.append("Setup statements:").append(System.lineSeparator());
        for (String setup : pair.schema.setupStatements) {
            sb.append(setup).append(";").append(System.lineSeparator());
        }
        for (String setup : pair.extraSetupStatements) {
            if (!pair.schema.setupStatements.contains(setup)) {
                sb.append(setup).append(";").append(System.lineSeparator());
            }
        }
        sb.append("VP table DDLs:").append(System.lineSeparator());
        appendTableDdl(sb, pair.schema.sourceTable);
        appendTableDdl(sb, pair.schema.leftTable);
        appendTableDdl(sb, pair.schema.rightTable);
        appendTableDdl(sb, pair.schema.viewName);
        appendTableDdl(sb, pair.schema.tempTable);
        sb.append("VP source snapshot:").append(System.lineSeparator());
        sb.append(formatSnapshot(pair.schema.sourceTable, pair.schema.columns));
        sb.append("VP left snapshot:").append(System.lineSeparator());
        sb.append(formatSnapshot(pair.schema.leftTable, pair.schema.leftColumns));
        sb.append("VP right snapshot:").append(System.lineSeparator());
        sb.append(formatSnapshot(pair.schema.rightTable, pair.schema.rightColumns));
        sb.append("Failing SQL: ").append(failingSql).append(System.lineSeparator());
        sb.append("Single query: ").append(pair.singleQuery).append(System.lineSeparator());
        sb.append("Split query: ").append(pair.splitQuery).append(System.lineSeparator());
        sb.append("Failing query plan:").append(System.lineSeparator());
        sb.append(formatExplainTree(failingSql));
        sb.append("Single query plan:").append(System.lineSeparator());
        sb.append(formatExplainTree(pair.singleQuery));
        sb.append("Split query plan:").append(System.lineSeparator());
        sb.append(formatExplainTree(pair.splitQuery));
        sb.append("Java stack trace:").append(System.lineSeparator());
        sb.append(stackTrace(error));
        return sb.toString();
    }

    private void appendTableDdl(StringBuilder sb, String tableName) {
        try {
            sb.append(tableName).append(": ").append(getCreateTable(tableName)).append(System.lineSeparator());
        } catch (SQLException e) {
            sb.append(tableName).append(": <unavailable: ").append(e.getMessage()).append(">")
                    .append(System.lineSeparator());
        }
    }

    private String stackTrace(Throwable throwable) {
        StringWriter sw = new StringWriter();
        throwable.printStackTrace(new PrintWriter(sw));
        return sw.toString();
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

    private String getSingleString(String sql) throws SQLException {
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getString(1);
            }
            return "<none>";
        }
    }

    private String captureSessionVars() {
        List<String> vars = List.of("TimeZone", "vectorize", "optimizer_use_histograms", "enable_zigzag_join",
                "distsql", "opt_split_scan_limit", "serial_normalization");
        return vars.stream().map(this::captureSessionVar).collect(Collectors.joining(", "));
    }

    private String captureSessionVar(String varName) {
        try {
            return varName + "=" + getSingleString("SHOW " + varName);
        } catch (SQLException e) {
            return varName + "=<unavailable: " + e.getMessage() + ">";
        }
    }

    private String captureSessionSetStatements() {
        List<String> vars = List.of("TimeZone", "vectorize", "optimizer_use_histograms", "enable_zigzag_join",
                "distsql", "opt_split_scan_limit", "serial_normalization");
        return vars.stream().map(this::captureSessionSetStatement).collect(Collectors.joining());
    }

    private String captureSessionSetStatement(String varName) {
        try {
            String value = getSingleString("SHOW " + varName);
            return "SET " + varName + " = " + sessionVariableLiteral(value) + ";" + System.lineSeparator();
        } catch (SQLException e) {
            return "-- SET SESSION " + varName + " unavailable: " + e.getMessage() + System.lineSeparator();
        }
    }

    private static String sessionVariableLiteral(String value) {
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
        String explain = "EXPLAIN (OPT) " + query;
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
        executeCleanup("DROP VIEW IF EXISTS " + quote(vp.viewName));
        for (String viewName : vp.viewNames) {
            executeCleanup("DROP VIEW IF EXISTS " + quote(viewName));
        }
        executeCleanup("DROP TABLE IF EXISTS " + quote(vp.tempTable));
        executeCleanup("DROP TABLE IF EXISTS " + quote(vp.rightTable));
        executeCleanup("DROP TABLE IF EXISTS " + quote(vp.leftTable));
        executeCleanup("DROP TABLE IF EXISTS " + quote(vp.sourceTable));
    }

    private void executeCleanup(String sql) throws Exception {
        try {
            executeInternalStatement("cleanup", sql, false);
        } catch (SQLException | AssertionError e) {
            if (isExpectedSetupOrCleanupError("cleanup", e)) {
                recordSkippedError("cleanup", sql, getErrorText(e));
                return;
            }
            throw e;
        }
    }

    private void cleanupIfNoBug(VpSchema vp) throws Exception {
        dropArtifacts(vp);
    }

    private boolean executeSetup(VpSchema vp, String sql) throws Exception {
        boolean success = executeInternalStatement("setup", sql, sql.startsWith("CREATE TABLE"));
        if (success) {
            vp.setupStatements.add(sql);
        }
        return success;
    }

    private void tryExecuteSetup(VpSchema vp, String sql) throws Exception {
        try {
            executeSetup(vp, sql);
        } catch (AssertionError e) {
            if (!isSkippableQueryError(e)) {
                throw e;
            }
            recordSkippedError("setup", sql, getErrorText(e));
        }
    }

    private boolean executeOptional(String sql) {
        try {
            return executeNoSchemaRefresh(sql);
        } catch (Exception | AssertionError ignored) {
            return false;
        }
    }

    private boolean executeNoSchemaRefresh(String sql) throws Exception {
        return executeInternalStatement("setup", sql, false);
    }

    private boolean executeInternalStatement(String stage, String sql, boolean couldAffectSchema) throws Exception {
        SQLQueryAdapter query = new SQLQueryAdapter(sql, errors, couldAffectSchema);
        ExecutionTimer timer = null;
        if (state.getOptions().logExecutionTime()) {
            timer = new ExecutionTimer().start();
        }
        if (state.getOptions().printAllStatements()) {
            System.out.println(query.getLogString());
        }
        if (state.getOptions().logEachSelect()) {
            if (timer != null) {
                state.getLogger().writeCurrentNoLineBreak(query.getLogString());
            } else {
                state.getLogger().writeCurrent(query.getLogString());
            }
        }
        boolean success = false;
        boolean schemaPermitAcquired = false;
        try {
            CockroachDBProvider.acquireVerticalPartitioningSchemaChangePermit(state, query.getQueryString());
            schemaPermitAcquired = query.getQueryString().stripLeading().toUpperCase(Locale.ROOT).startsWith("CREATE ")
                    || query.getQueryString().stripLeading().toUpperCase(Locale.ROOT).startsWith("ALTER ")
                    || query.getQueryString().stripLeading().toUpperCase(Locale.ROOT).startsWith("DROP ")
                    || query.getQueryString().stripLeading().toUpperCase(Locale.ROOT).startsWith("TRUNCATE ");
            for (int attempt = 0; attempt < TRANSIENT_CLUSTER_RETRIES; attempt++) {
                try (Statement s = state.getConnection().createStatement()) {
                    s.execute(query.getQueryString());
                    success = true;
                    break;
                } catch (SQLException e) {
                    if (isTransientClusterAvailabilityError(e) && attempt + 1 < TRANSIENT_CLUSTER_RETRIES) {
                        waitForTransientClusterRecovery(attempt);
                        continue;
                    }
                    if (isTransientClusterAvailabilityError(e)) {
                        recordSkippedError("cluster-transient-" + stage, query.getQueryString(), getErrorText(e));
                        break;
                    }
                    if (isExpectedQueryError(e) || isExpectedSetupOrCleanupError(stage, e)) {
                        recordSkippedError(stage, query.getQueryString(), getErrorText(e));
                        break;
                    }
                    throw e;
                }
            }
        } finally {
            if (schemaPermitAcquired) {
                CockroachDBProvider.releaseVerticalPartitioningSchemaChangePermit(query.getQueryString());
            }
            if (success) {
                Main.nrSuccessfulActions.addAndGet(1);
            } else {
                Main.nrUnsuccessfulActions.addAndGet(1);
            }
        }
        if (state.getOptions().logExecutionTime()) {
            state.getLogger().writeCurrent(" -- " + timer.end().asString());
        }
        if (success && couldAffectSchema) {
            refreshSchemaOrSkipTransient(stage);
        }
        return success;
    }

    private boolean isTransientClusterAvailabilityError(Throwable error) {
        String message = getErrorText(error).toLowerCase(Locale.ROOT);
        return message.contains("replica unavailable") || message.contains("replica has been leaderless")
                || message.contains("leaderless for") || message.contains("closed timestamp")
                || message.contains("slow proposal") || message.contains("txn exec: context canceled")
                || message.contains("txn exec: context cancelled") || message.contains("node liveness error")
                || message.contains("must be after replica gc threshold")
                || message.contains("i/o error occurred while sending to the backend")
                || message.contains("eofexception") || message.contains("connection reset")
                || message.contains("connection closed") || message.contains("terminating connection")
                || message.contains("maximum number of concurrently open transactions")
                || message.contains("too many open transactions");
    }

    private void waitForTransientClusterRecovery(int attempt) throws IgnoreMeException {
        try {
            Thread.sleep(250L * (attempt + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IgnoreMeException();
        }
    }

    private void refreshSchemaOrSkipTransient(String stage) throws Exception {
        for (int attempt = 0; attempt < RESOURCE_READ_RETRIES; attempt++) {
            try {
                state.updateSchema();
                return;
            } catch (SQLException e) {
                if (!isTransientClusterAvailabilityError(e)) {
                    throw e;
                }
                if (attempt + 1 < RESOURCE_READ_RETRIES) {
                    waitForTransientClusterRecovery(attempt);
                    continue;
                }
                recordSkippedError("cluster-transient-" + stage, "UPDATE SCHEMA", getErrorText(e));
                throw new IgnoreMeException();
            }
        }
        throw new AssertionError("CockroachDB VP schema refresh retry loop exhausted");
    }

    private String toSQLType(CockroachDBCompositeDataType type) {
        return type.toString();
    }

    private String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private String sanitizeIdentifier(String identifier) {
        return identifier.replaceAll("[^A-Za-z0-9_]", "_");
    }
}
