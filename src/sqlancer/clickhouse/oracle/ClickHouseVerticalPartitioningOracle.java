package sqlancer.clickhouse.oracle;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseOptions;
import sqlancer.clickhouse.ClickHouseProvider;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
import sqlancer.clickhouse.ClickHouseVisitor;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseColumn;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseLancerDataType;
import sqlancer.clickhouse.ast.ClickHouseExpression.ClickHouseText;
import sqlancer.clickhouse.ast.ClickHouseColumnReference;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseSelect;
import sqlancer.clickhouse.ast.ClickHouseAggregate;
import sqlancer.clickhouse.ast.ClickHouseAggregate.ClickHouseAggregateFunction;
import sqlancer.clickhouse.gen.ClickHouseExpressionGenerator;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;

/**
 * ClickHouse implementation of the VP metamorphic oracle. It deliberately keeps the same lifecycle as the MySQL and
 * TiDB implementations: choose a base shape, enumerate compatible splits, materialize each split, exercise all
 * reconstructions, render an AST outer query, and compare the result as a tolerant multiset.
 */
public final class ClickHouseVerticalPartitioningOracle implements TestOracle<ClickHouseGlobalState> {

    private static final String ROW_ID = "vp_rowid";
    private static final int DEFAULT_SOURCE_ROW_LIMIT = 256;
    private static final Object STATS_LOCK = new Object();
    private static final Object CLUSTER_DDL_LOCK = new Object();
    private static final AtomicLong RUN_ID = new AtomicLong();
    private static final AtomicLong ORACLE_RUN_ID = new AtomicLong();
    private static final AtomicBoolean STATS_HOOK = new AtomicBoolean();
    private static final Map<SplitStrategy, AtomicLong> SPLIT_COUNTS = counters(SplitStrategy.values());
    private static final Map<ReconstructionStrategy, AtomicLong> RECONSTRUCTION_COUNTS = counters(
            ReconstructionStrategy.values());
    private static final Map<String, AtomicLong> RECONSTRUCTION_SEMANTIC_COUNTS = new ConcurrentHashMap<>();
    private static final Map<BaseShape, AtomicLong> SHAPE_COUNTS = counters(BaseShape.values());
    private static final Map<String, AtomicLong> SCHEMA_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, AtomicLong> SETTINGS_PROFILE_COUNTS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> DISTRIBUTED_PROFILE_COUNTS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> DISTRIBUTED_TOPOLOGY_COUNTS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> DISTRIBUTED_RETRY_COUNTS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> DISTRIBUTED_FAILURE_COUNTS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> PHYSICAL_STATE_COUNTS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> ACCESS_PATH_COUNTS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> NATIVE_RELATION_COUNTS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> NATIVE_RELATION_MISMATCH_COUNTS = new ConcurrentHashMap<>();
    private static final Map<String, AtomicLong> COMPARISON_LEVEL_COUNTS = new ConcurrentHashMap<>();
    private static final Map<KnownPattern, AtomicLong> KNOWN_PATTERN_COUNTS = counters(KnownPattern.values());
    private static final Map<AdvancedFeature, AtomicLong> SETUP_ATTEMPT_COUNTS = counters(AdvancedFeature.values());
    private static final Map<AdvancedFeature, AtomicLong> SETUP_SUCCESS_COUNTS = counters(AdvancedFeature.values());
    private static final Map<AdvancedFeature, AtomicLong> QUERY_ATTEMPT_COUNTS = counters(AdvancedFeature.values());
    private static final Map<AdvancedFeature, AtomicLong> QUERY_SUCCESS_COUNTS = counters(AdvancedFeature.values());
    private static final Map<AdvancedFeature, AtomicLong> QUERY_SKIP_COUNTS = counters(AdvancedFeature.values());
    private static final Map<String, AtomicLong> FEATURE_SKIP_REASON_COUNTS = new ConcurrentHashMap<>();
    private static final AtomicLong SCHEMA_SHAPE_SAMPLES = new AtomicLong();
    private static final AtomicLong COMPLETED_RUNS = new AtomicLong();
    private static final AtomicLong COMPLETED_PAIRS = new AtomicLong();
    private static final AtomicLong ATTEMPTS = new AtomicLong();
    private static final AtomicLong TYPED_COMPARISONS = new AtomicLong();
    private static final AtomicLong ORDERED_COMPARISONS = new AtomicLong();
    private static volatile String COVERAGE_CONFIGURATION = "uninitialized";
    private static final List<String> CONFIGURATION_SNAPSHOTS = new ArrayList<>();

    private final ClickHouseGlobalState state;
    private QuerySettingsProfile settingsProfile;
    private String distributedClusterName;
    private final ExpectedErrors setupErrors = ExpectedErrors.newErrors()
            .with(ClickHouseErrors.getExpectedExpressionErrors()).with("Cannot parse").with("Illegal type")
            .with("Not found column").with("Unknown identifier").with("Cannot convert").with("Nested type")
            .with("Memory limit").with("ILLEGAL_TYPE_OF_ARGUMENT").with("No operation ").with("ARGUMENT_OUT_OF_BOUND")
            .with("Decimal value is too big").with("does not support PREWHERE")
            .with("Can't execute any of specified algorithms")
            // ClickHouse 26.7 only supports a subset of correlated-subquery forms. Keep a narrow fallback for
            // analyzer limitations observed while exploring those forms, rather than failing the entire VP run.
            .with("Correlated subqueries are not supported as IN function arguments")
            .with("can't find correlated column").with("Unknown data type: Variant").with("Unknown setting")
            .with("UNKNOWN_SETTING").with("TABLE_ALREADY_EXISTS").withRegexString("Setting .* is not supported")
            // A remote shard can return an HTTP 500/Code 999 while distributed DDL is converging.
            // Treat this as unavailable profile infrastructure, not as a VP semantic failure.
            .with("transport error: 500").with("Code: 999")
            .build();
    private final ExpectedErrors resultErrors = ExpectedErrors.newErrors().with("Memory limit").with("Unknown setting")
            .with("UNKNOWN_SETTING").with("does not support PREWHERE").with("Can't execute any of specified algorithms")
            // ClickHouse 26.7 still rejects a subset of analyzer rewrites that the oracle can generate. These are
            // treated as unsupported query shapes, while conversion/type failures in result queries are not.
            .with("Correlated subqueries are not supported as IN function arguments")
            .with("can't find correlated column").with("Cannot decorrelate query")
            .with("DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED").withRegexString("Setting .* is not supported").build();

    private enum BaseShape {
        EMPTY, SINGLE_COLUMN, MULTI_COLUMN
    }

    private enum SplitStrategy {
        EMPTY_SPLIT, SINGLE_REPLICATED, SINGLE_LEFT_ROW_ID_ONLY, SINGLE_RIGHT_ROW_ID_ONLY, EVEN_SPLIT, OVERLAPPED_SPLIT,
        SKEWED_SPLIT, TYPE_DIRECTED_SPLIT
    }

    private enum ReconstructionStrategy {
        JOIN, LEFT_RIGHT_JOIN, FULL_OUTER_SIMULATION, SCALAR_SUBQUERY, EXISTS_FILTER, IN_FILTER, DERIVED_TABLE, CTE,
        VIEW, MATERIALIZED_MEMORY_TABLE, AGGREGATE_THEN_MERGE, ROW_NUMBER_ALIGNMENT, NATURAL_OR_USING, ANTI_JOIN_FILTER,
        JSON_PACK, STRING_PACK
    }

    private enum KnownPattern {
        NONFINITE_FLOAT, NOT_FOUND_COLUMN_IN_BLOCK, PROJECTION_NOT_USED
    }

    private enum AdvancedFeature {
        SOURCE_SCHEMA_OBJECT_COPY_SUCCESS, SOURCE_SCHEMA_OBJECT_COPY_FALLBACK, QUERY_AGGREGATE, QUERY_GROUPED,
        QUERY_ROLLUP, QUERY_GROUP_ARRAY_STRING_CONCAT, QUERY_WINDOW, QUERY_UNION_ALL, QUERY_CORRELATED_SUBQUERY,
        QUERY_SORTED_TOP_N, QUERY_NULLABILITY, QUERY_LOW_CARDINALITY, HELPER_INDEX_ATTEMPTED, HELPER_INDEX_CREATED,
        HELPER_INDEX_FALLBACK, HELPER_INDEX_MINMAX, HELPER_INDEX_BLOOM_FILTER, HELPER_INDEX_SET,
        HELPER_INDEX_ASYMMETRIC, HELPER_BASE_ENGINE_MEMORY, HELPER_BASE_ENGINE_LOG, HELPER_BASE_ENGINE_MERGETREE,
        HELPER_BASE_ENGINE_REJECTED, HELPER_TABLE_FALLBACK, QUERY_AGGREGATE_VARIANTS, QUERY_TYPE_ORDERING,
        QUERY_BOUNDARY_MUTATION, QUERY_COMPLEX_TYPE_EXPRESSIONS, QUERY_FLOAT_AGGREGATE, QUERY_OPTIMIZER_BUG_PROBE,
        DISTRIBUTED_SOURCE_ACCESS
    }

    private enum ApproximateKind {
        NONE(0.0, 0.0), FLOAT32(1e-6, 1e-5), FLOAT64(1e-12, 1e-9);

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
            return left == FLOAT32 || right == FLOAT32 ? FLOAT32 : left == FLOAT64 || right == FLOAT64 ? FLOAT64 : NONE;
        }
    }

    private static final class QuerySettingsProfile {
        private final String name;
        private final String clause;

        private QuerySettingsProfile(List<String> settings) {
            this.name = settings.stream().map(s -> s.replace('=', '-')).collect(Collectors.joining(","));
            this.clause = " SETTINGS " + String.join(", ", settings);
        }
    }

    private static final class VpColumn {
        private final String name;
        private final String type;

        private VpColumn(String name, String type) {
            this.name = name;
            this.type = type;
        }
    }

    private static final class BaseTableInfo {
        private final String name;
        private final List<VpColumn> columns;
        private final BaseShape shape;
        private final String engine;
        private final List<String> schemaObjects;

        private BaseTableInfo(String name, List<VpColumn> columns, BaseShape shape, String engine,
                List<String> schemaObjects) {
            this.name = name;
            this.columns = columns;
            this.shape = shape;
            this.engine = engine;
            this.schemaObjects = schemaObjects;
        }
    }

    private static final class VpSchema {
        private final String source;
        private final String left;
        private final String right;
        private final String view;
        private final String temp;
        private final List<VpColumn> columns;
        private final List<VpColumn> leftColumns;
        private final List<VpColumn> rightColumns;
        private final List<VpColumn> overlapColumns;
        private final SplitStrategy split;
        private final List<String> setupStatements = new ArrayList<>();
        private final List<DistributedAccessPath> distributedAccessPaths = new ArrayList<>();
        private final List<ReplicatedAccessPath> replicatedAccessPaths = new ArrayList<>();
        private final List<String> mergeStoppedTables = new ArrayList<>();
        private final List<HelperIndex> helperIndexes = new ArrayList<>();
        @SuppressWarnings("unused")
        private String physicalProjection;
        private boolean viewMaterialized;
        private boolean tempMaterialized;

        private VpSchema(String prefix, List<VpColumn> columns, List<VpColumn> leftColumns, List<VpColumn> rightColumns,
                List<VpColumn> overlapColumns, SplitStrategy split) {
            this.source = prefix + "_source";
            this.left = prefix + "_left";
            this.right = prefix + "_right";
            this.view = prefix + "_view";
            this.temp = prefix + "_temp";
            this.columns = columns;
            this.leftColumns = leftColumns;
            this.rightColumns = rightColumns;
            this.overlapColumns = overlapColumns;
            this.split = split;
        }
    }

    private static final class DistributedAccessPath {
        private final String local;
        private final String distributed;

        private DistributedAccessPath(String local, String distributed) {
            this.local = local;
            this.distributed = distributed;
        }
    }

    private static final class ReplicatedAccessPath {
        private final String local;
        private final String distributed;
        private String readReplica;

        private ReplicatedAccessPath(String local, String distributed) {
            this.local = local;
            this.distributed = distributed;
        }
    }

    private static final class HelperIndex {
        private final String table;
        private final String name;

        private HelperIndex(String table, String name) {
            this.table = table;
            this.name = name;
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

    private static final class QueryPair {
        private final String single;
        private final String split;
        private final String description;
        private final boolean orderedComparisonEnabled;
        private final AdvancedFeature feature;
        private final List<ApproximateKind> approximateColumns;

        private QueryPair(String single, String split, String description, boolean orderedComparisonEnabled) {
            this(single, split, description, orderedComparisonEnabled, null);
        }

        private QueryPair(String single, String split, String description, boolean orderedComparisonEnabled,
                AdvancedFeature feature) {
            this(single, split, description, orderedComparisonEnabled, feature, List.of());
        }

        private QueryPair(String single, String split, String description, boolean orderedComparisonEnabled,
                AdvancedFeature feature, List<ApproximateKind> approximateColumns) {
            this.single = single;
            this.split = split;
            this.description = description;
            this.orderedComparisonEnabled = orderedComparisonEnabled;
            this.feature = feature;
            this.approximateColumns = approximateColumns;
        }
    }

    private static final class QueryResultMultiset {
        private final Map<String, Integer> rows;
        private final List<ApproximateKind> approximateColumns;
        private final List<String> orderedRows;
        private final List<String> columnTypes;

        private QueryResultMultiset(Map<String, Integer> rows, List<ApproximateKind> approximateColumns,
                List<String> orderedRows, List<String> columnTypes) {
            this.rows = rows;
            this.approximateColumns = approximateColumns;
            this.orderedRows = orderedRows;
            this.columnTypes = columnTypes;
        }
    }

    private static final class NativeRelationMismatchError extends AssertionError {
        private static final long serialVersionUID = 1L;

        private NativeRelationMismatchError(String message) {
            super(message);
        }
    }

    public ClickHouseVerticalPartitioningOracle(ClickHouseGlobalState state) {
        this.state = state;
        recordCoverageConfiguration(state);
        registerStatsHook();
    }

    /** Registers persistent VP reporting before database generation, where an early connection failure can occur. */
    public static void initializeStatsLogging() {
        registerStatsHook();
        flushStats();
    }

    public static void initializeStatsLogging(ClickHouseGlobalState state) {
        recordCoverageConfiguration(state);
        registerStatsHook();
        flushStats();
    }

    @Override
    public void check() throws Exception {
        long oracleRunId = ORACLE_RUN_ID.incrementAndGet();
        recordRunConfiguration(oracleRunId, state.getDbmsSpecificOptions());
        flushStats();
        settingsProfile = chooseSettingsProfile();
        if (settingsProfile != null) {
            SETTINGS_PROFILE_COUNTS.computeIfAbsent(settingsProfile.name, ignored -> new AtomicLong())
                    .incrementAndGet();
        }
        ClickHouseProvider.ensureVerticalPartitioningBaseTables(state, 2);
        BaseTableInfo base = selectBaseTable();
        int variants = Math.max(1, state.getDbmsSpecificOptions().verticalPartitioningQueryVariantsPerCombination);
        int checked = 0;
        int attempts = 0;
        List<SplitStrategy> splitSchedule = compatibleSplits(base.shape);
        for (SplitStrategy split : splitSchedule) {
            VpSchema vp = materialize(base, split);
            boolean preserveMismatchTables = false;
            try {
                SPLIT_COUNTS.get(split).incrementAndGet();
                runOptimizerBugProbe(base, vp);
                for (ReconstructionStrategy reconstruction : ReconstructionStrategy.values()) {
                    for (int i = 0; i < variants; i++) {
                        attempts++;
                        QueryPair pair = null;
                        try {
                            pair = generatePair(vp, reconstruction);
                            compare(pair);
                            querySuccess(pair.feature);
                            RECONSTRUCTION_COUNTS.get(reconstruction).incrementAndGet();
                            incrementString(RECONSTRUCTION_SEMANTIC_COUNTS, reconstructionSemanticTag(reconstruction));
                            checked++;
                        } catch (IgnoreMeException ignored) {
                            if (pair != null) {
                                querySkip(pair.feature, skipReason(ignored));
                            }
                            // Unsupported dialect combinations are expected exploration skips, not a failed VP run.
                        }
                    }
                }
                if (state.getDbmsSpecificOptions().verticalPartitioningDistributedProfile) {
                    try {
                        runDistributedProfile(vp);
                    } catch (NativeRelationMismatchError error) {
                        throw new AssertionError(error.getMessage()
                                + "\nClickHouse distributed VP replay tables preserved: " + q(vp.source) + ", "
                                + q(vp.left) + ", " + q(vp.right) + distributedReplayNames(vp), error);
                    } catch (LinkageError error) {
                        // An incomplete/stale deployment can fail while resolving the optional distributed-profile
                        // helper classes. This is an environment/build problem, not a ClickHouse mismatch; keep the
                        // core VP relation useful and make the downgrade visible in vp-stats.log.
                        incrementString(FEATURE_SKIP_REASON_COUNTS, "distributed-profile-linkage-error");
                    }
                }
                if (state.getDbmsSpecificOptions().verticalPartitioningReplicatedMergeTreeProfile) {
                    runReplicatedMergeTreeProfile(vp);
                }
                if (state.getDbmsSpecificOptions().verticalPartitioningPhysicalStateProfile
                        || state.getDbmsSpecificOptions().verticalPartitioningPartLayoutProfile) {
                    runPhysicalStateProfile(vp);
                }
            } catch (AssertionError error) {
                preserveMismatchTables = isComparisonMismatch(error);
                if (preserveMismatchTables) {
                    throw new AssertionError(error.getMessage() + "\nVP replay tables preserved: " + q(vp.source) + ", "
                            + q(vp.left) + ", " + q(vp.right), error);
                }
                throw error;
            } finally {
                cleanup(vp, preserveMismatchTables);
            }
        }
        ATTEMPTS.addAndGet(attempts);
        if (checked == 0) {
            throw new IgnoreMeException();
        }
        COMPLETED_RUNS.incrementAndGet();
        COMPLETED_PAIRS.addAndGet(checked);
        // Native relations are supplemental ClickHouse-specific checks. Run them only after the core VP relation has
        // completed so a PREWHERE/FINAL/projection mismatch cannot prevent this oracle from exercising VP at all.
        if (state.getDbmsSpecificOptions().verticalPartitioningNativeRelations) {
            try {
                runNativeRelations(base);
            } catch (NativeRelationMismatchError error) {
                throw new AssertionError(error.getMessage() + "\nClickHouse native relation replay table preserved: "
                        + q(base.name) + "\nCore VP relation completed before native relation checks.", error);
            }
        }
        ClickHouseProvider.replaceVerticalPartitioningTableAfterSuccessfulCheck(state, base.name);
    }

    private BaseTableInfo selectBaseTable() throws Exception {
        state.updateSchema();
        boolean includeNormalWorkload = state.getDbmsSpecificOptions().verticalPartitioningIncludeNormalWorkload;
        List<ClickHouseTable> tables = state.getSchema().getDatabaseTables().stream().filter(t -> !t.isView()).filter(
                t -> t.getName().startsWith("vp_base_") || (includeNormalWorkload && !t.getName().startsWith("vp_")))
                .filter(t -> !t.getColumns().isEmpty()).collect(Collectors.toList());
        if (tables.isEmpty()) {
            throw new IgnoreMeException();
        }
        ClickHouseTable table = Randomly.fromList(tables);
        List<VpColumn> columns = table.getColumns().stream().filter(c -> !c.isAlias() && !c.isMaterialized())
                .map(c -> new VpColumn(c.getName(), c.getType().toString())).collect(Collectors.toList());
        long rowCount = getRowCount(table.getName());
        BaseShape shape = rowCount == 0 ? BaseShape.EMPTY
                : columns.size() == 1 ? BaseShape.SINGLE_COLUMN : BaseShape.MULTI_COLUMN;
        SHAPE_COUNTS.get(shape).incrementAndGet();
        recordSchemaShapeSample();
        schemaShapeCount("schema-total");
        schemaShapeCount("schema-" + shape.name().toLowerCase(Locale.ROOT));
        schemaShapeCount("columns-" + columns.size());
        schemaShapeCount("engine-" + normalizeStatKey(table.getEngine()));
        for (VpColumn column : columns) {
            String type = column.type.toUpperCase(Locale.ROOT);
            if (type.contains("NULLABLE")) {
                schemaShapeCount("nullable-column");
            }
            if (type.contains("LOWCARDINALITY")) {
                schemaShapeCount("low-cardinality-column");
            }
        }
        if (!table.getSchemaObjects().isEmpty()) {
            schemaShapeCount("with-schema-objects");
        }
        return new BaseTableInfo(table.getName(), columns, shape, table.getEngine(), table.getSchemaObjects());
    }

    private long getRowCount(String tableName) throws SQLException {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery("SELECT count() FROM " + q(tableName))) {
            if (!result.next()) {
                throw new SQLException("ClickHouse did not return a row count for " + tableName);
            }
            return result.getLong(1);
        }
    }

    private void runNativeRelations(BaseTableInfo base) throws Exception {
        if (base.columns.isEmpty()) {
            return;
        }
        VpColumn valueColumn = base.columns.get(0);
        String table = q(base.name);
        String value = q(valueColumn.name);
        String predicate = "isNotNull(" + value + ")";
        boolean replacingMergeTree = "ReplacingMergeTree".equalsIgnoreCase(base.engine);
        // ReplacingMergeTree without FINAL does not provide a stable snapshot:
        // background merges can remove duplicate sorting keys between the two
        // statements of a native relation. Comparing WHERE with PREWHERE (or
        // two aggregate/projection rewrites) across that merge is therefore an
        // oracle race, not a database metamorphic relation. FINAL below is the
        // dedicated, stable ReplacingMergeTree relation.
        if (!replacingMergeTree) {
            runNativePair("PREWHERE", "SELECT " + value + " FROM " + table + " WHERE " + predicate,
                    "SELECT " + value + " FROM " + table + " PREWHERE " + predicate);

            String computedExpression = null;
            String computedColumn = null;
            String computedRelation = null;
            try (Statement statement = state.getConnection().createStatement();
                    ResultSet result = statement.executeQuery("DESCRIBE " + table)) {
                while (result.next()) {
                    computedColumn = result.getString("name");
                    String defaultType = result.getString("default_type");
                    if ("ALIAS".equals(defaultType) || "MATERIALIZED".equals(defaultType)) {
                        computedRelation = defaultType;
                        computedExpression = result.getString("default_expression");
                        if (computedExpression != null && !computedExpression.isBlank()) {
                            break;
                        }
                    }
                }
            }
            if (computedExpression != null && !computedExpression.isBlank() && computedRelation != null
                    && computedColumn != null && !computedColumn.isBlank()) {
                runNativePair(computedRelation, "SELECT " + computedExpression + " FROM " + table,
                        "SELECT " + q(computedColumn) + " FROM " + table);
            }

            // A source scan and a reconstructed join may feed an aggregate in a
            // different physical order. SUM/AVG over Float32/Float64 are not
            // associative, in particular around +/- Double.MAX_VALUE, so those
            // comparisons are not valid VP metamorphic relations. Keep native
            // aggregate relations on integral/Decimal inputs, where the aggregate
            // state is order-independent for the values generated here.
            VpColumn numeric = base.columns.stream().filter(this::isOrderStableAggregateInput).findFirst().orElse(null);
            if (numeric != null) {
                String numericValue = q(numeric.name);
                runNativePair("AGGREGATE_IF",
                        "SELECT sumIf(toNullable(" + numericValue + "), " + predicate + ") FROM " + table,
                        "SELECT sum(toNullable(" + numericValue + ")) FROM " + table + " WHERE " + predicate);
                runNativePair(
                        "AGGREGATE_STATE_MERGE", "SELECT sumMerge(state) FROM (SELECT sumState(toNullable("
                                + numericValue + ")) AS state FROM " + table + ")",
                        "SELECT sum(toNullable(" + numericValue + ")) FROM " + table);

                String arrayColumn = base.columns.stream().filter(this::isIntArray).map(c -> q(c.name)).findFirst()
                        .orElse(null);
                if (arrayColumn != null) {
                    runNativePair("AGGREGATE_ARRAY", "SELECT sumArray(" + arrayColumn + ") FROM " + table,
                            "SELECT ifNull(sum(toNullable(arrayValue)), 0) FROM " + table + " ARRAY JOIN " + arrayColumn
                                    + " AS arrayValue");
                }

                runNativePair("AGGREGATE_OR_NULL",
                        "SELECT toNullable(toFloat64(sumOrNull(toNullable(" + numericValue + "))) ) FROM " + table,
                        "SELECT if(count(toNullable(" + numericValue + ")) = 0, CAST(NULL, 'Nullable(Float64)'), "
                                + "toFloat64(sum(toNullable(" + numericValue + ")))) FROM " + table);

                VpColumn resampleValue = base.columns.stream().filter(this::isIntegralNumeric).findFirst().orElse(null);
                VpColumn bucket = base.columns.stream().filter(this::isIntegralNumeric)
                        .filter(c -> resampleValue != null && !c.name.equals(resampleValue.name)).findFirst()
                        .orElse(null);
                if (resampleValue != null && bucket != null) {
                    String resampleNumericValue = q(resampleValue.name);
                    String bucketValue = q(bucket.name);
                    runNativePair("AGGREGATE_RESAMPLE",
                            "SELECT sumResample(0, 4, 2)(toInt64(" + resampleNumericValue + "), toInt64(" + bucketValue
                                    + ")) FROM " + table,
                            "SELECT [ifNull((SELECT sumIf(toInt64(" + resampleNumericValue + "), toInt64(" + bucketValue
                                    + ") >= 0 AND toInt64(" + bucketValue + ") < 2) FROM " + table + "), 0), "
                                    + "ifNull((SELECT sumIf(toInt64(" + resampleNumericValue + "), toInt64("
                                    + bucketValue + ") >= 2 AND toInt64(" + bucketValue + ") < 4) FROM " + table
                                    + "), 0)] FROM numbers(1)");
                }
            }
        } else {
            NATIVE_RELATION_COUNTS.computeIfAbsent("REPLACING_SNAPSHOT_SKIP", ignored -> new AtomicLong())
                    .incrementAndGet();
        }

        if (largeDataExploration() && "MergeTree".equalsIgnoreCase(base.engine) && hasProjection(base.schemaObjects)) {
            runNativePair("PROJECTION", "SELECT " + value + " FROM " + table,
                    "SELECT " + value + " FROM " + table + " SETTINGS force_optimize_projection = 1");
        }
        if (largeDataExploration() && replacingMergeTree && !base.columns.get(0).type.startsWith("Nested(")
                && !isFloatLike(base.columns.get(0))) {
            // ReplacingMergeTree FINAL deduplicates on the sorting key. Compare
            // that exact key expression on both sides; Float keys are skipped
            // here because -0.0/0.0 and non-finite values are not a stable
            // equality surface between FINAL, GROUP BY, and string result reads.
            String finalKey = replacingMergeTreeKey(base.columns.get(0), value);
            runNativePair("FINAL", "SELECT " + finalKey + " FROM " + table + " FINAL",
                    "SELECT " + finalKey + " FROM " + table + " GROUP BY " + finalKey);
        }
    }

    private void runDistributedProfile(VpSchema vp) throws Exception {
        if (!state.getDbmsSpecificOptions().verticalPartitioningDistributedProfile) {
            return;
        }
        if (!prepareDistributedCluster()) {
            return;
        }
        if (!executeDirect("CREATE DATABASE IF NOT EXISTS " + q(state.getDatabaseName()) + " ON CLUSTER "
                + distributedClusterName)) {
            throw new IgnoreMeException();
        }
        if (distributedClusterName == null) {
            return;
        }
        if (state.getDbmsSpecificOptions().verticalPartitioningDistributedTopologyProfile) {
            runDistributedTopologyProfile();
        }
        createDistributedAccessPath(vp, vp.source, vp.columns, "source", true);
        createDistributedAccessPath(vp, vp.left, vp.leftColumns, "left", true);
        createDistributedAccessPath(vp, vp.right, vp.rightColumns, "right", true);
        compareDistributedAccess(vp.source, vp.distributedAccessPaths.get(0).distributed,
                "DISTRIBUTED_SOURCE_ACCESS_SOURCE");
        compareDistributedAccess(vp.left, vp.distributedAccessPaths.get(1).distributed,
                "DISTRIBUTED_SOURCE_ACCESS_LEFT");
        compareDistributedAccess(vp.right, vp.distributedAccessPaths.get(2).distributed,
                "DISTRIBUTED_SOURCE_ACCESS_RIGHT");
        compareDistributedReconstruction(vp, vp.distributedAccessPaths.get(1).distributed,
                vp.distributedAccessPaths.get(2).distributed);
        if (state.getDbmsSpecificOptions().verticalPartitioningDistributedShardKeyProfile) {
            runDistributedShardKeyProfile(vp);
        }
        if (state.getDbmsSpecificOptions().verticalPartitioningMultiGatewayProfile) {
            compareMultiGateway(vp, vp.distributedAccessPaths.get(1).distributed,
                    vp.distributedAccessPaths.get(2).distributed);
        }
        if (state.getDbmsSpecificOptions().verticalPartitioningDistributedRetryProfile) {
            runDistributedRetryProfile(vp);
        }
        if (state.getDbmsSpecificOptions().verticalPartitioningDistributedFailureProfile) {
            runDistributedFailureProfile();
        }
    }

    private void runDistributedShardKeyProfile(VpSchema vp) throws Exception {
        List<String> keys = new ArrayList<>();
        keys.add("rand()");
        keys.add("sipHash64(" + q(ROW_ID) + ")");
        VpColumn business = vp.columns.isEmpty() ? null : vp.columns.get(0);
        // Distributed requires the sharding expression to be a non-null integer.
        // Hashing a Nullable business column directly produces Nullable(UInt64),
        // which ClickHouse rejects while creating the Distributed table.
        if (business != null)
            keys.add("ifNull(cityHash64(" + q(business.name) + "), 0)");
        if (business != null)
            keys.add("ifNull(cityHash64(" + q(ROW_ID) + ", " + q(business.name) + "), 0)");
        long sourceRowCount = getRowCount(vp.source);
        for (int i = 0; i < keys.size(); i++) {
            String role = "route_" + i;
            DistributedAccessPath path = createDistributedAccessPath(vp, vp.source, vp.columns, role, i % 2 == 0,
                    keys.get(i));
            // Asynchronous Distributed inserts are only visible after the local
            // distribution queue has shipped its files to the shards. Comparing
            // immediately here turns normal eventual-consistency into a false
            // VP mismatch (and was the source of the SHARD_KEY_1/3 reports).
            if (i % 2 != 0 && !waitForDistributedQueue(path.distributed, "shard-key")) {
                incrementString(DISTRIBUTED_PROFILE_COUNTS, "shard-key-queue-timeout=" + i);
                continue;
            }
            compareDistributedAccess(vp.source, path.distributed, "DISTRIBUTED_SHARD_KEY_" + i);
            String escaped = distributedClusterName.replace("'", "''");
            String database = state.getDatabaseName().replace("'", "''");
            String table = path.local.replace("'", "''");
            try (Statement statement = state.getConnection().createStatement();
                    ResultSet result = statement
                            .executeQuery("SELECT _shard_num, hostName(), count() FROM clusterAllReplicas('" + escaped
                                    + "', '" + database + "', '" + table + "') GROUP BY _shard_num, hostName()")) {
                long routedRows = 0;
                while (result.next()) {
                    routedRows += result.getLong(3);
                    incrementString(DISTRIBUTED_PROFILE_COUNTS, "shard-route-observed=" + i);
                }
                if (routedRows == 0 && sourceRowCount > 0) {
                    throw new NativeRelationMismatchError("Distributed shard-key route lost all rows: " + keys.get(i));
                }
            }
            incrementString(DISTRIBUTED_PROFILE_COUNTS, "shard-key-profile=" + i);
        }
    }

    private void runReplicatedMergeTreeProfile(VpSchema vp) throws Exception {
        if (!prepareDistributedCluster()) {
            incrementString(PHYSICAL_STATE_COUNTS, "replicated-profile-cluster-unavailable");
            return;
        }
        if (!executeDirect("CREATE DATABASE IF NOT EXISTS " + q(state.getDatabaseName()) + " ON CLUSTER "
                + distributedClusterName)) {
            incrementString(PHYSICAL_STATE_COUNTS, "replicated-profile-database-unavailable");
            return;
        }
        ReplicatedAccessPath source = createReplicatedAccessPath(vp, vp.source, vp.columns, "source");
        ReplicatedAccessPath left = createReplicatedAccessPath(vp, vp.left, vp.leftColumns, "left");
        ReplicatedAccessPath right = createReplicatedAccessPath(vp, vp.right, vp.rightColumns, "right");
        if (!waitForReplicatedQueues(vp)) {
            incrementString(PHYSICAL_STATE_COUNTS, "replicated-queue-not-drained");
            return;
        }
        compareDistributedAccess(vp.source, source.distributed, "REPLICATED_SOURCE_ACCESS_SOURCE");
        compareDistributedReconstruction(vp, left.distributed, right.distributed);
        compareReplicatedLocalReplica(vp, source);
        incrementString(PHYSICAL_STATE_COUNTS, "replicated-vp-compare");
    }

    private void compareReplicatedLocalReplica(VpSchema vp, ReplicatedAccessPath path) throws Exception {
        if (path.readReplica == null) {
            incrementString(PHYSICAL_STATE_COUNTS, "replicated-local-read-skipped-insufficient-replicas");
            return;
        }
        String query = "SELECT * FROM " + q(path.local) + " ORDER BY " + q(ROW_ID);
        String distributed = "SELECT * FROM " + q(path.distributed) + " ORDER BY " + q(ROW_ID);
        try (Connection connection = gatewayConnection(path.readReplica)) {
            QueryResultMultiset local = resultMultiset(connection, query, List.of());
            QueryResultMultiset routed = resultMultiset(distributed, List.of());
            if (!orderedRowsEqual(routed, local) || !routed.columnTypes.equals(local.columnTypes)) {
                throw new NativeRelationMismatchError("ClickHouse replicated local replica mismatch at "
                        + path.readReplica + "\nlocal=" + local.orderedRows + "\nrouted=" + routed.orderedRows);
            }
            incrementString(PHYSICAL_STATE_COUNTS, "replicated-read-other-replica");
            incrementString(PHYSICAL_STATE_COUNTS, "replicated-full-value-compare");
        }
    }

    private ReplicatedAccessPath createReplicatedAccessPath(VpSchema vp, String relation, List<VpColumn> columns,
            String role) throws Exception {
        String local = vp.source + "_rep_" + role + "_local";
        String distributed = vp.source + "_rep_" + role;
        String clusterClause = " ON CLUSTER " + distributedClusterName;
        String definitions = q(ROW_ID) + " UInt64" + optionalDefinitions(columns);
        String names = q(ROW_ID) + optionalNames(columns);
        String keeperPath = "/sqlancer/vp/" + state.getDatabaseName().replaceAll("[^A-Za-z0-9_]", "_") + "/" + local
                + "/{shard}";
        if (!executeDirect("CREATE TABLE " + q(local) + clusterClause + " (" + definitions
                + ") ENGINE = ReplicatedMergeTree('" + keeperPath + "', '{replica}') PARTITION BY cityHash64("
                + q(ROW_ID) + ") % 4 ORDER BY " + q(ROW_ID))) {
            throw new IgnoreMeException();
        }
        if (!executeDirect(
                "CREATE TABLE " + q(distributed) + clusterClause + " AS " + q(local) + " ENGINE = Distributed("
                        + distributedClusterName + ", " + q(state.getDatabaseName()) + ", " + q(local) + ", rand())")) {
            throw new IgnoreMeException();
        }
        ReplicatedAccessPath path = new ReplicatedAccessPath(local, distributed);
        String writeReplica = findReplicaEndpoint(0);
        path.readReplica = findReplicaEndpoint(1);
        String insertSql = "INSERT INTO " + q(local) + " (" + names + ") SELECT " + names + " FROM " + q(relation);
        if (writeReplica != null) {
            try (Connection connection = gatewayConnection(writeReplica);
                    Statement statement = connection.createStatement()) {
                statement.execute(insertSql);
            } catch (SQLException | RuntimeException unavailable) {
                if (!isEndpointUnavailable(unavailable)) {
                    throw unavailable;
                }
                incrementString(PHYSICAL_STATE_COUNTS, "replicated-write-replica-unavailable");
                writeReplica = null;
            }
            if (writeReplica != null) {
                incrementString(PHYSICAL_STATE_COUNTS, "replicated-write-replica");
            }
        }
        if (writeReplica == null && !executeDirect("INSERT INTO " + q(distributed) + " (" + names + ") SELECT " + names
                + " FROM " + q(relation) + " SETTINGS insert_distributed_sync = 1")) {
            throw new IgnoreMeException();
        }
        if (path.readReplica == null) {
            incrementString(PHYSICAL_STATE_COUNTS, "replicated-profile-skipped-insufficient-replicas");
        } else {
            incrementString(PHYSICAL_STATE_COUNTS, "replicated-read-replica-selected");
        }
        vp.replicatedAccessPaths.add(path);
        incrementString(PHYSICAL_STATE_COUNTS, "replicated-helper-created");
        return path;
    }

    private boolean waitForReplicatedQueues(VpSchema vp) throws SQLException, InterruptedException {
        String tables = vp.replicatedAccessPaths.stream().map(path -> "'" + path.local.replace("'", "''") + "'")
                .collect(Collectors.joining(","));
        String cluster = distributedClusterName.replace("'", "''");
        String database = state.getDatabaseName().replace("'", "''");
        for (int attempt = 0; attempt < 40; attempt++) {
            String sql = "SELECT coalesce(sum(queue_size), 0), coalesce(sum(inserts_in_queue), 0), "
                    + "coalesce(min(active_replicas), 0), coalesce(max(total_replicas), 0), "
                    + "coalesce(max(absolute_delay), 0), countIf(is_readonly) FROM clusterAllReplicas('" + cluster
                    + "', system.replicas) WHERE database = '" + database + "' AND table IN (" + tables + ")";
            try (Statement statement = state.getConnection().createStatement();
                    ResultSet result = statement.executeQuery(sql)) {
                if (result.next()) {
                    long queue = result.getLong(1);
                    long inserts = result.getLong(2);
                    long active = result.getLong(3);
                    long total = result.getLong(4);
                    long delay = result.getLong(5);
                    long readonly = result.getLong(6);
                    if (attempt == 0 || (queue == 0 && inserts == 0)) {
                        incrementString(PHYSICAL_STATE_COUNTS, "replicated-queue-size=" + queue);
                        incrementString(PHYSICAL_STATE_COUNTS, "replicated-inserts-in-queue=" + inserts);
                        incrementString(PHYSICAL_STATE_COUNTS, "replicated-active-replicas=" + active);
                        incrementString(PHYSICAL_STATE_COUNTS, "replicated-total-replicas=" + total);
                        incrementString(PHYSICAL_STATE_COUNTS, "replicated-absolute-delay=" + delay);
                    }
                    if (queue == 0 && inserts == 0 && readonly == 0 && total > 0 && active >= total && delay == 0) {
                        incrementString(PHYSICAL_STATE_COUNTS, "replicated-queue-drained");
                        return true;
                    }
                }
            }
            Thread.sleep(50);
        }
        return false;
    }

    private String findReplicaEndpoint(int replicaNumber) {
        String escaped = distributedClusterName.replace("'", "''");
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement
                        .executeQuery("SELECT host_name, port FROM system.clusters WHERE cluster = '" + escaped
                                + "' ORDER BY shard_num, replica_num LIMIT 1 OFFSET " + replicaNumber)) {
            if (result.next()) {
                return result.getString(1) + ":" + result.getInt(2);
            }
        } catch (SQLException error) {
            if (!isEndpointUnavailable(error)) {
                throw new IllegalStateException("Unable to inspect configured replica endpoints", error);
            }
        }
        return null;
    }

    private boolean isEndpointUnavailable(Throwable error) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            String message = current.getMessage();
            if (current instanceof java.net.UnknownHostException || current instanceof java.net.ConnectException
                    || (message != null
                            && (message.contains("Unknown host") || message.contains("Connection refused")))) {
                return true;
            }
        }
        return false;
    }

    private void runDistributedTopologyProfile() throws SQLException {
        String escaped = distributedClusterName.replace("'", "''");
        int shards = 0;
        int replicas = 0;
        Set<String> endpoints = new HashSet<>();
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(
                        "SELECT shard_num, replica_num, host_name, port FROM system.clusters WHERE cluster = '"
                                + escaped + "' ORDER BY shard_num, replica_num")) {
            while (result.next()) {
                shards = Math.max(shards, result.getInt(1));
                replicas++;
                endpoints.add(result.getString(3) + ":" + result.getInt(4));
            }
        }
        incrementString(DISTRIBUTED_TOPOLOGY_COUNTS, "shards=" + shards);
        incrementString(DISTRIBUTED_TOPOLOGY_COUNTS, "replicas=" + replicas);
        incrementString(DISTRIBUTED_TOPOLOGY_COUNTS, "endpoints=" + endpoints.size());
        try (Statement statement = state.getConnection().createStatement(); ResultSet result = statement.executeQuery(
                "SELECT countDistinct(hostName()) FROM clusterAllReplicas('" + escaped + "', system.one)")) {
            int live = result.next() ? result.getInt(1) : 0;
            incrementString(DISTRIBUTED_TOPOLOGY_COUNTS, "live-endpoints=" + live);
            incrementString(DISTRIBUTED_TOPOLOGY_COUNTS,
                    live == endpoints.size() ? "topology-live" : "topology-partial");
        }
    }

    private void runDistributedRetryProfile(VpSchema vp) throws Exception {
        DistributedAccessPath retryPath = createDistributedAccessPath(vp, vp.source, vp.columns, "retry", false);
        incrementString(DISTRIBUTED_RETRY_COUNTS, "async-insert");
        if (!waitForDistributedQueue(retryPath.distributed, "retry")) {
            incrementString(DISTRIBUTED_RETRY_COUNTS, "queue-observation-timeout");
            return;
        }
        compareDistributedAccess(vp.source, retryPath.distributed, "DISTRIBUTED_ASYNC_SOURCE");
        if (!executeDirect(
                "OPTIMIZE TABLE " + q(retryPath.local) + " ON CLUSTER " + distributedClusterName + " FINAL")) {
            incrementString(DISTRIBUTED_RETRY_COUNTS, "optimize-final-failed");
            return;
        }
        incrementString(DISTRIBUTED_RETRY_COUNTS, "optimize-final");
        compareDistributedAccess(vp.source, retryPath.distributed, "DISTRIBUTED_POST_MERGE_SOURCE");
    }

    /**
     * Wait until all files for a Distributed table have been sent and no queue entry reports an error. This is
     * deliberately per-table, so concurrent VP checks do not serialize one another.
     */
    private boolean waitForDistributedQueue(String distributedTable, String profile)
            throws SQLException, InterruptedException {
        String database = state.getDatabaseName().replace("'", "''");
        String table = distributedTable.replace("'", "''");
        String cluster = distributedClusterName.replace("'", "''");
        long errors = 0;
        for (int attempt = 0; attempt < 20; attempt++) {
            try (Statement statement = state.getConnection().createStatement();
                    ResultSet result = statement.executeQuery(
                            "SELECT coalesce(sum(data_files), 0), coalesce(sum(error_count), 0) FROM clusterAllReplicas('"
                                    + cluster + "', system.distribution_queue) WHERE database = '" + database
                                    + "' AND table = '" + table + "'")) {
                long files = 0;
                if (result.next()) {
                    files = result.getLong(1);
                    errors = result.getLong(2);
                }
                if (files == 0 && errors == 0) {
                    incrementString(profile.equals("retry") ? DISTRIBUTED_RETRY_COUNTS : DISTRIBUTED_PROFILE_COUNTS,
                            profile + "-queue-drained");
                    return true;
                }
            }
            Thread.sleep(50);
        }
        if (errors > 0) {
            incrementString(profile.equals("retry") ? DISTRIBUTED_RETRY_COUNTS : DISTRIBUTED_PROFILE_COUNTS,
                    profile + "-queue-error");
        }
        incrementString(profile.equals("retry") ? DISTRIBUTED_RETRY_COUNTS : DISTRIBUTED_PROFILE_COUNTS,
                profile + "-queue-not-drained");
        return false;
    }

    private void runDistributedFailureProfile() throws SQLException {
        String escaped = distributedClusterName.replace("'", "''");
        int minimumReplicas = Integer.MAX_VALUE;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement
                        .executeQuery("SELECT shard_num, count() FROM system.clusters WHERE cluster = '" + escaped
                                + "' GROUP BY shard_num")) {
            while (result.next()) {
                minimumReplicas = Math.min(minimumReplicas, result.getInt(2));
            }
        }
        if (minimumReplicas < 2 || minimumReplicas == Integer.MAX_VALUE) {
            incrementString(DISTRIBUTED_FAILURE_COUNTS, "failover-skipped-no-replica");
            return;
        }
        incrementString(DISTRIBUTED_FAILURE_COUNTS, "failover-capable");
        incrementString(DISTRIBUTED_FAILURE_COUNTS, "failure-injection-skipped");
    }

    private DistributedAccessPath createDistributedAccessPath(VpSchema vp, String relation, List<VpColumn> columns,
            String role, boolean synchronous) throws Exception {
        return createDistributedAccessPath(vp, relation, columns, role, synchronous, "rand()");
    }

    private DistributedAccessPath createDistributedAccessPath(VpSchema vp, String relation, List<VpColumn> columns,
            String role, boolean synchronous, String shardingKey) throws Exception {
        setupAttempt(AdvancedFeature.DISTRIBUTED_SOURCE_ACCESS);
        String suffix = "_dist_" + role;
        String local = vp.source + suffix + "_local";
        String distributed = vp.source + suffix;
        DistributedAccessPath path = new DistributedAccessPath(local, distributed);
        vp.distributedAccessPaths.add(path);
        String clusterClause = " ON CLUSTER " + distributedClusterName;
        String definitions = q(ROW_ID) + " UInt64" + optionalDefinitions(columns);
        String names = q(ROW_ID) + optionalNames(columns);
        // ON CLUSTER DDL is asynchronous; delayed tasks from a prior retry can race with CREATE.
        executeDirect("DROP TABLE IF EXISTS " + q(distributed) + clusterClause);
        executeDirect("DROP TABLE IF EXISTS " + q(local) + clusterClause);
        if (!executeDirect("CREATE TABLE " + q(local) + clusterClause + " (" + definitions
                + ") ENGINE = MergeTree ORDER BY tuple()")) {
            throw new IgnoreMeException();
        }
        if (!executeDirect("CREATE TABLE " + q(distributed) + clusterClause + " AS " + q(local)
                + " ENGINE = Distributed(" + distributedClusterName + ", " + q(state.getDatabaseName()) + ", "
                + q(local) + ", " + shardingKey + ")")) {
            throw new IgnoreMeException();
        }
        String settings = synchronous ? " SETTINGS insert_distributed_sync = 1"
                : " SETTINGS insert_distributed_sync = 0";
        if (!executeDirect("INSERT INTO " + q(distributed) + " (" + names + ") SELECT " + names + " FROM " + q(relation)
                + settings)) {
            throw new IgnoreMeException();
        }
        setupSuccess(AdvancedFeature.DISTRIBUTED_SOURCE_ACCESS);
        vp.setupStatements.add("-- distributed VP access " + role + ": " + local + ", " + distributed);
        return path;
    }

    private String distributedReplayNames(VpSchema vp) {
        if (vp.distributedAccessPaths.isEmpty()) {
            return "";
        }
        return ", distributed=" + vp.distributedAccessPaths.stream()
                .map(path -> q(path.local) + "/" + q(path.distributed)).collect(Collectors.joining(","));
    }

    private void compareDistributedAccess(String relation, String distributed, String role) throws Exception {
        queryAttempt(AdvancedFeature.DISTRIBUTED_SOURCE_ACCESS);
        try {
            compareDistributedCardinality(relation, distributed, role);
            compareDistributedRows(relation, distributed, role);
            compareDistributedFingerprint(relation, distributed, role);
            querySuccess(AdvancedFeature.DISTRIBUTED_SOURCE_ACCESS);
        } catch (IgnoreMeException ignored) {
            querySkip(AdvancedFeature.DISTRIBUTED_SOURCE_ACCESS, "EXPECTED_OTHER");
        }
    }

    private void compareMultiGateway(VpSchema vp, String distributedLeft, String distributedRight) throws Exception {
        List<String> gateways = new ArrayList<>();
        String escaped = distributedClusterName.replace("'", "''");
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement
                        .executeQuery("SELECT host_name, port FROM system.clusters WHERE cluster = '" + escaped
                                + "' ORDER BY shard_num, replica_num")) {
            while (result.next() && gateways.size() < 3) {
                String endpoint = result.getString(1) + ":" + result.getInt(2);
                if (!gateways.contains(endpoint)) {
                    gateways.add(endpoint);
                }
            }
        }
        if (gateways.size() < 2) {
            incrementString(DISTRIBUTED_PROFILE_COUNTS, "multi-gateway-skipped-insufficient-endpoints");
            return;
        }
        String reconstructed = "SELECT * FROM (" + reconstructSelectFrom(vp, q(distributedLeft)
                + " AS l GLOBAL INNER JOIN " + q(distributedRight) + " AS r ON l." + q(ROW_ID) + " = r." + q(ROW_ID))
                + ") AS s ORDER BY " + q(ROW_ID);
        String source = "SELECT * FROM " + q(vp.source) + " ORDER BY " + q(ROW_ID);
        // vp.source only exists on the initiating gateway. It is the reference result, not a relation that can be
        // queried directly through other gateways. Each remote gateway evaluates the Distributed reconstruction.
        QueryResultMultiset baselineSource = resultMultiset(source, List.of());
        QueryResultMultiset baselineReconstructed = resultMultiset(reconstructed, List.of());
        assertGatewayEqual(baselineSource, baselineReconstructed, "initiating gateway");
        for (String gateway : gateways) {
            try (Connection connection = gatewayConnection(gateway)) {
                QueryResultMultiset gatewayReconstructed = resultMultiset(connection, reconstructed, List.of());
                assertGatewayEqual(baselineReconstructed, gatewayReconstructed, gateway + " reconstructed");
                assertGatewayEqual(baselineSource, gatewayReconstructed, gateway + " reconstruction");
                incrementString(DISTRIBUTED_PROFILE_COUNTS, "multi-gateway-full-value-compare");
            } catch (SQLException | RuntimeException error) {
                incrementString(DISTRIBUTED_PROFILE_COUNTS, "multi-gateway-unavailable");
            } catch (AssertionError error) {
                // resultMultiset wraps endpoint connection failures as AssertionError. A configured hostname
                // that is not resolvable is an environment capability issue, not a VP result mismatch.
                if (error.getMessage() != null && error.getMessage().contains("VP result query failed")) {
                    incrementString(DISTRIBUTED_PROFILE_COUNTS, "multi-gateway-unavailable");
                } else {
                    throw error;
                }
            }
        }
    }

    private Connection gatewayConnection(String endpoint) throws SQLException {
        int separator = endpoint.lastIndexOf(':');
        String host = separator < 0 ? endpoint : endpoint.substring(0, separator);
        int port = separator < 0 ? 8123 : Integer.parseInt(endpoint.substring(separator + 1));
        // system.clusters reports container DNS names and the native port (9000). SQLancer usually runs on
        // the host, where those names are not resolvable and HTTP is exposed on 8123..8128. Translate the
        // bundled clickhouseN topology to its published host endpoint before opening JDBC.
        if (host.matches("clickhouse[1-9][0-9]*")) {
            int ordinal = Integer.parseInt(host.substring("clickhouse".length()));
            host = "127.0.0.1";
            port = 8122 + ordinal;
        }
        String url = "jdbc:clickhouse://" + host + ":" + port + "/" + state.getDatabaseName()
                + "?socket_timeout=300000&compress=false";
        return DriverManager.getConnection(url, state.getOptions().getUserName(), state.getOptions().getPassword());
    }

    private void assertGatewayEqual(QueryResultMultiset expected, QueryResultMultiset actual, String gateway) {
        if (!orderedRowsEqual(expected, actual) || !expected.columnTypes.equals(actual.columnTypes)) {
            throw new NativeRelationMismatchError("ClickHouse multi-gateway full-value mismatch at " + gateway
                    + "\nexpected rows=" + expected.orderedRows + "\nactual rows=" + actual.orderedRows);
        }
    }

    private void compareDistributedCardinality(String relation, String distributed, String role) throws SQLException {
        String left = cardinalityFingerprint(relation);
        String right = cardinalityFingerprint(distributed);
        if (!left.equals(right)) {
            throw new NativeRelationMismatchError("ClickHouse distributed cardinality mismatch (" + role + ")\nsource: "
                    + relation + " count/uniq=" + left + "\ndistributed: " + distributed + " count/uniq=" + right);
        }
        incrementString(COMPARISON_LEVEL_COUNTS, "level1-count-uniq");
    }

    private String cardinalityFingerprint(String relation) throws SQLException {
        String sql = "SELECT count(), uniqExact(" + q(ROW_ID) + ") FROM " + q(relation);
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            if (!result.next()) {
                throw new SQLException("ClickHouse cardinality query returned no row: " + sql);
            }
            return result.getLong(1) + ":" + result.getLong(2);
        }
    }

    private void compareDistributedReconstruction(VpSchema vp, String distributedLeft, String distributedRight)
            throws Exception {
        // Two Distributed tables are rejected by ClickHouse's default distributed_product_mode=deny. GLOBAL JOIN
        // makes the right side a broadcast subquery and preserves the intended VP reconstruction semantics without
        // changing the server-wide setting.
        String reconstructed = reconstructSelectFrom(vp, q(distributedLeft) + " AS l GLOBAL INNER JOIN "
                + q(distributedRight) + " AS r ON l." + q(ROW_ID) + " = r." + q(ROW_ID));
        String sourceQuery = "SELECT * FROM " + q(vp.source) + " ORDER BY " + q(ROW_ID);
        String distributedQuery = "SELECT * FROM (" + reconstructed + ") AS s ORDER BY " + q(ROW_ID);
        QueryResultMultiset source = resultMultiset(sourceQuery, List.of());
        QueryResultMultiset split = resultMultiset(distributedQuery, List.of());
        if (!orderedRowsEqual(source, split)) {
            throw new NativeRelationMismatchError("ClickHouse distributed VP reconstruction mismatch\nsource: "
                    + sourceQuery + "\nreconstructed: " + distributedQuery + "\nsource rows=" + source.orderedRows
                    + "\nreconstructed rows=" + split.orderedRows);
        }
        incrementString(DISTRIBUTED_PROFILE_COUNTS, "distributed-reconstruction-vp-compare");
    }

    private void compareDistributedRows(String relation, String distributed, String role) throws Exception {
        QueryResultMultiset source = resultMultiset("SELECT * FROM " + q(relation) + " ORDER BY " + q(ROW_ID),
                List.of());
        QueryResultMultiset routed = resultMultiset("SELECT * FROM " + q(distributed) + " ORDER BY " + q(ROW_ID),
                List.of());
        if (!orderedRowsEqual(source, routed)) {
            throw new NativeRelationMismatchError("ClickHouse distributed full-row mismatch (" + role + ")\nsource: "
                    + relation + "\ndistributed: " + distributed + "\nsource rows=" + source.orderedRows
                    + "\ndistributed rows=" + routed.orderedRows + "\nsource column-types=" + source.columnTypes
                    + "\ndistributed column-types=" + routed.columnTypes);
        }
        incrementString(COMPARISON_LEVEL_COUNTS, "level2-row-aligned");
        incrementString(DISTRIBUTED_PROFILE_COUNTS, "full-row-compare=" + role);
    }

    private void compareDistributedFingerprint(String relation, String distributed, String role) throws SQLException {
        String source = distributedFingerprint(relation);
        String routed = distributedFingerprint(distributed);
        if (!source.equals(routed)) {
            throw new NativeRelationMismatchError(
                    "ClickHouse distributed row fingerprint mismatch (" + role + ")\nsource: " + relation
                            + " fingerprint=" + source + "\ndistributed: " + distributed + " fingerprint=" + routed);
        }
        incrementString(COMPARISON_LEVEL_COUNTS, "level3-canonical-hash");
        incrementString(DISTRIBUTED_PROFILE_COUNTS, "row-fingerprint-compare=" + role);
    }

    private String distributedFingerprint(String relation) throws SQLException {
        String sql = "SELECT count(), groupBitXor(cityHash64(tuple(*))) FROM " + q(relation);
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            if (!result.next()) {
                throw new SQLException("ClickHouse distributed fingerprint returned no row: " + sql);
            }
            return result.getString(1) + ":" + result.getString(2);
        }
    }

    private void cleanupDistributedAccessPath(DistributedAccessPath path) {
        try {
            executeDirect("DROP TABLE IF EXISTS " + q(path.distributed) + " ON CLUSTER " + distributedClusterName);
        } catch (Exception | AssertionError ignored) {
        }
        try {
            executeDirect("DROP TABLE IF EXISTS " + q(path.local) + " ON CLUSTER " + distributedClusterName);
        } catch (Exception | AssertionError ignored) {
        }
    }

    private boolean prepareDistributedCluster() throws Exception {
        // Probe on every VP split. A ClickHouse cluster can lose a shard after the first split; caching a successful
        // probe would turn the subsequent distributed checks into misleading setup failures or false mismatches.
        distributedClusterName = null;
        String configured = state.getDbmsSpecificOptions().verticalPartitioningDistributedCluster;
        String cluster = configured == null || configured.isBlank() ? null : configured.trim();
        int configuredNodes = 0;
        if (cluster != null) {
            // An explicit cluster is an invariant of the run. Do not silently replace it with another
            // (usually the built-in single-node "default") cluster when the probe cannot find it.
            try (Statement statement = state.getConnection().createStatement();
                    ResultSet result = statement.executeQuery("SELECT count() FROM system.clusters WHERE cluster = '"
                            + cluster.replace("'", "''") + "'")) {
                if (result.next()) {
                    configuredNodes = result.getInt(1);
                }
            } catch (SQLException e) {
                configuredNodes = 0;
            }
            if (configuredNodes == 0) {
                synchronized (STATS_LOCK) {
                    incrementString(DISTRIBUTED_PROFILE_COUNTS, "configured-cluster-not-found");
                }
                return false;
            }
            distributedClusterName = cluster;
        }
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery("SELECT cluster, count() AS nodes FROM system.clusters "
                        + (cluster == null ? "" : "WHERE cluster = '" + cluster.replace("'", "''") + "' ")
                        + "GROUP BY cluster ORDER BY nodes DESC, cluster LIMIT 1")) {
            if (result.next()) {
                distributedClusterName = result.getString(1);
                if (cluster == null) {
                    configuredNodes = result.getInt(2);
                }
            }
        } catch (SQLException e) {
            synchronized (STATS_LOCK) {
                incrementString(DISTRIBUTED_PROFILE_COUNTS, "cluster-probe-skipped");
            }
            return false;
        }
        if (distributedClusterName == null || distributedClusterName.isBlank()) {
            synchronized (STATS_LOCK) {
                incrementString(DISTRIBUTED_PROFILE_COUNTS, "cluster-not-found");
            }
            return false;
        }
        synchronized (STATS_LOCK) {
            incrementString(DISTRIBUTED_PROFILE_COUNTS, "cluster=" + distributedClusterName);
            incrementString(DISTRIBUTED_PROFILE_COUNTS, "configured-nodes=" + configuredNodes);
        }
        int minimum = Math.max(2, state.getDbmsSpecificOptions().verticalPartitioningDistributedMinLiveNodes);
        int liveNodes;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery("SELECT countDistinct(hostName()) FROM clusterAllReplicas('"
                        + distributedClusterName.replace("'", "''") + "', system.one)")) {
            if (!result.next()) {
                liveNodes = 0;
            } else {
                liveNodes = result.getInt(1);
            }
        } catch (SQLException e) {
            synchronized (STATS_LOCK) {
                incrementString(DISTRIBUTED_PROFILE_COUNTS, "cluster-probe-live-skipped");
            }
            distributedClusterName = null;
            return false;
        }
        synchronized (STATS_LOCK) {
            incrementString(DISTRIBUTED_PROFILE_COUNTS, "live-nodes=" + liveNodes);
        }
        if (liveNodes < minimum) {
            synchronized (STATS_LOCK) {
                incrementString(DISTRIBUTED_PROFILE_COUNTS, "cluster-skipped-insufficient-nodes");
            }
            distributedClusterName = null;
            return false;
        }
        return true;
    }

    private String replacingMergeTreeKey(VpColumn column, String value) {
        String type = column.type.toUpperCase(Locale.ROOT);
        // ReplacingMergeTree FINAL deduplicates by the table's sorting key, not
        // by a display serialization of that key. Keep this expression in sync
        // with ClickHouseProvider.verticalPartitioningOrderKey(): converting a
        // Float/Decimal key to String can merge distinct values and makes the
        // native relation report a false mismatch.
        if (type.startsWith("ARRAY(") || type.startsWith("MAP(") || type.startsWith("TUPLE(")
                || type.startsWith("NESTED(") || type.startsWith("ENUM8(") || type.startsWith("ENUM16(")
                || type.startsWith("DATETIME64(") || type.startsWith("DECIMAL256(") || type.equals("UUID")
                || type.equals("IPV4") || type.equals("IPV6") || type.startsWith("LOWCARDINALITY(ENUM8(")) {
            return "toString(" + value + ")";
        }
        if (type.startsWith("NULLABLE(")) {
            String inner = type.substring("NULLABLE(".length(), type.length() - 1);
            if (inner.startsWith("STRING") || inner.startsWith("FIXEDSTRING")) {
                return "ifNull(" + value + ", '')";
            }
            if (inner.startsWith("FLOAT") || inner.startsWith("DECIMAL") || inner.startsWith("INT")
                    || inner.startsWith("UINT")) {
                return "ifNull(" + value + ", 0.)";
            }
            return "ifNull(" + value + ", 0)";
        }
        return value;
    }

    private void runNativePair(String relation, String left, String right) throws Exception {
        try {
            QueryResultMultiset leftResult = resultMultiset(left, List.of());
            QueryResultMultiset rightResult = resultMultiset(right, List.of());
            boolean equal = "AGGREGATE_RESAMPLE".equals(relation) ? resampleResultsEqual(leftResult, rightResult)
                    : multisetsEqual(leftResult, rightResult);
            if (!equal) {
                String message = "ClickHouse native relation mismatch (" + relation + ")\nleft: " + left + "\nright: "
                        + right + "\nleft rows=" + leftResult.rows + "\nright rows=" + rightResult.rows
                        + "\nleft column-types=" + leftResult.columnTypes + "\nright column-types="
                        + rightResult.columnTypes;
                KnownPattern known = knownPattern(message);
                if (state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns && known != null) {
                    KNOWN_PATTERN_COUNTS.get(known).incrementAndGet();
                    return;
                }
                NATIVE_RELATION_MISMATCH_COUNTS.computeIfAbsent(relation, ignored -> new AtomicLong())
                        .incrementAndGet();
                throw new NativeRelationMismatchError(message);
            }
            NATIVE_RELATION_COUNTS.computeIfAbsent(relation, ignored -> new AtomicLong()).incrementAndGet();
        } catch (IgnoreMeException ignored) {
            NATIVE_RELATION_COUNTS.computeIfAbsent(relation + "_SKIP", ignored2 -> new AtomicLong()).incrementAndGet();
        }
    }

    private boolean resampleResultsEqual(QueryResultMultiset left, QueryResultMultiset right) {
        if (!left.columnTypes.equals(right.columnTypes)) {
            return false;
        }
        if (left.rows.size() != right.rows.size()) {
            return false;
        }
        Map<String, Integer> remaining = new LinkedHashMap<>(right.rows);
        for (Map.Entry<String, Integer> entry : left.rows.entrySet()) {
            String match = remaining.keySet().stream()
                    .filter(candidate -> resampleRowApproximatelyEqual(entry.getKey(), candidate)).findFirst()
                    .orElse(null);
            if (match == null || remaining.get(match) < entry.getValue()) {
                return false;
            }
            int count = remaining.get(match) - entry.getValue();
            if (count == 0) {
                remaining.remove(match);
            } else {
                remaining.put(match, count);
            }
        }
        return remaining.isEmpty();
    }

    private boolean resampleRowApproximatelyEqual(String left, String right) {
        List<String> leftValues = decode(left);
        List<String> rightValues = decode(right);
        if (leftValues.size() != 1 || rightValues.size() != 1) {
            return false;
        }
        String leftArray = leftValues.get(0);
        String rightArray = rightValues.get(0);
        if (leftArray == null || rightArray == null) {
            return leftArray == rightArray;
        }
        leftArray = unwrapArrayValue(leftArray);
        rightArray = unwrapArrayValue(rightArray);
        if (leftArray == null || rightArray == null) {
            return java.util.Objects.equals(leftArray, rightArray);
        }
        String[] leftElements = leftArray.split(",", -1);
        String[] rightElements = rightArray.split(",", -1);
        if (leftElements.length != rightElements.length) {
            return false;
        }
        for (int i = 0; i < leftElements.length; i++) {
            try {
                double l = Double.parseDouble(leftElements[i].trim());
                double r = Double.parseDouble(rightElements[i].trim());
                double scale = Math.max(Math.abs(l), Math.abs(r));
                if (Math.abs(l - r) > 1e-9 * Math.max(1.0, scale)) {
                    return false;
                }
            } catch (NumberFormatException e) {
                if (!leftElements[i].trim().equals(rightElements[i].trim())) {
                    return false;
                }
            }
        }
        return true;
    }

    private String unwrapArrayValue(String value) {
        String result = value.trim();
        while (result.length() >= 2 && result.charAt(0) == '(' && result.charAt(result.length() - 1) == ')') {
            result = result.substring(1, result.length() - 1).trim();
        }
        if (result.length() < 2 || result.charAt(0) != '[' || result.charAt(result.length() - 1) != ']') {
            return null;
        }
        return result.substring(1, result.length() - 1);
    }

    private List<SplitStrategy> compatibleSplits(BaseShape shape) {
        List<SplitStrategy> splits = new ArrayList<>();
        if (shape == BaseShape.EMPTY) {
            splits.add(SplitStrategy.EMPTY_SPLIT);
        } else if (shape == BaseShape.SINGLE_COLUMN) {
            Collections.addAll(splits, SplitStrategy.SINGLE_REPLICATED, SplitStrategy.SINGLE_LEFT_ROW_ID_ONLY,
                    SplitStrategy.SINGLE_RIGHT_ROW_ID_ONLY);
        } else {
            Collections.addAll(splits, SplitStrategy.EVEN_SPLIT, SplitStrategy.OVERLAPPED_SPLIT,
                    SplitStrategy.SKEWED_SPLIT, SplitStrategy.TYPE_DIRECTED_SPLIT);
        }
        Collections.shuffle(splits);
        return splits;
    }

    private VpSchema materialize(BaseTableInfo base, SplitStrategy split) throws Exception {
        SplitPlan splitPlan = createSplitPlan(base.columns, split);
        // Include a process-unique timestamp so delayed ON CLUSTER DDL from a previous run cannot collide after
        // the in-process counter is reset on restart.
        String prefix = "vp_" + System.nanoTime() + "_" + RUN_ID.incrementAndGet();
        VpSchema vp = new VpSchema(prefix, base.columns, splitPlan.leftColumns, splitPlan.rightColumns,
                splitPlan.overlapColumns, split);
        String sourceDefinition = q(ROW_ID) + " UInt64" + optionalDefinitions(base.columns);
        createHelperTable(vp, vp.source, sourceDefinition, base.engine);
        copySchemaObjectsToSource(vp, base.schemaObjects);
        createHelperTable(vp, vp.left, q(ROW_ID) + " UInt64" + optionalDefinitions(vp.leftColumns), base.engine);
        createHelperTable(vp, vp.right, q(ROW_ID) + " UInt64" + optionalDefinitions(vp.rightColumns), base.engine);
        boolean physicalProfile = state.getDbmsSpecificOptions().verticalPartitioningPhysicalStateProfile
                || state.getDbmsSpecificOptions().verticalPartitioningPartLayoutProfile;
        if (physicalProfile && isMergeTreeEngine(base.engine)) {
            stopHelperMerges(vp);
        }
        String sourceNames = base.columns.isEmpty() ? q(ROW_ID) : sourceColumns(base.columns) + ", " + q(ROW_ID);
        String sourceSelect = base.columns.isEmpty() ? "row_number() OVER (ORDER BY tuple())"
                : sourceColumns(base.columns) + ", row_number() OVER (ORDER BY tuple())";
        // The helper schema deliberately puts the row identifier first so it is convenient for joins and indexes,
        // while its population SELECT puts it last. Keep the mapping explicit; otherwise ClickHouse assigns by
        // position and can silently corrupt compatible numeric columns or reject FixedString source tables.
        String sourceLimit = sourceRowLimitClause();
        if (physicalProfile && isMergeTreeEngine(base.engine)) {
            insertPhysicalParts(vp, vp.source, sourceNames, sourceSelect, base.name, sourceLimit);
        } else {
            executeSetup(vp, "INSERT INTO " + q(vp.source) + " (" + sourceNames + ") SELECT " + sourceSelect + " FROM "
                    + q(base.name) + sourceLimit);
        }
        maybeCreateHelperIndexes(vp);
        if (physicalProfile) {
            addPhysicalLayoutIndexes(vp);
        }
        if (physicalProfile && isMergeTreeEngine(base.engine)) {
            insertHelperPhysicalParts(vp, vp.left, vp.leftColumns);
            insertHelperPhysicalParts(vp, vp.right, vp.rightColumns);
        } else {
            executeSetup(vp, "INSERT INTO " + q(vp.left) + " SELECT " + q(ROW_ID) + optionalNames(vp.leftColumns)
                    + " FROM " + q(vp.source));
            executeSetup(vp, "INSERT INTO " + q(vp.right) + " SELECT " + q(ROW_ID) + optionalNames(vp.rightColumns)
                    + " FROM " + q(vp.source));
        }
        return vp;
    }

    private boolean isMergeTreeEngine(String engine) {
        return engine != null
                && (engine.equalsIgnoreCase("MergeTree") || engine.equalsIgnoreCase("ReplacingMergeTree"));
    }

    private void insertPhysicalParts(VpSchema vp, String target, String targetNames, String select, String source,
            String sourceLimit) throws Exception {
        int parts = Math.max(2,
                state.getDbmsSpecificOptions().verticalPartitioningPartLayoutProfile
                        ? state.getDbmsSpecificOptions().verticalPartitioningPartInsertsPerTable
                        : state.getDbmsSpecificOptions().verticalPartitioningPhysicalStateInsertParts);
        long rows;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery("SELECT count() FROM " + q(source))) {
            rows = result.next() ? result.getLong(1) : 0;
        }
        int configuredLimit = state.getDbmsSpecificOptions().verticalPartitioningSourceRowLimit;
        if (!largeDataExploration() && configuredLimit > 0) {
            rows = Math.min(rows, configuredLimit);
        }
        if (rows == 0) {
            executeSetup(vp, "INSERT INTO " + q(target) + " (" + targetNames + ") SELECT " + select + " FROM "
                    + q(source) + sourceLimit);
            return;
        }
        long batch = Math.max(1, (rows + parts - 1) / parts);
        for (long offset = 0; offset < rows; offset += batch) {
            long length = Math.min(batch, rows - offset);
            String sql = "INSERT INTO " + q(target) + " (" + targetNames + ") SELECT " + select + " FROM " + q(source)
                    + " LIMIT " + offset + ", " + length;
            executeSetup(vp, sql);
            incrementString(PHYSICAL_STATE_COUNTS, "part-insert");
        }
    }

    private void insertHelperPhysicalParts(VpSchema vp, String target, List<VpColumn> columns) throws Exception {
        int parts = Math.max(2,
                state.getDbmsSpecificOptions().verticalPartitioningPartLayoutProfile
                        ? state.getDbmsSpecificOptions().verticalPartitioningPartInsertsPerTable
                        : state.getDbmsSpecificOptions().verticalPartitioningPhysicalStateInsertParts);
        long rows;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery("SELECT count() FROM " + q(vp.source))) {
            rows = result.next() ? result.getLong(1) : 0;
        }
        if (rows == 0) {
            executeSetup(vp, "INSERT INTO " + q(target) + " SELECT " + q(ROW_ID) + optionalNames(columns) + " FROM "
                    + q(vp.source));
            return;
        }
        long batch = Math.max(1, (rows + parts - 1) / parts);
        for (long offset = 0; offset < rows; offset += batch) {
            long length = Math.min(batch, rows - offset);
            String sql = "INSERT INTO " + q(target) + " SELECT " + q(ROW_ID) + optionalNames(columns) + " FROM "
                    + q(vp.source) + " ORDER BY " + q(ROW_ID) + " LIMIT " + offset + ", " + length;
            executeSetup(vp, sql);
            incrementString(PHYSICAL_STATE_COUNTS, "helper-part-insert");
        }
    }

    private void runPhysicalStateProfile(VpSchema vp) throws Exception {
        if (vp.mergeStoppedTables.isEmpty()) {
            incrementString(PHYSICAL_STATE_COUNTS, "not-merge-tree");
            return;
        }
        if (!verifyMultipleActiveParts(vp)) {
            incrementString(PHYSICAL_STATE_COUNTS, "pre-merge-parts-unavailable");
            return;
        }
        incrementString(PHYSICAL_STATE_COUNTS, "pre-merge-parts-verified");
        incrementString(PHYSICAL_STATE_COUNTS, "pre-merge-vp-compare");
        QueryPair preMergePair = generatePair(vp, ReconstructionStrategy.JOIN);
        compare(preMergePair);
        incrementString(PHYSICAL_STATE_COUNTS, "pre-merge-vp-compared");
        boolean partLayoutOnly = state.getDbmsSpecificOptions().verticalPartitioningPartLayoutProfile
                && !state.getDbmsSpecificOptions().verticalPartitioningPhysicalStateProfile;
        if (partLayoutOnly) {
            if (state.getDbmsSpecificOptions().verticalPartitioningPartitionPruningProfile) {
                verifyPartitionPruning(vp);
            }
            if (state.getDbmsSpecificOptions().verticalPartitioningAutoMergeProfile) {
                startHelperMerges(vp);
                waitForAutomaticMerges(vp);
                incrementString(PHYSICAL_STATE_COUNTS, "auto-merge-wait");
            }
            if (state.getDbmsSpecificOptions().verticalPartitioningOptimizeFinalProfile) {
                optimizePhysicalTables(vp);
            }
            compare(preMergePair);
            incrementString(PHYSICAL_STATE_COUNTS, "post-merge-vp-compare");
            return;
        }
        verifyHelperIndexUse(vp);
        verifySourceProjectionUse(vp);
        if (state.getDbmsSpecificOptions().verticalPartitioningPartitionPruningProfile) {
            verifyPartitionPruning(vp);
        }
        // Mutations require merge processing to make progress when mutations_sync=1. Temporarily resume merges for
        // the lifecycle transition, then stop them again so the post-mutation physical comparison still observes
        // stable multi-part state.
        startHelperMerges(vp);
        try {
            runMutationLifecycleProfile(vp);
        } finally {
            stopHelperMerges(vp);
        }
        if (state.getDbmsSpecificOptions().verticalPartitioningAutoMergeProfile) {
            startHelperMerges(vp);
            waitForAutomaticMerges(vp);
            incrementString(PHYSICAL_STATE_COUNTS, "auto-merge-wait");
        } else {
            startHelperMerges(vp);
        }
        optimizePhysicalTables(vp);
        QueryPair pair = generatePair(vp, ReconstructionStrategy.JOIN);
        compare(pair);
        incrementString(PHYSICAL_STATE_COUNTS, "post-merge-vp-compare");
    }

    private void optimizePhysicalTables(VpSchema vp) throws Exception {
        if (!state.getDbmsSpecificOptions().verticalPartitioningOptimizeFinalProfile)
            return;
        for (String table : List.of(vp.source, vp.left, vp.right)) {
            if (!executeDirect("OPTIMIZE TABLE " + q(table) + " FINAL")) {
                incrementString(PHYSICAL_STATE_COUNTS, "optimize-skipped");
                return;
            }
        }
        incrementString(PHYSICAL_STATE_COUNTS, "optimize-final");
    }

    private void waitForAutomaticMerges(VpSchema vp) throws SQLException, InterruptedException {
        for (int attempt = 0; attempt < 20; attempt++) {
            boolean stable = true;
            for (String table : List.of(vp.source, vp.left, vp.right)) {
                try (Statement statement = state.getConnection().createStatement();
                        ResultSet result = statement.executeQuery(
                                "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = '"
                                        + table.replace("'", "''") + "'")) {
                    if (result.next() && result.getLong(1) > 0)
                        stable = false;
                }
            }
            if (stable)
                return;
            Thread.sleep(100);
        }
        incrementString(PHYSICAL_STATE_COUNTS, "auto-merge-timeout");
    }

    private void verifyPartitionPruning(VpSchema vp) {
        String table = vp.source;
        String sql = "EXPLAIN indexes = 1 SELECT count() FROM " + q(table) + " WHERE " + q(ROW_ID) + " = 1";
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            StringBuilder plan = new StringBuilder();
            while (result.next())
                plan.append(result.getString(1)).append('\n');
            String text = plan.toString().toLowerCase(Locale.ROOT);
            incrementString(PHYSICAL_STATE_COUNTS,
                    text.contains("part") ? "partition-pruning-observed" : "partition-pruning-unconfirmed");
        } catch (SQLException ignored) {
            incrementString(PHYSICAL_STATE_COUNTS, "partition-pruning-unavailable");
        }
    }

    private void runMutationLifecycleProfile(VpSchema vp) throws Exception {
        VpColumn column = vp.columns.stream().filter(this::isLifecycleMutationColumn).findFirst().orElse(null);
        if (column == null) {
            incrementString(PHYSICAL_STATE_COUNTS, "mutation-lifecycle-no-compatible-column");
            return;
        }
        String assignment = lifecycleMutationAssignment(column);
        String predicate = q(ROW_ID) + " % 2 = 0";
        long sourceRows = countRows(vp.source);
        long matchingRows = countRows(vp.source + " WHERE " + predicate);
        if (sourceRows == 0 || matchingRows == 0) {
            incrementString(PHYSICAL_STATE_COUNTS, "mutation-lifecycle-skipped-zero-match");
            return;
        }
        mutateLifecycleTable(vp.source, column, assignment, predicate);
        if (contains(vp.leftColumns, column.name)) {
            mutateLifecycleTable(vp.left, column, assignment, predicate);
        }
        if (contains(vp.rightColumns, column.name)) {
            mutateLifecycleTable(vp.right, column, assignment, predicate);
        }
        if (countRows(vp.source) != sourceRows) {
            throw new NativeRelationMismatchError("Mutation changed source row count");
        }
        incrementString(PHYSICAL_STATE_COUNTS, "mutation-lifecycle-row-count-stable");
        QueryPair pair = generatePair(vp, ReconstructionStrategy.JOIN);
        compare(pair);
        incrementString(PHYSICAL_STATE_COUNTS, "mutation-lifecycle-post-mutation-compare");
    }

    private boolean isLifecycleMutationColumn(VpColumn column) {
        String type = scalarType(column);
        return type.equals("STRING") || type.startsWith("INT") || type.startsWith("UINT") || type.startsWith("FLOAT")
                || type.startsWith("DECIMAL");
    }

    private String lifecycleMutationAssignment(VpColumn column) {
        String value = q(column.name);
        String type = scalarType(column);
        if (type.equals("STRING")) {
            return "if(isNull(" + value + "), " + value + ", concat(" + value + ", '_vp_mut'))";
        }
        return "if(isNull(" + value + "), " + value + ", " + value + " + 1)";
    }

    private void mutateLifecycleTable(String table, VpColumn column, String assignment, String predicate)
            throws Exception {
        String sql = "ALTER TABLE " + q(table) + " UPDATE " + q(column.name) + " = " + assignment + " WHERE "
                + predicate + " SETTINGS mutations_sync = 1";
        if (!executeDirect(sql)) {
            throw new IgnoreMeException();
        }
        incrementString(PHYSICAL_STATE_COUNTS, "mutation-lifecycle-applied");
    }

    private long countRows(String relation) throws SQLException {
        String sql = "SELECT count() FROM " + q(relation);
        int where = relation.indexOf(" WHERE ");
        if (where >= 0) {
            sql = "SELECT count() FROM " + q(relation.substring(0, where)) + relation.substring(where);
        }
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            return result.next() ? result.getLong(1) : 0;
        }
    }

    private void stopHelperMerges(VpSchema vp) throws Exception {
        for (String table : List.of(vp.source, vp.left, vp.right)) {
            if (!executeDirect("SYSTEM STOP MERGES " + q(table))) {
                startHelperMerges(vp);
                incrementString(PHYSICAL_STATE_COUNTS, "stop-merges-unavailable");
                return;
            }
            vp.mergeStoppedTables.add(table);
        }
        incrementString(PHYSICAL_STATE_COUNTS, "merges-stopped");
    }

    private void addPhysicalLayoutIndexes(VpSchema vp) {
        addPhysicalLayoutIndex(vp, vp.source, "source");
        addPhysicalLayoutIndex(vp, vp.left, "left");
        addPhysicalLayoutIndex(vp, vp.right, "right");
    }

    private void addPhysicalLayoutIndex(VpSchema vp, String table, String role) {
        String name = vp.source + "_" + role + "_rowid_idx";
        String sql = "ALTER TABLE " + q(table) + " ADD INDEX " + q(name) + " " + q(ROW_ID)
                + " TYPE minmax GRANULARITY 1";
        try {
            if (executeDirect(sql)) {
                vp.setupStatements.add(sql);
                vp.helperIndexes.add(new HelperIndex(table, name));
                incrementString(ACCESS_PATH_COUNTS, "skip-index-created");
            } else {
                incrementString(ACCESS_PATH_COUNTS, "skip-index-create-skipped");
            }
        } catch (SQLException | AssertionError ignored) {
            incrementString(ACCESS_PATH_COUNTS, "skip-index-create-skipped");
        }
    }

    private void verifyHelperIndexUse(VpSchema vp) throws Exception {
        for (HelperIndex index : vp.helperIndexes) {
            String escapedName = index.name.replace("'", "''");
            String query = "SELECT count() FROM " + q(index.table) + " WHERE " + q(ROW_ID)
                    + " = 1 SETTINGS force_data_skipping_indices = '" + escapedName + "'";
            String explain = "EXPLAIN indexes = 1 " + query;
            try (Statement statement = state.getConnection().createStatement();
                    ResultSet result = statement.executeQuery(explain)) {
                StringBuilder plan = new StringBuilder();
                while (result.next()) {
                    plan.append(result.getString(1)).append('\n');
                }
                if (!plan.toString().contains(index.name)) {
                    incrementString(ACCESS_PATH_COUNTS, "skip-index-explain-not-used");
                    continue;
                }
                incrementString(ACCESS_PATH_COUNTS, "skip-index-explain-used");
                QueryResultMultiset normal = resultMultiset("SELECT " + q(ROW_ID) + " FROM " + q(index.table)
                        + " WHERE " + q(ROW_ID) + " = 1 ORDER BY " + q(ROW_ID), List.of());
                QueryResultMultiset forced = resultMultiset(
                        "SELECT " + q(ROW_ID) + " FROM " + q(index.table) + " WHERE " + q(ROW_ID) + " = 1 ORDER BY "
                                + q(ROW_ID) + " SETTINGS force_data_skipping_indices = '" + escapedName + "'",
                        List.of());
                if (!orderedRowsEqual(normal, forced)) {
                    throw new NativeRelationMismatchError("ClickHouse skip-index result mismatch: " + index.name);
                }
                incrementString(ACCESS_PATH_COUNTS, "skip-index-forced-query-ok");
                incrementString(ACCESS_PATH_COUNTS, "skip-index-result-compare");
            } catch (SQLException | AssertionError ignored) {
                incrementString(ACCESS_PATH_COUNTS, "skip-index-force-unavailable");
            }
        }
    }

    private void verifySourceProjectionUse(VpSchema vp) {
        String projection = vp.source + "_rowid_projection";
        String add = "ALTER TABLE " + q(vp.source) + " ADD PROJECTION " + q(projection) + " (SELECT " + q(ROW_ID)
                + " ORDER BY " + q(ROW_ID) + ")";
        try {
            if (!executeDirect(add) || !executeDirect("ALTER TABLE " + q(vp.source) + " MATERIALIZE PROJECTION "
                    + q(projection) + " SETTINGS mutations_sync = 1")) {
                incrementString(ACCESS_PATH_COUNTS, "projection-create-skipped");
                return;
            }
            vp.physicalProjection = projection;
            incrementString(ACCESS_PATH_COUNTS, "projection-created");
            String predicate = q(ROW_ID) + " % 4 = 0";
            String sourceQuery = "SELECT " + q(ROW_ID) + " FROM " + q(vp.source) + " WHERE " + predicate
                    + " SETTINGS force_optimize_projection = 1";
            String explain = explain(sourceQuery);
            if (!explain.contains(projection)) {
                incrementString(ACCESS_PATH_COUNTS, "projection-explain-not-used");
                return;
            }
            incrementString(ACCESS_PATH_COUNTS, "projection-explain-used");
            String splitQuery = "SELECT " + q(ROW_ID) + " FROM "
                    + reconstructionRelation(vp, ReconstructionStrategy.JOIN) + " WHERE " + predicate;
            String normalQuery = "SELECT " + q(ROW_ID) + " FROM " + q(vp.source) + " WHERE " + predicate
                    + " SETTINGS force_optimize_projection = 0";
            QueryResultMultiset normal = resultMultiset(normalQuery, List.of());
            QueryResultMultiset source = resultMultiset(sourceQuery, List.of());
            if (!orderedRowsEqual(normal, source)) {
                throw new NativeRelationMismatchError("ClickHouse projection normal/forced result mismatch");
            }
            incrementString(ACCESS_PATH_COUNTS, "projection-normal-forced-compare");
            QueryResultMultiset split = resultMultiset(splitQuery, List.of());
            if (!multisetsEqual(source, split)) {
                throw new NativeRelationMismatchError(
                        "ClickHouse projection VP mismatch\nsource: " + sourceQuery + "\nreconstructed: " + splitQuery);
            }
            incrementString(ACCESS_PATH_COUNTS, "projection-forced-vp-compare");
        } catch (NativeRelationMismatchError error) {
            throw error;
        } catch (SQLException | AssertionError ignored) {
            incrementString(ACCESS_PATH_COUNTS, "projection-force-unavailable");
        } catch (Exception ignored) {
            incrementString(ACCESS_PATH_COUNTS, "projection-force-unavailable");
        }
    }

    private String explain(String query) throws SQLException {
        StringBuilder plan = new StringBuilder();
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery("EXPLAIN indexes = 1 " + query)) {
            while (result.next()) {
                plan.append(result.getString(1)).append('\n');
            }
        }
        return plan.toString();
    }

    private void startHelperMerges(VpSchema vp) {
        for (String table : vp.mergeStoppedTables) {
            try {
                executeDirect("SYSTEM START MERGES " + q(table));
            } catch (SQLException | AssertionError ignored) {
                // Cleanup will still attempt to drop the helper table. Do not turn a physical-profile capability
                // limitation into a VP mismatch.
            }
        }
        if (!vp.mergeStoppedTables.isEmpty()) {
            incrementString(PHYSICAL_STATE_COUNTS, "merges-started");
            vp.mergeStoppedTables.clear();
        }
    }

    private boolean verifyMultipleActiveParts(VpSchema vp) throws SQLException {
        for (String table : vp.mergeStoppedTables) {
            String escapedTable = table.replace("'", "''");
            try (Statement statement = state.getConnection().createStatement();
                    ResultSet result = statement.executeQuery(
                            "SELECT count() FROM system.parts WHERE active AND database = currentDatabase() AND table = '"
                                    + escapedTable + "'")) {
                long parts = result.next() ? result.getLong(1) : 0;
                incrementString(PHYSICAL_STATE_COUNTS, "active-parts=" + parts);
                if (parts < 2) {
                    return false;
                }
            }
        }
        return true;
    }

    private void createHelperTable(VpSchema vp, String table, String definition, String baseEngine) throws Exception {
        String preferred = "CREATE TABLE " + q(table) + " (" + definition + ") " + helperEngine(baseEngine);
        try {
            executeSetup(vp, preferred);
            setupSuccess(helperBaseEngineFeature(baseEngine));
        } catch (Exception failure) {
            // Engines such as Distributed or ReplicatedMergeTree are not portable helper-table engines. Keep the
            // complete attempted statement, then retain the logical relation with the standard local engine.
            String fallback = "CREATE TABLE " + q(table) + " (" + definition + ") ENGINE = MergeTree ORDER BY "
                    + (largeDataExploration() ? q(ROW_ID) : "tuple()");
            // Memory and Log are valid row-preserving helper engines. Do not silently
            // replace them with MergeTree when the base-engine exploration is enabled.
            if (state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine && baseEngine != null
                    && (baseEngine.equalsIgnoreCase("Memory") || baseEngine.equalsIgnoreCase("Log"))) {
                throw failure;
            }
            if (preferred.equals(fallback)) {
                throw failure;
            }
            setupAttempt(AdvancedFeature.HELPER_TABLE_FALLBACK);
            vp.setupStatements.add("-- fallback after failed helper table: " + preferred);
            executeSetup(vp, fallback);
            setupSuccess(AdvancedFeature.HELPER_TABLE_FALLBACK);
        }
    }

    private AdvancedFeature helperBaseEngineFeature(String baseEngine) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine || baseEngine == null) {
            return null;
        }
        switch (baseEngine.toUpperCase(Locale.ROOT)) {
        case "MEMORY":
            return AdvancedFeature.HELPER_BASE_ENGINE_MEMORY;
        case "LOG":
            return AdvancedFeature.HELPER_BASE_ENGINE_LOG;
        case "MERGETREE":
        case "REPLACINGMERGETREE":
            return AdvancedFeature.HELPER_BASE_ENGINE_MERGETREE;
        default:
            return null;
        }
    }

    private void copySchemaObjectsToSource(VpSchema vp, List<String> schemaObjects) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSourceCopySchemaObjects) {
            return;
        }
        for (String object : schemaObjects) {
            String sql = "ALTER TABLE " + q(vp.source) + " ADD " + object;
            try {
                if (tryExecute(sql)) {
                    vp.setupStatements.add(sql);
                    setupAttempt(AdvancedFeature.SOURCE_SCHEMA_OBJECT_COPY_SUCCESS);
                    setupSuccess(AdvancedFeature.SOURCE_SCHEMA_OBJECT_COPY_SUCCESS);
                } else {
                    vp.setupStatements.add("-- skipped source schema object: " + sql);
                    setupAttempt(AdvancedFeature.SOURCE_SCHEMA_OBJECT_COPY_FALLBACK);
                }
            } catch (Exception ignored) {
                vp.setupStatements.add("-- skipped source schema object: " + sql);
                setupAttempt(AdvancedFeature.SOURCE_SCHEMA_OBJECT_COPY_FALLBACK);
            }
        }
    }

    private void maybeCreateHelperIndexes(VpSchema vp) {
        if (!advancedEnabled()) {
            return;
        }
        // One sampled skip index keeps a VP invocation query-heavy. The table, expression order, and index family
        // vary across checks, giving the same long-running coverage without nine ALTER TABLE operations per split.
        switch (Randomly.fromOptions("source", "left", "right")) {
        case "source":
            addOptionalHelperIndex(vp, vp.source, vp.columns, "src", false);
            break;
        case "left":
            addOptionalHelperIndex(vp, vp.left, vp.leftColumns, "left", true);
            break;
        case "right":
        default:
            addOptionalHelperIndex(vp, vp.right, vp.rightColumns, "right", false);
            break;
        }
    }

    private void addOptionalHelperIndex(VpSchema vp, String table, List<VpColumn> columns, String suffix,
            boolean preferRowId) {
        VpColumn column = columns.stream().filter(this::isNumeric).findFirst()
                .orElseGet(() -> columns.stream().filter(this::isTextual).findFirst().orElse(null));
        boolean minmax = column != null;
        String expression;
        String indexType;
        if (preferRowId || column == null) {
            expression = q(ROW_ID);
            indexType = "minmax";
            minmax = true;
        } else if (isTextual(column)) {
            expression = q(column.name);
            indexType = "bloom_filter(0.01)";
            minmax = false;
        } else {
            expression = q(column.name);
            indexType = "minmax";
        }
        String variant = column != null && isNumeric(column) ? Randomly.fromOptions("base", "asymmetric", "set")
                : Randomly.fromOptions("base", "asymmetric");
        if ("base".equals(variant)) {
            addOptionalHelperIndex(vp, table, suffix, expression, indexType,
                    minmax ? AdvancedFeature.HELPER_INDEX_MINMAX : AdvancedFeature.HELPER_INDEX_BLOOM_FILTER);
        } else if ("asymmetric".equals(variant)) {
            String asymmetric = column == null || preferRowId ? q(ROW_ID)
                    : "tuple(" + q(column.name) + ", " + q(ROW_ID) + ")";
            if (column != null && preferRowId) {
                asymmetric = "tuple(" + q(ROW_ID) + ", " + q(column.name) + ")";
            }
            addOptionalHelperIndex(vp, table, suffix + "_asym", asymmetric, "minmax",
                    AdvancedFeature.HELPER_INDEX_ASYMMETRIC);
        } else {
            addOptionalHelperIndex(vp, table, suffix + "_set", q(column.name), "set(100)",
                    AdvancedFeature.HELPER_INDEX_SET);
        }
    }

    private void addOptionalHelperIndex(VpSchema vp, String table, String suffix, String expression, String indexType,
            AdvancedFeature kind) {
        String sql = "ALTER TABLE " + q(table) + " ADD INDEX " + q(vp.source + "_" + suffix + "_idx") + " " + expression
                + " TYPE " + indexType + " GRANULARITY 1";
        setupAttempt(AdvancedFeature.HELPER_INDEX_ATTEMPTED);
        try {
            if (tryExecute(sql)) {
                vp.setupStatements.add(sql);
                setupSuccess(AdvancedFeature.HELPER_INDEX_CREATED);
                setupAttempt(kind);
                setupSuccess(kind);
            } else {
                vp.setupStatements.add("-- skipped helper index: " + sql);
                setupAttempt(AdvancedFeature.HELPER_INDEX_FALLBACK);
            }
        } catch (Exception ignored) {
            vp.setupStatements.add("-- skipped helper index: " + sql);
            setupAttempt(AdvancedFeature.HELPER_INDEX_FALLBACK);
        }
    }

    private boolean isTextual(VpColumn column) {
        String type = column.type.toUpperCase(Locale.ROOT);
        return type.contains("STRING") || type.contains("FIXEDSTRING") || type.contains("LOWCARDINALITY");
    }

    private SplitPlan createSplitPlan(List<VpColumn> columns, SplitStrategy split) {
        List<VpColumn> left = new ArrayList<>();
        List<VpColumn> right = new ArrayList<>();
        List<VpColumn> overlap = new ArrayList<>();
        if (columns.isEmpty()) {
            return new SplitPlan(left, right, overlap);
        }
        switch (split) {
        case SINGLE_REPLICATED:
            left.addAll(columns);
            right.addAll(columns);
            overlap.addAll(columns);
            break;
        case SINGLE_LEFT_ROW_ID_ONLY:
            right.addAll(columns);
            break;
        case SINGLE_RIGHT_ROW_ID_ONLY:
            left.addAll(columns);
            break;
        case OVERLAPPED_SPLIT:
            splitEven(columns, left, right);
            VpColumn overlapped = Randomly.fromList(columns);
            addIfMissing(left, overlapped);
            addIfMissing(right, overlapped);
            overlap.add(overlapped);
            break;
        case SKEWED_SPLIT:
            List<VpColumn> skewed = new ArrayList<>(columns);
            Collections.shuffle(skewed);
            left.add(skewed.get(0));
            right.addAll(skewed.subList(1, skewed.size()));
            if (right.isEmpty()) {
                right.add(skewed.get(0));
                overlap.add(skewed.get(0));
            }
            break;
        case TYPE_DIRECTED_SPLIT:
            for (VpColumn column : columns) {
                (isNumeric(column) ? left : right).add(column);
            }
            if (left.isEmpty() || right.isEmpty()) {
                left.clear();
                right.clear();
                splitEven(columns, left, right);
            }
            break;
        case EVEN_SPLIT:
        default:
            splitEven(columns, left, right);
            break;
        }
        return new SplitPlan(left, right, overlap);
    }

    private void splitEven(List<VpColumn> columns, List<VpColumn> left, List<VpColumn> right) {
        List<VpColumn> shuffled = new ArrayList<>(columns);
        Collections.shuffle(shuffled);
        for (int i = 0; i < shuffled.size(); i++) {
            (i % 2 == 0 ? left : right).add(shuffled.get(i));
        }
        if (right.isEmpty() && !left.isEmpty()) {
            right.add(left.get(0));
        }
    }

    private static void addIfMissing(List<VpColumn> target, VpColumn column) {
        if (target.stream().noneMatch(existing -> existing.name.equals(column.name))) {
            target.add(column);
        }
    }

    private QueryPair generatePair(VpSchema vp, ReconstructionStrategy reconstruction) throws Exception {
        QueryShape shape = chooseQueryShape(vp, reconstruction);
        String sourceRelation = q(vp.source) + " AS s";
        String rebuilt = reconstructionRelation(vp, reconstruction);
        String single = renderOuterQuery(sourceRelation, shape);
        String split = renderOuterQuery(rebuilt, shape);
        if (reconstruction == ReconstructionStrategy.CTE) {
            split = cteQuery(vp, shape);
        }
        return new QueryPair(single, split,
                "split=" + vp.split + ", overlap="
                        + vp.overlapColumns.stream().map(c -> c.name).collect(Collectors.joining("|")) + ", setup="
                        + vp.setupStatements.size() + ", reconstruction=" + reconstruction + ", " + shape.kind,
                orderedComparisonEnabled(shape), shape.feature, shape.approximateColumns);
    }

    private enum QueryKind {
        PROJECTION, DISTINCT, FILTER, AGGREGATE, GROUPED, ROLLUP, GROUP_ARRAY_STRING_CONCAT, NULLABILITY,
        LOW_CARDINALITY, TYPE_ORDERING, BOUNDARY_MUTATION, SORTED_TOP_N, WINDOW, UNION_ALL, CORRELATED
    }

    private enum CorrelatedVariant {
        EXISTS_NOT_NULL, EXISTS_NULL, EXISTS_VALUE_MATCH
    }

    private static final class QueryShape {
        private final QueryKind kind;
        private final ClickHouseSelect.SelectType selectType;
        private final List<ClickHouseExpression> fetch;
        private final ClickHouseExpression where;
        private final List<ClickHouseExpression> groupBy;
        private final ClickHouseExpression having;
        private final List<ClickHouseExpression> orderBy;
        private final boolean limit;
        private final AdvancedFeature feature;
        private final List<ClickHouseExpression> unionAllFetch;
        private final ClickHouseExpression unionAllWhere;
        private final CorrelatedVariant correlatedVariant;
        private final ClickHouseColumnReference correlatedColumn;
        private final List<ApproximateKind> approximateColumns;

        private QueryShape(QueryKind kind, ClickHouseSelect.SelectType selectType, List<ClickHouseExpression> fetch,
                ClickHouseExpression where, List<ClickHouseExpression> groupBy, ClickHouseExpression having,
                List<ClickHouseExpression> orderBy, boolean limit) {
            this(kind, selectType, fetch, where, groupBy, having, orderBy, limit, null, List.of(), null, null, null);
        }

        private QueryShape(QueryKind kind, ClickHouseSelect.SelectType selectType, List<ClickHouseExpression> fetch,
                ClickHouseExpression where, List<ClickHouseExpression> groupBy, ClickHouseExpression having,
                List<ClickHouseExpression> orderBy, boolean limit, AdvancedFeature feature) {
            this(kind, selectType, fetch, where, groupBy, having, orderBy, limit, feature, List.of(), null, null, null);
        }

        private QueryShape(QueryKind kind, ClickHouseSelect.SelectType selectType, List<ClickHouseExpression> fetch,
                ClickHouseExpression where, List<ClickHouseExpression> groupBy, ClickHouseExpression having,
                List<ClickHouseExpression> orderBy, boolean limit, AdvancedFeature feature,
                List<ApproximateKind> approximateColumns) {
            this(kind, selectType, fetch, where, groupBy, having, orderBy, limit, feature, List.of(), null, null, null,
                    approximateColumns);
        }

        private QueryShape(QueryKind kind, ClickHouseSelect.SelectType selectType, List<ClickHouseExpression> fetch,
                ClickHouseExpression where, List<ClickHouseExpression> groupBy, ClickHouseExpression having,
                List<ClickHouseExpression> orderBy, boolean limit, AdvancedFeature feature,
                List<ClickHouseExpression> unionAllFetch, ClickHouseExpression unionAllWhere,
                CorrelatedVariant correlatedVariant, ClickHouseColumnReference correlatedColumn) {
            this(kind, selectType, fetch, where, groupBy, having, orderBy, limit, feature, unionAllFetch, unionAllWhere,
                    correlatedVariant, correlatedColumn, List.of());
        }

        private QueryShape(QueryKind kind, ClickHouseSelect.SelectType selectType, List<ClickHouseExpression> fetch,
                ClickHouseExpression where, List<ClickHouseExpression> groupBy, ClickHouseExpression having,
                List<ClickHouseExpression> orderBy, boolean limit, AdvancedFeature feature,
                List<ClickHouseExpression> unionAllFetch, ClickHouseExpression unionAllWhere,
                CorrelatedVariant correlatedVariant, ClickHouseColumnReference correlatedColumn,
                List<ApproximateKind> approximateColumns) {
            this.kind = kind;
            this.selectType = selectType;
            this.fetch = fetch;
            this.where = where;
            this.groupBy = groupBy;
            this.having = having;
            this.orderBy = orderBy;
            this.limit = limit;
            this.feature = feature;
            this.unionAllFetch = unionAllFetch;
            this.unionAllWhere = unionAllWhere;
            this.correlatedVariant = correlatedVariant;
            this.correlatedColumn = correlatedColumn;
            this.approximateColumns = approximateColumns;
        }
    }

    private QueryShape chooseQueryShape(VpSchema vp, ReconstructionStrategy reconstruction) {
        List<ClickHouseColumnReference> columns = expressionColumns(vp);
        ClickHouseExpressionGenerator generator = new ClickHouseExpressionGenerator(state).allowAggregates(false);
        boolean advanced = advancedEnabled();
        boolean deep = state.getDbmsSpecificOptions().verticalPartitioningDeepExploration;
        boolean scalarNumericSafeMode = !deep && reconstruction == ReconstructionStrategy.SCALAR_SUBQUERY
                && vp.columns.stream().anyMatch(this::isNumeric);
        List<QueryKind> kinds = new ArrayList<>(List.of(QueryKind.PROJECTION, QueryKind.DISTINCT, QueryKind.FILTER));
        if (advanced) {
            Collections.addAll(kinds, QueryKind.AGGREGATE, QueryKind.GROUPED, QueryKind.ROLLUP,
                    QueryKind.GROUP_ARRAY_STRING_CONCAT, QueryKind.TYPE_ORDERING, QueryKind.BOUNDARY_MUTATION,
                    QueryKind.SORTED_TOP_N);
            if (columns.stream().anyMatch(
                    column -> column.getColumn().getType().toString().toUpperCase(Locale.ROOT).contains("NULLABLE"))) {
                kinds.add(QueryKind.NULLABILITY);
            }
            if (columns.stream().anyMatch(column -> column.getColumn().getType().toString().toUpperCase(Locale.ROOT)
                    .contains("LOWCARDINALITY"))) {
                kinds.add(QueryKind.LOW_CARDINALITY);
            }
        }
        if (deep) {
            Collections.addAll(kinds, QueryKind.WINDOW, QueryKind.UNION_ALL, QueryKind.CORRELATED);
        }
        QueryKind kind = Randomly.fromList(kinds);
        ClickHouseExpression selected = Randomly.fromList(columns);
        ClickHouseExpression predicate = scalarNumericSafeMode ? scalarSafePredicate(columns)
                : stablePredicate(columns);
        List<ClickHouseExpression> projection = scalarNumericSafeMode ? scalarSafeProjection(columns)
                : randomAstProjection(generator, columns);
        List<ClickHouseExpression> orderBy = Randomly.getBoolean()
                ? List.of(new ClickHouseText(orderExpression(Randomly.fromList(columns)))) : List.of();
        switch (kind) {
        case DISTINCT:
            return new QueryShape(kind, ClickHouseSelect.SelectType.DISTINCT, projection, null, List.of(), null,
                    orderBy, false);
        case FILTER:
            return new QueryShape(kind, ClickHouseSelect.SelectType.ALL, projection, predicate, List.of(), null,
                    orderBy, true);
        case AGGREGATE:
            queryAttempt(AdvancedFeature.QUERY_AGGREGATE);
            queryAttempt(AdvancedFeature.QUERY_AGGREGATE_VARIANTS);
            ClickHouseExpression aggregateValue = comparableValueExpression(Randomly.fromList(columns));
            ClickHouseColumnReference aggregateNumeric = aggregateNumericExpression(columns);
            if (aggregateNumeric == null) {
                throw new IgnoreMeException();
            }
            ApproximateKind aggregateApproximation = aggregateApproximation(aggregateNumeric);
            return new QueryShape(kind, ClickHouseSelect.SelectType.ALL,
                    List.of(new ClickHouseAggregate(new ClickHouseText("*"), ClickHouseAggregateFunction.COUNT),
                            new ClickHouseAggregate(aggregateValue, ClickHouseAggregateFunction.MIN),
                            new ClickHouseAggregate(aggregateValue, ClickHouseAggregateFunction.MAX),
                            new ClickHouseAggregate(aggregateNumeric, ClickHouseAggregateFunction.AVG),
                            new ClickHouseAggregate(aggregateNumeric, ClickHouseAggregateFunction.SUM),
                            new ClickHouseText("uniqExact(" + ClickHouseVisitor.asString(aggregateValue) + ")"),
                            new ClickHouseText(
                                    "countIf(isNotNull(" + ClickHouseVisitor.asString(aggregateValue) + "))")),
                    stablePredicate(columns), List.of(), null, List.of(), false, AdvancedFeature.QUERY_AGGREGATE,
                    List.of(ApproximateKind.NONE, ApproximateKind.NONE, ApproximateKind.NONE, aggregateApproximation,
                            aggregateApproximation, ApproximateKind.NONE, ApproximateKind.NONE));
        case GROUPED:
            queryAttempt(AdvancedFeature.QUERY_GROUPED);
            ClickHouseExpression groupKey = groupKeyExpression(columns);
            return new QueryShape(kind, ClickHouseSelect.SelectType.ALL,
                    List.of(groupKey,
                            new ClickHouseAggregate(new ClickHouseText("*"), ClickHouseAggregateFunction.COUNT)),
                    stablePredicate(columns), List.of(groupKey), new ClickHouseText("count() >= 0"), List.of(groupKey),
                    false, AdvancedFeature.QUERY_GROUPED);
        case ROLLUP:
            queryAttempt(AdvancedFeature.QUERY_ROLLUP);
            ClickHouseExpression rollupColumn = groupKeyExpression(columns);
            String rollupKey = ClickHouseVisitor.asString(rollupColumn);
            ClickHouseColumnReference rollupNumeric = aggregateNumericExpression(columns);
            if (rollupNumeric == null) {
                throw new IgnoreMeException();
            }
            ApproximateKind rollupApproximation = aggregateApproximation(rollupNumeric);
            return new QueryShape(kind, ClickHouseSelect.SelectType.ALL,
                    List.of(rollupColumn,
                            new ClickHouseAggregate(new ClickHouseText("*"), ClickHouseAggregateFunction.COUNT),
                            new ClickHouseAggregate(rollupNumeric, ClickHouseAggregateFunction.SUM)),
                    stablePredicate(columns), List.of(new ClickHouseText(rollupKey + " WITH ROLLUP")), null, List.of(),
                    false, AdvancedFeature.QUERY_ROLLUP,
                    List.of(ApproximateKind.NONE, ApproximateKind.NONE, rollupApproximation));
        case GROUP_ARRAY_STRING_CONCAT:
            queryAttempt(AdvancedFeature.QUERY_GROUP_ARRAY_STRING_CONCAT);
            String groupedValue = stableStringValue(Randomly.fromList(columns));
            return new QueryShape(kind, ClickHouseSelect.SelectType.ALL,
                    List.of(new ClickHouseText("arrayStringConcat(arraySort(groupArray(" + groupedValue + ")), ',')"),
                            new ClickHouseAggregate(new ClickHouseText("*"), ClickHouseAggregateFunction.COUNT)),
                    stablePredicate(columns), List.of(), null, List.of(), false,
                    AdvancedFeature.QUERY_GROUP_ARRAY_STRING_CONCAT);
        case NULLABILITY:
            queryAttempt(AdvancedFeature.QUERY_NULLABILITY);
            return new QueryShape(kind, ClickHouseSelect.SelectType.ALL,
                    List.of(selected, new ClickHouseText("isNull(" + ClickHouseVisitor.asString(selected) + ")")), null,
                    List.of(), null, List.of(new ClickHouseText("s." + q(ROW_ID))), true,
                    AdvancedFeature.QUERY_NULLABILITY);
        case LOW_CARDINALITY:
            queryAttempt(AdvancedFeature.QUERY_LOW_CARDINALITY);
            return new QueryShape(kind, ClickHouseSelect.SelectType.ALL,
                    List.of(new ClickHouseText(
                            "toLowCardinality(toString(" + ClickHouseVisitor.asString(selected) + "))")),
                    null, List.of(), null, List.of(new ClickHouseText("s." + q(ROW_ID))), true,
                    AdvancedFeature.QUERY_LOW_CARDINALITY);
        case TYPE_ORDERING:
            queryAttempt(AdvancedFeature.QUERY_TYPE_ORDERING);
            // Keep the type name and value tied to the same source column.
            ClickHouseColumnReference typeOrderingColumn = Randomly.fromList(columns);
            String typeOrderingValue = stableStringValue(typeOrderingColumn);
            return new QueryShape(kind, ClickHouseSelect.SelectType.ALL,
                    List.of(new ClickHouseText("toTypeName(" + columnSql("s", typeOrderingColumn) + ")"),
                            new ClickHouseText(typeOrderingValue)),
                    stablePredicate(columns), List.of(), null,
                    List.of(new ClickHouseText("lowerUTF8(" + typeOrderingValue + ")"),
                            new ClickHouseText("s." + q(ROW_ID))),
                    true, AdvancedFeature.QUERY_TYPE_ORDERING);
        case BOUNDARY_MUTATION:
            queryAttempt(AdvancedFeature.QUERY_BOUNDARY_MUTATION);
            ClickHouseColumnReference boundaryColumn = Randomly.fromList(columns);
            return new QueryShape(kind, ClickHouseSelect.SelectType.ALL, boundaryProjection(boundaryColumn),
                    boundaryPredicate(boundaryColumn), List.of(), null, List.of(new ClickHouseText("s." + q(ROW_ID))),
                    true, AdvancedFeature.QUERY_BOUNDARY_MUTATION);
        case SORTED_TOP_N:
            queryAttempt(AdvancedFeature.QUERY_SORTED_TOP_N);
            ClickHouseExpression sortValue = Randomly.fromList(columns);
            return new QueryShape(kind, ClickHouseSelect.SelectType.ALL, sortedTopNProjection(columns),
                    stablePredicate(columns), List.of(), null,
                    List.of(new ClickHouseText(
                            orderExpression(sortValue) + " NULLS " + Randomly.fromOptions("FIRST", "LAST"))),
                    true, AdvancedFeature.QUERY_SORTED_TOP_N);
        case WINDOW:
            queryAttempt(AdvancedFeature.QUERY_WINDOW);
            return new QueryShape(kind, ClickHouseSelect.SelectType.ALL,
                    List.of(Randomly.fromList(columns), generateWindowExpression(columns)), stablePredicate(columns),
                    List.of(), null, List.of(new ClickHouseText("2")), true, AdvancedFeature.QUERY_WINDOW,
                    List.of(ApproximateKind.NONE, ApproximateKind.FLOAT64));
        case UNION_ALL:
            queryAttempt(AdvancedFeature.QUERY_UNION_ALL);
            int unionColumns = state.getRandomly().getInteger(1, Math.min(4, columns.size() + 1));
            List<ClickHouseExpression> unionProjection = createUnionAllProjection(generator, columns, unionColumns);
            return new QueryShape(kind, ClickHouseSelect.SelectType.ALL, unionProjection,
                    createUnionAllPredicate(columns), List.of(), null, List.of(), false,
                    AdvancedFeature.QUERY_UNION_ALL, unionProjection, createUnionAllPredicate(columns), null, null);
        case CORRELATED:
            queryAttempt(AdvancedFeature.QUERY_CORRELATED_SUBQUERY);
            return new QueryShape(kind, ClickHouseSelect.SelectType.ALL, List.of(selected), stablePredicate(columns),
                    List.of(), null, List.of(new ClickHouseText("s." + q(ROW_ID))), true,
                    AdvancedFeature.QUERY_CORRELATED_SUBQUERY, List.of(), null,
                    Randomly.fromOptions(CorrelatedVariant.values()), correlatedColumn(columns));
        case PROJECTION:
        default:
            return new QueryShape(kind, ClickHouseSelect.SelectType.ALL, projection, null, List.of(), null, orderBy,
                    true);
        }
    }

    private List<ClickHouseColumnReference> expressionColumns(VpSchema vp) {
        List<ClickHouseColumnReference> columns = new ArrayList<>();
        for (VpColumn column : vp.columns) {
            // In the default tier, keep generic expression inputs scalar-like to avoid invalid arithmetic over
            // structures. In deep exploration, add complex/ClickHouse-native values back through the stable expression
            // pool below (raw projection, toString, type name, hash), rather than feeding them to arbitrary operators.
            if (isComplexExpressionType(column.type) && !complexExpressionExploration()) {
                continue;
            }
            if (isComplexExpressionType(column.type)) {
                queryAttempt(AdvancedFeature.QUERY_COMPLEX_TYPE_EXPRESSIONS);
            }
            columns.add(new ClickHouseColumnReference(
                    new ClickHouseColumn(column.name, new ClickHouseLancerDataType(column.type), false, false, null),
                    null, "s"));
        }
        if (columns.isEmpty()) {
            // vp_rowid is an alignment key, not a user-data expression input. Skip pairs
            // whose business columns are unsupported in the selected expression tier.
            throw new IgnoreMeException();
        }
        return columns;
    }

    private boolean isComplexExpressionType(String type) {
        String normalized = type.toUpperCase(Locale.ROOT);
        return normalized.startsWith("ARRAY(") || normalized.startsWith("MAP(") || normalized.startsWith("TUPLE(")
                || normalized.startsWith("NESTED(") || normalized.startsWith("LOWCARDINALITY(ARRAY(")
                || normalized.startsWith("LOWCARDINALITY(MAP(") || normalized.startsWith("LOWCARDINALITY(TUPLE(")
                // The generic generator emits arbitrary string literals. These
                // types need typed literals and otherwise fail before comparison.
                || normalized.contains("DATETIME") || normalized.contains("DATE32") || normalized.equals("DATE")
                || normalized.contains("UUID") || normalized.contains("IPV4") || normalized.contains("IPV6")
                || normalized.startsWith("ENUM8") || normalized.startsWith("ENUM16");
    }

    private boolean complexExpressionExploration() {
        return state.getDbmsSpecificOptions().verticalPartitioningDeepExploration
                && state.getDbmsSpecificOptions().verticalPartitioningComplexTypes;
    }

    private boolean hasProjection(List<String> schemaObjects) {
        return schemaObjects.stream().anyMatch(object -> object.toUpperCase(Locale.ROOT).contains("PROJECTION"));
    }

    /**
     * Selects an input for aggregate shapes whose result does not depend on the physical row order introduced by a VP
     * reconstruction. IEEE-754 addition is non-associative, so Float SUM/AVG must not be compared across a base scan
     * and a join as exact values even when the two relations contain the same multiset.
     */
    private ClickHouseColumnReference aggregateNumericExpression(List<ClickHouseColumnReference> columns) {
        if (state.getDbmsSpecificOptions().verticalPartitioningDeepExploration && Randomly.getBoolean()) {
            ClickHouseColumnReference floating = columns.stream()
                    .filter(column -> !column.getColumn().getName().equals(ROW_ID)).filter(this::isFloatLike)
                    .findFirst().orElse(null);
            if (floating != null) {
                queryAttempt(AdvancedFeature.QUERY_FLOAT_AGGREGATE);
                return floating;
            }
        }
        return columns.stream().filter(this::isOrderStableAggregateInput).findFirst().orElse(null);
    }

    private ApproximateKind aggregateApproximation(ClickHouseColumnReference column) {
        String type = scalarType(column.getColumn().getType().toString());
        if (type.startsWith("FLOAT32")) {
            return ApproximateKind.FLOAT32;
        }
        return ApproximateKind.FLOAT64;
    }

    private boolean isOrderStableAggregateInput(ClickHouseColumnReference column) {
        return isOrderStableAggregateType(column.getColumn().getType().toString());
    }

    private ClickHouseExpression groupKeyExpression(List<ClickHouseColumnReference> columns) {
        ClickHouseColumnReference column = Randomly.fromList(columns);
        if (isRawComparableExpression(column)) {
            return column;
        }
        queryAttempt(AdvancedFeature.QUERY_COMPLEX_TYPE_EXPRESSIONS);
        return new ClickHouseText(stableStringValue(column));
    }

    private ClickHouseExpression comparableValueExpression(ClickHouseColumnReference column) {
        if (isRawComparableExpression(column)) {
            return column;
        }
        queryAttempt(AdvancedFeature.QUERY_COMPLEX_TYPE_EXPRESSIONS);
        return new ClickHouseText(stableStringValue(column));
    }

    private String orderExpression(ClickHouseExpression expression) {
        if (expression instanceof ClickHouseColumnReference
                && !isRawComparableExpression((ClickHouseColumnReference) expression)) {
            queryAttempt(AdvancedFeature.QUERY_COMPLEX_TYPE_EXPRESSIONS);
            return stableStringValue((ClickHouseColumnReference) expression);
        }
        return ClickHouseVisitor.asString(expression);
    }

    private boolean isRawComparableExpression(ClickHouseColumnReference column) {
        String type = scalarType(column.getColumn().getType().toString());
        return !(type.startsWith("MAP(") || type.startsWith("NESTED("));
    }

    private String stableStringValue(ClickHouseColumnReference column) {
        return "ifNull(toString(" + columnSql("s", column) + "), '<null>')";
    }

    private ClickHouseExpression stablePredicate(List<ClickHouseColumnReference> columns) {
        ClickHouseColumnReference column = Randomly.fromList(columns);
        String value = stableStringValue(column);
        return new ClickHouseText(Randomly.fromOptions("isNotNull(" + columnSql("s", column) + ")",
                "lengthUTF8(" + value + ") >= 0", "cityHash64(" + value + ") % 5 != 1"));
    }

    private List<ClickHouseExpression> boundaryProjection(ClickHouseColumnReference column) {
        String value = stableStringValue(column);
        return List.of(new ClickHouseText("s." + q(ROW_ID)), new ClickHouseText(value),
                new ClickHouseText("lengthUTF8(" + value + ")"));
    }

    private ClickHouseExpression boundaryPredicate(ClickHouseColumnReference column) {
        String value = columnSql("s", column);
        String type = scalarType(column.getColumn().getType().toString());
        if (type.startsWith("FLOAT")) {
            return new ClickHouseText(value + " IN (nan, inf, -inf)");
        }
        if (type.startsWith("DECIMAL")) {
            return new ClickHouseText(
                    value + " >= toDecimal64('999999999999', 2) OR " + value + " <= toDecimal64('-999999999999', 2)");
        }
        if (type.startsWith("UINT")) {
            return new ClickHouseText(value + " >= 255 OR " + value + " = 0");
        }
        if (type.startsWith("INT")) {
            return new ClickHouseText(value + " <= -128 OR " + value + " >= 127");
        }
        return new ClickHouseText("isNull(" + value + ") OR lengthUTF8(ifNull(toString(" + value + "), '')) >= 0");
    }

    private List<ClickHouseExpression> randomAstProjection(ClickHouseExpressionGenerator generator,
            List<ClickHouseColumnReference> columns) {
        int count = state.getRandomly().getInteger(1, Math.min(4, columns.size() + 1));
        List<ClickHouseExpression> projection = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (int attempts = 0; projection.size() < count && attempts < count * 16; attempts++) {
            addUniqueProjection(projection, seen, stableProjectionExpression(Randomly.fromList(columns)));
        }
        for (ClickHouseColumnReference column : columns) {
            if (projection.size() >= count) {
                break;
            }
            addUniqueProjection(projection, seen, column);
        }
        return projection;
    }

    private List<ClickHouseExpression> scalarSafeProjection(List<ClickHouseColumnReference> columns) {
        int count = state.getRandomly().getInteger(1, Math.min(3, columns.size()) + 1);
        List<ClickHouseExpression> projection = new ArrayList<>();
        List<ClickHouseColumnReference> shuffled = new ArrayList<>(columns);
        Collections.shuffle(shuffled);
        for (ClickHouseColumnReference column : shuffled) {
            projection.add(column);
            if (projection.size() == count) {
                break;
            }
        }
        return projection;
    }

    private ClickHouseExpression stableProjectionExpression(ClickHouseColumnReference column) {
        String value = columnSql("s", column);
        String stringValue = "ifNull(toString(" + value + "), '<null>')";
        List<String> expressions = new ArrayList<>(
                List.of(value, stringValue, "toString(lengthUTF8(" + stringValue + "))",
                        "toString(cityHash64(" + stringValue + "))", "toTypeName(" + value + ")"));
        if (isNumeric(column)) {
            String numeric = "toFloat64(" + value + ")";
            expressions.add(numeric);
            expressions.add("(" + numeric + " + 1.)");
            expressions.add("(-(" + numeric + "))");
            expressions.add("pow(abs(" + numeric + "), 2)");
        }
        return new ClickHouseText(Randomly.fromList(expressions));
    }

    private ClickHouseExpression scalarSafePredicate(List<ClickHouseColumnReference> columns) {
        ClickHouseColumnReference column = Randomly.fromList(columns);
        String value = stableStringValue(column);
        return new ClickHouseText(Randomly.fromOptions("isNotNull(" + columnSql("s", column) + ")",
                "lengthUTF8(" + value + ") >= 0", "cityHash64(" + value + ") % 5 != 1"));
    }

    private List<ClickHouseExpression> sortedTopNProjection(List<ClickHouseColumnReference> columns) {
        int count = state.getRandomly().getInteger(1, Math.min(3, columns.size()) + 1);
        List<ClickHouseExpression> projection = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (int attempts = 0; projection.size() < count && attempts < count * 8; attempts++) {
            addUniqueProjection(projection, seen, sortedTopNStringExpression(Randomly.fromList(columns)));
        }
        return projection;
    }

    private ClickHouseExpression sortedTopNStringExpression(ClickHouseColumnReference column) {
        String value = "ifNull(toString(" + columnSql("s", column) + "), '<null>')";
        return new ClickHouseText(Randomly.fromOptions(value, "lowerUTF8(" + value + ")",
                "toString(lengthUTF8(" + value + "))", "concat('h:', toString(cityHash64(" + value + ")))"));
    }

    private List<ClickHouseExpression> createUnionAllProjection(ClickHouseExpressionGenerator generator,
            List<ClickHouseColumnReference> columns, int fetchColumns) {
        List<ClickHouseExpression> selected = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (int attempts = 0; selected.size() < fetchColumns && attempts < fetchColumns * 16; attempts++) {
            addUniqueProjection(selected, seen, stableProjectionExpression(Randomly.fromList(columns)));
        }
        for (ClickHouseColumnReference column : columns) {
            if (selected.size() >= fetchColumns) {
                break;
            }
            addUniqueProjection(selected, seen, column);
        }
        return selected;
    }

    private void addUniqueProjection(List<ClickHouseExpression> projection, Set<String> seen,
            ClickHouseExpression expression) {
        if (seen.add(ClickHouseVisitor.asString(expression))) {
            projection.add(expression);
        }
    }

    private ClickHouseExpression createUnionAllPredicate(List<ClickHouseColumnReference> columns) {
        if (Randomly.getBoolean()) {
            return null;
        }
        ClickHouseColumnReference column = Randomly.fromList(columns);
        String value = unionAllStringValue(column);
        return new ClickHouseText(Randomly.fromOptions("isNotNull(" + columnSql("s", column) + ")",
                "lengthUTF8(" + value + ") >= 0", "cityHash64(" + value + ") % 5 != 1"));
    }

    private String unionAllStringValue(ClickHouseColumnReference column) {
        return "ifNull(toString(" + columnSql("s", column) + "), '<null>')";
    }

    private ClickHouseExpression generateWindowExpression(List<ClickHouseColumnReference> columns) {
        String rowId = "s." + q(ROW_ID);
        String order = orderExpression(Randomly.fromList(columns)) + ", " + rowId;
        String partition = Randomly.getBoolean() ? "PARTITION BY cityHash64(" + rowId + ") % 4 " : "";
        String frame = Randomly.fromOptions("", " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                " ROWS BETWEEN 1 PRECEDING AND CURRENT ROW");
        String value = orderExpression(Randomly.fromList(columns));
        ClickHouseColumnReference numeric = columns.stream().filter(this::isNumeric).findFirst().orElse(null);
        List<String> windows = new ArrayList<>(List.of("row_number() OVER (" + partition + "ORDER BY " + order + ")",
                "rank() OVER (" + partition + "ORDER BY " + order + ")",
                "dense_rank() OVER (" + partition + "ORDER BY " + order + ")",
                "count() OVER (" + partition + "ORDER BY " + rowId + frame + ")",
                "min(" + value + ") OVER (" + partition + "ORDER BY " + rowId + frame + ")"));
        if (numeric != null) {
            windows.add("sum(" + ClickHouseVisitor.asString(numeric) + ") OVER (" + partition + "ORDER BY " + rowId
                    + frame + ")");
        }
        return new ClickHouseText(Randomly.fromList(windows));
    }

    private ClickHouseColumnReference correlatedColumn(List<ClickHouseColumnReference> columns) {
        List<ClickHouseColumnReference> dataColumns = columns.stream()
                .filter(column -> !column.getColumn().getName().equals(ROW_ID)).collect(Collectors.toList());
        return dataColumns.isEmpty() ? columns.get(0) : Randomly.fromList(dataColumns);
    }

    private String renderOuterQuery(String relation, QueryShape shape) {
        String query = renderAstSelect(relation, shape);
        if (shape.kind == QueryKind.UNION_ALL) {
            QueryShape branch = new QueryShape(shape.kind, shape.selectType, shape.unionAllFetch, shape.unionAllWhere,
                    List.of(), null, List.of(), false);
            query = query + " UNION ALL " + renderAstSelect(relation, branch);
        }
        if (shape.kind == QueryKind.CORRELATED) {
            String projection = ClickHouseVisitor.asString(shape.fetch.get(0));
            String predicate = ClickHouseVisitor.asString(shape.where);
            String subqueryRelation = relation.replaceFirst("(?i)\\s+AS\\s+s$", " AS s2");
            String rowId = "s." + q(ROW_ID);
            String subRowId = "s2." + q(ROW_ID);
            String outerValue = columnSql("s", shape.correlatedColumn);
            String subValue = columnSql("s2", shape.correlatedColumn);
            String outerPredicate = predicate == null ? "1" : "(" + predicate + ")";
            switch (shape.correlatedVariant) {
            case EXISTS_NOT_NULL:
                return "SELECT " + projection + " FROM " + relation + " WHERE EXISTS (SELECT 1 FROM " + subqueryRelation
                        + " WHERE " + subRowId + " = " + rowId + " AND isNotNull(" + subValue + ")) AND "
                        + outerPredicate;
            case EXISTS_NULL:
                return "SELECT " + projection + " FROM " + relation + " WHERE EXISTS (SELECT 1 FROM " + subqueryRelation
                        + " WHERE " + subRowId + " = " + rowId + " AND isNull(" + subValue + ")) AND " + outerPredicate;
            case EXISTS_VALUE_MATCH:
            default:
                return "SELECT " + projection + " FROM " + relation + " WHERE EXISTS (SELECT 1 FROM " + subqueryRelation
                        + " WHERE " + subRowId + " = " + rowId + " AND isNotNull(" + subValue + ") AND " + subValue
                        + " = " + outerValue + ") AND " + outerPredicate;
            }
        }
        return query;
    }

    private String columnSql(String alias, ClickHouseColumnReference column) {
        return alias + "." + q(column.getColumn().getName());
    }

    private String renderAstSelect(String relation, QueryShape shape) {
        ClickHouseSelect select = new ClickHouseSelect();
        select.setSelectType(shape.selectType);
        select.setFetchColumns(resultProjection(shape.fetch));
        select.setFromList(List.of(new ClickHouseText(relation)));
        select.setWhereClause(shape.where);
        select.setGroupByClause(shape.groupBy);
        select.setHavingClause(shape.having);
        List<ClickHouseExpression> orderBy = new ArrayList<>(shape.orderBy);
        if (orderedComparisonEnabled(shape)) {
            orderBy.add(new ClickHouseText("s." + q(ROW_ID)));
        }
        select.setOrderByClauses(orderBy);
        if (shape.limit) {
            select.setLimitClause(new ClickHouseText("32"));
        }
        return ClickHouseVisitor.asString(select);
    }

    private List<ClickHouseExpression> resultProjection(List<ClickHouseExpression> expressions) {
        List<ClickHouseExpression> aliased = new ArrayList<>();
        for (int i = 0; i < expressions.size(); i++) {
            aliased.add(new ClickHouseText(
                    "(" + ClickHouseVisitor.asString(expressions.get(i)) + ") AS `vp_result_" + i + "`"));
        }
        return aliased;
    }

    private String reconstructionRelation(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        String select = reconstructSelect(vp);
        switch (strategy) {
        case LEFT_RIGHT_JOIN:
            return "(" + reconstructSelect(vp, "LEFT JOIN") + ") AS s";
        case FULL_OUTER_SIMULATION:
            return "(" + reconstructSelect(vp, "FULL OUTER JOIN") + ") AS s";
        case SCALAR_SUBQUERY:
            return "(" + scalarSubqueryRelation(vp) + ") AS s";
        case EXISTS_FILTER:
            return "(" + existsRelation(vp) + ") AS s";
        case IN_FILTER:
            return "(" + inRelation(vp) + ") AS s";
        case DERIVED_TABLE:
            return "(" + derivedTableRelation(vp) + ") AS s";
        case VIEW:
            if (!vp.viewMaterialized) {
                dropReconstructionObject("DROP VIEW IF EXISTS " + q(vp.view));
                executeSetup(vp, "CREATE VIEW " + q(vp.view) + " AS " + select);
                vp.viewMaterialized = true;
            }
            return q(vp.view) + " AS s";
        case MATERIALIZED_MEMORY_TABLE:
            if (!vp.tempMaterialized) {
                dropReconstructionObject("DROP TABLE IF EXISTS " + q(vp.temp));
                executeSetup(vp, "CREATE TABLE " + q(vp.temp) + " ENGINE = Memory AS " + select);
                vp.tempMaterialized = true;
            }
            return q(vp.temp) + " AS s";
        case AGGREGATE_THEN_MERGE:
            return "(" + aggregateThenMergeRelation(vp) + ") AS s";
        case ROW_NUMBER_ALIGNMENT:
            return "(" + rowNumberAlignmentRelation(vp) + ") AS s";
        case NATURAL_OR_USING:
            return "(" + reconstructSelectUsing(vp) + ") AS s";
        case ANTI_JOIN_FILTER:
            return "(" + antiJoinFilterRelation(vp) + ") AS s";
        case JSON_PACK:
            return "(" + jsonPackRelation(vp) + ") AS s";
        case STRING_PACK:
            return "(" + stringPackRelation(vp) + ") AS s";
        case CTE:
        case JOIN:
        default:
            return "(" + select + ") AS s";
        }
    }

    private void dropReconstructionObject(String sql) throws Exception {
        try {
            executeDirect(sql);
        } catch (AssertionError ignored) {
            // The final split cleanup will make another best-effort attempt.
        }
    }

    private String reconstructionSemanticTag(ReconstructionStrategy strategy) {
        switch (strategy) {
        case JSON_PACK:
            return "json-pack-business-columns";
        case STRING_PACK:
            return "string-pack-business-columns";
        case ANTI_JOIN_FILTER:
            return "left-anti-join";
        case MATERIALIZED_MEMORY_TABLE:
            return "memory-table-materialized";
        case VIEW:
            return "persistent-view-over-join";
        case DERIVED_TABLE:
            return "derived-table-over-join";
        case CTE:
            return "cte-over-join";
        case NATURAL_OR_USING:
            return "using-or-natural-join";
        case JOIN:
            return "inner-join-rowid";
        case LEFT_RIGHT_JOIN:
            return "left-join-rowid";
        case FULL_OUTER_SIMULATION:
            return "full-outer-join-rowid";
        default:
            return strategy.name().toLowerCase(Locale.ROOT);
        }
    }

    private String derivedTableRelation(VpSchema vp) {
        String left = "(SELECT * FROM " + q(vp.left) + ") AS l";
        String right = "(SELECT * FROM " + q(vp.right) + ") AS r";
        return reconstructSelectFrom(vp, left + " INNER JOIN " + right + " ON l." + q(ROW_ID) + " = r." + q(ROW_ID));
    }

    private String cteQuery(VpSchema vp, QueryShape shape) {
        String relation = "WITH left_part AS (SELECT * FROM " + q(vp.left) + "), right_part AS (SELECT * FROM "
                + q(vp.right) + "), reconstructed AS (" + reconstructSelectFrom(vp,
                        "left_part AS l INNER JOIN right_part AS r ON l." + q(ROW_ID) + " = r." + q(ROW_ID))
                + ") ";
        return relation + renderOuterQuery("reconstructed AS s", shape);
    }

    private String reconstructSelect(VpSchema vp) {
        return reconstructSelect(vp, "INNER JOIN");
    }

    private String reconstructSelect(VpSchema vp, String join) {
        List<String> projected = new ArrayList<>();
        projected.add("l." + q(ROW_ID) + " AS " + q(ROW_ID));
        for (VpColumn column : vp.columns) {
            boolean onLeft = contains(vp.leftColumns, column.name);
            if (!onLeft && !contains(vp.rightColumns, column.name)) {
                throw new AssertionError("VP split lost column " + column.name);
            }
            String owner = onLeft ? "l" : "r";
            projected.add(owner + "." + q(column.name) + " AS " + q(column.name));
        }
        return "SELECT " + String.join(", ", projected) + " FROM " + q(vp.left) + " AS l " + join + " " + q(vp.right)
                + " AS r ON l." + q(ROW_ID) + " = r." + q(ROW_ID);
    }

    private String scalarSubqueryRelation(VpSchema vp) {
        List<String> projected = new ArrayList<>();
        projected.add("l." + q(ROW_ID) + " AS " + q(ROW_ID));
        for (VpColumn column : vp.columns) {
            if (contains(vp.leftColumns, column.name)) {
                projected.add("l." + q(column.name) + " AS " + q(column.name));
            } else {
                // vp_rowid is unique, but ClickHouse cannot decorrelate LIMIT in a scalar subquery. any() retains
                // the one-row value while producing a scalar relation the analyzer can decorrelate. ClickHouse
                // infers the scalar subquery itself as Nullable(T), even when r.column is T. Cast it back to the
                // source type so type-observing projections and ROLLUP default values retain VP equivalence.
                String scalar = "(SELECT any(r." + q(column.name) + ") FROM " + q(vp.right) + " AS r WHERE r."
                        + q(ROW_ID) + " = l." + q(ROW_ID) + ")";
                projected.add(castToColumnType(scalar, column) + " AS " + q(column.name));
            }
        }
        return "SELECT " + String.join(", ", projected) + " FROM " + q(vp.left) + " AS l";
    }

    private String castToColumnType(String expression, VpColumn column) {
        return "CAST(" + expression + ", '" + column.type.replace("'", "''") + "')";
    }

    private String existsRelation(VpSchema vp) {
        String predicate = "EXISTS (SELECT 1 FROM " + q(vp.right) + " AS er WHERE er." + q(ROW_ID) + " = l." + q(ROW_ID)
                + ")";
        return reconstructSelectFrom(vp, q(vp.left) + " AS l INNER JOIN " + q(vp.right) + " AS r ON l." + q(ROW_ID)
                + " = r." + q(ROW_ID) + " WHERE " + predicate);
    }

    private String inRelation(VpSchema vp) {
        return reconstructSelectFrom(vp,
                q(vp.left) + " AS l INNER JOIN " + q(vp.right) + " AS r ON l." + q(ROW_ID) + " = r." + q(ROW_ID)
                        + " WHERE l." + q(ROW_ID) + " IN (SELECT " + q(ROW_ID) + " FROM " + q(vp.right) + ")");
    }

    private String antiJoinFilterRelation(VpSchema vp) {
        String presentRows = "(SELECT " + q(ROW_ID) + ", CAST(1, 'Nullable(UInt8)') AS vp_present_marker FROM "
                + q(vp.left) + ") AS present";
        String missingRows = "(SELECT anti." + q(ROW_ID) + " AS " + q(ROW_ID)
                + ", CAST(1, 'Nullable(UInt8)') AS vp_missing_marker FROM " + q(vp.right) + " AS anti LEFT JOIN "
                + presentRows + " ON anti." + q(ROW_ID) + " = present." + q(ROW_ID)
                + " WHERE present.vp_present_marker IS NULL) AS missing";
        return reconstructSelectFrom(vp,
                q(vp.left) + " AS l INNER JOIN " + q(vp.right) + " AS r ON l." + q(ROW_ID) + " = r." + q(ROW_ID)
                        + " LEFT JOIN " + missingRows + " ON missing." + q(ROW_ID) + " = l." + q(ROW_ID)
                        + " WHERE missing.vp_missing_marker IS NULL");
    }

    private String jsonPackRelation(VpSchema vp) {
        List<VpColumn> packedColumns = rightOnlyColumns(vp);
        if (packedColumns.isEmpty() || packedColumns.stream().anyMatch(c -> !isJsonPackSupported(c))) {
            throw new IgnoreMeException();
        }
        String tupleType = "Tuple(" + packedColumns.stream().map(c -> c.type).collect(Collectors.joining(", ")) + ")";
        String packedRight = "(SELECT " + q(ROW_ID) + ", toJSONString(tuple("
                + packedColumns.stream().map(c -> q(c.name)).collect(Collectors.joining(", "))
                + ")) AS vp_json_pack FROM " + q(vp.right) + ") AS r";
        List<String> rightExpressions = new ArrayList<>();
        for (int i = 0; i < packedColumns.size(); i++) {
            VpColumn column = packedColumns.get(i);
            String element = "tupleElement(JSONExtract(r.vp_json_pack, '" + tupleType.replace("'", "''") + "'), "
                    + (i + 1) + ")";
            rightExpressions.add(castToColumnType(element, column) + " AS " + q(column.name));
        }
        return reconstructSelectFrom(vp,
                q(vp.left) + " AS l INNER JOIN " + packedRight + " ON l." + q(ROW_ID) + " = r." + q(ROW_ID),
                rightExpressions);
    }

    private String stringPackRelation(VpSchema vp) {
        List<VpColumn> packedColumns = rightOnlyColumns(vp);
        if (packedColumns.size() != 1 || !isStringPackSupported(packedColumns.get(0))) {
            throw new IgnoreMeException();
        }
        VpColumn column = packedColumns.get(0);
        String packedRight = "(SELECT " + q(ROW_ID) + ", toString(" + q(column.name) + ") AS vp_string_pack FROM "
                + q(vp.right) + ") AS r";
        return reconstructSelectFrom(vp,
                q(vp.left) + " AS l INNER JOIN " + packedRight + " ON l." + q(ROW_ID) + " = r." + q(ROW_ID),
                List.of(castToColumnType("r.vp_string_pack", column) + " AS " + q(column.name)));
    }

    private String aggregateThenMergeRelation(VpSchema vp) {
        List<String> right = new ArrayList<>();
        right.add(q(ROW_ID));
        for (VpColumn column : vp.rightColumns) {
            // Aggregate functions unwrap LowCardinality(T) (and can otherwise change the inferred result type).
            // The reconstruction is row-preserving, so restore the exact source type before the outer query can
            // observe it through toTypeName(), ROLLUP, or nullable-aware expressions.
            right.add(castToColumnType("any(" + q(column.name) + ")", column) + " AS " + q(column.name));
        }
        String aggregatedRight = "(SELECT " + String.join(", ", right) + " FROM " + q(vp.right) + " GROUP BY "
                + q(ROW_ID) + ") AS r";
        return reconstructSelectFrom(vp,
                q(vp.left) + " AS l INNER JOIN " + aggregatedRight + " ON l." + q(ROW_ID) + " = r." + q(ROW_ID));
    }

    private String rowNumberAlignmentRelation(VpSchema vp) {
        String left = "(SELECT *, row_number() OVER (ORDER BY " + q(ROW_ID) + ") AS vp_rn FROM " + q(vp.left)
                + ") AS l";
        String right = "(SELECT *, row_number() OVER (ORDER BY " + q(ROW_ID) + ") AS vp_rn FROM " + q(vp.right)
                + ") AS r";
        return reconstructSelectFrom(vp, left + " INNER JOIN " + right + " ON l.vp_rn = r.vp_rn");
    }

    private String reconstructSelectUsing(VpSchema vp) {
        return reconstructSelectFrom(vp,
                q(vp.left) + " AS l INNER JOIN " + q(vp.right) + " AS r USING (" + q(ROW_ID) + ")");
    }

    private String reconstructSelectFrom(VpSchema vp, String from) {
        return reconstructSelectFrom(vp, from, List.of());
    }

    private String reconstructSelectFrom(VpSchema vp, String from, List<String> rightColumnExpressions) {
        List<String> projected = new ArrayList<>();
        projected.add("l." + q(ROW_ID) + " AS " + q(ROW_ID));
        int rightExpressionIndex = 0;
        for (VpColumn column : vp.columns) {
            if (contains(vp.leftColumns, column.name)) {
                projected.add("l." + q(column.name) + " AS " + q(column.name));
            } else if (!rightColumnExpressions.isEmpty()) {
                projected.add(rightColumnExpressions.get(rightExpressionIndex++));
            } else {
                projected.add("r." + q(column.name) + " AS " + q(column.name));
            }
        }
        return "SELECT " + String.join(", ", projected) + " FROM " + from;
    }

    private List<VpColumn> rightOnlyColumns(VpSchema vp) {
        return vp.columns.stream().filter(c -> !contains(vp.leftColumns, c.name) && contains(vp.rightColumns, c.name))
                .collect(Collectors.toList());
    }

    private boolean isJsonPackSupported(VpColumn column) {
        String type = scalarType(column);
        return type.startsWith("INT") || type.startsWith("UINT");
    }

    private boolean isStringPackSupported(VpColumn column) {
        return isJsonPackSupported(column);
    }

    private void compare(QueryPair pair) throws Exception {
        QueryResultMultiset original = resultMultiset(pair.single, pair.approximateColumns);
        QueryResultMultiset reconstructed = resultMultiset(pair.split, pair.approximateColumns);
        TYPED_COMPARISONS.incrementAndGet();
        if (!multisetsEqual(original, reconstructed)) {
            String message = "ClickHouse vertical partitioning mismatch (" + pair.description + ")\noriginal: "
                    + pair.single + "\nreconstructed: " + pair.split + "\noriginal rows=" + original.rows
                    + "\nreconstructed rows=" + reconstructed.rows + "\noriginal approximate-columns="
                    + original.approximateColumns + "\nreconstructed approximate-columns="
                    + reconstructed.approximateColumns + "\noriginal column-types=" + original.columnTypes
                    + "\nreconstructed column-types=" + reconstructed.columnTypes;
            KnownPattern known = knownPattern(message);
            if (state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns && known != null) {
                KNOWN_PATTERN_COUNTS.get(known).incrementAndGet();
                return;
            }
            throw new AssertionError(message);
        }
        if (pair.orderedComparisonEnabled) {
            ORDERED_COMPARISONS.incrementAndGet();
            if (!orderedRowsEqual(original, reconstructed)) {
                throw new AssertionError("ClickHouse vertical partitioning ordered mismatch (" + pair.description
                        + ")\noriginal: " + pair.single + "\nreconstructed: " + pair.split + "\noriginal ordered rows="
                        + original.orderedRows + "\nreconstructed ordered rows=" + reconstructed.orderedRows
                        + "\noriginal approximate-columns=" + original.approximateColumns
                        + "\nreconstructed approximate-columns=" + reconstructed.approximateColumns);
            }
        }
    }

    private QueryResultMultiset resultMultiset(String sql, List<ApproximateKind> expectedApproximateColumns)
            throws Exception {
        return resultMultiset(((SQLConnection) state.getConnection()).getJdbcConnection(), sql,
                expectedApproximateColumns);
    }

    private QueryResultMultiset resultMultiset(Connection connection, String sql,
            List<ApproximateKind> expectedApproximateColumns) throws Exception {
        Map<String, Integer> rows = new LinkedHashMap<>();
        List<ApproximateKind> approximateColumns = new ArrayList<>();
        List<String> orderedRows = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        // Keep VP progress reporting consistent with MySQL, TiDB, and OceanBase: each source or reconstructed
        // result query contributes to Main.nrQueries in addition to the completed oracle check.
        Main.nrQueries.addAndGet(1);
        String readSql = resultReadQuery(sql);
        // VP result SELECTs are intentionally omitted from cur.log by default because a deep run can generate
        // millions of rows of logging. When SQLancer's explicit --log-each-select switch is enabled, include the
        // initiating connection's comparison query so the log accurately reflects executed VP reads.
        if (state.getOptions().logEachSelect()
                && connection == ((SQLConnection) state.getConnection()).getJdbcConnection()) {
            state.getState().logStatement(readSql);
        }
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery(readSql)) {
            Main.nrSuccessfulActions.addAndGet(1);
            ResultSetMetaData meta = rs.getMetaData();
            int count = meta.getColumnCount();
            if (!expectedApproximateColumns.isEmpty()) {
                approximateColumns.addAll(expectedApproximateColumns);
            } else {
                for (int i = 1; i <= count; i++) {
                    approximateColumns.add(approximateKind(meta.getColumnTypeName(i)));
                }
            }
            for (int i = 1; i <= count; i++) {
                columnTypes.add(canonicalColumnType(meta, i));
            }
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= count; i++) {
                    String value = canonicalColumnValue(rs, meta, i);
                    if (value == null) {
                        row.append("N;");
                    } else {
                        row.append("S:").append(value.length()).append(':').append(value).append(';');
                    }
                }
                String encoded = row.toString();
                rows.merge(encoded, 1, Integer::sum);
                orderedRows.add(encoded);
            }
        } catch (SQLException | RuntimeException e) {
            KnownPattern known = knownPattern(e);
            if (state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns && known != null) {
                KNOWN_PATTERN_COUNTS.get(known).incrementAndGet();
                throw ignore(e);
            }
            if (expectedResultError(e)) {
                throw ignore(e);
            }
            throw new AssertionError(
                    "ClickHouse VP result query failed:\ninput query:\n" + sql + "\nexecuted query:\n" + readSql, e);
        }
        return new QueryResultMultiset(rows, approximateColumns, orderedRows, columnTypes);
    }

    private String resultReadQuery(String sql) {
        // Dedicated probes carry their own SETTINGS clause. Do not append a second
        // clause when the normal randomized settings profile is also enabled.
        return sql.toUpperCase(Locale.ROOT).contains(" SETTINGS ") ? sql
                : settingsProfile == null ? sql : sql + settingsProfile.clause;
    }

    private void runOptimizerBugProbe(BaseTableInfo base, VpSchema vp) throws Exception {
        if (!state.getDbmsSpecificOptions().verticalPartitioningOptimizerBugProbes
                || base.shape != BaseShape.MULTI_COLUMN) {
            return;
        }

        // The known ClickHouse failure needs two columns on the left: one is
        // projected and the other is only referenced by the outer filter. At
        // least one business column must remain on the right so the LEFT JOIN
        // is a real vertical reconstruction rather than a redundant copy.
        List<VpColumn> leftData = vp.leftColumns.stream().filter(column -> !column.name.equals(ROW_ID))
                .filter(this::isIntegralNumeric).collect(Collectors.toList());
        List<VpColumn> rightOnly = rightOnlyColumns(vp);
        if (leftData.size() < 2 || rightOnly.isEmpty()) {
            return;
        }

        VpColumn filterColumn = leftData.get(0);
        VpColumn projectionColumn = leftData.get(1);
        String sourceRelation = q(vp.source) + " AS s";
        String reconstructed = "(" + reconstructSelect(vp, "LEFT JOIN") + ") AS s";
        String sourceQuery = "SELECT s." + q(projectionColumn.name) + " AS `vp_probe_result` FROM " + sourceRelation
                + " WHERE s." + q(filterColumn.name) + " != 255" + " ORDER BY s." + q(ROW_ID)
                + " LIMIT 1 SETTINGS optimize_move_to_prewhere = 0";
        String reconstructedQuery = "SELECT s." + q(projectionColumn.name) + " AS `vp_probe_result` FROM "
                + reconstructed + " WHERE s." + q(filterColumn.name) + " != 255" + " ORDER BY s." + q(ROW_ID)
                + " LIMIT 1 SETTINGS optimize_move_to_prewhere = 0";

        queryAttempt(AdvancedFeature.QUERY_OPTIMIZER_BUG_PROBE);
        QueryResultMultiset original = resultMultiset(sourceQuery, List.of());
        QueryResultMultiset rebuilt = resultMultiset(reconstructedQuery, List.of());
        if (!multisetsEqual(original, rebuilt)) {
            throw new AssertionError("ClickHouse vertical partitioning optimizer probe mismatch"
                    + " (LEFT JOIN, filtered-column-not-projected, ORDER BY/LIMIT, " + "optimize_move_to_prewhere=0)"
                    + "\noriginal: " + sourceQuery + "\nreconstructed: " + reconstructedQuery + "\noriginal rows="
                    + original.rows + "\nreconstructed rows=" + rebuilt.rows);
        }
        querySuccess(AdvancedFeature.QUERY_OPTIMIZER_BUG_PROBE);
    }

    private String canonicalColumnType(ResultSetMetaData meta, int column) throws SQLException {
        String type = meta.getColumnTypeName(column);
        String className;
        try {
            className = meta.getColumnClassName(column);
        } catch (SQLException | RuntimeException e) {
            // ClickHouse JDBC 0.8.x can NPE while mapping complex ClickHouse types
            // such as Map/Nested to Java classes. The SQL type name is the stable
            // signal the oracle needs for typed comparisons.
            className = "";
        }
        return normalizeTypeSignature(type) + "|" + (className == null ? "" : className);
    }

    private String normalizeTypeSignature(String type) {
        if (type == null) {
            return "";
        }
        return type.replaceAll("\\s+", "").toUpperCase(Locale.ROOT);
    }

    private String canonicalColumnValue(ResultSet rs, ResultSetMetaData meta, int column) throws SQLException {
        Object objectValue = null;
        SQLException objectFailure = null;
        try {
            objectValue = rs.getObject(column);
            if (objectValue == null && rs.wasNull()) {
                return null;
            }
        } catch (SQLException e) {
            objectFailure = e;
        }
        if (objectFailure == null && objectValue != null) {
            return canonicalObjectValue(objectValue);
        }
        String stringValue = rs.getString(column);
        if (stringValue == null && rs.wasNull()) {
            return null;
        }
        String type = normalizeTypeSignature(meta.getColumnTypeName(column));
        if (type.startsWith("FIXEDSTRING") || type.equals("STRING")) {
            try {
                byte[] bytes = rs.getBytes(column);
                if (bytes != null) {
                    return "bytes:" + Base64.getEncoder().encodeToString(bytes);
                }
            } catch (SQLException ignored) {
                // Some ClickHouse JDBC value classes expose textual values only. Fall through to the exact string
                // representation, which is still length-prefixed at the row level.
            }
        }
        if (objectFailure != null && stringValue == null) {
            throw objectFailure;
        }
        return stringValue;
    }

    private String canonicalObjectValue(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return "bytes:" + Base64.getEncoder().encodeToString((byte[]) value);
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        }
        if (value instanceof Float) {
            return canonicalDouble(((Float) value).doubleValue());
        }
        if (value instanceof Double) {
            return canonicalDouble((Double) value);
        }
        if (value instanceof java.sql.Array) {
            return canonicalObjectValue(((java.sql.Array) value).getArray());
        }
        if (value instanceof Map<?, ?>) {
            Map<?, ?> map = (Map<?, ?>) value;
            List<String> entries = new ArrayList<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                entries.add(encodeCanonicalPart(canonicalObjectValue(entry.getKey())) + "=>"
                        + encodeCanonicalPart(canonicalObjectValue(entry.getValue())));
            }
            Collections.sort(entries);
            return "map[" + String.join(",", entries) + "]";
        }
        if (value instanceof Iterable<?>) {
            Iterable<?> iterable = (Iterable<?>) value;
            List<String> values = new ArrayList<>();
            for (Object element : iterable) {
                values.add(encodeCanonicalPart(canonicalObjectValue(element)));
            }
            return "array[" + String.join(",", values) + "]";
        }
        Class<?> clazz = value.getClass();
        if (clazz.isArray()) {
            List<String> values = new ArrayList<>();
            int length = java.lang.reflect.Array.getLength(value);
            for (int i = 0; i < length; i++) {
                values.add(encodeCanonicalPart(canonicalObjectValue(java.lang.reflect.Array.get(value, i))));
            }
            return "array[" + String.join(",", values) + "]";
        }
        return value.toString();
    }

    private String canonicalDouble(double value) {
        if (Double.isNaN(value)) {
            return "NaN";
        }
        if (value == Double.POSITIVE_INFINITY) {
            return "Infinity";
        }
        if (value == Double.NEGATIVE_INFINITY) {
            return "-Infinity";
        }
        if (value == 0d) {
            return "0.0";
        }
        return Double.toString(value);
    }

    private String encodeCanonicalPart(String value) {
        return value == null ? "N" : "S:" + value.length() + ":" + value;
    }

    private QuerySettingsProfile chooseSettingsProfile() {
        if (!state.getDbmsSpecificOptions().verticalPartitioningQuerySettingsProfiles) {
            return null;
        }
        int size = state.getDbmsSpecificOptions().verticalPartitioningQuerySettingsProfileSize;
        if (size != 3 && size != 5) {
            throw new IllegalArgumentException("--vp-query-settings-profile-size must be 3 or 5, but was " + size);
        }
        List<String> candidates = new ArrayList<>(List.of("optimize_move_to_prewhere=" + Randomly.fromOptions(0, 1),
                "optimize_read_in_order=" + Randomly.fromOptions(0, 1),
                "optimize_aggregation_in_order=" + Randomly.fromOptions(0, 1),
                "optimize_functions_to_subcolumns=" + Randomly.fromOptions(0, 1),
                "optimize_if_chain_to_multiif=" + Randomly.fromOptions(0, 1),
                "optimize_substitute_columns=" + Randomly.fromOptions(0, 1),
                "join_algorithm='"
                        + Randomly.fromOptions("hash", "partial_merge", "grace_hash", "full_sorting_merge", "auto")
                        + "'"));
        Collections.shuffle(candidates);
        return new QuerySettingsProfile(new ArrayList<>(candidates.subList(0, size)));
    }

    private boolean multisetsEqual(QueryResultMultiset left, QueryResultMultiset right) {
        if (!left.columnTypes.equals(right.columnTypes)) {
            return false;
        }
        if (left.rows.equals(right.rows)) {
            return true;
        }
        List<String> leftRows = expandRows(left.rows);
        List<String> rightRows = expandRows(right.rows);
        if (leftRows.size() != rightRows.size()) {
            return false;
        }
        int[] matchedLeftByRight = new int[rightRows.size()];
        java.util.Arrays.fill(matchedLeftByRight, -1);
        for (int leftIndex = 0; leftIndex < leftRows.size(); leftIndex++) {
            boolean[] seenRight = new boolean[rightRows.size()];
            if (!findApproximateMatching(leftIndex, leftRows, rightRows, left.approximateColumns,
                    right.approximateColumns, matchedLeftByRight, seenRight)) {
                return false;
            }
        }
        return true;
    }

    private List<String> expandRows(Map<String, Integer> rows) {
        List<String> expanded = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : rows.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                expanded.add(entry.getKey());
            }
        }
        return expanded;
    }

    private boolean findApproximateMatching(int leftIndex, List<String> leftRows, List<String> rightRows,
            List<ApproximateKind> leftTypes, List<ApproximateKind> rightTypes, int[] matchedLeftByRight,
            boolean[] seenRight) {
        for (int rightIndex = 0; rightIndex < rightRows.size(); rightIndex++) {
            if (seenRight[rightIndex] || !approximateRowEquals(leftRows.get(leftIndex), rightRows.get(rightIndex),
                    leftTypes, rightTypes)) {
                continue;
            }
            seenRight[rightIndex] = true;
            if (matchedLeftByRight[rightIndex] == -1 || findApproximateMatching(matchedLeftByRight[rightIndex],
                    leftRows, rightRows, leftTypes, rightTypes, matchedLeftByRight, seenRight)) {
                matchedLeftByRight[rightIndex] = leftIndex;
                return true;
            }
        }
        return false;
    }

    private boolean orderedRowsEqual(QueryResultMultiset left, QueryResultMultiset right) {
        if (!left.columnTypes.equals(right.columnTypes)) {
            return false;
        }
        if (left.orderedRows.size() != right.orderedRows.size()) {
            return false;
        }
        for (int i = 0; i < left.orderedRows.size(); i++) {
            if (!approximateRowEquals(left.orderedRows.get(i), right.orderedRows.get(i), left.approximateColumns,
                    right.approximateColumns)) {
                return false;
            }
        }
        return true;
    }

    private boolean approximateRowEquals(String left, String right, List<ApproximateKind> leftTypes,
            List<ApproximateKind> rightTypes) {
        List<String> l = decode(left);
        List<String> r = decode(right);
        if (l.size() != r.size()) {
            return false;
        }
        for (int i = 0; i < l.size(); i++) {
            if (java.util.Objects.equals(l.get(i), r.get(i))) {
                continue;
            }
            ApproximateKind kind = ApproximateKind.looser(approximateKindAt(leftTypes, i),
                    approximateKindAt(rightTypes, i));
            if (!kind.isApproximate() || !approximatelyEqual(l.get(i), r.get(i), kind)) {
                return false;
            }
        }
        return true;
    }

    private ApproximateKind approximateKindAt(List<ApproximateKind> types, int index) {
        return index < types.size() ? types.get(index) : ApproximateKind.NONE;
    }

    private List<String> decode(String row) {
        List<String> values = new ArrayList<>();
        for (int at = 0; at < row.length();) {
            if (row.startsWith("N;", at)) {
                values.add(null);
                at += 2;
                continue;
            }
            int colon = row.indexOf(':', at + 2);
            int length = Integer.parseInt(row.substring(at + 2, colon));
            int start = colon + 1;
            values.add(row.substring(start, start + length));
            at = start + length + 1;
        }
        return values;
    }

    private boolean approximatelyEqual(String left, String right, ApproximateKind kind) {
        if (left == null || right == null) {
            return false;
        }
        try {
            BigDecimal a = new BigDecimal(left);
            BigDecimal b = new BigDecimal(right);
            if (!Double.isFinite(a.doubleValue()) || !Double.isFinite(b.doubleValue())) {
                return false;
            }
            BigDecimal delta = a.subtract(b).abs();
            BigDecimal scale = a.abs().max(b.abs());
            return delta.compareTo(BigDecimal.valueOf(kind.absoluteTolerance)) <= 0
                    || delta.compareTo(scale.multiply(BigDecimal.valueOf(kind.relativeTolerance))) <= 0;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    private boolean isNumeric(VpColumn column) {
        String type = scalarType(column);
        return type.startsWith("INT") || type.startsWith("UINT") || type.startsWith("FLOAT")
                || type.startsWith("DECIMAL");
    }

    private boolean isNumeric(ClickHouseColumnReference column) {
        String type = scalarType(column.getColumn().getType().toString());
        return type.startsWith("INT") || type.startsWith("UINT") || type.startsWith("FLOAT")
                || type.startsWith("DECIMAL");
    }

    private boolean isIntegralNumeric(VpColumn column) {
        String type = scalarType(column);
        return type.startsWith("INT") || type.startsWith("UINT");
    }

    private boolean isOrderStableAggregateInput(VpColumn column) {
        return isOrderStableAggregateType(column.type);
    }

    private boolean isOrderStableAggregateType(String typeName) {
        String type = scalarType(typeName);
        return type.startsWith("INT") || type.startsWith("UINT") || type.startsWith("DECIMAL");
    }

    private boolean isFloatLike(VpColumn column) {
        return scalarType(column).startsWith("FLOAT");
    }

    private boolean isFloatLike(ClickHouseColumnReference column) {
        return scalarType(column.getColumn().getType().toString()).startsWith("FLOAT");
    }

    private String scalarType(VpColumn column) {
        return scalarType(column.type);
    }

    private String scalarType(String typeName) {
        String type = typeName.replace(" ", "").toUpperCase(Locale.ROOT);
        while (type.startsWith("NULLABLE(") || type.startsWith("LOWCARDINALITY(")) {
            int open = type.indexOf('(');
            type = type.substring(open + 1, type.length() - 1);
        }
        return type;
    }

    private boolean isIntArray(VpColumn column) {
        return column.type.replace(" ", "").equalsIgnoreCase("Array(Int32)");
    }

    private boolean contains(List<VpColumn> columns, String name) {
        return columns.stream().anyMatch(c -> c.name.equals(name));
    }

    private boolean advancedEnabled() {
        return state.getDbmsSpecificOptions().verticalPartitioningAdvancedQueryFeatures
                || state.getDbmsSpecificOptions().verticalPartitioningDeepExploration;
    }

    private static void recordCoverageConfiguration(ClickHouseGlobalState state) {
        ClickHouseOptions options = state.getDbmsSpecificOptions();
        COVERAGE_CONFIGURATION = configurationSnapshot(options);
    }

    private static String configurationSnapshot(ClickHouseOptions options) {
        boolean advanced = options.verticalPartitioningAdvancedQueryFeatures;
        boolean deep = options.verticalPartitioningDeepExploration;
        boolean advancedEffective = advanced || deep;
        return "advanced=" + advanced + ",deep=" + deep + ",advanced-effective=" + advancedEffective
                + ",aggregate-effective=" + advancedEffective + ",window-effective=" + deep + ",union-all-effective="
                + deep + ",correlated-effective=" + deep + ",boundary-mutation-effective=" + advancedEffective
                + ",complex-types=" + options.verticalPartitioningComplexTypes + ",query-variants="
                + options.verticalPartitioningQueryVariantsPerCombination + ",helper-base-engine="
                + options.verticalPartitioningHelperUseBaseEngine + ",schema-object-copy="
                + options.verticalPartitioningSourceCopySchemaObjects + ",row-deletes="
                + options.verticalPartitioningAllowRowDeletes + ",update-mutations="
                + options.verticalPartitioningUpdateMutations + ",normal-workload="
                + options.verticalPartitioningIncludeNormalWorkload + ",suppress-known-patterns="
                + options.verticalPartitioningSuppressKnownPatterns + ",large-data="
                + options.verticalPartitioningLargeDataExploration + ",query-settings-profiles="
                + options.verticalPartitioningQuerySettingsProfiles + ",native-relations="
                + options.verticalPartitioningNativeRelations + ",distributed-profile="
                + options.verticalPartitioningDistributedProfile + ",distributed-cluster="
                + (options.verticalPartitioningDistributedCluster == null ? ""
                        : options.verticalPartitioningDistributedCluster)
                + ",distributed-min-live-nodes=" + options.verticalPartitioningDistributedMinLiveNodes
                + ",query-settings-profile-size=" + options.verticalPartitioningQuerySettingsProfileSize
                + ",distributed-topology=" + options.verticalPartitioningDistributedTopologyProfile
                + ",distributed-retry=" + options.verticalPartitioningDistributedRetryProfile + ",distributed-failure="
                + options.verticalPartitioningDistributedFailureProfile + ",shard-key-profile="
                + options.verticalPartitioningDistributedShardKeyProfile + ",physical-state="
                + options.verticalPartitioningPhysicalStateProfile + ",multi-gateway="
                + options.verticalPartitioningMultiGatewayProfile + ",source-row-limit="
                + options.verticalPartitioningSourceRowLimit + ",part-layout="
                + options.verticalPartitioningPartLayoutProfile + ",part-inserts-per-table="
                + options.verticalPartitioningPartInsertsPerTable + ",optimize-final="
                + options.verticalPartitioningOptimizeFinalProfile + ",partition-pruning="
                + options.verticalPartitioningPartitionPruningProfile + ",auto-merge="
                + options.verticalPartitioningAutoMergeProfile + ",optimizer-bug-probes="
                + options.verticalPartitioningOptimizerBugProbes;
    }

    private static void recordRunConfiguration(long runId, ClickHouseOptions options) {
        String snapshot = "run=" + runId + " " + configurationSnapshot(options);
        synchronized (STATS_LOCK) {
            CONFIGURATION_SNAPSHOTS.add(snapshot);
        }
    }

    private boolean orderedComparisonEnabled(QueryShape shape) {
        // DISTINCT and aggregate forms cannot append the row identifier without changing their result cardinality.
        return shape.kind == QueryKind.PROJECTION || shape.kind == QueryKind.FILTER || shape.kind == QueryKind.WINDOW
                || shape.kind == QueryKind.NULLABILITY || shape.kind == QueryKind.LOW_CARDINALITY
                || shape.kind == QueryKind.SORTED_TOP_N;
    }

    private ApproximateKind approximateKind(String type) {
        if (type == null) {
            return ApproximateKind.NONE;
        }
        String normalized = type.toUpperCase(Locale.ROOT);
        if (normalized.contains("FLOAT32")) {
            return ApproximateKind.FLOAT32;
        }
        if (normalized.contains("FLOAT64") || normalized.contains("DOUBLE")) {
            return ApproximateKind.FLOAT64;
        }
        return ApproximateKind.NONE;
    }

    private String helperEngine(String baseEngine) {
        // Current VP base tables are row-preserving MergeTree tables. Do not copy engines with
        // deduplication/aggregation
        // semantics; using the option must never silently change the logical relation being tested.
        if (state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine && baseEngine != null) {
            String normalized = baseEngine.toUpperCase(Locale.ROOT);
            if (normalized.equals("MEMORY")) {
                setupAttempt(AdvancedFeature.HELPER_BASE_ENGINE_MEMORY);
                return "ENGINE = Memory";
            }
            if (normalized.equals("LOG")) {
                setupAttempt(AdvancedFeature.HELPER_BASE_ENGINE_LOG);
                return "ENGINE = Log";
            }
            if (normalized.equals("MERGETREE") || normalized.equals("REPLACINGMERGETREE")) {
                setupAttempt(AdvancedFeature.HELPER_BASE_ENGINE_MERGETREE);
                String helperEngine = normalized.equals("REPLACINGMERGETREE") ? "ReplacingMergeTree" : "MergeTree";
                return mergeTreeHelperEngine(helperEngine);
            }
            setupAttempt(AdvancedFeature.HELPER_BASE_ENGINE_REJECTED);
        }
        return mergeTreeHelperEngine("MergeTree");
    }

    private String mergeTreeHelperEngine(String engine) {
        if (state.getDbmsSpecificOptions().verticalPartitioningPhysicalStateProfile
                || state.getDbmsSpecificOptions().verticalPartitioningPartLayoutProfile) {
            return "ENGINE = " + engine + " PARTITION BY cityHash64(" + q(ROW_ID) + ") % 4 ORDER BY " + q(ROW_ID);
        }
        return "ENGINE = " + engine + " ORDER BY " + (largeDataExploration() ? q(ROW_ID) : "tuple()");
    }

    private boolean largeDataExploration() {
        return state.getDbmsSpecificOptions().verticalPartitioningLargeDataExploration;
    }

    private String sourceRowLimitClause() {
        if (largeDataExploration()) {
            return "";
        }
        int configured = state.getDbmsSpecificOptions().verticalPartitioningSourceRowLimit;
        int limit = configured < 0 ? DEFAULT_SOURCE_ROW_LIMIT : configured;
        return limit == 0 ? "" : " LIMIT " + limit;
    }

    private void execute(String sql) throws Exception {
        if (!executeDirect(sql)) {
            throw new IgnoreMeException();
        }
    }

    private void executeSetup(VpSchema vp, String sql) throws Exception {
        execute(sql);
        vp.setupStatements.add(sql);
    }

    private boolean tryExecute(String sql) throws Exception {
        try {
            return executeDirect(sql);
        } catch (AssertionError ignored) {
            // Schema-object copying is best effort, matching the existing VP contract for source objects.
            return false;
        }
    }

    private boolean executeDirect(String sql) throws SQLException {
        if (sql.toUpperCase(Locale.ROOT).contains(" ON CLUSTER ")) {
            synchronized (CLUSTER_DDL_LOCK) {
                return executeDirectUnlocked(sql);
            }
        }
        return executeDirectUnlocked(sql);
    }

    private boolean executeDirectUnlocked(String sql) throws SQLException {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
            Main.nrSuccessfulActions.addAndGet(1);
            return true;
        } catch (SQLException e) {
            Main.nrUnsuccessfulActions.addAndGet(1);
            if (expectedSetupError(e)) {
                return false;
            }
            throw new AssertionError(sql, e);
        }
    }

    private boolean isComparisonMismatch(AssertionError error) {
        String message = error.getMessage();
        return message != null && (message.startsWith("ClickHouse vertical partitioning ")
                && (message.contains(" mismatch (") || message.contains(" ordered mismatch ("))
                || message.startsWith("ClickHouse native relation mismatch")
                || message.startsWith("ClickHouse distributed relation mismatch")
                || knownPattern(error) == KnownPattern.NOT_FOUND_COLUMN_IN_BLOCK);
    }

    private void cleanup(VpSchema vp, boolean preserveReplayTables) throws Exception {
        startHelperMerges(vp);
        if (!preserveReplayTables) {
            for (DistributedAccessPath path : vp.distributedAccessPaths) {
                cleanupDistributedAccessPath(path);
            }
            for (ReplicatedAccessPath path : vp.replicatedAccessPaths) {
                try {
                    executeDirect(
                            "DROP TABLE IF EXISTS " + q(path.distributed) + " ON CLUSTER " + distributedClusterName);
                    executeDirect("DROP TABLE IF EXISTS " + q(path.local) + " ON CLUSTER " + distributedClusterName);
                } catch (Exception cleanupError) {
                    incrementString(PHYSICAL_STATE_COUNTS, "replicated-cleanup-failed");
                }
            }
        }
        List<String> statements = new ArrayList<>(
                List.of("DROP VIEW IF EXISTS " + q(vp.view), "DROP TABLE IF EXISTS " + q(vp.temp)));
        if (!preserveReplayTables) {
            statements.add("DROP TABLE IF EXISTS " + q(vp.right));
            statements.add("DROP TABLE IF EXISTS " + q(vp.left));
            statements.add("DROP TABLE IF EXISTS " + q(vp.source));
        }
        for (String sql : statements) {
            try {
                executeDirect(sql);
            } catch (AssertionError ignored) {
                // Cleanup must not hide the original comparison failure.
            }
        }
    }

    private boolean expectedSetupError(Throwable throwable) {
        return expected(throwable, setupErrors);
    }

    private boolean expectedResultError(Throwable throwable) {
        return expected(throwable, resultErrors);
    }

    private boolean expected(Throwable throwable, ExpectedErrors expectedErrors) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (current.getMessage() != null && expectedErrors.errorIsExpected(current.getMessage())) {
                return true;
            }
        }
        return false;
    }

    private KnownPattern knownPattern(String message) {
        String normalized = message.toUpperCase(Locale.ROOT);
        if (normalized.contains("NOT_FOUND_COLUMN_IN_BLOCK") || normalized.contains("NOT FOUND COLUMN")) {
            return KnownPattern.NOT_FOUND_COLUMN_IN_BLOCK;
        }
        if (normalized.contains("PROJECTION_NOT_USED")) {
            return KnownPattern.PROJECTION_NOT_USED;
        }
        return normalized.contains("NAN") || normalized.contains("INF") ? KnownPattern.NONFINITE_FLOAT : null;
    }

    private KnownPattern knownPattern(Throwable throwable) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (current.getMessage() != null) {
                KnownPattern known = knownPattern(current.getMessage());
                if (known != null) {
                    return known;
                }
            }
        }
        return null;
    }

    private static <E extends Enum<E>> Map<E, AtomicLong> counters(E[] values) {
        Map<E, AtomicLong> counters = new EnumMap<>(values[0].getDeclaringClass());
        for (E value : values) {
            counters.put(value, new AtomicLong());
        }
        return counters;
    }

    private static void setupAttempt(AdvancedFeature feature) {
        if (feature != null) {
            SETUP_ATTEMPT_COUNTS.get(feature).incrementAndGet();
        }
    }

    private static void setupSuccess(AdvancedFeature feature) {
        if (feature != null) {
            SETUP_SUCCESS_COUNTS.get(feature).incrementAndGet();
        }
    }

    private static void queryAttempt(AdvancedFeature feature) {
        if (feature != null) {
            QUERY_ATTEMPT_COUNTS.get(feature).incrementAndGet();
        }
    }

    private static void querySuccess(AdvancedFeature feature) {
        if (feature != null) {
            QUERY_SUCCESS_COUNTS.get(feature).incrementAndGet();
        }
    }

    private static void querySkip(AdvancedFeature feature, String reason) {
        if (feature != null) {
            QUERY_SKIP_COUNTS.get(feature).incrementAndGet();
            FEATURE_SKIP_REASON_COUNTS.computeIfAbsent(feature + ":" + reason, ignored -> new AtomicLong())
                    .incrementAndGet();
        }
    }

    private static IgnoreMeException ignore(Throwable cause) {
        IgnoreMeException ignored = new IgnoreMeException();
        ignored.initCause(cause);
        return ignored;
    }

    private static String skipReason(IgnoreMeException ignored) {
        Throwable cause = ignored.getCause();
        if (cause == null) {
            return "UNSPECIFIED";
        }
        String message = exceptionMessage(cause);
        if (message.contains("ARGUMENT_OUT_OF_BOUND")) {
            return "EXPECTED_ARGUMENT_OUT_OF_BOUND";
        } else if (message.contains("Decimal value is too big")) {
            return "EXPECTED_DECIMAL_RANGE";
        } else if (message.contains("Correlated subqueries are not supported")
                || message.contains("can't find correlated column")) {
            return "EXPECTED_CORRELATED_UNSUPPORTED";
        } else if (message.contains("Unknown data type: Variant")) {
            return "EXPECTED_VARIANT";
        } else if (message.contains("Cannot parse")) {
            return "EXPECTED_PARSE";
        } else if (message.contains("Illegal type") || message.contains("ILLEGAL_TYPE_OF_ARGUMENT")
                || message.contains("Nested type")) {
            return "EXPECTED_TYPE";
        } else if (message.contains("Not found column") || message.contains("Unknown identifier")) {
            return "EXPECTED_IDENTIFIER";
        } else if (message.contains("Cannot convert")) {
            return "EXPECTED_CONVERSION";
        } else if (message.contains("Memory limit")) {
            return "EXPECTED_MEMORY";
        }
        return "EXPECTED_OTHER";
    }

    private static String exceptionMessage(Throwable throwable) {
        StringBuilder messages = new StringBuilder();
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (current.getMessage() != null) {
                messages.append(current.getMessage()).append('\n');
            }
        }
        return messages.toString();
    }

    private static synchronized void schemaShapeCount(String key) {
        SCHEMA_SHAPE_COUNTS.computeIfAbsent(key, ignored -> new AtomicLong()).incrementAndGet();
    }

    private static void incrementString(Map<String, AtomicLong> counts, String key) {
        counts.computeIfAbsent(key, ignored -> new AtomicLong()).incrementAndGet();
    }

    private static void recordSchemaShapeSample() {
        SCHEMA_SHAPE_SAMPLES.incrementAndGet();
    }

    private static String normalizeStatKey(String value) {
        return value == null ? "unknown" : value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-");
    }

    private static void registerStatsHook() {
        if (STATS_HOOK.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(
                    new Thread(ClickHouseVerticalPartitioningOracle::flushFinalStats, "sqlancer-clickhouse-vp-stats"));
        }
    }

    private static synchronized void flushStats() {
        writeStats("process-checkpoint");
    }

    private static synchronized void flushFinalStats() {
        writeStats("process-final");
    }

    private static void writeStats(String event) {
        File file = new File(new File(Main.LOG_DIRECTORY, "clickhouse"), "vp-stats.log");
        String text = "-- [vp-stats] event=" + event + " dbms=clickhouse\n" + "-- [vp-stats] oracle-runs completed="
                + COMPLETED_RUNS.get() + " pairs=" + COMPLETED_PAIRS.get() + " attempts=" + ATTEMPTS.get() + "\n"
                + "-- [vp-stats] coverage-configuration " + COVERAGE_CONFIGURATION + "\n" + configurationSnapshotLines()
                + "-- [vp-stats] split-counts " + format(SPLIT_COUNTS) + "\n" + "-- [vp-stats] selected-shape-counts "
                + format(SHAPE_COUNTS) + "\n" + "-- [vp-stats] reconstruction-counts " + format(RECONSTRUCTION_COUNTS)
                + "\n" + "-- [vp-stats] reconstruction-semantic-counts " + format(RECONSTRUCTION_SEMANTIC_COUNTS) + "\n"
                + "-- [vp-stats] schema-shape samples=" + SCHEMA_SHAPE_SAMPLES.get() + " " + format(SCHEMA_SHAPE_COUNTS)
                + "\n" + "-- [vp-stats] known-suppressed-counts " + format(KNOWN_PATTERN_COUNTS) + "\n"
                + "-- [vp-stats] setup-attempt-counts " + format(SETUP_ATTEMPT_COUNTS) + "\n"
                + "-- [vp-stats] setup-success-counts " + format(SETUP_SUCCESS_COUNTS) + "\n"
                + "-- [vp-stats] query-attempt-counts " + format(QUERY_ATTEMPT_COUNTS) + "\n"
                + "-- [vp-stats] query-success-counts " + format(QUERY_SUCCESS_COUNTS) + "\n"
                + "-- [vp-stats] query-skip-counts " + format(QUERY_SKIP_COUNTS) + "\n"
                + "-- [vp-stats] feature-skip-reasons " + format(FEATURE_SKIP_REASON_COUNTS) + "\n"
                + "-- [vp-stats] comparison-counts typed=" + TYPED_COMPARISONS.get() + " ordered="
                + ORDERED_COMPARISONS.get() + "\n" + "-- [vp-stats] comparison-level-counts "
                + format(COMPARISON_LEVEL_COUNTS) + "\n" + "-- [vp-stats] settings-profile-counts "
                + format(SETTINGS_PROFILE_COUNTS) + "\n" + "-- [vp-stats] distributed-profile-counts "
                + format(DISTRIBUTED_PROFILE_COUNTS) + "\n" + "-- [vp-stats] distributed-topology-counts "
                + format(DISTRIBUTED_TOPOLOGY_COUNTS) + "\n" + "-- [vp-stats] distributed-retry-counts "
                + format(DISTRIBUTED_RETRY_COUNTS) + "\n" + "-- [vp-stats] distributed-failure-counts "
                + format(DISTRIBUTED_FAILURE_COUNTS) + "\n" + "-- [vp-stats] physical-state-counts "
                + format(PHYSICAL_STATE_COUNTS) + "\n" + "-- [vp-stats] native-relation-counts "
                + format(NATIVE_RELATION_COUNTS) + "\n" + "-- [vp-stats] native-relation-mismatch-counts "
                + format(NATIVE_RELATION_MISMATCH_COUNTS) + "\n";
        try {
            Path target = file.toPath();
            Files.createDirectories(target.getParent());
            Path temporary = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
            try {
                Files.writeString(temporary, text, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
                try {
                    Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                    Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                Files.deleteIfExists(temporary);
            }
        } catch (IOException ignored) {
            // Stats are observational and must not affect fuzzing.
        }
    }

    private static String configurationSnapshotLines() {
        synchronized (STATS_LOCK) {
            if (CONFIGURATION_SNAPSHOTS.isEmpty()) {
                return "-- [vp-stats] oracle-run-configurations none\n";
            }
            return CONFIGURATION_SNAPSHOTS.stream()
                    .map(snapshot -> "-- [vp-stats] oracle-run-configuration " + snapshot + "\n")
                    .collect(Collectors.joining());
        }
    }

    private static String format(Map<?, AtomicLong> values) {
        return values.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue().get())
                .collect(Collectors.joining(","));
    }

    private String columnDefinition(List<VpColumn> columns) {
        return columns.stream().map(c -> q(c.name) + " " + c.type).collect(Collectors.joining(", "));
    }

    private String sourceColumns(List<VpColumn> columns) {
        return columns.stream().map(c -> q(c.name)).collect(Collectors.joining(", "));
    }

    private String optionalDefinitions(List<VpColumn> columns) {
        return columns.isEmpty() ? "" : ", " + columnDefinition(columns);
    }

    private String optionalNames(List<VpColumn> columns) {
        return columns.isEmpty() ? "" : ", " + sourceColumns(columns);
    }

    private String q(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
