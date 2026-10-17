package sqlancer.databend.oracle;

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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.databend.DatabendBugs;
import sqlancer.databend.DatabendErrors;
import sqlancer.databend.DatabendExpressionGenerator;
import sqlancer.databend.DatabendProvider;
import sqlancer.databend.DatabendProvider.DatabendGlobalState;
import sqlancer.databend.DatabendSchema.DatabendColumn;
import sqlancer.databend.DatabendSchema.DatabendCompositeDataType;
import sqlancer.databend.DatabendSchema.DatabendDataType;
import sqlancer.databend.DatabendSchema.DatabendTable;
import sqlancer.databend.ast.DatabendColumnReference;
import sqlancer.databend.ast.DatabendConstant;
import sqlancer.databend.ast.DatabendConstant.DatabendIntConstant;
import sqlancer.databend.ast.DatabendAggregateOperation;
import sqlancer.databend.ast.DatabendAggregateOperation.DatabendAggregateFunction;
import sqlancer.databend.ast.DatabendExpression;
import sqlancer.databend.ast.DatabendSelect;
import sqlancer.databend.ast.DatabendTableReference;
import sqlancer.databend.ast.DatabendText;
import sqlancer.databend.visitor.DatabendVisitor;

public class DatabendVerticalPartitioningOracle implements TestOracle<DatabendGlobalState> {

    private static final double FLOAT_ABSOLUTE_TOLERANCE = 1e-6;
    private static final double FLOAT_RELATIVE_TOLERANCE = 1e-6;
    private static final double DOUBLE_ABSOLUTE_TOLERANCE = 1e-12;
    private static final double DOUBLE_RELATIVE_TOLERANCE = 1e-12;

    private static final class EngineLayout {
        final String sourceEngine;
        final String leftEngine;
        final String rightEngine;

        EngineLayout(String sourceEngine, String leftEngine, String rightEngine) {
            this.sourceEngine = sourceEngine;
            this.leftEngine = leftEngine;
            this.rightEngine = rightEngine;
        }
    }

    private static final AtomicLong ORACLE_RUN_ID = new AtomicLong();
    private static final String ROW_ID = "vp_rowid";
    private static final int MAX_DIFF_ROWS = 8;
    private static final int MAX_RESULT_SAMPLE = 12;
    private static final int MAX_DIAGNOSTIC_ROWS = 256;
    private static final int MAX_DIAGNOSTIC_CHARS = 65536;
    private static final int MAX_SKIPPED_ERROR_GROUPS = 200;
    private static final int MAX_SKIPPED_ERROR_SAMPLE_GROUPS = 50;
    private static final int MAX_SKIPPED_ERROR_SAMPLES_PER_GROUP = 2;
    private static final Pattern VP_ROW_ID_TOP_N_PATTERN = Pattern.compile(
            "\\bORDER BY\\s+(.+?)\\s+LIMIT\\s+(\\d+)(?:\\s+OFFSET\\s+(\\d+))?\\s*;?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern TOP_N_PRUNING_PATTERN = Pattern.compile("TOPN PRUNING:\\s*(\\d+)\\s+TO\\s+(\\d+)",
            Pattern.CASE_INSENSITIVE);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final List<String> REPRODUCIBILITY_SETTINGS = List.of("timezone", "sql_dialect",
            "default_order_by_null", "numeric_cast_option", "enable_cbo", "disable_join_reorder",
            "enable_bloom_runtime_filter", "enable_join_runtime_filter", "enable_query_result_cache", "max_threads");
    private static final Object STATS_LOCK = new Object();
    private static final Map<SplitStrategy, Integer> SPLIT_STRATEGY_COUNTS = new EnumMap<>(SplitStrategy.class);
    private static final Map<BaseShape, Integer> SELECTED_BASE_SHAPE_COUNTS = new EnumMap<>(BaseShape.class);
    private static final Map<ReconstructionStrategy, Integer> RECONSTRUCTION_STRATEGY_COUNTS = new EnumMap<>(
            ReconstructionStrategy.class);
    private static final Map<ReconstructionStrategy, Map<PipelineMetric, Integer>> PIPELINE_COUNTS = new EnumMap<>(
            ReconstructionStrategy.class);
    private static final Map<KnownVpBug, Integer> KNOWN_BUG_COUNTS = new EnumMap<>(KnownVpBug.class);
    private static final Map<KnownVpCrash, Integer> KNOWN_CRASH_COUNTS = new EnumMap<>(KnownVpCrash.class);
    private static final Map<String, Integer> SKIPPED_ERROR_COUNTS = new LinkedHashMap<>();
    private static final Map<String, List<SkippedErrorSample>> SKIPPED_ERROR_SAMPLES = new LinkedHashMap<>();
    private static final Map<QueryErrorDisposition, Integer> SKIPPED_ERROR_CATEGORY_COUNTS = new EnumMap<>(
            QueryErrorDisposition.class);
    private static final Map<ApproximateKind, Integer> APPROXIMATE_MATCH_COUNTS = new EnumMap<>(ApproximateKind.class);
    private static final Map<ApproximateKind, Long> APPROXIMATE_MAX_ULPS = new EnumMap<>(ApproximateKind.class);
    private static final Map<AdvancedFeature, Integer> ADVANCED_FEATURE_COUNTS = new EnumMap<>(AdvancedFeature.class);
    private static final Map<String, Map<String, Integer>> FEATURE_PIPELINE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SCHEMA_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean();
    private static final AtomicLong KNOWN_BUG_LOG_ID = new AtomicLong();
    private static final File DATABEND_LOG_DIRECTORY = new File(Main.LOG_DIRECTORY, "databend");
    private static final File STATS_FILE = new File(DATABEND_LOG_DIRECTORY, "vp-stats.log");
    private static final File SKIPPED_ERRORS_FILE = new File(DATABEND_LOG_DIRECTORY, "vp-skipped-errors.log");
    private static int startedOracleRuns;
    private static int completedOracleRuns;
    private static int abortedOracleRuns;
    private static int completedQueryPairs;
    private static int completedAttempts;
    private static int totalQueryPairs;
    private static int totalAttempts;
    private static int schemaShapeSamples;

    private final DatabendGlobalState state;
    private final ExpectedErrors errors = new ExpectedErrors();
    private final boolean knownDatabendVersion;
    private final boolean databend950Build;
    private static final Map<String, Long> QUERY_QUALITY_COUNTS = new LinkedHashMap<>();
    private static final List<String> QUERY_QUALITY_SAMPLES = new ArrayList<>();
    private boolean streamLicenseExpired;

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
        NATURAL_OR_USING, LATERAL_DERIVED
    }

    private enum PipelineMetric {
        GENERATED("generated"), SETUP_SUCCESS("setup_success"), SINGLE_SUCCESS("single_ok"), SPLIT_SUCCESS("split_ok"),
        COMPARED("compared"), PRE_EXECUTION_SUPPRESSED("pre_execution_suppressed"),
        KNOWN_CRASH_SUPPRESSED("known_crash_suppressed"), KNOWN_BUG_SUPPRESSED("known_bug_suppressed"),
        SKIPPED_PARSER_ERROR("skipped_parser_error"), SKIPPED_SERVER_ERROR("skipped_server_error"),
        MISMATCH("mismatch"), APPROXIMATE_MATCH("approximate_match"),
        CANONICALIZATION_COLLISION("canonicalization_collision");

        private final String logName;

        PipelineMetric(String logName) {
            this.logName = logName;
        }
    }

    enum SchemaProfile {
        PROJECTION_ONLY, NULL_DEFAULT, CONSTRAINTS;

        static SchemaProfile fromOption(String value) {
            if (value == null) {
                return NULL_DEFAULT;
            }
            try {
                return valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                return NULL_DEFAULT;
            }
        }
    }

    static String schemaProfileForTesting(String value) {
        return SchemaProfile.fromOption(value).name();
    }

    static String parsedColumnForTesting(String createTable, String columnName) {
        ColumnDefinition definition = parseTableDefinition(createTable).columns
                .get(columnName.toLowerCase(Locale.ROOT));
        if (definition == null) {
            return "<missing>";
        }
        return (definition.nullable ? "NULL" : "NOT NULL") + ":" + definition.defaultExpression;
    }

    static List<String> parsedConstraintsForTesting(String createTable) {
        return parseTableDefinition(createTable).constraints;
    }

    static String parsedSqlTypeForTesting(String createTable, String columnName) {
        ColumnDefinition definition = parseTableDefinition(createTable).columns
                .get(columnName.toLowerCase(Locale.ROOT));
        return definition == null ? "<missing>" : definition.sqlType;
    }

    static String firstUniqueColumnForTesting(List<String> columns, String name) {
        Map<String, String> unique = new LinkedHashMap<>();
        for (String column : columns) {
            unique.putIfAbsent(columnKey(column), column);
        }
        return unique.get(columnKey(name));
    }

    private static String columnKey(String name) {
        return name == null ? "" : name.trim().replace("`", "").toLowerCase(Locale.ROOT);
    }

    static String effectiveBaseShapeForTesting(long rowCount, int columnCount) {
        return effectiveBaseShape(rowCount, columnCount).name();
    }

    private static BaseShape effectiveBaseShape(long rowCount, int columnCount) {
        return rowCount == 0 ? BaseShape.EMPTY : columnCount == 1 ? BaseShape.SINGLE_COLUMN : BaseShape.MULTI_COLUMN;
    }

    private enum QueryErrorDisposition {
        CONFIRMED_FEATURE_GAP(true), LICENSE_RESTRICTION(true), CONFIRMED_GENERATION_ERROR(true),
        CONFIRMED_VERSION_BUG(true), STORAGE_TIMEOUT(false), UNCONFIRMED_SERVER_ERROR(false);

        private final boolean skippable;

        QueryErrorDisposition(boolean skippable) {
            this.skippable = skippable;
        }
    }

    private static final class SkippedErrorSample {
        private final String stage;
        private final String strategy;
        private final String sql;

        private SkippedErrorSample(String stage, String strategy, String sql) {
            this.stage = stage;
            this.strategy = strategy;
            this.sql = sql;
        }
    }

    /** Databend result-difference bugs with independently verified reproducers. */
    private enum KnownVpBug {
        // Observed in logs/databend911/2bug.log.
        DATABEND_FULL_OUTER_PREDICATE_PUSHDOWN_DUPLICATION,
        // Observed in logs/databend912/database4.log: LATERAL aggregate loses one row.
        DATABEND_LATERAL_MIN_GROUP_LIMIT_DROP,
        // Observed in logs/databend912/database6.log: LATERAL aggregate loses all rows.
        DATABEND_LATERAL_SUM_GROUP_EMPTY,
        // https://github.com/databendlabs/databend/issues/20601
        // LEFT JOIN + filtered FUSE build + GROUP/ORDER/LIMIT truncates the
        // runtime-filter key set before the predicate is applied.
        DATABEND_RUNTIME_FILTER_RANK_LIMIT_TRUNCATION,
        // Observed in logs/databend915/database15.log and eleven reports in
        // logs/databend916. FUSE Top-N block pruning can stop after blocks
        // containing LIMIT rows even though a pruned block has a smaller key.
        DATABEND_FUSE_TOPN_BLOCK_PRUNING_ROW_SKIP,
        // Observed in logs/databend914/database56.log and across thirteen
        // reconstruction strategies in logs/databend916. ARRAY_AGG skips NULL
        // ARRAY values on a direct scan, but reconstruction can materialize
        // each as [].
        DATABEND_ARRAY_AGG_NULL_JOIN_MATERIALIZATION,
        // FLOAT AVG/SUM with both extreme signs is execution-order dependent.
        DATABEND_FLOAT_AGGREGATE_CANCELLATION,
        // Observed in logs/databend916/34bug.log. A leading untyped Null in a
        // composite GROUP BY key is not merged across MEMORY blocks.
        DATABEND_LEADING_NULL_COMPOSITE_GROUP_KEY,
        // databend917: 2/3/6bug and database7/8/9.
        DATABEND_NULLABLE_COMPLEX_COUNT_METADATA,
        // databend917: 13/16/17bug.
        DATABEND_MEMORY_PREDICATE_FILTER,
        // databend917: 10/11bug.
        DATABEND_FUSE_EMPTY_PATTERN_LIKE,
        // databend917: 20bug; key-only projection also fails (issue 20639).
        DATABEND_TEMP_GEOMETRY_TOPN,
        // databend917: 4bug; keep the outer-join/grouping case separate.
        DATABEND_OUTER_JOIN_LIKE_NULL_AVG,
        // databend917: 5bug.
        DATABEND_TEMP_DATE_ORDER_LIMIT,
        // databend918: 0bug/2bug/7bug; 919/database6 and current 921/database2,9 also lose nullable self filters.
        DATABEND_NULLABLE_SELF_COMPARISON_FILTER_LOSS, // databend919/database20: NOT BETWEEN is reversed while pushed
                                                       // through RIGHT JOIN.
        DATABEND_OUTER_JOIN_RANGE_PREDICATE_DIRECTION, // databend920/database7: correlated IN decorrelation admits NULL
                                                       // FUSE probe rows.
        DATABEND_CORRELATED_IN_NULL_FILTER_LEAK, DATABEND_CORRELATED_SCALAR_FLOAT_ZERO_NULL_MATCH,
        DATABEND_FLOAT_SELF_NOT_IN_JOIN_FILTER_LOSS, DATABEND_FUSE_TOPN_ORDERED_ROW_REPLACEMENT,
        // databend921/database5: JOIN path treats NOT (nullable LIKE AND LIKE) as true for NULL.
        DATABEND_NULLABLE_CONJUNCTIVE_LIKE_FILTER_LOSS;
    }

    private enum KnownVpBugFamily {
        FUSE_SCAN_PRUNING, RECONSTRUCTED_FILTER_PUSHDOWN
    }

    private static KnownVpBugFamily knownBugFamily(KnownVpBug bug) {
        switch (bug) {
        case DATABEND_FUSE_EMPTY_PATTERN_LIKE: // 917/10 and 920/4
        case DATABEND_TEMP_GEOMETRY_TOPN: // 917/20
        case DATABEND_FUSE_TOPN_BLOCK_PRUNING_ROW_SKIP: // 915/8
        case DATABEND_FUSE_TOPN_ORDERED_ROW_REPLACEMENT: // 920/1,5,16
            return KnownVpBugFamily.FUSE_SCAN_PRUNING;
        case DATABEND_FULL_OUTER_PREDICATE_PUSHDOWN_DUPLICATION: // 911/2
        case DATABEND_OUTER_JOIN_LIKE_NULL_AVG: // 917/4
        case DATABEND_NULLABLE_SELF_COMPARISON_FILTER_LOSS: // 918/0
        case DATABEND_OUTER_JOIN_RANGE_PREDICATE_DIRECTION: // 919/20
        case DATABEND_FLOAT_SELF_NOT_IN_JOIN_FILTER_LOSS: // 920/7
        case DATABEND_NULLABLE_CONJUNCTIVE_LIKE_FILTER_LOSS: // 921/5
            return KnownVpBugFamily.RECONSTRUCTED_FILTER_PUSHDOWN;
        default:
            return null;
        }
    }

    /** Query shapes known to abort Databend query execution. */
    private enum KnownVpCrash {
        DATABEND_CORRELATED_IN_RANGE_MIN, DATABEND_PARTITION_TOTAL_ASSERTION,
        // Databend 1.2.925 unwraps NULL result cells in several nullable
        // composite/ordered projections and aborts with Code 1104.
        DATABEND_NULL_RESULT_UNWRAP,
        // databend919/59bug.log: scalar lookup + EXISTS + signed self NOT IN.
        DATABEND_CORRELATED_EXISTS_SIGNED_NOT_IN,
        // https://github.com/databendlabs/databend/issues/20484
        // Covers the narrow SQL shapes observed in databend912 and
        // databend914. All raise partition.total > 0, and normalizing their
        // +/-0.0 grouping/sort key with + 0.0 prevents the assertion.
        DATABEND_SIGNED_ZERO_PARTITION_TOTAL_ASSERTION
    }

    public enum AdvancedFeature {
        ROLLUP, CORRELATED_SUBQUERY, GROUP_CONCAT, ASYMMETRIC_INDEX, BOUNDARY_MUTATION, FLATTEN, UNNEST, JSON_PATH,
        COPY_INTO, INSERT_OVERWRITE, STREAM, TIME_TRAVEL_TIMESTAMP, TIME_TRAVEL_SNAPSHOT, TIME_TRAVEL_SNAPSHOT_MIXED,
        CLUSTER_KEY, CLUSTERING_INFORMATION, JSON_PATH_PREDICATE, ARRAY_AGG, DECIMAL_BOUNDARY, BITMAP_AGGREGATION,
        QUALIFY_WINDOW, SET_OPERATION_TYPE_MERGE, PROJECTION_PRUNING, RUNTIME_FILTER, JOIN_REORDER, TOP_K_PUSHDOWN,
        NULL_ORDERING, COMPACTION_READ, SCHEMA_EVOLUTION, SPILL, DELETE_MUTATION, MUTATION_CONSISTENCY,
        SEMISTRUCTURED_CONSISTENCY
    }

    private static final class VpColumn {
        private final String name;
        private final DatabendDataType type;
        private final DatabendCompositeDataType compositeType;
        private final boolean nullable;
        private final String defaultExpression;
        private final String declaredSqlType;

        private VpColumn(String name, DatabendCompositeDataType compositeType) {
            this(name, compositeType, true, null, null);
        }

        private VpColumn(String name, DatabendCompositeDataType compositeType, boolean nullable,
                String defaultExpression) {
            this(name, compositeType, nullable, defaultExpression, null);
        }

        private VpColumn(String name, DatabendCompositeDataType compositeType, boolean nullable,
                String defaultExpression, String declaredSqlType) {
            this.name = name;
            this.type = compositeType.getPrimitiveDataType();
            this.compositeType = compositeType;
            this.nullable = nullable;
            this.defaultExpression = defaultExpression;
            this.declaredSqlType = declaredSqlType;
        }
    }

    private static final class BaseTableInfo {
        private final String tableName;
        private final List<VpColumn> columns;
        private final BaseShape shape;
        private final String stableOrderStrategy;
        private final long rowCount;
        private final String engine;
        private final List<String> constraints;

        private BaseTableInfo(String tableName, List<VpColumn> columns, BaseShape shape, String stableOrderStrategy,
                long rowCount, String engine, List<String> constraints) {
            this.tableName = tableName;
            this.columns = columns;
            this.shape = shape;
            this.stableOrderStrategy = stableOrderStrategy;
            this.rowCount = rowCount;
            this.engine = engine;
            this.constraints = constraints;
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
        private final boolean clusteredHelpers;
        private final boolean insertOverwrite;
        private final SchemaProfile schemaProfile;
        private final List<String> baseConstraints;
        private String sourceEngine;
        private String leftEngine;
        private String rightEngine;
        private final List<String> setupStatements = new ArrayList<>();
        private boolean mutationIncomplete;
        private boolean wideInitialized;
        private String semiDocumentColumn;
        private String semiOppositeColumn;

        private VpSchema(String prefix, String baseTable, String sourceTable, String leftTable, String rightTable,
                String viewName, String tempTable, List<VpColumn> columns, List<VpColumn> leftColumns,
                List<VpColumn> rightColumns, List<VpColumn> overlapColumns, BaseShape baseShape,
                SplitStrategy splitStrategy, boolean usesPhysicalForeignKey, String stableOrderStrategy,
                long baseRowCount, String baseEngine, boolean clusteredHelpers, boolean insertOverwrite,
                SchemaProfile schemaProfile, List<String> baseConstraints) {
            this.prefix = prefix;
            this.baseTable = baseTable;
            this.sourceTable = sourceTable;
            this.leftTable = leftTable;
            this.rightTable = rightTable;
            this.viewName = viewName;
            this.tempTable = tempTable;
            this.columns = new ArrayList<>(columns);
            this.leftColumns = new ArrayList<>(leftColumns);
            this.rightColumns = new ArrayList<>(rightColumns);
            this.overlapColumns = new ArrayList<>(overlapColumns);
            this.baseShape = baseShape;
            this.splitStrategy = splitStrategy;
            this.usesPhysicalForeignKey = usesPhysicalForeignKey;
            this.stableOrderStrategy = stableOrderStrategy;
            this.baseRowCount = baseRowCount;
            this.baseEngine = baseEngine;
            this.clusteredHelpers = clusteredHelpers;
            this.insertOverwrite = insertOverwrite;
            this.schemaProfile = schemaProfile;
            this.baseConstraints = baseConstraints;
            this.sourceEngine = "FUSE";
            this.leftEngine = "FUSE";
            this.rightEngine = "FUSE";
        }
    }

    private static final class VpQueryPair {
        private final VpSchema schema;
        private final ReconstructionStrategy reconstructionStrategy;
        private final String singleQuery;
        private final String splitQuery;
        private final List<String> extraSetupStatements;
        private final List<String> cleanupStatements = new ArrayList<>();
        private final Map<Integer, String> mutationCheckpoints = new LinkedHashMap<>();
        private final String description;
        private final boolean orderedComparisonEnabled;
        private final boolean approximateComparisonEnabled;
        private final List<DatabendDataType> resultTypes;

        private VpQueryPair(VpSchema schema, ReconstructionStrategy reconstructionStrategy, String singleQuery,
                String splitQuery, List<String> extraSetupStatements, String description,
                boolean orderedComparisonEnabled, boolean approximateComparisonEnabled) {
            this(schema, reconstructionStrategy, singleQuery, splitQuery, extraSetupStatements, description,
                    orderedComparisonEnabled, approximateComparisonEnabled, List.of());
        }

        private VpQueryPair(VpSchema schema, ReconstructionStrategy reconstructionStrategy, String singleQuery,
                String splitQuery, List<String> extraSetupStatements, String description,
                boolean orderedComparisonEnabled, boolean approximateComparisonEnabled,
                List<DatabendDataType> resultTypes) {
            this.schema = schema;
            this.reconstructionStrategy = reconstructionStrategy;
            this.singleQuery = singleQuery;
            this.splitQuery = splitQuery;
            this.extraSetupStatements = extraSetupStatements;
            this.description = description;
            this.orderedComparisonEnabled = orderedComparisonEnabled;
            this.approximateComparisonEnabled = approximateComparisonEnabled;
            this.resultTypes = resultTypes;
        }
    }

    private static final class QueryResultMultiset {
        private final Map<String, Integer> rows;
        private final Map<String, List<ApproximateKind>> approximateColumns;
        private final List<String> orderedRows;
        private final List<List<ApproximateKind>> orderedApproximateColumns;
        private final boolean approximateComparisonEnabled;

        private QueryResultMultiset(Map<String, Integer> rows, Map<String, List<ApproximateKind>> approximateColumns,
                List<String> orderedRows, List<List<ApproximateKind>> orderedApproximateColumns,
                boolean approximateComparisonEnabled) {
            this.rows = rows;
            this.approximateColumns = approximateColumns;
            this.orderedRows = orderedRows;
            this.orderedApproximateColumns = orderedApproximateColumns;
            this.approximateComparisonEnabled = approximateComparisonEnabled;
        }
    }

    private enum ApproximateKind {
        NONE(0, 0, 0), FLOAT(4, FLOAT_ABSOLUTE_TOLERANCE, FLOAT_RELATIVE_TOLERANCE),
        DOUBLE(8, DOUBLE_ABSOLUTE_TOLERANCE, DOUBLE_RELATIVE_TOLERANCE);

        private final long maxUlps;
        private final double absoluteTolerance;
        private final double relativeTolerance;

        ApproximateKind(long maxUlps, double absoluteTolerance, double relativeTolerance) {
            this.maxUlps = maxUlps;
            this.absoluteTolerance = absoluteTolerance;
            this.relativeTolerance = relativeTolerance;
        }

        private boolean isApproximate() {
            return this != NONE;
        }

        private static ApproximateKind looser(ApproximateKind left, ApproximateKind right) {
            if (left == DOUBLE || right == DOUBLE) {
                return DOUBLE;
            }
            if (left == FLOAT || right == FLOAT) {
                return FLOAT;
            }
            return NONE;
        }
    }

    public DatabendVerticalPartitioningOracle(DatabendGlobalState state) {
        this.state = state;
        String detectedVersion;
        try {
            detectedVersion = state.getConnection().getDatabaseVersion();
        } catch (SQLException e) {
            detectedVersion = null;
        }
        this.knownDatabendVersion = isKnownDatabendVersion(detectedVersion);
        String build950 = "8.0.90-v1.2.950-nightly-7d6d8c143a";
        this.databend950Build = detectedVersion != null
                && (detectedVersion.equals(build950) || detectedVersion.startsWith(build950 + "("));
        registerVpStatsShutdownHook();
        DatabendErrors.addExpressionErrors(errors);
        errors.add("This version of Databend doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'");
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
        errors.add("Unsupported modify column");
    }

    @Override
    public void check() throws Exception {
        resetSessionForVerticalPartitioningOracle();
        DatabendProvider.ensureVerticalPartitioningBaseTables(state, 2);
        BaseTableInfo baseTable = selectBaseTable();
        int queryVariantsPerCombination = Math.max(1,
                state.getDbmsSpecificOptions().verticalPartitioningQueryVariantsPerCombination);
        int checkedPairs = 0;
        int attempts = 0;
        recordOracleRunStarted();
        boolean completed = false;
        try {
            List<SplitStrategy> splitSchedule = createSplitSchedule(baseTable.shape);
            for (SplitStrategy splitStrategy : splitSchedule) {
                VpSchema vp = materializeTable(baseTable, splitStrategy);
                incrementSplitStrategy(vp.splitStrategy);
                List<ReconstructionStrategy> reconstructionSchedule = createReconstructionSchedule(vp);
                if (reconstructionSchedule.contains(ReconstructionStrategy.TEMP_TABLE)) {
                    prepareReusableTemporaryTable(vp);
                }
                for (ReconstructionStrategy strategy : reconstructionSchedule) {
                    for (int i = 0; i < queryVariantsPerCombination; i++) {
                        attempts++;
                        recordAttempt();
                        try {
                            VpQueryPair pair = generateRandomRewritePair(vp, strategy);
                            incrementReconstructionStrategy(pair.reconstructionStrategy);
                            recordPipelineMetric(pair.reconstructionStrategy, PipelineMetric.GENERATED);
                            if (suppressFatalCrashBeforeExecution(pair)) {
                                recordPipelineMetric(pair.reconstructionStrategy,
                                        PipelineMetric.PRE_EXECUTION_SUPPRESSED);
                                recordPipelineMetric(pair.reconstructionStrategy,
                                        PipelineMetric.KNOWN_CRASH_SUPPRESSED);
                                throw new IgnoreMeException();
                            }
                            comparePair(pair);
                            checkedPairs++;
                            recordCheckedPair();
                        } catch (IgnoreMeException ignored) {
                            if (vp.mutationIncomplete) {
                                // A partially applied mutation must never become the next query's fixture.
                                throw ignored;
                            }
                        }
                    }
                }
                cleanupIfNoBug(vp);
            }
            if (checkedPairs == 0) {
                throw new IgnoreMeException();
            }
            DatabendProvider.replaceVerticalPartitioningTableAfterSuccessfulCheck(state, baseTable.tableName);
            recordCompletedVpRun(checkedPairs, attempts);
            completed = true;
        } finally {
            if (!completed) {
                recordAbortedOracleRun();
            }
        }
    }

    private BaseTableInfo selectBaseTable() throws Exception {
        state.updateSchema();
        List<DatabendTable> candidates = state.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> !t.getName().startsWith("vp_")).filter(t -> !t.getColumns().isEmpty())
                .collect(Collectors.toList());
        if (candidates.isEmpty()) {
            throw new IgnoreMeException();
        }
        BaseShape target = BaseShape
                .valueOf(DatabendVpQueryQuality.targetShape((int) Randomly.getNotCachedInteger(0, 100)));
        List<DatabendTable> preferred = new ArrayList<>();
        for (DatabendTable table : candidates) {
            if (effectiveBaseShape(getRowCount(table.getName()), table.getColumns().size()) == target) {
                preferred.add(table);
            }
        }
        recordQualityCount("selection_target_" + target, 1);
        if (preferred.isEmpty()) {
            recordQualityCount("selection_fallback", 1);
        }
        return createBaseTableInfo(Randomly.fromList(preferred.isEmpty() ? candidates : preferred));
    }

    private BaseTableInfo createBaseTableInfo(DatabendTable base) throws Exception {
        String createTable = getCreateTable(base.getName());
        ParsedTableDefinition parsed = parseTableDefinition(createTable);
        Map<String, DatabendColumn> uniqueColumns = new LinkedHashMap<>();
        for (DatabendColumn column : base.getColumns()) {
            uniqueColumns.putIfAbsent(columnKey(column.getName()), column);
        }
        List<VpColumn> sourceColumns = uniqueColumns.values().stream().map(c -> {
            ColumnDefinition definition = parsed.columns.get(c.getName().toLowerCase(Locale.ROOT));
            return definition == null ? new VpColumn(c.getName(), c.getType(), c.isNullable(), null) : new VpColumn(
                    c.getName(), c.getType(), definition.nullable, definition.defaultExpression, definition.sqlType);
        }).collect(Collectors.toList());
        long rowCount = getRowCount(base.getName());
        BaseShape shape = effectiveBaseShape(rowCount, sourceColumns.size());
        incrementSelectedBaseShape(shape);
        String stableOrder = getStableOrder(base, sourceColumns, createTable);
        return new BaseTableInfo(base.getName(), sourceColumns, shape, stableOrder, rowCount,
                extractEngineFromCreateTable(createTable), parsed.constraints);
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

    private List<ReconstructionStrategy> createReconstructionSchedule(VpSchema vp) {
        List<ReconstructionStrategy> shuffled = new ArrayList<>(supportedReconstructionStrategies(vp));
        Collections.shuffle(shuffled);
        return shuffled;
    }

    private List<ReconstructionStrategy> supportedReconstructionStrategies(VpSchema vp) {
        List<ReconstructionStrategy> strategies = new ArrayList<>(List.of(ReconstructionStrategy.JOIN,
                ReconstructionStrategy.LEFT_RIGHT_JOIN, ReconstructionStrategy.FULL_OUTER_SIMULATION,
                ReconstructionStrategy.SCALAR_SUBQUERY, ReconstructionStrategy.EXISTS_FILTER,
                ReconstructionStrategy.IN_FILTER, ReconstructionStrategy.NOT_EXISTS_ANTI,
                ReconstructionStrategy.DERIVED_TABLE, ReconstructionStrategy.CTE, ReconstructionStrategy.VIEW,
                ReconstructionStrategy.AGGREGATE_THEN_MERGE, ReconstructionStrategy.ROW_NUMBER_ALIGNMENT));
        if (advancedFeaturesEnabled()) {
            strategies.addAll(List.of(ReconstructionStrategy.JSON_PACK, ReconstructionStrategy.STRING_PACK,
                    ReconstructionStrategy.NATURAL_OR_USING, ReconstructionStrategy.LATERAL_DERIVED));
            // Reuse one temporary table per VP schema. Complex types are
            // excluded because their temporary-table DDL is not reliable on
            // the tested Databend release.
            if (supportsReusableTemporaryTable(vp)) {
                strategies.add(ReconstructionStrategy.TEMP_TABLE);
            }
        }
        return strategies;
    }

    private boolean supportsReusableTemporaryTable(VpSchema vp) {
        return vp.columns.stream().allMatch(c -> supportsCopyRoundTripType(c.type));
    }

    private void prepareReusableTemporaryTable(VpSchema vp) throws Exception {
        executeSetup(vp, "DROP TABLE IF EXISTS " + quote(vp.tempTable));
        executeSetup(vp, createTemporaryReconstructionTableSQL(vp));
    }

    private void resetSessionForVerticalPartitioningOracle() throws Exception {
        try (Statement statement = state.getConnection().createStatement()) {
            for (String reset : sessionResetStatements()) {
                statement.execute(reset);
            }
        }
    }

    static List<String> sessionResetStatements() {
        return List.of("SET timezone = 'UTC'", "SET enable_query_result_cache = 0",
                "SET force_aggregate_data_spill = 0", "SET force_sort_data_spill = 0", "SET force_join_data_spill = 0",
                "SET force_window_data_spill = 0");
    }

    private VpSchema materializeTable(BaseTableInfo base, SplitStrategy splitStrategy) throws Exception {
        String prefix = "vp_" + sanitizeIdentifier(state.getDatabaseName()) + "_" + ORACLE_RUN_ID.incrementAndGet()
                + "_";
        String source = prefix + "source";
        String left = prefix + "l";
        String right = prefix + "r";
        String view = prefix + "v";
        String temp = prefix + "joined";

        // Databend's information schema can expose a nested-field entry with
        // the same top-level name as its parent ARRAY/MAP/TUPLE column. Keep
        // only one top-level definition before rendering helper DDL; otherwise
        // CREATE TABLE fails with "Duplicated column name".
        List<VpColumn> uniqueBaseColumns = deduplicateVpColumns(base.columns);
        // The schedule was created from the deduplicated metadata in
        // createBaseTableInfo(). Recompute the effective shape defensively,
        // while preserving EMPTY for zero-row tables; otherwise an
        // EMPTY_SPLIT can be applied to a one-column empty table and assert.
        BaseShape effectiveShape = effectiveBaseShape(base.rowCount, uniqueBaseColumns.size());
        SplitPlan splitPlan = createSplitPlan(uniqueBaseColumns, effectiveShape, splitStrategy);
        // Databend 1.2.925 does not implement ALTER TABLE ... FOREIGN KEY.
        // Keep the field for schema parity, but never emit unsupported DDL.
        boolean useForeignKey = false;
        boolean helperTablesUseFuse = !state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine
                || "FUSE".equalsIgnoreCase(base.engine);
        boolean clusteredHelpers = advancedFeaturesEnabled() && helperTablesUseFuse
                && Randomly.getBooleanWithRatherLowProbability();
        boolean insertOverwrite = advancedFeaturesEnabled() && Randomly.getBooleanWithRatherLowProbability();
        if (clusteredHelpers) {
            recordAdvancedFeature(AdvancedFeature.CLUSTER_KEY);
        }
        if (insertOverwrite) {
            recordAdvancedFeature(AdvancedFeature.INSERT_OVERWRITE);
        }

        SchemaProfile schemaProfile = state.getDbmsSpecificOptions().verticalPartitioningSourceCopySchemaObjects
                ? SchemaProfile.fromOption(state.getDbmsSpecificOptions().verticalPartitioningSchemaProfile)
                : SchemaProfile.PROJECTION_ONLY;
        VpSchema vp = new VpSchema(prefix, base.tableName, source, left, right, view, temp, uniqueBaseColumns,
                splitPlan.leftColumns, splitPlan.rightColumns, splitPlan.overlapColumns, effectiveShape, splitStrategy,
                useForeignKey, base.stableOrderStrategy, base.rowCount, base.engine, clusteredHelpers, insertOverwrite,
                schemaProfile, base.constraints);
        dropArtifacts(vp);
        EngineLayout layout = chooseEngineLayout(base.engine);
        copyBaseSchemaObjectsToSource(vp);
        createSourceTable(vp, layout.sourceEngine);
        populateSourceTable(vp, base.stableOrderStrategy);
        maybeRoundTripSourceThroughCopy(vp);
        createPartitionTables(vp, layout);
        configureAsymmetricClustering(vp);
        populatePartitionTables(vp);
        if (advancedFeaturesEnabled() && allFuseHelpers(vp)) {
            verifyPhysicalState(vp, "layout-loaded");
            maintainOneSide(vp, Randomly.getBoolean() ? "COMPACT" : "RECLUSTER");
        }
        validateClusteringInformation(vp);
        maybeCreateHelperIndexes(vp);
        if (useForeignKey) {
            tryExecuteSetup(vp,
                    "ALTER TABLE " + quote(vp.rightTable) + " ADD CONSTRAINT " + quote(vp.prefix + "fk_r_l")
                            + " FOREIGN KEY (" + quote(ROW_ID) + ") REFERENCES " + quote(vp.leftTable) + "("
                            + quote(ROW_ID) + ")");
        }
        return vp;
    }

    private static List<VpColumn> deduplicateVpColumns(List<VpColumn> columns) {
        Map<String, VpColumn> unique = new LinkedHashMap<>();
        for (VpColumn column : columns) {
            unique.putIfAbsent(columnKey(column.name), column);
        }
        return new ArrayList<>(unique.values());
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

    private String getStableOrder(DatabendTable base, List<VpColumn> columns, String createTable) {
        List<String> primary = base.getColumns().stream().filter(DatabendColumn::isPrimaryKey)
                .map(c -> quote(c.getName())).collect(Collectors.toList());
        if (!primary.isEmpty()) {
            return String.join(", ", primary);
        }
        List<String> clusterKey = extractClusterKeyColumns(createTable, columns);
        if (!clusterKey.isEmpty()) {
            return String.join(", ", clusterKey);
        }
        List<String> comparable = columns.stream().filter(c -> isStableOrderType(c.type)).map(c -> quote(c.name))
                .collect(Collectors.toList());
        if (!comparable.isEmpty()) {
            return String.join(", ", comparable);
        }
        return null;
    }

    static List<String> extractClusterKeyColumns(String createTable, List<VpColumn> columns) {
        return extractClusterKeyColumnsByName(createTable,
                columns.stream().map(c -> c.name).collect(Collectors.toList()),
                columns.stream().map(c -> c.type).collect(Collectors.toList()));
    }

    static List<String> extractClusterKeyColumns(String createTable, List<DatabendColumn> columns,
            boolean schemaColumns) {
        return extractClusterKeyColumnsByName(createTable,
                columns.stream().map(DatabendColumn::getName).collect(Collectors.toList()),
                columns.stream().map(c -> c.getType().getPrimitiveDataType()).collect(Collectors.toList()));
    }

    private static List<String> extractClusterKeyColumnsByName(String createTable, List<String> names,
            List<DatabendDataType> types) {
        if (createTable == null) {
            return List.of();
        }
        java.util.regex.Matcher matcher = Pattern.compile("(?i)\\bCLUSTER\\s+BY\\s+(?:linear\\s*)?\\(([^)]*)\\)")
                .matcher(createTable);
        if (!matcher.find()) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (String expression : matcher.group(1).split(",")) {
            String column = expression.trim().replaceAll("^`|`$", "");
            int index = -1;
            for (int i = 0; i < names.size(); i++) {
                if (names.get(i).equalsIgnoreCase(column)) {
                    index = i;
                    break;
                }
            }
            if (index >= 0 && isStableOrderType(types.get(index))) {
                result.add(quote(column));
            }
        }
        return result;
    }

    static boolean isStableOrderType(DatabendDataType type) {
        if (type == null) {
            return false;
        }
        switch (type) {
        case ARRAY:
        case MAP:
        case TUPLE:
        case VARIANT:
        case BITMAP:
        case GEOMETRY:
        case FLOATING:
            return false;
        default:
            return true;
        }
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

    private EngineLayout chooseEngineLayout(String baseEngine) {
        String profile = state.getDbmsSpecificOptions().verticalPartitioningEngineLayoutProfile;
        if (!"FUSE_FOCUSED".equalsIgnoreCase(profile)) {
            return new EngineLayout("FUSE", "FUSE", "FUSE");
        }
        if (Randomly.getNotCachedInteger(0, 100) < 50) {
            return new EngineLayout("FUSE", "FUSE", "FUSE");
        }
        return Randomly.fromList(List.of(new EngineLayout("MEMORY", "MEMORY", "MEMORY"),
                new EngineLayout("FUSE", "FUSE", "MEMORY"), new EngineLayout("FUSE", "MEMORY", "FUSE"),
                new EngineLayout("MEMORY", "FUSE", "FUSE"), new EngineLayout("MEMORY", "MEMORY", "FUSE"),
                new EngineLayout("MEMORY", "FUSE", "MEMORY"), new EngineLayout("FUSE", "MEMORY", "MEMORY")));
    }

    private void createSourceTable(VpSchema vp, String preferredEngine) throws Exception {
        vp.sourceEngine = createTableWithEngineFallback(vp, vp.sourceTable, sourceColumnDefs(vp), preferredEngine);
    }

    private List<String> sourceColumnDefs(VpSchema vp) {
        List<String> columnDefs = new ArrayList<>();
        columnDefs.add(quote(ROW_ID) + " BIGINT NOT NULL");
        for (VpColumn column : vp.columns) {
            columnDefs.add(sourceColumnDef(column, vp.schemaProfile));
        }
        if (vp.schemaProfile == SchemaProfile.CONSTRAINTS) {
            columnDefs.addAll(vp.baseConstraints);
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
            vp.sourceEngine = recreateHelperTableWithFUSE(vp, vp.sourceTable, sourceColumnDefs(vp));
            executeSetup(vp, populateSourceSQL(vp, stableOrder));
        }
    }

    private String populateSourceSQL(VpSchema vp, String stableOrder) {
        String columnList = vp.columns.stream().map(c -> quote(c.name)).collect(Collectors.joining(", "));
        String sql = "INSERT INTO " + quote(vp.sourceTable) + " (" + quote(ROW_ID) + ", " + columnList
                + ") SELECT ROW_NUMBER() OVER (ORDER BY " + (stableOrder == null ? "1" : stableOrder) + "), "
                + columnList + " FROM " + quote(vp.baseTable) + " ORDER BY 1";
        if (state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows > 0) {
            sql += " LIMIT " + state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows;
        }
        return sql;
    }

    private void createPartitionTables(VpSchema vp, EngineLayout layout) throws Exception {
        vp.leftEngine = createPartitionTable(vp, vp.leftTable, vp.leftColumns, layout.leftEngine);
        vp.rightEngine = createPartitionTable(vp, vp.rightTable, vp.rightColumns, layout.rightEngine);
    }

    private String createPartitionTable(VpSchema vp, String tableName, List<VpColumn> columns, String preferredEngine)
            throws Exception {
        return createTableWithEngineFallback(vp, tableName, partitionColumnDefs(columns), preferredEngine);
    }

    private List<String> partitionColumnDefs(List<VpColumn> columns) {
        List<String> defs = new ArrayList<>();
        defs.add(quote(ROW_ID) + " BIGINT NOT NULL");
        for (VpColumn column : columns) {
            defs.add(columnDef(column));
        }
        return defs;
    }

    private String columnDef(VpColumn column) {
        String type = effectiveDeclaredSqlType(column);
        return quote(column.name) + " " + type + " NULL";
    }

    private String sourceColumnDef(VpColumn column, SchemaProfile profile) {
        StringBuilder definition = new StringBuilder(quote(column.name)).append(" ")
                .append(effectiveDeclaredSqlType(column));
        if (profile != SchemaProfile.PROJECTION_ONLY) {
            definition.append(column.nullable ? " NULL" : " NOT NULL");
            if (isSafeDefaultType(column.type) && column.defaultExpression != null
                    && !column.defaultExpression.isEmpty()) {
                definition.append(" DEFAULT ").append(column.defaultExpression);
            }
        } else {
            definition.append(" NULL");
        }
        return definition.toString();
    }

    private String effectiveDeclaredSqlType(VpColumn column) {
        // SHOW CREATE expands tuple/map members into named nullable fields on
        // this Databend release (e.g. TUPLE(1 BIGINT NULL, ...)). Those names
        // are metadata, not valid DDL type syntax; use the generator's
        // canonical type for nested values.
        if (column.declaredSqlType != null && !column.declaredSqlType.isBlank()) {
            // SHOW CREATE preserves nested ARRAY/MAP members and DECIMAL precision/scale. Reusing
            // that declaration avoids silently changing the type on helper tables.
            if (column.type == DatabendDataType.ARRAY || column.type == DatabendDataType.MAP
                    || column.type == DatabendDataType.DECIMAL || column.type == DatabendDataType.NUMERIC) {
                return column.declaredSqlType;
            }
            if (column.type == DatabendDataType.TUPLE) {
                return normalizeTupleType(column.declaredSqlType);
            }
        }
        return sqlTypeForPartitionColumn(column.compositeType);
    }

    private static String normalizeTupleType(String type) {
        String trimmed = type.trim();
        if (!trimmed.regionMatches(true, 0, "TUPLE(", 0, 6) || !trimmed.endsWith(")")) {
            return trimmed;
        }
        String body = trimmed.substring(6, trimmed.length() - 1);
        List<String> members = splitTopLevel(body);
        for (int i = 0; i < members.size(); i++) {
            members.set(i, members.get(i).trim().replaceFirst("^\\d+\\s+", ""));
        }
        return "TUPLE(" + String.join(", ", members) + ")";
    }

    private boolean isSafeDefaultType(DatabendDataType type) {
        return type != DatabendDataType.ARRAY && type != DatabendDataType.MAP && type != DatabendDataType.TUPLE
                && type != DatabendDataType.VARIANT && type != DatabendDataType.BITMAP
                && type != DatabendDataType.GEOMETRY;
    }

    static String sqlTypeForPartitionColumn(DatabendCompositeDataType type) {
        if (type.getPrimitiveDataType() == DatabendDataType.DECIMAL) {
            return type.toString();
        }
        return toSQLType(type.getPrimitiveDataType());
    }

    private String createTableWithEngineFallback(VpSchema vp, String tableName, List<String> columnDefs,
            String preferredEngine) throws Exception {
        try {
            executeSetup(vp, createTableSQL(tableName, columnDefs, preferredEngine, vp.clusteredHelpers));
            return preferredEngine;
        } catch (AssertionError e) {
            if (!canFallbackHelperEngine(preferredEngine)) {
                throw e;
            }
            return recreateHelperTableWithFUSE(vp, tableName, columnDefs);
        }
    }

    static String createTableSQL(String tableName, List<String> columnDefs, String engine, boolean clustered) {
        String sql = "CREATE TABLE " + quote(tableName) + " (" + String.join(", ", columnDefs) + ")";
        if (engine != null && !engine.isEmpty()) {
            sql += " ENGINE=" + engine;
        }
        if (clustered && "FUSE".equalsIgnoreCase(engine)) {
            sql += " CLUSTER BY (" + quote(ROW_ID) + ")";
        }
        return sql;
    }

    private boolean canFallbackHelperEngine(String engine) {
        return engine != null && !"FUSE".equalsIgnoreCase(engine);
    }

    private String recreateHelperTableWithFUSE(VpSchema vp, String tableName, List<String> columnDefs)
            throws Exception {
        executeSetup(vp, "DROP TABLE IF EXISTS " + quote(tableName));
        executeSetup(vp, createTableSQL(tableName, columnDefs, "FUSE", vp.clusteredHelpers));
        return "FUSE";
    }

    private void executeP1Setup(VpSchema vp, String sql) throws Exception {
        vp.setupStatements.add(sql);
        if (!state.executeStatement(new SQLQueryAdapter(sql, errors, false))) {
            throw new IgnoreMeException();
        }
    }

    private List<VpColumn> exclusiveBusinessColumns(List<VpColumn> own, List<VpColumn> other) {
        return own.stream().filter(c -> !containsColumn(other, c.name) && !c.name.startsWith("vp_p1_")
                && DatabendVpQueryQuality.supportsTextRoundTrip(c.type)).collect(Collectors.toList());
    }

    private boolean hasExclusiveBusinessSides(VpSchema vp) {
        return !exclusiveBusinessColumns(vp.leftColumns, vp.rightColumns).isEmpty()
                && !exclusiveBusinessColumns(vp.rightColumns, vp.leftColumns).isEmpty();
    }

    private String layoutKey(List<VpColumn> columns) {
        return columns.stream().filter(c -> isStableOrderType(c.type)).map(c -> quote(c.name)).findFirst()
                .orElse(quote(ROW_ID));
    }

    private String sideLayoutKey(List<VpColumn> own, List<VpColumn> other) {
        List<VpColumn> exclusive = exclusiveBusinessColumns(own, other);
        return layoutKey(exclusive.isEmpty() ? own : exclusive);
    }

    private void configureAsymmetricClustering(VpSchema vp) throws Exception {
        if (!advancedFeaturesEnabled() || !allFuseHelpers(vp)) {
            return;
        }
        executeP1Setup(vp, "ALTER TABLE " + quote(vp.sourceTable) + " CLUSTER BY (" + quote(ROW_ID) + ")");
        executeP1Setup(vp, "ALTER TABLE " + quote(vp.leftTable) + " CLUSTER BY ("
                + sideLayoutKey(vp.leftColumns, vp.rightColumns) + ")");
        String rightKey = sideLayoutKey(vp.rightColumns, vp.leftColumns);
        executeP1Setup(vp, "ALTER TABLE " + quote(vp.rightTable) + " CLUSTER BY (" + rightKey
                + (rightKey.equals(quote(ROW_ID)) ? "" : ", " + quote(ROW_ID)) + ")");
        recordAdvancedFeature(AdvancedFeature.CLUSTER_KEY);
    }

    private void populateAsymmetricPartitions(VpSchema vp) throws Exception {
        long rows = getRowCount(vp.sourceTable);
        long batch = Math.max(1, (rows + 7) / 8);
        // Rowids are frozen by source materialization; all batches read the same logical snapshot.
        for (long upper = rows; upper > 0; upper -= batch) {
            executeP1Setup(vp,
                    "INSERT INTO " + quote(vp.leftTable) + " (" + partitionColumnList(vp.leftColumns) + ") SELECT "
                            + partitionColumnList(vp.leftColumns) + " FROM " + quote(vp.sourceTable) + " WHERE "
                            + quote(ROW_ID) + " > " + Math.max(0, upper - batch) + " AND " + quote(ROW_ID) + " <= "
                            + upper + " ORDER BY " + quote(ROW_ID) + " DESC");
        }
        for (int part = 0; part < 2; part++) {
            executeP1Setup(vp,
                    "INSERT INTO " + quote(vp.rightTable) + " (" + partitionColumnList(vp.rightColumns) + ") SELECT "
                            + partitionColumnList(vp.rightColumns) + " FROM " + quote(vp.sourceTable) + " WHERE MOD("
                            + quote(ROW_ID) + ", 2) = " + part + " ORDER BY "
                            + sideLayoutKey(vp.rightColumns, vp.leftColumns) + " NULLS FIRST, " + quote(ROW_ID));
        }
        qualityEvidence(vp,
                "physical-layout source=one-ordered-batch left=reverse-small-batches right=business-sorted-two-batches"
                        + " rows=" + rows + " left-batch=" + batch + " right-order="
                        + sideLayoutKey(vp.rightColumns, vp.leftColumns));
    }

    private void verifyPhysicalState(VpSchema vp, String phase) throws Exception {
        for (String side : List.of("left", "right")) {
            List<VpColumn> columns = side.equals("left") ? vp.leftColumns : vp.rightColumns;
            String table = side.equals("left") ? vp.leftTable : vp.rightTable;
            String projection = partitionColumnList(columns);
            VpQueryPair check = new VpQueryPair(vp, ReconstructionStrategy.JOIN,
                    "SELECT " + projection + " FROM " + quote(vp.sourceTable) + " ORDER BY " + quote(ROW_ID),
                    "SELECT " + projection + " FROM " + quote(table) + " ORDER BY " + quote(ROW_ID), List.of(),
                    "physical-phase=" + phase + ", side=" + side + ", preserve_on_failure=true", true, false);
            QueryResultMultiset source = diagnosticRows(check.singleQuery, List.of());
            QueryResultMultiset partition = diagnosticRows(check.splitQuery, List.of());
            if (!source.rows.equals(partition.rows) || !source.orderedRows.equals(partition.orderedRows)) {
                throw new AssertionError(formatBugReport(check, source, partition));
            }
        }
        recordQualityCount("physical_check_" + phase, 1);
        qualityEvidence(vp, "physical-checkpoint=" + phase + " both_sides_exact=true rebuild_partitions=false");
    }

    private void maintainOneSide(VpSchema vp, String operation) throws Exception {
        if (!allFuseHelpers(vp) || !(operation.equals("COMPACT") || operation.equals("RECLUSTER"))) {
            throw new IgnoreMeException();
        }
        // Keep this guard set until both checkpoints succeed, including on expected SQL errors.
        vp.mutationIncomplete = true;
        verifyPhysicalState(vp, "before-" + operation);
        String stableQuery = "SELECT " + sourceColumnList(vp) + " FROM " + quote(vp.sourceTable) + " ORDER BY "
                + quote(ROW_ID);
        QueryResultMultiset before = diagnosticRows(stableQuery, List.of());
        String sql = operation.equals("COMPACT") ? "OPTIMIZE TABLE " + quote(vp.leftTable) + " COMPACT"
                : "ALTER TABLE " + quote(vp.leftTable) + " RECLUSTER FINAL";
        executeP1Setup(vp, sql);
        verifyPhysicalState(vp, "after-" + operation);
        QueryResultMultiset after = diagnosticRows(stableQuery, List.of());
        if (!before.rows.equals(after.rows) || !before.orderedRows.equals(after.orderedRows)) {
            VpQueryPair stability = new VpQueryPair(vp, ReconstructionStrategy.JOIN, stableQuery, stableQuery,
                    List.of(), "physical-source-stability=" + operation + ", preserve_on_failure=true", true, false);
            throw new AssertionError(formatBugReport(stability, before, after));
        }
        vp.mutationIncomplete = false;
        qualityEvidence(vp,
                "physical-maintenance side=left operation=" + operation + " source/right=untouched source_stable=true");
    }

    private void addFixtureColumn(VpSchema vp, VpColumn column, boolean onLeft, String expression) throws Exception {
        List<VpColumn> own = onLeft ? vp.leftColumns : vp.rightColumns;
        String partition = onLeft ? vp.leftTable : vp.rightTable;
        for (String table : List.of(vp.sourceTable, partition)) {
            executeP1Setup(vp, "ALTER TABLE " + quote(table) + " ADD COLUMN " + columnDef(column));
        }
        vp.columns.add(column);
        own.add(column);
        for (String table : List.of(vp.sourceTable, partition)) {
            executeP1Setup(vp, "UPDATE " + quote(table) + " SET " + quote(column.name) + " = " + expression);
        }
    }

    private void ensureWideFixture(VpSchema vp) throws Exception {
        if (vp.wideInitialized) {
            return;
        }
        if (!allFuseHelpers(vp) || !hasExclusiveBusinessSides(vp)) {
            throw new IgnoreMeException();
        }
        vp.mutationIncomplete = true;
        int missing = Math.max(0, 16 - vp.columns.size());
        for (int i = 0; i < missing; i++) {
            VpColumn column = new VpColumn("vp_p1_wide_" + i, new DatabendCompositeDataType(DatabendDataType.VARCHAR),
                    true, null);
            addFixtureColumn(vp, column, i % 2 == 0, "CONCAT(REPEAT('wide-', 32), TO_VARCHAR(" + quote(ROW_ID) + "))");
        }
        if (supportsReusableTemporaryTable(vp)) {
            prepareReusableTemporaryTable(vp);
        }
        verifyPhysicalState(vp, "wide-ready");
        vp.wideInitialized = true;
        vp.mutationIncomplete = false;
    }

    private void ensureSemistructuredFixture(VpSchema vp) throws Exception {
        if (vp.semiDocumentColumn != null) {
            return;
        }
        if (!allFuseHelpers(vp) || !hasExclusiveBusinessSides(vp) || getRowCount(vp.sourceTable) < 6) {
            throw new IgnoreMeException();
        }
        Long cases = readMetadataLong(
                "SELECT COUNT(DISTINCT MOD(" + quote(ROW_ID) + ", 6)) FROM " + quote(vp.sourceTable));
        if (cases == null || cases != 6) {
            recordQualityCount("semi_missing_boundary_witness", 1);
            throw new IgnoreMeException();
        }
        VpColumn left = exclusiveBusinessColumns(vp.leftColumns, vp.rightColumns).get(0);
        VpColumn right = exclusiveBusinessColumns(vp.rightColumns, vp.leftColumns).get(0);
        vp.mutationIncomplete = true;
        VpColumn document = new VpColumn("vp_p1_document", new DatabendCompositeDataType(DatabendDataType.VARIANT),
                true, null);
        addFixtureColumn(vp, document, true, DatabendVpP1.semiDocument(quote(left.name)));
        if (supportsReusableTemporaryTable(vp)) {
            prepareReusableTemporaryTable(vp);
        }
        verifyPhysicalState(vp, "semi-ready");
        vp.semiDocumentColumn = document.name;
        vp.semiOppositeColumn = right.name;
        vp.mutationIncomplete = false;
    }

    private void recordPruningPlans(VpQueryPair pair) {
        for (String side : List.of("source", "split")) {
            String plan = explainPlanText(side.equals("source") ? pair.singleQuery : pair.splitQuery);
            String evidence = DatabendVpP1.planEvidence(plan);
            qualityEvidence(pair.schema, "950-pruning-plan side=" + side + " " + evidence);
            if (plan != null && state.getLogger() != null && state.getOptions() != null
                    && state.getOptions().logEachSelect()) {
                state.getLogger().writeCurrent(
                        "-- [vp-plan] feature=projection-pruning side=" + side + " " + plan + System.lineSeparator());
            }
            recordFeatureMetric("projection-pruning", side + (plan == null ? "_plan_unknown" : "_plan_observed"));
            if (evidence.contains("filter_pushdown=true")) {
                recordFeatureMetric("projection-pruning", side + "_filter_pushdown");
            }
            if (evidence.contains("pruning_stats=true")) {
                recordFeatureMetric("projection-pruning", side + "_pruning_stats_observed");
            }
            if (evidence.contains("partitions_reduced=true")) {
                recordFeatureMetric("projection-pruning", side + "_partitions_reduced");
            }
            if (evidence.contains("late_materialization=true")) {
                recordFeatureMetric("projection-pruning", side + "_late_materialization");
            }
        }
    }

    private void populatePartitionTables(VpSchema vp) throws Exception {
        if (advancedFeaturesEnabled() && allFuseHelpers(vp) && vp.baseRowCount > 0) {
            populateAsymmetricPartitions(vp);
            return;
        }
        vp.leftEngine = populatePartitionTable(vp, vp.leftTable, vp.leftColumns, vp.leftEngine);
        vp.rightEngine = populatePartitionTable(vp, vp.rightTable, vp.rightColumns, vp.rightEngine);
    }

    private String populatePartitionTable(VpSchema vp, String tableName, List<VpColumn> columns, String engine)
            throws Exception {
        try {
            executeSetup(vp, populatePartitionSQL(tableName, columns, vp.sourceTable, vp.insertOverwrite));
            return engine;
        } catch (AssertionError e) {
            if (!canFallbackHelperEngine(engine)) {
                throw e;
            }
            String fallbackEngine = recreateHelperTableWithFUSE(vp, tableName, partitionColumnDefs(columns));
            executeSetup(vp, populatePartitionSQL(tableName, columns, vp.sourceTable, vp.insertOverwrite));
            return fallbackEngine;
        }
    }

    private String populatePartitionSQL(String partitionTable, List<VpColumn> columns, String sourceTable,
            boolean overwrite) {
        List<String> names = new ArrayList<>();
        names.add(quote(ROW_ID));
        names.addAll(columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        if (overwrite) {
            return "INSERT OVERWRITE " + quote(partitionTable) + " (" + String.join(", ", names) + ") SELECT "
                    + String.join(", ", names) + " FROM " + quote(sourceTable);
        }
        return "INSERT INTO " + quote(partitionTable) + " (" + String.join(", ", names) + ") SELECT "
                + String.join(", ", names) + " FROM " + quote(sourceTable);
    }

    private void maybeRoundTripSourceThroughCopy(VpSchema vp) throws Exception {
        if (!advancedFeaturesEnabled() || vp.baseRowCount == 0 || !supportsCopyRoundTrip(vp)
                || !"FUSE".equalsIgnoreCase(vp.sourceEngine) || !Randomly.getBoolean()) {
            return;
        }
        for (String boundarySetup : copyBoundarySetup(vp)) {
            try {
                executeSetup(vp, boundarySetup);
            } catch (AssertionError e) {
                if (handleQueryError(e, "copy-boundary-setup", boundarySetup, null)) {
                    return;
                }
                throw e;
            }
        }
        String stagePath = "@~/" + vp.prefix + "copy";
        String referenceTable = vp.prefix + "copy_reference";
        try {
            // Failed prior runs can leave Parquet files with an incompatible schema under this prefix.
            executeSetup(vp, "REMOVE " + stagePath);
            // Preserve an independent pre-COPY reference. Otherwise source and
            // helpers could inherit the same corrupted reload.
            executeSetup(vp, "DROP TABLE IF EXISTS " + quote(referenceTable));
            executeSetup(vp,
                    "CREATE TEMPORARY TABLE " + quote(referenceTable) + " AS SELECT * FROM " + quote(vp.sourceTable));
            executeSetup(vp, "COPY INTO " + stagePath + " FROM (SELECT * FROM " + quote(vp.sourceTable)
                    + ") FILE_FORMAT=(TYPE=PARQUET)");
            executeSetup(vp, "TRUNCATE TABLE " + quote(vp.sourceTable));
            executeSetup(vp, "COPY INTO " + quote(vp.sourceTable) + " FROM " + stagePath
                    + " FILE_FORMAT=(TYPE=PARQUET) FORCE=TRUE");
            if (!copyRoundTripRowsEqual(vp.sourceTable, referenceTable)) {
                throw new IgnoreMeException();
            }
            executeSetup(vp, "REMOVE " + stagePath);
            executeSetup(vp, "DROP TABLE IF EXISTS " + quote(referenceTable));
        } catch (AssertionError e) {
            // COPY failures after TRUNCATE must abort this sample instead of
            // materializing helpers from an empty or partial source.
            throw new IgnoreMeException();
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        recordAdvancedFeature(AdvancedFeature.COPY_INTO);
    }

    private boolean copyRoundTripRowsEqual(String source, String reference) throws SQLException {
        // Databend supports EXCEPT DISTINCT, but not EXCEPT ALL. Compare exact client
        // multisets, including rowid, so duplicate loss and NULL changes remain detectable.
        return diagnosticRows("SELECT * FROM " + quote(source), List.of()).rows
                .equals(diagnosticRows("SELECT * FROM " + quote(reference), List.of()).rows);
    }

    private List<String> copyBoundarySetup(VpSchema vp) {
        List<String> setup = new ArrayList<>();
        String row = quote(ROW_ID) + " = (SELECT MIN(" + quote(ROW_ID) + ") FROM " + quote(vp.sourceTable) + ")";
        for (VpColumn column : vp.columns) {
            String target = quote(column.name);
            String expression = copyBoundaryExpression(column);
            if (expression != null) {
                setup.add("UPDATE " + quote(vp.sourceTable) + " SET " + target + " = " + expression + " WHERE " + row);
            }
        }
        VpColumn nullableColumn = vp.columns.stream().filter(c -> c.nullable).findFirst().orElse(null);
        if (nullableColumn != null) {
            String target = quote(nullableColumn.name);
            String anotherRow = quote(ROW_ID) + " = (SELECT MAX(" + quote(ROW_ID) + ") FROM " + quote(vp.sourceTable)
                    + " WHERE " + quote(ROW_ID) + " <> (SELECT MIN(" + quote(ROW_ID) + ") FROM " + quote(vp.sourceTable)
                    + "))";
            setup.add("UPDATE " + quote(vp.sourceTable) + " SET " + target + " = NULL WHERE " + anotherRow);
        }
        return setup;
    }

    private String copyBoundaryExpression(VpColumn column) {
        switch (column.type) {
        case DECIMAL:
        case NUMERIC:
            return decimalBoundaryExpression(column.declaredSqlType);
        case ARRAY:
            return "[]";
        case MAP:
            return "{}";
        case VARIANT:
            return "PARSE_JSON('{\"sqlancer\":[null,[]],\"n\":1}')";
        case TIMESTAMP:
        case DATETIME:
            return "TIMESTAMP '2026-10-03 03:17:18.123456'";
        case FLOAT:
        case FLOATING:
            return Randomly.fromOptions("CAST('-0.0' AS DOUBLE)", "CAST('NaN' AS DOUBLE)", "CAST('Infinity' AS DOUBLE)",
                    "CAST('-Infinity' AS DOUBLE)");
        default:
            return null;
        }
    }

    static String decimalBoundaryExpression(String declaredSqlType) {
        if (declaredSqlType == null) {
            return "CAST(0 AS DECIMAL(38, 10))";
        }
        java.util.regex.Matcher matcher = Pattern.compile("(?i)DECIMAL\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)")
                .matcher(declaredSqlType);
        if (!matcher.find()) {
            return "CAST(0 AS " + declaredSqlType + ")";
        }
        int precision = Integer.parseInt(matcher.group(1));
        int scale = Integer.parseInt(matcher.group(2));
        int integerDigits = precision - scale;
        // DECIMAL(p,p) has no integral digit. Generating `9.99` for it is
        // out of range (the largest legal value is 0.99) and Databend reports
        // a vague BadArguments error. Keep the boundary value valid while
        // still exercising every declared fractional digit.
        String integerPart = integerDigits == 0 ? "0" : "9".repeat(integerDigits);
        String value = scale == 0 ? integerPart : integerPart + "." + "9".repeat(scale);
        return "CAST('" + value + "' AS " + declaredSqlType + ")";
    }

    private boolean supportsCopyRoundTrip(VpSchema vp) {
        return vp.columns.stream().allMatch(c -> supportsCopyRoundTripType(c.type));
    }

    static boolean supportsCopyRoundTripType(DatabendDataType type) {
        switch (type) {
        case INT:
        case FLOAT:
        case FLOATING:
        case VARCHAR:
        case TEXT:
        case CHAR:
        case BOOLEAN:
        case DATE:
        case TIMESTAMP:
        case DECIMAL:
        case NUMERIC:
        case ARRAY:
        case MAP:
        case VARIANT:
        case BITMAP:
        case GEOMETRY:
            return true;
        default:
            // Other types have not completed a verified PARQUET round trip on the tested release.
            return false;
        }
    }

    private void validateClusteringInformation(VpSchema vp) throws SQLException {
        // CLUSTERING_INFORMATION is a FUSE-only table function. A mixed
        // FUSE/MEMORY helper layout must not query it for MEMORY tables.
        if (!supportsClusteringInformation(vp.clusteredHelpers, allFuseHelpers(vp))) {
            return;
        }
        for (String table : List.of(vp.sourceTable, vp.leftTable, vp.rightTable)) {
            getSingleString("SELECT cluster_key FROM CLUSTERING_INFORMATION('" + state.getDatabaseName() + "', '"
                    + table + "')");
        }
        recordAdvancedFeature(AdvancedFeature.CLUSTERING_INFORMATION);
    }

    private void maybeCreateHelperIndexes(VpSchema vp) throws Exception {
        if (!advancedFeaturesEnabled()) {
            return;
        }
        maybeCreateHelperIndex(vp, vp.sourceTable, vp.columns, "src");
        maybeCreateHelperIndex(vp, vp.leftTable, vp.leftColumns, "l");
        maybeCreateHelperIndex(vp, vp.rightTable, vp.rightColumns, "r");
    }

    private void maybeCreateHelperIndex(VpSchema vp, String tableName, List<VpColumn> columns, String suffix)
            throws Exception {
        vp.setupStatements.add("-- skipped helper index: Databend 1.2.925 has no CREATE INDEX support");
    }

    private void copyBaseSchemaObjectsToSource(VpSchema vp) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSourceCopySchemaObjects) {
            vp.setupStatements.add("-- source schema profile disabled: PROJECTION_ONLY");
            return;
        }
        vp.setupStatements.add("-- source schema profile: " + vp.schemaProfile);
        if (vp.schemaProfile == SchemaProfile.PROJECTION_ONLY) {
            vp.setupStatements.add("-- copied schema objects: none; helper columns are nullable projections");
        } else if (vp.schemaProfile == SchemaProfile.NULL_DEFAULT) {
            vp.setupStatements.add("-- copied source column nullability and defaults from SHOW CREATE TABLE");
            vp.setupStatements
                    .add("-- helper columns intentionally remain nullable projections for outer-join reconstruction");
        } else {
            vp.setupStatements
                    .add("-- copied source column nullability/defaults and supported PRIMARY/UNIQUE/CHECK constraints");
            vp.setupStatements
                    .add("-- foreign keys, indexes, generated expressions, and helper constraints are not copied");
        }
    }

    private VpQueryPair generateRandomRewritePair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        if (strategy == ReconstructionStrategy.TEMP_TABLE && !supportsReusableTemporaryTable(vp)) {
            throw new IgnoreMeException();
        }
        SourceAst sourceAst = createSourceAst(vp);
        DatabendExpressionGenerator gen = new DatabendExpressionGenerator(state).setColumns(sourceAst.columns);
        // Databend-specific paths are the high-value part of this oracle. Keep them
        // frequent enough to get measurable coverage instead of the generic 1% tail.
        if (advancedFeaturesEnabled() && shouldGenerateDatabendFeaturePair()) {
            return generateDatabendFeaturePair(vp, strategy);
        }
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()) {
            return generateCorrelatedSubqueryPair(vp, strategy, sourceAst);
        }
        if (Randomly.getNotCachedInteger(0, 100) < 70) {
            VpQueryPair witnessed = generateWitnessedBusinessPair(vp, strategy);
            if (witnessed != null) {
                return witnessed;
            }
        }
        DatabendSelect select = new DatabendSelect();
        int fetchColumns = Math.max(1, Randomly.smallNumber() + 1);
        boolean useAggregate = Randomly.getBoolean();
        List<DatabendColumn> crossPartitionColumns = chooseCrossPartitionColumns(sourceAst.columns,
                columnNamesList(vp.leftColumns), columnNamesList(vp.rightColumns));
        boolean requireCrossPartition = !crossPartitionColumns.isEmpty() && Randomly.getBoolean();
        boolean hasWindowExpression = false;
        List<DatabendExpression> selected = new ArrayList<>();
        List<DatabendExpression> nonAggregateExpressions = new ArrayList<>();
        if (useAggregate) {
            selected.add(generateVpAggregate(gen, sourceAst.columns));
            List<DatabendColumn> groupableColumns = sourceAst.columns.stream()
                    .filter(c -> isStableOrderType(c.getType().getPrimitiveDataType())).collect(Collectors.toList());
            for (int i = 1; i < fetchColumns; i++) {
                DatabendExpression expr = new DatabendColumnReference(Randomly.fromList(groupableColumns));
                selected.add(expr);
                nonAggregateExpressions.add(expr);
            }
        } else {
            for (int i = 0; i < fetchColumns; i++) {
                DatabendExpression expr;
                if (advancedFeaturesEnabled() && Randomly.getBooleanWithRatherLowProbability()) {
                    expr = generateAdvancedSelectExpression(sourceAst, gen);
                    hasWindowExpression = true;
                } else {
                    expr = gen.generateExpression();
                    expr = semanticResultExpression(expr);
                    nonAggregateExpressions.add(expr);
                }
                selected.add(expr);
            }
        }
        if (requireCrossPartition && !useAggregate) {
            for (DatabendColumn column : crossPartitionColumns) {
                DatabendExpression expression = semanticResultExpression(new DatabendColumnReference(column));
                selected.add(expression);
                nonAggregateExpressions.add(expression);
            }
        }
        if (selected.isEmpty()) {
            selected.add(semanticResultExpression(new DatabendColumnReference(Randomly.fromList(sourceAst.columns))));
        }
        select.setFetchColumns(selected);
        select.setFromList(List.of(new DatabendTableReference(sourceAst.table)));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateBooleanExpression());
        }
        if (requireCrossPartition) {
            DatabendExpression dependency = crossPartitionPredicate(crossPartitionColumns.get(0),
                    crossPartitionColumns.get(1));
            if (select.getWhereClause() != null) {
                dependency = new sqlancer.databend.ast.DatabendBinaryLogicalOperation(dependency,
                        select.getWhereClause(),
                        sqlancer.databend.ast.DatabendBinaryLogicalOperation.DatabendBinaryLogicalOperator.AND);
            }
            select.setWhereClause(dependency);
        }
        if (useAggregate) {
            List<DatabendExpression> groupByExpressions = chooseGroupByExpressions(nonAggregateExpressions);
            select.setGroupByExpressions(maybeAppendRollup(groupByExpressions, hasWindowExpression,
                    isRollupSafe(nonAggregateExpressions, groupByExpressions)));
        } else if (!hasWindowExpression && supportsCompleteGrouping(nonAggregateExpressions)
                && Randomly.getBooleanWithRatherLowProbability()) {
            List<DatabendExpression> groupByExpressions = chooseGroupByExpressions(nonAggregateExpressions);
            select.setGroupByExpressions(maybeAppendRollup(groupByExpressions, hasWindowExpression,
                    isRollupSafe(nonAggregateExpressions, groupByExpressions)));
            if (Randomly.getBoolean() && !groupByExpressions.isEmpty()) {
                select.setHavingClause(generateHavingExpression(sourceAst.columns));
            }
        }
        if (Randomly.getBoolean()) {
            List<DatabendExpression> orderBys;
            if (useAggregate || !select.getGroupByExpressions().isEmpty()) {
                // With ONLY_FULL_GROUP_BY semantics, ORDER BY may only refer
                // to grouped expressions (or aggregates). The row id is not
                // valid for a scalar aggregate and is not a tie breaker for
                // a grouped query.
                orderBys = new ArrayList<>(select.getGroupByExpressions());
                // WITH ROLLUP is a GROUP BY modifier, not an ORDER BY term.
                // Reusing the decorated expression produces "... ORDER BY
                // expr WITH ROLLUP", which Databend parses as invalid SQL.
                orderBys.removeIf(e -> DatabendVisitor.asString(e).toUpperCase(Locale.ROOT).contains(" WITH ROLLUP"));
                orderBys = stableGroupOrder(orderBys);
            } else {
                orderBys = nonAggregateExpressions.isEmpty()
                        ? List.of(new DatabendColumnReference(sourceAst.rowIdColumn))
                        : generateVpOrderBys(sourceAst.columns);
            }
            select.setOrderByClauses(orderBys);
        }
        boolean orderedComparisonEnabled = isRowPreservingQuery(useAggregate, select)
                && hasVpRowIdTieBreaker(select, sourceAst.rowIdColumn);
        if (Randomly.getBoolean()) {
            boolean rowPreservingQuery = isRowPreservingQuery(useAggregate, select);
            if (select.getOrderByClauses().isEmpty() && rowPreservingQuery) {
                select.setOrderByClauses(List.of(new DatabendColumnReference(sourceAst.rowIdColumn)));
                orderedComparisonEnabled = rowPreservingQuery;
            } else if (select.getOrderByClauses().isEmpty() && !select.getGroupByExpressions().isEmpty()) {
                // LIMIT/OFFSET without an ORDER BY can select different subsets
                // from equivalent source and reconstructed relations. The full
                // grouping key identifies one aggregate result row, so it is a
                // legal and deterministic ordering for grouped queries.
                select.setOrderByClauses(stableGroupOrder(select.getGroupByExpressions()));
            } else if (rowPreservingQuery) {
                List<DatabendExpression> orderBys = new ArrayList<>(select.getOrderByClauses());
                orderBys.add(new DatabendColumnReference(sourceAst.rowIdColumn));
                select.setOrderByClauses(orderBys);
                orderedComparisonEnabled = true;
            }
            select.setLimitClause(DatabendConstant.createIntConstant(Randomly.getNotCachedInteger(0, 25)));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                select.setOffsetClause(DatabendConstant.createIntConstant(Randomly.getNotCachedInteger(0, 5)));
            }
        }
        if (!hasWindowExpression && !select.getGroupByExpressions().isEmpty() && select.getGroupByExpressions().stream()
                .noneMatch(e -> DatabendVisitor.asString(e).toUpperCase(Locale.ROOT).contains("ROLLUP"))) {
            select.setOrderByClauses(stableGroupOrder(select.getGroupByExpressions()));
            orderedComparisonEnabled = true;
        }
        DatabendSelect unionAllBranch = null;
        if (advancedFeaturesEnabled() && !useAggregate && select.getOrderByClauses().isEmpty()
                && select.getLimitClause() == null && select.getOffsetClause() == null
                && Randomly.getBooleanWithSmallProbability()) {
            unionAllBranch = createUnionAllBranch(sourceAst, gen, selected);
            orderedComparisonEnabled = false;
        }
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        String singleQuery = renderQueryWithFrom(select, unionAllBranch, new DatabendTableReference(sourceAst.table));
        String splitQuery = renderQueryWithFrom(select, unionAllBranch, new DatabendText(relation));
        return new VpQueryPair(vp, strategy, singleQuery, splitQuery, extraSetup,
                "shape=" + vp.baseShape + ", split=" + vp.splitStrategy + ", reconstruction=" + strategy,
                orderedComparisonEnabled, useAggregate,
                selected.stream().map(DatabendExpression::getExpectedType).collect(Collectors.toList()));
    }

    static List<DatabendExpression> stableGroupOrder(List<DatabendExpression> groups) {
        return groups.stream().map(expression -> {
            if (!isFloatingType(expression.getExpectedType())) {
                return expression;
            }
            // GROUP BY treats +/-0 as equal and can retain either representative.
            // Databend sorts their signs separately. Normalize only the sort key so
            // execution-dependent representatives cannot change LIMIT or sequence comparison.
            String sql = DatabendVisitor.asString(expression);
            return (DatabendExpression) new DatabendText("CASE WHEN (" + sql
                    + ") = 0 THEN CAST(0 AS DOUBLE) ELSE (" + sql + ") END", expression.getExpectedType());
        }).collect(Collectors.toList());
    }

    private static void recordQualityCount(String metric, long amount) {
        synchronized (STATS_LOCK) {
            QUERY_QUALITY_COUNTS.merge(metric, amount, Long::sum);
        }
    }

    private void qualityEvidence(VpSchema vp, String evidence) {
        String line = "-- [vp-quality] " + evidence;
        vp.setupStatements.add(line);
        synchronized (STATS_LOCK) {
            if (QUERY_QUALITY_SAMPLES.size() < 20) {
                QUERY_QUALITY_SAMPLES.add(evidence);
            }
        }
        if (state.getLogger() != null && state.getOptions() != null && state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(line);
        }
    }

    private VpQueryPair generateWitnessedBusinessPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        List<VpColumn> left = vp.leftColumns.stream().filter(
                c -> !containsColumn(vp.rightColumns, c.name) && DatabendVpQueryQuality.supportsTextRoundTrip(c.type))
                .collect(Collectors.toList());
        List<VpColumn> right = vp.rightColumns.stream().filter(
                c -> !containsColumn(vp.leftColumns, c.name) && DatabendVpQueryQuality.supportsTextRoundTrip(c.type))
                .collect(Collectors.toList());
        if (left.isEmpty() || right.isEmpty()) {
            recordQualityCount("cross_business_ineligible", 1);
            return null;
        }
        for (int retry = 0; retry < 6; retry++) {
            VpQueryPair pair = witnessedBusinessColumns(vp, strategy, Randomly.fromList(left),
                    Randomly.fromList(right));
            if (pair != null) {
                return pair;
            }
        }
        return null;
    }

    private VpQueryPair witnessedBusinessColumns(VpSchema vp, ReconstructionStrategy strategy, VpColumn l, VpColumn r)
            throws Exception {
        List<String[]> witnesses = new ArrayList<>();
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement
                        .executeQuery("SELECT TO_VARCHAR(" + quote(l.name) + "), TO_VARCHAR(" + quote(r.name)
                                + ") FROM " + quote(vp.sourceTable) + " ORDER BY " + quote(ROW_ID) + " LIMIT 32")) {
            while (result.next()) {
                witnesses.add(new String[] { result.getString(1), result.getString(2) });
            }
        }
        for (int attempt = 0; attempt < Math.min(4, witnesses.size()); attempt++) {
            String[] witness = witnesses.get(attempt * witnesses.size() / Math.min(4, witnesses.size()));
            String lp = DatabendVpQueryQuality.witnessedTerm(l.name, witness[0]);
            String rp = DatabendVpQueryQuality.witnessedTerm(r.name, witness[1]);
            String predicate = "(" + lp + " AND " + rp + ")";
            long total, leftHits, rightHits, matched;
            String counts = "SELECT COUNT(*), SUM(CASE WHEN " + lp + " THEN 1 ELSE 0 END), SUM(CASE WHEN " + rp
                    + " THEN 1 ELSE 0 END), SUM(CASE WHEN " + predicate + " THEN 1 ELSE 0 END) FROM "
                    + quote(vp.sourceTable);
            try (Statement statement = state.getConnection().createStatement();
                    ResultSet result = statement.executeQuery(counts)) {
                result.next();
                total = result.getLong(1);
                leftHits = result.getLong(2);
                rightHits = result.getLong(3);
                matched = result.getLong(4);
            }
            if (matched == 0 || matched == total || leftHits == total || rightHits == total) {
                continue;
            }
            // Equality witnesses fix the left business value, hence the business-derived group key.
            // Require multiple matching rows so aggregate coverage actually exercises repeated keys.
            boolean aggregate = matched > 1 && Randomly.getBoolean();
            if (aggregate) {
                recordQualityCount("aggregate_repeated_group_pairs", 1);
            }
            List<String> setup = new ArrayList<>();
            String relation = buildReconstructionRelation(vp, strategy, setup);
            String feature = aggregate ? "cross-business-aggregate" : "cross-business-projection";
            qualityEvidence(vp,
                    "source=" + vp.sourceTable + " left-only=" + l.name + " right-only=" + r.name + " matched="
                            + matched + "/" + total + " left_hits=" + leftHits + " right_hits=" + rightHits
                            + " template=" + feature + " ordered=true predicate=" + predicate);
            recordQualityCount("witnessed_pairs", 1);
            recordQualityCount("predicate_matched_rows", matched);
            recordQualityCount("predicate_total_rows", total);
            return featurePair(vp, strategy,
                    DatabendVpQueryQuality.businessQuery(quote(vp.sourceTable), l.name, r.name, predicate, aggregate),
                    DatabendVpQueryQuality.businessQuery(relation, l.name, r.name, predicate, aggregate), setup,
                    feature);
        }
        recordQualityCount("cross_business_no_selective_witness", 1);
        return null;
    }

    static boolean shouldGenerateDatabendFeaturePair() {
        // One quarter of advanced VP pairs exercise Databend-native operators;
        // the remainder retain broad relational coverage.
        return Randomly.getNotCachedInteger(0, 4) == 0;
    }

    private static List<String> columnNamesList(List<VpColumn> columns) {
        return columns.stream().map(c -> c.name).collect(Collectors.toList());
    }

    static List<DatabendColumn> chooseCrossPartitionColumns(List<DatabendColumn> columns, List<String> left,
            List<String> right) {
        List<DatabendColumn> leftOnly = columns.stream()
                .filter(c -> !columnKey(c.getName()).equals(ROW_ID)
                        && left.stream().anyMatch(n -> columnKey(n).equals(columnKey(c.getName())))
                        && right.stream().noneMatch(n -> columnKey(n).equals(columnKey(c.getName()))))
                .collect(Collectors.toList());
        List<DatabendColumn> rightOnly = columns.stream()
                .filter(c -> !columnKey(c.getName()).equals(ROW_ID)
                        && right.stream().anyMatch(n -> columnKey(n).equals(columnKey(c.getName())))
                        && left.stream().noneMatch(n -> columnKey(n).equals(columnKey(c.getName()))))
                .collect(Collectors.toList());
        return leftOnly.isEmpty() || rightOnly.isEmpty() ? List.of()
                : List.of(Randomly.fromList(leftOnly), Randomly.fromList(rightOnly));
    }

    static DatabendExpression crossPartitionPredicate(DatabendColumn left, DatabendColumn right) {
        DatabendDataType type = left.getType().getPrimitiveDataType();
        String leftSql = DatabendVisitor.asString(new DatabendColumnReference(left));
        String rightSql = DatabendVisitor.asString(new DatabendColumnReference(right));
        if (type == right.getType().getPrimitiveDataType() && isStableOrderType(type)) {
            return new DatabendText("(" + leftSql + (Randomly.getBoolean() ? " <= " : " >= ") + rightSql + ")",
                    DatabendDataType.BOOLEAN);
        }
        // NULL tests work for complex and mutually incompatible column types too.
        return new DatabendText("((" + leftSql + " IS NULL) <> (" + rightSql + " IS NULL))", DatabendDataType.BOOLEAN);
    }

    private static DatabendExpression semanticResultExpression(DatabendExpression expression) {
        DatabendDataType type = expression.getExpectedType();
        String sql = DatabendVisitor.asString(expression);
        if (type == DatabendDataType.VARIANT) {
            return new DatabendText("JSON_TO_STRING(" + sql + ")", DatabendDataType.VARCHAR);
        }
        if (type == DatabendDataType.BITMAP) {
            return new DatabendText("BITMAP_TO_ARRAY(" + sql + ")", DatabendDataType.ARRAY);
        }
        if (type == DatabendDataType.GEOMETRY) {
            return new DatabendText("HEX(ST_ASWKB(" + sql + "))", DatabendDataType.VARCHAR);
        }
        return expression;
    }

    static String semanticResultExpressionForTesting(String expression, DatabendDataType type) {
        return DatabendVisitor.asString(semanticResultExpression(new DatabendText(expression, type)));
    }

    private VpQueryPair generateDatabendFeaturePair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        if (!streamLicenseExpired && allFuseHelpers(vp) && vp.baseRowCount > 0
                && Randomly.getBooleanWithRatherLowProbability()) {
            return generateMutationConsistencyPair(vp, strategy);
        }
        List<AdvancedFeature> candidates = new ArrayList<>();
        if (vp.columns.stream().anyMatch(c -> c.type == DatabendDataType.VARIANT)) {
            candidates.add(AdvancedFeature.FLATTEN);
            candidates.add(AdvancedFeature.JSON_PATH);
            candidates.add(AdvancedFeature.JSON_PATH_PREDICATE);
        }
        if (hasExclusiveBusinessSides(vp) && vp.baseRowCount >= 6) {
            candidates.add(AdvancedFeature.SEMISTRUCTURED_CONSISTENCY);
        }
        if (vp.columns.stream().anyMatch(c -> c.type == DatabendDataType.ARRAY)) {
            candidates.add(AdvancedFeature.UNNEST);
        }
        if (supportsTimeTravel(allFuseHelpers(vp), vp.baseRowCount)) {
            candidates.add(AdvancedFeature.TIME_TRAVEL_TIMESTAMP);
            candidates.add(AdvancedFeature.TIME_TRAVEL_SNAPSHOT);
        }
        // Databend streams are supported only for FUSE tables. Mixed FUSE/MEMORY
        // helper runs must not manufacture CREATE STREAM statements for MEMORY.
        if (!streamLicenseExpired && supportsStreams(allFuseHelpers(vp), vp.baseRowCount)) {
            candidates.add(AdvancedFeature.STREAM);
        }
        if (vp.columns.stream().anyMatch(c -> c.type == DatabendDataType.ARRAY || c.type == DatabendDataType.INT
                || c.type == DatabendDataType.VARCHAR)) {
            candidates.add(AdvancedFeature.ARRAY_AGG);
        }
        if (vp.columns.stream().anyMatch(c -> isDecimalType(c.type))) {
            candidates.add(AdvancedFeature.DECIMAL_BOUNDARY);
        }
        if (vp.columns.stream().anyMatch(c -> c.type == DatabendDataType.BITMAP)) {
            candidates.add(AdvancedFeature.BITMAP_AGGREGATION);
        }
        if (vp.baseRowCount > 0) {
            candidates.add(AdvancedFeature.QUALIFY_WINDOW);
            candidates.add(AdvancedFeature.SET_OPERATION_TYPE_MERGE);
            candidates.add(AdvancedFeature.PROJECTION_PRUNING);
            candidates.add(AdvancedFeature.RUNTIME_FILTER);
            candidates.add(AdvancedFeature.JOIN_REORDER);
            candidates.add(AdvancedFeature.TOP_K_PUSHDOWN);
        }
        if (vp.columns.stream().anyMatch(c -> c.nullable && isStableOrderType(c.type))) {
            candidates.add(AdvancedFeature.NULL_ORDERING);
        }
        if (allFuseHelpers(vp)) {
            candidates.add(AdvancedFeature.COMPACTION_READ);
            if (!streamLicenseExpired && vp.baseRowCount > 0) {
                candidates.add(AdvancedFeature.MUTATION_CONSISTENCY);
            }
        }
        if (vp.baseRowCount > 0) {
            if (allFuseHelpers(vp)) {
                candidates.add(AdvancedFeature.SCHEMA_EVOLUTION);
            }
            candidates.add(AdvancedFeature.SPILL);
        }
        if (candidates.isEmpty()) {
            throw new IgnoreMeException();
        }
        AdvancedFeature feature = Randomly.fromList(candidates);
        switch (feature) {
        case FLATTEN:
            return generateFlattenPair(vp, strategy);
        case UNNEST:
            return generateUnnestPair(vp, strategy);
        case JSON_PATH:
            return generateJsonPathPair(vp, strategy);
        case JSON_PATH_PREDICATE:
            return generateJsonPathPredicatePair(vp, strategy);
        case ARRAY_AGG:
            return generateArrayAggPair(vp, strategy);
        case DECIMAL_BOUNDARY:
            return generateDecimalBoundaryPair(vp, strategy);
        case TIME_TRAVEL_TIMESTAMP:
            return generateTimeTravelPair(vp, strategy, false);
        case TIME_TRAVEL_SNAPSHOT:
            return generateTimeTravelPair(vp, strategy, true);
        case STREAM:
            return generateStreamPair(vp, strategy);
        case BITMAP_AGGREGATION:
            return generateBitmapAggregationPair(vp, strategy);
        case QUALIFY_WINDOW:
            return generateQualifyPair(vp, strategy);
        case SET_OPERATION_TYPE_MERGE:
            return generateSetOperationPair(vp, strategy);
        case PROJECTION_PRUNING:
            return generateProjectionPruningPair(vp, strategy);
        case RUNTIME_FILTER:
            return generateRuntimeFilterPair(vp, strategy);
        case JOIN_REORDER:
            return generateJoinReorderPair(vp, strategy);
        case TOP_K_PUSHDOWN:
            return generateTopKPair(vp, strategy);
        case NULL_ORDERING:
            return generateNullOrderingPair(vp, strategy);
        case COMPACTION_READ:
            return generateCompactionPair(vp, strategy);
        case MUTATION_CONSISTENCY:
            return generateMutationConsistencyPair(vp, strategy);
        case SEMISTRUCTURED_CONSISTENCY:
            return generateSemistructuredConsistencyPair(vp, strategy);
        case SCHEMA_EVOLUTION:
            return generateSchemaEvolutionPair(vp, strategy);
        case SPILL:
            return generateSpillPair(vp, strategy);
        default:
            throw new AssertionError(feature);
        }
    }

    private boolean allFuseHelpers(VpSchema vp) {
        return "FUSE".equalsIgnoreCase(vp.sourceEngine) && "FUSE".equalsIgnoreCase(vp.leftEngine)
                && "FUSE".equalsIgnoreCase(vp.rightEngine);
    }

    /**
     * Clustering metadata is valid only when clustering was requested and all materialized helper tables are actually
     * FUSE tables. The latter is evaluated after engine fallback, rather than from the preferred engine.
     */
    static boolean supportsClusteringInformation(boolean clusteredHelpers, boolean allFuseHelpers) {
        return clusteredHelpers && allFuseHelpers;
    }

    static boolean supportsTimeTravel(boolean allFuseHelpers, long rowCount) {
        return allFuseHelpers && rowCount > 0;
    }

    static boolean supportsStreams(boolean allFuseHelpers, long rowCount) {
        return allFuseHelpers && rowCount > 0;
    }

    private VpQueryPair generateFlattenPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        VpColumn column = Randomly.fromList(
                vp.columns.stream().filter(c -> c.type == DatabendDataType.VARIANT).collect(Collectors.toList()));
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String projection = qualified(vp.sourceTable, ROW_ID) + ", f.value";
        String suffix = ", LATERAL FLATTEN(input => " + qualified(vp.sourceTable, column.name) + ") f";
        recordAdvancedFeature(AdvancedFeature.FLATTEN);
        return featurePair(vp, strategy, "SELECT " + projection + " FROM " + quote(vp.sourceTable) + suffix,
                "SELECT " + projection + " FROM " + relation + suffix, setup, "flatten");
    }

    private VpQueryPair generateUnnestPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        VpColumn column = Randomly.fromList(
                vp.columns.stream().filter(c -> c.type == DatabendDataType.ARRAY).collect(Collectors.toList()));
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String projection = qualified(vp.sourceTable, ROW_ID) + ", UNNEST(" + qualified(vp.sourceTable, column.name)
                + ")";
        recordAdvancedFeature(AdvancedFeature.UNNEST);
        return featurePair(vp, strategy, "SELECT " + projection + " FROM " + quote(vp.sourceTable),
                "SELECT " + projection + " FROM " + relation, setup, "unnest");
    }

    private VpQueryPair generateJsonPathPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        VpColumn column = Randomly.fromList(
                vp.columns.stream().filter(c -> c.type == DatabendDataType.VARIANT).collect(Collectors.toList()));
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String value = qualified(vp.sourceTable, column.name);
        String projection = qualified(vp.sourceTable, ROW_ID) + ", " + jsonPathProjection(value, "sqlancer");
        recordAdvancedFeature(AdvancedFeature.JSON_PATH);
        return featurePair(vp, strategy, "SELECT " + projection + " FROM " + quote(vp.sourceTable),
                "SELECT " + projection + " FROM " + relation, setup, "json-path");
    }

    private VpQueryPair generateJsonPathPredicatePair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        VpColumn column = Randomly.fromList(
                vp.columns.stream().filter(c -> c.type == DatabendDataType.VARIANT).collect(Collectors.toList()));
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String value = qualified(vp.sourceTable, column.name);
        String predicate = value + ":sqlancer = 'true'";
        String projection = qualified(vp.sourceTable, ROW_ID);
        recordAdvancedFeature(AdvancedFeature.JSON_PATH_PREDICATE);
        return featurePair(vp, strategy,
                "SELECT " + projection + " FROM " + quote(vp.sourceTable) + " WHERE " + predicate,
                "SELECT " + projection + " FROM " + relation + " WHERE " + predicate, setup, "json-path-predicate");
    }

    private VpQueryPair generateSemistructuredConsistencyPair(VpSchema vp, ReconstructionStrategy strategy)
            throws Exception {
        ensureSemistructuredFixture(vp);
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String doc = qualified(vp.sourceTable, vp.semiDocumentColumn);
        String opposite = qualified(vp.sourceTable, vp.semiOppositeColumn);
        qualityEvidence(vp, "semi-document=" + vp.semiDocumentColumn + " opposite-business=" + vp.semiOppositeColumn
                + " boundaries=sql-null/json-null/missing/empty-array/nested-array path_side=left value_side=right");
        recordAdvancedFeature(AdvancedFeature.SEMISTRUCTURED_CONSISTENCY);
        recordAdvancedFeature(AdvancedFeature.FLATTEN);
        recordAdvancedFeature(AdvancedFeature.JSON_PATH);
        return featurePair(vp, strategy, DatabendVpP1.semiQuery(quote(vp.sourceTable), doc, opposite),
                DatabendVpP1.semiQuery(relation, doc, opposite), setup, "semistructured-consistency");
    }

    private VpQueryPair generateArrayAggPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        VpColumn aggregateColumn = vp.columns.stream().filter(c -> c.type == DatabendDataType.ARRAY).findFirst()
                .orElse(null);
        String value = aggregateColumn == null ? quote(ROW_ID) : quote(aggregateColumn.name);
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        // Sorting the aggregate makes comparison independent of Databend's parallel
        // aggregation order while still exercising ARRAY_AGG state merging.
        String aggregate = "ARRAY_SORT(ARRAY_AGG(" + value + "))";
        recordAdvancedFeature(AdvancedFeature.ARRAY_AGG);
        return featurePair(vp, strategy, "SELECT " + aggregate + " FROM " + quote(vp.sourceTable),
                "SELECT " + aggregate + " FROM " + relation, setup, "array-agg");
    }

    private VpQueryPair generateDecimalBoundaryPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        VpColumn column = Randomly
                .fromList(vp.columns.stream().filter(c -> isDecimalType(c.type)).collect(Collectors.toList()));
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String expression = "SUM(CAST(" + quote(column.name) + " AS DECIMAL(65, 30)))";
        recordAdvancedFeature(AdvancedFeature.DECIMAL_BOUNDARY);
        return featurePair(vp, strategy, "SELECT " + expression + " FROM " + quote(vp.sourceTable),
                "SELECT " + expression + " FROM " + relation, setup, "decimal-boundary");
    }

    static String jsonPathProjection(String expression, String key) {
        return expression + ":" + key + ", " + expression + "['" + key.replace("'", "''") + "']";
    }

    private VpQueryPair generateTimeTravelPair(VpSchema vp, ReconstructionStrategy strategy, boolean snapshot)
            throws Exception {
        String sourceAt;
        String leftAt;
        String rightAt;
        AdvancedFeature feature;
        List<String> setup = new ArrayList<>();
        String description;
        if (snapshot) {
            // Keep the ordinary snapshot-id case, and occasionally compare a
            // source snapshot captured before helper compaction with helper
            // reads resolved after compaction. Compaction does not change rows,
            // so this exercises T versus T2 without comparing unrelated data
            // versions such as the pre-insert table snapshot.
            boolean mixedCompaction = Randomly.getNotCachedInteger(0, 3) == 2;
            sourceAt = snapshotTableReference(vp.sourceTable);
            if (mixedCompaction) {
                if (!allFuseHelpers(vp)) {
                    throw new IgnoreMeException();
                }
                maintainOneSide(vp, "COMPACT");
                leftAt = snapshotTableReference(vp.leftTable);
                rightAt = snapshotTableReference(vp.rightTable);
                recordAdvancedFeature(AdvancedFeature.COMPACTION_READ);
                recordAdvancedFeature(AdvancedFeature.TIME_TRAVEL_SNAPSHOT_MIXED);
                description = "time-travel-snapshot-mixed-compaction";
            } else {
                leftAt = snapshotTableReference(vp.leftTable);
                rightAt = snapshotTableReference(vp.rightTable);
                description = "time-travel-snapshot";
            }
            feature = AdvancedFeature.TIME_TRAVEL_SNAPSHOT;
        } else {
            String timestamp = getSingleString(latestHelperSnapshotTimestampQuery(state.getDatabaseName(), vp));
            if (isUnavailableMetadataValue(timestamp)) {
                throw new IgnoreMeException();
            }
            sourceAt = timestampTableReference(vp.sourceTable, timestamp);
            leftAt = timestampTableReference(vp.leftTable, timestamp);
            rightAt = timestampTableReference(vp.rightTable, timestamp);
            feature = AdvancedFeature.TIME_TRAVEL_TIMESTAMP;
            description = "time-travel-timestamp";
        }
        String projection = joinProjection(vp, "l", "r", false);
        // Keep the projection limited to the VP schema. Schema-evolution pairs may
        // add columns after materialization; SELECT * would then compare a newer
        // source snapshot against helpers that intentionally retain the original
        // projection.
        String singleQuery = "SELECT " + sourceColumnList(vp) + " FROM " + sourceAt + " ORDER BY " + quote(ROW_ID);
        String splitQuery = "SELECT " + projection + " FROM " + leftAt + " l JOIN " + rightAt + " r ON l."
                + quote(ROW_ID) + " = r." + quote(ROW_ID) + " ORDER BY l." + quote(ROW_ID);
        recordAdvancedFeature(feature);
        return featurePair(vp, strategy, singleQuery, splitQuery, setup, description);
    }

    private String snapshotTableReference(String table) throws SQLException {
        String snapshot = getSingleString("SELECT snapshot_id FROM FUSE_SNAPSHOT('" + state.getDatabaseName() + "', '"
                + table + "') ORDER BY timestamp DESC LIMIT 1");
        if (isUnavailableMetadataValue(snapshot)) {
            throw new IgnoreMeException();
        }
        return quote(table) + " AT (SNAPSHOT => '" + snapshot.replace("'", "''") + "')";
    }

    static String timestampTableReference(String table, String timestamp) {
        return quote(table) + " AT (TIMESTAMP => TIMESTAMP '" + timestamp.replace("'", "''") + "')";
    }

    static boolean isUnavailableMetadataValue(String value) {
        return value == null || value.equals("<none>") || value.equals("<unavailable>");
    }

    private static String latestHelperSnapshotTimestampQuery(String database, VpSchema vp) {
        String databaseLiteral = database.replace("'", "''");
        // Connector/J truncates Databend's microsecond snapshot timestamp when reading it as a string. Advancing to
        // the next whole second keeps the reference at or after all three commits, where AT resolves their latest
        // snapshots.
        return "SELECT MAX(timestamp) + INTERVAL 1 SECOND FROM (SELECT timestamp FROM FUSE_SNAPSHOT('" + databaseLiteral
                + "', '" + vp.sourceTable.replace("'", "''") + "') UNION ALL SELECT timestamp FROM FUSE_SNAPSHOT('"
                + databaseLiteral + "', '" + vp.leftTable.replace("'", "''")
                + "') UNION ALL SELECT timestamp FROM FUSE_SNAPSHOT('" + databaseLiteral + "', '"
                + vp.rightTable.replace("'", "''") + "'))";
    }

    private VpQueryPair generateBitmapAggregationPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        VpColumn column = Randomly.fromList(
                vp.columns.stream().filter(c -> c.type == DatabendDataType.BITMAP).collect(Collectors.toList()));
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String aggregate = "BITMAP_TO_ARRAY(BITMAP_OR_AGG(" + quote(column.name) + "))";
        recordAdvancedFeature(AdvancedFeature.BITMAP_AGGREGATION);
        return featurePair(vp, strategy, "SELECT " + aggregate + " FROM " + quote(vp.sourceTable),
                "SELECT " + aggregate + " FROM " + relation, setup, "bitmap-aggregation");
    }

    private VpQueryPair generateQualifyPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String single = "SELECT " + quote(ROW_ID) + ", ROW_NUMBER() OVER (ORDER BY " + quote(ROW_ID) + ") AS rn FROM "
                + quote(vp.sourceTable) + " QUALIFY rn <= 3 ORDER BY " + quote(ROW_ID);
        String split = "SELECT " + quote(ROW_ID) + ", ROW_NUMBER() OVER (ORDER BY " + quote(ROW_ID) + ") AS rn FROM "
                + relation + " QUALIFY rn <= 3 ORDER BY " + quote(ROW_ID);
        recordAdvancedFeature(AdvancedFeature.QUALIFY_WINDOW);
        return featurePair(vp, strategy, single, split, setup, "qualify-window");
    }

    private VpQueryPair generateSetOperationPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        List<VpColumn> integers = vp.columns.stream().filter(c -> c.type == DatabendDataType.INT)
                .collect(Collectors.toList());
        String single, split;
        if (!integers.isEmpty()) {
            String column = quote(Randomly.fromList(integers).name);
            single = businessTypeMergeQuery(quote(vp.sourceTable), column);
            split = businessTypeMergeQuery(relation, column);
            qualityEvidence(vp, "type-merge-column=" + column + " branch_types=BIGINT/DECIMAL(38,4)");
        } else {
            single = typeMergeQuery(quote(vp.sourceTable));
            split = typeMergeQuery(relation);
        }
        recordAdvancedFeature(AdvancedFeature.SET_OPERATION_TYPE_MERGE);
        return featurePair(vp, strategy, single, split, setup, "set-operation-type-merge");
    }

    private VpQueryPair generateProjectionPruningPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        ensureWideFixture(vp);
        List<VpColumn> left = exclusiveBusinessColumns(vp.leftColumns, vp.rightColumns);
        List<VpColumn> right = exclusiveBusinessColumns(vp.rightColumns, vp.leftColumns);
        if (left.isEmpty() || right.isEmpty()) {
            throw new IgnoreMeException();
        }
        VpColumn filter = Randomly.fromList(left), projected = Randomly.fromList(right);
        String term = null;
        long matched = 0, total = getRowCount(vp.sourceTable);
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(
                        "SELECT TO_VARCHAR(" + quote(filter.name) + "), COUNT(*) FROM " + quote(vp.sourceTable)
                                + " GROUP BY " + quote(filter.name) + " ORDER BY COUNT(*) LIMIT 1")) {
            if (result.next()) {
                String value = result.getString(1);
                term = value == null ? quote(filter.name) + " IS NULL" : quote(filter.name) + " = CAST("
                        + DatabendVpQueryQuality.literal(value) + " AS " + effectiveDeclaredSqlType(filter) + ")";
                matched = result.getLong(2);
            }
        }
        if (term == null || matched == 0 || matched >= total) {
            recordQualityCount("pruning_no_selective_witness", 1);
            throw new IgnoreMeException();
        }
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String projection = quote(ROW_ID) + ", " + quote(projected.name);
        String suffix = " WHERE " + term + " ORDER BY " + quote(filter.name) + " NULLS FIRST, " + quote(ROW_ID)
                + " LIMIT 5";
        qualityEvidence(vp, "pruning-width=" + vp.columns.size() + " predicate-business=" + filter.name
                + " projection-business=" + projected.name + " matched=" + matched + "/" + total);
        recordAdvancedFeature(AdvancedFeature.PROJECTION_PRUNING);
        return featurePair(vp, strategy, "SELECT " + projection + " FROM " + quote(vp.sourceTable) + suffix,
                "SELECT " + projection + " FROM " + relation + suffix, setup, "projection-pruning");
    }

    private VpQueryPair generateRuntimeFilterPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String single = runtimeFilterJoinQuery(quote(vp.sourceTable));
        String split = runtimeFilterJoinQuery(relation);
        recordAdvancedFeature(AdvancedFeature.RUNTIME_FILTER);
        return featurePair(vp, strategy, single, split, setup, "runtime-filter");
    }

    static String runtimeFilterJoinQuery(String relation) {
        // The probe side scans the complete relation. The build side is a
        // selective, materializable row-id set; this shape gives the optimizer
        // an actual equi-join on which to build a runtime filter.
        String probe = "(SELECT * FROM " + relation + ")";
        String build = probe;
        return "SELECT p." + quote(ROW_ID) + " FROM " + probe + " p JOIN (SELECT " + quote(ROW_ID) + " FROM " + build
                + " b0 WHERE b0." + quote(ROW_ID) + " % 3 = 1) b ON p." + quote(ROW_ID) + " = b." + quote(ROW_ID)
                + " ORDER BY p." + quote(ROW_ID);
    }

    private VpQueryPair generateJoinReorderPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String single = joinGraphQuery(quote(vp.sourceTable));
        String split = joinGraphQuery("(SELECT * FROM " + relation + ")");
        recordAdvancedFeature(AdvancedFeature.JOIN_REORDER);
        return featurePair(vp, strategy, single, split, setup, "join-reorder");
    }

    private VpQueryPair generateTopKPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String single = "SELECT " + quote(ROW_ID) + " FROM " + quote(vp.sourceTable) + " ORDER BY " + quote(ROW_ID)
                + " LIMIT 3";
        String split = "SELECT " + quote(ROW_ID) + " FROM " + relation + " ORDER BY " + quote(ROW_ID) + " LIMIT 3";
        recordAdvancedFeature(AdvancedFeature.TOP_K_PUSHDOWN);
        return featurePair(vp, strategy, single, split, setup, "top-k-pushdown");
    }

    private VpQueryPair generateNullOrderingPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        List<VpColumn> orderable = vp.columns.stream().filter(c -> c.nullable && isStableOrderType(c.type))
                .collect(Collectors.toList());
        if (orderable.isEmpty()) {
            throw new IgnoreMeException();
        }
        VpColumn column = Randomly.fromList(orderable);
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String single = "SELECT " + quote(ROW_ID) + " FROM " + quote(vp.sourceTable) + " ORDER BY " + quote(column.name)
                + " NULLS FIRST, " + quote(ROW_ID) + " LIMIT 5";
        String split = "SELECT " + quote(ROW_ID) + " FROM " + relation + " ORDER BY " + quote(column.name)
                + " NULLS FIRST, " + quote(ROW_ID) + " LIMIT 5";
        recordAdvancedFeature(AdvancedFeature.NULL_ORDERING);
        return featurePair(vp, strategy, single, split, setup, "null-ordering");
    }

    private VpQueryPair generateCompactionPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        maintainOneSide(vp, Randomly.getBoolean() ? "COMPACT" : "RECLUSTER");
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String single = "SELECT " + sourceColumnList(vp) + " FROM " + quote(vp.sourceTable) + " ORDER BY "
                + quote(ROW_ID);
        String split = "SELECT " + sourceColumnList(vp) + " FROM " + relation + " ORDER BY " + quote(ROW_ID);
        recordAdvancedFeature(AdvancedFeature.COMPACTION_READ);
        return featurePair(vp, strategy, single, split, setup, "compaction-read");
    }

    private VpQueryPair generateMutationConsistencyPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        if (!allFuseHelpers(vp) || vp.baseRowCount == 0) {
            throw new IgnoreMeException();
        }
        String sourceBefore = snapshotTableReference(vp.sourceTable);
        String leftBefore = snapshotTableReference(vp.leftTable);
        String rightBefore = snapshotTableReference(vp.rightTable);
        Long nextRowIdValue = readMetadataLong(
                "SELECT COALESCE(MAX(" + quote(ROW_ID) + "), 0) + 1 FROM " + quote(vp.sourceTable));
        Long deletedRowIdValue = readMetadataLong("SELECT MIN(" + quote(ROW_ID) + ") FROM " + quote(vp.sourceTable));
        // A concurrent cleanup or an empty snapshot can legitimately make MIN() NULL. Do not let
        // metadata parsing abort the whole run with NumberFormatException; this feature is optional.
        if (nextRowIdValue == null || deletedRowIdValue == null) {
            throw new IgnoreMeException();
        }
        long nextRowId = nextRowIdValue;
        long deletedRowId = deletedRowIdValue;
        String mutationSuffix = "_" + ORACLE_RUN_ID.incrementAndGet();
        String sourceStream = vp.prefix + "mutation_source_stream" + mutationSuffix;
        String leftStream = vp.prefix + "mutation_left_stream" + mutationSuffix;
        String rightStream = vp.prefix + "mutation_right_stream" + mutationSuffix;
        String sourceDeltaTable = vp.prefix + "mutation_source_delta" + mutationSuffix;
        String leftDeltaTable = vp.prefix + "mutation_left_delta" + mutationSuffix;
        String rightDeltaTable = vp.prefix + "mutation_right_delta" + mutationSuffix;
        List<String> setup = new ArrayList<>();
        for (String stream : List.of(sourceStream, leftStream, rightStream)) {
            setup.add("DROP STREAM IF EXISTS " + quote(stream));
        }
        for (String delta : List.of(sourceDeltaTable, leftDeltaTable, rightDeltaTable)) {
            setup.add("DROP TABLE IF EXISTS " + quote(delta));
        }
        setup.add("CREATE STREAM " + quote(sourceStream) + " ON TABLE " + quote(vp.sourceTable));
        setup.add("CREATE STREAM " + quote(leftStream) + " ON TABLE " + quote(vp.leftTable));
        setup.add("CREATE STREAM " + quote(rightStream) + " ON TABLE " + quote(vp.rightTable));
        setup.add(createMutationDeltaTableSQL(sourceDeltaTable, vp.columns));
        setup.add(createMutationDeltaTableSQL(leftDeltaTable, vp.leftColumns));
        setup.add(createMutationDeltaTableSQL(rightDeltaTable, vp.rightColumns));

        String sourceNames = sourceColumnList(vp);
        String sourceValues = vp.columns.stream().map(c -> quote(c.name)).collect(Collectors.joining(", "));
        String manifest = vp.prefix + "mutation_manifest" + mutationSuffix;
        // Freeze all values before any write. Each table consumes its own projection of this immutable plan.
        setup.add("CREATE TEMPORARY TABLE " + quote(manifest) + " AS SELECT " + nextRowId + " AS " + quote(ROW_ID)
                + ", " + sourceValues + " FROM " + quote(vp.sourceTable) + " ORDER BY " + quote(ROW_ID) + " LIMIT 1");
        setup.add("INSERT INTO " + quote(vp.sourceTable) + " (" + sourceNames + ") SELECT " + sourceNames + " FROM "
                + quote(manifest));
        setup.add("INSERT INTO " + quote(vp.leftTable) + " (" + partitionColumnList(vp.leftColumns) + ") SELECT "
                + partitionColumnList(vp.leftColumns) + " FROM " + quote(manifest));
        setup.add("INSERT INTO " + quote(vp.rightTable) + " (" + partitionColumnList(vp.rightColumns) + ") SELECT "
                + partitionColumnList(vp.rightColumns) + " FROM " + quote(manifest));
        setup.add("INSERT INTO " + quote(sourceDeltaTable) + " (" + sourceNames + ") SELECT " + sourceNames + " FROM "
                + quote(sourceStream));
        setup.add("INSERT INTO " + quote(leftDeltaTable) + " (" + partitionColumnList(vp.leftColumns) + ") SELECT "
                + partitionColumnList(vp.leftColumns) + " FROM " + quote(leftStream));
        setup.add("INSERT INTO " + quote(rightDeltaTable) + " (" + partitionColumnList(vp.rightColumns) + ") SELECT "
                + partitionColumnList(vp.rightColumns) + " FROM " + quote(rightStream));

        int insertCheckpoint = setup.size();
        for (String table : List.of(vp.sourceTable, vp.leftTable, vp.rightTable)) {
            setup.add("DELETE FROM " + quote(table) + " WHERE " + quote(ROW_ID) + " = " + deletedRowId);
        }
        int deleteCheckpoint = setup.size();
        List<VpColumn> nullable = vp.columns.stream().filter(c -> c.nullable).collect(Collectors.toList());
        if (!nullable.isEmpty()) {
            VpColumn changed = Randomly.fromList(nullable);
            setup.add("UPDATE " + quote(vp.sourceTable) + " SET " + quote(changed.name) + " = NULL WHERE "
                    + quote(ROW_ID) + " = " + nextRowId);
            if (containsColumn(vp.leftColumns, changed.name)) {
                setup.add("UPDATE " + quote(vp.leftTable) + " SET " + quote(changed.name) + " = NULL WHERE "
                        + quote(ROW_ID) + " = " + nextRowId);
            }
            if (containsColumn(vp.rightColumns, changed.name)) {
                setup.add("UPDATE " + quote(vp.rightTable) + " SET " + quote(changed.name) + " = NULL WHERE "
                        + quote(ROW_ID) + " = " + nextRowId);
            }
        }
        int updateCheckpoint = setup.size();
        setup.add("DROP TABLE " + quote(manifest));
        for (String stream : List.of(sourceStream, leftStream, rightStream)) {
            setup.add("DROP STREAM " + quote(stream));
        }
        qualityEvidence(vp, "mutation-phase=before/insert-delta/after manifest=" + manifest + " inserted_rowid="
                + nextRowId + " deleted_rowid=" + deletedRowId + " rebuild_partitions=false");

        String sourceSnapshot = "SELECT 'snapshot-before' AS phase, " + sourceColumnList(vp) + " FROM " + sourceBefore;
        String splitSnapshot = "SELECT 'snapshot-before' AS phase, " + joinProjection(vp, "l", "r", false) + " FROM "
                + leftBefore + " l JOIN " + rightBefore + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID);
        String sourceCurrent = "SELECT 'snapshot-after' AS phase, " + sourceColumnList(vp) + " FROM "
                + quote(vp.sourceTable);
        String splitCurrent = "SELECT 'snapshot-after' AS phase, " + joinProjection(vp, "l", "r", false) + " FROM "
                + quote(vp.leftTable) + " l JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID) + " = r."
                + quote(ROW_ID);
        String sourceDelta = "SELECT 'insert-delta' AS phase, " + sourceColumnList(vp) + " FROM "
                + quote(sourceDeltaTable);
        String splitDelta = "SELECT 'insert-delta' AS phase, " + joinProjection(vp, "l", "r", false) + " FROM "
                + quote(leftDeltaTable) + " l JOIN " + quote(rightDeltaTable) + " r ON l." + quote(ROW_ID) + " = r."
                + quote(ROW_ID);
        String single = sourceSnapshot + " UNION ALL " + sourceCurrent + " UNION ALL " + sourceDelta;
        String split = splitSnapshot + " UNION ALL " + splitCurrent + " UNION ALL " + splitDelta;
        recordAdvancedFeature(AdvancedFeature.MUTATION_CONSISTENCY);
        recordAdvancedFeature(AdvancedFeature.STREAM);
        recordAdvancedFeature(AdvancedFeature.TIME_TRAVEL_SNAPSHOT);
        recordAdvancedFeature(AdvancedFeature.DELETE_MUTATION);
        VpQueryPair pair = featurePair(vp, strategy, single, split, setup, "mutation-consistency");
        pair.mutationCheckpoints.put(insertCheckpoint, "after-insert");
        pair.mutationCheckpoints.put(deleteCheckpoint, "after-delete");
        pair.mutationCheckpoints.put(updateCheckpoint, "after-update");
        for (String delta : List.of(sourceDeltaTable, leftDeltaTable, rightDeltaTable)) {
            pair.cleanupStatements.add("DROP TABLE IF EXISTS " + quote(delta));
        }
        return pair;
    }

    private String createMutationDeltaTableSQL(String table, List<VpColumn> columns) {
        List<String> definitions = new ArrayList<>();
        definitions.add(quote(ROW_ID) + " BIGINT NULL");
        columns.forEach(column -> definitions.add(columnDef(column)));
        return "CREATE TEMPORARY TABLE " + quote(table) + " (" + String.join(", ", definitions) + ")";
    }

    private String partitionColumnList(List<VpColumn> columns) {
        List<String> names = new ArrayList<>();
        names.add(quote(ROW_ID));
        names.addAll(columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        return String.join(", ", names);
    }

    private VpQueryPair generateSchemaEvolutionPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        if (!allFuseHelpers(vp)) {
            throw new IgnoreMeException();
        }
        List<String> setup = new ArrayList<>();
        String column = sanitizeIdentifier(vp.prefix + "evolved_" + ORACLE_RUN_ID.incrementAndGet());
        for (String table : List.of(vp.sourceTable, vp.leftTable, vp.rightTable)) {
            String alter = "ALTER TABLE " + quote(table) + " ADD COLUMN " + quote(column) + " BIGINT NULL DEFAULT 7";
            // Apply the schema change before materializing any reconstruction
            // relation. Delaying ALTER until query execution can leave cached
            // temporary/derived tables without the new column.
            executeSetup(vp, alter);
        }
        VpColumn evolved = new VpColumn(column, new DatabendCompositeDataType(DatabendDataType.INT), true, "7");
        vp.columns.add(evolved);
        vp.leftColumns.add(evolved);
        vp.rightColumns.add(evolved);
        vp.overlapColumns.add(evolved);
        prepareReusableTemporaryTable(vp);
        for (String table : List.of(vp.sourceTable, vp.leftTable, vp.rightTable)) {
            setup.add("UPDATE " + quote(table) + " SET " + quote(column) + " = CASE WHEN MOD(" + quote(ROW_ID)
                    + ", 5) = 0 THEN NULL ELSE MOD(" + quote(ROW_ID) + ", 3) END WHERE MOD(" + quote(ROW_ID)
                    + ", 4) = 0");
        }
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String projection = quote(ROW_ID) + ", " + quote(column) + ", " + quote(column) + " + " + quote(ROW_ID);
        String single = "SELECT " + projection + " FROM " + quote(vp.sourceTable) + " WHERE " + quote(column)
                + " IS NULL OR " + quote(column) + " >= 1 ORDER BY " + quote(column) + " NULLS FIRST, " + quote(ROW_ID);
        String split = "SELECT " + projection + " FROM " + relation + " WHERE " + quote(column) + " IS NULL OR "
                + quote(column) + " >= 1 ORDER BY " + quote(column) + " NULLS FIRST, " + quote(ROW_ID);
        if (Randomly.getBoolean()) {
            String grouped = "SELECT " + quote(column) + ", COUNT(*) FROM ";
            String suffix = " GROUP BY " + quote(column) + " ORDER BY " + quote(column) + " NULLS FIRST";
            single = grouped + quote(vp.sourceTable) + suffix;
            split = grouped + relation + suffix;
        }
        qualityEvidence(vp, "evolved-column=" + column + " query_reads_evolved=true ordered=true");
        recordAdvancedFeature(AdvancedFeature.SCHEMA_EVOLUTION);
        return featurePair(vp, strategy, single, split, setup, "schema-evolution");
    }

    private VpQueryPair generateSpillPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        List<String> setup = new ArrayList<>();
        setup.add("SET force_aggregate_data_spill = 1");
        setup.add("SET force_sort_data_spill = 1");
        String relation = buildReconstructionRelation(vp, strategy, setup);
        String single = "SELECT " + quote(ROW_ID) + ", SUM(" + quote(ROW_ID) + ") FROM " + quote(vp.sourceTable)
                + " GROUP BY " + quote(ROW_ID) + " ORDER BY " + quote(ROW_ID) + " LIMIT 16";
        String split = "SELECT " + quote(ROW_ID) + ", SUM(" + quote(ROW_ID) + ") FROM " + relation + " GROUP BY "
                + quote(ROW_ID) + " ORDER BY " + quote(ROW_ID) + " LIMIT 16";
        recordAdvancedFeature(AdvancedFeature.SPILL);
        return featurePair(vp, strategy, single, split, setup, "spill");
    }

    private VpQueryPair generateStreamPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        Long nextRowIdValue = readMetadataLong(
                "SELECT COALESCE(MAX(" + quote(ROW_ID) + "), 0) + 1 FROM " + quote(vp.sourceTable));
        if (nextRowIdValue == null) {
            throw new IgnoreMeException();
        }
        long nextRowId = nextRowIdValue;
        String sourceStream = vp.prefix + "source_stream";
        String leftStream = vp.prefix + "left_stream";
        String rightStream = vp.prefix + "right_stream";
        List<String> setup = new ArrayList<>();
        for (String stream : List.of(sourceStream, leftStream, rightStream)) {
            setup.add("DROP STREAM IF EXISTS " + quote(stream));
        }
        setup.add("CREATE STREAM " + quote(sourceStream) + " ON TABLE " + quote(vp.sourceTable));
        setup.add("CREATE STREAM " + quote(leftStream) + " ON TABLE " + quote(vp.leftTable));
        setup.add("CREATE STREAM " + quote(rightStream) + " ON TABLE " + quote(vp.rightTable));
        String sourceColumns = vp.columns.stream().map(c -> quote(c.name)).collect(Collectors.joining(", "));
        setup.add("INSERT INTO " + quote(vp.sourceTable) + " (" + quote(ROW_ID) + ", " + sourceColumns + ") SELECT "
                + nextRowId + ", " + sourceColumns + " FROM " + quote(vp.sourceTable) + " ORDER BY " + quote(ROW_ID)
                + " LIMIT 1");
        setup.add(insertStreamRowSQL(vp.leftTable, vp.leftColumns, vp.sourceTable, nextRowId));
        setup.add(insertStreamRowSQL(vp.rightTable, vp.rightColumns, vp.sourceTable, nextRowId));
        String singleQuery = "SELECT " + sourceColumnList(vp) + " FROM " + quote(sourceStream);
        String splitQuery = "SELECT " + joinProjection(vp, "l", "r", false) + " FROM " + quote(leftStream) + " l JOIN "
                + quote(rightStream) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID);
        recordAdvancedFeature(AdvancedFeature.STREAM);
        return featurePair(vp, strategy, singleQuery, splitQuery, setup, "stream");
    }

    private String insertStreamRowSQL(String table, List<VpColumn> columns, String sourceTable, long rowId) {
        List<String> names = new ArrayList<>();
        names.add(quote(ROW_ID));
        names.addAll(columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        return "INSERT INTO " + quote(table) + " (" + String.join(", ", names) + ") SELECT " + String.join(", ", names)
                + " FROM " + quote(sourceTable) + " WHERE " + quote(ROW_ID) + " = " + rowId;
    }

    private VpQueryPair featurePair(VpSchema vp, ReconstructionStrategy strategy, String singleQuery, String splitQuery,
            List<String> setup, String feature) {
        recordFeatureMetric(feature, "generated");
        return new VpQueryPair(vp, strategy, singleQuery, splitQuery, setup, "shape=" + vp.baseShape + ", split="
                + vp.splitStrategy + ", reconstruction=" + strategy + ", databend=" + feature, orderedFeature(feature),
                false);
    }

    private static void recordFeatureMetric(String feature, String metric) {
        synchronized (STATS_LOCK) {
            Map<String, Integer> counts = FEATURE_PIPELINE_COUNTS.computeIfAbsent(feature, k -> new LinkedHashMap<>());
            counts.merge(metric, 1, Integer::sum);
        }
    }

    private static String pairFeature(VpQueryPair pair) {
        String marker = ", databend=";
        int index = pair.description.indexOf(marker);
        return index < 0 ? null : pair.description.substring(index + marker.length());
    }

    static boolean orderedFeature(String feature) {
        return List.of("cross-business-projection", "cross-business-aggregate", "qualify-window", "null-ordering",
                "top-k-pushdown", "projection-pruning", "semistructured-consistency", "schema-evolution",
                "compaction-read", "spill", "time-travel-timestamp", "time-travel-snapshot",
                "time-travel-snapshot-mixed-compaction").contains(feature);
    }

    static String businessTypeMergeQuery(String relation, String column) {
        return "SELECT CAST(" + column + " AS BIGINT) AS merged FROM " + relation + " UNION ALL SELECT CAST(" + column
                + " AS DECIMAL(38,4)) + CAST(0.125 AS DECIMAL(38,4)) FROM " + relation + " WHERE " + column
                + " IS NULL OR MOD(" + column + ", 3) = 1";
    }

    static String typeMergeQuery(String relation) {
        return "SELECT CAST(vp_rowid AS DECIMAL(38, 0)) AS merged FROM " + relation
                + " UNION ALL SELECT CAST(vp_rowid AS DECIMAL(38, 18)) + CAST(0.125 AS DECIMAL(38, 18)) FROM "
                + relation + " WHERE vp_rowid % 3 = 1";
    }

    static String joinGraphQuery(String relation) {
        return "SELECT a.vp_rowid, b.vp_rowid, c.vp_rowid FROM " + relation + " a JOIN " + relation
                + " b ON a.vp_rowid % 3 = b.vp_rowid % 3 JOIN " + relation
                + " c ON b.vp_rowid = c.vp_rowid WHERE c.vp_rowid % 2 = 0";
    }

    private static String qualified(String table, String column) {
        return quote(table) + "." + quote(column);
    }

    private boolean isRollupSafe(List<DatabendExpression> nonAggregateExpressions,
            List<DatabendExpression> groupByExpressions) {
        if (nonAggregateExpressions.size() != groupByExpressions.size()) {
            return false;
        }
        List<String> groupBySql = groupByExpressions.stream().map(DatabendVisitor::asString)
                .collect(Collectors.toList());
        for (DatabendExpression expression : nonAggregateExpressions) {
            if (!groupBySql.contains(DatabendVisitor.asString(expression))) {
                return false;
            }
        }
        return true;
    }

    private List<DatabendExpression> maybeAppendRollup(List<DatabendExpression> groupByExpressions,
            boolean hasWindowExpression, boolean rollupSafe) {
        // Databend 1.2.925 does not accept the MySQL WITH ROLLUP spelling.
        // Keep ordinary GROUP BY coverage instead of manufacturing parser
        // failures in the VP oracle.
        return groupByExpressions;
    }

    private VpQueryPair generateCorrelatedSubqueryPair(VpSchema vp, ReconstructionStrategy strategy,
            SourceAst sourceAst) throws Exception {
        // Repeating a relation containing CTE names for both sides of a
        // correlated query triggers Databend's "Duplicate common table
        // expression" parser error. These variants are not executable on
        // this server version.
        if (strategy == ReconstructionStrategy.TEMP_TABLE || strategy == ReconstructionStrategy.CTE
                || strategy == ReconstructionStrategy.ROW_NUMBER_ALIGNMENT) {
            throw new IgnoreMeException();
        }
        recordAdvancedFeature(AdvancedFeature.CORRELATED_SUBQUERY);
        String sourceAlias = "s";
        String subAlias = "s2";
        String selected = simpleColumnOrRowIdForSQL(sourceAst.columns);
        String outerColumn = qualifySelectedExpression(selected, sourceAlias);
        String predicateColumn = qualifySelectedExpression(simplePredicateColumnOrRowIdForSQL(sourceAst.columns),
                subAlias);
        List<String> correlatedQueries = new ArrayList<>();
        correlatedQueries.add("SELECT " + outerColumn + " FROM " + quote(vp.sourceTable) + " " + sourceAlias
                + " WHERE EXISTS (SELECT 1 FROM " + quote(vp.sourceTable) + " " + subAlias + " WHERE "
                + subAlias + "." + quote(ROW_ID) + " = " + sourceAlias + "." + quote(ROW_ID) + " AND "
                + predicateColumn + " IS NOT NULL)");
        correlatedQueries.add("SELECT " + outerColumn + " FROM " + quote(vp.sourceTable) + " " + sourceAlias
                + " WHERE " + sourceAlias + "." + quote(ROW_ID) + " IN (SELECT " + subAlias + "."
                + quote(ROW_ID) + " FROM " + quote(vp.sourceTable) + " " + subAlias + " WHERE "
                + predicateColumn + " IS NOT NULL)");
        // 950 SIGSEGV (#20717): retain EXISTS coverage, but omit only the
        // correlated MIN(range) template for the exact reproducing layout.
        if (!(strategy == ReconstructionStrategy.EXISTS_FILTER && isKnownDatabendVersion()
                && isCorrelatedRangeMinCrashLayout(vp)
                && predicateColumn.equals(subAlias + "." + quote(ROW_ID)))) {
            correlatedQueries.add("SELECT " + outerColumn + ", (SELECT MIN(" + predicateColumn + ") FROM "
                    + quote(vp.sourceTable) + " " + subAlias + " WHERE " + subAlias + "." + quote(ROW_ID)
                    + " <= " + sourceAlias + "." + quote(ROW_ID) + ") FROM " + quote(vp.sourceTable) + " "
                    + sourceAlias);
        }
        String singleQuery = Randomly.fromList(correlatedQueries);
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        String splitQuery = replaceCorrelatedSourceAliases(singleQuery, vp.sourceTable, relation, sourceAlias,
                subAlias);
        return new VpQueryPair(
                vp, strategy, singleQuery, splitQuery, extraSetup, "shape=" + vp.baseShape + ", split="
                        + vp.splitStrategy + ", reconstruction=" + strategy + ", advanced=correlated-subquery",
                false, false);
    }

    private String qualifySelectedExpression(String expression, String alias) {
        return alias + "." + expression;
    }

    private String simplePredicateColumnOrRowIdForSQL(List<DatabendColumn> columns) {
        List<DatabendColumn> scalarColumns = columns.stream()
                .filter(c -> isCorrelatedPredicateType(c.getType().getPrimitiveDataType()))
                .collect(Collectors.toList());
        return simpleColumnOrRowIdForSQL(scalarColumns);
    }

    static boolean isCorrelatedPredicateType(DatabendDataType type) {
        return isStableOrderType(type);
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

    private static boolean isGroupByOrdinalLiteral(DatabendExpression expression) {
        return expression instanceof DatabendIntConstant;
    }

    private List<DatabendExpression> chooseGroupByExpressions(List<DatabendExpression> nonAggregateExpressions) {
        List<DatabendExpression> groupable = nonAggregateExpressions.stream().filter(e -> !isGroupByOrdinalLiteral(e))
                .filter(DatabendVerticalPartitioningOracle::isGroupableExpression).collect(Collectors.toList());
        // Databend enforces ONLY_FULL_GROUP_BY. Partial grouping would make
        // the selected non-aggregate expressions invalid.
        return groupable;
    }

    static boolean supportsCompleteGrouping(List<DatabendExpression> expressions) {
        return expressions.stream().allMatch(e -> isGroupByOrdinalLiteral(e) || isGroupableExpression(e));
    }

    private static boolean isGroupableExpression(DatabendExpression expression) {
        if (expression instanceof DatabendColumnReference) {
            DatabendColumnReference reference = (DatabendColumnReference) expression;
            return isStableOrderType(reference.getColumn().getType().getPrimitiveDataType());
        }
        return isStableOrderType(expression.getExpectedType());
    }

    private List<DatabendExpression> generateVpOrderBys(List<DatabendColumn> columns) {
        List<DatabendColumn> orderable = columns.stream()
                .filter(c -> isStableOrderType(c.getType().getPrimitiveDataType())).collect(Collectors.toList());
        if (orderable.isEmpty()) {
            return List.of();
        }
        return Randomly.nonEmptySubset(orderable).stream().map(DatabendColumnReference::new)
                .collect(Collectors.toList());
    }

    private DatabendExpression generateHavingExpression(List<DatabendColumn> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            return new DatabendText(
                    Randomly.fromOptions("COUNT(*) > 0", "SUM(" + numericExpressionForSQL(columns) + ") IS NOT NULL",
                            // Databend parses <=> as cosine_distance, rather than
                            // MySQL's NULL-safe equality. It is not valid for the
                            // scalar/tuple aggregate operands generated here.
                            "COUNT(*) > 0"));
        }
        // A random row-level expression is not valid in HAVING under Databend's
        // ONLY_FULL_GROUP_BY rules. Keep the ordinary path aggregate-safe.
        return new DatabendText("COUNT(*) > 0");
    }

    private DatabendExpression generateAdvancedSelectExpression(SourceAst sourceAst, DatabendExpressionGenerator gen) {
        String order = simpleOrderExpressionForSQL(sourceAst.columns) + ", " + quote(ROW_ID);
        String value = simpleColumnOrRowIdForSQL(sourceAst.columns);
        String numeric = numericExpressionForSQL(sourceAst.columns);
        return new DatabendText(Randomly.fromOptions("ROW_NUMBER() OVER (ORDER BY " + order + ")",
                "RANK() OVER (ORDER BY " + order + ")", "LAG(" + value + ") OVER (ORDER BY " + quote(ROW_ID) + ")",
                "SUM(" + numeric + ") OVER (ORDER BY " + quote(ROW_ID) + ")"));
    }

    private DatabendSelect createUnionAllBranch(SourceAst sourceAst, DatabendExpressionGenerator gen,
            List<DatabendExpression> projection) {
        DatabendSelect right = new DatabendSelect();
        // Set-operation columns are matched by position. Reusing the typed
        // projection prevents independently generated branches from pairing,
        // for example, a BOOLEAN LAG with an integer window aggregate.
        right.setFetchColumns(new ArrayList<>(projection));
        right.setFromList(List.of(new DatabendTableReference(sourceAst.table)));
        if (Randomly.getBoolean()) {
            right.setWhereClause(gen.generateBooleanExpression());
        }
        return right;
    }

    static String renderQueryWithFrom(DatabendSelect select, DatabendSelect unionAllBranch,
            DatabendExpression fromRelation) {
        select.setFromList(List.of(fromRelation));
        String query = DatabendVisitor.asString(select);
        if (unionAllBranch == null) {
            return query;
        }
        unionAllBranch.setFromList(List.of(fromRelation));
        return "(" + query + ") UNION ALL (" + DatabendVisitor.asString(unionAllBranch) + ")";
    }

    private boolean isRowPreservingQuery(boolean useAggregate, DatabendSelect select) {
        return !useAggregate && select.getGroupByExpressions().isEmpty();
    }

    private boolean hasVpRowIdTieBreaker(DatabendSelect select, DatabendColumn rowIdColumn) {
        List<DatabendExpression> orderBys = select.getOrderByClauses();
        if (orderBys.isEmpty()) {
            return false;
        }
        DatabendExpression last = orderBys.get(orderBys.size() - 1);
        return last instanceof DatabendColumnReference
                && ((DatabendColumnReference) last).getColumn().getName().equals(rowIdColumn.getName());
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

    private static void recordPipelineMetric(ReconstructionStrategy strategy, PipelineMetric metric) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            incrementPipelineMetric(strategy, metric);
        }
    }

    private static void incrementPipelineMetric(ReconstructionStrategy strategy, PipelineMetric metric) {
        Map<PipelineMetric, Integer> counts = PIPELINE_COUNTS.get(strategy);
        if (counts == null) {
            counts = new EnumMap<>(PipelineMetric.class);
            PIPELINE_COUNTS.put(strategy, counts);
        }
        counts.put(metric, counts.getOrDefault(metric, 0) + 1);
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

    private static void recordOracleRunStarted() {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            startedOracleRuns++;
        }
    }

    private static void recordAttempt() {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            totalAttempts++;
        }
    }

    private static void recordCheckedPair() {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            totalQueryPairs++;
        }
    }

    private static void recordCompletedVpRun(int checkedPairs, int attempts) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            completedOracleRuns++;
            completedQueryPairs += checkedPairs;
            completedAttempts += attempts;
        }
    }

    private static void recordAbortedOracleRun() {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            abortedOracleRuns++;
        }
    }

    private static void recordKnownBugSuppressed(KnownVpBug knownBug) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            increment(KNOWN_BUG_COUNTS, knownBug);
        }
    }

    private static void recordKnownCrashSuppressed(KnownVpCrash knownCrash) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            increment(KNOWN_CRASH_COUNTS, knownCrash);
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
        sb.append("-- [vp-stats] event=process-final dbms=databend");
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] oracle-runs started=").append(startedOracleRuns);
        sb.append(" completed=").append(completedOracleRuns);
        sb.append(" aborted=").append(abortedOracleRuns);
        sb.append(" checkedPairs=").append(totalQueryPairs);
        sb.append(" attempts=").append(totalAttempts);
        sb.append(" completedCheckedPairs=").append(completedQueryPairs);
        sb.append(" completedAttempts=").append(completedAttempts);
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
        for (ReconstructionStrategy strategy : ReconstructionStrategy.values()) {
            sb.append("-- [vp-stats] strategy=").append(strategy.name());
            Map<PipelineMetric, Integer> counts = PIPELINE_COUNTS.getOrDefault(strategy, Collections.emptyMap());
            for (PipelineMetric metric : PipelineMetric.values()) {
                sb.append(" ").append(metric.logName).append("=").append(counts.getOrDefault(metric, 0));
            }
            sb.append(System.lineSeparator());
        }
        sb.append("-- [vp-stats] known-suppressed-counts ")
                .append(formatEnumCounts(KnownVpBug.values(), KNOWN_BUG_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] known-family-suppressed-counts ");
        for (KnownVpBugFamily family : KnownVpBugFamily.values()) {
            int count = 0;
            for (KnownVpBug bug : KnownVpBug.values()) {
                if (knownBugFamily(bug) == family) {
                    count += KNOWN_BUG_COUNTS.getOrDefault(bug, 0);
                }
            }
            sb.append(family).append("=").append(count).append(" ");
        }
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] known-crash-suppressed-counts ")
                .append(formatEnumCounts(KnownVpCrash.values(), KNOWN_CRASH_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] skipped-error-category-counts ")
                .append(formatEnumCounts(QueryErrorDisposition.values(), SKIPPED_ERROR_CATEGORY_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] skipped-error-counts ").append(formatStringCounts(SKIPPED_ERROR_COUNTS));
        sb.append(System.lineSeparator());
        if (SKIPPED_ERROR_COUNTS.size() > MAX_SKIPPED_ERROR_GROUPS) {
            sb.append("-- [vp-stats] skipped-error-counts-truncated distinctErrors=")
                    .append(SKIPPED_ERROR_COUNTS.size()).append(" shown=").append(MAX_SKIPPED_ERROR_GROUPS)
                    .append(System.lineSeparator());
        }
        for (Map.Entry<String, List<SkippedErrorSample>> entry : SKIPPED_ERROR_SAMPLES.entrySet()) {
            for (SkippedErrorSample sample : entry.getValue()) {
                sb.append("-- [vp-stats] skipped-error-sample error=").append(entry.getKey()).append(" stage=")
                        .append(sample.stage).append(" strategy=").append(sample.strategy).append(" sql=")
                        .append(sample.sql).append(System.lineSeparator());
            }
        }
        if (SKIPPED_ERROR_COUNTS.size() > SKIPPED_ERROR_SAMPLES.size()) {
            sb.append("-- [vp-stats] skipped-error-samples-truncated distinctErrors=")
                    .append(SKIPPED_ERROR_COUNTS.size()).append(" sampledErrors=").append(SKIPPED_ERROR_SAMPLES.size())
                    .append(" maxSamplesPerError=").append(MAX_SKIPPED_ERROR_SAMPLES_PER_GROUP)
                    .append(System.lineSeparator());
        }
        sb.append("-- [vp-stats] approximate-match-counts ").append(formatApproximateStats());
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] query-quality-counts ").append(QUERY_QUALITY_COUNTS).append(System.lineSeparator());
        for (String sample : QUERY_QUALITY_SAMPLES) {
            sb.append("-- [vp-stats] query-quality-sample ").append(sample).append(System.lineSeparator());
        }
        sb.append("-- [vp-stats] advanced-feature-counts ")
                .append(formatEnumCounts(AdvancedFeature.values(), ADVANCED_FEATURE_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] feature-pipeline-counts ").append(FEATURE_PIPELINE_COUNTS)
                .append(System.lineSeparator());
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

    private static String formatStringCounts(Map<String, Integer> counts) {
        return counts.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(", "));
    }

    private static String formatApproximateStats() {
        return java.util.Arrays.stream(ApproximateKind.values()).filter(ApproximateKind::isApproximate)
                .map(kind -> kind.name() + "{count=" + APPROXIMATE_MATCH_COUNTS.getOrDefault(kind, 0) + ",maxUlps="
                        + APPROXIMATE_MAX_ULPS.getOrDefault(kind, 0L) + "}")
                .collect(Collectors.joining(", "));
    }

    public static void logSchemaShapeStats(DatabendGlobalState state, String phase) throws Exception {
        registerVpStatsShutdownHook();
        int emptySingle = 0;
        int emptyMulti = 0;
        int nonEmptySingle = 0;
        int nonEmptyMulti = 0;
        state.updateSchema();
        for (DatabendTable table : state.getSchema().getDatabaseTables()) {
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
            Runtime.getRuntime().addShutdownHook(new Thread(DatabendVerticalPartitioningOracle::writeFinalVpStats));
        }
    }

    private static void writeFinalVpStats() {
        String log;
        String skippedErrorsLog;
        synchronized (STATS_LOCK) {
            log = formatVpStats();
            skippedErrorsLog = formatSkippedErrorsLog();
        }
        writeVpLog(STATS_FILE, log);
        writeVpLog(SKIPPED_ERRORS_FILE, skippedErrorsLog);
    }

    private static String formatSkippedErrorsLog() {
        StringBuilder sb = new StringBuilder();
        sb.append("# Databend VP query errors (bounded)").append(System.lineSeparator());
        sb.append("categories=").append(formatEnumCounts(QueryErrorDisposition.values(), SKIPPED_ERROR_CATEGORY_COUNTS))
                .append(System.lineSeparator());
        sb.append("counts=").append(formatStringCounts(SKIPPED_ERROR_COUNTS)).append(System.lineSeparator());
        for (Map.Entry<String, List<SkippedErrorSample>> entry : SKIPPED_ERROR_SAMPLES.entrySet()) {
            for (SkippedErrorSample sample : entry.getValue()) {
                sb.append("sample error=").append(entry.getKey()).append(" stage=").append(sample.stage)
                        .append(" strategy=").append(sample.strategy).append(" sql=").append(sample.sql)
                        .append(System.lineSeparator());
            }
        }
        return sb.toString();
    }

    private static void writeVpLog(File file, String log) {
        File parent = file.getParentFile();
        Path target = file.toPath();
        try {
            if (parent != null) {
                Files.createDirectories(parent.toPath());
            }
            Files.writeString(target, utf8LogText(log), StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private DatabendExpression generateVpAggregate(DatabendExpressionGenerator gen, List<DatabendColumn> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            return new DatabendText(randomAdvancedAggregateSQL(columns));
        }
        DatabendAggregateFunction func = Randomly.fromOptions(DatabendAggregateFunction.values());
        if (func == DatabendAggregateFunction.SUM || func == DatabendAggregateFunction.AVG
                || func == DatabendAggregateFunction.STDDEV_POP || func == DatabendAggregateFunction.COVAR_POP
                || func == DatabendAggregateFunction.COVAR_SAMP) {
            List<DatabendExpression> args = new ArrayList<>();
            for (int i = 0; i < func.getNrArgs(); i++) {
                args.add(generateNumericAggregateInput(columns, func));
            }
            return new DatabendAggregateOperation(args, func);
        }
        List<DatabendExpression> args = new ArrayList<>();
        for (int i = 0; i < func.getNrArgs(); i++) {
            args.add(gen.generateExpression());
        }
        return new DatabendAggregateOperation(args, func);
    }

    static DatabendExpression generateNumericAggregateInput(List<DatabendColumn> columns) {
        return generateNumericAggregateInput(columns, null);
    }

    private static DatabendExpression generateNumericAggregateInput(List<DatabendColumn> columns,
            DatabendAggregateFunction function) {
        DatabendColumn rowIdColumn = columns.stream()
                .filter(c -> c.getName().equals(ROW_ID) && c.getType().getPrimitiveDataType() == DatabendDataType.INT)
                .findFirst().orElseThrow(() -> new AssertionError("missing synthetic BIGINT column " + ROW_ID));
        List<DatabendColumn> numericColumns = columns.stream()
                .filter(c -> !c.getName().equals(ROW_ID)
                        && isNumericAggregateInputType(c.getType().getPrimitiveDataType(), function))
                .collect(Collectors.toList());
        if (numericColumns.isEmpty() && function == null) {
            // Keep integer inputs preferred for stable statistical results, but do not
            // collapse coverage to vp_rowid when a table only has FLOAT/DECIMAL values.
            numericColumns = columns.stream()
                    .filter(c -> !c.getName().equals(ROW_ID)
                            && isStatisticalAggregateFallbackInputType(c.getType().getPrimitiveDataType()))
                    .collect(Collectors.toList());
        }
        if (!numericColumns.isEmpty()
                && (function == null || isStatisticalAggregate(function) || Randomly.getBoolean())) {
            DatabendColumn column = Randomly.fromList(numericColumns);
            return aggregateInputForColumn(column, function);
        }
        // The synthetic row id is always a non-null BIGINT. It also avoids
        // NULL-typed literals, which Databend rejects for statistical
        // aggregates even though NULL is legal in ordinary expressions.
        return new DatabendColumnReference(rowIdColumn);
    }

    static DatabendExpression aggregateInputForColumn(DatabendColumn column, DatabendAggregateFunction function) {
        if ((function == DatabendAggregateFunction.SUM || function == DatabendAggregateFunction.AVG)
                && isDecimalType(column.getType().getPrimitiveDataType())) {
            // Databend keeps SUM/AVG's Decimal accumulator at the input
            // precision. A valid value can therefore overflow as soon as
            // several rows are aggregated, especially for DECIMAL(p, p).
            return new DatabendText("CAST(" + quote(column.getName()) + " AS DOUBLE)", DatabendDataType.FLOAT);
        }
        return new DatabendColumnReference(column);
    }

    static boolean isAggregateNumericType(DatabendDataType type) {
        return type == DatabendDataType.INT || type == DatabendDataType.FLOAT || type == DatabendDataType.FLOATING
                || isDecimalType(type);
    }

    static boolean isStatisticalAggregateInputType(DatabendDataType type) {
        return type == DatabendDataType.INT;
    }

    static boolean isStatisticalAggregateFallbackInputType(DatabendDataType type) {
        // Databend COVAR_* rejects Decimal inputs with BadDataValueType. This
        // classification remains useful for general numeric generation; the
        // statistical SQL path deliberately avoids it because COPY round-trip
        // probes may leave NaN/Infinity in floating columns.
        return type == DatabendDataType.FLOAT || type == DatabendDataType.FLOATING;
    }

    private static boolean isNumericAggregateInputType(DatabendDataType type, DatabendAggregateFunction function) {
        return isStatisticalAggregate(function) ? isStatisticalAggregateInputType(type) : isAggregateNumericType(type);
    }

    private static boolean isDecimalType(DatabendDataType type) {
        return type == DatabendDataType.DECIMAL || type == DatabendDataType.NUMERIC;
    }

    private static boolean isStatisticalAggregate(DatabendAggregateFunction function) {
        return function == DatabendAggregateFunction.STDDEV_POP || function == DatabendAggregateFunction.COVAR_POP
                || function == DatabendAggregateFunction.COVAR_SAMP;
    }

    private String randomAdvancedAggregateSQL(List<DatabendColumn> columns) {
        String numeric = numericExpressionForSQL(columns);
        String statistical = statisticalExpressionForSQL(columns);
        return Randomly.fromOptions("AVG(" + numeric + ")", "STDDEV_POP(" + statistical + ")",
                "COVAR_POP(" + statistical + ", " + statistical + ")");
    }

    private String numericExpressionForSQL(List<DatabendColumn> columns) {
        List<DatabendColumn> numericColumns = columns.stream()
                .filter(c -> isAggregateNumericType(c.getType().getPrimitiveDataType())).collect(Collectors.toList());
        if (!numericColumns.isEmpty() && Randomly.getBoolean()) {
            DatabendColumn column = Randomly.fromList(numericColumns);
            String expression = quote(column.getName());
            return isDecimalType(column.getType().getPrimitiveDataType()) ? "CAST(" + expression + " AS DOUBLE)"
                    : expression;
        }
        return quote(ROW_ID);
    }

    private String statisticalExpressionForSQL(List<DatabendColumn> columns) {
        List<DatabendColumn> integerColumns = columns.stream().filter(
                c -> !c.getName().equals(ROW_ID) && isStatisticalAggregateInputType(c.getType().getPrimitiveDataType()))
                .collect(Collectors.toList());
        if (!integerColumns.isEmpty()) {
            if (Randomly.getBoolean()) {
                return quote(Randomly.fromList(integerColumns).getName());
            }
            return quote(ROW_ID);
        }
        return quote(ROW_ID);
    }

    private String simpleColumnOrRowIdForSQL(List<DatabendColumn> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        return quote(Randomly.fromList(columns).getName());
    }

    private String simpleOrderExpressionForSQL(List<DatabendColumn> columns) {
        List<DatabendColumn> orderableColumns = columns.stream()
                .filter(c -> isStableOrderType(c.getType().getPrimitiveDataType())).collect(Collectors.toList());
        if (orderableColumns.isEmpty()) {
            return quote(ROW_ID);
        }
        return simpleColumnOrRowIdForSQL(orderableColumns);
    }

    private static final class SourceAst {
        private final DatabendTable table;
        private final DatabendColumn rowIdColumn;
        private final List<DatabendColumn> columns;

        private SourceAst(DatabendTable table, DatabendColumn rowIdColumn, List<DatabendColumn> columns) {
            this.table = table;
            this.rowIdColumn = rowIdColumn;
            this.columns = columns;
        }
    }

    private SourceAst createSourceAst(VpSchema vp) {
        List<DatabendColumn> astColumns = new ArrayList<>();
        DatabendColumn rowId = new DatabendColumn(ROW_ID, DatabendCompositeDataType.getInt(8), true, false, false);
        astColumns.add(rowId);
        for (VpColumn column : vp.columns) {
            astColumns.add(new DatabendColumn(column.name, column.compositeType, false, true, false));
        }
        DatabendTable table = new DatabendTable(vp.sourceTable, astColumns, List.of(), false);
        for (DatabendColumn column : astColumns) {
            column.setTable(table);
        }
        return new SourceAst(table, rowId, astColumns);
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
            // The table is created once per VP schema. Recreating it for every
            // pair exhausts Databend's session-global temporary-table ids.
            extraSetup.add("TRUNCATE TABLE " + quote(vp.tempTable));
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
        defs.add(quote(ROW_ID) + " BIGINT NOT NULL");
        for (VpColumn column : vp.columns) {
            defs.add(columnDef(column));
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
        // Databend rejects this relation when it is nested more than once
        // (for example in an IN subquery): the repeated CTE names `l`/`r`
        // produce "Duplicate common table expression". Keep the same CTE
        // semantics as a pair of derived relations, which are safely scoped
        // at every nesting level.
        return "(SELECT " + joinProjection(vp, "l", "r", false) + " FROM (SELECT * FROM " + quote(vp.leftTable)
                + ") l JOIN (SELECT * FROM " + quote(vp.rightTable) + ") r ON l." + quote(ROW_ID) + " = r."
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
        // Use derived tables instead of a WITH clause. Databend rejects the
        // same CTE names when this relation is nested inside an IN/EXISTS
        // subquery ("Duplicate common table expression: l").
        String left = "(SELECT ROW_NUMBER() OVER (ORDER BY " + quote(ROW_ID) + ") AS rn, "
                + rowNumberCteProjection(vp.leftColumns) + " FROM " + quote(vp.leftTable) + ") l";
        String right = "(SELECT ROW_NUMBER() OVER (ORDER BY " + quote(ROW_ID) + ") AS rn, "
                + rowNumberCteProjection(vp.rightColumns) + " FROM " + quote(vp.rightTable) + ") r";
        return "(SELECT " + joinProjection(vp, "l", "r", false) + " FROM " + left + " JOIN " + right
                + " ON l.rn = r.rn) " + quote(vp.sourceTable);
    }

    private String rowNumberCteProjection(List<VpColumn> columns) {
        List<String> parts = new ArrayList<>();
        parts.add(quote(ROW_ID));
        parts.addAll(columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        return String.join(", ", parts);
    }

    private String jsonPackRelation(VpSchema vp) {
        List<VpColumn> packed = vp.rightColumns.stream()
                .filter(c -> DatabendVpQueryQuality.supportsTextRoundTrip(c.type)).collect(Collectors.toList());
        if (packed.isEmpty()) {
            recordQualityCount("json_pack_no_lossless_business_type", 1);
            throw new IgnoreMeException();
        }
        List<String> fields = new ArrayList<>(List.of("'vp'", quote(ROW_ID)));
        for (VpColumn column : packed) {
            fields.add(DatabendVpQueryQuality.literal(column.name + "_null"));
            fields.add(quote(column.name) + " IS NULL");
            fields.add(DatabendVpQueryQuality.literal(column.name));
            fields.add("COALESCE(TO_VARCHAR(" + quote(column.name) + "), '')");
        }
        List<String> projection = new ArrayList<>(List.of("l." + quote(ROW_ID) + " AS " + quote(ROW_ID)));
        boolean needsRawRight = false;
        for (VpColumn column : vp.columns) {
            String value;
            if (containsColumn(packed, column.name)) {
                value = DatabendVpQueryQuality.decodedJsonColumn(column.name, effectiveDeclaredSqlType(column));
            } else if (containsColumn(vp.leftColumns, column.name)) {
                value = "l." + quote(column.name);
            } else {
                value = "r." + quote(column.name);
                needsRawRight = true;
            }
            projection.add(value + " AS " + quote(column.name));
        }
        recordQualityCount("json_business_decoded_columns", packed.size());
        String from = " FROM " + quote(vp.leftTable) + " l JOIN (SELECT JSON_OBJECT(" + String.join(", ", fields)
                + ") AS payload FROM " + quote(vp.rightTable) + ") jp ON l." + quote(ROW_ID)
                + " = CAST(jp.payload:vp AS BIGINT)";
        if (needsRawRight) {
            from += " JOIN " + quote(vp.rightTable) + " r ON r." + quote(ROW_ID) + " = l." + quote(ROW_ID);
        }
        return reconstructedSubquery(vp, String.join(", ", projection), from);
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
        String feature = pairFeature(pair);
        if (!pair.mutationCheckpoints.isEmpty()) {
            pair.schema.mutationIncomplete = true;
        }
        boolean spillSettings = false;
        try {
            int setupIndex = 0;
            for (String setup : pair.extraSetupStatements) {
                executeNoSchemaRefresh(setup);
                pair.schema.setupStatements.add(setup);
                spillSettings |= setup.startsWith("SET force_") && setup.endsWith(" = 1");
                String phase = pair.mutationCheckpoints.get(++setupIndex);
                if (phase != null) {
                    verifyMutationCheckpoint(pair, phase);
                }
            }
        } catch (Exception | AssertionError e) {
            if (spillSettings) {
                resetSpillSettings();
            }
            throw e;
        }
        recordPipelineMetric(pair.reconstructionStrategy, PipelineMetric.SETUP_SUCCESS);
        QueryResultMultiset singleRows;
        QueryResultMultiset splitRows;
        try {
            singleRows = getResultMultiset(pair.singleQuery, pair.approximateComparisonEnabled, "single",
                    pair.reconstructionStrategy, pair.resultTypes);
            recordPipelineMetric(pair.reconstructionStrategy, PipelineMetric.SINGLE_SUCCESS);
            splitRows = getResultMultiset(pair.splitQuery, pair.approximateComparisonEnabled, "split",
                    pair.reconstructionStrategy, pair.resultTypes);
            recordPipelineMetric(pair.reconstructionStrategy, PipelineMetric.SPLIT_SUCCESS);
            if (feature != null) {
                recordFeatureMetric(feature, "execution_success");
            }
        } catch (SQLException | AssertionError e) {
            if (suppressKnownCrashError(pair, e)) {
                throw new IgnoreMeException();
            }
            throw e;
        }
        try {
            boolean exactMultisetMatch = singleRows.rows.equals(splitRows.rows);
            boolean multisetMatch = exactMultisetMatch || resultMultisetsMatch(singleRows, splitRows);
            recordPipelineMetric(pair.reconstructionStrategy, PipelineMetric.COMPARED);
            if (!exactMultisetMatch && multisetMatch) {
                recordPipelineMetric(pair.reconstructionStrategy, PipelineMetric.APPROXIMATE_MATCH);
            }
            if (!multisetMatch) {
                recordPipelineMetric(pair.reconstructionStrategy, PipelineMetric.MISMATCH);
                KnownVpBug knownBug = classifyKnownBug(pair, singleRows, splitRows);
                if (suppressKnownBug(pair, knownBug, singleRows, splitRows, false)) {
                    throw new IgnoreMeException();
                }
                throw new AssertionError(formatBugReport(pair, singleRows, splitRows));
            }
            if (pair.orderedComparisonEnabled && !orderedRowsMatch(singleRows, splitRows)) {
                recordPipelineMetric(pair.reconstructionStrategy, PipelineMetric.MISMATCH);
                KnownVpBug knownBug = classifyKnownOrderedBug(pair, singleRows, splitRows);
                if (suppressKnownBug(pair, knownBug, singleRows, splitRows, true)) {
                    throw new IgnoreMeException();
                }
                throw new AssertionError(formatOrderedBugReport(pair, singleRows, splitRows));
            }
            if (feature != null) {
                recordFeatureMetric(feature, "comparison_success");
                if (feature.equals("projection-pruning")) {
                    recordPruningPlans(pair);
                }
                if (feature.equals("runtime-filter") || feature.equals("join-reorder") || feature.equals("spill")) {
                    String plan = feature.equals("runtime-filter") ? analyzeProfile(pair.splitQuery)
                            : explainPlanText(pair.splitQuery);
                    boolean hit = plan != null && (feature.equals("runtime-filter") ? hasRuntimeFilterEvidence(plan)
                            : feature.equals("join-reorder") ? false : plan.contains("AGGREGATE"));
                    recordFeatureMetric(feature, plan == null || feature.equals("join-reorder") ? "plan_unknown"
                            : hit ? "plan_hit" : "plan_miss");
                    if (feature.equals("spill")) {
                        for (String query : List.of(pair.singleQuery, pair.splitQuery)) {
                            String profile = analyzeProfile(query);
                            recordFeatureMetric(feature, profile == null ? "actual_spill_unknown"
                                    : hasActualSpill(profile) ? "actual_spill_hit" : "actual_spill_not_observed");
                        }
                    }
                }
            }
        } finally {
            if (spillSettings) {
                resetSpillSettings();
            }
        }
        for (String cleanup : pair.cleanupStatements) {
            executeNoSchemaRefresh(cleanup);
        }
        if (!pair.mutationCheckpoints.isEmpty()) {
            pair.schema.mutationIncomplete = false;
        }
    }

    private void verifyMutationCheckpoint(VpQueryPair parent, String phase) throws Exception {
        VpSchema vp = parent.schema;
        String projection = sourceColumnList(vp);
        String relation = buildReconstructionRelation(vp, ReconstructionStrategy.JOIN, new ArrayList<>());
        VpQueryPair checkpoint = new VpQueryPair(vp, ReconstructionStrategy.JOIN,
                "SELECT " + projection + " FROM " + quote(vp.sourceTable) + " ORDER BY " + quote(ROW_ID),
                "SELECT " + projection + " FROM " + relation + " ORDER BY " + quote(ROW_ID), List.of(),
                "mutation-phase=" + phase + ", rebuild_partitions=false", true, false);
        QueryResultMultiset direct = diagnosticRows(checkpoint.singleQuery, List.of());
        QueryResultMultiset reconstructed = diagnosticRows(checkpoint.splitQuery, List.of());
        if (!direct.rows.equals(reconstructed.rows) || !direct.orderedRows.equals(reconstructed.orderedRows)) {
            throw new AssertionError(formatBugReport(checkpoint, direct, reconstructed));
        }
        recordQualityCount("mutation_checkpoint_" + phase, 1);
        qualityEvidence(vp, "mutation-checkpoint=" + phase + " comparison_success=true rebuild_partitions=false");
    }

    private void resetSpillSettings() throws SQLException {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute("SET force_aggregate_data_spill = 0");
            statement.execute("SET force_sort_data_spill = 0");
            statement.execute("SET force_join_data_spill = 0");
            statement.execute("SET force_window_data_spill = 0");
        }
    }

    private String analyzeProfile(String query) {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery("EXPLAIN ANALYZE " + query)) {
            StringBuilder profile = new StringBuilder();
            while (result.next()) {
                for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                    profile.append(result.getString(i)).append('\n');
                }
            }
            return profile.toString();
        } catch (SQLException e) {
            return null;
        }
    }

    static boolean hasActualSpill(String profile) {
        java.util.regex.Matcher matcher = Pattern
                .compile("(?:bytes|numbers)[^\\n:]*spill[^\\n:]*:\\s*([0-9]+(?:\\.[0-9]+)?)", Pattern.CASE_INSENSITIVE)
                .matcher(profile);
        while (matcher.find()) {
            if (Double.parseDouble(matcher.group(1)) > 0) {
                return true;
            }
        }
        return false;
    }

    static boolean hasRuntimeFilterEvidence(String profile) {
        String normalized = profile.toUpperCase(Locale.ROOT);
        return normalized.contains("RUNTIME FILTER") || normalized.contains("RUNTIMEFILTER")
                || normalized.contains("DYNAMIC FILTER") || normalized.contains("BLOOM FILTER");
    }

    private boolean suppressKnownCrashPattern(VpQueryPair pair) {
        if (isDatabendSignedZeroSortPartitionCrash(pair, pair.singleQuery)
                || isDatabendSignedZeroSortPartitionCrash(pair, pair.splitQuery)
                || isDatabendSignedZeroGroupSortPartitionCrash(pair, pair.singleQuery)
                || isDatabendSignedZeroGroupSortPartitionCrash(pair, pair.splitQuery)) {
            return suppressSignedZeroPartitionCrash();
        }

        if (isDatabendEmptyGroupSortPartitionCrash(pair, pair.singleQuery)
                || isDatabendEmptyGroupSortPartitionCrash(pair, pair.splitQuery)) {
            return suppressSignedZeroPartitionCrash();
        }

        if (isDatabendStddevFloatRankSortCrash(pair, pair.singleQuery)
                || isDatabendStddevFloatRankSortCrash(pair, pair.splitQuery)) {
            return suppressSignedZeroPartitionCrash();
        }

        if (isDatabendSelfModuloGroupSortCrash(pair, pair.singleQuery)
                || isDatabendSelfModuloGroupSortCrash(pair, pair.splitQuery)) {
            return suppressSignedZeroPartitionCrash();
        }

        return false;
    }

    /**
     * Issue #20477 and the 950 EXISTS variant terminate the query service. Keep this sole pre-execution
     * guard narrow; all recoverable crashes must execute and be classified from their actual error.
     */
    private boolean suppressFatalCrashBeforeExecution(VpQueryPair pair) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns
                || (pair.reconstructionStrategy != ReconstructionStrategy.IN_FILTER
                        && pair.reconstructionStrategy != ReconstructionStrategy.EXISTS_FILTER)
                || !pair.description.contains("advanced=correlated-subquery")
                || !isDatabendCorrelatedInRangeMinCrash(pair)) {
            return false;
        }
        recordKnownCrashSuppressed(KnownVpCrash.DATABEND_CORRELATED_IN_RANGE_MIN);
        return true;
    }

    private boolean suppressSignedZeroPartitionCrash() {
        KnownVpCrash crash = KnownVpCrash.DATABEND_SIGNED_ZERO_PARTITION_TOTAL_ASSERTION;
        recordKnownCrashSuppressed(crash);
        return true;
    }

    private boolean suppressKnownCrashError(VpQueryPair pair, Throwable error) {
        String message = getErrorText(error).toLowerCase(Locale.ROOT);
        if (isKnownDatabendVersion() && pair.reconstructionStrategy == ReconstructionStrategy.EXISTS_FILTER
                && "MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)
                && "MEMORY".equalsIgnoreCase(pair.schema.leftEngine)
                && "MEMORY".equalsIgnoreCase(pair.schema.rightEngine)
                && matchesCorrelatedExistsSignedNotIn(pair.splitQuery, "vp_rowid", pair.schema.leftTable,
                        pair.schema.rightTable)
                && matchesCorrelatedExistsSignedNotInError(message, pair.schema.rightTable, "vp_rowid")) {
            recordKnownCrashSuppressed(KnownVpCrash.DATABEND_CORRELATED_EXISTS_SIGNED_NOT_IN);
            recordPipelineMetric(pair.reconstructionStrategy, PipelineMetric.KNOWN_CRASH_SUPPRESSED);
            return true;
        }
        if (message.contains("unwinderror") && message.contains("code: 1104") && message.contains("unwrap")
                && message.contains("null") && isKnownDatabendVersion()
                && (matchesNullFilterAggregateCrashShape(pair) || matchesNullCompositeProjectionCrashShape(pair)
                        || matchesDatabend919TempNullProjectionCrashShape(pair)
                        || matchesDatabend921922TempNullUnwrapShape(pair)
                        || message.contains("called `result::unwrap()` on an `err` value: null")
                                && matchesFuseMapNullXorCrashShape(pair))) {
            recordKnownCrashSuppressed(KnownVpCrash.DATABEND_NULL_RESULT_UNWRAP);
            recordPipelineMetric(pair.reconstructionStrategy, PipelineMetric.KNOWN_CRASH_SUPPRESSED);
            return true;
        }
        if (!message.contains("partition.total > 0")) {
            return false;
        }
        boolean suppressed = suppressKnownCrashPattern(pair);
        if (!suppressed && isKnownDatabendVersion()) {
            recordKnownCrashSuppressed(KnownVpCrash.DATABEND_PARTITION_TOTAL_ASSERTION);
            suppressed = true;
        }
        if (suppressed) {
            recordPipelineMetric(pair.reconstructionStrategy, PipelineMetric.KNOWN_CRASH_SUPPRESSED);
        }
        return suppressed;
    }

    // 950 nightly: the same NULL unwrap also occurs before reconstruction, on a direct FUSE source scan.
    private boolean matchesFuseMapNullXorCrashShape(VpQueryPair pair) {
        if (!"FUSE".equalsIgnoreCase(pair.schema.sourceEngine)) {
            return false;
        }
        int from = topLevelKeyword(pair.singleQuery, " FROM ");
        int where = topLevelKeyword(pair.singleQuery, " WHERE ");
        if (from < 0 || where < 0 || !normalizeKnownBugQuery(pair.singleQuery.substring(from + 6, where)).trim()
                .equals(pair.schema.sourceTable.toUpperCase(Locale.ROOT))) {
            return false;
        }
        for (VpColumn map : pair.schema.columns) {
            if (map.type != DatabendDataType.MAP || !map.nullable) {
                continue;
            }
            for (VpColumn other : pair.schema.columns) {
                // A NOT NULL peer still triggers the 950 NULL unwrap (saved database0/database3).
                if (!other.name.equals(map.name)
                        && matchesMapNullXorPredicate(pair.singleQuery, map.name, other.name)) {
                    return true;
                }
            }
        }
        return false;
    }

    static boolean matchesMapNullXorPredicate(String query, String mapColumn, String otherColumn) {
        String map = "(?:[A-Z0-9_]+\\.)?" + Pattern.quote(mapColumn.toUpperCase(Locale.ROOT));
        String other = "(?:[A-Z0-9_]+\\.)?" + Pattern.quote(otherColumn.toUpperCase(Locale.ROOT));
        String xor = "(?:" + map + " IS NULL <> " + other + " IS NULL|" + other + " IS NULL <> " + map + " IS NULL)";
        return Pattern.compile("(?<![A-Z0-9_.])" + xor + "(?![A-Z0-9_])").matcher(predicateShapeText(query)).find();
    }

    private boolean matchesDatabend919TempNullProjectionCrashShape(VpQueryPair pair) {
        if (!isKnownDatabendVersion() || pair.reconstructionStrategy != ReconstructionStrategy.TEMP_TABLE) {
            return false;
        }
        String q = normalizeKnownBugQuery(pair.splitQuery);
        return q.contains("FROM ") && q.contains("JOINED")
                && q.matches(".*SELECT [A-Z0-9_.]*C[0-9]+DATE, [A-Z0-9_.]*C[0-9]+MAP, [A-Z0-9_.]*C[0-9]+FLOAT FROM .*")
                && q.contains("IS NULL") && q.contains("<>") && q.contains("ORDER BY VP_ROWID")
                && q.matches(".* LIMIT [0-9]+ OFFSET [0-9]+;?");
    }

    /** Databend 1.2.925 also unwraps NULL for the broader TEMP_TABLE null-xor family. */
    private boolean matchesDatabend921922TempNullUnwrapShape(VpQueryPair pair) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.TEMP_TABLE || !isKnownDatabendVersion()) {
            return false;
        }
        String query = normalizeKnownBugQuery(pair.splitQuery);
        boolean nullXor = query.contains("IS NULL") && query.contains("<>");
        boolean materialized = query.contains("FROM ") && query.contains("JOINED");
        boolean resultShape = query.matches(".*\\b(MIN|MAX|COUNT|AVG|SUM|COVAR_POP|COVAR_SAMP|STDDEV_POP)\\s*\\(.*")
                || query.contains("ORDER BY") || hasRepeatedProjectionTerm(query) || hasTempMapNullXorProjection(query);
        return nullXor && materialized && resultShape;
    }

    static boolean matchesDatabend921922TempNullUnwrapShapeForTesting(String query) {
        String normalized = normalizeKnownBugQuery(query);
        return normalized.contains("IS NULL") && normalized.contains("<>") && normalized.contains("JOINED")
                && (normalized.matches(".*\\b(MIN|MAX|COUNT|AVG|SUM|COVAR_POP|COVAR_SAMP|STDDEV_POP)\\s*\\(.*")
                        || normalized.contains("ORDER BY") || hasRepeatedProjectionTerm(normalized)
                        || hasTempMapNullXorProjection(normalized));
    }

    // databend920: nullable MAP projections also panic without grouping, ordering or duplicate end terms.
    // This nominates a shape only; suppression still requires the actual Code 1104 NULL unwrap and known version.
    private static boolean hasTempMapNullXorProjection(String query) {
        int from = topLevelKeyword(query, " FROM ");
        return from > 0
                && splitTopLevel(query.substring(7, from)).stream()
                        .anyMatch(term -> term.trim().matches("(?:[A-Z0-9_]+\\.)?C[0-9]+MAP"))
                && query.indexOf("IS NULL") != query.lastIndexOf("IS NULL");
    }

    private static boolean hasRepeatedProjectionTerm(String query) {
        int select = query.indexOf("SELECT ");
        int from = query.indexOf(" FROM ", select + 7);
        if (select < 0 || from <= select) {
            return false;
        }
        List<String> terms = splitTopLevel(query.substring(select + 7, from));
        return terms.size() >= 2 && terms.get(0).trim().equals(terms.get(terms.size() - 1).trim());
    }

    static boolean matchesNullFilterAggregateCrashShapeForTesting(String query) {
        return matchesNullFilterAggregateCrashShape(query);
    }

    private boolean matchesNullFilterAggregateCrashShape(VpQueryPair pair) {
        return matchesNullFilterAggregateCrashShape(pair.singleQuery)
                || matchesNullFilterAggregateCrashShape(pair.splitQuery);
    }

    private static boolean matchesNullFilterAggregateCrashShape(String query) {
        if (query == null) {
            return false;
        }
        String normalized = query.toUpperCase(Locale.ROOT).replace('`', ' ').replaceAll("\\s+", " ").trim();
        boolean nullComparison = normalized.contains("IS NULL") && normalized.contains("<>")
                && normalized.indexOf("IS NULL") != normalized.lastIndexOf("IS NULL");
        return nullComparison && normalized.contains("GROUP BY")
                && (normalized.contains("MIN(") || normalized.contains("MAX(") || normalized.contains("COUNT(")
                        || normalized.contains("AVG(") || normalized.contains("SUM("))
                && normalized.contains("ORDER BY");
    }

    private boolean matchesNullCompositeProjectionCrashShape(VpQueryPair pair) {
        return matchesNullCompositeProjectionCrashShape(pair.singleQuery)
                || matchesNullCompositeProjectionCrashShape(pair.splitQuery);
    }

    static boolean matchesNullCompositeProjectionCrashShapeForTesting(String query) {
        return matchesNullCompositeProjectionCrashShape(query);
    }

    private static boolean matchesNullCompositeProjectionCrashShape(String query) {
        if (query == null) {
            return false;
        }
        String normalized = normalizeKnownBugQuery(query);
        int select = normalized.indexOf("SELECT ");
        int from = normalized.indexOf(" FROM ");
        if (select < 0 || from <= select || normalized.contains(" GROUP BY ") || normalized.contains(" ORDER BY ")
                || normalized.matches(".*\\b(MIN|MAX|COUNT|AVG|SUM)\\s*\\(.*")) {
            return false;
        }
        String projection = normalized.substring(select + 7, from);
        List<String> terms = splitTopLevel(projection);
        return countOccurrences(normalized, "IS NULL") >= 2 && normalized.contains("<>") && terms.size() >= 2
                && terms.get(0).trim().equals(terms.get(terms.size() - 1).trim());
    }

    private boolean isDatabendCorrelatedInRangeMinCrash(VpQueryPair pair) {
        if (pair.reconstructionStrategy == ReconstructionStrategy.EXISTS_FILTER
                && !isCorrelatedRangeMinCrashLayout(pair.schema)) {
            return false;
        }
        String leftTable = pair.schema.leftTable.toUpperCase(Locale.ROOT);
        String rightTable = pair.schema.rightTable.toUpperCase(Locale.ROOT);
        return isDatabendCorrelatedInRangeMinShape(pair.splitQuery, leftTable, rightTable) && isKnownDatabendVersion();
    }

    private boolean isCorrelatedRangeMinCrashLayout(VpSchema schema) {
        if (schema.baseRowCount < 50 || schema.columns.size() != 3
                || !"FUSE".equalsIgnoreCase(schema.sourceEngine)
                || !"FUSE".equalsIgnoreCase(schema.leftEngine)
                || !"FUSE".equalsIgnoreCase(schema.rightEngine)) {
            return false;
        }
        long geometry = schema.columns.stream().filter(c -> c.type == DatabendDataType.GEOMETRY).count();
        long decimal = schema.columns.stream().filter(c -> c.type == DatabendDataType.DECIMAL).count();
        if (geometry == 2 && decimal == 1) {
            return true;
        }
        // databend922/database2: another independently confirmed SIGSEGV layout.
        // Keep the new branch restricted to the saved size, nullable types and routing.
        return databend950Build && matches922RangeMinLayout(schema.baseRowCount,
                schema.columns.stream().map(c -> c.type).collect(Collectors.toList()),
                schema.columns.stream().allMatch(c -> c.nullable),
                schema.leftColumns.stream().map(c -> c.type).collect(Collectors.toList()),
                schema.rightColumns.stream().map(c -> c.type).collect(Collectors.toList()),
                schema.overlapColumns.isEmpty());
    }

    static boolean matches922RangeMinLayout(long rows, List<DatabendDataType> columns, boolean allNullable,
            List<DatabendDataType> left, List<DatabendDataType> right, boolean noOverlap) {
        return rows == 74 && allNullable && noOverlap && columns.size() == 3
                && java.util.Collections.frequency(columns, DatabendDataType.VARIANT) == 2
                && java.util.Collections.frequency(columns, DatabendDataType.FLOAT) == 1
                && left.size() == 2 && left.contains(DatabendDataType.VARIANT) && left.contains(DatabendDataType.FLOAT)
                && right.equals(List.of(DatabendDataType.VARIANT));
    }

    private static boolean isDatabendCorrelatedInRangeMinShape(String sql, String leftTable, String rightTable) {
        String query = sql.toUpperCase(Locale.ROOT).replace("`", "").replaceAll("\\s+", " ");
        boolean hasRangeAggregate = query.contains("SELECT MIN(") && query.contains(" <= ");
        boolean hasInSemiJoin = query.contains(" IN (SELECT ") && query.contains("FROM " + rightTable);
        String correlatedLookup = "\\(SELECT R\\.[^)]* FROM " + Pattern.quote(rightTable)
                + " R WHERE R\\.[^)]* = L\\.[^)]*\\)";
        boolean hasCorrelatedRightLookup = Pattern.compile(correlatedLookup).matcher(query).find()
                && query.contains("FROM " + leftTable);
        // Saved database9 on 950 terminates the service with the equivalent correlated EXISTS filter.
        // Require the actual rowid correlation; an unrelated EXISTS must not disable a query.
        String existsSemiJoin = "WHERE EXISTS \\(SELECT 1 FROM " + Pattern.quote(rightTable)
                + " R WHERE R\\.VP_ROWID = L\\.VP_ROWID\\)";
        boolean hasExistsSemiJoin = Pattern.compile(existsSemiJoin).matcher(query).find()
                && query.contains("SELECT MIN(S2.VP_ROWID)") && query.contains("S2.VP_ROWID <= S.VP_ROWID");
        return hasRangeAggregate && (hasInSemiJoin || hasExistsSemiJoin) && hasCorrelatedRightLookup;
    }

    static boolean matchesCorrelatedInRangeMinFatalShapeForTesting(String sql, String leftTable, String rightTable) {
        return isDatabendCorrelatedInRangeMinShape(sql, leftTable.toUpperCase(Locale.ROOT),
                rightTable.toUpperCase(Locale.ROOT));
    }

    private boolean isDatabendSignedZeroSortPartitionCrash(VpQueryPair pair, String query) {
        String orderBy = normalizedOrderByClause(query);
        String columnName = signedZeroSortColumn(orderBy);
        if (columnName == null || !isKnownDatabendVersion()) {
            return false;
        }
        boolean integerColumn = pair.schema.columns.stream()
                .anyMatch(c -> c.name.equalsIgnoreCase(columnName) && c.type == DatabendDataType.INT);
        return integerColumn && hasSignedZeroSortPlanToken(query)
                && hasOppositeSignedValues(pair.schema.sourceTable, columnName);
    }

    private boolean isDatabendEmptyGroupSortPartitionCrash(VpQueryPair pair, String query) {
        if (!isKnownDatabendVersion() || !matchesEmptyGroupSortPartitionShape(query)
                || !hasPlanTokens(query, "AGGREGATE", "SORT")) {
            return false;
        }
        return query.toUpperCase(Locale.ROOT).contains(quote(ROW_ID).toUpperCase(Locale.ROOT));
    }

    private boolean isDatabendSignedZeroGroupSortPartitionCrash(VpQueryPair pair, String query) {
        return isKnownDatabendVersion() && matchesSignedZeroGroupSortPartitionShape(query)
                && hasPlanTokens(query, "AGGREGATE", "SORT") && pair.schema.columns.stream()
                        .anyMatch(c -> c.name.equalsIgnoreCase(ROW_ID) && c.type == DatabendDataType.INT);
    }

    private boolean isDatabendStddevFloatRankSortCrash(VpQueryPair pair, String query) {
        return isKnownDatabendVersion() && matchesStddevFloatRankSortPartitionShape(query)
                && hasPlanTokens(query, "STDDEV_POP", "RANK LIMIT", "SORT(SINGLE)")
                && hasBothSignedZeros(pair.schema.sourceTable, "c0float");
    }

    static boolean matchesStddevFloatRankSortPartitionShape(String query) {
        String normalized = normalizeKnownBugQuery(query).replaceAll("\\b[A-Z_][A-Z0-9_]*\\.", "");
        return normalized.contains("SELECT STDDEV_POP(C0FLOAT), C0FLOAT, VP_ROWID")
                && normalized.contains("GROUP BY C0FLOAT, VP_ROWID")
                && normalized.contains("ORDER BY C0FLOAT, VP_ROWID")
                && normalized.matches(".* LIMIT 16 OFFSET 0(?:;)?$");
    }

    private boolean isDatabendSelfModuloGroupSortCrash(VpQueryPair pair, String query) {
        return isKnownDatabendVersion() && matchesSelfModuloGroupSortPartitionShape(query)
                && hasPlanTokens(query, "AGGREGATE", "SORT(SINGLE)")
                && hasOppositeSignedValues(pair.schema.sourceTable, "c0int");
    }

    static boolean matchesSelfModuloGroupSortPartitionShape(String query) {
        String normalized = normalizeKnownBugQuery(query).replaceAll("\\b[A-Z_][A-Z0-9_]*\\.", "");
        int groupBy = normalized.indexOf(" GROUP BY ");
        int having = normalized.indexOf(" HAVING COUNT(*) > 0", groupBy);
        int orderBy = normalized.indexOf(" ORDER BY ", having);
        if (groupBy < 0 || having < 0 || orderBy < 0) {
            return false;
        }
        String group = normalized.substring(groupBy + " GROUP BY ".length(), having).trim();
        String order = normalized.substring(orderBy + " ORDER BY ".length()).replaceFirst(";+$", "").trim();
        String compact = normalized.replace(" ", "");
        String groupCompact = group.replace(" ", "");
        List<String> groupTerms = splitTopLevel(group);
        List<String> orderTerms = splitTopLevel(order);
        return group.equals(order) && groupTerms.size() == 3 && orderTerms.size() == 3
                && groupCompact.contains("C0INT%C0INT") && groupCompact.contains("VP_ROWID")
                && countOccurrences(groupCompact, "%") >= 3
                && Pattern.compile(".*C0INT%C0INT.*?/\\(*[-+]C0INT\\)*.*").matcher(compact).matches();
    }

    static boolean matchesSignedZeroGroupSortPartitionShape(String query) {
        if (query == null) {
            return false;
        }
        String normalized = query.toUpperCase(Locale.ROOT).replace("`", "").replaceAll("\\s+", " ").trim();
        int groupBy = normalized.indexOf(" GROUP BY ");
        int orderBy = normalized.indexOf(" ORDER BY ", groupBy + " GROUP BY ".length());
        if (groupBy < 0 || orderBy < 0) {
            return false;
        }
        String groupExpression = normalized.substring(groupBy + " GROUP BY ".length(), orderBy).trim();
        int orderEnd = normalized.length();
        for (String keyword : List.of(" LIMIT ", " OFFSET ", ";")) {
            int index = normalized.indexOf(keyword, orderBy + " ORDER BY ".length());
            if (index >= 0) {
                orderEnd = Math.min(orderEnd, index);
            }
        }
        String orderExpression = normalized.substring(orderBy + " ORDER BY ".length(), orderEnd).trim();
        if (groupExpression.isEmpty() || orderExpression.isEmpty()) {
            return false;
        }
        String compact = normalized.replace(" ", "");
        return compact.contains("VP_ROWID") && compact.contains("VP_ROWID)/(L.VP_ROWID)")
                && compact.contains("VP_ROWID)-(") && compact.contains("1-2*") && compact.contains("%")
                && compact.contains("*");
    }

    static boolean matchesEmptyGroupSortPartitionShape(String query) {
        if (query == null) {
            return false;
        }
        String normalized = query.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ").trim();
        int groupBy = normalized.indexOf(" GROUP BY ");
        int having = normalized.indexOf(" HAVING COUNT(*) > 0", groupBy);
        int orderBy = normalized.indexOf(" ORDER BY ", having);
        if (groupBy < 0 || having < 0 || orderBy < 0 || normalized.indexOf(" LIMIT ", orderBy) < 0) {
            return false;
        }
        String groupExpression = normalized.substring(groupBy + " GROUP BY ".length(), having).trim();
        String orderExpression = normalized
                .substring(orderBy + " ORDER BY ".length(), normalized.indexOf(" LIMIT ", orderBy)).trim();
        if (groupExpression.isEmpty() || !groupExpression.equals(orderExpression)) {
            return false;
        }
        String rowId = "`" + ROW_ID.toUpperCase(Locale.ROOT) + "`";
        return normalized.contains("(" + rowId + ")/(" + rowId + ")")
                && normalized.contains("(" + rowId + ")-(" + rowId + ")") && normalized.contains("%")
                && normalized.matches(".* LIMIT [0-9]+(?: OFFSET [0-9]+)?\\s*;?");
    }

    private String normalizedOrderByClause(String query) {
        String normalized = query.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
        int orderByIndex = normalized.indexOf(" ORDER BY ");
        if (orderByIndex < 0) {
            return "";
        }
        int endIndex = normalized.length();
        for (String keyword : List.of(" LIMIT ", " OFFSET ")) {
            int keywordIndex = normalized.indexOf(keyword, orderByIndex + " ORDER BY ".length());
            if (keywordIndex >= 0) {
                endIndex = Math.min(endIndex, keywordIndex);
            }
        }
        return normalized.substring(orderByIndex + " ORDER BY ".length(), endIndex);
    }

    static boolean matchesSignedZeroSortOrder(String orderBy) {
        return signedZeroSortColumn(orderBy) != null;
    }

    private static String signedZeroSortColumn(String orderBy) {
        if (orderBy == null || orderBy.isBlank()) {
            return null;
        }
        List<String> terms = splitTopLevelOrderTerms(orderBy);
        if (terms.size() != 2) {
            return null;
        }
        String first = stripOuterParentheses(terms.get(0)).replaceAll("\\s+", " ").trim();
        String second = stripOuterParentheses(terms.get(1)).replaceAll("\\s+", " ").trim();
        String identifier = "(?:`?[A-Z_][A-Z0-9_]*`?)";
        Pattern modulo = Pattern.compile(
                "(?i)^(" + identifier + "(?:\\." + identifier + ")?)\\s*%\\s*" + "[-+]?\\d+\\.\\d+(?:E[-+]?\\d+)?$");
        java.util.regex.Matcher moduloMatcher = modulo.matcher(first);
        if (!moduloMatcher.matches()) {
            return null;
        }
        String firstColumn = unqualifiedIdentifier(moduloMatcher.group(1));
        Pattern negated = Pattern.compile("(?i)^-\\s*(" + identifier + "(?:\\." + identifier + ")?)$");
        java.util.regex.Matcher negatedMatcher = negated.matcher(second);
        if (!negatedMatcher.matches()
                || !firstColumn.equalsIgnoreCase(unqualifiedIdentifier(negatedMatcher.group(1)))) {
            return null;
        }
        return firstColumn;
    }

    private static List<String> splitTopLevelOrderTerms(String orderBy) {
        List<String> terms = new ArrayList<>();
        int depth = 0;
        int start = 0;
        for (int i = 0; i < orderBy.length(); i++) {
            char ch = orderBy.charAt(i);
            if (ch == '(') {
                depth++;
            } else if (ch == ')' && depth > 0) {
                depth--;
            } else if (ch == ',' && depth == 0) {
                terms.add(orderBy.substring(start, i));
                start = i + 1;
            }
        }
        terms.add(orderBy.substring(start));
        return terms;
    }

    private static String stripOuterParentheses(String value) {
        String result = value.trim();
        while (result.startsWith("(") && result.endsWith(")") && isWrappedByOuterParentheses(result)) {
            result = result.substring(1, result.length() - 1).trim();
        }
        return result;
    }

    private static boolean isWrappedByOuterParentheses(String value) {
        int depth = 0;
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '(') {
                depth++;
            } else if (ch == ')') {
                depth--;
                if (depth == 0 && i != value.length() - 1) {
                    return false;
                }
            }
        }
        return depth == 0;
    }

    private static String unqualifiedIdentifier(String identifier) {
        String[] parts = identifier.replace("`", "").split("\\.");
        return parts[parts.length - 1];
    }

    static boolean isKnownDatabendVersion(String version) {
        String nightlyBuild = "8.0.90-v1.2.950-nightly-7d6d8c143a";
        return version != null && (version.startsWith("8.0.90-v1.2.925-patch-11") || version.equals(nightlyBuild)
                || version.startsWith(nightlyBuild + "("));
    }

    private boolean isKnownDatabendVersion() {
        return knownDatabendVersion;
    }

    private boolean hasSignedZeroSortPlanToken(String query) {
        return hasPlanTokens(query, "SORT(SINGLE)", "SORT KEYS:");
    }

    private boolean hasPlanTokens(String query, String... tokens) {
        String plan = explainPlanText(query);
        return plan != null && java.util.Arrays.stream(tokens).allMatch(plan::contains);
    }

    private String explainPlanText(String query) {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery("EXPLAIN " + query)) {
            StringBuilder plan = new StringBuilder();
            int columnCount = result.getMetaData().getColumnCount();
            while (result.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    plan.append(' ').append(result.getString(i));
                }
            }
            return plan.toString().toUpperCase(Locale.ROOT);
        } catch (SQLException e) {
            return null;
        }
    }

    private boolean hasOppositeSignedValues(String tableName, String columnName) {
        String query = "SELECT MIN(" + quote(columnName) + "), MAX(" + quote(columnName) + ") FROM " + quote(tableName);
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(query)) {
            if (!result.next()) {
                return false;
            }
            BigDecimal min = result.getBigDecimal(1);
            BigDecimal max = result.getBigDecimal(2);
            return min != null && max != null && min.signum() < 0 && max.signum() > 0;
        } catch (SQLException e) {
            return false;
        }
    }

    private boolean hasBothSignedZeros(String tableName, String columnName) {
        String column = quote(columnName);
        String query = "SELECT COUNT_IF(" + column + " = 0 AND TO_STRING(" + column + ") = '0'), " + "COUNT_IF("
                + column + " = 0 AND TO_STRING(" + column + ") = '-0') FROM " + quote(tableName);
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(query)) {
            return result.next() && result.getLong(1) > 0 && result.getLong(2) > 0;
        } catch (SQLException e) {
            return false;
        }
    }

    private boolean suppressKnownBug(VpQueryPair pair, KnownVpBug knownBug, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows, boolean orderedReport) throws SQLException {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns || knownBug == null) {
            return false;
        }
        logKnownBug(pair, knownBug, singleRows, splitRows, orderedReport);
        recordKnownBugSuppressed(knownBug);
        recordPipelineMetric(pair.reconstructionStrategy, PipelineMetric.KNOWN_BUG_SUPPRESSED);
        return true;
    }

    private KnownVpBug classifyKnownBug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        KnownVpBug unionBug = classifyKnownUnionAllBug(pair, singleRows, splitRows);
        if (unionBug != null) {
            return unionBug;
        }
        KnownVpBug current919 = classifyDatabend919Bug(pair, singleRows, splitRows);
        if (current919 != null) {
            return current919;
        }
        if (matchesNullableSelfComparisonFilterLoss(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_NULLABLE_SELF_COMPARISON_FILTER_LOSS;
        }
        if (matchesNullableConjunctiveLikeFilterLoss(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_NULLABLE_CONJUNCTIVE_LIKE_FILTER_LOSS;
        }
        if (matchesFloatSelfNotInJoinFilterLoss(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_FLOAT_SELF_NOT_IN_JOIN_FILTER_LOSS;
        }
        if (matchesCorrelatedScalarFloatZeroNullMatch(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_CORRELATED_SCALAR_FLOAT_ZERO_NULL_MATCH;
        }
        KnownVpBug confirmed917 = classifyDatabend917Bug(pair, singleRows, splitRows);
        if (confirmed917 != null) {
            return confirmed917;
        }
        if (pair.reconstructionStrategy == ReconstructionStrategy.FULL_OUTER_SIMULATION
                && pair.schema.splitStrategy == SplitStrategy.SINGLE_REPLICATED
                && pair.schema.baseShape == BaseShape.SINGLE_COLUMN && pair.schema.columns.size() == 1
                && pair.schema.leftColumns.size() == 1 && pair.schema.rightColumns.size() == 1
                && pair.schema.overlapColumns.size() == 1
                && containsColumn(pair.schema.leftColumns, pair.schema.columns.get(0).name)
                && containsColumn(pair.schema.rightColumns, pair.schema.columns.get(0).name)
                && containsColumn(pair.schema.overlapColumns, pair.schema.columns.get(0).name)
                && matchesDatabendFullOuterPredicatePushdownShape(pair.splitQuery)) {
            return KnownVpBug.DATABEND_FULL_OUTER_PREDICATE_PUSHDOWN_DUPLICATION;
        }
        if (matchesDatabase4LateralAggregateBug(pair)) {
            return KnownVpBug.DATABEND_LATERAL_MIN_GROUP_LIMIT_DROP;
        }
        if (matchesDatabase6LateralAggregateBug(pair)) {
            return KnownVpBug.DATABEND_LATERAL_SUM_GROUP_EMPTY;
        }
        if (matchesDatabendRuntimeFilterRankLimitBug(pair)) {
            return KnownVpBug.DATABEND_RUNTIME_FILTER_RANK_LIMIT_TRUNCATION;
        }
        if (matchesDatabendFuseTopNBlockPruningBug(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_FUSE_TOPN_BLOCK_PRUNING_ROW_SKIP;
        }
        if (matchesDatabendArrayAggNullJoinBug(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_ARRAY_AGG_NULL_JOIN_MATERIALIZATION;
        }
        // The same ARRAY NULL/empty materialization defect also affects
        // predicates after a correlated scalar lookup, without ARRAY_AGG.
        if (matchesDatabendArrayNullPredicateJoinBug(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_ARRAY_AGG_NULL_JOIN_MATERIALIZATION;
        }
        if (matchesDatabendFloatAggregateCancellation(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_FLOAT_AGGREGATE_CANCELLATION;
        }
        if (matchesDatabendLeadingNullCompositeGroupBug(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_LEADING_NULL_COMPOSITE_GROUP_KEY;
        }
        return null;
    }

    // Classify set-operation branches independently, and verify that their exact multisets
    // reproduce the reported UNION ALL. A defect in an unrelated branch must remain visible.
    private KnownVpBug classifyKnownUnionAllBug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!isKnownDatabendVersion() || !hasSupportedKnownEngines(pair) || singleRows.rows.equals(splitRows.rows)) {
            return null;
        }
        List<String> directBranches = knownUnionAllBranches(pair.singleQuery);
        List<String> splitBranches = knownUnionAllBranches(pair.splitQuery);
        if (directBranches.size() != 2 || splitBranches.size() != 2) {
            return null;
        }
        Map<String, Integer> directTotal = new LinkedHashMap<>(), splitTotal = new LinkedHashMap<>();
        KnownVpBug label = null;
        try {
            for (int i = 0; i < 2; i++) {
                VpQueryPair branch = new VpQueryPair(pair.schema, pair.reconstructionStrategy, directBranches.get(i),
                        splitBranches.get(i), List.of(), "known UNION ALL branch", false, false);
                QueryResultMultiset direct = diagnosticRows(branch.singleQuery, branch);
                QueryResultMultiset split = diagnosticRows(branch.splitQuery, branch);
                direct.rows.forEach((row, count) -> directTotal.merge(row, count, Math::addExact));
                split.rows.forEach((row, count) -> splitTotal.merge(row, count, Math::addExact));
                if (!completeReconstructionEvidence(branch)) {
                    return null;
                }
                if (!direct.rows.equals(split.rows)) {
                    KnownVpBug candidate = classifyKnownBug(branch, direct, split);
                    if (candidate == null || label != null && label != candidate) {
                        return null;
                    }
                    label = candidate;
                }
            }
            return directTotal.equals(singleRows.rows) && splitTotal.equals(splitRows.rows) ? label : null;
        } catch (SQLException | IgnoreMeException | ArithmeticException e) {
            return null;
        }
    }

    static List<String> knownUnionAllBranches(String query) {
        int union = topLevelKeyword(query, " UNION ALL ");
        if (union < 0) {
            return List.of();
        }
        List<String> result = new ArrayList<>();
        for (String branch : List.of(query.substring(0, union), query.substring(union + 11))) {
            branch = branch.trim();
            if (!branch.startsWith("(") || !branch.endsWith(")")) {
                return List.of();
            }
            branch = branch.substring(1, branch.length() - 1).trim();
            if (!branch.toUpperCase(Locale.ROOT).startsWith("SELECT ") || topLevelKeyword(branch, " UNION ALL ") >= 0) {
                return List.of();
            }
            result.add(branch);
        }
        return result;
    }

    /** Candidate shapes require plan and independent data/predicate evidence. */
    private KnownVpBug classifyDatabend919Bug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!isKnownDatabendVersion() || singleRows.rows.equals(splitRows.rows)) {
            return null;
        }
        if (matchesCorrelatedInNullFilterLeak(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_CORRELATED_IN_NULL_FILTER_LEAK;
        }
        if (matchesReconstructedNullableSelfFilterLoss(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_NULLABLE_SELF_COMPARISON_FILTER_LOSS;
        }
        if (matchesReconstructedReverseLikeDifference(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_FUSE_EMPTY_PATTERN_LIKE;
        }
        if (isDatabend919TempTopNShape(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_FUSE_TOPN_BLOCK_PRUNING_ROW_SKIP;
        }
        if (pair.reconstructionStrategy == ReconstructionStrategy.TEMP_TABLE
                && "MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)
                && "FUSE".equalsIgnoreCase(pair.schema.leftEngine) && "FUSE".equalsIgnoreCase(pair.schema.rightEngine)
                && matchesTempRowIdTautology(pair.singleQuery) && topLevelKeyword(pair.singleQuery, " ORDER BY ") >= 0
                && topLevelKeyword(pair.singleQuery, " LIMIT ") >= 0
                && hasPlanTokens(pair.splitQuery, "SORT(SINGLE)", "TOPN PRUNING:")
                && completeReconstructionEvidence(pair) && predicateEvaluationEvidence(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_FUSE_TOPN_BLOCK_PRUNING_ROW_SKIP;
        }
        if (isDatabend919RangeDirectionShape(pair) && completeReconstructionEvidence(pair)
                && predicateEvaluationEvidence(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_OUTER_JOIN_RANGE_PREDICATE_DIRECTION;
        }
        return null;
    }

    private boolean matchesCorrelatedInNullFilterLeak(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.IN_FILTER
                || !"MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)
                || !"FUSE".equalsIgnoreCase(pair.schema.leftEngine)
                || !"MEMORY".equalsIgnoreCase(pair.schema.rightEngine)) {
            return false;
        }
        String directPlan = explainPlanText(pair.singleQuery);
        String splitPlan = explainPlanText(pair.splitQuery);
        if (directPlan == null || splitPlan == null
                || !normalizeKnownBugQuery(directPlan).contains("FILTERS: [IS_TRUE(")
                || !hasCorrelatedInJoinPlan(splitPlan)) {
            return false;
        }
        for (VpColumn left : pair.schema.leftColumns) {
            if (!left.nullable || !isFloatingType(left.type)) {
                continue;
            }
            for (VpColumn right : pair.schema.rightColumns) {
                if (right.nullable && isFloatingType(right.type)
                        && crossColumnInPredicate(pair.singleQuery, left.name, right.name)
                        && completeReconstructionEvidence(pair)
                        && correlatedInNullLeakEvidence(pair, left.name, right.name)
                        && predicateEvaluationEvidence(pair, singleRows, splitRows)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean matchesFloatSelfNotInJoinFilterLoss(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!isKnownDatabendVersion()
                || pair.reconstructionStrategy != ReconstructionStrategy.JOIN
                        && pair.reconstructionStrategy != ReconstructionStrategy.STRING_PACK
                        && pair.reconstructionStrategy != ReconstructionStrategy.JSON_PACK
                || !"MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)
                || !"MEMORY".equalsIgnoreCase(pair.schema.leftEngine)
                || !"FUSE".equalsIgnoreCase(pair.schema.rightEngine)
                || !normalizeKnownBugQuery(pair.singleQuery).startsWith("SELECT SUM(")
                || !hasPlanTokens(pair.splitQuery, "HASHJOIN") || !completeReconstructionEvidence(pair)) {
            return false;
        }
        String directPlan = explainPlanText(pair.singleQuery);
        String splitPlan = explainPlanText(pair.splitQuery);
        if (directPlan == null || splitPlan == null) {
            return false;
        }
        for (VpColumn c : pair.schema.leftColumns) {
            if (c.nullable && isFloatingType(c.type) && selfNotInPredicate(pair.singleQuery, c.name)
                    && hasSelfEqualityPlan(directPlan, c.name)
                    && !normalizeKnownBugQuery(splitPlan).contains(c.name.toUpperCase(Locale.ROOT))
                    && predicateEvaluationEvidence(pair, singleRows, splitRows)) {
                return true;
            }
        }
        return false;
    }

    private boolean matchesNullableConjunctiveLikeFilterLoss(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!isKnownDatabendVersion() || pair.reconstructionStrategy != ReconstructionStrategy.STRING_PACK
                || !"FUSE".equalsIgnoreCase(pair.schema.sourceEngine)
                || !"MEMORY".equalsIgnoreCase(pair.schema.leftEngine)
                || !"FUSE".equalsIgnoreCase(pair.schema.rightEngine)
                || !normalizeKnownBugQuery(pair.singleQuery).startsWith("SELECT AVG(")
                || !hasPlanTokens(pair.splitQuery, "HASHJOIN", "FILTERS: [TRUE]")
                || !completeReconstructionEvidence(pair)) {
            return false;
        }
        for (VpColumn column : pair.schema.rightColumns) {
            if (column.nullable && column.type == DatabendDataType.VARCHAR
                    && matchesNegatedConjunctiveLikePredicate(pair.singleQuery, column.name)
                    && hasNullSourceColumnWitness(pair, column.name)
                    && predicateEvaluationEvidence(pair, singleRows, splitRows)) {
                return true;
            }
        }
        return false;
    }

    static boolean matchesNegatedConjunctiveLikePredicate(String query, String column) {
        String predicate = predicateShapeText(query).replaceAll("[A-Z0-9_]+\\.", "");
        String name = Pattern.quote(column.toUpperCase(Locale.ROOT));
        return predicate.matches("NOT " + name + " LIKE '\\?' AND " + name + " LIKE '\\?'");
    }

    private boolean hasNullSourceColumnWitness(VpQueryPair pair, String column) {
        try (Statement statement = state.getConnection().createStatement(); ResultSet result = statement.executeQuery(
                "SELECT COUNT(*) FROM " + quote(pair.schema.sourceTable) + " WHERE " + quote(column) + " IS NULL")) {
            return result.next() && result.getLong(1) > 0;
        } catch (SQLException e) {
            return false;
        }
    }

    static boolean selfNotInPredicate(String query, String column) {
        String name = Pattern.quote(column.toUpperCase(Locale.ROOT));
        String ref = "(?:[A-Z0-9_]+\\.)?" + name;
        return Pattern.compile("\\bNOT\\s+" + ref + "\\s+NOT IN\\s+" + ref + "(?![A-Z0-9_])")
                .matcher(predicateShapeText(query)).find();
    }

    // Current 921/database0 also reproduces this with EXISTS_FILTER and MEMORY/FUSE/MEMORY.
    private boolean matchesCorrelatedScalarFloatZeroNullMatch(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        boolean correlatedIn = pair.reconstructionStrategy == ReconstructionStrategy.IN_FILTER
                && "FUSE".equalsIgnoreCase(pair.schema.sourceEngine) && "FUSE".equalsIgnoreCase(pair.schema.leftEngine)
                && "FUSE".equalsIgnoreCase(pair.schema.rightEngine);
        boolean correlatedExists = pair.reconstructionStrategy == ReconstructionStrategy.EXISTS_FILTER
                && "MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)
                && "FUSE".equalsIgnoreCase(pair.schema.leftEngine)
                && "MEMORY".equalsIgnoreCase(pair.schema.rightEngine);
        if (!isKnownDatabendVersion() || singleRows.rows.equals(splitRows.rows) || !(correlatedIn || correlatedExists)
                || topLevelKeyword(pair.singleQuery, " WHERE ") < 0 || !completeReconstructionEvidence(pair)) {
            return false;
        }
        String directPlan = explainPlanText(pair.singleQuery);
        String splitPlan = explainPlanText(pair.splitQuery);
        if (directPlan == null || splitPlan == null
                || (correlatedIn && (!normalizeKnownBugQuery(directPlan).contains("IS_TRUE(CAST(")
                        || !hasCorrelatedInJoinPlan(splitPlan)))
                || (correlatedExists && !hasScalarEqualityJoinPlan(directPlan, splitPlan))) {
            return false;
        }
        for (VpColumn left : pair.schema.leftColumns) {
            for (VpColumn right : pair.schema.rightColumns) {
                if (left.nullable && isFloatingType(left.type) && isFloatingType(right.type)
                        && (crossColumnEqualityPredicate(pair.singleQuery, right.name, left.name)
                                || crossColumnEqualityPredicate(pair.singleQuery, left.name, right.name))
                        && normalizeKnownBugQuery(pair.splitQuery)
                                .contains("SELECT R." + right.name.toUpperCase(Locale.ROOT))
                        && hasNullZeroFloatWitness(pair, left.name, right.name)
                        && predicateEvaluationEvidence(pair, singleRows, splitRows)) {
                    return true;
                }
            }
        }
        return false;
    }

    // EXISTS decorrelation moves the nullable equality into scalar-subquery INNER join keys.
    static boolean hasScalarEqualityJoinPlan(String directPlan, String splitPlan) {
        String direct = normalizeKnownBugQuery(directPlan);
        String split = normalizeKnownBugQuery(splitPlan);
        return direct.contains("IS_TRUE(") && direct.contains(" = ") && split.contains("HASHJOIN")
                && split.contains("JOIN TYPE: LEFT SINGLE") && split.contains("JOIN TYPE: INNER")
                && split.contains("BUILD KEYS: [SCALAR_SUBQUERY_") && !split.contains("IS_TRUE(");
    }

    static boolean crossColumnEqualityPredicate(String query, String right, String left) {
        String r = "(?:[A-Z0-9_]+\\.)?" + Pattern.quote(right.toUpperCase(Locale.ROOT));
        String l = "(?:[A-Z0-9_]+\\.)?" + Pattern.quote(left.toUpperCase(Locale.ROOT));
        return Pattern.compile("(?<![A-Z0-9_])" + r + "\\s*=\\s*" + l + "(?![A-Z0-9_])")
                .matcher(predicateShapeText(query)).find();
    }

    private boolean hasNullZeroFloatWitness(VpQueryPair pair, String left, String right) {
        String query = "SELECT COUNT(*) FROM " + quote(pair.schema.sourceTable) + " WHERE " + quote(left)
                + " IS NULL AND " + quote(right) + " = 0";
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(query)) {
            return result.next() && result.getLong(1) > 0;
        } catch (SQLException e) {
            return false;
        }
    }

    private static boolean isFloatingType(DatabendDataType type) {
        return type == DatabendDataType.FLOAT || type == DatabendDataType.FLOATING;
    }

    static boolean crossColumnInPredicate(String query, String leftColumn, String rightColumn) {
        String left = "(?:[A-Z0-9_]+\\.)?" + Pattern.quote(leftColumn.toUpperCase(Locale.ROOT));
        String right = "(?:[A-Z0-9_]+\\.)?" + Pattern.quote(rightColumn.toUpperCase(Locale.ROOT));
        return predicateShapeText(query).matches(left + "\\s+IN\\s+" + right);
    }

    static boolean hasCorrelatedInJoinPlan(String plan) {
        String normalized = normalizeKnownBugQuery(plan);
        return normalized.contains("HASHJOIN") && normalized.contains("JOIN TYPE: INNER")
                && normalized.contains("BUILD KEYS: [SCALAR_SUBQUERY_") && normalized.contains("VP_ROWID")
                && normalized.contains("APPLY JOIN FILTERS:");
    }

    private boolean correlatedInNullLeakEvidence(VpQueryPair pair, String leftColumn, String rightColumn) {
        int from = topLevelKeyword(pair.splitQuery, " FROM ");
        int where = topLevelKeyword(pair.splitQuery, " WHERE ");
        if (from < 0 || where < 0) {
            return false;
        }
        String relation = pair.splitQuery.substring(from + 6, where);
        String splitPredicate = pair.splitQuery.substring(where + 7, predicateEnd(pair.splitQuery, where + 7));
        String sourcePredicate = quote(leftColumn) + " IN (" + quote(rightColumn) + ")";
        java.util.Set<Long> expected = new java.util.LinkedHashSet<>();
        java.util.Set<Long> nullPredicate = new java.util.LinkedHashSet<>();
        java.util.Set<Long> actual = new java.util.LinkedHashSet<>();
        try (Statement statement = state.getConnection().createStatement(); ResultSet source = statement.executeQuery(
                "SELECT " + quote(ROW_ID) + ", (" + sourcePredicate + ") FROM " + quote(pair.schema.sourceTable))) {
            while (source.next()) {
                long id = source.getLong(1);
                boolean selected = source.getBoolean(2);
                if (source.wasNull()) {
                    nullPredicate.add(id);
                } else if (selected) {
                    expected.add(id);
                }
            }
        } catch (SQLException e) {
            return false;
        }
        try (Statement statement = state.getConnection().createStatement(); ResultSet reconstructed = statement
                .executeQuery("SELECT " + quote(ROW_ID) + " FROM " + relation + " WHERE " + splitPredicate)) {
            while (reconstructed.next()) {
                if (!actual.add(reconstructed.getLong(1))) {
                    return false;
                }
            }
        } catch (SQLException e) {
            return false;
        }
        java.util.Set<Long> leaked = new java.util.LinkedHashSet<>(actual);
        leaked.removeAll(expected);
        return actual.containsAll(expected) && !leaked.isEmpty() && nullPredicate.containsAll(leaked);
    }

    private boolean hasSupportedKnownEngines(VpQueryPair pair) {
        return java.util.stream.Stream.of(pair.schema.sourceEngine, pair.schema.leftEngine, pair.schema.rightEngine)
                .allMatch(e -> "MEMORY".equalsIgnoreCase(e) || "FUSE".equalsIgnoreCase(e));
    }

    private boolean matchesReconstructedReverseLikeDifference(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!hasSupportedKnownEngines(pair)) {
            return false;
        }
        boolean candidate = pair.schema.columns.stream()
                .anyMatch(c -> c.type == DatabendDataType.VARCHAR && hasReverseLikePredicate(pair.singleQuery, c.name));
        return candidate && hasPlanTokens(pair.singleQuery, "TABLESCAN", "LIKE(")
                && hasPlanTokens(pair.splitQuery, "TABLESCAN", "LIKE(") && completeReconstructionEvidence(pair)
                && predicateEvaluationEvidence(pair, singleRows, splitRows);
    }

    private boolean matchesReconstructedNullableSelfFilterLoss(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!hasSupportedKnownEngines(pair)) {
            return false;
        }
        String directPlan = explainPlanText(pair.singleQuery);
        String splitPlan = explainPlanText(pair.splitQuery);
        if (directPlan == null || splitPlan == null || !splitPlan.contains("HASHJOIN")) {
            return false;
        }
        for (VpColumn column : pair.schema.columns) {
            if (column.nullable
                    && (column.type == DatabendDataType.DATE || column.type == DatabendDataType.INT
                            || column.type == DatabendDataType.TIMESTAMP || column.type == DatabendDataType.FLOAT
                            || column.type == DatabendDataType.DECIMAL || column.type == DatabendDataType.BOOLEAN
                            || column.type == DatabendDataType.VARCHAR)
                    && hasNullableSelfPredicate(pair.singleQuery, column.name)
                    && hasSelfEqualityPlan(directPlan, column.name) && !hasSelfEqualityPlan(splitPlan, column.name)
                    && completeReconstructionEvidence(pair)
                    && predicateEvaluationEvidence(pair, singleRows, splitRows)) {
                return true;
            }
        }
        return false;
    }

    // Only shape recognition discards parentheses. Replace complete string literals with markers first,
    // so parentheses, LIKE/IN text, newlines and escaped quotes inside a literal cannot nominate a bug.
    private static String predicateShapeText(String query) {
        int where = topLevelKeyword(query, " WHERE ");
        if (where < 0) {
            return "";
        }
        String predicate = query.substring(where + 7, predicateEnd(query, where + 7));
        StringBuilder shape = new StringBuilder();
        for (int i = 0; i < predicate.length(); i++) {
            char ch = predicate.charAt(i);
            if (ch == '\'') {
                int start = i++;
                while (i < predicate.length()) {
                    if (predicate.charAt(i) == '\\') {
                        i += 2;
                        continue;
                    }
                    if (predicate.charAt(i) == '\'') {
                        if (i + 1 < predicate.length() && predicate.charAt(i + 1) == '\'') {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i++;
                }
                shape.append(i == start + 1 ? "''" : "'?' ");
            } else if (ch == '(' || ch == ')') {
                shape.append(' ');
            } else if (ch != '`') {
                shape.append(ch);
            }
        }
        return shape.toString().toUpperCase(Locale.ROOT).replaceAll("\\s+", " ").trim();
    }

    static boolean hasReverseLikePredicate(String query, String column) {
        String ref = "(?:[A-Z0-9_]+\\.)?" + Pattern.quote(column.toUpperCase(Locale.ROOT));
        return Pattern.compile("(?:''|'\\?')\\s+LIKE\\s+" + ref + "(?![A-Z0-9_])").matcher(predicateShapeText(query))
                .find();
    }

    static boolean hasNullableSelfPredicate(String query, String column) {
        String ref = "(?:[A-Z0-9_]+\\.)?" + Pattern.quote(column.toUpperCase(Locale.ROOT));
        String self = "(" + ref + ")";
        return Pattern.compile("(?<![A-Z0-9_.])" + self + "\\s*=\\s*\\1(?![A-Z0-9_])")
                .matcher(predicateShapeText(query)).find()
                || Pattern
                        .compile("(?<![A-Z0-9_.])" + self + "\\s+(?:NOT\\s+)?IN\\s+\\1"
                                + "(?:\\s*,\\s*(?:\\1|NULL))*(?=\\s+(?:AND|OR|$)|$)")
                        .matcher(predicateShapeText(query)).find();
    }

    static boolean hasSelfEqualityPlan(String plan, String column) {
        String ref = "(?:[A-Z0-9_]+\\.)?" + Pattern.quote(column.toUpperCase(Locale.ROOT));
        String normalized = plan.toUpperCase(Locale.ROOT).replaceAll("\\s*\\(#[0-9]+\\)", "");
        return Pattern.compile("(?<![A-Z0-9_.])(" + ref + ")\\s*=\\s*\\1(?![A-Z0-9_])").matcher(normalized).find();
    }

    private boolean isDatabend919TempTopNShape(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.TEMP_TABLE
                || !"MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)
                || !"FUSE".equalsIgnoreCase(pair.schema.leftEngine)
                || !"MEMORY".equalsIgnoreCase(pair.schema.rightEngine)
                || !pair.singleQuery.toUpperCase(Locale.ROOT).contains("ORDER BY")
                || !pair.singleQuery.toUpperCase(Locale.ROOT).contains("LIMIT")
                || !pair.singleQuery.toUpperCase(Locale.ROOT).contains("VP_ROWID")
                || !hasPlanTokens(pair.splitQuery, "TOPN PRUNING:")) {
            return false;
        }
        return !singleRows.rows.equals(splitRows.rows);
    }

    static boolean matchesTempRowIdTautology(String query) {
        String ref = "((?:[A-Z0-9_]+\\.)?VP_ROWID)";
        return Pattern.compile(ref + " \\+ \\1 NOT IN \\1 \\+ \\1 IS NOT NULL").matcher(predicateShapeText(query))
                .matches();
    }

    private boolean isDatabend919RangeDirectionShape(VpQueryPair pair) {
        if ((pair.reconstructionStrategy != ReconstructionStrategy.LEFT_RIGHT_JOIN
                && pair.reconstructionStrategy != ReconstructionStrategy.FULL_OUTER_SIMULATION)
                || !hasSupportedKnownEngines(pair) || !"FUSE".equalsIgnoreCase(pair.schema.leftEngine)) {
            return false;
        }
        String directPlan = explainPlanText(pair.singleQuery);
        String splitPlan = explainPlanText(pair.splitQuery);
        return hasNegatedLiteralComparisonDirectionEvidence(pair.singleQuery, directPlan, splitPlan, ROW_ID,
                pair.schema.leftTable)
                || hasRangeDirectionPlanEvidence(pair.singleQuery, directPlan, splitPlan, "vp_rowid",
                        pair.schema.leftTable)
                || pair.reconstructionStrategy == ReconstructionStrategy.FULL_OUTER_SIMULATION
                        && "FUSE".equalsIgnoreCase(pair.schema.rightEngine) && hasRangeDirectionPlanEvidence(
                                pair.singleQuery, directPlan, splitPlan, "vp_rowid", pair.schema.rightTable);
    }

    /** databend921/database3: NOT (negative literal > id) becomes NOT (id > literal) on the FUSE probe. */
    static boolean hasNegatedLiteralComparisonDirectionEvidence(String query, String directPlan, String splitPlan,
            String column, String leftTable) {
        String predicate = predicateShapeText(query).replaceAll("[A-Z0-9_]+\\.", "");
        String key = Pattern.quote(column.toUpperCase(Locale.ROOT));
        java.util.regex.Matcher literal = Pattern.compile("NOT\\s+\\+?\\s*(-[0-9]+)\\s*>\\s*" + key + "(?![A-Z0-9_])")
                .matcher(predicate);
        if (!literal.matches() || directPlan == null || splitPlan == null) {
            return false;
        }
        String direct = normalizeKnownBugQuery(directPlan).replaceAll("\\s*\\(#[0-9]+\\)", "");
        String split = normalizeKnownBugQuery(splitPlan).replaceAll("\\s*\\(#[0-9]+\\)", "");
        String number = Pattern.quote(literal.group(1));
        String probe = Pattern.quote(leftTable.toUpperCase(Locale.ROOT)) + "\\." + key;
        return Pattern.compile("NOT\\s+" + number + "\\s*>\\s*(?:[A-Z0-9_]+\\.)?" + key).matcher(direct).find()
                && Pattern.compile("NOT\\s+" + probe + "\\s*>\\s*" + number + "(?![0-9])").matcher(split).find()
                && split.contains("HASHJOIN") && split.contains("RANGE PRUNING: 1 TO 0");
    }

    // Compare the same inequality operator and literal on the source and FUSE probe scans.
    // A correct exchange of operands must also invert the operator, so it cannot match this rule.
    static boolean hasRangeDirectionPlanEvidence(String query, String directPlan, String splitPlan, String column,
            String leftTable) {
        if (directPlan == null || splitPlan == null || !splitPlan.toUpperCase(Locale.ROOT).contains("HASHJOIN")) {
            return false;
        }
        String predicate = predicateShapeText(query);
        String ref = "(?:[A-Z0-9_]+\\.)?" + Pattern.quote(column.toUpperCase(Locale.ROOT));
        boolean literalBounds = Pattern
                .compile("-?[0-9]+ (?:NOT )?BETWEEN (?:" + ref + " AND -?[0-9]+|-?[0-9]+ AND " + ref + ")")
                .matcher(predicate).find();
        // 920/database11 has an arithmetic constant; current 921/database3 also negates the column lower bound.
        // The actual plans below must still demonstrate the incorrect operand exchange.
        boolean columnLowerBound = Pattern
                .compile("NOT BETWEEN (?:\\+?\\s*-\\s*)?[A-Z0-9_.]+ AND \\+?\\s*" + ref + "(?![A-Z0-9_])")
                .matcher(predicate).find();
        if (!literalBounds && !columnLowerBound) {
            return false;
        }
        String direct = normalizeKnownBugQuery(directPlan).replaceAll("\\s*\\(#[0-9]+\\)", "");
        String split = normalizeKnownBugQuery(splitPlan).replaceAll("\\s*\\(#[0-9]+\\)", "");
        java.util.regex.Matcher comparison = Pattern
                .compile("(?<![A-Z0-9_])-?[0-9]+\\s*(>=|>|<=|<)\\s*" + ref + "(?![A-Z0-9_])").matcher(direct);
        while (comparison.find()) {
            String literal = comparison.group().split("[<>]")[0].trim();
            String probe = Pattern.quote(leftTable.toUpperCase(Locale.ROOT)) + "\\."
                    + Pattern.quote(column.toUpperCase(Locale.ROOT));
            if (Pattern.compile(
                    probe + "\\s*" + Pattern.quote(comparison.group(1)) + "\\s*" + Pattern.quote(literal) + "(?![0-9])")
                    .matcher(split).find()) {
                return true;
            }
        }
        return false;
    }

    static boolean matchesCorrelatedExistsSignedNotInError(String message, String rightTable, String column) {
        String error = message.toLowerCase(Locale.ROOT);
        return error.contains("semanticerror") && error.contains("code: 1065")
                && error.contains("cannot access the \"" + rightTable.toLowerCase(Locale.ROOT) + "\".\""
                        + column.toLowerCase(Locale.ROOT) + "\" in anti or semi join");
    }

    static boolean matchesCorrelatedExistsSignedNotIn(String query, String column, String leftTable,
            String rightTable) {
        String sql = normalizeKnownBugQuery(query);
        String key = Pattern.quote(column.toUpperCase(Locale.ROOT));
        String right = Pattern.quote(rightTable.toUpperCase(Locale.ROOT));
        String lookup = "SELECT R\\.[A-Z0-9_]+ FROM " + right + " R WHERE R\\." + key + "\\s*=\\s*L\\." + key;
        String exists = "EXISTS\\s*\\(SELECT 1 FROM " + right + " R WHERE R\\." + key + "\\s*=\\s*L\\." + key + "\\)";
        if (!Pattern.compile(lookup).matcher(sql).find() || !Pattern.compile(exists).matcher(sql).find()
                || !sql.contains("FROM " + leftTable.toUpperCase(Locale.ROOT) + " L")) {
            return false;
        }
        String predicate = predicateShapeText(query).replaceAll("[A-Z0-9_]+\\.", "").replaceAll("\\s+", " ").trim();
        boolean signedEquality = Pattern.compile("-\\s*" + key + "\\s*=\\s*\\+?\\s*" + key + "(?![A-Z0-9_])")
                .matcher(predicate).find();
        java.util.regex.Matcher in = Pattern.compile("NOT IN (.*?)(?: AND |$)").matcher(predicate);
        return signedEquality && in.find()
                && Pattern.compile("(?<![-+A-Z0-9_])" + key + "(?![A-Z0-9_])").matcher(in.group(1)).find()
                && Pattern.compile("-\\s*" + key + "(?![A-Z0-9_])").matcher(in.group(1)).find();
    }

    private boolean matchesNullableSelfComparisonFilterLoss(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!isKnownDatabendVersion() || singleRows.rows.equals(splitRows.rows)) {
            return false;
        }
        for (VpColumn column : pair.schema.columns) {
            if (!column.nullable) {
                continue;
            }
            int shape = nullableSelfComparisonShape(pair.singleQuery, column.name);
            boolean engines = shape == 1 && column.type == DatabendDataType.BOOLEAN
                    && pair.reconstructionStrategy == ReconstructionStrategy.JOIN
                    && "FUSE".equalsIgnoreCase(pair.schema.sourceEngine)
                    && "FUSE".equalsIgnoreCase(pair.schema.leftEngine)
                    && "MEMORY".equalsIgnoreCase(pair.schema.rightEngine)
                    || shape == 2 && column.type == DatabendDataType.DATE
                            && pair.reconstructionStrategy == ReconstructionStrategy.JOIN
                            && "MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)
                            && "FUSE".equalsIgnoreCase(pair.schema.leftEngine)
                            && "FUSE".equalsIgnoreCase(pair.schema.rightEngine)
                    || shape == 3 && column.type == DatabendDataType.TIMESTAMP
                            && pair.reconstructionStrategy == ReconstructionStrategy.FULL_OUTER_SIMULATION
                            && "MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)
                            && "MEMORY".equalsIgnoreCase(pair.schema.leftEngine)
                            && "FUSE".equalsIgnoreCase(pair.schema.rightEngine)
                    || shape == 4 && column.type == DatabendDataType.DATE
                            && pair.reconstructionStrategy == ReconstructionStrategy.ROW_NUMBER_ALIGNMENT
                            && "MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)
                            && "FUSE".equalsIgnoreCase(pair.schema.leftEngine)
                            && "MEMORY".equalsIgnoreCase(pair.schema.rightEngine)
                            && containsColumn(pair.schema.leftColumns, column.name);
            if (!engines) {
                continue;
            }
            String singlePlan = explainPlanText(pair.singleQuery).toUpperCase(Locale.ROOT);
            String splitPlan = explainPlanText(pair.splitQuery).toUpperCase(Locale.ROOT);
            String name = column.name.toUpperCase(Locale.ROOT);
            // The predicate column must survive in the direct plan, but be entirely pruned from
            // the reconstructed plan. This also rejects retained IS NOT NULL/self-equality filters.
            if (singlePlan.contains(name) && singlePlan.contains(" = ") && splitPlan.contains("HASHJOIN")
                    && (shape == 3 ? !splitPlan.contains("IS_TRUE(") && !splitPlan.contains(" = ")
                            : !splitPlan.contains(name))
                    && (shape != 3 || splitPlan.contains("UNIONALL"))
                    && (shape != 4 || splitPlan.contains("WINDOW") && splitPlan.contains("ROW_NUMBER"))
                    && predicateEvaluationEvidence(pair, singleRows, splitRows)) {
                return true;
            }
        }
        return false;
    }

    static int nullableSelfComparisonShape(String query, String column) {
        int where = topLevelKeyword(query, " WHERE ");
        if (where < 0) {
            return 0;
        }
        String predicate = normalizeKnownBugQuery(query.substring(where + 7, predicateEnd(query, where + 7)))
                .replace("(", " ").replace(")", " ").replaceAll("\\s+", " ").trim();
        String ref = "(?:[A-Z0-9_]+\\.)?" + Pattern.quote(column.toUpperCase(Locale.ROOT));
        if (predicate.matches("([A-Z0-9_.]+) IS NULL <> ([A-Z0-9_.]+) IS NULL AND NOT NOT (" + ref + ") NOT IN NOT \\3")
                && normalizeKnownBugQuery(query).startsWith("SELECT AVG(")) {
            return 1;
        }
        if (predicate
                .matches("(" + ref
                        + ")=\\1 OR NOT TRUE AND DATE '1970-01-20' NOT BETWEEN DATE '1969-12-09' AND DATE '1970-01-02'")
                || predicate.matches("(" + ref
                        + ") = \\1 OR NOT TRUE AND DATE '1970-01-20' NOT BETWEEN DATE '1969-12-09' AND DATE '1970-01-02'")) {
            return 2;
        }
        if (predicate.matches("(" + ref + ") IN \\1, \\1, \\1")) {
            return 3;
        }
        if (isRowIdStddevQuery(query) && predicate.matches("NOT (" + ref + ") NOT IN \\1")) {
            return 4;
        }
        return 0;
    }

    static boolean isRowIdStddevQuery(String query) {
        return normalizeKnownBugQuery(query)
                .matches("SELECT STDDEV_POP\\((?:[A-Z0-9_]+\\.)?VP_ROWID\\) FROM .* WHERE .*?(?: LIMIT [0-9]+)?;?")
                && topLevelKeyword(query, " GROUP BY ") < 0 && topLevelKeyword(query, " HAVING ") < 0;
    }

    private KnownVpBug classifyDatabend917Bug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!isKnownDatabendVersion() || singleRows.rows.equals(splitRows.rows)) {
            return null;
        }
        String single = normalizeKnownBugQuery(pair.singleQuery);
        String split = normalizeKnownBugQuery(pair.splitQuery);
        String shape = databend917Shape(pair.singleQuery);
        if (shape == null) {
            return null;
        }
        if (shape.equals("COUNT")) {
            for (VpColumn column : pair.schema.columns) {
                if (column.nullable && (column.type == DatabendDataType.MAP || column.type == DatabendDataType.ARRAY)
                        && single.contains("COUNT(" + pair.schema.sourceTable.toUpperCase(Locale.ROOT) + "."
                                + column.name.toUpperCase(Locale.ROOT) + ")")
                        && ("FUSE".equalsIgnoreCase(pair.schema.sourceEngine)
                                || "MEMORY".equalsIgnoreCase(pair.schema.sourceEngine))
                        && (hasPlanTokens(pair.splitQuery, "DUMMYTABLESCAN", "EXPRESSIONS:")
                                || hasPlanTokens(pair.splitQuery, "AGGREGATEFINAL", "COUNT("))
                        && complexCountEvidence(pair, column, singleRows, splitRows)) {
                    return KnownVpBug.DATABEND_NULLABLE_COMPLEX_COUNT_METADATA;
                }
            }
            return null;
        }
        if (shape.equals("DATE_TOPN") || shape.equals("GEOMETRY_TOPN")) {
            DatabendDataType type = shape.equals("DATE_TOPN") ? DatabendDataType.DATE : DatabendDataType.GEOMETRY;
            if (pair.reconstructionStrategy != ReconstructionStrategy.TEMP_TABLE
                    || !"MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)
                    || !"FUSE".equalsIgnoreCase(pair.schema.leftEngine)
                    || !"MEMORY".equalsIgnoreCase(pair.schema.rightEngine)
                    || pair.schema.columns.stream().noneMatch(c -> c.type == type && c.nullable)
                    || !hasPlanTokens(pair.splitQuery, "SORT(SINGLE)", "TOPN PRUNING:")
                    || !hasReducedTopNPruning(pair.splitQuery)
                    || !sameCompleteRowIds(pair.schema.sourceTable, pair.schema.tempTable)
                    || !completeOrderedProjectionEvidence(pair, singleRows)) {
                return null;
            }
            long[] topN = parseVpRowIdTopNShape(pair.singleQuery);
            if (topN == null) {
                return null;
            }
            if (type == DatabendDataType.GEOMETRY
                    && !sameTopNRowIds(pair.schema.sourceTable, pair.schema.tempTable, topN)) {
                return KnownVpBug.DATABEND_TEMP_GEOMETRY_TOPN;
            }
            if (type == DatabendDataType.DATE && pair.schema.columns.size() == 3
                    && pair.schema.leftColumns.stream().anyMatch(c -> c.type == DatabendDataType.DECIMAL)
                    && pair.schema.leftColumns.stream().anyMatch(c -> c.type == DatabendDataType.FLOAT)
                    && pair.schema.rightColumns.size() == 1
                    && pair.schema.rightColumns.get(0).type == DatabendDataType.DATE
                    && hasPlanTokens(pair.splitQuery, "ROWFETCH", "COLUMNS TO FETCH:")) {
                return KnownVpBug.DATABEND_TEMP_DATE_ORDER_LIMIT;
            }
            return null;
        }
        boolean memoryPredicate = "FUSE".equalsIgnoreCase(pair.schema.sourceEngine) && predicateColumnOnEngine(pair,
                "MEMORY", shape.equals("SELF_IN") ? DatabendDataType.TIMESTAMP : DatabendDataType.VARCHAR);
        boolean fusePredicate = "MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)
                && predicateColumnOnEngine(pair, "FUSE", DatabendDataType.VARCHAR)
                || "FUSE".equalsIgnoreCase(pair.schema.sourceEngine)
                        && predicateColumnOnEngine(pair, "MEMORY", DatabendDataType.VARCHAR);
        if (shape.equals("OUTER_AVG")) {
            if (pair.reconstructionStrategy == ReconstructionStrategy.LEFT_RIGHT_JOIN
                    && "MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)
                    && "FUSE".equalsIgnoreCase(pair.schema.leftEngine)
                    && "FUSE".equalsIgnoreCase(pair.schema.rightEngine)
                    && pair.schema.columns.stream().anyMatch(c -> c.type == DatabendDataType.DATE && c.nullable)
                    && hasPlanTokens(pair.splitQuery, "RIGHT OUTER", "LIKE(")
                    && predicateEvaluationEvidence(pair, singleRows, splitRows)) {
                return KnownVpBug.DATABEND_OUTER_JOIN_LIKE_NULL_AVG;
            }
        } else if ((shape.equals("EMPTY_LIKE") || shape.equals("SELF_IN")) && memoryPredicate
                && (pair.reconstructionStrategy == ReconstructionStrategy.JSON_PACK
                        || pair.reconstructionStrategy == ReconstructionStrategy.STRING_PACK
                        || pair.reconstructionStrategy == ReconstructionStrategy.VIEW
                        || pair.reconstructionStrategy == ReconstructionStrategy.EXISTS_FILTER
                        || pair.reconstructionStrategy == ReconstructionStrategy.SCALAR_SUBQUERY
                        || shape.equals("EMPTY_LIKE") && pair.reconstructionStrategy == ReconstructionStrategy.JOIN)
                && hasPlanTokens(pair.splitQuery, "TABLESCAN", "HASHJOIN")
                && predicateEvaluationEvidence(pair, singleRows, splitRows)) {
            if (shape.equals("EMPTY_LIKE") && pair.reconstructionStrategy == ReconstructionStrategy.EXISTS_FILTER) {
                return KnownVpBug.DATABEND_FUSE_EMPTY_PATTERN_LIKE;
            }
            return KnownVpBug.DATABEND_MEMORY_PREDICATE_FILTER;
        } else if (shape.equals("NONEMPTY_LIKE") && fusePredicate
                && (pair.reconstructionStrategy == ReconstructionStrategy.STRING_PACK
                        || pair.reconstructionStrategy == ReconstructionStrategy.EXISTS_FILTER
                        || pair.reconstructionStrategy == ReconstructionStrategy.DERIVED_TABLE
                        || pair.reconstructionStrategy == ReconstructionStrategy.NATURAL_OR_USING
                        || pair.reconstructionStrategy == ReconstructionStrategy.SCALAR_SUBQUERY
                        || pair.reconstructionStrategy == ReconstructionStrategy.LEFT_RIGHT_JOIN
                                && single.startsWith("SELECT MIN(") && single.contains(" GROUP BY ")
                                && "MEMORY".equalsIgnoreCase(pair.schema.rightEngine)
                                && hasPlanTokens(pair.splitQuery, "RIGHT OUTER", "LIKE("))
                && (single.contains(" ORDER BY ") && split.contains(" ORDER BY ")
                        || pair.reconstructionStrategy == ReconstructionStrategy.EXISTS_FILTER
                                && isRowIdStddevQuery(pair.singleQuery)
                                && hasPlanTokens(pair.splitQuery, "TABLESCAN", "HASHJOIN", "LIKE("))
                && hasEmptyPatternValue(pair) && predicateEvaluationEvidence(pair, singleRows, splitRows)) {
            return KnownVpBug.DATABEND_FUSE_EMPTY_PATTERN_LIKE;
        }
        return null;
    }

    // Shape gates only nominate a candidate. Engine/type/plan gates and independent result evidence above
    // are mandatory before suppression. Strip redundant parentheses only for these bounded shape checks.
    static String databend917Shape(String query) {
        String sql = normalizeKnownBugQuery(query).trim().replaceAll(";$", "");
        if (sql.matches("SELECT COUNT\\([A-Z0-9_.]+\\) FROM .*") && !sql.contains(" GROUP BY ")
                && !sql.contains(" HAVING ")) {
            return "COUNT";
        }
        if (!sql.contains(" WHERE ") && sql.matches(".* ORDER BY [A-Z0-9_.]*VP_ROWID LIMIT [0-9]+")) {
            if (sql.startsWith("SELECT HEX(ST_ASWKB(")) {
                return "GEOMETRY_TOPN";
            }
            if (sql.matches("SELECT [A-Z0-9_.]*C[0-9]+DATE FROM .*")) {
                return "DATE_TOPN";
            }
        }
        int where = topLevelKeyword(query, " WHERE ");
        if (where < 0) {
            return null;
        }
        String predicate = normalizeKnownBugQuery(query.substring(where + 7, predicateEnd(query, where + 7)))
                .replace("(", " ").replace(")", " ").replaceAll("\\s+", " ").trim();
        if (sql.startsWith("SELECT AVG(") && sql.contains(" GROUP BY ")
                && predicate.matches("[A-Z0-9_.]+ IS NULL <> [A-Z0-9_.]+ IS NULL AND '[^']+' LIKE [A-Z0-9_.]+")) {
            return "OUTER_AVG";
        }
        if (predicate.matches("[A-Z0-9_.]+ IN [A-Z0-9_.]+ OR '[^']*' LIKE '[^']*'")) {
            String[] terms = predicate.split(" ");
            return terms[0].equals(terms[2]) ? "SELF_IN" : null;
        }
        if (predicate.equals("'' LIKE " + predicate.substring(predicate.lastIndexOf(' ') + 1))
                || predicate.matches("[A-Z0-9_.]+ IS NULL <> [A-Z0-9_.]+ IS NULL AND '' LIKE [A-Z0-9_.]+")
                || predicate.matches(
                        "[A-Z0-9_.]+ IS NULL <> [A-Z0-9_.]+ IS NULL AND '' LIKE ([A-Z0-9_.]+) AND '' LIKE \\1")) {
            return "EMPTY_LIKE";
        }
        if (predicate.matches("(?:NOT )?'[^']+' LIKE [A-Z0-9_.]+")) {
            return "NONEMPTY_LIKE";
        }
        if (predicate.matches(".*'[^']+' LIKE [A-Z0-9_.]+.*")) {
            return "NONEMPTY_LIKE";
        }
        if (predicate.contains(" LIKE ") && predicate.matches(".* LIKE [A-Z0-9_.]+$")) {
            return "NONEMPTY_LIKE";
        }
        return null;
    }

    private boolean predicateColumnOnEngine(VpQueryPair pair, String engine, DatabendDataType type) {
        String sql = normalizeKnownBugQuery(pair.singleQuery);
        int where = sql.indexOf(" WHERE ");
        String predicate = where < 0 ? "" : sql.substring(where);
        return pair.schema.columns.stream().anyMatch(c -> c.type == type
                && predicate.contains("." + c.name.toUpperCase(Locale.ROOT))
                && (engine.equalsIgnoreCase(pair.schema.leftEngine) && containsColumn(pair.schema.leftColumns, c.name)
                        || engine.equalsIgnoreCase(pair.schema.rightEngine)
                                && containsColumn(pair.schema.rightColumns, c.name)));
    }

    private boolean complexCountEvidence(VpQueryPair pair, VpColumn column, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        long total;
        long nonNull;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery("SELECT COUNT(*), COUNT_IF(" + quote(column.name)
                        + " IS NOT NULL) FROM " + quote(pair.schema.sourceTable))) {
            if (!result.next()) {
                return false;
            }
            total = result.getLong(1);
            nonNull = result.getLong(2);
        } catch (SQLException e) {
            return false;
        }
        return nonNull > 0 && nonNull < total && singleRows.rows.size() == 1 && splitRows.rows.size() == 1
                && !singleRows.rows.equals(splitRows.rows);
    }

    private boolean hasEmptyPatternValue(VpQueryPair pair) {
        for (VpColumn column : pair.schema.columns) {
            if (column.type != DatabendDataType.VARCHAR
                    || !normalizeKnownBugQuery(pair.singleQuery).contains("." + column.name.toUpperCase(Locale.ROOT))) {
                continue;
            }
            try (Statement statement = state.getConnection().createStatement(); ResultSet result = statement
                    .executeQuery("SELECT " + quote(column.name) + " FROM " + quote(pair.schema.sourceTable))) {
                while (result.next()) {
                    if ("".equals(result.getString(1))) {
                        return true;
                    }
                }
            } catch (SQLException e) {
                return false;
            }
        }
        return false;
    }

    private boolean predicateEvaluationEvidence(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        int where = topLevelKeyword(pair.singleQuery, " WHERE ");
        if (where < 0) {
            return false;
        }
        String predicate = pair.singleQuery.substring(where + 7, predicateEnd(pair.singleQuery, where + 7));
        List<String> selectedIds = new ArrayList<>();
        // Project the predicate on every source row: no scan filter is eligible for pushdown.
        String query = "SELECT " + quote(ROW_ID) + ", (" + predicate + ") FROM " + quote(pair.schema.sourceTable);
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(query)) {
            while (result.next()) {
                if (result.getBoolean(2) && !result.wasNull()) {
                    selectedIds.add(Long.toString(result.getLong(1)));
                }
            }
            String replacement = selectedIds.isEmpty() ? "FALSE"
                    : quote(ROW_ID) + " IN (" + String.join(",", selectedIds) + ")";
            QueryResultMultiset direct = diagnosticRows(replaceOuterPredicate(pair.singleQuery, replacement), pair);
            QueryResultMultiset reconstructed = diagnosticRows(replaceOuterPredicate(pair.splitQuery, replacement),
                    pair);
            return predicateEvidenceResultsEqual(populationDiagnosticQuery(pair), direct.rows, reconstructed.rows)
                    && (predicateEvidenceResultsEqual(populationDiagnosticQuery(pair), direct.rows, singleRows.rows)
                            ^ predicateEvidenceResultsEqual(populationDiagnosticQuery(pair), direct.rows,
                                    splitRows.rows));
        } catch (SQLException | IgnoreMeException e) {
            return false;
        }
    }

    private String populationDiagnosticQuery(VpQueryPair pair) {
        String query = pair.singleQuery;
        for (VpColumn column : pair.schema.columns) {
            if (column.type != DatabendDataType.INT) {
                continue;
            }
            String ref = "(?:`?[A-Za-z0-9_]+`?\\.)?`?" + Pattern.quote(column.name) + "`?";
            Pattern covariance = Pattern.compile("(?i)COVAR_POP\\(\\s*(" + ref + ")\\s*,\\s*\\1\\s*\\)");
            query = covariance.matcher(query).replaceAll("COVAR_POP(vp_rowid, vp_rowid)");
        }
        return query;
    }

    // Only known-predicate diagnostics tolerate rounding in one rowid population aggregate.
    // Bucket by the exact remaining cells, then match sorted aggregate values and multiplicities.
    // This does not change normal VP result comparison or permit NULL/value or group-key changes.
    static boolean predicateEvidenceResultsEqual(String query, Map<String, Integer> left, Map<String, Integer> right) {
        if (left.equals(right)) {
            return true;
        }
        int from = topLevelKeyword(query, " FROM ");
        if (from < 0 || !query.trim().toUpperCase(Locale.ROOT).startsWith("SELECT ")
                || topLevelKeyword(query, " HAVING ") >= 0) {
            return false;
        }
        List<String> terms = splitTopLevel(
                query.substring(query.toUpperCase(Locale.ROOT).indexOf("SELECT ") + 7, from));
        String rowid = "(?:[A-Z0-9_]+\\.)?VP_ROWID";
        int aggregate = -1;
        for (int i = 0; i < terms.size(); i++) {
            String term = normalizeKnownBugQuery(terms.get(i)).trim();
            if (term.matches("STDDEV_POP\\(\\s*" + rowid + "\\s*\\)" + "|COVAR_POP\\(\\s*" + rowid + "\\s*,\\s*" + rowid
                    + "\\s*\\)")) {
                if (aggregate >= 0) {
                    return false;
                }
                aggregate = i;
            }
        }
        if (aggregate < 0) {
            return false;
        }
        try {
            Map<String, List<Double>> a = predicateAggregateBuckets(left, terms.size(), aggregate);
            Map<String, List<Double>> b = predicateAggregateBuckets(right, terms.size(), aggregate);
            if (!a.keySet().equals(b.keySet())) {
                return false;
            }
            for (String key : a.keySet()) {
                List<Double> l = a.get(key), r = b.get(key);
                if (l.size() != r.size()) {
                    return false;
                }
                Collections.sort(l);
                Collections.sort(r);
                for (int i = 0; i < l.size(); i++) {
                    if (ulpDistance(l.get(i), r.get(i)) > 16) {
                        return false;
                    }
                }
            }
            return true;
        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            return false;
        }
    }

    private static Map<String, List<Double>> predicateAggregateBuckets(Map<String, Integer> rows, int columns,
            int aggregate) {
        Map<String, List<Double>> buckets = new LinkedHashMap<>();
        int total = 0;
        for (Map.Entry<String, Integer> entry : rows.entrySet()) {
            if (entry.getValue() <= 0 || (total += entry.getValue()) > 4096) {
                throw new IllegalArgumentException("Invalid diagnostic multiplicity");
            }
            String row = entry.getKey();
            StringBuilder key = new StringBuilder();
            Double value = null;
            int offset = 0;
            for (int column = 0; column < columns; column++) {
                int start = offset;
                if (row.startsWith("N;", offset)) {
                    offset += 2;
                    if (column == aggregate) {
                        key.append("AGG_NULL;");
                    }
                } else if (row.startsWith("S:", offset)) {
                    int colon = row.indexOf(':', offset + 2);
                    int length = Integer.parseInt(row.substring(offset + 2, colon));
                    int end = colon + 1 + length;
                    if (length < 0 || row.charAt(end) != ';') {
                        throw new IllegalArgumentException("Malformed diagnostic cell");
                    }
                    if (column == aggregate) {
                        value = Double.parseDouble(row.substring(colon + 1, end));
                        if (!Double.isFinite(value) || value < 0) {
                            throw new IllegalArgumentException("Invalid population aggregate");
                        }
                        key.append("AGG_VALUE;");
                    }
                    offset = end + 1;
                } else {
                    throw new IllegalArgumentException("Malformed diagnostic row");
                }
                if (column != aggregate) {
                    key.append(row, start, offset);
                }
            }
            if (offset != row.length()) {
                throw new IllegalArgumentException("Unexpected diagnostic columns");
            }
            List<Double> values = buckets.computeIfAbsent(key.toString(), ignored -> new ArrayList<>());
            for (int i = 0; i < entry.getValue(); i++) {
                values.add(value == null ? 0.0 : value);
            }
        }
        return buckets;
    }

    private QueryResultMultiset diagnosticRows(String query, VpQueryPair pair) throws SQLException {
        return diagnosticRows(query, pair.resultTypes);
    }

    private QueryResultMultiset diagnosticRows(String query, List<DatabendDataType> resultTypes) throws SQLException {
        Map<String, Integer> rows = new LinkedHashMap<>();
        List<String> ordered = new ArrayList<>();
        try (Statement statement = state.getConnection().createStatement();
                SQLancerResultSet result = new SQLancerResultSet(statement.executeQuery(query))) {
            while (result.next()) {
                String row = canonicalizeRow(result, result.getColumnCount(), resultTypes);
                ordered.add(row);
                rows.merge(row, 1, Integer::sum);
            }
        }
        return new QueryResultMultiset(rows, Map.of(), ordered, List.of(), false);
    }

    private boolean completeReconstructionEvidence(VpQueryPair pair) {
        int from = topLevelKeyword(pair.splitQuery, " FROM ");
        int where = topLevelKeyword(pair.splitQuery, " WHERE ");
        if (from < 0) {
            return false;
        }
        int end = where >= 0 ? where : predicateEnd(pair.splitQuery, from + 6);
        String relation = pair.splitQuery.substring(from + 6, end).replaceFirst(";\\s*$", "");
        String projection = quote(ROW_ID)
                + pair.schema.columns.stream().map(c -> ", " + quote(c.name)).collect(Collectors.joining());
        try {
            // Use metadata for this full-row diagnostic; the original pair's resultTypes describe its
            // aggregate/projection, not the reconstructed schema. Include rowid to reject missing/duplicate rows.
            QueryResultMultiset source = diagnosticRows(
                    "SELECT " + projection + " FROM " + quote(pair.schema.sourceTable) + " ORDER BY " + quote(ROW_ID),
                    List.of());
            QueryResultMultiset reconstructed = diagnosticRows(
                    "SELECT " + projection + " FROM " + relation + " ORDER BY " + quote(ROW_ID), List.of());
            List<Long> ids = readOrderedRowIds(pair.schema.sourceTable, null);
            return ids != null && !ids.isEmpty() && ids.size() == new java.util.HashSet<>(ids).size()
                    && ids.size() == source.orderedRows.size() && source.orderedRows.equals(reconstructed.orderedRows);
        } catch (SQLException | IgnoreMeException e) {
            return false;
        }
    }

    private boolean completeOrderedProjectionEvidence(VpQueryPair pair, QueryResultMultiset singleRows) {
        int singleLimit = topLevelKeyword(pair.singleQuery, " LIMIT ");
        int splitLimit = topLevelKeyword(pair.splitQuery, " LIMIT ");
        if (singleLimit < 0 || splitLimit < 0) {
            return false;
        }
        try {
            String single = pair.singleQuery.substring(0, singleLimit);
            String split = pair.splitQuery.substring(0, splitLimit);
            QueryResultMultiset direct = diagnosticRows(single, pair);
            QueryResultMultiset reconstructed = diagnosticRows(split, pair);
            int limit = Integer.parseInt(pair.singleQuery.substring(singleLimit + 7).trim());
            return direct.orderedRows.equals(reconstructed.orderedRows) && direct.orderedRows
                    .subList(0, Math.min(limit, direct.orderedRows.size())).equals(singleRows.orderedRows);
        } catch (SQLException | IgnoreMeException | NumberFormatException e) {
            return false;
        }
    }

    static String replaceOuterPredicate(String query, String predicate) {
        int where = topLevelKeyword(query, " WHERE ");
        if (where < 0) {
            throw new IllegalArgumentException("Missing outer WHERE");
        }
        return query.substring(0, where + 7) + predicate + query.substring(predicateEnd(query, where + 7));
    }

    private static int predicateEnd(String query, int start) {
        int end = query.length();
        for (String keyword : List.of(" GROUP BY ", " HAVING ", " ORDER BY ", " LIMIT ")) {
            int index = topLevelKeyword(query, keyword);
            if (index >= start) {
                end = Math.min(end, index);
            }
        }
        return end;
    }

    private static int topLevelKeyword(String query, String keyword) {
        int depth = 0;
        char quoted = 0;
        for (int i = 0; i < query.length(); i++) {
            char c = query.charAt(i);
            if (quoted != 0) {
                if (c == '\\') {
                    i++;
                } else if (c == quoted) {
                    if (i + 1 < query.length() && query.charAt(i + 1) == quoted) {
                        i++;
                    } else {
                        quoted = 0;
                    }
                }
            } else if (c == '\'' || c == '`' || c == '"') {
                quoted = c;
            } else if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            } else if (depth == 0 && query.regionMatches(true, i, keyword, 0, keyword.length())) {
                return i;
            }
        }
        return -1;
    }

    private boolean matchesDatabendFloatAggregateCancellation(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!isKnownDatabendVersion() || singleRows.rows.size() != 1 || splitRows.rows.size() != 1
                || singleRows.rows.equals(splitRows.rows)) {
            return false;
        }
        if (!"MEMORY".equalsIgnoreCase(pair.schema.sourceEngine) || !"MEMORY".equalsIgnoreCase(pair.schema.leftEngine)
                || !"MEMORY".equalsIgnoreCase(pair.schema.rightEngine)) {
            return false;
        }
        for (VpColumn column : pair.schema.columns) {
            // Normalized SQL is only for shape matching: quoted Databend column names are case sensitive.
            if (isFloatingType(column.type) && matchesFloatCancellationAvg(pair.singleQuery, column.name)
                    && matchesFloatCancellationAvg(pair.splitQuery, column.name)
                    && hasOppositeExtremeFloatValues(pair.schema.sourceTable, column.name)
                    && completeReconstructionEvidence(pair)) {
                return true;
            }
        }
        return false;
    }

    static boolean matchesFloatCancellationAvg(String query, String column) {
        return column.matches("c[0-9]+float") && normalizeKnownBugQuery(query)
                .matches("SELECT AVG\\(" + Pattern.quote(column.toUpperCase(Locale.ROOT)) + "\\) FROM .*");
    }

    private boolean hasOppositeExtremeFloatValues(String tableName, String columnName) {
        String column = quote(columnName);
        String query = "SELECT MIN(" + column + "), MAX(" + column + ") FROM " + quote(tableName);
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(query)) {
            return result.next() && result.getDouble(1) < -Float.MAX_VALUE * 0.99
                    && result.getDouble(2) > Float.MAX_VALUE * 0.99;
        } catch (SQLException e) {
            return false;
        }
    }

    private boolean matchesDatabendLeadingNullCompositeGroupBug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        return isKnownDatabendVersion() && pair.reconstructionStrategy == ReconstructionStrategy.JSON_PACK
                && "MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)
                && "MEMORY".equalsIgnoreCase(pair.schema.leftEngine)
                && "MEMORY".equalsIgnoreCase(pair.schema.rightEngine)
                && matchesLeadingNullCompositeGroupShape(pair.singleQuery)
                && matchesLeadingNullCompositeGroupShape(pair.splitQuery)
                && matchesLeadingNullCompositeGroupResults(singleRows.rows, splitRows.rows);
    }

    static boolean matchesLeadingNullCompositeGroupShapeForTesting(String query) {
        return matchesLeadingNullCompositeGroupShape(query);
    }

    private static boolean matchesLeadingNullCompositeGroupShape(String query) {
        String normalized = normalizeKnownBugQuery(query).trim();
        int groupBy = normalized.lastIndexOf(" GROUP BY ");
        if (groupBy < 0) {
            return false;
        }
        int groupEnd = normalized.length();
        for (String terminator : List.of(" HAVING ", " ORDER BY ", " LIMIT ", ";")) {
            int index = normalized.indexOf(terminator, groupBy + " GROUP BY ".length());
            if (index >= 0) {
                groupEnd = Math.min(groupEnd, index);
            }
        }
        List<String> terms = splitTopLevel(normalized.substring(groupBy + " GROUP BY ".length(), groupEnd).trim());
        if (terms.size() < 2 || !isUntypedNullPropagatingGroupTerm(terms.get(0))) {
            return false;
        }
        List<String> trailingTerms = terms.subList(1, terms.size());
        return trailingTerms.stream().noneMatch(DatabendVerticalPartitioningOracle::containsBareNull)
                && trailingTerms.stream().anyMatch(term -> term.matches(".*[1-9].*"));
    }

    private static boolean isUntypedNullPropagatingGroupTerm(String term) {
        String normalized = term.trim();
        if (!containsBareNull(normalized)) {
            return false;
        }
        if (normalized.equals("NULL")) {
            return true;
        }
        // Keep this deliberately narrower than SQL constant folding. Calls
        // such as CAST/COALESCE/IF can assign a concrete nullable type or
        // replace NULL, neither of which has the database34 defect.
        return !Pattern.compile("[A-Z_][A-Z0-9_]*\\s*\\(").matcher(normalized).find()
                && normalized.matches("[A-Z0-9_.$+*/%()\\-\\s]+") && normalized.matches(".*[+*/%\\-].*");
    }

    private static boolean containsBareNull(String expression) {
        return Pattern.compile("(^|[^A-Z0-9_])NULL([^A-Z0-9_]|$)").matcher(expression).find();
    }

    static boolean matchesLeadingNullCompositeGroupResultsForTesting(String singleRow, int singleCount, String splitRow,
            int splitCount) {
        Map<String, Integer> single = new LinkedHashMap<>();
        Map<String, Integer> split = new LinkedHashMap<>();
        single.put(singleRow, singleCount);
        split.put(splitRow, splitCount);
        return matchesLeadingNullCompositeGroupResults(single, split);
    }

    private static boolean matchesLeadingNullCompositeGroupResults(Map<String, Integer> singleRows,
            Map<String, Integer> splitRows) {
        if (singleRows.size() != 1 || splitRows.size() != 1 || !singleRows.keySet().equals(splitRows.keySet())) {
            return false;
        }
        String row = singleRows.keySet().iterator().next();
        return splitRows.get(row) == 1 && singleRows.get(row) > splitRows.get(row);
    }

    private boolean matchesDatabendFuseTopNBlockPruningBug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!isKnownDatabendVersion() || !"MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)
                || !"FUSE".equalsIgnoreCase(pair.schema.leftEngine)
                || pair.reconstructionStrategy != ReconstructionStrategy.TEMP_TABLE
                        && pair.reconstructionStrategy != ReconstructionStrategy.SCALAR_SUBQUERY) {
            return false;
        }
        if (pair.reconstructionStrategy == ReconstructionStrategy.TEMP_TABLE && !hasRowDependentFilter(pair)
                && hasPlanTokens(pair.splitQuery, "SORT(SINGLE)", "TOPN PRUNING:")
                && pair.schema.columns.stream().anyMatch(c -> c.type == DatabendDataType.MAP
                        || c.type == DatabendDataType.ARRAY || c.type == DatabendDataType.GEOMETRY)
                && !singleRows.rows.equals(splitRows.rows)) {
            return true;
        }
        long[] singleShape = parseVpRowIdTopNShape(pair.singleQuery);
        long[] splitShape = parseVpRowIdTopNShape(pair.splitQuery);
        if (singleShape == null || splitShape == null || singleShape[0] != splitShape[0]
                || singleShape[1] != splitShape[1] || hasRowDependentFilter(pair)
                || !hasReducedTopNPruning(pair.splitQuery)) {
            return false;
        }
        String splitTable = pair.reconstructionStrategy == ReconstructionStrategy.TEMP_TABLE ? pair.schema.tempTable
                : pair.schema.leftTable;
        return sameCompleteRowIds(pair.schema.sourceTable, splitTable) && !singleRows.rows.equals(splitRows.rows);
    }

    private boolean hasRowDependentFilter(VpQueryPair pair) {
        String normalized = normalizeKnownBugQuery(pair.singleQuery);
        int where = normalized.indexOf(" WHERE ");
        int order = normalized.lastIndexOf(" ORDER BY ");
        if (where < 0 || order < where) {
            return false;
        }
        String predicate = normalized.substring(where, order);
        if (isAlwaysTrueTopNPredicate(predicate)) {
            return false;
        }
        if (predicate.contains(ROW_ID.toUpperCase(Locale.ROOT))) {
            return true;
        }
        return pair.schema.columns.stream().anyMatch(c -> predicate.contains(c.name.toUpperCase(Locale.ROOT)));
    }

    static boolean isAlwaysTrueTopNPredicateForTesting(String predicate) {
        return isAlwaysTrueTopNPredicate(normalizeKnownBugQuery(predicate));
    }

    private static boolean isAlwaysTrueTopNPredicate(String predicate) {
        String compact = predicate.replace(" ", "");
        return (compact.contains("TRUE)OR") || compact.contains("TRUEOR")) && compact.contains("ISNOTNULL");
    }

    static boolean matchesFuseTopNRowSkipShapeForTesting(String query) {
        return parseVpRowIdTopNShape(query) != null;
    }

    private static long[] parseVpRowIdTopNShape(String query) {
        String normalized = normalizeKnownBugQuery(query).trim();
        java.util.regex.Matcher matcher = VP_ROW_ID_TOP_N_PATTERN.matcher(normalized);
        if (!matcher.find()) {
            return null;
        }
        for (String key : matcher.group(1).split(",")) {
            String normalizedKey = key.trim().replaceAll("^[A-Z_][A-Z0-9_]*\\.", "")
                    .replaceAll("\\s+NULLS\\s+(FIRST|LAST)$", "").replaceAll("\\s+ASC$", "").trim();
            if (!normalizedKey.equals(ROW_ID.toUpperCase(Locale.ROOT))) {
                return null;
            }
        }
        try {
            long limit = Long.parseLong(matcher.group(2));
            long offset = matcher.group(3) == null ? 0 : Long.parseLong(matcher.group(3));
            return limit > 0 && offset >= 0 ? new long[] { limit, offset } : null;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    static boolean hasReducedTopNPruningForTesting(String plan) {
        return hasReducedTopNPruningText(plan);
    }

    private boolean hasReducedTopNPruning(String query) {
        String plan = explainPlanText(query);
        return plan != null && hasReducedTopNPruningText(plan);
    }

    private static boolean hasReducedTopNPruningText(String plan) {
        java.util.regex.Matcher matcher = TOP_N_PRUNING_PATTERN.matcher(plan == null ? "" : plan);
        while (matcher.find()) {
            try {
                if (Long.parseLong(matcher.group(1)) > Long.parseLong(matcher.group(2))) {
                    return true;
                }
            } catch (NumberFormatException ignored) {
                return false;
            }
        }
        return false;
    }

    private boolean sameCompleteRowIds(String sourceTable, String splitTable) {
        List<Long> source = readOrderedRowIds(sourceTable, null);
        List<Long> split = readOrderedRowIds(splitTable, null);
        return source != null && source.equals(split);
    }

    private boolean sameTopNRowIds(String sourceTable, String splitTable, long[] shape) {
        List<Long> source = readOrderedRowIds(sourceTable, shape);
        List<Long> split = readOrderedRowIds(splitTable, shape);
        return source != null && source.equals(split);
    }

    private List<Long> readOrderedRowIds(String table, long[] shape) {
        StringBuilder query = new StringBuilder("SELECT ").append(quote(ROW_ID)).append(" FROM ").append(quote(table))
                .append(" ORDER BY ").append(quote(ROW_ID));
        if (shape != null) {
            query.append(" LIMIT ").append(shape[0]);
            if (shape[1] > 0) {
                query.append(" OFFSET ").append(shape[1]);
            }
        }
        List<Long> rows = new ArrayList<>();
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(query.toString())) {
            while (result.next()) {
                rows.add(result.getLong(1));
            }
            return rows;
        } catch (SQLException e) {
            return null;
        }
    }

    private boolean matchesDatabendRuntimeFilterRankLimitBug(VpQueryPair pair) {
        return isKnownDatabendVersion() && "MEMORY".equalsIgnoreCase(pair.schema.leftEngine)
                && "FUSE".equalsIgnoreCase(pair.schema.rightEngine)
                && pair.schema.rightColumns.stream()
                        .anyMatch(c -> c.type == DatabendDataType.BOOLEAN || c.type == DatabendDataType.BOOL)
                && matchesDatabendRuntimeFilterRankLimitShape(pair.singleQuery)
                && matchesDatabendRuntimeFilterRankLimitShape(pair.splitQuery)
                && hasPlanTokens(pair.splitQuery, "RANK LIMIT", "APPLY JOIN FILTERS");
    }

    static boolean matchesDatabendRuntimeFilterRankLimitShape(String query) {
        String normalized = normalizeKnownBugQuery(query).replaceAll("\\b[A-Z_][A-Z0-9_]*\\.", "");
        return (normalized.contains("COUNT(") || normalized.contains("SUM(")) && normalized.contains("WHERE")
                && normalized.contains("C0BOOLEAN") && normalized.contains("GROUP BY VP_ROWID, VP_ROWID")
                && normalized.contains("ORDER BY VP_ROWID, VP_ROWID")
                && normalized.matches(".* LIMIT [0-9]+(?: OFFSET [0-9]+)?(?:;)?$")
                && (normalized.contains(" JOIN ") || normalized.matches(".* FROM [A-Z0-9_]*SOURCE(?: |$).*$"));
    }

    private boolean matchesDatabendArrayAggNullJoinBug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!isKnownDatabendVersion() || !pair.description.contains("databend=array-agg")) {
            return false;
        }
        String single = normalizeKnownBugQuery(pair.singleQuery);
        String split = normalizeKnownBugQuery(pair.splitQuery);
        VpColumn arrayColumn = pair.schema.columns.stream().filter(c -> c.type == DatabendDataType.ARRAY).filter(
                c -> matchesArrayAggNullMaterializationQueryShape(single, split, pair.schema.sourceTable, c.name))
                .findFirst().orElse(null);
        if (arrayColumn == null || singleRows.rows.size() != 1 || splitRows.rows.size() != 1) {
            return false;
        }
        String singleRow = singleRows.rows.keySet().iterator().next();
        String splitRow = splitRows.rows.keySet().iterator().next();
        long[] nullProfile = getArrayNullProfile(pair.schema.sourceTable, arrayColumn.name);
        if (nullProfile == null) {
            return false;
        }
        if (matchesArrayAggNullMaterializationResults(singleRow, splitRow, nullProfile[0], nullProfile[1])) {
            return true;
        }
        // 920/31,38,42: both execution paths may materialize only a subset of NULL ARRAY cells.
        // Matching non-empty elements alone cannot establish reconstruction correctness.
        return hasSupportedKnownEngines(pair)
                && List.of(" WHERE ", " GROUP BY ", " HAVING ", " ORDER BY ", " LIMIT ").stream().noneMatch(
                        k -> topLevelKeyword(pair.singleQuery, k) >= 0 || topLevelKeyword(pair.splitQuery, k) >= 0)
                && completeReconstructionEvidence(pair) && matchesPartialArrayAggNullMaterializationResults(singleRow,
                        splitRow, nullProfile[0], nullProfile[1]);
    }

    private boolean matchesDatabendArrayNullPredicateJoinBug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!isKnownDatabendVersion() || !hasSupportedKnownEngines(pair)
                || !"FUSE".equalsIgnoreCase(pair.schema.sourceEngine)
                || singleRows.rows.equals(splitRows.rows)
                || !normalizeKnownBugQuery(pair.singleQuery).contains("COVAR_POP(")
                || !normalizeKnownBugQuery(pair.splitQuery).contains("EXISTS (SELECT 1 FROM")
                || !hasPlanTokens(pair.splitQuery, "HASHJOIN")) {
            return false;
        }
        for (VpColumn array : pair.schema.columns) {
            if (array.type != DatabendDataType.ARRAY || !array.nullable
                    || !matchesArrayNullPredicate(pair.singleQuery, array.name)) {
                continue;
            }
            boolean scalarLookup = pair.schema.rightColumns.stream().anyMatch(right -> right.name.equals(array.name)
                    && normalizeKnownBugQuery(pair.splitQuery).contains("SELECT R." + array.name.toUpperCase(Locale.ROOT)
                            + " FROM " + pair.schema.rightTable.toUpperCase(Locale.ROOT)));
            if (scalarLookup && hasEmptyAndNonEmptyArrayValues(pair.schema.sourceTable, array.name)
                    && completeReconstructionEvidence(pair)) {
                return true;
            }
        }
        return false;
    }

    static boolean matchesArrayNullPredicate(String query, String arrayColumn) {
        String array = "(?:[A-Z0-9_]+\\.)?" + Pattern.quote(arrayColumn.toUpperCase(Locale.ROOT));
        String shape = predicateShapeText(query);
        return Pattern.compile("(?<![A-Z0-9_.])" + array
                + "\\s+IS\\s+NULL\\s*<>\\s*(?:[A-Z0-9_]+\\.)?[A-Z0-9_]+\\s+IS\\s+NULL"
                + "|(?<![A-Z0-9_.])(?:[A-Z0-9_]+\\.)?[A-Z0-9_]+\\s+IS\\s+NULL\\s*<>\\s*" + array
                + "\\s+IS\\s+NULL", Pattern.CASE_INSENSITIVE).matcher(shape).find();
    }

    private boolean hasEmptyAndNonEmptyArrayValues(String table, String column) {
        boolean empty = false;
        boolean nonEmpty = false;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery("SELECT " + quote(column) + " FROM " + quote(table))) {
            while (result.next()) {
                String value = result.getString(1);
                if (value == null) {
                    continue;
                }
                if (value.replaceAll("\\s+", "").equals("[]")) {
                    empty = true;
                } else if (value.trim().startsWith("[")) {
                    nonEmpty = true;
                }
            }
        } catch (SQLException e) {
            return false;
        }
        return empty && nonEmpty;
    }

    static boolean matchesArrayAggNullMaterializationQueryShapeForTesting(String singleQuery, String splitQuery,
            String sourceTable, String arrayColumn) {
        return matchesArrayAggNullMaterializationQueryShape(normalizeKnownBugQuery(singleQuery),
                normalizeKnownBugQuery(splitQuery), sourceTable, arrayColumn);
    }

    private static boolean matchesArrayAggNullMaterializationQueryShape(String single, String split, String sourceTable,
            String arrayColumn) {
        String aggregate = "ARRAY_SORT(ARRAY_AGG(" + arrayColumn.toUpperCase(Locale.ROOT) + "))";
        String source = sourceTable.toUpperCase(Locale.ROOT);
        // Do not depend on a particular reconstruction operator or physical plan. The same server defect is
        // exposed by JOIN, VIEW, correlated subqueries, row-number alignment, packing, and full-outer simulation.
        // The result/cardinality check below remains the decisive guard against hiding unrelated mismatches.
        return single.contains("SELECT " + aggregate + " FROM " + source)
                && split.contains("SELECT " + aggregate + " FROM ") && !single.equals(split);
    }

    private long[] getArrayNullProfile(String table, String column) {
        // Nullable complex COUNT metadata has its own known defect; read cells independently.
        String query = "SELECT " + quote(column) + " FROM " + quote(table);
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(query)) {
            long rows = 0;
            long nonNull = 0;
            while (result.next()) {
                rows++;
                if (result.getString(1) != null) {
                    nonNull++;
                }
            }
            return new long[] { rows, nonNull };
        } catch (SQLException e) {
            return null;
        }
    }

    static boolean matchesArrayAggNullMaterializationResultsForTesting(String singleRow, String splitRow,
            long sourceRows, long nonNullArrays) {
        return matchesArrayAggNullMaterializationResults(singleRow, splitRow, sourceRows, nonNullArrays);
    }

    private static boolean matchesArrayAggNullMaterializationResults(String singleRow, String splitRow, long sourceRows,
            long nonNullArrays) {
        if (sourceRows <= 0 || nonNullArrays < 0 || nonNullArrays > sourceRows || splitRow == null
                || !singleRow.startsWith("S:") || !splitRow.startsWith("S:")) {
            return false;
        }
        int singleElements = countTopLevelArrayElements(singleRow);
        int splitElements = countTopLevelArrayElements(splitRow);
        if (singleElements < nonNullArrays || splitElements < nonNullArrays || singleElements > sourceRows
                || splitElements > sourceRows || singleElements == splitElements
                || singleElements != sourceRows && splitElements != sourceRows) {
            return false;
        }
        return nonEmptyTopLevelArrayElements(singleRow).equals(nonEmptyTopLevelArrayElements(splitRow));
    }

    private static List<String> nonEmptyTopLevelArrayElements(String row) {
        int start = row.indexOf('[');
        int end = row.lastIndexOf(']');
        if (start < 0 || end <= start) {
            return List.of();
        }
        return splitTopLevel(row.substring(start + 1, end)).stream().map(String::trim)
                .filter(element -> !element.isEmpty() && !element.equals("[]")).collect(Collectors.toList());
    }

    static boolean matchesPartialArrayAggNullMaterializationResults(String singleRow, String splitRow, long sourceRows,
            long nonNullArrays) {
        if (sourceRows <= nonNullArrays || nonNullArrays < 0 || singleRow == null || splitRow == null
                || !singleRow.matches("S:[0-9]+:\\[.*\\];") || !splitRow.matches("S:[0-9]+:\\[.*\\];")) {
            return false;
        }
        int singleElements = countTopLevelArrayElements(singleRow);
        int splitElements = countTopLevelArrayElements(splitRow);
        return singleElements >= nonNullArrays && splitElements >= nonNullArrays && singleElements <= sourceRows
                && splitElements <= sourceRows && singleElements != splitElements
                && nonEmptyTopLevelArrayElements(singleRow).equals(nonEmptyTopLevelArrayElements(splitRow));
    }

    private static int countTopLevelArrayElements(String row) {
        int start = row.indexOf("[");
        int end = row.lastIndexOf("]");
        if (start < 0 || end <= start) {
            return -1;
        }
        int depth = 0;
        int elements = 0;
        boolean content = false;
        for (int i = start + 1; i < end; i++) {
            char c = row.charAt(i);
            if (c == '[') {
                depth++;
                content = true;
            } else if (c == ']') {
                depth--;
                content = true;
            } else if (c == ',' && depth == 0) {
                elements++;
                content = false;
            } else if (!Character.isWhitespace(c)) {
                content = true;
            }
        }
        return content ? elements + 1 : elements;
    }

    private boolean matchesDatabase4LateralAggregateBug(VpQueryPair pair) {
        return pair.reconstructionStrategy == ReconstructionStrategy.LATERAL_DERIVED
                && pair.schema.splitStrategy == SplitStrategy.SKEWED_SPLIT
                && pair.schema.baseShape == BaseShape.MULTI_COLUMN && "MEMORY".equalsIgnoreCase(pair.schema.leftEngine)
                && "FUSE".equalsIgnoreCase(pair.schema.rightEngine)
                && matchesDatabase4LateralAggregateShape(pair.singleQuery)
                && matchesDatabase4LateralAggregateShape(pair.splitQuery)
                && normalizeKnownBugQuery(pair.splitQuery).contains("JOIN LATERAL");
    }

    private boolean matchesDatabase6LateralAggregateBug(VpQueryPair pair) {
        return pair.reconstructionStrategy == ReconstructionStrategy.LATERAL_DERIVED
                && pair.schema.splitStrategy == SplitStrategy.TYPE_DIRECTED_SPLIT
                && pair.schema.baseShape == BaseShape.MULTI_COLUMN && "MEMORY".equalsIgnoreCase(pair.schema.leftEngine)
                && "FUSE".equalsIgnoreCase(pair.schema.rightEngine)
                && matchesDatabase6LateralAggregateShape(pair.singleQuery)
                && matchesDatabase6LateralAggregateShape(pair.splitQuery)
                && normalizeKnownBugQuery(pair.splitQuery).contains("JOIN LATERAL");
    }

    static boolean matchesDatabase4LateralAggregateShape(String query) {
        String normalized = normalizeKnownBugQuery(query);
        return normalized.contains("MIN(") && normalized.contains("GROUP BY") && normalized.contains("ORDER BY")
                && normalized.contains("LIMIT") && normalized.contains("OFFSET") && normalized.contains("VP_ROWID")
                && normalized.contains("C2BOOLEAN");
    }

    static boolean matchesDatabase6LateralAggregateShape(String query) {
        String normalized = normalizeKnownBugQuery(query);
        return normalized.contains("SUM(") && normalized.contains("GROUP BY") && normalized.contains("ORDER BY")
                && normalized.contains("LIMIT") && normalized.contains("VP_ROWID") && normalized.contains("C1INT")
                && normalized.contains("C0BOOLEAN");
    }

    private static String normalizeKnownBugQuery(String query) {
        return query == null ? "" : query.toUpperCase(Locale.ROOT).replace("`", "").replaceAll("\\s+", " ");
    }

    private KnownVpBug classifyKnownOrderedBug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!isKnownDatabendVersion() || !singleRows.rows.equals(splitRows.rows) || !hasSupportedKnownEngines(pair)
                || !"MEMORY".equalsIgnoreCase(pair.schema.sourceEngine)) {
            return null;
        }
        long[] direct = parseVpRowIdTopNShape(pair.singleQuery);
        long[] split = parseVpRowIdTopNShape(pair.splitQuery);
        if (direct == null || split == null || direct[0] != split[0] || direct[1] != split[1]
                || !sameCompleteRowIds(pair.schema.sourceTable,
                        pair.reconstructionStrategy == ReconstructionStrategy.TEMP_TABLE ? pair.schema.tempTable
                                : pair.schema.leftTable)) {
            return null;
        }
        if (matchesOrderedReverseLikePruning(pair)) {
            return KnownVpBug.DATABEND_FUSE_EMPTY_PATTERN_LIKE;
        }
        if ((pair.reconstructionStrategy == ReconstructionStrategy.TEMP_TABLE
                || pair.reconstructionStrategy == ReconstructionStrategy.SCALAR_SUBQUERY)
                && "FUSE".equalsIgnoreCase(pair.schema.leftEngine) && !hasRowDependentFilter(pair)
                && hasReducedTopNPruning(pair.splitQuery)
                && hasPlanTokens(pair.splitQuery, "SORT(SINGLE)", "TOPN PRUNING:")) {
            return KnownVpBug.DATABEND_FUSE_TOPN_ORDERED_ROW_REPLACEMENT;
        }
        return null;
    }

    private boolean matchesOrderedReverseLikePruning(VpQueryPair pair) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.STRING_PACK
                || !"FUSE".equalsIgnoreCase(pair.schema.leftEngine) || !"FUSE".equalsIgnoreCase(pair.schema.rightEngine)
                || hasReducedTopNPruning(pair.splitQuery)
                || !hasPlanTokens(pair.singleQuery, "SORT(SINGLE)", "FILTERS: [IS_TRUE(NOT LIKE(")
                || !hasPlanTokens(pair.splitQuery, "HASHJOIN", "NOT LIKE(")) {
            return false;
        }
        for (VpColumn column : pair.schema.leftColumns) {
            if (column.nullable && column.type == DatabendDataType.VARCHAR
                    && hasReverseLikePredicate(pair.singleQuery, column.name) && completeReconstructionEvidence(pair)
                    && hasEmptyPatternValue(pair)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Matches the concrete SQL shape from logs/databend911/2bug.log. The outer predicate is the row-id modulo
     * arithmetic that Databend pushes into the left scan of the RIGHT JOIN anti-join branch.
     */
    static boolean matchesDatabendFullOuterPredicatePushdownShape(String query) {
        if (query == null) {
            return false;
        }
        String normalized = query.toUpperCase(Locale.ROOT).replace('"', '`').replaceAll("\\s+", " ").trim();
        int unionAll = normalized.indexOf(" UNION ALL ");
        if (unionAll < 0) {
            return false;
        }
        int leftJoin = normalized.indexOf(" LEFT JOIN ");
        int rightJoin = normalized.indexOf(" RIGHT JOIN ", unionAll);
        if (leftJoin < 0 || leftJoin > unionAll || rightJoin < 0) {
            return false;
        }

        int antiJoinWhere = normalized.indexOf("WHERE L.`VP_ROWID` IS NULL");
        if (antiJoinWhere < 0) {
            antiJoinWhere = normalized.indexOf("WHERE L.VP_ROWID IS NULL");
        }
        if (antiJoinWhere < rightJoin) {
            return false;
        }

        int outerWhere = normalized.lastIndexOf(" WHERE ");
        if (outerWhere <= antiJoinWhere) {
            return false;
        }
        String outerPredicate = normalized.substring(outerWhere);
        return countOccurrences(outerPredicate, "VP_ROWID") >= 2 && outerPredicate.contains("%")
                && outerPredicate.contains("-");
    }

    private static int countOccurrences(String text, String needle) {
        int count = 0;
        for (int index = text.indexOf(needle); index >= 0; index = text.indexOf(needle, index + needle.length())) {
            count++;
        }
        return count;
    }

    // Random SQL can contain isolated UTF-16 surrogates. Keep their identity in diagnostics
    // without letting the strict UTF-8 file encoder abort an already classified known report.
    static String utf8LogText(String text) {
        StringBuilder result = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isHighSurrogate(c) && i + 1 < text.length() && Character.isLowSurrogate(text.charAt(i + 1))) {
                result.append(c).append(text.charAt(++i));
            } else if (Character.isSurrogate(c)) {
                result.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    private void logKnownBug(VpQueryPair pair, KnownVpBug knownBug, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows, boolean orderedReport) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("Known Databend VP bug: ");
        KnownVpBugFamily family = knownBugFamily(knownBug);
        if (family != null) {
            sb.append(family).append(" / ");
        }
        sb.append(knownBug).append(System.lineSeparator()).append(System.lineSeparator());
        if (orderedReport) {
            sb.append(formatOrderedBugReport(pair, singleRows, splitRows));
        } else {
            sb.append(formatBugReport(pair, singleRows, splitRows));
        }
        File dir = new File(Main.LOG_DIRECTORY, "databend");
        try {
            Files.createDirectories(dir.toPath());
            Path target;
            do {
                target = new File(dir, "vp-known" + KNOWN_BUG_LOG_ID.incrementAndGet() + ".log").toPath();
            } while (Files.exists(target));
            Files.writeString(target, utf8LogText(sb.toString()), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private boolean resultMultisetsMatch(QueryResultMultiset left, QueryResultMultiset right) {
        if (left.rows.equals(right.rows)) {
            return true;
        }
        Map<String, Integer> remaining = new LinkedHashMap<>(right.rows);
        for (Map.Entry<String, Integer> entry : left.rows.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                String match = findApproximateRowMatch(entry.getKey(), left.approximateColumns.get(entry.getKey()),
                        remaining, right.approximateColumns,
                        left.approximateComparisonEnabled && right.approximateComparisonEnabled);
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
            Map<String, Integer> candidates, Map<String, List<ApproximateKind>> candidateApproximateColumns,
            boolean approximateAllowed) {
        Integer exactCount = candidates.get(row);
        if (exactCount != null && exactCount > 0) {
            return row;
        }
        for (Map.Entry<String, Integer> candidate : candidates.entrySet()) {
            if (candidate.getValue() > 0 && rowsApproximatelyEqual(row, leftApproximateColumns, candidate.getKey(),
                    candidateApproximateColumns.get(candidate.getKey()), approximateAllowed)) {
                return candidate.getKey();
            }
        }
        return null;
    }

    private boolean rowsApproximatelyEqual(String left, List<ApproximateKind> leftApproximateColumns, String right,
            List<ApproximateKind> rightApproximateColumns, boolean approximateAllowed) {
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
            if (!approximateAllowed || !approximateKind.isApproximate() || l == null || r == null
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
            double difference = Math.abs(l - r);
            double scale = Math.max(Math.abs(l), Math.abs(r));
            if (difference <= approximateKind.absoluteTolerance
                    || difference <= approximateKind.relativeTolerance * scale) {
                recordApproximateMatch(approximateKind, ulpDistance(l, r));
                return true;
            }
            long ulps = ulpDistance(l, r);
            boolean matches = ulps <= approximateKind.maxUlps;
            if (matches) {
                recordApproximateMatch(approximateKind, ulps);
            }
            return matches;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    static long ulpDistanceForTesting(double left, double right) {
        long leftBits = Double.doubleToLongBits(left);
        long rightBits = Double.doubleToLongBits(right);
        if (leftBits < 0) {
            leftBits = Long.MIN_VALUE - leftBits;
        }
        if (rightBits < 0) {
            rightBits = Long.MIN_VALUE - rightBits;
        }
        long distance = leftBits - rightBits;
        return distance == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(distance);
    }

    static long doubleAggregateUlpsForTesting() {
        return ApproximateKind.DOUBLE.maxUlps;
    }

    private static long ulpDistance(double left, double right) {
        return ulpDistanceForTesting(left, right);
    }

    private static void recordApproximateMatch(ApproximateKind kind, long ulps) {
        synchronized (STATS_LOCK) {
            APPROXIMATE_MATCH_COUNTS.put(kind, APPROXIMATE_MATCH_COUNTS.getOrDefault(kind, 0) + 1);
            APPROXIMATE_MAX_ULPS.put(kind, Math.max(APPROXIMATE_MAX_ULPS.getOrDefault(kind, 0L), ulps));
        }
    }

    private QueryResultMultiset getResultMultiset(String query, boolean approximateComparisonEnabled, String stage,
            ReconstructionStrategy strategy, List<DatabendDataType> resultTypes) throws SQLException {
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(query);
        }
        /*
         * Result queries deliberately use no ExpectedErrors. A broad expected-error entry turns a server error into a
         * null result before this oracle can distinguish a feature gap from a binding or execution bug.
         */
        SQLQueryAdapter adapter = new SQLQueryAdapter(query, new ExpectedErrors(), false,
                state.getOptions().canonicalizeSqlString());
        Map<String, Integer> rows = new LinkedHashMap<>();
        Map<String, List<ApproximateKind>> approximateColumns = new LinkedHashMap<>();
        Map<String, String> firstRawRowByCanonicalRow = new LinkedHashMap<>();
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
                String row = canonicalizeRow(rs, columnCount, resultTypes);
                String rawRow = rawRow(rs, columnCount);
                String firstRawRow = firstRawRowByCanonicalRow.putIfAbsent(row, rawRow);
                if (firstRawRow != null && !firstRawRow.equals(rawRow)) {
                    recordPipelineMetric(strategy, PipelineMetric.CANONICALIZATION_COLLISION);
                }
                List<ApproximateKind> rowApproximateColumns = getApproximateColumns(rs, columnCount);
                orderedRows.add(row);
                orderedApproximateColumns.add(rowApproximateColumns);
                rows.put(row, rows.getOrDefault(row, 0) + 1);
                approximateColumns.putIfAbsent(row, rowApproximateColumns);
            }
            completed = true;
        } catch (AssertionError e) {
            if (handleQueryError(e, "result-" + stage, query, strategy)) {
                throw new IgnoreMeException();
            }
            throw e;
        } catch (SQLException e) {
            if (isSkippableResultReadError(e)) {
                recordPipelineMetric(strategy, PipelineMetric.SKIPPED_SERVER_ERROR);
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
        return new QueryResultMultiset(rows, approximateColumns, orderedRows, orderedApproximateColumns,
                approximateComparisonEnabled);
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
        return classifyQueryError(getErrorText(e)).skippable;
    }

    private boolean handleQueryError(AssertionError error, String stage, String sql, ReconstructionStrategy strategy) {
        String message = getErrorText(error);
        streamLicenseExpired |= isExpiredStreamLicense(message);
        QueryErrorDisposition disposition = classifyQueryError(message);
        recordSkippedQueryError(disposition, stage, strategy, sql, message);
        return isSkippableQueryError(error);
    }

    static boolean isSkippableQueryErrorForTesting(String message) {
        return classifyQueryError(message).skippable;
    }

    static String classifyQueryErrorForTesting(String message) {
        return classifyQueryError(message).name();
    }

    static String errorKindForTesting(String message) {
        return errorKind(message);
    }

    static boolean isExpiredStreamLicense(String message) {
        String lower = message == null ? "" : message.toLowerCase(Locale.ROOT);
        return lower.contains("licensekeyexpired") && lower.contains("code: 1404")
                && lower.contains("feature 'stream' requires databend enterprise edition license")
                && lower.contains("license key expired for tenant:");
    }

    static boolean canContinueStreamCleanup(String sql, String message) {
        return normalizeKnownBugQuery(sql).trim().startsWith("DROP STREAM IF EXISTS ")
                && isExpiredStreamLicense(message);
    }

    private static QueryErrorDisposition classifyQueryError(String message) {
        if (message == null || message.isEmpty()) {
            return QueryErrorDisposition.UNCONFIRMED_SERVER_ERROR;
        }
        String lower = message.toLowerCase(Locale.ROOT);
        if (isExpiredStreamLicense(message)) {
            return QueryErrorDisposition.LICENSE_RESTRICTION;
        }
        if (isTransientStorageError(lower)) {
            return QueryErrorDisposition.STORAGE_TIMEOUT;
        }
        // AT(TIMESTAMP) on a table that has never produced a historical snapshot is a
        // normal consequence of empty helper tables, not a Databend correctness failure.
        if (lower.contains("tablehistoricaldatanotfound") || lower.contains("empty table has no historical data")) {
            return QueryErrorDisposition.CONFIRMED_GENERATION_ERROR;
        }
        if (lower.contains("you have an error in your sql syntax") || lower.contains("doesn't yet support")
                || lower.contains("not supported") || lower.contains("function") && (lower.contains("does not exist")
                        || lower.contains("unknown function") || lower.contains("function not found"))) {
            return QueryErrorDisposition.CONFIRMED_FEATURE_GAP;
        }
        if (lower.contains("incorrect decimal value") || lower.contains("truncated incorrect")
                || lower.contains("data truncation") || lower.contains("data too long")
                || lower.contains("out of range value") || lower.contains("division by 0")
                || lower.contains("division by zero") || lower.contains("divided by zero")
                || lower.contains("invalid json") || lower.contains("number overflowed while evaluating function")
                || lower.contains("no overload satisfies") || lower.contains("expect to have two arguments")) {
            return QueryErrorDisposition.CONFIRMED_GENERATION_ERROR;
        }
        if (isConfirmedDatabendVersionBug(lower)) {
            return QueryErrorDisposition.CONFIRMED_VERSION_BUG;
        }
        // Databend 1.2.925's Parquet reader panics on some otherwise valid
        // DECIMAL round-trips and surfaces this generic internal error.
        // Treat it as a known release defect so COPY coverage does not abort
        // the oracle or create false positives.
        return QueryErrorDisposition.UNCONFIRMED_SERVER_ERROR;
    }

    private static boolean isConfirmedDatabendVersionBug(String message) {
        return DatabendBugs.bug9018 && message.contains("index out of bounds")
                || DatabendBugs.bug9162 && message.contains("downcast column error")
                || DatabendBugs.bug9163 && message.contains("validity must be equal to the array's length")
                || DatabendBugs.bug9224
                        && message.contains("can't cast column from nullable data into non-nullable type")
                || DatabendBugs.bug9234 && message.contains("called `option::unwrap()` on a `none` value")
                || DatabendBugs.bug9264 && message.contains("assertion failed: offset + length <= self.length")
                || DatabendBugs.bug9806 && message.contains("segment pruning failure")
                || DatabendBugs.bug15568 && message.contains("decimal overflow at line");
    }

    private static boolean isTransientStorageError(String message) {
        return message.contains("storageother") && message.contains("io timeout reached");
    }

    private static void recordSkippedQueryError(QueryErrorDisposition disposition, String stage,
            ReconstructionStrategy strategy, String sql, String message) {
        registerVpStatsShutdownHook();
        String key = disposition.name() + ":" + errorKind(message);
        synchronized (STATS_LOCK) {
            if (disposition.skippable) {
                PipelineMetric metric = disposition == QueryErrorDisposition.CONFIRMED_FEATURE_GAP
                        || disposition == QueryErrorDisposition.CONFIRMED_GENERATION_ERROR
                                ? PipelineMetric.SKIPPED_PARSER_ERROR : PipelineMetric.SKIPPED_SERVER_ERROR;
                // Setup/COPY errors occur before a reconstruction strategy is
                // selected. EnumMap rejects a null strategy key; preserve the
                // global error accounting without aborting error handling.
                if (strategy != null) {
                    incrementPipelineMetric(strategy, metric);
                }
            }
            increment(SKIPPED_ERROR_CATEGORY_COUNTS, disposition);
            if (!SKIPPED_ERROR_COUNTS.containsKey(key) && SKIPPED_ERROR_COUNTS.size() >= MAX_SKIPPED_ERROR_GROUPS) {
                key = disposition.name() + ":<other>";
            }
            SKIPPED_ERROR_COUNTS.put(key, SKIPPED_ERROR_COUNTS.getOrDefault(key, 0) + 1);
            List<SkippedErrorSample> samples = SKIPPED_ERROR_SAMPLES.get(key);
            if (samples == null) {
                if (SKIPPED_ERROR_SAMPLES.size() >= MAX_SKIPPED_ERROR_SAMPLE_GROUPS
                        && shouldReserveSkippedErrorSample(disposition)) {
                    String generationSample = SKIPPED_ERROR_SAMPLES.keySet().stream()
                            .filter(k -> k.startsWith(QueryErrorDisposition.CONFIRMED_GENERATION_ERROR.name() + ":"))
                            .findFirst().orElse(null);
                    if (generationSample != null) {
                        SKIPPED_ERROR_SAMPLES.remove(generationSample);
                    }
                }
                if (SKIPPED_ERROR_SAMPLES.size() < MAX_SKIPPED_ERROR_SAMPLE_GROUPS) {
                    samples = new ArrayList<>();
                    SKIPPED_ERROR_SAMPLES.put(key, samples);
                }
            }
            if (samples != null && samples.size() < MAX_SKIPPED_ERROR_SAMPLES_PER_GROUP) {
                samples.add(new SkippedErrorSample(stage, strategy == null ? "<none>" : strategy.name(),
                        normalizeSkippedSql(sql)));
            }
        }
    }

    private static boolean shouldReserveSkippedErrorSample(QueryErrorDisposition disposition) {
        return disposition == QueryErrorDisposition.UNCONFIRMED_SERVER_ERROR
                || disposition == QueryErrorDisposition.STORAGE_TIMEOUT;
    }

    private static String errorKind(String message) {
        String lower = message == null ? "" : message.toLowerCase(Locale.ROOT);
        if (isExpiredStreamLicense(message)) {
            return "code-1404:stream-license-expired";
        }
        if (isTransientStorageError(lower)) {
            return "storage-io-timeout";
        }
        if (lower.contains("unknown column")) {
            return "unknown-column";
        }
        if (lower.contains("operand should contain")) {
            return "operand-cardinality";
        }
        if (lower.contains("subquery returns more than 1 row")) {
            return "subquery-cardinality";
        }
        if (lower.contains("invalid use of group function")) {
            return "invalid-group-function";
        }
        if (lower.contains("no function matches signature")) {
            return "function-signature";
        }
        if (lower.contains("division by 0") || lower.contains("division by zero")
                || lower.contains("divided by zero")) {
            return "division-by-zero";
        }
        if (lower.contains("unwinderror") && lower.contains("entered unreachable")) {
            return "unwind-entered-unreachable";
        }
        if (lower.contains("assertion failed")) {
            return "assertion-failed";
        }
        if (lower.contains("window")) {
            return "window";
        }
        if (lower.contains("lateral")) {
            return "lateral";
        }
        String compact = message == null ? "<empty>" : message.replaceAll("\\s+", " ").trim();
        int newline = compact.indexOf('\n');
        if (newline > 0) {
            compact = compact.substring(0, newline);
        }
        return compact.length() > 180 ? compact.substring(0, 180) : compact;
    }

    private static String normalizeSkippedSql(String sql) {
        if (sql == null || sql.isEmpty()) {
            return "<unavailable>";
        }
        String normalized = sql.replaceAll("\\s+", " ").trim();
        return normalized.length() > 600 ? normalized.substring(0, 600) : normalized;
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

    private String canonicalizeRow(SQLancerResultSet rs, int columnCount, List<DatabendDataType> resultTypes)
            throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            DatabendDataType expectedType = i <= resultTypes.size() ? resultTypes.get(i - 1) : null;
            String value = rs.getString(i);
            if (value == null) {
                sb.append("N;");
            } else {
                String canonical = canonicalizeTypedValue(value, null, rs.getType(i), expectedType);
                sb.append("S:").append(canonical.length()).append(":").append(canonical).append(";");
            }
        }
        return sb.toString();
    }

    private String rawRow(SQLancerResultSet rs, int columnCount) throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            String value = rs.getString(i);
            if (value == null) {
                sb.append("N;");
            } else {
                sb.append("S:").append(value.length()).append(":").append(value).append(";");
            }
        }
        return sb.toString();
    }

    static String canonicalizeValue(String value, String typeName) {
        if (value == null || !isNumericResultType(typeName)) {
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

    static String canonicalizeTypedValue(String value, byte[] bytes, String typeName, DatabendDataType expectedType) {
        if (value == null) {
            return null;
        }
        if (expectedType == DatabendDataType.VARIANT || expectedType == DatabendDataType.GEOMETRY) {
            String json = canonicalizeJson(value);
            return json == null ? value : json;
        }
        return canonicalizeValue(value, typeName);
    }

    private static String canonicalizeJson(String value) {
        try {
            return renderCanonicalJson(JSON_MAPPER.readTree(value));
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    private static String renderCanonicalJson(JsonNode node) {
        if (node.isObject()) {
            List<String> names = new ArrayList<>();
            node.fieldNames().forEachRemaining(names::add);
            Collections.sort(names);
            return names.stream().map(name -> {
                try {
                    return JSON_MAPPER.writeValueAsString(name) + ":" + renderCanonicalJson(node.get(name));
                } catch (JsonProcessingException e) {
                    throw new AssertionError(e);
                }
            }).collect(Collectors.joining(",", "{", "}"));
        }
        if (node.isArray()) {
            List<String> values = new ArrayList<>();
            node.forEach(value -> values.add(renderCanonicalJson(value)));
            return String.join(",", values).replaceFirst("^", "[") + "]";
        }
        return node.toString();
    }

    private static boolean isNumericResultType(String typeName) {
        if (typeName == null) {
            return false;
        }
        String upper = typeName.toUpperCase(Locale.ROOT);
        return upper.matches(".*\\b(?:DECIMAL|NUMERIC|TINYINT|SMALLINT|INT|INTEGER|BIGINT|"
                + "INT8|INT16|INT32|INT64|INT128|INT256|UINT8|UINT16|UINT32|UINT64|UINT128|"
                + "UINT256|FLOAT|FLOAT32|FLOAT64|DOUBLE|REAL)\\b.*");
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
        sb.append("Databend version: ").append(getSingleString("SELECT VERSION()")).append(System.lineSeparator());
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
        sb.append("Schema profile: ").append(pair.schema.schemaProfile).append(System.lineSeparator());
        sb.append("Source schema fidelity: ").append(sourceSchemaFidelity(pair.schema)).append(System.lineSeparator());
        sb.append(
                "Helper schema fidelity: projection-only; columns are nullable and constraints/defaults are not copied")
                .append(System.lineSeparator());
        sb.append("Copied source constraints: ").append(pair.schema.baseConstraints).append(System.lineSeparator());
        sb.append("Reconstruction strategy: ").append(pair.reconstructionStrategy).append(System.lineSeparator());
        sb.append("Description: ").append(pair.description).append(System.lineSeparator());
        Map<String, String> sessionSettings = captureSessionSettings();
        sb.append("Databend session settings: ").append(formatSessionSettings(sessionSettings))
                .append(System.lineSeparator());
        sb.append("Session SET statements:").append(System.lineSeparator());
        sb.append(formatSessionSetStatements(sessionSettings));
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

    private String sourceSchemaFidelity(VpSchema vp) {
        if (vp.schemaProfile == SchemaProfile.PROJECTION_ONLY) {
            return "column types only";
        }
        if (vp.schemaProfile == SchemaProfile.NULL_DEFAULT) {
            return "column types, nullability, and defaults; keys/constraints/generated expressions omitted";
        }
        return "column types, nullability, defaults, and supported PRIMARY/UNIQUE/CHECK constraints; foreign keys, indexes, and generated expressions omitted";
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
                getOrderedApproximateColumns(right, rightIndex),
                left.approximateComparisonEnabled && right.approximateComparisonEnabled);
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
                        right.approximateColumns,
                        left.approximateComparisonEnabled && right.approximateComparisonEnabled);
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

    private static final class ColumnDefinition {
        private final boolean nullable;
        private final String defaultExpression;
        private final String sqlType;

        private ColumnDefinition(boolean nullable, String defaultExpression, String sqlType) {
            this.nullable = nullable;
            this.defaultExpression = defaultExpression;
            this.sqlType = sqlType;
        }
    }

    private static final class ParsedTableDefinition {
        private final Map<String, ColumnDefinition> columns = new LinkedHashMap<>();
        private final List<String> constraints = new ArrayList<>();
    }

    private static ParsedTableDefinition parseTableDefinition(String createTable) {
        ParsedTableDefinition result = new ParsedTableDefinition();
        if (createTable == null) {
            return result;
        }
        int open = createTable.indexOf('(');
        int close = matchingParenthesis(createTable, open);
        if (open < 0 || close <= open) {
            return result;
        }
        for (String item : splitTopLevel(createTable.substring(open + 1, close))) {
            String trimmed = item.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String identifier = firstIdentifier(trimmed);
            String upper = trimmed.toUpperCase(Locale.ROOT);
            if (identifier == null || upper.startsWith("PRIMARY KEY") || upper.startsWith("UNIQUE")
                    || upper.startsWith("CONSTRAINT") || upper.startsWith("CHECK")) {
                if (isCopyableConstraint(upper)) {
                    result.constraints.add(trimmed);
                }
                continue;
            }
            int identifierEnd = identifierEnd(trimmed);
            String attributes = trimmed.substring(identifierEnd).trim();
            boolean nullable = !containsTopLevelKeyword(attributes, "NOT NULL");
            String defaultExpression = extractDefaultExpression(attributes);
            String sqlType = extractSqlType(attributes);
            result.columns.put(identifier.toLowerCase(Locale.ROOT),
                    new ColumnDefinition(nullable, defaultExpression, sqlType));
        }
        return result;
    }

    private static boolean isCopyableConstraint(String upper) {
        return upper.startsWith("PRIMARY KEY") || upper.startsWith("UNIQUE")
                || upper.startsWith("CONSTRAINT")
                        && (upper.contains(" PRIMARY KEY") || upper.contains(" UNIQUE") || upper.contains(" CHECK"))
                || upper.startsWith("CHECK");
    }

    private static String extractDefaultExpression(String attributes) {
        String upper = attributes.toUpperCase(Locale.ROOT);
        int defaultIndex = upper.indexOf("DEFAULT");
        if (defaultIndex < 0) {
            return null;
        }
        String expression = attributes.substring(defaultIndex + "DEFAULT".length()).trim();
        for (String terminator : List.of(" NOT NULL", " NULL", " COMMENT", " COLLATE", " PRIMARY KEY", " UNIQUE",
                " CHECK", " REFERENCES")) {
            int index = indexOfTopLevelIgnoreCase(expression, terminator.trim());
            if (index >= 0) {
                expression = expression.substring(0, index).trim();
            }
        }
        return expression.isEmpty() ? null : expression;
    }

    private static String extractSqlType(String attributes) {
        int end = attributes.length();
        for (String keyword : List.of("NOT NULL", "NULL", "DEFAULT", "COMMENT", "COLLATE", "PRIMARY KEY", "UNIQUE",
                "CHECK", "REFERENCES")) {
            int index = indexOfTopLevelIgnoreCase(attributes, keyword);
            if (index >= 0) {
                end = Math.min(end, index);
            }
        }
        String type = attributes.substring(0, end).trim();
        return type.isEmpty() ? null : type;
    }

    private static boolean containsTopLevelKeyword(String value, String keyword) {
        return indexOfTopLevelIgnoreCase(value, keyword) >= 0;
    }

    private static int indexOfTopLevelIgnoreCase(String value, String needle) {
        int depth = 0;
        boolean singleQuote = false;
        boolean doubleQuote = false;
        String upperNeedle = needle.toUpperCase(Locale.ROOT);
        for (int i = 0; i <= value.length() - needle.length(); i++) {
            char c = value.charAt(i);
            if (c == '\'' && !doubleQuote && !isBackslashEscaped(value, i)) {
                if (i + 1 < value.length() && value.charAt(i + 1) == '\'') {
                    i++;
                } else {
                    singleQuote = !singleQuote;
                }
            } else if (c == '"' && !singleQuote && !isBackslashEscaped(value, i)) {
                doubleQuote = !doubleQuote;
            } else if (!singleQuote && !doubleQuote) {
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                }
                if (depth == 0 && value.regionMatches(true, i, upperNeedle, 0, needle.length())
                        && (i == 0 || Character.isWhitespace(value.charAt(i - 1)))) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static List<String> splitTopLevel(String value) {
        List<String> result = new ArrayList<>();
        int start = 0;
        int depth = 0;
        int bracketDepth = 0;
        boolean singleQuote = false;
        boolean doubleQuote = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\'' && !doubleQuote && !isBackslashEscaped(value, i)) {
                if (i + 1 < value.length() && value.charAt(i + 1) == '\'') {
                    i++;
                } else {
                    singleQuote = !singleQuote;
                }
            } else if (c == '"' && !singleQuote && !isBackslashEscaped(value, i)) {
                doubleQuote = !doubleQuote;
            } else if (!singleQuote && !doubleQuote) {
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    depth--;
                } else if (c == '[') {
                    bracketDepth++;
                } else if (c == ']') {
                    bracketDepth--;
                } else if (c == ',' && depth == 0 && bracketDepth == 0) {
                    result.add(value.substring(start, i));
                    start = i + 1;
                }
            }
        }
        result.add(value.substring(start));
        return result;
    }

    private static int matchingParenthesis(String value, int open) {
        if (open < 0) {
            return -1;
        }
        int depth = 0;
        boolean singleQuote = false;
        for (int i = open; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\'' && !isBackslashEscaped(value, i)) {
                singleQuote = !singleQuote;
            } else if (!singleQuote) {
                if (c == '(') {
                    depth++;
                } else if (c == ')' && --depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static boolean isBackslashEscaped(String value, int index) {
        int backslashes = 0;
        for (int i = index - 1; i >= 0 && value.charAt(i) == '\\'; i--) {
            backslashes++;
        }
        return (backslashes & 1) != 0;
    }

    private static String firstIdentifier(String definition) {
        String trimmed = definition.trim();
        if (trimmed.startsWith("`") && trimmed.indexOf('`', 1) > 0) {
            return trimmed.substring(1, trimmed.indexOf('`', 1)).replace("``", "`");
        }
        int end = 0;
        while (end < trimmed.length() && !Character.isWhitespace(trimmed.charAt(end))) {
            end++;
        }
        return end == 0 ? null : trimmed.substring(0, end);
    }

    private static int identifierEnd(String definition) {
        String trimmed = definition.trim();
        if (trimmed.startsWith("`")) {
            int end = trimmed.indexOf('`', 1);
            return end < 0 ? trimmed.length() : end + 1;
        }
        int end = 0;
        while (end < trimmed.length() && !Character.isWhitespace(trimmed.charAt(end))) {
            end++;
        }
        return end;
    }

    private String extractEngineFromCreateTable(String createTable) {
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("(?i)\\bENGINE\\s*=\\s*([A-Za-z0-9_]+)")
                .matcher(createTable);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return "FUSE";
    }

    private String getSingleString(String sql) throws SQLException {
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getString(1);
            }
            return "<none>";
        }
    }

    /** Reads an optional numeric metadata value without turning NULL into a harness failure. */
    private Long readMetadataLong(String sql) throws SQLException {
        String value = getSingleString(sql);
        if (isUnavailableMetadataValue(value)) {
            return null;
        }
        try {
            return Long.valueOf(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static String sessionSettingsQuery() {
        return "SHOW SETTINGS WHERE name IN ("
                + REPRODUCIBILITY_SETTINGS.stream().map(name -> "'" + name + "'").collect(Collectors.joining(", "))
                + ")";
    }

    private Map<String, String> captureSessionSettings() {
        Map<String, String> values = new LinkedHashMap<>();
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(sessionSettingsQuery())) {
            while (result.next()) {
                values.put(result.getString("name"), result.getString("value"));
            }
            return values;
        } catch (SQLException e) {
            values.clear();
            values.put("", e.getMessage());
            return values;
        }
    }

    private String formatSessionSettings(Map<String, String> snapshot) {
        if (snapshot.containsKey("")) {
            return "<unavailable: " + snapshot.get("") + ">";
        }
        return snapshot.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(", "));
    }

    private String formatSessionSetStatements(Map<String, String> snapshot) {
        if (snapshot.containsKey("")) {
            return "-- Databend session settings unavailable: " + snapshot.get("") + System.lineSeparator();
        }
        return snapshot
                .entrySet().stream().map(entry -> "SET " + entry.getKey() + " = "
                        + sessionVariableLiteral(entry.getValue()) + ";" + System.lineSeparator())
                .collect(Collectors.joining());
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
                + quote(ROW_ID) + " LIMIT " + MAX_DIAGNOSTIC_ROWS;
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
            int rowCount = 0;
            while (rs.next() && rowCount++ < MAX_DIAGNOSTIC_ROWS && sb.length() < MAX_DIAGNOSTIC_CHARS) {
                List<String> values = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    values.add(rs.getString(i));
                }
                sb.append(values.stream().map(v -> v == null ? "NULL" : v).collect(Collectors.joining(" | ")))
                        .append(System.lineSeparator());
            }
            if (sb.length() >= MAX_DIAGNOSTIC_CHARS || rowCount >= MAX_DIAGNOSTIC_ROWS) {
                sb.append("<diagnostic output truncated>").append(System.lineSeparator());
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
        executeNoSchemaRefresh("DROP STREAM IF EXISTS " + quote(vp.prefix + "source_stream"));
        executeNoSchemaRefresh("DROP STREAM IF EXISTS " + quote(vp.prefix + "left_stream"));
        executeNoSchemaRefresh("DROP STREAM IF EXISTS " + quote(vp.prefix + "right_stream"));
        executeNoSchemaRefresh("DROP VIEW IF EXISTS " + quote(vp.viewName));
        // Databend accepts temporary tables but not MySQL's DROP TEMPORARY TABLE form.
        executeNoSchemaRefresh("DROP TABLE IF EXISTS " + quote(vp.tempTable));
        executeNoSchemaRefresh("DROP TABLE IF EXISTS " + quote(vp.prefix + "mutation_source_delta"));
        executeNoSchemaRefresh("DROP TABLE IF EXISTS " + quote(vp.prefix + "mutation_left_delta"));
        executeNoSchemaRefresh("DROP TABLE IF EXISTS " + quote(vp.prefix + "mutation_right_delta"));
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
            if (!handleQueryError(e, "setup", sql, null)) {
                throw e;
            }
        }
    }

    private void executeNoSchemaRefresh(String sql) throws Exception {
        if (streamLicenseExpired && normalizeKnownBugQuery(sql).startsWith("DROP STREAM IF EXISTS ")) {
            return;
        }
        try {
            state.executeStatement(new SQLQueryAdapter(sql, errors, false));
        } catch (AssertionError e) {
            if (canContinueStreamCleanup(sql, getErrorText(e))) {
                streamLicenseExpired = true;
                recordSkippedQueryError(QueryErrorDisposition.LICENSE_RESTRICTION, "cleanup", null, sql,
                        getErrorText(e));
                return;
            }
            if (handleQueryError(e, "cleanup", sql, null)) {
                throw new IgnoreMeException();
            }
            throw e;
        }
    }

    private static String toSQLType(DatabendDataType type) {
        switch (type) {
        case INT:
            return "BIGINT";
        case FLOAT:
            return "DOUBLE";
        case VARCHAR:
            return "VARCHAR";
        case TEXT:
        case CHAR:
            return "TEXT";
        case FLOATING:
            return "DOUBLE";
        case DECIMAL:
        case NUMERIC:
            return "DECIMAL(65, 30)";
        case BOOLEAN:
        case BOOL:
            return "BOOLEAN";
        case BLOB:
            return "BLOB";
        case DATE:
            return "DATE";
        case TIMESTAMP:
            return "TIMESTAMP";
        case DATETIME:
            return "DATETIME";
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
        default:
            throw new AssertionError(type);
        }
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private String sanitizeIdentifier(String identifier) {
        return identifier.replaceAll("[^A-Za-z0-9_]", "_");
    }
}
