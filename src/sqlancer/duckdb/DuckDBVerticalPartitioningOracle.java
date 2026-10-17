package sqlancer.duckdb;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBSchema.DuckDBColumn;
import sqlancer.duckdb.DuckDBSchema.DuckDBCompositeDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBDataType;
import sqlancer.duckdb.DuckDBSchema.DuckDBTable;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator;
import sqlancer.duckdb.gen.DuckDBExpressionGenerator.DuckDBAggregateFunction;
import sqlancer.duckdb.ast.DuckDBConstant;
import sqlancer.duckdb.ast.DuckDBColumnReference;
import sqlancer.duckdb.ast.DuckDBExpression;
import sqlancer.duckdb.ast.DuckDBFunction;
import sqlancer.duckdb.ast.DuckDBSelect;
import sqlancer.duckdb.ast.DuckDBTableReference;
import sqlancer.duckdb.ast.DuckDBText;

public class DuckDBVerticalPartitioningOracle implements TestOracle<DuckDBGlobalState> {

    private static final AtomicLong ORACLE_RUN_ID = new AtomicLong();
    private static final String ROW_ID = "vp_rowid";
    private static final List<String> DUCKDB_ROW_ID_CANDIDATES = List.of("rowid", "_rowid_", "oid");
    private static final int MAX_DIFF_ROWS = 8;
    private static final int MAX_RESULT_SAMPLE = 12;
    private static final double FLOAT_ABSOLUTE_TOLERANCE = 1e-6;
    private static final double FLOAT_RELATIVE_TOLERANCE = 1e-5;
    private static final double DOUBLE_ABSOLUTE_TOLERANCE = 1e-12;
    private static final double DOUBLE_RELATIVE_TOLERANCE = 1e-9;
    private static final String VP_SPLIT_DISABLED_OPTIMIZERS = "filter_pushdown,join_order";
    private static final AtomicLong OPTIMIZER_MODE_CURSOR = new AtomicLong();
    /*
     * DuckDB JDBC executes through a process-wide native bridge. Concurrent VP pairs from separate SQLancer workers
     * have repeatedly crashed in duckdb_jdbc_execute_pending, even when each connection uses one DuckDB execution
     * thread. Serialize only the paired query window; database setup and ordinary random statements remain concurrent.
     */
    private static final Object VP_NATIVE_EXECUTION_LOCK = new Object();
    private static final int[] VECTOR_BOUNDARY_ROWS = { 2047, 2048, 2049 };
    private static final int[] ROW_GROUP_BOUNDARY_ROWS = { 122879, 122880, 122881 };
    private static final long LIMIT0_CTE_SMALL_CRASH_MIN_ROWS = 4096;
    private static final long LIMIT0_CTE_CRASH_MIN_ROWS = ROW_GROUP_BOUNDARY_ROWS[0];
    private static final String STORAGE_BOUNDARY_MEMORY_LIMIT = "256MB";
    private static final Object STATS_LOCK = new Object();
    private static final Object STATS_WRITE_LOCK = new Object();
    private static final Map<SplitStrategy, Integer> SPLIT_STRATEGY_COUNTS = new EnumMap<>(SplitStrategy.class);
    private static final Map<BaseShape, Integer> SELECTED_BASE_SHAPE_COUNTS = new EnumMap<>(BaseShape.class);
    private static final Map<ReconstructionStrategy, Integer> RECONSTRUCTION_STRATEGY_COUNTS = new EnumMap<>(
            ReconstructionStrategy.class);
    private static final Map<KnownVpBug, Integer> KNOWN_BUG_COUNTS = new EnumMap<>(KnownVpBug.class);
    private static final Map<AdvancedFeature, Integer> ADVANCED_FEATURE_COUNTS = new EnumMap<>(AdvancedFeature.class);
    private static final Map<DeepFeature, Integer> DEEP_FEATURE_COUNTS = new EnumMap<>(DeepFeature.class);
    private static final Map<String, Integer> SCHEMA_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SOURCE_ROW_COUNT_BUCKETS = new LinkedHashMap<>();
    private static final Map<OptimizerMode, Integer> OPTIMIZER_MODE_COUNTS = new EnumMap<>(OptimizerMode.class);
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean();
    private static final AtomicLong STATS_UPDATE_COUNT = new AtomicLong();
    private static final AtomicLong LAST_STATS_PERSIST_NANOS = new AtomicLong();
    private static final AtomicLong KNOWN_BUG_LOG_ID = new AtomicLong();
    private static final Object NATIVE_CRASH_CONTEXT_LOCK = new Object();
    private static final long STATS_CHECKPOINT_INTERVAL = 1000;
    private static final long STATS_CHECKPOINT_PERIOD_NANOS = 5_000_000_000L;
    private static final File STATS_FILE = new File(new File(Main.LOG_DIRECTORY, "duckdb"), "vp-stats.log");
    private static final Pattern DUCKDB_CHAR_FUNCTION = Pattern.compile("(?i)(^|[^A-Z0-9_])CHAR\\s*\\(");
    private static final Pattern DUCKDB_EQUIVALENCE_COLLATION = Pattern
            .compile("(?i)\\bCOLLATE\\s+(NOCASE|C|POSIX)\\b");
    private static final Pattern INTERVAL_HMS_PATTERN = Pattern
            .compile("^(-?)(\\d+):(\\d{2}):(\\d{2})(?:\\.(\\d{1,6}))?$");
    private static final BigInteger PARQUET_UINT32_MODULUS = BigInteger.ONE.shiftLeft(32);
    private static int completedOracleRuns;
    private static int completedQueryPairs;
    private static int completedAttempts;
    private static int schemaShapeSamples;
    private static int sourceRowCountSamples;

    private final DuckDBGlobalState state;
    private final ExpectedErrors errors = new ExpectedErrors();
    private Boolean jsonExtensionAvailable;
    private Boolean ftsExtensionAvailable;
    private Boolean spatialExtensionAvailable;
    private int storageBoundaryTargetRows = -1;
    private String storageBoundaryOriginalMemoryLimit;
    private String storageBoundaryOriginalCheckpointThreshold;
    private String storageBoundaryProfileName;

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

    private enum OptimizerMode {
        DEFAULT(""), DISABLED_BOTH("filter_pushdown,join_order"), DISABLED_FILTER("filter_pushdown"),
        DISABLED_JOIN("join_order");

        private final String disabledOptimizers;

        OptimizerMode(String disabledOptimizers) {
            this.disabledOptimizers = disabledOptimizers;
        }
    }

    private enum KnownVpBug {
        DUCKDB_GROUP_BY_ALL_NOCASE_COLLATION_MISMATCH, DUCKDB_NATIVE_BASIC_STRING_NULL_DEEP_VP_EXECUTION,
        // DuckDB issues #24794, #25601 and #25663 are one native LIMIT 0
        // VP crash family. Keep one pool key so a newly observed shape is
        // absorbed together with the previously known LEFT_DELIM/MARK JOIN,
        // correlated reconstruction, and rank/aggregate variants.
        DUCKDB_NATIVE_LIMIT0_VP_CRASH, DUCKDB_NATIVE_EMPTY_FILTER_BINDING_DEEP_VP_EXECUTION,
        DUCKDB_HISTOGRAM_NONFINITE_ORDER_DEPENDENCE, DUCKDB_PARQUET_INTERVAL_UINT32_MILLISECONDS_OVERFLOW,
        DUCKDB_PARQUET_INTERVAL_MILLISECOND_PRECISION
    }

    public enum AdvancedFeature {
        ROLLUP, CORRELATED_SUBQUERY, GROUP_CONCAT, ASYMMETRIC_INDEX, CHARSET_COLLATION_COLUMN, BOUNDARY_MUTATION,
        FRIENDLY_SQL_REWRITE, UNION_BY_NAME, DISTINCT_ON, QUALIFY_WINDOW, UNNEST_LIST, STRUCT_FIELD_PROJECTION,
        UPDATE_FROM, DELETE_USING, MERGE, ON_CONFLICT, POSITIONAL_JOIN, ASOF_JOIN, SAMPLE, PIVOT, UNPIVOT,
        AGGREGATE_FILTER, ORDERED_AGGREGATE, WINDOW_ROWS_FRAME, ARG_MAX_MIN, MODE, QUANTILE_HISTOGRAM, LATERAL_QUERY,
        RISKY_PREDICATE,
        RECURSIVE_CTE, PARQUET_ROUNDTRIP, PARQUET_COMPRESSION, PARQUET_ROW_GROUP, PARQUET_EMPTY, PARQUET_SINGLE_ROW,
        PARQUET_PROJECTION_FILTER, PARQUET_UNION_BY_NAME, JSON_QUERY, FTS_QUERY, FTS_UNAVAILABLE, SPATIAL_QUERY,
        SPATIAL_UNAVAILABLE, STORAGE_BOUNDARY
    }

    private enum DeepFeature {
        NATIVE_ROLLUP, GROUPING_SETS, CONSTRAINTS, TRANSACTION_SNAPSHOT, VIEW_TEMP_MATERIALIZATION, BOUNDARY_MUTATION
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
        private final DuckDBCompositeDataType type;
        private final boolean nullable;

        private VpColumn(String name, DuckDBCompositeDataType type) {
            this(name, type, true);
        }

        private VpColumn(String name, DuckDBCompositeDataType type, boolean nullable) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
        }
    }

    private static final class BaseTableInfo {
        private final String tableName;
        private final List<VpColumn> columns;
        private final BaseShape shape;
        private final String stableOrderStrategy;
        private final long rowCount;
        private final String engine;
        private final String rowIdExpression;
        private final VpRowIdType rowIdType;

        private BaseTableInfo(String tableName, List<VpColumn> columns, BaseShape shape, String stableOrderStrategy,
                long rowCount, String engine, String rowIdExpression, VpRowIdType rowIdType) {
            this.tableName = tableName;
            this.columns = columns;
            this.shape = shape;
            this.stableOrderStrategy = stableOrderStrategy;
            this.rowCount = rowCount;
            this.engine = engine;
            this.rowIdExpression = rowIdExpression;
            this.rowIdType = rowIdType;
        }
    }

    private static final class VpRowIdType {
        private final String sqlType;
        private final int astTypeSize;

        private VpRowIdType(String sqlType, int astTypeSize) {
            this.sqlType = sqlType;
            this.astTypeSize = astTypeSize;
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
        private final String rowIdExpression;
        private final VpRowIdType rowIdType;
        private final Map<String, String> columnCollationClauses = new LinkedHashMap<>();
        private String sourceEngine;
        private String leftEngine;
        private String rightEngine;
        private final List<String> setupStatements = new ArrayList<>();

        private VpSchema(String prefix, String baseTable, String sourceTable, String leftTable, String rightTable,
                String viewName, String tempTable, List<VpColumn> columns, List<VpColumn> leftColumns,
                List<VpColumn> rightColumns, List<VpColumn> overlapColumns, BaseShape baseShape,
                SplitStrategy splitStrategy, boolean usesPhysicalForeignKey, String stableOrderStrategy,
                long baseRowCount, String baseEngine, String rowIdExpression, VpRowIdType rowIdType) {
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
            this.rowIdExpression = rowIdExpression;
            this.rowIdType = rowIdType;
            this.sourceEngine = "ROWID";
            this.leftEngine = "ROWID";
            this.rightEngine = "ROWID";
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

    private static final class CollationFallbackRequiredException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    private static final class QueryResultMultiset {
        private final Map<String, Integer> rows;
        private final Map<String, List<ApproximateKind>> approximateColumns;
        private final List<String> orderedRows;
        private final List<List<ApproximateKind>> orderedApproximateColumns;
        private final List<String> columnTypes;

        private QueryResultMultiset(Map<String, Integer> rows, Map<String, List<ApproximateKind>> approximateColumns,
                List<String> orderedRows, List<List<ApproximateKind>> orderedApproximateColumns,
                List<String> columnTypes) {
            this.rows = rows;
            this.approximateColumns = approximateColumns;
            this.orderedRows = orderedRows;
            this.orderedApproximateColumns = orderedApproximateColumns;
            this.columnTypes = List.copyOf(columnTypes);
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

    public DuckDBVerticalPartitioningOracle(DuckDBGlobalState state) {
        this.state = state;
        registerVpStatsShutdownHook();
        DuckDBErrors.addExpressionErrors(errors);
        // Keep this list limited to DuckDB expression/setup failures. Query-pair execution uses
        // an empty ExpectedErrors instance so an error on only one side remains observable.
        errors.add("doesn't yet support");
        errors.add("Unsupported");
        errors.add("Not implemented");
        errors.add("Invalid JSON");
    }

    @Override
    public void check() throws Exception {
        configureStorageBoundarySession();
        try {
            checkVerticalPartitioning();
        } finally {
            restoreStorageBoundarySession();
        }
    }

    private void checkVerticalPartitioning() throws Exception {
        resetSessionForVerticalPartitioningOracle();
        // Record extension availability independently of random feature
        // selection. A zero FTS/SPATIAL execution count must not be confused
        // with an environment where the extensions are unavailable.
        if (advancedFeaturesEnabled()) {
            isFtsExtensionAvailable();
            isSpatialExtensionAvailable();
        }
        DuckDBProvider.ensureVerticalPartitioningBaseTables(state, 2);
        BaseTableInfo baseTable = selectBaseTable();
        int queryVariantsPerCombination = Math.max(1,
                state.getDbmsSpecificOptions().verticalPartitioningQueryVariantsPerCombination);
        int checkedPairs = 0;
        int attempts = 0;
        List<SplitStrategy> splitSchedule = createSplitSchedule(baseTable.shape);
        boolean deepFeatureChecksCompleted = false;
        boolean parquetRoundTripCompleted = false;
        for (SplitStrategy splitStrategy : splitSchedule) {
            VpSchema vp = materializeTable(baseTable, splitStrategy);
            incrementSplitStrategy(vp.splitStrategy);
            if (state.getDbmsSpecificOptions().verticalPartitioningParquetRoundTrip && !parquetRoundTripCompleted
                    && supportsParquetRoundTrip(vp)) {
                runParquetRoundTripCheck(vp);
                parquetRoundTripCompleted = true;
            }
            if (deepExplorationEnabled() && !deepFeatureChecksCompleted) {
                runDeepDuckDBFeatureChecks(vp);
                deepFeatureChecksCompleted = true;
            }
            if (advancedFeaturesEnabled()) {
                runInstalledExtensionWorkloads(vp);
            }
            List<ReconstructionStrategy> reconstructionSchedule = createReconstructionSchedule(vp);
            for (ReconstructionStrategy strategy : reconstructionSchedule) {
                for (int i = 0; i < queryVariantsPerCombination; i++) {
                    attempts++;
                    try {
                        VpQueryPair pair = generateRandomRewritePair(vp, strategy);
                        OptimizerMode optimizerMode = nextOptimizerMode();
                        recordOptimizerMode(optimizerMode);
                        incrementReconstructionStrategy(pair.reconstructionStrategy);
                        comparePair(pair, optimizerMode);
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
        DuckDBProvider.replaceVerticalPartitioningTableAfterSuccessfulCheck(state, baseTable.tableName);
    }

    private boolean supportsParquetRoundTrip(VpSchema vp) {
        return vp.columns.stream().allMatch(column -> column.type.supportsParquetRoundTrip());
    }

    private void runInstalledExtensionWorkloads(VpSchema vp) throws Exception {
        List<String> setup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, ReconstructionStrategy.JOIN, setup);
        for (String statement : setup) {
            tryExecuteNoSchemaRefresh(statement);
        }
        if (isFtsExtensionAvailable()) {
            try {
                VpQueryPair pair = ftsQueryPair(vp, ReconstructionStrategy.JOIN, relation, new ArrayList<>());
                comparePair(pair);
                recordAdvancedFeature(AdvancedFeature.FTS_QUERY);
            } catch (IgnoreMeException | SQLException ignored) {
                // Extension-specific version/type limitations should not stop VP.
            }
        }
        if (isSpatialExtensionAvailable()) {
            try {
                VpQueryPair pair = spatialQueryPair(vp, ReconstructionStrategy.JOIN, relation, new ArrayList<>());
                comparePair(pair);
                recordAdvancedFeature(AdvancedFeature.SPATIAL_QUERY);
            } catch (IgnoreMeException | SQLException ignored) {
                // Extension-specific version/type limitations should not stop VP.
            }
        }
    }

    private void runParquetRoundTripCheck(VpSchema vp) throws Exception {
        String[] compressions = { "UNCOMPRESSED", "SNAPPY", "ZSTD", "GZIP" };
        int ordinal = 0;
        for (String compression : compressions) {
            Path parquet = Files.createTempFile("sqlancer-duckdb-vp-" + compression.toLowerCase(Locale.ROOT) + "-",
                    ".parquet");
            Files.deleteIfExists(parquet);
            String path = "'" + escapeLiteral(parquet.toAbsolutePath().toString()) + "'";
            try {
                int rowGroupSize = ordinal++ % 2 == 0 ? 2048 : 122880;
                try (Statement statement = state.getConnection().createStatement()) {
                    statement.execute("COPY " + quote(vp.rightTable) + " TO " + path + " (FORMAT PARQUET, COMPRESSION "
                            + compression + ", ROW_GROUP_SIZE " + rowGroupSize + ")");
                }
                String parquetRelation = "read_parquet(" + path + ")";
                String reconstruction = parquetReconstructionRelation(vp, parquetRelation);
                comparePair(parquetPair(vp, "SELECT * FROM " + quote(vp.sourceTable) + " ORDER BY " + quote(ROW_ID),
                        "SELECT * FROM " + reconstruction + " ORDER BY " + quote(ROW_ID),
                        "PARQUET_RIGHT_PARTITION_RECONSTRUCTION_" + compression));
                recordAdvancedFeature(AdvancedFeature.PARQUET_ROUNDTRIP);
                recordAdvancedFeature(AdvancedFeature.PARQUET_COMPRESSION);
                recordAdvancedFeature(AdvancedFeature.PARQUET_ROW_GROUP);
                runParquetProjectionFilterChecks(vp, parquetRelation, compression);
            } catch (IgnoreMeException ignored) {
                // Known Parquet representation differences are classified by comparePair.
            } catch (SQLException e) {
                if (!isSkippableResultReadError(e) && !isSkippableQueryErrorText(getErrorText(e))
                        && !isParquetUnsupportedTypeError(e)) {
                    throw e;
                }
            } finally {
                Files.deleteIfExists(parquet);
            }
        }
        runParquetCardinalityBoundary(vp, true);
        runParquetCardinalityBoundary(vp, false);
    }

    private void runParquetProjectionFilterChecks(VpSchema vp, String parquetRelation, String compression)
            throws Exception {
        String row = quote(ROW_ID);
        String projected = vp.rightColumns.isEmpty() ? row : quote(vp.rightColumns.get(0).name);
        comparePair(parquetPair(vp,
                "SELECT COUNT(*), COUNT(" + projected + ") FROM " + quote(vp.rightTable) + " WHERE " + row
                        + " % 3 <> 1",
                "SELECT COUNT(*), COUNT(" + projected + ") FROM " + parquetRelation + " WHERE " + row + " % 3 <> 1",
                "PARQUET_PROJECTION_FILTER_" + compression));
        recordAdvancedFeature(AdvancedFeature.PARQUET_PROJECTION_FILTER);
        comparePair(parquetPair(vp,
                "SELECT COUNT(*) FROM (SELECT " + row + " FROM " + quote(vp.rightTable) + " UNION BY NAME SELECT " + row
                        + " FROM " + quote(vp.rightTable) + " WHERE FALSE) u",
                "SELECT COUNT(*) FROM (SELECT " + row + " FROM " + parquetRelation + " UNION BY NAME SELECT " + row
                        + " FROM " + parquetRelation + " WHERE FALSE) u",
                "PARQUET_UNION_BY_NAME_" + compression));
        recordAdvancedFeature(AdvancedFeature.PARQUET_UNION_BY_NAME);
    }

    private void runParquetCardinalityBoundary(VpSchema vp, boolean empty) throws Exception {
        Path parquet = Files.createTempFile("sqlancer-duckdb-vp-cardinality-", ".parquet");
        Files.deleteIfExists(parquet);
        String path = "'" + escapeLiteral(parquet.toAbsolutePath().toString()) + "'";
        String row = quote(ROW_ID);
        String suffix = empty ? " WHERE FALSE" : " ORDER BY " + row + " LIMIT 1";
        try {
            try (Statement statement = state.getConnection().createStatement()) {
                statement.execute("COPY (SELECT * FROM " + quote(vp.rightTable) + suffix + ") TO " + path
                        + " (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 2048)");
            }
            comparePair(parquetPair(vp, "SELECT COUNT(*) FROM (SELECT * FROM " + quote(vp.rightTable) + suffix + ") q",
                    "SELECT COUNT(*) FROM read_parquet(" + path + ")", empty ? "PARQUET_EMPTY" : "PARQUET_SINGLE_ROW"));
            recordAdvancedFeature(empty ? AdvancedFeature.PARQUET_EMPTY : AdvancedFeature.PARQUET_SINGLE_ROW);
        } catch (IgnoreMeException ignored) {
        } catch (SQLException e) {
            if (!isSkippableQueryErrorText(getErrorText(e)) && !isParquetUnsupportedTypeError(e)) {
                throw e;
            }
        } finally {
            Files.deleteIfExists(parquet);
        }
    }

    private boolean isParquetUnsupportedTypeError(SQLException error) {
        String message = getErrorText(error).toLowerCase(Locale.ROOT);
        return message.contains("parquet files do not support negative intervals");
    }

    private String parquetReconstructionRelation(VpSchema vp, String parquetRelation) {
        List<String> projection = new ArrayList<>();
        projection.add("l." + quote(ROW_ID) + " AS " + quote(ROW_ID));
        for (VpColumn column : vp.columns) {
            String alias = containsColumn(vp.leftColumns, column.name) ? "l" : "p";
            projection.add(alias + "." + quote(column.name) + " AS " + quote(column.name));
        }
        return reconstructedSubquery(vp, String.join(", ", projection), " FROM " + quote(vp.leftTable) + " l JOIN "
                + parquetRelation + " p ON l." + quote(ROW_ID) + " = p." + quote(ROW_ID));
    }

    private VpQueryPair parquetPair(VpSchema vp, String source, String parquet, String feature) {
        return new VpQueryPair(vp, ReconstructionStrategy.JOIN, source, parquet, List.of(),
                "shape=" + vp.baseShape + ", split=" + vp.splitStrategy + ", parquet=" + feature, true);
    }

    private BaseTableInfo selectBaseTable() throws Exception {
        state.updateSchema();
        List<DuckDBTable> candidates = state.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> !t.getName().startsWith("vp_")).filter(t -> !t.getColumns().isEmpty())
                .collect(Collectors.toList());
        if (candidates.isEmpty()) {
            throw new IgnoreMeException();
        }
        return createBaseTableInfo(Randomly.fromList(candidates));
    }

    private BaseTableInfo createBaseTableInfo(DuckDBTable base) throws Exception {
        List<VpColumn> sourceColumns = base.getColumns().stream().filter(c -> !c.isSyntheticRowId())
                .map(c -> new VpColumn(c.getName(), c.getType(), c.isNullable())).collect(Collectors.toList());
        long rowCount = getRowCount(base.getName());
        BaseShape shape = rowCount == 0 ? BaseShape.EMPTY
                : sourceColumns.size() == 1 ? BaseShape.SINGLE_COLUMN : BaseShape.MULTI_COLUMN;
        incrementSelectedBaseShape(shape);
        String stableOrder = getStableOrder(base, sourceColumns);
        String rowIdExpression = getRowIdExpression(base, stableOrder);
        VpRowIdType rowIdType = getRowIdType(base.getName(), rowIdExpression);
        return new BaseTableInfo(base.getName(), sourceColumns, shape, stableOrder, rowCount,
                extractEngineFromCreateTable(getCreateTable(base.getName())), rowIdExpression, rowIdType);
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
        List<ReconstructionStrategy> shuffled = new ArrayList<>(SUPPORTED_RECONSTRUCTION_STRATEGIES);
        if (vp.columns.stream().anyMatch(c -> !c.type.supportsReconstruction("JSON_PACK"))) {
            shuffled.remove(ReconstructionStrategy.JSON_PACK);
            shuffled.remove(ReconstructionStrategy.STRING_PACK);
        }
        Collections.shuffle(shuffled);
        return shuffled;
    }

    private void resetSessionForVerticalPartitioningOracle() throws Exception {
        String threshold = storageBoundaryTestsEnabled() && "rowgroup".equals(storageBoundaryProfileName) ? "1MB"
                : "1 byte";
        tryExecuteNoSchemaRefresh("PRAGMA checkpoint_threshold='" + threshold + "'");
    }

    private boolean storageBoundaryTestsEnabled() {
        return state.getDbmsSpecificOptions().verticalPartitioningStorageBoundaryTests;
    }

    private void configureStorageBoundarySession() throws Exception {
        if (!storageBoundaryTestsEnabled()) {
            return;
        }
        String profile = storageBoundaryProfile();
        storageBoundaryProfileName = profile;
        storageBoundaryTargetRows = chooseStorageBoundaryRows(profile);
        try {
            storageBoundaryOriginalCheckpointThreshold = getSingleString("PRAGMA checkpoint_threshold");
        } catch (SQLException ignored) {
            // DuckDB builds without checkpoint_threshold still support CHECKPOINT;
            // threshold tuning is optional and must not fail the oracle itself.
            storageBoundaryOriginalCheckpointThreshold = null;
        }
        if (profile.equals("memory") || profile.equals("all")) {
            storageBoundaryOriginalMemoryLimit = getSingleString("SELECT current_setting('memory_limit')");
            executeNoSchemaRefresh("SET memory_limit='" + STORAGE_BOUNDARY_MEMORY_LIMIT + "'");
        }
    }

    private void restoreStorageBoundarySession() {
        if (storageBoundaryOriginalMemoryLimit == null && storageBoundaryOriginalCheckpointThreshold == null) {
            storageBoundaryProfileName = null;
            return;
        }
        try {
            if (storageBoundaryOriginalMemoryLimit != null) {
                executeNoSchemaRefresh("SET memory_limit='" + escapeLiteral(storageBoundaryOriginalMemoryLimit) + "'");
            }
            if (storageBoundaryOriginalCheckpointThreshold != null) {
                executeNoSchemaRefresh("PRAGMA checkpoint_threshold='"
                        + escapeLiteral(storageBoundaryOriginalCheckpointThreshold) + "'");
            }
        } catch (Exception ignored) {
            // The query result is the useful signal; session cleanup is best effort.
        } finally {
            storageBoundaryOriginalMemoryLimit = null;
            storageBoundaryOriginalCheckpointThreshold = null;
            storageBoundaryProfileName = null;
        }
    }

    private String storageBoundaryProfile() {
        String profile = state.getDbmsSpecificOptions().verticalPartitioningStorageBoundaryProfile;
        if (profile == null) {
            return "all";
        }
        profile = profile.toLowerCase(Locale.ROOT).trim();
        if (!profile.equals("vector") && !profile.equals("rowgroup") && !profile.equals("memory")
                && !profile.equals("all")) {
            throw new IllegalArgumentException("Unknown --vp-storage-boundary-profile: " + profile
                    + " (expected vector, rowgroup, memory, or all)");
        }
        return profile;
    }

    private int chooseStorageBoundaryRows(String profile) {
        int[] candidates;
        if (profile.equals("rowgroup") || profile.equals("memory")) {
            candidates = ROW_GROUP_BOUNDARY_ROWS;
        } else if (profile.equals("vector")) {
            candidates = VECTOR_BOUNDARY_ROWS;
        } else {
            candidates = Randomly.getBoolean() ? VECTOR_BOUNDARY_ROWS : ROW_GROUP_BOUNDARY_ROWS;
        }
        return candidates[state.getRandomly().getInteger(0, candidates.length - 1)];
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
                useForeignKey, base.stableOrderStrategy, base.rowCount, base.engine, base.rowIdExpression,
                base.rowIdType);
        initializeColumnCollations(vp);
        try {
            materializeTables(vp, helperEngineCandidate(base), useForeignKey);
        } catch (CollationFallbackRequiredException ignored) {
            clearColumnCollations(vp);
            dropArtifacts(vp);
            materializeTables(vp, helperEngineCandidate(base), useForeignKey);
        }
        return vp;
    }

    private void materializeTables(VpSchema vp, String preferredEngine, boolean useForeignKey) throws Exception {
        dropArtifacts(vp);
        createSourceTable(vp, preferredEngine);
        populateSourceTable(vp, vp.stableOrderStrategy);
        copyBaseSchemaObjectsToSource(vp);
        createPartitionTables(vp, preferredEngine);
        populatePartitionTables(vp);
        maybeCreateHelperIndexes(vp);
        forceStorageBoundaryCheckpoint(vp);
        if (useForeignKey) {
            tryExecuteSetup(vp,
                    "ALTER TABLE " + quote(vp.rightTable) + " ADD CONSTRAINT " + quote(vp.prefix + "fk_r_l")
                            + " FOREIGN KEY (" + quote(ROW_ID) + ") REFERENCES " + quote(vp.leftTable) + "("
                            + quote(ROW_ID) + ")");
        }
    }

    private void runDeepDuckDBFeatureChecks(VpSchema vp) throws Exception {
        runDeepNativeRollupCheck(vp);
        runDeepGroupingSetsCheck(vp);
        runDeepConstraintChecks(vp);
        runDeepTransactionSnapshotCheck(vp);
        runDeepViewAndTempMaterializationCheck(vp);
        runDeepBoundaryValueMutationCheck(vp);
    }

    private void runDeepNativeRollupCheck(VpSchema vp) throws Exception {
        String groupKey = quote(ROW_ID);
        String sourceQuery = "SELECT " + groupKey + " AS vp_group_key, COUNT(*) AS vp_count FROM "
                + sourceRelation(vp.sourceTable) + " GROUP BY ROLLUP (" + groupKey + ")";
        String reconstruction = reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID)
                        + " = r." + quote(ROW_ID));
        String splitQuery = "SELECT " + groupKey + " AS vp_group_key, COUNT(*) AS vp_count FROM " + reconstruction
                + " GROUP BY ROLLUP (" + groupKey + ")";
        assertDeepQueriesEquivalent("native ROLLUP", sourceQuery, splitQuery);
        recordDeepFeature(DeepFeature.NATIVE_ROLLUP);
    }

    private void runDeepGroupingSetsCheck(VpSchema vp) throws Exception {
        String groupKey = quote(ROW_ID);
        String select = "SELECT " + groupKey + " AS vp_group_key, GROUPING(" + groupKey
                + ") AS vp_grouping, COUNT(*) AS vp_count FROM ";
        String groupingSets = " GROUP BY GROUPING SETS ((" + groupKey + "), ())";
        String reconstruction = reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID)
                        + " = r." + quote(ROW_ID));
        assertDeepQueriesEquivalent("GROUPING SETS", select + sourceRelation(vp.sourceTable) + groupingSets,
                select + reconstruction + groupingSets);
        recordDeepFeature(DeepFeature.GROUPING_SETS);
    }

    private void runDeepConstraintChecks(VpSchema vp) throws Exception {
        String parent = vp.prefix + "deep_parent";
        String child = vp.prefix + "deep_child";
        try {
            executeDeepStatement("CREATE TABLE " + quote(parent) + " (id INTEGER PRIMARY KEY)");
            executeDeepStatement(
                    "CREATE TABLE " + quote(child) + " (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES "
                            + quote(parent) + "(id), token INTEGER NOT NULL, UNIQUE(parent_id, token))");
            executeDeepStatement("INSERT INTO " + quote(parent) + " VALUES (1)");
            executeDeepStatement("INSERT INTO " + quote(child) + " VALUES (1, 1, 7)");
            if (!statementIsRejected("INSERT INTO " + quote(child) + " VALUES (2, 1, 7)")) {
                throw new AssertionError("VP deep DuckDB UNIQUE constraint accepted a duplicate");
            }
            if (!statementIsRejected("INSERT INTO " + quote(child) + " VALUES (3, 2, 8)")) {
                throw new AssertionError("VP deep DuckDB foreign key accepted a missing parent");
            }
            if (!statementIsRejected("INSERT INTO " + quote(child) + " VALUES (4, 1, NULL)")) {
                throw new AssertionError("VP deep DuckDB NOT NULL constraint accepted NULL");
            }
            recordDeepFeature(DeepFeature.CONSTRAINTS);
        } finally {
            dropDeepTable(child);
            dropDeepTable(parent);
        }
    }

    private void runDeepTransactionSnapshotCheck(VpSchema vp) throws Exception {
        try (Statement statement = state.getConnection().createStatement()) {
            boolean transactionOpen = false;
            statement.execute("BEGIN TRANSACTION");
            transactionOpen = true;
            try {
                String reconstruction = "SELECT " + joinProjection(vp, "l", "r", false) + " FROM " + quote(vp.leftTable)
                        + " l JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID);
                String source = "SELECT " + sourceProjection(vp) + " FROM " + quote(vp.sourceTable);
                boolean jdbcMatches = resultMultisetsMatch(getResultMultiset(source), getResultMultiset(reconstruction));
                if (!jdbcMatches) {
                    throw new AssertionError("VP deep DuckDB transaction snapshot changed JDBC reconstructed rows");
                }
                long differences = queryLong(statement,
                        bidirectionalDifferenceQuery(source, reconstruction, "transaction_snapshot_diff"));
                statement.execute("ROLLBACK");
                transactionOpen = false;
                if (differences != 0) {
                    throw new AssertionError("VP deep DuckDB transaction snapshot changed reconstructed rows");
                }
                recordDeepFeature(DeepFeature.TRANSACTION_SNAPSHOT);
            } finally {
                if (transactionOpen) {
                    try {
                        statement.execute("ROLLBACK");
                    } catch (SQLException ignored) {
                    }
                }
            }
        }
    }

    private void runDeepViewAndTempMaterializationCheck(VpSchema vp) throws Exception {
        String deepView = vp.prefix + "deep_v";
        String deepTemp = vp.prefix + "deep_temp";
        String reconstruction = joinProjection(vp, "l", "r", false) + " FROM " + quote(vp.leftTable) + " l JOIN "
                + quote(vp.rightTable) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID);
        try {
            executeDeepStatement("CREATE VIEW " + quote(deepView) + " AS SELECT " + reconstruction);
            executeDeepStatement("CREATE TEMP TABLE " + quote(deepTemp) + " AS SELECT " + reconstruction);
            long sourceCount = queryLong("SELECT COUNT(*) FROM " + quote(vp.sourceTable));
            long viewCount = queryLong("SELECT COUNT(*) FROM " + quote(deepView));
            long tempCount = queryLong("SELECT COUNT(*) FROM " + quote(deepTemp));
            if (sourceCount != viewCount || sourceCount != tempCount) {
                throw new AssertionError("VP deep DuckDB view/temp materialization changed row ownership");
            }
            recordDeepFeature(DeepFeature.VIEW_TEMP_MATERIALIZATION);
        } finally {
            dropDeepTable(deepView);
            dropDeepTable(deepTemp);
        }
    }

    private void runDeepBoundaryValueMutationCheck(VpSchema vp) throws Exception {
        String deepSource = vp.prefix + "deep_boundary_source";
        String deepLeft = vp.prefix + "deep_boundary_l";
        String deepRight = vp.prefix + "deep_boundary_r";
        List<String> names = new ArrayList<>();
        List<String> values = new ArrayList<>();
        names.add(quote(ROW_ID));
        values.add("0");
        for (VpColumn column : vp.columns) {
            names.add(quote(column.name));
            values.add(boundaryValueForVpColumn(column));
        }
        try {
            // CTAS deliberately avoids copied base constraints: this probe isolates the boundary values and the
            // current VP split/reconstruction contract rather than arbitrary constraints of the fuzzed base table.
            executeDeepStatement("CREATE TEMP TABLE " + quote(deepSource) + " AS SELECT * FROM " + quote(vp.sourceTable)
                    + " WHERE FALSE");
            executeDeepStatement("INSERT INTO " + quote(deepSource) + " (" + String.join(", ", names) + ") VALUES ("
                    + String.join(", ", values) + ")");
            executeDeepStatement("CREATE TEMP TABLE " + quote(deepLeft) + " AS SELECT "
                    + partitionProjection(vp.leftColumns) + " FROM " + quote(deepSource));
            executeDeepStatement("CREATE TEMP TABLE " + quote(deepRight) + " AS SELECT "
                    + partitionProjection(vp.rightColumns) + " FROM " + quote(deepSource));
            String reconstruction = "SELECT " + joinProjection(vp, "l", "r", false) + " FROM " + quote(deepLeft)
                    + " l JOIN " + quote(deepRight) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID);
            String allColumns = String.join(", ", names);
            String source = "SELECT " + allColumns + " FROM " + quote(deepSource);
            boolean jdbcMatches = resultMultisetsMatch(getResultMultiset(source), getResultMultiset(reconstruction));
            if (!jdbcMatches) {
                throw new AssertionError("VP deep DuckDB boundary mutation changed the JDBC reconstructed row");
            }
            long differences = queryLong(bidirectionalDifferenceQuery(source, reconstruction, "boundary_diff"));
            if (differences != 0) {
                throw new AssertionError("VP deep DuckDB boundary mutation changed the reconstructed row");
            }
            recordDeepFeature(DeepFeature.BOUNDARY_MUTATION);
        } finally {
            dropDeepTable(deepRight);
            dropDeepTable(deepLeft);
            dropDeepTable(deepSource);
        }
    }

    private String partitionProjection(List<VpColumn> columns) {
        List<String> projection = new ArrayList<>();
        projection.add(quote(ROW_ID));
        projection.addAll(columns.stream().map(column -> quote(column.name)).collect(Collectors.toList()));
        return String.join(", ", projection);
    }

    private String sourceProjection(VpSchema vp) {
        List<String> projection = new ArrayList<>();
        projection.add(quote(ROW_ID));
        projection.addAll(vp.columns.stream().map(column -> quote(column.name)).collect(Collectors.toList()));
        return String.join(", ", projection);
    }

    private String bidirectionalDifferenceQuery(String source, String reconstruction, String alias) {
        String left = "SELECT * FROM (" + source + ") vp_left";
        String right = "SELECT * FROM (" + reconstruction + ") vp_right";
        return "SELECT COUNT(*) FROM ((" + left + " EXCEPT ALL " + right + ") UNION ALL (" + right + " EXCEPT ALL "
                + left + ")) " + alias;
    }

    private void assertDeepQueriesEquivalent(String feature, String sourceQuery, String splitQuery) throws Exception {
        QueryResultMultiset sourceRows = getResultMultiset(sourceQuery);
        QueryResultMultiset splitRows = getResultMultiset(splitQuery);
        if (!resultMultisetsMatch(sourceRows, splitRows)) {
            throw new AssertionError("VP deep DuckDB " + feature + " changed the reconstructed result");
        }
    }

    private String boundaryValueForVpColumn(VpColumn column) {
        switch (column.type.getPrimitiveDataType()) {
        case INT:
            return "0";
        case FLOAT:
            return "-0.0";
        case VARCHAR:
            return "''";
        case BOOLEAN:
            return "FALSE";
        case DATE:
            return "DATE '0001-01-01'";
        case TIMESTAMP:
            return "TIMESTAMP '0001-01-01 00:00:00'";
        case DECIMAL:
            return "CAST('0.000' AS " + column.type.toSQLType() + ")";
        case HUGEINT:
            return "CAST(0 AS HUGEINT)";
        case UUID:
            return "UUID '00000000-0000-0000-0000-000000000000'";
        case BLOB:
            return "BLOB ''";
        case TIME:
            return "TIME '00:00:00'";
        case INTERVAL:
            return "INTERVAL '0 days'";
        case LIST:
        case STRUCT:
        case MAP:
            return "CAST(NULL AS " + column.type.toSQLType() + ")";
        case NULL:
            return "NULL";
        default:
            throw new AssertionError(column.type);
        }
    }

    private boolean statementIsRejected(String sql) throws Exception {
        try {
            executeDeepStatement(sql);
            return false;
        } catch (SQLException expected) {
            return true;
        }
    }

    private void executeDeepStatement(String sql) throws SQLException {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
        }
    }

    private long queryLong(String sql) throws SQLException {
        try (Statement statement = state.getConnection().createStatement()) {
            return queryLong(statement, sql);
        }
    }

    private long queryLong(Statement statement, String sql) throws SQLException {
        try (ResultSet rs = statement.executeQuery(sql)) {
            if (!rs.next()) {
                throw new AssertionError("VP deep scalar query returned no row: " + sql);
            }
            return rs.getLong(1);
        }
    }

    private void dropDeepTable(String table) {
        try {
            executeDeepStatement("DROP VIEW IF EXISTS " + quote(table));
        } catch (SQLException ignored) {
        }
        try {
            executeDeepStatement("DROP TABLE IF EXISTS " + quote(table));
        } catch (SQLException ignored) {
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

    private boolean isNumericType(DuckDBCompositeDataType type) {
        return type.isNumeric();
    }

    private String getStableOrder(DuckDBTable base, List<VpColumn> columns) {
        List<String> primary = base.getColumns().stream().filter(DuckDBColumn::isPrimaryKey)
                .map(c -> quote(c.getName())).collect(Collectors.toList());
        if (!primary.isEmpty()) {
            return String.join(", ", primary);
        }
        List<String> comparable = columns.stream().filter(c -> c.type.supportsStableOrdering()).map(c -> quote(c.name))
                .collect(Collectors.toList());
        if (!comparable.isEmpty()) {
            return String.join(", ", comparable);
        }
        return null;
    }

    private String getRowIdExpression(DuckDBTable base, String stableOrder) {
        for (String candidate : DUCKDB_ROW_ID_CANDIDATES) {
            if (!hasPhysicalColumnNamed(base, candidate) && canUseRowIdExpressionAsKey(base.getName(), candidate)) {
                return candidate;
            }
        }
        String order = stableOrder;
        if (order == null || order.isEmpty()) {
            order = base.getColumns().stream().filter(c -> !c.isSyntheticRowId()).map(c -> quote(c.getName()))
                    .collect(Collectors.joining(", "));
        }
        if (order == null || order.isEmpty()) {
            order = "(SELECT 1)";
        }
        return "ROW_NUMBER() OVER (ORDER BY " + order + ")";
    }

    private boolean hasPhysicalColumnNamed(DuckDBTable table, String name) {
        return table.getColumns().stream().filter(c -> !c.isSyntheticRowId())
                .anyMatch(c -> c.getName().equalsIgnoreCase(name));
    }

    private boolean canUseRowIdExpressionAsKey(String tableName, String expression) {
        try (Statement s = state.getConnection().createStatement();
                ResultSet rs = s.executeQuery("SELECT COUNT(*) AS total_rows, COUNT(DISTINCT " + expression
                        + ") AS distinct_rows, COALESCE(SUM(CASE WHEN " + expression + " IS NULL THEN 1 ELSE 0 END), 0)"
                        + " AS null_rows FROM " + quote(tableName))) {
            if (rs.next()) {
                return rs.getLong("total_rows") == rs.getLong("distinct_rows") && rs.getLong("null_rows") == 0;
            }
            return false;
        } catch (SQLException ignored) {
            return false;
        }
    }

    private VpRowIdType getRowIdType(String tableName, String expression) {
        String sql = "SELECT " + expression + " AS " + quote(ROW_ID) + " FROM " + quote(tableName) + " LIMIT 0";
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData metadata = rs.getMetaData();
            return rowIdTypeFromJdbcName(metadata.getColumnTypeName(1));
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
    }

    private VpRowIdType rowIdTypeFromJdbcName(String jdbcTypeName) {
        String type = jdbcTypeName.toUpperCase(Locale.ROOT);
        switch (type) {
        case "TINYINT":
            return new VpRowIdType("TINYINT", 1);
        case "SMALLINT":
            return new VpRowIdType("SMALLINT", 2);
        case "INTEGER":
        case "INT":
        case "INT4":
            return new VpRowIdType("INTEGER", 4);
        case "BIGINT":
        case "INT8":
            return new VpRowIdType("BIGINT", 8);
        case "HUGEINT":
            return new VpRowIdType("HUGEINT", 16);
        case "UBIGINT":
            return new VpRowIdType("UBIGINT", 8);
        default:
            throw new IgnoreMeException();
        }
    }

    public static SQLQueryAdapter createBoundaryValueMutation(DuckDBGlobalState state) {
        DuckDBTable table = state.getSchema().getRandomTable(t -> !t.isView() && !t.getName().startsWith("vp_"));
        List<DuckDBColumn> columns = table.getColumns().stream().filter(c -> !c.isSyntheticRowId())
                .collect(Collectors.toList());
        if (columns.isEmpty()) {
            throw new IgnoreMeException();
        }
        String names = columns.stream().map(c -> DuckDBProvider.quoteIdentifier(c.getName()))
                .collect(Collectors.joining(", "));
        String values = columns.stream().map(DuckDBVerticalPartitioningOracle::boundaryValueForColumn)
                .collect(Collectors.joining(", "));
        ExpectedErrors errors = new ExpectedErrors();
        DuckDBErrors.addInsertErrors(errors);
        DuckDBErrors.addExpressionErrors(errors);
        recordAdvancedFeature(AdvancedFeature.BOUNDARY_MUTATION);
        return new SQLQueryAdapter("INSERT INTO " + DuckDBProvider.quoteIdentifier(table.getName()) + "(" + names
                + ") VALUES (" + values + ")", errors);
    }

    private static String boundaryValueForColumn(DuckDBColumn column) {
        switch (column.getType().getPrimitiveDataType()) {
        case INT:
            return Randomly.fromOptions("0", "1", "-1", "9223372036854775807", "-9223372036854775808");
        case FLOAT:
            return Randomly.fromOptions("0.0", "-0.0", "1e308", "-1e308", "'NaN'", "'+Inf'", "'-Inf'");
        case VARCHAR:
            return Randomly.fromOptions("''", "' '", "'0'", "'NaN'", "'A'", "'a'", "'aa'", "'zz'");
        case BOOLEAN:
            return Randomly.fromOptions("TRUE", "FALSE", "NULL");
        case DATE:
            return Randomly.fromOptions("DATE '1970-01-01'", "DATE '0001-01-01'", "DATE '9999-12-31'", "NULL");
        case TIMESTAMP:
            return Randomly.fromOptions("TIMESTAMP '1970-01-01 00:00:00'", "TIMESTAMP '0001-01-01 00:00:00'",
                    "TIMESTAMP '9999-12-31 23:59:59'", "NULL");
        case NULL:
            return "NULL";
        default:
            return "NULL";
        }
    }

    private long getRowCount(String tableName) throws Exception {
        String sql = "SELECT COUNT(*) FROM " + quote(tableName);
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (!rs.next()) {
                throw new AssertionError("COUNT(*) returned no row for " + tableName);
            }
            return rs.getLong(1);
        } catch (SQLException e) {
            if (isSkippableResultReadError(e)) {
                throw new IgnoreMeException();
            }
            throw e;
        }
    }

    private String helperEngineCandidate(BaseTableInfo base) {
        if (state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine) {
            return base.engine;
        }
        return "ROWID";
    }

    private void createSourceTable(VpSchema vp, String preferredEngine) throws Exception {
        vp.sourceEngine = createTableWithEngineFallback(vp, vp.sourceTable, sourceColumnDefs(vp), preferredEngine);
    }

    private void initializeColumnCollations(VpSchema vp) {
        for (VpColumn column : vp.columns) {
            String clause = "";
            if (deepExplorationEnabled() && isCharacterLike(column.type) && Randomly.getBoolean()) {
                clause = " COLLATE " + Randomly.fromOptions("NOCASE", "C", "POSIX");
                recordAdvancedFeature(AdvancedFeature.CHARSET_COLLATION_COLUMN);
            }
            vp.columnCollationClauses.put(column.name, clause);
        }
    }

    private void clearColumnCollations(VpSchema vp) {
        vp.columnCollationClauses.replaceAll((column, ignored) -> "");
    }

    private List<String> sourceColumnDefs(VpSchema vp) {
        List<String> columnDefs = new ArrayList<>();
        columnDefs.add(rowIdColumnDef(vp));
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
            vp.sourceEngine = recreateHelperTableWithRowId(vp, vp.sourceTable, sourceColumnDefs(vp));
            executeSetup(vp, populateSourceSQL(vp, stableOrder));
        }
        long sourceRows = materializedSourceRowCount(vp);
        recordSourceRowCount(sourceRows);
        if (storageBoundaryTestsEnabled() && sourceRows >= storageBoundaryTargetRows) {
            recordAdvancedFeature(AdvancedFeature.STORAGE_BOUNDARY);
        }
    }

    private long materializedSourceRowCount(VpSchema vp) {
        int maxRows = effectiveSourceRowLimit();
        return maxRows > 0 ? Math.min(vp.baseRowCount, maxRows) : vp.baseRowCount;
    }

    private int effectiveSourceRowLimit() {
        if (storageBoundaryTestsEnabled()) {
            return storageBoundaryTargetRows;
        }
        return state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows;
    }

    private String populateSourceSQL(VpSchema vp, String stableOrder) {
        String columnList = vp.columns.stream().map(c -> quote(c.name)).collect(Collectors.joining(", "));
        String sql = "INSERT INTO " + quote(vp.sourceTable) + " (" + quote(ROW_ID)
                + (columnList.isEmpty() ? "" : ", " + columnList) + ") SELECT " + vp.rowIdExpression
                + (columnList.isEmpty() ? "" : ", " + columnList) + " FROM " + quote(vp.baseTable);
        if (stableOrder != null) {
            sql += " ORDER BY " + stableOrder;
        }
        int maxRows = effectiveSourceRowLimit();
        if (maxRows > 0) {
            sql += " LIMIT " + maxRows;
        }
        return sql;
    }

    private void forceStorageBoundaryCheckpoint(VpSchema vp) throws Exception {
        if (!storageBoundaryTestsEnabled()) {
            return;
        }
        executeNoSchemaRefresh("CHECKPOINT");
        vp.setupStatements.add("CHECKPOINT");
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
        defs.add(rowIdColumnDef(vp));
        for (VpColumn column : columns) {
            defs.add(columnDef(vp, column));
        }
        return defs;
    }

    private String columnDef(VpSchema vp, VpColumn column) {
        return quote(column.name) + " " + toSQLType(column.type)
                + vp.columnCollationClauses.getOrDefault(column.name, "") + (column.nullable ? " NULL" : " NOT NULL");
    }

    private String rowIdColumnDef(VpSchema vp) {
        return quote(ROW_ID) + " " + vp.rowIdType.sqlType + " NOT NULL PRIMARY KEY";
    }

    private boolean isCharacterLike(DuckDBCompositeDataType type) {
        return type.isCharacterLike();
    }

    private String createTableWithEngineFallback(VpSchema vp, String tableName, List<String> columnDefs,
            String preferredEngine) throws Exception {
        try {
            executeSetup(vp, createTableSQL(tableName, columnDefs, preferredEngine));
            return preferredEngine;
        } catch (AssertionError e) {
            if (deepExplorationEnabled() && containsCharsetCollation(columnDefs)) {
                throw new CollationFallbackRequiredException();
            }
            if (!canFallbackHelperEngine(preferredEngine)) {
                throw e;
            }
            return recreateHelperTableWithRowId(vp, tableName, columnDefs);
        }
    }

    private boolean containsCharsetCollation(List<String> columnDefs) {
        return columnDefs.stream().anyMatch(def -> def.toUpperCase(Locale.ROOT).contains(" COLLATE "));
    }

    private String createTableSQL(String tableName, List<String> columnDefs, String engine) {
        return "CREATE TABLE " + quote(tableName) + " (" + String.join(", ", columnDefs) + ")";
    }

    private boolean canFallbackHelperEngine(String engine) {
        return state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine && engine != null
                && !"ROWID".equalsIgnoreCase(engine);
    }

    private String recreateHelperTableWithRowId(VpSchema vp, String tableName, List<String> columnDefs)
            throws Exception {
        executeSetup(vp, "DROP TABLE IF EXISTS " + quote(tableName));
        executeSetup(vp, createTableSQL(tableName, columnDefs, "ROWID"));
        return "ROWID";
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
            String fallbackEngine = recreateHelperTableWithRowId(vp, tableName, partitionColumnDefs(vp, columns));
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
        return columns.stream().filter(c -> isDuckDBHelperIndexable(c.type)).collect(Collectors.toList());
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

    private boolean isDuckDBHelperIndexable(DuckDBCompositeDataType type) {
        return type.isIndexable();
    }

    private void copyBaseSchemaObjectsToSource(VpSchema vp) {
        if (!shouldPreserveSchemaObjects()) {
            return;
        }
        int copied = 0;
        for (String indexSql : getBaseIndexCreateStatements(vp.baseTable)) {
            String rewritten = rewriteDuckDBIndexForSource(vp, indexSql, copied);
            if (rewritten == null) {
                continue;
            }
            try {
                executeSetup(vp, rewritten);
                copied++;
            } catch (AssertionError ignored) {
                vp.setupStatements.add("-- skipped copied schema object: " + rewritten);
            } catch (Exception ignored) {
                vp.setupStatements.add("-- skipped copied schema object: " + rewritten);
            }
        }
        for (String constraintSql : getBaseConstraintAlterStatements(vp, copied)) {
            try {
                executeSetup(vp, constraintSql);
                copied++;
            } catch (AssertionError ignored) {
                vp.setupStatements.add("-- skipped copied schema object: " + constraintSql);
            } catch (Exception ignored) {
                vp.setupStatements.add("-- skipped copied schema object: " + constraintSql);
            }
        }
    }

    /**
     * Select whether this VP instance keeps base-table indexes/constraints. The explicit legacy switch remains
     * supported; mixed mode deliberately exercises both semantics across generated databases.
     */
    private boolean shouldPreserveSchemaObjects() {
        if (state.getDbmsSpecificOptions().verticalPartitioningSourceCopySchemaObjects) {
            return true;
        }
        String mode = state.getDbmsSpecificOptions().verticalPartitioningSchemaFidelityMode;
        if (mode == null) {
            return false;
        }
        switch (mode.trim().toLowerCase(Locale.ROOT)) {
        case "strict":
        case "preserve":
            return true;
        case "mixed":
            return Randomly.getBoolean();
        case "drop":
        case "none":
            return false;
        default:
            throw new AssertionError(
                    "Unknown --vp-schema-fidelity-mode: " + mode + " (expected drop, strict, or mixed)");
        }
    }

    private List<String> getBaseIndexCreateStatements(String baseTable) {
        List<String> result = new ArrayList<>();
        String sql = "SELECT sql FROM duckdb_indexes() WHERE table_name = '" + escapeLiteral(baseTable)
                + "' AND sql IS NOT NULL";
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        } catch (SQLException ignored) {
        }
        return result;
    }

    private String rewriteDuckDBIndexForSource(VpSchema vp, String indexSql, int ordinal) {
        String newName = quote(vp.prefix + "src_idx_copy_" + ordinal);
        String rewritten = indexSql.replaceFirst(
                "(?i)^\\s*CREATE\\s+(UNIQUE\\s+)?INDEX\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(\"[^\"]+\"|`[^`]+`|\\[[^\\]]+\\]|\\S+)",
                "CREATE $1INDEX " + newName);
        String basePattern = "(?i)\\s+ON\\s+(" + Pattern.quote(quote(vp.baseTable)) + "|"
                + Pattern.quote("`" + vp.baseTable.replace("`", "``") + "`") + "|"
                + Pattern.quote("[" + vp.baseTable.replace("]", "]]") + "]") + "|" + Pattern.quote(vp.baseTable)
                + ")\\s*\\(";
        rewritten = Pattern.compile(basePattern).matcher(rewritten)
                .replaceFirst(java.util.regex.Matcher.quoteReplacement(" ON " + quote(vp.sourceTable) + "("));
        if (rewritten.equals(indexSql)) {
            return null;
        }
        return rewritten;
    }

    private List<String> getBaseConstraintAlterStatements(VpSchema vp, int startOrdinal) {
        List<String> result = new ArrayList<>();
        String sql = "SELECT constraint_type, constraint_text FROM duckdb_constraints() WHERE table_name = '"
                + escapeLiteral(vp.baseTable) + "' ORDER BY constraint_index";
        int ordinal = startOrdinal;
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                String type = rs.getString(1);
                String text = rs.getString(2);
                if (type == null || text == null) {
                    continue;
                }
                String normalizedType = type.toUpperCase(Locale.ROOT);
                if (!normalizedType.contains("CHECK") && !normalizedType.contains("UNIQUE")) {
                    continue;
                }
                String constraintName = quote(vp.prefix + "src_chk_copy_" + ordinal++);
                result.add("ALTER TABLE " + quote(vp.sourceTable) + " ADD CONSTRAINT " + constraintName + " " + text);
            }
        } catch (SQLException ignored) {
        }
        return result;
    }

    private VpQueryPair generateRandomRewritePair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        SourceAst sourceAst = createSourceAst(vp);
        DuckDBExpressionGenerator gen = new DuckDBExpressionGenerator(state).setAllowUnsafeCasts(false)
                .setColumns(sourceAst.columns);
        if (state.getRandomly().getInteger(0, 19) == 0) {
            VpQueryPair friendly = generateFriendlySqlPair(vp, strategy, sourceAst);
            if (friendly != null) {
                return friendly;
            }
        }
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()) {
            return generateRollupPair(vp, strategy, sourceAst);
        }
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()) {
            return generateCorrelatedSubqueryPair(vp, strategy, sourceAst);
        }
        DuckDBSelect select = new DuckDBSelect();
        int fetchColumns = Math.max(1, Randomly.smallNumber() + 1);
        boolean useAggregate = Randomly.getBoolean();
        boolean hasWindowExpression = false;
        List<DuckDBExpression> selected = new ArrayList<>();
        List<DuckDBExpression> aggregateExpressions = new ArrayList<>();
        List<DuckDBExpression> nonAggregateExpressions = new ArrayList<>();
        List<DuckDBExpression> groupByExpressions = new ArrayList<>();
        if (useAggregate) {
            DuckDBExpression aggregate = generateVpAggregate(vp, gen, sourceAst.columns);
            aggregateExpressions.add(aggregate);
            selected.add(aggregate);
            for (int i = 1; i < fetchColumns; i++) {
                DuckDBExpression expr = new DuckDBColumnReference(Randomly.fromList(sourceAst.columns));
                nonAggregateExpressions.add(expr);
            }
        } else {
            for (int i = 0; i < fetchColumns; i++) {
                DuckDBExpression expr;
                if (advancedFeaturesEnabled() && Randomly.getBooleanWithRatherLowProbability()) {
                    expr = generateAdvancedSelectExpression(sourceAst, gen);
                    hasWindowExpression = true;
                } else {
                    expr = generateStableValueExpression(sourceAst);
                    nonAggregateExpressions.add(expr);
                }
                selected.add(expr);
            }
        }
        if (selected.isEmpty()) {
            selected.add(new DuckDBColumnReference(Randomly.fromList(sourceAst.columns)));
        }
        select.setFromList(List.of(new DuckDBTableReference(sourceAst.table)));
        if (Randomly.getBoolean()) {
            select.setWhereClause(generatePredicate(gen, sourceAst));
        }
        if (useAggregate) {
            groupByExpressions = chooseGroupByExpressions(nonAggregateExpressions);
            select.setGroupByClause(maybeAppendRollup(groupByExpressions, hasWindowExpression,
                    isRollupSafe(nonAggregateExpressions, groupByExpressions)));
            selected = new ArrayList<>(aggregateExpressions);
            selected.addAll(canonicalGroupProjections(vp, groupByExpressions));
        } else if (!hasWindowExpression && Randomly.getBooleanWithRatherLowProbability()) {
            groupByExpressions = chooseGroupByExpressions(nonAggregateExpressions);
            select.setGroupByClause(maybeAppendRollup(groupByExpressions, hasWindowExpression,
                    isRollupSafe(nonAggregateExpressions, groupByExpressions)));
            if (!groupByExpressions.isEmpty()) {
                selected = canonicalGroupProjections(vp, groupByExpressions);
            }
            if (Randomly.getBoolean() && !groupByExpressions.isEmpty()) {
                select.setHavingClause(generateHavingExpression(vp, gen, sourceAst.columns));
            }
        }
        if (selected.isEmpty()) {
            selected.add(new DuckDBColumnReference(sourceAst.rowIdColumn));
        }
        select.setFetchColumns(selected);
        if (Randomly.getBoolean()) {
            List<DuckDBExpression> orderBys = createStableOrderByExpressions(gen, sourceAst, useAggregate,
                    groupByExpressions, nonAggregateExpressions);
            if (!orderBys.isEmpty()) {
                select.setOrderByClauses(orderBys);
            }
        }
        boolean orderedComparisonEnabled = isRowPreservingQuery(useAggregate, select)
                && hasVpRowIdTieBreaker(select, sourceAst.rowIdColumn);
        boolean rowPreservingQuery = isRowPreservingQuery(useAggregate, select);
        if (rowPreservingQuery && Randomly.getBoolean()) {
            if (select.getOrderByClauses().isEmpty()) {
                select.setOrderByClauses(List.of(new DuckDBColumnReference(sourceAst.rowIdColumn)));
                orderedComparisonEnabled = rowPreservingQuery;
            } else if (rowPreservingQuery) {
                List<DuckDBExpression> orderBys = new ArrayList<>(select.getOrderByClauses());
                orderBys.add(new DuckDBColumnReference(sourceAst.rowIdColumn));
                select.setOrderByClauses(orderBys);
                orderedComparisonEnabled = true;
            } else {
                select.setOrderByClauses(List.of(new DuckDBColumnReference(sourceAst.rowIdColumn)));
                orderedComparisonEnabled = false;
            }
            select.setLimitClause(DuckDBConstant.createIntConstant(Randomly.getNotCachedInteger(0, 25)));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                select.setOffsetClause(DuckDBConstant.createIntConstant(Randomly.getNotCachedInteger(0, 5)));
            }
        }
        boolean appendUnionBranch = advancedFeaturesEnabled() && !useAggregate && select.getOrderByClauses().isEmpty()
                && select.getLimitClause() == null && select.getOffsetClause() == null
                && Randomly.getBooleanWithSmallProbability();
        DuckDBSelect unionBranch = null;
        if (appendUnionBranch) {
            unionBranch = createUnionAllBranch(sourceAst, gen, selected.size());
            orderedComparisonEnabled = false;
        }
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        if (deepExplorationEnabled() && state.getRandomly().getInteger(0, 9) == 0) {
            addPostPartitionMutationProbe(vp, extraSetup);
        }
        DuckDBExpression sourceFrom = new DuckDBText(sourceRelation(vp.sourceTable), null);
        DuckDBExpression splitFrom = new DuckDBText(relation, null);
        String singleQuery = renderSelectPair(select, unionBranch, sourceFrom);
        if (usesStringAgg(singleQuery)) {
            sourceFrom = new DuckDBText(orderedSourceRelation(vp.sourceTable), null);
            relation = orderRelationForGroupConcat(relation, vp.sourceTable);
            splitFrom = new DuckDBText(relation, null);
            singleQuery = renderSelectPair(select, unionBranch, sourceFrom);
        }
        String splitQuery = renderSelectPair(select, unionBranch, splitFrom);
        return new VpQueryPair(vp, strategy, singleQuery, splitQuery, extraSetup,
                "shape=" + vp.baseShape + ", split=" + vp.splitStrategy + ", reconstruction=" + strategy,
                orderedComparisonEnabled);
    }

    private boolean isRollupSafe(List<DuckDBExpression> nonAggregateExpressions,
            List<DuckDBExpression> groupByExpressions) {
        if (nonAggregateExpressions.size() != groupByExpressions.size()) {
            return false;
        }
        List<String> groupBySql = groupByExpressions.stream().map(DuckDBToStringVisitor::asString)
                .collect(Collectors.toList());
        for (DuckDBExpression expression : nonAggregateExpressions) {
            if (!groupBySql.contains(DuckDBToStringVisitor.asString(expression))) {
                return false;
            }
        }
        return true;
    }

    private VpQueryPair generateFriendlySqlPair(VpSchema vp, ReconstructionStrategy strategy, SourceAst sourceAst)
            throws Exception {
        List<DuckDBColumn> valueColumns = sourceAst.columns.stream().filter(c -> !c.getName().equals(ROW_ID))
                .collect(Collectors.toList());
        List<DuckDBColumn> comparable = valueColumns.stream().filter(c -> c.getType().supportsComparable())
                .collect(Collectors.toList());
        List<DuckDBColumn> orderable = valueColumns.stream().filter(c -> c.getType().supportsOrderable())
                .collect(Collectors.toList());
        List<DuckDBColumn> pivotable = valueColumns.stream()
                .filter(c -> c.getType().supportsComparable() && c.getType().supportsOrderable())
                .collect(Collectors.toList());
        List<DuckDBColumn> temporal = valueColumns.stream().filter(c -> {
            return c.getType().isTemporal() && c.getType().supportsComparable() && c.getType().supportsOrderable();
        }).collect(Collectors.toList());
        boolean reconstructionSafe = supportsFriendlyReconstruction(vp, strategy);
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        // Exercise DuckDB-native relational syntax independently of the generic
        // expression generator. These forms are deliberately count/order stable
        // so that VP equality remains a meaningful oracle check.
        if (advancedFeaturesEnabled() && reconstructionSafe && state.getRandomly().getInteger(0, 11) == 0) {
            int nativeChoice = state.getRandomly().getInteger(0, 4);
            if (nativeChoice == 0) {
                recordAdvancedFeature(AdvancedFeature.UNION_BY_NAME);
                return unionByNamePair(vp, strategy, relation, extraSetup);
            }
            if (nativeChoice == 1 && !comparable.isEmpty()) {
                recordAdvancedFeature(AdvancedFeature.DISTINCT_ON);
                return distinctOnPair(vp, strategy, relation, comparable.get(0), extraSetup);
            }
            if (nativeChoice == 2) {
                recordAdvancedFeature(AdvancedFeature.QUALIFY_WINDOW);
                return qualifyWindowPair(vp, strategy, relation, extraSetup);
            }
            if (nativeChoice == 3 && valueColumns.stream().anyMatch(c -> c.getType().toString().contains("[]"))) {
                recordAdvancedFeature(AdvancedFeature.UNNEST_LIST);
                return unnestListPair(vp, strategy, relation, extraSetup);
            }
            if (nativeChoice == 4 && valueColumns.stream().anyMatch(c -> c.getType().toString().contains("STRUCT"))) {
                recordAdvancedFeature(AdvancedFeature.STRUCT_FIELD_PROJECTION);
                return structFieldPair(vp, strategy, relation, extraSetup);
            }
        }
        int choice = state.getRandomly().getInteger(0, 21);
        if (choice == 0) {
            recordAdvancedFeature(AdvancedFeature.AGGREGATE_FILTER);
            return aggregateFilterPair(vp, strategy, relation, valueColumns, extraSetup);
        }
        if (choice == 1) {
            recordAdvancedFeature(AdvancedFeature.ORDERED_AGGREGATE);
            return orderedAggregatePair(vp, strategy, relation, valueColumns, extraSetup);
        }
        if (choice == 2 && !orderable.isEmpty()) {
            recordAdvancedFeature(AdvancedFeature.WINDOW_ROWS_FRAME);
            return windowFramePair(vp, strategy, relation, extraSetup);
        }
        if (choice == 3 && !comparable.isEmpty()) {
            recordAdvancedFeature(AdvancedFeature.ARG_MAX_MIN);
            return argMaxMinPair(vp, strategy, relation, comparable.get(0), extraSetup);
        }
        if (choice == 4 && !comparable.isEmpty()) {
            recordAdvancedFeature(AdvancedFeature.MODE);
            return modePair(vp, strategy, relation, comparable.get(0), extraSetup);
        }
        List<DuckDBColumn> aggregateOrderable = orderable.stream()
                .filter(c -> c.getType().supportsAggregate("QUANTILE")).collect(Collectors.toList());
        if (choice == 5 && !aggregateOrderable.isEmpty()) {
            recordAdvancedFeature(AdvancedFeature.QUANTILE_HISTOGRAM);
            return quantileHistogramPair(vp, strategy, relation, aggregateOrderable.get(0), extraSetup);
        }
        if (choice == 6 && reconstructionSafe) {
            recordAdvancedFeature(AdvancedFeature.LATERAL_QUERY);
            return lateralQueryPair(vp, strategy, relation, extraSetup);
        }
        if (choice == 7 && reconstructionSafe) {
            recordAdvancedFeature(AdvancedFeature.RECURSIVE_CTE);
            return recursiveCtePair(vp, strategy, relation, extraSetup);
        }
        choice -= 8;
        if (choice == 0 && reconstructionSafe) {
            recordAdvancedFeature(AdvancedFeature.FRIENDLY_SQL_REWRITE);
            return qualifyFriendlyPair(vp, strategy, relation, extraSetup);
        }
        if (choice == 1 && reconstructionSafe && !comparable.isEmpty()) {
            recordAdvancedFeature(AdvancedFeature.FRIENDLY_SQL_REWRITE);
            return groupByAllFriendlyPair(vp, strategy, relation, comparable.get(0), extraSetup);
        }
        if (choice == 2 && reconstructionSafe && !orderable.isEmpty()) {
            recordAdvancedFeature(AdvancedFeature.FRIENDLY_SQL_REWRITE);
            return orderByAllFriendlyPair(vp, strategy, relation, orderable.get(0), extraSetup);
        }
        if (choice == 3 && reconstructionSafe && !valueColumns.isEmpty()) {
            recordAdvancedFeature(AdvancedFeature.FRIENDLY_SQL_REWRITE);
            return starProjectionFriendlyPair(vp, strategy, relation, extraSetup, false);
        }
        if (choice == 4 && reconstructionSafe && !valueColumns.isEmpty()) {
            recordAdvancedFeature(AdvancedFeature.FRIENDLY_SQL_REWRITE);
            return starProjectionFriendlyPair(vp, strategy, relation, extraSetup, true);
        }
        if (choice == 5 && reconstructionSafe && !valueColumns.isEmpty()) {
            recordAdvancedFeature(AdvancedFeature.FRIENDLY_SQL_REWRITE);
            return columnsFriendlyPair(vp, strategy, relation, extraSetup);
        }
        if (choice == 6 && reconstructionSafe) {
            recordAdvancedFeature(AdvancedFeature.POSITIONAL_JOIN);
            return positionalJoinPair(vp, strategy, relation, extraSetup);
        }
        if (choice == 7 && !temporal.isEmpty()) {
            recordAdvancedFeature(AdvancedFeature.ASOF_JOIN);
            return asofJoinPair(vp, strategy, relation, temporal.get(0), extraSetup);
        }
        if (choice == 8 && reconstructionSafe) {
            return samplePair(vp, strategy, relation, extraSetup);
        }
        if (choice == 9 && reconstructionSafe && !pivotable.isEmpty()) {
            return pivotPair(vp, strategy, relation, pivotable.get(0), extraSetup);
        }
        if (choice == 10 && reconstructionSafe && valueColumns.size() >= 2) {
            return unpivotPair(vp, strategy, relation, valueColumns, extraSetup);
        }
        if (choice == 11 && isJsonExtensionAvailable() && reconstructionSafe) {
            recordAdvancedFeature(AdvancedFeature.JSON_QUERY);
            return jsonQueryPair(vp, strategy, relation, extraSetup);
        }
        if (choice == 12 && isSpatialExtensionAvailable() && reconstructionSafe) {
            recordAdvancedFeature(AdvancedFeature.SPATIAL_QUERY);
            return spatialQueryPair(vp, strategy, relation, extraSetup);
        }
        if (choice == 13 && isFtsExtensionAvailable() && reconstructionSafe) {
            recordAdvancedFeature(AdvancedFeature.FTS_QUERY);
            return ftsQueryPair(vp, strategy, relation, extraSetup);
        }
        return null;
    }

    private VpQueryPair unionByNamePair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup) {
        String row = quote(ROW_ID);
        String source = "SELECT COUNT(*) FROM (SELECT " + row + " FROM " + quote(vp.sourceTable)
                + " UNION BY NAME SELECT " + row + " FROM " + quote(vp.sourceTable) + " WHERE " + row + " < 0) u";
        String split = "SELECT COUNT(*) FROM (SELECT " + row + " FROM " + relation + " UNION BY NAME SELECT " + row
                + " FROM " + relation + " WHERE " + row + " < 0) u";
        return friendlyPair(vp, strategy, source, split, setup, "UNION_BY_NAME");
    }

    /**
     * Exercise DuckDB DML syntax after the partitions have been materialized. The probes are deliberately
     * data-preserving (empty UPDATE/DELETE/MERGE source and ON CONFLICT DO NOTHING), so they test binding, visibility,
     * and ownership without manufacturing a VP mismatch.
     */
    private void addPostPartitionMutationProbe(VpSchema vp, List<String> setup) {
        String probe = quote(vp.prefix + "dml_probe");
        setup.add("DROP TABLE IF EXISTS " + probe);
        setup.add("CREATE TEMP TABLE " + probe + " AS SELECT " + quote(ROW_ID) + " FROM " + quote(vp.sourceTable)
                + " WHERE FALSE");
        switch (state.getRandomly().getInteger(0, 3)) {
        case 0:
            recordAdvancedFeature(AdvancedFeature.UPDATE_FROM);
            setup.add("UPDATE " + quote(vp.sourceTable) + " AS t SET " + quote(ROW_ID) + " = t." + quote(ROW_ID)
                    + " FROM " + probe + " AS p WHERE t." + quote(ROW_ID) + " = p." + quote(ROW_ID));
            break;
        case 1:
            recordAdvancedFeature(AdvancedFeature.DELETE_USING);
            setup.add("DELETE FROM " + quote(vp.sourceTable) + " USING " + probe + " AS p WHERE "
                    + quote(vp.sourceTable) + "." + quote(ROW_ID) + " = p." + quote(ROW_ID));
            break;
        case 2:
            recordAdvancedFeature(AdvancedFeature.MERGE);
            setup.add("MERGE INTO " + quote(vp.sourceTable) + " AS t USING " + probe + " AS p ON t." + quote(ROW_ID)
                    + " = p." + quote(ROW_ID) + " WHEN MATCHED THEN UPDATE SET " + quote(ROW_ID) + " = t."
                    + quote(ROW_ID));
            break;
        default:
            recordAdvancedFeature(AdvancedFeature.ON_CONFLICT);
            setup.add("INSERT INTO " + quote(vp.sourceTable) + " (" + quote(ROW_ID) + ") SELECT " + quote(ROW_ID)
                    + " FROM " + probe + " ON CONFLICT (" + quote(ROW_ID) + ") DO NOTHING");
            break;
        }
    }

    private VpQueryPair distinctOnPair(VpSchema vp, ReconstructionStrategy strategy, String relation, DuckDBColumn key, List<String> setup) {
        String k = quote(key.getName());
        String row = quote(ROW_ID);
        String source = "SELECT DISTINCT ON (" + k + ") " + k + ", " + row + " FROM " + quote(vp.sourceTable)
                + " ORDER BY " + k + " NULLS LAST, " + row;
        String split = "SELECT DISTINCT ON (" + k + ") " + k + ", " + row + " FROM " + relation + " ORDER BY " + k
                + " NULLS LAST, " + row;
        return friendlyPair(vp, strategy, source, split, setup, "DISTINCT_ON");
    }

    private VpQueryPair qualifyWindowPair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup) {
        String row = quote(ROW_ID);
        String source = "SELECT " + row + ", ROW_NUMBER() OVER (ORDER BY " + row + ") AS rn FROM "
                + quote(vp.sourceTable) + " QUALIFY rn <= 3";
        String split = "SELECT " + row + ", rn FROM (SELECT " + row + ", ROW_NUMBER() OVER (ORDER BY " + row
                + ") AS rn FROM " + relation + ") q WHERE rn <= 3";
        return friendlyPair(vp, strategy, source, split, setup, "QUALIFY_WINDOW");
    }

    private VpQueryPair unnestListPair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup) {
        String list = vp.columns.stream().filter(c -> c.type.toString().contains("[]")).map(c -> quote(c.name))
                .findFirst().orElse(quote(ROW_ID));
        String row = quote(ROW_ID);
        String source = "SELECT " + row + ", COUNT(*) AS n FROM " + quote(vp.sourceTable) + " CROSS JOIN UNNEST(" + list
                + ") u GROUP BY " + row + " ORDER BY " + row;
        String split = "SELECT " + row + ", COUNT(*) AS n FROM " + relation + " CROSS JOIN UNNEST(" + list
                + ") u GROUP BY " + row + " ORDER BY " + row;
        return friendlyPair(vp, strategy, source, split, setup, "UNNEST_LIST");
    }

    private VpQueryPair structFieldPair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup) {
        String struct = vp.columns.stream().filter(c -> c.type.toString().contains("STRUCT")).map(c -> quote(c.name))
                .findFirst().orElse(quote(ROW_ID));
        String row = quote(ROW_ID);
        String source = "SELECT " + row + ", " + struct + ".f0 FROM " + quote(vp.sourceTable) + " ORDER BY " + row;
        String split = "SELECT " + row + ", " + struct + ".f0 FROM " + relation + " ORDER BY " + row;
        return friendlyPair(vp, strategy, source, split, setup, "STRUCT_FIELD_PROJECTION");
    }

    private boolean supportsFriendlyReconstruction(VpSchema vp, ReconstructionStrategy strategy) {
        return vp.columns.stream().allMatch(c -> c.type.supportsReconstruction(strategy.name()));
    }

    private VpQueryPair aggregateFilterPair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<DuckDBColumn> columns,
            List<String> setup) {
        String value = columns.isEmpty() ? quote(ROW_ID) : quote(Randomly.fromList(columns).getName());
        String source = aggregateFilterSQL(quote(vp.sourceTable), value);
        String split = aggregateFilterSQL(relation, value);
        return friendlyPair(vp, strategy, source, split, setup, "FILTER");
    }

    private String aggregateFilterSQL(String relation, String value) {
        String row = quote(ROW_ID);
        return "SELECT COUNT(*) AS vp_input_count, " + "COUNT(*) FILTER (WHERE " + row + " % 2 = 0) AS vp_even_count, "
                + "COUNT(" + value + ") FILTER (WHERE " + row + " % 2 = 0) AS vp_even_nonnull, "
                + "COUNT(*) FILTER (WHERE " + value + " IS NULL) AS vp_null_count, "
                + "COUNT(*) FILTER (WHERE FALSE) AS vp_empty_count, " + "SUM(" + row + ") FILTER (WHERE " + row
                + " % 2 = 0) AS vp_even_sum, " + "SUM(CASE WHEN " + row + " % 2 = 0 THEN " + row
                + " ELSE 0 END) AS vp_even_sum_case, " + "(SELECT COUNT(*) FROM (SELECT * FROM " + relation
                + " WHERE FALSE) vp_empty) AS vp_empty_input_count, " + "(SELECT SUM(" + row + ") FROM (SELECT * FROM "
                + relation + " WHERE FALSE) vp_empty) AS vp_empty_input_sum FROM " + relation;
    }

    private VpQueryPair orderedAggregatePair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<DuckDBColumn> columns,
            List<String> setup) {
        String value = columns.isEmpty() ? quote(ROW_ID) : quote(Randomly.fromList(columns).getName());
        String textValue = "CAST(" + value + " AS VARCHAR)";
        String source = orderedAggregateSQL(quote(vp.sourceTable), value, textValue);
        String split = orderedAggregateSQL(relation, value, textValue);
        return friendlyPair(vp, strategy, source, split, setup, "ORDERED_AGGREGATE");
    }

    private String orderedAggregateSQL(String relation, String value, String textValue) {
        String row = quote(ROW_ID);
        return "SELECT list(" + value + " ORDER BY " + row + ") AS vp_list, " + "string_agg(" + textValue
                + ", ',' ORDER BY " + row + ") AS vp_string_agg, " + "first(" + value + " ORDER BY " + row
                + ") AS vp_first, " + "last(" + value + " ORDER BY " + row + ") AS vp_last FROM " + relation;
    }

    private VpQueryPair windowFramePair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup) {
        String frame;
        switch (state.getRandomly().getInteger(0, 3)) {
        case 0:
            frame = "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW";
            break;
        case 1:
            frame = "RANGE BETWEEN 1 PRECEDING AND CURRENT ROW";
            break;
        case 2:
            frame = "GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW";
            break;
        default:
            frame = "ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW";
            break;
        }
        String source = windowFrameSQL(quote(vp.sourceTable), frame);
        String split = windowFrameSQL(relation, frame);
        return new VpQueryPair(vp, strategy, source, split, setup,
                "shape=" + vp.baseShape + ", split=" + vp.splitStrategy + ", friendly=WINDOW_ROWS_FRAME", true);
    }

    private String windowFrameSQL(String relation, String frame) {
        String row = quote(ROW_ID);
        String orderKey = "CASE WHEN " + row + " % 5 = 0 THEN NULL ELSE " + row + " % 3 END";
        String order = orderKey + " NULLS LAST";
        // ROWS frames are sensitive to the physical order of peers. The source
        // table and a reconstructed join may enumerate equal order keys
        // differently, so add the stable row id only for ROWS. RANGE/GROUPS
        // deliberately retain peer groups and must not receive this tie-breaker.
        if (frame.startsWith("ROWS")) {
            order += ", " + row + " ASC";
        }
        return "SELECT " + row + ", " + orderKey + " AS vp_order_key, SUM(" + row + ") OVER (ORDER BY " + order + " "
                + frame + ") AS vp_frame_sum, COUNT(*) OVER (ORDER BY " + order + " " + frame
                + ") AS vp_frame_count FROM " + relation + " ORDER BY " + row;
    }

    private VpQueryPair argMaxMinPair(VpSchema vp, ReconstructionStrategy strategy, String relation, DuckDBColumn value, List<String> setup) {
        String column = quote(value.getName());
        String source = argMaxMinSQL(quote(vp.sourceTable), column);
        String split = argMaxMinSQL(relation, column);
        return friendlyPair(vp, strategy, source, split, setup, "ARG_MAX_MIN");
    }

    private String argMaxMinSQL(String relation, String value) {
        String row = quote(ROW_ID);
        String duplicateKey = "CASE WHEN " + row + " % 3 = 0 THEN 2 WHEN " + row + " % 2 = 0 THEN 1 ELSE 1 END";
        String deterministicKey = "struct_pack(bucket := " + duplicateKey + ", tie := " + row + ")";
        String nullableValue = "CASE WHEN " + row + " % 4 = 0 THEN NULL ELSE " + value + " END";
        // A plain arg_max(value, duplicateKey) is intentionally nondeterministic
        // when several rows share the maximum key. It is useful as an exploratory
        // expression, but cannot be part of a strict VP equality check because the
        // split relation may expose tied rows in a different order.
        return "SELECT arg_max(" + nullableValue + ", " + deterministicKey + ") AS vp_arg_max, " + "arg_min("
                + nullableValue + ", " + deterministicKey + ") AS vp_arg_min, " + "arg_max(" + row + ", "
                + deterministicKey + ") AS vp_selected_row, " + "arg_max(" + duplicateKey + ", " + duplicateKey
                + ") AS vp_arg_max_key, " + "arg_max(" + duplicateKey + ", " + deterministicKey + ") AS vp_max_key, "
                + "MAX(" + duplicateKey + ") AS vp_max_bucket, COUNT(*) AS vp_count, COUNT(" + nullableValue
                + ") AS vp_nonnull_value FROM " + relation;
    }

    private VpQueryPair modePair(VpSchema vp, ReconstructionStrategy strategy, String relation, DuckDBColumn value, List<String> setup) {
        String row = quote(ROW_ID);
        String column = "CASE WHEN " + row + " % 5 = 0 THEN NULL WHEN " + row + " % 2 = 0 THEN 1 ELSE 2 END";
        String order = " ORDER BY " + row;
        String source = modeSQL(quote(vp.sourceTable), column, order);
        String split = modeSQL(relation, column, order);
        return friendlyPair(vp, strategy, source, split, setup, "MODE");
    }

    private String modeSQL(String relation, String column, String order) {
        return "SELECT mode(" + column + order + ") AS vp_mode, COUNT(*) AS vp_count, COUNT(" + column
                + ") AS vp_nonnull, COUNT(*) FILTER (WHERE " + column + " IS NULL) AS vp_null_count, "
                + "COUNT(*) FILTER (WHERE " + column + " = 1) AS vp_ones, COUNT(*) FILTER (WHERE " + column
                + " = 2) AS vp_twos, (SELECT mode(" + column + ") FROM (SELECT * FROM " + relation
                + " WHERE FALSE) vp_empty) AS vp_empty_mode FROM " + relation;
    }

    private VpQueryPair quantileHistogramPair(VpSchema vp, ReconstructionStrategy strategy, String relation, DuckDBColumn value, List<String> setup) {
        String column = quote(value.getName());
        String source = quantileHistogramSQL(quote(vp.sourceTable), column);
        String split = quantileHistogramSQL(relation, column);
        return friendlyPair(vp, strategy, source, split, setup, "QUANTILE_HISTOGRAM");
    }

    private String quantileHistogramSQL(String relation, String value) {
        String nullable = "CASE WHEN " + quote(ROW_ID) + " % 4 = 0 THEN NULL ELSE " + value + " END";
        return "SELECT quantile(" + nullable + ", 0.5) AS vp_median, " + "quantile(" + nullable
                + ", 0.5) FILTER (WHERE FALSE) AS vp_empty_quantile, " + "histogram(" + nullable
                + ") AS vp_histogram, histogram(" + nullable + ") FILTER (WHERE FALSE) AS vp_empty_histogram, COUNT("
                + nullable + ") AS vp_nonnull, COUNT(*) AS vp_count, " + "(SELECT quantile(" + quote(ROW_ID)
                + ", 0.5) FROM (SELECT * FROM " + relation + " WHERE FALSE) vp_empty) AS vp_empty_input_quantile, "
                + "(SELECT histogram(" + quote(ROW_ID) + ") FROM (SELECT * FROM " + relation
                + " WHERE FALSE) vp_empty) AS vp_empty_input_histogram FROM " + relation;
    }

    private VpQueryPair lateralQueryPair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup) {
        String source = lateralQuerySQL(quote(vp.sourceTable));
        String split = lateralQuerySQL(relation);
        return friendlyPair(vp, strategy, source, split, setup, "LATERAL");
    }

    private VpQueryPair recursiveCtePair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup) {
        String source = recursiveCteSQL(quote(vp.sourceTable));
        String split = recursiveCteSQL(relation);
        return friendlyPair(vp, strategy, source, split, setup, "RECURSIVE_CTE");
    }

    private VpQueryPair jsonQueryPair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup) {
        String source = jsonQuerySQL(quote(vp.sourceTable));
        String split = jsonQuerySQL(relation);
        return friendlyPair(vp, strategy, source, split, setup, "JSON_QUERY");
    }

    private String jsonQuerySQL(String relation) {
        String row = quote(ROW_ID);
        String payload = "json_object('rowid', " + row + ", 'bucket', " + row + " % 3, 'items', json_array(" + row
                + ", " + row + " % 2))";
        return "SELECT " + row + ", json(" + payload + ") AS vp_json, json_extract(" + payload
                + ", '$.rowid') AS vp_json_rowid, json_type(" + payload + ", '$.items') AS vp_json_items_type, "
                + "json_keys(" + payload + ") AS vp_json_keys, json_array_length(json_extract(" + payload
                + ", '$.items')) AS vp_json_items_len FROM " + relation + " ORDER BY " + row;
    }

    private VpQueryPair spatialQueryPair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup) {
        String source = spatialQuerySQL(quote(vp.sourceTable));
        String split = spatialQuerySQL(relation);
        return friendlyPair(vp, strategy, source, split, setup, "SPATIAL_QUERY");
    }

    private String spatialQuerySQL(String relation) {
        String row = quote(ROW_ID);
        String point = "ST_Point(CAST(" + row + " AS DOUBLE), CAST(" + row + " % 11 AS DOUBLE))";
        return "SELECT " + row + ", " + point + " AS vp_geom, ST_IsValid(" + point + ") AS vp_valid, " + "ST_AsText("
                + point + ") AS vp_wkt, ST_Distance(" + point + ", ST_Point(0, 0)) AS vp_distance FROM " + relation
                + " ORDER BY " + row;
    }

    private VpQueryPair ftsQueryPair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup) {
        String sourceProbe = vp.prefix + "fts_source";
        String splitProbe = vp.prefix + "fts_split";
        setup.add("DROP TABLE IF EXISTS " + quote(sourceProbe));
        setup.add("DROP TABLE IF EXISTS " + quote(splitProbe));
        setup.add("CREATE TEMP TABLE " + quote(sourceProbe) + " AS SELECT " + quote(ROW_ID) + " AS doc_id, CASE WHEN "
                + quote(ROW_ID) + " % 4 = 0 THEN 'DuckDB alpha alpha'" + " WHEN " + quote(ROW_ID)
                + " % 4 = 1 THEN 'beta Unicode café'" + " WHEN " + quote(ROW_ID)
                + " % 4 = 2 THEN '' ELSE NULL END AS body FROM " + quote(vp.sourceTable));
        setup.add("CREATE TEMP TABLE " + quote(splitProbe) + " AS SELECT " + quote(ROW_ID) + " AS doc_id, CASE WHEN "
                + quote(ROW_ID) + " % 4 = 0 THEN 'DuckDB alpha alpha'" + " WHEN " + quote(ROW_ID)
                + " % 4 = 1 THEN 'beta Unicode café'" + " WHEN " + quote(ROW_ID)
                + " % 4 = 2 THEN '' ELSE NULL END AS body FROM " + relation);
        setup.add("PRAGMA create_fts_index('" + escapeLiteral(sourceProbe) + "', 'doc_id', 'body')");
        setup.add("PRAGMA create_fts_index('" + escapeLiteral(splitProbe) + "', 'doc_id', 'body')");
        String source = ftsQuerySQL(sourceProbe);
        String split = ftsQuerySQL(splitProbe);
        return friendlyPair(vp, strategy, source, split, setup, "FTS_QUERY");
    }

    private String ftsQuerySQL(String table) {
        // DuckDB's FTS extension creates one schema per indexed table, named
        // `fts_main_<table>` (for example, fts_main_documents). The older
        // `fts_main.<table>.match_bm25` spelling binds as a non-existent
        // catalog and causes the workload to be silently discarded during
        // EXPLAIN validation.
        String indexSchema = quote("fts_main_" + table);
        String qualified = indexSchema + ".match_bm25(doc_id, 'alpha')";
        return "SELECT doc_id, " + qualified + " AS vp_score FROM " + quote(table) + " WHERE " + qualified
                + " IS NOT NULL ORDER BY doc_id";
    }

    private boolean isJsonExtensionAvailable() {
        if (jsonExtensionAvailable == null) {
            try (Statement statement = state.getConnection().createStatement();
                    ResultSet result = statement.executeQuery("SELECT json_valid('{\"vp\":1}')")) {
                jsonExtensionAvailable = result.next() && result.getBoolean(1);
            } catch (SQLException e) {
                jsonExtensionAvailable = false;
            }
        }
        return jsonExtensionAvailable;
    }

    private boolean isFtsExtensionAvailable() {
        if (ftsExtensionAvailable == null) {
            ftsExtensionAvailable = loadOptionalExtension("fts");
            if (!ftsExtensionAvailable) {
                recordAdvancedFeature(AdvancedFeature.FTS_UNAVAILABLE);
            }
        }
        return ftsExtensionAvailable;
    }

    private boolean isSpatialExtensionAvailable() {
        if (spatialExtensionAvailable == null) {
            spatialExtensionAvailable = loadOptionalExtension("spatial");
            if (!spatialExtensionAvailable) {
                recordAdvancedFeature(AdvancedFeature.SPATIAL_UNAVAILABLE);
            }
        }
        return spatialExtensionAvailable;
    }

    private boolean loadOptionalExtension(String extension) {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement
                        .executeQuery("SELECT loaded, installed FROM duckdb_extensions() WHERE extension_name = '"
                                + escapeLiteral(extension) + "'")) {
            if (!result.next()) {
                return false;
            }
            if (result.getBoolean(1)) {
                return true;
            }
            if (!result.getBoolean(2)) {
                return false;
            }
        } catch (SQLException e) {
            return false;
        }
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute("LOAD " + extension);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    private String recursiveCteSQL(String relation) {
        String row = quote(ROW_ID);
        // The fixed depth and seed bound keep the oracle itself finite while still
        // exercising recursive evaluation over the reconstructed relation.
        return "WITH RECURSIVE walk(" + row + ", depth) AS (SELECT " + row + ", 0 FROM " + relation + " WHERE " + row
                + " < 2 UNION ALL SELECT " + row + ", depth + 1 FROM walk WHERE depth < 2) "
                + "SELECT COUNT(*) AS vp_walk_rows, MAX(depth) AS vp_max_depth FROM walk";
    }

    private String lateralQuerySQL(String relation) {
        String row = quote(ROW_ID);
        String input = "(SELECT * FROM " + relation + ")";
        return "SELECT t." + row + " AS vp_rowid, x.vp_seen, COALESCE(e.vp_empty_seen, 0) AS vp_empty_seen " + "FROM "
                + input + " t CROSS JOIN LATERAL (SELECT COUNT(*) AS vp_seen FROM " + input + " q WHERE q." + row
                + " <= t." + row + ") x LEFT JOIN LATERAL (SELECT COUNT(*) AS vp_empty_seen FROM " + input
                + " empty_q WHERE empty_q." + row + " < 0 AND empty_q." + row + " = t." + row
                + ") e ON TRUE ORDER BY t." + row;
    }

    private VpQueryPair qualifyFriendlyPair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup) {
        String projection = quote(ROW_ID);
        String source = "SELECT " + projection + " FROM " + quote(vp.sourceTable)
                + " QUALIFY ROW_NUMBER() OVER (ORDER BY " + quote(ROW_ID) + ") <= 2";
        String split = "SELECT " + projection + " FROM (SELECT " + projection + ", ROW_NUMBER() OVER (ORDER BY "
                + quote(ROW_ID) + ") AS vp_rn FROM " + relation + ") q WHERE vp_rn <= 2";
        return friendlyPair(vp, strategy, source, split, setup, "QUALIFY");
    }

    private VpQueryPair groupByAllFriendlyPair(VpSchema vp, ReconstructionStrategy strategy, String relation, DuckDBColumn key, List<String> setup) {
        String column = quote(key.getName());
        String source = "SELECT " + column + ", COUNT(*) AS vp_count FROM " + quote(vp.sourceTable) + " GROUP BY ALL";
        String split = "SELECT " + column + ", COUNT(*) AS vp_count FROM " + relation + " GROUP BY " + column;
        return friendlyPair(vp, strategy, source, split, setup, "GROUP BY ALL");
    }

    private VpQueryPair orderByAllFriendlyPair(VpSchema vp, ReconstructionStrategy strategy, String relation, DuckDBColumn key, List<String> setup) {
        String column = quote(key.getName());
        String projection = quote(ROW_ID) + ", " + column;
        String source = "SELECT " + projection + " FROM " + quote(vp.sourceTable) + " ORDER BY ALL";
        String split = "SELECT " + projection + " FROM " + relation + " ORDER BY " + column + ", " + quote(ROW_ID);
        return friendlyPair(vp, strategy, source, split, setup, "ORDER BY ALL");
    }

    private VpQueryPair starProjectionFriendlyPair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup, boolean replace) {
        VpColumn selected = vp.columns.get(0);
        String excluded = quote(selected.name);
        String source;
        List<String> explicit = new ArrayList<>();
        explicit.add(quote(ROW_ID));
        explicit.addAll(vp.columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        if (replace) {
            source = "SELECT * REPLACE (" + excluded + " AS " + excluded + ") FROM " + quote(vp.sourceTable);
        } else {
            source = "SELECT * EXCLUDE (" + excluded + ") FROM " + quote(vp.sourceTable);
            explicit.remove(excluded);
        }
        if (explicit.isEmpty()) {
            explicit.add(quote(ROW_ID));
        }
        String split = "SELECT " + String.join(", ", explicit) + " FROM " + relation;
        return friendlyPair(vp, strategy, source, split, setup, replace ? "SELECT * REPLACE" : "SELECT * EXCLUDE");
    }

    private VpQueryPair columnsFriendlyPair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup) {
        String explicit = vp.columns.stream().map(c -> quote(c.name)).collect(Collectors.joining(", "));
        String source = "SELECT COLUMNS('^c[0-9]+$') FROM " + quote(vp.sourceTable);
        String split = "SELECT " + explicit + " FROM " + relation;
        return friendlyPair(vp, strategy, source, split, setup, "COLUMNS");
    }

    private VpQueryPair positionalJoinPair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup) {
        String source = positionalAggregate(quote(vp.sourceTable));
        String split = positionalAggregate(relation);
        return friendlyPair(vp, strategy, source, split, setup, "POSITIONAL JOIN");
    }

    private String positionalAggregate(String relation) {
        return "SELECT COUNT(*) AS vp_rows, COUNT(l." + quote(ROW_ID) + ") AS vp_left_rows FROM (SELECT * FROM "
                + relation + " ORDER BY " + quote(ROW_ID) + ") l POSITIONAL JOIN (SELECT * FROM " + relation + " WHERE "
                + quote(ROW_ID) + " % 3 <> 0 ORDER BY " + quote(ROW_ID) + ") r";
    }

    private VpQueryPair asofJoinPair(VpSchema vp, ReconstructionStrategy strategy, String relation, DuckDBColumn temporal, List<String> setup) {
        String key = quote(temporal.getName());
        String source = asofAggregate(quote(vp.sourceTable), key);
        String split = asofAggregate(relation, key);
        return friendlyPair(vp, strategy, source, split, setup, "ASOF JOIN");
    }

    private String asofAggregate(String relation, String key) {
        return "SELECT COUNT(*) AS vp_rows, COUNT(r." + quote(ROW_ID) + ") AS vp_matches FROM (SELECT * FROM "
                + relation + " ORDER BY " + key + ", " + quote(ROW_ID) + ") l ASOF JOIN (SELECT * FROM " + relation
                + " ORDER BY " + key + ", " + quote(ROW_ID) + ") r ON l." + key + " >= r." + key;
    }

    private VpQueryPair samplePair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<String> setup) {
        String source = "SELECT COUNT(*) AS vp_sample_rows FROM " + quote(vp.sourceTable)
                + " USING SAMPLE reservoir(100%) (REPEATABLE (42))";
        String split = "SELECT COUNT(*) AS vp_sample_rows FROM " + relation
                + " USING SAMPLE reservoir(100%) (REPEATABLE (42))";
        recordAdvancedFeature(AdvancedFeature.SAMPLE);
        return friendlyPair(vp, strategy, source, split, setup, "SAMPLE");
    }

    private VpQueryPair pivotPair(VpSchema vp, ReconstructionStrategy strategy, String relation, DuckDBColumn key, List<String> setup) {
        String pivotKey = quote(key.getName());
        String measure = vp.columns.stream().filter(c -> !c.name.equals(key.getName()))
                .filter(c -> c.type.supportsAggregate("LIST")).map(c -> quote(c.name)).findFirst()
                .orElse(quote(ROW_ID));
        String aggregate = "list(" + measure + " ORDER BY " + quote(ROW_ID) + ")";
        // A directed source intentionally contains high-cardinality random values.
        // Pivoting directly on one of them creates one output column per row and
        // turns the memory-bound profile into a deterministic resource exhaustion
        // test rather than a useful equivalence test. Preserve PIVOT coverage while
        // restricting its domain to 17 deterministic buckets (including NULL).
        String bucketedInput = "(SELECT *, CASE WHEN " + pivotKey + " IS NULL THEN -1 ELSE LENGTH(CAST(" + pivotKey
                + " AS VARCHAR)) % 16 END AS " + quote("vp_pivot_bucket") + " FROM %s)";
        String source = "SELECT * FROM (PIVOT " + bucketedInput.replace("%s", quote(vp.sourceTable)) + " ON "
                + quote("vp_pivot_bucket") + " USING " + aggregate + ") p";
        String split = "SELECT * FROM (PIVOT " + bucketedInput.replace("%s", relation) + " ON "
                + quote("vp_pivot_bucket") + " USING " + aggregate + ") p";
        recordAdvancedFeature(AdvancedFeature.PIVOT);
        return friendlyPair(vp, strategy, source, split, setup, "PIVOT");
    }

    private VpQueryPair unpivotPair(VpSchema vp, ReconstructionStrategy strategy, String relation, List<DuckDBColumn> columns,
            List<String> setup) {
        String first = quote(columns.get(0).getName());
        String second = quote(columns.get(1).getName());
        String sourceInput = "(SELECT CAST(" + first + " AS VARCHAR) AS vp_u0, CAST(" + second
                + " AS VARCHAR) AS vp_u1 FROM " + quote(vp.sourceTable) + ")";
        String splitInput = "(SELECT CAST(" + first + " AS VARCHAR) AS vp_u0, CAST(" + second
                + " AS VARCHAR) AS vp_u1 FROM " + relation + ")";
        String source = "SELECT vp_name, vp_value FROM (UNPIVOT " + sourceInput
                + " ON vp_u0, vp_u1 INTO NAME vp_name VALUE vp_value) u";
        String split = "SELECT vp_name, vp_value FROM (UNPIVOT " + splitInput
                + " ON vp_u0, vp_u1 INTO NAME vp_name VALUE vp_value) u";
        recordAdvancedFeature(AdvancedFeature.UNPIVOT);
        return friendlyPair(vp, strategy, source, split, setup, "UNPIVOT");
    }

    private VpQueryPair friendlyPair(VpSchema vp, ReconstructionStrategy strategy, String source, String split, List<String> setup, String feature) {
        return new VpQueryPair(vp, strategy, source, split, setup,
                "shape=" + vp.baseShape + ", split=" + vp.splitStrategy + ", reconstruction=" + strategy
                        + ", friendly=" + feature,
                false);
    }

    private List<DuckDBExpression> maybeAppendRollup(List<DuckDBExpression> groupByExpressions,
            boolean hasWindowExpression, boolean rollupSafe) {
        if (!advancedFeaturesEnabled() || hasWindowExpression || !rollupSafe || groupByExpressions.isEmpty()
                || !Randomly.getBooleanWithSmallProbability()) {
            return groupByExpressions;
        }
        return List.of(rollupExpression(groupByExpressions));
    }

    private DuckDBExpression rollupExpression(List<DuckDBExpression> groupByExpressions) {
        String expressions = groupByExpressions.stream().map(DuckDBToStringVisitor::asString)
                .collect(Collectors.joining(", "));
        return new DuckDBText("ROLLUP (" + expressions + ")", null);
    }

    /* A NOCASE group may legally expose either spelling of an equivalent key. Select a stable binary representative. */
    private List<DuckDBExpression> canonicalGroupProjections(VpSchema vp, List<DuckDBExpression> groupByExpressions) {
        return groupByExpressions.stream().map(expression -> canonicalGroupProjection(vp, expression))
                .collect(Collectors.toList());
    }

    private DuckDBExpression canonicalGroupProjection(VpSchema vp, DuckDBExpression expression) {
        if (!(expression instanceof DuckDBColumnReference)) {
            return expression;
        }
        DuckDBColumn column = ((DuckDBColumnReference) expression).getColumn();
        String collation = vp.columnCollationClauses.getOrDefault(column.getName(), "");
        if (!collation.toUpperCase(Locale.ROOT).contains("NOCASE")) {
            return expression;
        }
        return new DuckDBText("MIN(" + DuckDBToStringVisitor.asString(expression) + " COLLATE C)", null);
    }

    private VpQueryPair generateRollupPair(VpSchema vp, ReconstructionStrategy strategy, SourceAst sourceAst)
            throws Exception {
        String groupKey = simpleColumnOrRowIdForSQL(sourceAst.columns);
        String aggregate = randomAdvancedAggregateSQL(sourceAst.columns);
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        String sourceRelation = sourceRelation(vp.sourceTable);
        String singleQuery = nativeRollupQuery(vp, sourceRelation, groupKey, aggregate);
        if (usesStringAgg(singleQuery)) {
            sourceRelation = orderedSourceRelation(vp.sourceTable);
            relation = orderRelationForGroupConcat(relation, vp.sourceTable);
            singleQuery = nativeRollupQuery(vp, sourceRelation, groupKey, aggregate);
        }
        String splitQuery = nativeRollupQuery(vp, relation, groupKey, aggregate);
        return new VpQueryPair(
                vp, strategy, singleQuery, splitQuery, extraSetup, "shape=" + vp.baseShape + ", split="
                        + vp.splitStrategy + ", reconstruction=" + strategy + ", advanced=duckdb-group-by-rollup",
                false);
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
        String predicate = simpleColumnOrRowIdForSQL(sourceAst.columns);
        String predicateColumn = qualifySelectedExpression(predicate, subAlias);
        String minimum = canonicalNoCaseAggregate(vp, "MIN", predicate, predicateColumn);
        int variant = state.getRandomly().getInteger(0, 3);
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        String singleQuery = correlatedSubquerySQL(variant, sourceRelationWithAlias(vp.sourceTable, sourceAlias),
                sourceRelationWithAlias(vp.sourceTable, subAlias), sourceAlias, subAlias, outerColumn, predicateColumn,
                minimum);
        String splitQuery = correlatedSubquerySQL(variant, relationWithAlias(relation, vp.sourceTable, sourceAlias),
                relationWithAlias(relation, vp.sourceTable, subAlias), sourceAlias, subAlias, outerColumn,
                predicateColumn, minimum);
        return new VpQueryPair(vp, strategy, singleQuery, splitQuery, extraSetup, "shape=" + vp.baseShape + ", split="
                + vp.splitStrategy + ", reconstruction=" + strategy + ", advanced=correlated-subquery", false);
    }

    private String qualifySelectedExpression(String expression, String alias) {
        return expression.replace(quote(ROW_ID), alias + "." + quote(ROW_ID));
    }

    private String correlatedSubquerySQL(int variant, String outerRelation, String subqueryRelation, String outerAlias,
            String subqueryAlias, String outerColumn, String predicateColumn, String minimum) {
        switch (variant) {
        case 0:
            return "SELECT " + outerColumn + " FROM " + outerRelation + " WHERE EXISTS (SELECT 1 FROM "
                    + subqueryRelation + " WHERE " + subqueryAlias + "." + quote(ROW_ID) + " = " + outerAlias + "."
                    + quote(ROW_ID) + " AND " + predicateColumn + " IS NOT NULL)";
        case 1:
            return "SELECT " + outerColumn + " FROM " + outerRelation + " WHERE " + outerAlias + "." + quote(ROW_ID)
                    + " IN (SELECT " + subqueryAlias + "." + quote(ROW_ID) + " FROM " + subqueryRelation + " WHERE "
                    + predicateColumn + " IS NOT NULL)";
        case 2:
            return "SELECT " + outerColumn + ", (SELECT " + minimum + " FROM " + subqueryRelation + " WHERE "
                    + subqueryAlias + "." + quote(ROW_ID) + " <= " + outerAlias + "." + quote(ROW_ID) + ") FROM "
                    + outerRelation;
        default:
            throw new AssertionError(variant);
        }
    }

    private String relationWithAlias(String relation, String sourceTable, String alias) {
        String suffix = " " + quote(sourceTable);
        if (!relation.endsWith(suffix)) {
            throw new IgnoreMeException();
        }
        return relation.substring(0, relation.length() - suffix.length()) + " " + alias;
    }

    private boolean isGroupByOrdinalLiteral(DuckDBExpression expression) {
        return expression instanceof DuckDBConstant.DuckDBIntConstant;
    }

    private DuckDBExpression generateComparisonStableExpression(DuckDBExpressionGenerator gen, SourceAst sourceAst) {
        List<DuckDBColumn> columns = sourceAst.columns;
        if (columns.isEmpty()) {
            return new DuckDBText(quote(ROW_ID) + " IS NOT NULL", null);
        }
        DuckDBColumn column = Randomly.fromList(columns);
        String value = quote(column.getName());
        switch (state.getRandomly().getInteger(0, 3)) {
        case 0:
            return new DuckDBText(value + " IS NOT NULL", null);
        case 1:
            return new DuckDBText(value + " IS NULL", null);
        case 2:
            return new DuckDBText(value + " IS NOT DISTINCT FROM " + value, null);
        default:
            return new DuckDBText(quote(ROW_ID) + " IS NOT NULL", null);
        }
    }

    private DuckDBExpression generatePredicate(DuckDBExpressionGenerator gen, SourceAst sourceAst) {
        if (state.getDbmsSpecificOptions().verticalPartitioningRiskyPredicates
                && Randomly.getBooleanWithRatherLowProbability()) {
            DuckDBExpression risky = generateRiskyPredicate(sourceAst);
            if (risky != null) {
                recordAdvancedFeature(AdvancedFeature.RISKY_PREDICATE);
                return risky;
            }
        }
        return generateComparisonStableExpression(gen, sourceAst);
    }

    private DuckDBExpression generateRiskyPredicate(SourceAst sourceAst) {
        if (sourceAst.columns.isEmpty()) {
            return null;
        }
        DuckDBColumn column = Randomly.fromList(sourceAst.columns);
        String value = quote(column.getName());
        DuckDBDataType type = column.getType().getPrimitiveDataType();
        int variant = state.getRandomly().getInteger(0, 4);
        if (column.getType().isNumeric()) {
            switch (variant) {
            case 0:
                return new DuckDBText(value + " BETWEEN -1 AND 1", null);
            case 1:
                return new DuckDBText(value + " IN (NULL, 0, 1)", null);
            case 2:
                return new DuckDBText("(" + value + " = 0 OR " + value + " IS NULL)", null);
            case 3:
                return new DuckDBText(value + " = '0'", null);
            default:
                return new DuckDBText("(" + value + " = CAST('0' AS " + type + ") AND NULL)", null);
            }
        }
        if (column.getType().isCharacterLike()) {
            switch (variant) {
            case 0:
                return new DuckDBText(value + " IN ('0', '1', NULL)", null);
            case 1:
                return new DuckDBText("(TRY_CAST(" + value + " AS DOUBLE) BETWEEN -1 AND 1 OR " + value
                        + " IS NULL)", null);
            case 2:
                return new DuckDBText(value + " = 0", null);
            case 3:
                return new DuckDBText("(" + value + " = '0' OR " + value + " IS NULL)", null);
            default:
                return new DuckDBText(value + " BETWEEN 'a' AND 'z'", null);
            }
        }
        if (type == DuckDBDataType.DATE) {
            return new DuckDBText(value + " BETWEEN DATE '1970-01-01' AND DATE '2030-01-01'", null);
        }
        if (type == DuckDBDataType.TIMESTAMP) {
            return new DuckDBText(value + " BETWEEN TIMESTAMP '1970-01-01' AND TIMESTAMP '2030-01-01'", null);
        }
        if (type == DuckDBDataType.TIME) {
            return new DuckDBText(value + " BETWEEN TIME '00:00:00' AND TIME '23:59:59'", null);
        }
        if (type == DuckDBDataType.INTERVAL) {
            return new DuckDBText(value + " BETWEEN INTERVAL '0 days' AND INTERVAL '1 day'", null);
        }
        if (type == DuckDBDataType.BOOLEAN) {
            return new DuckDBText("(" + value + " = TRUE OR " + value + " = NULL)", null);
        }
        return null;
    }

    private DuckDBExpression generateStableValueExpression(SourceAst sourceAst) {
        if (sourceAst.projectionColumns.isEmpty()) {
            return new DuckDBColumnReference(sourceAst.rowIdColumn);
        }
        return new DuckDBColumnReference(Randomly.fromList(sourceAst.projectionColumns));
    }

    private List<DuckDBExpression> createStableOrderByExpressions(DuckDBExpressionGenerator gen, SourceAst sourceAst,
            boolean useAggregate, List<DuckDBExpression> groupByExpressions,
            List<DuckDBExpression> nonAggregateExpressions) {
        if (useAggregate || !groupByExpressions.isEmpty()) {
            return groupByExpressions.stream().filter(this::isComparisonStableExpression).collect(Collectors.toList());
        }
        List<DuckDBExpression> orderBys = new ArrayList<>();
        List<DuckDBColumn> stableColumns = sourceAst.columns.stream().filter(c -> c.getType().supportsStableOrdering())
                .collect(Collectors.toList());
        if (!stableColumns.isEmpty() && Randomly.getBoolean()) {
            orderBys.add(new DuckDBColumnReference(Randomly.fromList(stableColumns)));
        }
        orderBys.add(new DuckDBColumnReference(sourceAst.rowIdColumn));
        return orderBys;
    }

    private boolean isComparisonStableExpression(DuckDBExpression expression) {
        String sql = DuckDBToStringVisitor.asString(expression);
        return !containsUnsafeCharFunction(sql) && !containsEquivalenceCollation(sql);
    }

    private boolean containsUnsafeCharFunction(String sql) {
        return DUCKDB_CHAR_FUNCTION.matcher(sql).find();
    }

    private boolean containsEquivalenceCollation(String sql) {
        return DUCKDB_EQUIVALENCE_COLLATION.matcher(sql).find();
    }

    private List<DuckDBExpression> chooseGroupByExpressions(List<DuckDBExpression> nonAggregateExpressions) {
        List<DuckDBExpression> groupable = nonAggregateExpressions.stream().filter(e -> !isGroupByOrdinalLiteral(e))
                .filter(this::isComparisonStableExpression).filter(this::isGroupableColumnExpression)
                .collect(Collectors.toList());
        if (!advancedFeaturesEnabled() || groupable.size() <= 1) {
            return groupable;
        }
        Collections.shuffle(groupable);
        return groupable.subList(0, state.getRandomly().getInteger(1, Math.min(3, groupable.size()) + 1));
    }

    private boolean isGroupableColumnExpression(DuckDBExpression expression) {
        if (!(expression instanceof DuckDBColumnReference)) {
            return true;
        }
        return !((DuckDBColumnReference) expression).getColumn().getType().isNested();
    }

    private DuckDBExpression generateHavingExpression(VpSchema vp, DuckDBExpressionGenerator gen,
            List<DuckDBColumn> columns) {
        String groupValue = simpleColumnOrRowIdForSQL(columns);
        String stableGroupValue = canonicalNoCaseAggregateInput(vp, groupValue);
        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            return new DuckDBText(Randomly.fromOptions("COUNT(*) > 0",
                    "SUM(" + numericExpressionForSQL(columns) + " ORDER BY " + quote(ROW_ID) + ") IS NOT NULL",
                    "MIN(" + stableGroupValue + ") IS NOT DISTINCT FROM MAX(" + stableGroupValue + ")"), null);
        }
        return new DuckDBText(Randomly.fromOptions("COUNT(*) >= 0", "COUNT(" + groupValue + ") >= 0",
                "MIN(" + stableGroupValue + ") IS NOT DISTINCT FROM MAX(" + stableGroupValue + ")"), null);
    }

    private String canonicalNoCaseAggregate(VpSchema vp, String function, String input, String qualifiedInput) {
        return function + "(" + canonicalNoCaseAggregateInput(vp, input, qualifiedInput) + ")";
    }

    private String canonicalNoCaseAggregateInput(VpSchema vp, String input) {
        return canonicalNoCaseAggregateInput(vp, input, input);
    }

    private String canonicalNoCaseAggregateInput(VpSchema vp, String input, String qualifiedInput) {
        for (Map.Entry<String, String> entry : vp.columnCollationClauses.entrySet()) {
            if (input.equals(quote(entry.getKey())) && entry.getValue().toUpperCase(Locale.ROOT).contains("NOCASE")) {
                return qualifiedInput + " COLLATE C";
            }
        }
        return qualifiedInput;
    }

    private DuckDBExpression generateAdvancedSelectExpression(SourceAst sourceAst, DuckDBExpressionGenerator gen) {
        String order = simpleOrderExpressionForSQL(sourceAst.columns) + ", " + quote(ROW_ID);
        String value = simpleColumnOrRowIdForSQL(sourceAst.columns);
        String numeric = numericExpressionForSQL(sourceAst.columns);
        return new DuckDBText(Randomly.fromOptions("ROW_NUMBER() OVER (ORDER BY " + order + ")",
                "RANK() OVER (ORDER BY " + order + ")", "LAG(" + value + ") OVER (ORDER BY " + quote(ROW_ID) + ")",
                "SUM(" + numeric + ") OVER (ORDER BY " + quote(ROW_ID) + ")"), null);
    }

    private DuckDBSelect createUnionAllBranch(SourceAst sourceAst, DuckDBExpressionGenerator gen, int fetchColumns) {
        DuckDBSelect right = new DuckDBSelect();
        List<DuckDBExpression> selected = new ArrayList<>();
        for (int i = 0; i < fetchColumns; i++) {
            selected.add(
                    Randomly.getBooleanWithRatherLowProbability() ? generateAdvancedSelectExpression(sourceAst, gen)
                            : generateComparisonStableExpression(gen, sourceAst));
        }
        right.setFetchColumns(selected);
        right.setFromList(List.of(new DuckDBTableReference(sourceAst.table)));
        if (Randomly.getBoolean()) {
            right.setWhereClause(generateComparisonStableExpression(gen, sourceAst));
        }
        return right;
    }

    private boolean isRowPreservingQuery(boolean useAggregate, DuckDBSelect select) {
        return !useAggregate && select.getGroupByClause().isEmpty();
    }

    private boolean hasVpRowIdTieBreaker(DuckDBSelect select, DuckDBColumn rowIdColumn) {
        List<DuckDBExpression> orderBys = select.getOrderByClauses();
        if (orderBys.isEmpty()) {
            return false;
        }
        DuckDBExpression last = orderBys.get(orderBys.size() - 1);
        return last instanceof DuckDBColumnReference
                && ((DuckDBColumnReference) last).getColumn().getName().equals(rowIdColumn.getName());
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

    private static void recordSourceRowCount(long rowCount) {
        synchronized (STATS_LOCK) {
            String bucket = sourceRowCountBucket(rowCount);
            SOURCE_ROW_COUNT_BUCKETS.put(bucket, SOURCE_ROW_COUNT_BUCKETS.getOrDefault(bucket, 0) + 1);
            sourceRowCountSamples++;
        }
        maybePersistVpStats();
    }

    private static String sourceRowCountBucket(long rowCount) {
        if (rowCount == 0) {
            return "0";
        }
        if (rowCount == 1) {
            return "1";
        }
        if (rowCount <= 10) {
            return "2-10";
        }
        if (rowCount <= 100) {
            return "11-100";
        }
        if (rowCount <= 255) {
            return "101-255";
        }
        if (rowCount == 256) {
            return "256";
        }
        if (rowCount <= 1024) {
            return "257-1024";
        }
        return "1025+";
    }

    private static void recordCompletedVpRun(int checkedPairs, int attempts) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            completedOracleRuns++;
            completedQueryPairs += checkedPairs;
            completedAttempts += attempts;
        }
        maybePersistVpStats();
    }

    private static void recordKnownBugSuppressed(KnownVpBug knownBug) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            increment(KNOWN_BUG_COUNTS, knownBug);
        }
        maybePersistVpStats();
    }

    public static void recordAdvancedFeature(AdvancedFeature feature) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            increment(ADVANCED_FEATURE_COUNTS, feature);
        }
        maybePersistVpStats();
    }

    private static void recordDeepFeature(DeepFeature feature) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            increment(DEEP_FEATURE_COUNTS, feature);
        }
        maybePersistVpStats();
    }

    private static String formatVpStats(String event) {
        StringBuilder sb = new StringBuilder();
        sb.append("-- [vp-stats] event=").append(event).append(" dbms=duckdb");
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
        sb.append("-- [vp-stats] source-row-count-buckets samples=").append(sourceRowCountSamples).append(" ")
                .append(formatBucketCounts(SOURCE_ROW_COUNT_BUCKETS, sourceRowCountSamples));
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
        sb.append("-- [vp-stats] deep-feature-counts ")
                .append(formatEnumCounts(DeepFeature.values(), DEEP_FEATURE_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] optimizer-mode-counts ")
                .append(formatEnumCounts(OptimizerMode.values(), OPTIMIZER_MODE_COUNTS));
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

    private static String formatBucketCounts(Map<String, Integer> buckets, int samples) {
        return buckets.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue() + " ("
                        + String.format(Locale.ROOT, "%.2f%%", samples == 0 ? 0.0 : entry.getValue() * 100.0 / samples)
                        + ")")
                .collect(Collectors.joining(", "));
    }

    public static void logSchemaShapeStats(DuckDBGlobalState state, String phase) throws Exception {
        registerVpStatsShutdownHook();
        int emptySingle = 0;
        int emptyMulti = 0;
        int nonEmptySingle = 0;
        int nonEmptyMulti = 0;
        state.updateSchema();
        for (DuckDBTable table : state.getSchema().getDatabaseTables()) {
            if (table.isView() || table.getName().startsWith("vp_") || table.getColumns().isEmpty()) {
                continue;
            }
            boolean empty;
            try {
                empty = table.getNrRows(state) == 0;
            } catch (AssertionError | RuntimeException e) {
                if (isSkippableDuckDBError(e)) {
                    continue;
                }
                throw e;
            }
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
        maybePersistVpStats();
    }

    private static void registerVpStatsShutdownHook() {
        if (SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(DuckDBVerticalPartitioningOracle::writeFinalVpStats));
            persistVpStats("process-start");
        }
    }

    private static void writeFinalVpStats() {
        persistVpStats("process-final");
    }

    private static void maybePersistVpStats() {
        long updates = STATS_UPDATE_COUNT.incrementAndGet();
        long now = System.nanoTime();
        long last = LAST_STATS_PERSIST_NANOS.get();
        if (updates % STATS_CHECKPOINT_INTERVAL != 0 && now - last < STATS_CHECKPOINT_PERIOD_NANOS) {
            return;
        }
        if (!LAST_STATS_PERSIST_NANOS.compareAndSet(last, now)) {
            return;
        }
        persistVpStats("process-checkpoint");
    }

    private static void persistVpStats(String event) {
        String log;
        synchronized (STATS_LOCK) {
            log = formatVpStats(event);
        }
        File parent = STATS_FILE.getParentFile();
        Path target = STATS_FILE.toPath();
        synchronized (STATS_WRITE_LOCK) {
            try {
                if (parent != null) {
                    Files.createDirectories(parent.toPath());
                }
                Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
                Files.writeString(temporary, log, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
                try {
                    Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                    Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException ignored) {
                // Statistics are diagnostic data. A native crash must not be
                // delayed by an unavailable log directory or filesystem.
            }
        }
    }

    private DuckDBExpression generateVpAggregate(VpSchema vp, DuckDBExpressionGenerator gen,
            List<DuckDBColumn> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            return new DuckDBText(randomAdvancedAggregateSQL(columns), null);
        }
        DuckDBAggregateFunction func = Randomly.fromOptions(DuckDBAggregateFunction.MAX, DuckDBAggregateFunction.MIN,
                DuckDBAggregateFunction.AVG, DuckDBAggregateFunction.COUNT, DuckDBAggregateFunction.SUM,
                DuckDBAggregateFunction.STDDEV_SAMP, DuckDBAggregateFunction.STDDEV_POP,
                DuckDBAggregateFunction.VAR_POP, DuckDBAggregateFunction.VAR_SAMP);
        if (isOrderSensitiveNumericAggregate(func)) {
            DuckDBExpression input = func == DuckDBAggregateFunction.SUM ? generateSumInputExpression(gen, columns)
                    : generateNumericStableExpression(columns);
            return orderedAggregate(func, input);
        }
        List<DuckDBExpression> args = new ArrayList<>();
        DuckDBExpression input = generateStableValueExpression(columns);
        if (func == DuckDBAggregateFunction.MIN || func == DuckDBAggregateFunction.MAX) {
            String stableInput = canonicalNoCaseValueExpression(vp, input);
            if (!stableInput.equals(DuckDBToStringVisitor.asString(input))) {
                return new DuckDBText(func + "(" + stableInput + ")", null);
            }
        }
        args.add(input);
        return new DuckDBFunction<DuckDBAggregateFunction>(args, func);
    }

    private String canonicalNoCaseValueExpression(VpSchema vp, DuckDBExpression expression) {
        String sql = DuckDBToStringVisitor.asString(expression);
        if (!(expression instanceof DuckDBColumnReference)) {
            return sql;
        }
        DuckDBColumn column = ((DuckDBColumnReference) expression).getColumn();
        String collation = vp.columnCollationClauses.getOrDefault(column.getName(), "");
        return collation.toUpperCase(Locale.ROOT).contains("NOCASE") ? sql + " COLLATE C" : sql;
    }

    private boolean isOrderSensitiveNumericAggregate(DuckDBAggregateFunction function) {
        return function == DuckDBAggregateFunction.SUM || function == DuckDBAggregateFunction.AVG
                || function == DuckDBAggregateFunction.STDDEV_SAMP || function == DuckDBAggregateFunction.STDDEV_POP
                || function == DuckDBAggregateFunction.VAR_POP || function == DuckDBAggregateFunction.VAR_SAMP;
    }

    private DuckDBExpression orderedAggregate(DuckDBAggregateFunction function, DuckDBExpression input) {
        return new DuckDBText(
                function + "(" + DuckDBToStringVisitor.asString(input) + " ORDER BY " + quote(ROW_ID) + ")", null);
    }

    private DuckDBExpression generateSumInputExpression(DuckDBExpressionGenerator gen, List<DuckDBColumn> columns) {
        if (Randomly.getBoolean()) {
            DuckDBColumn column = Randomly.fromList(columns);
            DuckDBColumnReference ref = new DuckDBColumnReference(column);
            if (column.getType().getPrimitiveDataType() == DuckDBDataType.VARCHAR) {
                if (state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns
                        || advancedFeaturesEnabled()) {
                    if (state.getRandomly().getInteger(0, 100) < 70) {
                        return new DuckDBText(
                                "CAST(" + DuckDBToStringVisitor.asString(ref) + " AS " + randomSumCastType() + ")",
                                null);
                    }
                    return generateNumericStableExpression(columns);
                }
                return new DuckDBText("CAST(" + DuckDBToStringVisitor.asString(ref) + " AS REAL)", null);
            }
            return ref;
        }
        return generateNumericStableExpression(columns);
    }

    private DuckDBExpression generateStableValueExpression(List<DuckDBColumn> columns) {
        if (columns.isEmpty()) {
            return new DuckDBText(quote(ROW_ID), null);
        }
        return new DuckDBColumnReference(Randomly.fromList(columns));
    }

    private DuckDBExpression generateNumericStableExpression(List<DuckDBColumn> columns) {
        List<DuckDBColumn> numericColumns = columns.stream().filter(c -> isNumericType(c.getType()))
                .collect(Collectors.toList());
        if (numericColumns.isEmpty()) {
            return new DuckDBText(quote(ROW_ID), null);
        }
        return new DuckDBColumnReference(Randomly.fromList(numericColumns));
    }

    private String randomSumCastType() {
        return Randomly.fromOptions("INTEGER", "REAL", "NUMERIC", "TEXT");
    }

    private String randomAdvancedAggregateSQL(List<DuckDBColumn> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()) {
            recordAdvancedFeature(AdvancedFeature.GROUP_CONCAT);
            return "string_agg(" + groupConcatInputForSQL(columns) + ", ',')";
        }
        String numeric = numericExpressionForSQL(columns);
        return Randomly.fromOptions("AVG(" + numeric + " ORDER BY " + quote(ROW_ID) + ")",
                "SUM(" + numeric + " ORDER BY " + quote(ROW_ID) + ")", "MIN(" + numeric + ")", "MAX(" + numeric + ")",
                "COUNT(" + numeric + ")");
    }

    private String groupConcatInputForSQL(List<DuckDBColumn> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        DuckDBColumn column = Randomly.fromList(columns);
        if (isCharacterLike(column.getType()) || Randomly.getBoolean()) {
            return "CAST(" + quote(column.getName()) + " AS TEXT)";
        }
        return quote(column.getName());
    }

    private String numericExpressionForSQL(List<DuckDBColumn> columns) {
        List<DuckDBColumn> numericColumns = columns.stream().filter(c -> isNumericType(c.getType()))
                .collect(Collectors.toList());
        if (!numericColumns.isEmpty() && Randomly.getBoolean()) {
            return quote(Randomly.fromList(numericColumns).getName());
        }
        return quote(ROW_ID);
    }

    private String simpleColumnOrRowIdForSQL(List<DuckDBColumn> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        return quote(Randomly.fromList(columns).getName());
    }

    private String simpleOrderExpressionForSQL(List<DuckDBColumn> columns) {
        if (columns.isEmpty()) {
            return quote(ROW_ID);
        }
        return simpleColumnOrRowIdForSQL(columns);
    }

    private static final class SourceAst {
        private final DuckDBTable table;
        private final DuckDBColumn rowIdColumn;
        private final List<DuckDBColumn> columns;
        private final List<DuckDBColumn> projectionColumns;

        private SourceAst(DuckDBTable table, DuckDBColumn rowIdColumn, List<DuckDBColumn> columns,
                List<DuckDBColumn> projectionColumns) {
            this.table = table;
            this.rowIdColumn = rowIdColumn;
            this.columns = columns;
            this.projectionColumns = projectionColumns;
        }
    }

    private SourceAst createSourceAst(VpSchema vp) {
        List<DuckDBColumn> astColumns = new ArrayList<>();
        List<DuckDBColumn> expressionColumns = new ArrayList<>();
        DuckDBColumn rowId = new DuckDBColumn(ROW_ID,
                new DuckDBCompositeDataType(DuckDBDataType.INT, vp.rowIdType.astTypeSize), true, false);
        astColumns.add(rowId);
        expressionColumns.add(rowId);
        for (VpColumn column : vp.columns) {
            DuckDBColumn astColumn = new DuckDBColumn(column.name, column.type, false, true);
            astColumns.add(astColumn);
            if (supportsVpExpression(column.type)) {
                expressionColumns.add(astColumn);
            }
        }
        DuckDBTable table = new DuckDBTable(vp.sourceTable, astColumns, List.of(), false);
        for (DuckDBColumn column : astColumns) {
            column.setTable(table);
        }
        return new SourceAst(table, rowId, expressionColumns, astColumns);
    }

    private boolean supportsVpExpression(DuckDBCompositeDataType type) {
        return type.supportsVpExpression();
    }

    private String renderSelectPair(DuckDBSelect left, DuckDBSelect right, DuckDBExpression fromRelation) {
        String renderedLeft = renderSelectWithFrom(left, fromRelation);
        if (right == null) {
            return renderedLeft;
        }
        return "(" + renderedLeft + ") UNION ALL (" + renderSelectWithFrom(right, fromRelation) + ")";
    }

    private String renderSelectWithFrom(DuckDBSelect select, DuckDBExpression fromRelation) {
        List<DuckDBExpression> originalFromList = select.getFromList();
        select.setFromList(List.of(fromRelation));
        try {
            return DuckDBToStringVisitor.asString(select);
        } finally {
            select.setFromList(originalFromList);
        }
    }

    private boolean usesStringAgg(String query) {
        return query.toUpperCase(Locale.ROOT).contains("STRING_AGG");
    }

    private String sourceRelation(String sourceTable) {
        return quote(sourceTable) + " " + quote(sourceTable);
    }

    private String sourceRelationWithAlias(String sourceTable, String alias) {
        return quote(sourceTable) + " " + alias;
    }

    private String orderedSourceRelation(String sourceTable) {
        return "(SELECT * FROM " + quote(sourceTable) + " ORDER BY " + quote(ROW_ID) + ") " + quote(sourceTable);
    }

    private String nativeRollupQuery(VpSchema vp, String relation, String groupKey, String aggregate) {
        DuckDBSelect select = new DuckDBSelect();
        select.setFetchColumns(List.of(new DuckDBText(canonicalNoCaseGroupKey(vp, groupKey) + " AS vp_group_key", null),
                new DuckDBText(aggregate + " AS vp_agg", null)));
        select.setFromList(List.of(new DuckDBText(relation, null)));
        select.setGroupByClause(List.of(rollupExpression(List.of(new DuckDBText(groupKey, null)))));
        return DuckDBToStringVisitor.asString(select);
    }

    private String canonicalNoCaseGroupKey(VpSchema vp, String groupKey) {
        for (Map.Entry<String, String> entry : vp.columnCollationClauses.entrySet()) {
            if (groupKey.equals(quote(entry.getKey()))
                    && entry.getValue().toUpperCase(Locale.ROOT).contains("NOCASE")) {
                return "MIN(" + groupKey + " COLLATE C)";
            }
        }
        return groupKey;
    }

    private String orderRelationForGroupConcat(String relation, String sourceTable) {
        String suffix = " " + quote(sourceTable);
        if (!relation.endsWith(suffix)) {
            throw new IgnoreMeException();
        }
        String relationBody = relation.substring(0, relation.length() - suffix.length());
        return "(SELECT * FROM " + relationBody + " ORDER BY " + quote(ROW_ID) + ") " + quote(sourceTable);
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
            return reconstructedSubquery(vp, joinProjection(vp, "l", "r", true), " FROM " + quote(vp.rightTable)
                    + " r LEFT JOIN " + quote(vp.leftTable) + " l ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID));
        case FULL_OUTER_SIMULATION:
            String probedLeft = probedPartitionRelation(vp, vp.leftTable, vp.leftColumns, 1);
            String probedRight = probedPartitionRelation(vp, vp.rightTable, vp.rightColumns, 2);
            String fullOuter = "SELECT " + joinProjection(vp, "l", "r", false) + " FROM " + probedLeft + " l LEFT JOIN "
                    + quote(vp.rightTable) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID) + " UNION ALL SELECT "
                    + joinProjection(vp, "l", "r", true) + " FROM " + probedRight + " r LEFT JOIN "
                    + quote(vp.leftTable) + " l ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID)
                    + " WHERE NOT EXISTS (SELECT 1 FROM " + quote(vp.leftTable) + " lx WHERE lx." + quote(ROW_ID)
                    + " = r." + quote(ROW_ID) + ")";
            return "(SELECT * FROM (" + fullOuter + ") full_outer WHERE full_outer." + quote(ROW_ID) + " IN (SELECT "
                    + quote(ROW_ID) + " FROM " + quote(vp.sourceTable) + ")) " + quote(vp.sourceTable);
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
            extraSetup.add("DROP TABLE IF EXISTS " + quote(vp.tempTable));
            extraSetup.add("CREATE TEMP TABLE " + quote(vp.tempTable) + " AS SELECT "
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
                    " FROM " + quote(vp.leftTable) + " l CROSS JOIN " + quote(vp.rightTable) + " r WHERE l."
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
        String packedRowId = "CAST(json_extract_string(jp.payload, '$.vp') AS " + vp.rowIdType.sqlType + ")";
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN (SELECT " + quote(ROW_ID) + ", JSON_OBJECT('vp', "
                        + quote(ROW_ID) + ") AS payload FROM " + quote(vp.rightTable) + ") jp ON l." + quote(ROW_ID)
                        + " = " + packedRowId + " JOIN " + quote(vp.rightTable) + " r ON r." + quote(ROW_ID) + " = "
                        + packedRowId);
    }

    private String stringPackRelation(VpSchema vp) {
        String unpackedRowId = "CAST(split_part(sp.payload, ':', 2) AS " + vp.rowIdType.sqlType + ")";
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN (SELECT " + quote(ROW_ID) + ", 'vp:' || " + quote(ROW_ID)
                        + " AS payload FROM " + quote(vp.rightTable) + ") sp ON 'vp:' || l." + quote(ROW_ID)
                        + " = sp.payload JOIN " + quote(vp.rightTable) + " r ON r." + quote(ROW_ID) + " = "
                        + unpackedRowId);
    }

    private String lateralRelation(VpSchema vp) {
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN LATERAL (SELECT * FROM " + quote(vp.rightTable)
                        + " r0 WHERE r0." + quote(ROW_ID) + " = l." + quote(ROW_ID) + ") r ON TRUE");
    }

    private String probedPartitionRelation(VpSchema vp, String tableName, List<VpColumn> partitionColumns,
            int rowIdOffset) {
        List<String> columns = new ArrayList<>();
        columns.add(quote(ROW_ID));
        columns.addAll(partitionColumns.stream().map(column -> quote(column.name)).collect(Collectors.toList()));
        String projection = String.join(", ", columns);
        String extraRowId = "(SELECT COALESCE(MAX(" + quote(ROW_ID) + "), 0) + " + rowIdOffset + " FROM "
                + quote(tableName) + ")";
        String sampleProjection = columns.size() == 1 ? quote(ROW_ID) : columns.subList(1, columns.size()).stream()
                .map(column -> "sample." + column).collect(Collectors.joining(", "));
        String extraProjection = columns.size() == 1 ? "" : ", " + sampleProjection;
        String sample = "(SELECT "
                + (columns.size() == 1 ? quote(ROW_ID) : String.join(", ", columns.subList(1, columns.size())))
                + " FROM " + quote(tableName) + " ORDER BY " + quote(ROW_ID) + " DESC LIMIT 1) sample";
        return "(SELECT " + projection + " FROM " + quote(tableName) + " UNION ALL SELECT " + extraRowId
                + extraProjection + " FROM " + sample + ")";
    }

    private void comparePair(VpQueryPair pair) throws Exception {
        OptimizerMode optimizerMode = nextOptimizerMode();
        recordOptimizerMode(optimizerMode);
        KnownVpBug nativeCrashPattern = classifyKnownNativeCrashGenerationPattern(pair, optimizerMode);
        if (nativeCrashPattern != null
                && state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns) {
            recordKnownBugSuppressed(nativeCrashPattern);
            logKnownGenerationSuppression(pair, nativeCrashPattern, optimizerMode);
            throw new IgnoreMeException();
        }
        comparePair(pair, optimizerMode);
    }

    private void comparePair(VpQueryPair pair, OptimizerMode optimizerMode) throws Exception {
        for (String setup : pair.extraSetupStatements) {
            executeNoSchemaRefresh(setup);
            pair.schema.setupStatements.add(setup);
        }
        if (isKnownDisabledJoinOrderExplosion(pair.splitQuery, optimizerMode.disabledOptimizers,
                materializedSourceRowCount(pair.schema))) {
            // Disabling join_order is useful for plan-diversity testing, but on
            // multi-join VP rewrites DuckDB can legally leave a CROSS_PRODUCT
            // before applying the row-id predicates. At the directed-data size
            // this creates tens of billions of intermediate rows and stalls the
            // whole fuzzing run. Keep this combination out of native execution.
            throw new IgnoreMeException();
        }
        QueryExecution sourceValidation = explainQuery(pair.singleQuery);
        if (!sourceValidation.succeeded
                && isSkippableQueryErrorText(normalizeDuckDBQueryError(sourceValidation.error))) {
            throw new IgnoreMeException();
        }
        QueryExecution singleExecution = executePairQuery(pair, pair.singleQuery, false, optimizerMode);
        QueryExecution splitExecution = executePairQuery(pair, pair.splitQuery, true, optimizerMode);
        if (!singleExecution.succeeded || !splitExecution.succeeded) {
            if (singleExecution.succeeded != splitExecution.succeeded) {
                KnownVpBug knownBug = classifyKnownExecutionBug(pair, singleExecution, splitExecution);
                if (suppressKnownExecutionBug(pair, knownBug, singleExecution, splitExecution)) {
                    throw new IgnoreMeException();
                }
                // A one-sided failure is itself a differential signal. In particular,
                // LIMIT/empty-input short-circuiting can expose plan-dependent failures,
                // so do not hide it behind the generic invalid-query whitelist.
                throw new AssertionError(formatErrorBugReport(pair, singleExecution, splitExecution));
            }
            // The generated expression can be invalid before the relation rewrite is
            // considered. Different plans may then report different conversion details;
            // neither side produced a result, so this is not an asymmetric VP signal.
            if (bothFailedWithSameExpectedResourceFailure(singleExecution.error, splitExecution.error)) {
                // Both forms of the same known resource failure mean that neither
                // query produced a relational result. Do not conflate that with a
                // VP discrepancy. A different resource failure on each side remains
                // a candidate: it can expose a plan-dependent engine defect.
                throw new IgnoreMeException();
            }
            if (bothFailedWithSameDuckDBError(singleExecution.error, splitExecution.error)) {
                throw new IgnoreMeException();
            }
            if (bothAreSkippableDuckDBErrors(singleExecution.error, splitExecution.error)) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(formatErrorBugReport(pair, singleExecution, splitExecution));
        }
        QueryResultMultiset singleRows = singleExecution.result;
        QueryResultMultiset splitRows = splitExecution.result;
        boolean resultsMatch = resultMultisetsMatch(singleRows, splitRows);
        if (!resultsMatch) {
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
        recordExecutedAdvancedFeatures(pair);
    }

    static boolean isKnownDisabledJoinOrderExplosion(String query, String disabledOptimizers, long sourceRows) {
        if (query == null || disabledOptimizers == null || sourceRows < VECTOR_BOUNDARY_ROWS[0]
                || !(disabledOptimizers.equals("join_order")
                        || disabledOptimizers.equals(VP_SPLIT_DISABLED_OPTIMIZERS))) {
            return false;
        }
        String normalized = query.toUpperCase(Locale.ROOT);
        int joins = countWord(normalized, "JOIN");
        boolean hasGroupedJoinInput = normalized.contains("GROUP BY") && normalized.contains("VP_ROWID")
                && normalized.matches("(?s).*JOIN\\s*\\(\\s*SELECT.*GROUP\\s+BY.*");
        boolean explicitCrossJoin = normalized.contains("CROSS JOIN");
        // A correlated EXISTS duplicates the three-input aggregate reconstruction.
        // The 2047-row vector-boundary fixture already produces nested Cartesian
        // products here, so it cannot use the 4096-row threshold for a single copy.
        boolean repeatedAggregateExists = hasGroupedJoinInput && joins >= 4
                && countOccurrences(normalized, "GROUP BY") >= 2
                && normalized.matches("(?s).*\\bWHERE\\s+EXISTS\\s*\\(\\s*SELECT\\b.*");
        return repeatedAggregateExists
                || sourceRows >= 4096 && joins >= 2 && (hasGroupedJoinInput || explicitCrossJoin);
    }

    @SuppressWarnings("unused")
    private boolean isPlanIndependentInvalidIntervalBoolean(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        if (!single.succeeded || split.succeeded) {
            return false;
        }
        String error = normalizeDuckDBQueryError(split.error);
        if (!"Conversion Error: Unimplemented type for cast (INTERVAL -> BOOLEAN)".equals(error)) {
            return false;
        }
        return hasInvalidIntervalBooleanLiteral(pair.singleQuery) && hasInvalidIntervalBooleanLiteral(pair.splitQuery);
    }

    private boolean hasInvalidIntervalBooleanLiteral(String query) {
        return query.toUpperCase(Locale.ROOT).matches("(?s).*INTERVAL\\s*'[^']*'\\s*AND.*NULL.*");
    }

    @SuppressWarnings("unused")
    private boolean isPlanIndependentInvalidDatePredicate(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        if (!single.succeeded || split.succeeded) {
            return false;
        }
        String error = normalizeDuckDBQueryError(split.error);
        if (error == null || !error.startsWith("Conversion Error: invalid date field format: \"\"")) {
            return false;
        }
        return hasInvalidDatePredicateShape(pair.singleQuery) && hasInvalidDatePredicateShape(pair.splitQuery);
    }

    private boolean hasInvalidDatePredicateShape(String query) {
        String upper = query.toUpperCase(Locale.ROOT);
        return upper.contains("CASE '' WHEN") && upper.contains("DATE '") && upper.contains("AND(NULL)");
    }

    @SuppressWarnings("unused")
    private boolean isPlanIndependentInvalidIntegerPredicate(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        if (single.succeeded || !split.succeeded) {
            return false;
        }
        String error = normalizeDuckDBQueryError(single.error);
        if (error == null || !error.startsWith("Conversion Error: Could not convert string ")
                || !error.endsWith(" to INT64")) {
            return false;
        }
        return hasInvalidIntegerPredicateShape(pair.singleQuery) && hasInvalidIntegerPredicateShape(pair.splitQuery);
    }

    private boolean hasInvalidIntegerPredicateShape(String query) {
        String upper = query.toUpperCase(Locale.ROOT);
        return upper.contains("NOT IN") && query.matches("(?s).*NOT\\s+IN.*'[^']+'.*");
    }

    @SuppressWarnings("unused")
    private boolean isPlanIndependentInvalidIntegerOperatorPredicate(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        if (!single.succeeded || split.succeeded) {
            return false;
        }
        String error = normalizeDuckDBQueryError(split.error);
        if (error == null || !error.startsWith("Conversion Error: Could not convert string '")
                || !error.endsWith("' to INT64")) {
            return false;
        }
        return hasInvalidIntegerOperatorPredicateShape(pair.singleQuery)
                && hasInvalidIntegerOperatorPredicateShape(pair.splitQuery);
    }

    private boolean hasInvalidIntegerOperatorPredicateShape(String query) {
        String upper = query.toUpperCase(Locale.ROOT);
        return upper.contains("AND(NULL)") && query.matches("(?s).*\\|\\s*\\(\\s*'[^']+'\\s*\\).*AND\\s*\\(?\\s*NULL");
    }

    @SuppressWarnings("unused")
    private boolean isPlanIndependentInvalidMixedTypeConversion(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        QueryExecution failed = single.succeeded ? split : single;
        QueryExecution succeeded = single.succeeded ? single : split;
        if (!succeeded.succeeded || failed.succeeded) {
            return false;
        }
        String error = normalizeDuckDBQueryError(failed.error);
        if (error == null) {
            return false;
        }
        String source = pair.singleQuery.toUpperCase(Locale.ROOT);
        String rewritten = pair.splitQuery.toUpperCase(Locale.ROOT);
        if (!hasMixedTypeInPredicateShape(source) || !hasMixedTypeInPredicateShape(rewritten)) {
            return false;
        }
        if (error.equals("Conversion Error: Unimplemented type for cast (DATE -> BLOB)")) {
            return source.contains("DATE '") && rewritten.contains("DATE '") && source.contains("BLOB '")
                    && rewritten.contains("BLOB '");
        }
        if (error.startsWith("Conversion Error: Unimplemented type for cast (BIGINT -> BLOB)")) {
            return source.contains("BLOB '") && rewritten.contains("BLOB '")
                    && source.matches("(?s).*\\bIN\\s*\\(.*VP_ROWID.*")
                    && rewritten.matches("(?s).*\\bIN\\s*\\(.*VP_ROWID.*");
        }
        if (error.equals("Conversion Error: Unimplemented type for cast (INTEGER -> DATE)")) {
            return source.contains("NULL") && rewritten.contains("NULL")
                    && source.matches("(?s).*\\bIN\\s*\\(.*DATE\\s*'.*")
                    && rewritten.matches("(?s).*\\bIN\\s*\\(.*DATE\\s*'.*");
        }
        if (error.equals("Conversion Error: Unimplemented type for cast (TIMESTAMP -> BOOLEAN)")) {
            return source.contains("TIMESTAMP '") && rewritten.contains("TIMESTAMP '") && source.contains("AND(NULL)")
                    && rewritten.contains("AND(NULL)");
        }
        if (error.equals("Conversion Error: Unimplemented type for cast (DATE -> BOOLEAN)")) {
            return source.contains("DATE '") && rewritten.contains("DATE '") && source.contains("AND(NULL)")
                    && rewritten.contains("AND(NULL)");
        }
        return error.startsWith("Conversion Error: Could not convert string ") && error.contains(" to DECIMAL(")
                && source.contains("COLLATE") && rewritten.contains("COLLATE") && source.contains("NOT IN")
                && rewritten.contains("NOT IN") && source.contains("CAST('0.125' AS DECIMAL")
                && rewritten.contains("CAST('0.125' AS DECIMAL");
    }

    private boolean hasMixedTypeInPredicateShape(String query) {
        return query.matches("(?s).*\\bIN\\s*\\(.*");
    }

    @SuppressWarnings("unused")
    private boolean isPlanIndependentInvalidVarcharBooleanPredicate(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        if (!single.succeeded || split.succeeded) {
            return false;
        }
        String error = normalizeDuckDBQueryError(split.error);
        if (error == null || !error.startsWith("Conversion Error: Could not convert string '")
                || !error.matches(".*' to BOOL when casting from source column [A-Z0-9_]+")) {
            return false;
        }
        String column = error.substring(error.lastIndexOf("source column ") + "source column ".length())
                .toUpperCase(Locale.ROOT);
        return hasInvalidVarcharBooleanPredicateShape(pair.singleQuery, column)
                && hasInvalidVarcharBooleanPredicateShape(pair.splitQuery, column);
    }

    private boolean hasInvalidVarcharBooleanPredicateShape(String query, String column) {
        String upper = query.toUpperCase(Locale.ROOT);
        return upper.contains("AND(NULL)") && upper.contains("." + column);
    }

    @SuppressWarnings("unused")
    private boolean isPlanIndependentInvalidStringBooleanPredicate(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        if (!single.succeeded || split.succeeded) {
            return false;
        }
        String error = normalizeDuckDBQueryError(split.error);
        if (error == null || !error.startsWith("Conversion Error: Could not convert string '")
                || !error.matches(".*' to BOOL$")) {
            return false;
        }
        return hasInvalidStringBooleanPredicateShape(pair.singleQuery)
                && hasInvalidStringBooleanPredicateShape(pair.splitQuery);
    }

    private boolean hasInvalidStringBooleanPredicateShape(String query) {
        // A string literal used directly as the left operand of AND is an
        // invalid predicate. Require the same literal/NULL shape in both
        // plans so this filter cannot hide a reconstruction-dependent cast.
        return query.matches("(?s).*\\(\\s*'[^']+'\\s*\\)\\s*AND\\s*\\(?.*\\bNULL\\b.*");
    }

    @SuppressWarnings("unused")
    private boolean isPlanIndependentInvalidUuidBooleanPredicate(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        if (!single.succeeded || split.succeeded) {
            return false;
        }
        String error = normalizeDuckDBQueryError(split.error);
        if (!"Conversion Error: Unimplemented type for cast (UUID -> BOOLEAN)".equals(error)) {
            return false;
        }
        return hasInvalidUuidBooleanPredicateShape(pair.singleQuery)
                && hasInvalidUuidBooleanPredicateShape(pair.splitQuery);
    }

    private boolean hasInvalidUuidBooleanPredicateShape(String query) {
        String upper = query.toUpperCase(Locale.ROOT);
        return upper.matches("(?s).*UUID\\s*'[^']*'\\s*AND\\s*\\(?\\s*NULL\\s*\\)?.*");
    }

    @SuppressWarnings("unused")
    private boolean isPlanIndependentInvalidMixedTypeInPredicate(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        if (!single.succeeded || split.succeeded) {
            return false;
        }
        String error = normalizeDuckDBQueryError(split.error);
        boolean decimalToDate = error != null
                && error.startsWith("Conversion Error: Unimplemented type for cast (DECIMAL(")
                && error.endsWith(" -> DATE)");
        boolean hugeintToDate = "Conversion Error: Unimplemented type for cast (HUGEINT -> DATE)".equals(error);
        if (!decimalToDate && !hugeintToDate) {
            return false;
        }
        return hasInvalidMixedTypeInPredicateShape(pair.singleQuery)
                && hasInvalidMixedTypeInPredicateShape(pair.splitQuery);
    }

    private boolean hasInvalidMixedTypeInPredicateShape(String query) {
        String upper = query.toUpperCase(Locale.ROOT);
        return upper.contains(" IN (") && upper.contains("DATE '") && upper.matches("(?s).*\\bIN\\s*\\(.*DATE\\s*'.*");
    }

    @SuppressWarnings("unused")
    private boolean isPlanIndependentInvalidMixedTypeBetweenPredicate(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        if (!single.succeeded || split.succeeded) {
            return false;
        }
        String error = normalizeDuckDBQueryError(split.error);
        if (error == null || !error.startsWith("Conversion Error: Could not convert string '")
                || !error.endsWith(" to INT128")) {
            return false;
        }
        String source = pair.singleQuery.toUpperCase(Locale.ROOT);
        String rewritten = pair.splitQuery.toUpperCase(Locale.ROOT);
        return source.contains(" BETWEEN '") && rewritten.contains(" BETWEEN '") && source.contains(" AND ")
                && rewritten.contains(" AND ");
    }

    private QueryExecution explainQuery(String query) {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery("EXPLAIN " + query)) {
            return new QueryExecution(null, null, true);
        } catch (Throwable e) {
            return new QueryExecution(null, getErrorText(e), false);
        }
    }

    private static final class QueryExecution {
        private final QueryResultMultiset result;
        private final String error;
        private final boolean succeeded;

        private QueryExecution(QueryResultMultiset result, String error, boolean succeeded) {
            this.result = result;
            this.error = error;
            this.succeeded = succeeded;
        }
    }

    private QueryExecution executePairQuery(VpQueryPair pair, String query, boolean splitSide,
            OptimizerMode optimizerMode) {
        if (state.getDbmsSpecificOptions().verticalPartitioningDirectedData) {
            synchronized (VP_NATIVE_EXECUTION_LOCK) {
                return executePairQueryUnlocked(pair, query, splitSide, optimizerMode);
            }
        }
        return executePairQueryUnlocked(pair, query, splitSide, optimizerMode);
    }

    private QueryExecution executePairQueryUnlocked(VpQueryPair pair, String query, boolean splitSide,
            OptimizerMode optimizerMode) {
        try {
            writeNativeCrashContext(pair, query, splitSide, optimizerMode);
            setPairExecutionSettings(splitSide, optimizerMode);
            return new QueryExecution(getResultMultisetRaw(query), null, true);
        } catch (Throwable e) {
            return new QueryExecution(null, getErrorText(e), false);
        } finally {
            resetPairExecutionSettings();
        }
    }

    private void writeNativeCrashContext(VpQueryPair pair, String query, boolean splitSide,
            OptimizerMode optimizerMode) {
        if (!shouldWriteNativeCrashContext()) {
            return;
        }
        File dir = new File(Main.LOG_DIRECTORY, "duckdb");
        File target = new File(dir, "native-crash-context-" + sanitizeIdentifier(state.getDatabaseName()) + ".log");
        File temp = new File(dir, target.getName() + ".tmp");
        String snapshotPath = saveNativeCrashSnapshot(pair, query, splitSide, optimizerMode, dir);
        StringBuilder sb = new StringBuilder();
        sb.append("-- Last DuckDB VP query entered through JDBC native execution.").append(System.lineSeparator());
        sb.append("-- If the JVM terminates with SIGSEGV/SIGABRT before database.log is written,")
                .append(System.lineSeparator());
        sb.append("-- this is the best-effort in-flight query context, not a suppression record.")
                .append(System.lineSeparator());
        sb.append("database=").append(state.getDatabaseName()).append(System.lineSeparator());
        sb.append("javaThread=").append(Thread.currentThread().getName()).append(System.lineSeparator());
        sb.append("timestampMillis=").append(System.currentTimeMillis()).append(System.lineSeparator());
        sb.append("side=").append(splitSide ? "split" : "source").append(System.lineSeparator());
        sb.append("optimizerMode=").append(optimizerMode).append(System.lineSeparator());
        sb.append("disabledOptimizers='").append(splitSide ? optimizerMode.disabledOptimizers : "").append("'")
                .append(System.lineSeparator());
        sb.append("duckdbThreads=")
                .append(splitSide ? Math.max(1, state.getDbmsSpecificOptions().verticalPartitioningSplitThreads) : 1)
                .append(System.lineSeparator());
        sb.append("directedData=").append(state.getDbmsSpecificOptions().verticalPartitioningDirectedData)
                .append(System.lineSeparator());
        sb.append("directedSourceRows=").append(state.getDbmsSpecificOptions().verticalPartitioningDirectedSourceRows)
                .append(System.lineSeparator());
        sb.append("schemaPrefix=").append(pair.schema.prefix).append(System.lineSeparator());
        sb.append("baseTable=").append(pair.schema.baseTable).append(System.lineSeparator());
        sb.append("sourceTable=").append(pair.schema.sourceTable).append(System.lineSeparator());
        sb.append("leftTable=").append(pair.schema.leftTable).append(System.lineSeparator());
        sb.append("rightTable=").append(pair.schema.rightTable).append(System.lineSeparator());
        sb.append("splitStrategy=").append(pair.schema.splitStrategy).append(System.lineSeparator());
        sb.append("reconstructionStrategy=").append(pair.reconstructionStrategy).append(System.lineSeparator());
        sb.append("description=").append(pair.description).append(System.lineSeparator());
        sb.append("orderedComparison=").append(pair.orderedComparisonEnabled).append(System.lineSeparator());
        sb.append("baseRowCount=").append(pair.schema.baseRowCount).append(System.lineSeparator());
        sb.append("sourceEngine=").append(pair.schema.sourceEngine).append(System.lineSeparator());
        sb.append("leftEngine=").append(pair.schema.leftEngine).append(System.lineSeparator());
        sb.append("rightEngine=").append(pair.schema.rightEngine).append(System.lineSeparator());
        sb.append("snapshotPath=").append(snapshotPath).append(System.lineSeparator());
        sb.append(System.lineSeparator()).append("-- query").append(System.lineSeparator());
        sb.append(query).append(';').append(System.lineSeparator());
        synchronized (NATIVE_CRASH_CONTEXT_LOCK) {
            try {
                Files.createDirectories(dir.toPath());
                Files.writeString(temp.toPath(), sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
                try {
                    Files.move(temp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException atomicMoveUnsupported) {
                    Files.move(temp.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException ignored) {
                // This file is diagnostic only. Never let sidecar logging hide the query
                // result or the native failure we are trying to preserve.
            }
        }
    }

    private String saveNativeCrashSnapshot(VpQueryPair pair, String query, boolean splitSide,
            OptimizerMode optimizerMode, File logDir) {
        File snapshotDir = new File(logDir, "native-crash-snapshot-" + sanitizeIdentifier(state.getDatabaseName()));
        File marker = new File(snapshotDir, "SQLANCER_DUCKDB_NATIVE_CRASH_SNAPSHOT");
        synchronized (NATIVE_CRASH_CONTEXT_LOCK) {
            try {
                Files.createDirectories(logDir.toPath());
                deletePreviousNativeCrashSnapshot(snapshotDir.toPath(), marker.toPath());
                Files.createDirectories(snapshotDir.toPath());
                Files.writeString(marker.toPath(),
                        "Best-effort DuckDB EXPORT DATABASE snapshot captured before VP native execution."
                                + System.lineSeparator(),
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
                copyTableToParquetIfExists(pair.schema.sourceTable, new File(snapshotDir, "source.parquet"));
                copyTableToParquetIfExists(pair.schema.leftTable, new File(snapshotDir, "left.parquet"));
                copyTableToParquetIfExists(pair.schema.rightTable, new File(snapshotDir, "right.parquet"));
                Files.writeString(new File(snapshotDir, "repro.sql").toPath(),
                        nativeCrashReproSql(pair, query, splitSide, optimizerMode, snapshotDir), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                return snapshotDir.getAbsolutePath();
            } catch (Throwable e) {
                return "<unavailable: " + getErrorText(e) + ">";
            }
        }
    }

    private void copyTableToParquetIfExists(String tableName, File parquetFile) throws SQLException {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute("COPY (SELECT * FROM " + quote(tableName) + " ORDER BY " + quote(ROW_ID) + ") TO '"
                    + escapeLiteral(parquetFile.getAbsolutePath()) + "' (FORMAT PARQUET)");
        }
    }

    private String nativeCrashReproSql(VpQueryPair pair, String query, boolean splitSide, OptimizerMode optimizerMode,
            File snapshotDir) {
        StringBuilder sb = new StringBuilder();
        sb.append("-- SQLancer DuckDB VP native-crash repro snapshot").append(System.lineSeparator());
        sb.append("-- database=").append(state.getDatabaseName()).append(System.lineSeparator());
        sb.append("-- side=").append(splitSide ? "split" : "source").append(System.lineSeparator());
        sb.append("-- optimizerMode=").append(optimizerMode).append(System.lineSeparator());
        sb.append("SET threads TO ")
                .append(splitSide ? Math.max(1, state.getDbmsSpecificOptions().verticalPartitioningSplitThreads) : 1)
                .append(';').append(System.lineSeparator());
        sb.append("SET disabled_optimizers = '").append(splitSide ? optimizerMode.disabledOptimizers : "").append("';")
                .append(System.lineSeparator());
        sb.append("INSTALL fts;").append(System.lineSeparator());
        sb.append("LOAD fts;").append(System.lineSeparator());
        sb.append("INSTALL spatial;").append(System.lineSeparator());
        sb.append("LOAD spatial;").append(System.lineSeparator());
        sb.append("CREATE OR REPLACE TABLE ").append(quote(pair.schema.sourceTable))
                .append(" AS SELECT * FROM read_parquet('")
                .append(escapeLiteral(new File(snapshotDir, "source.parquet").getAbsolutePath())).append("');")
                .append(System.lineSeparator());
        sb.append("CREATE OR REPLACE TABLE ").append(quote(pair.schema.leftTable))
                .append(" AS SELECT * FROM read_parquet('")
                .append(escapeLiteral(new File(snapshotDir, "left.parquet").getAbsolutePath())).append("');")
                .append(System.lineSeparator());
        sb.append("CREATE OR REPLACE TABLE ").append(quote(pair.schema.rightTable))
                .append(" AS SELECT * FROM read_parquet('")
                .append(escapeLiteral(new File(snapshotDir, "right.parquet").getAbsolutePath())).append("');")
                .append(System.lineSeparator());
        for (String setup : pair.extraSetupStatements) {
            sb.append(setup).append(';').append(System.lineSeparator());
        }
        sb.append(query).append(';').append(System.lineSeparator());
        return sb.toString();
    }

    @SuppressWarnings("unused")
    private String saveNativeCrashFullDatabaseSnapshot(File logDir) {
        File snapshotDir = new File(logDir,
                "native-crash-full-snapshot-" + sanitizeIdentifier(state.getDatabaseName()));
        File marker = new File(snapshotDir, "SQLANCER_DUCKDB_NATIVE_CRASH_SNAPSHOT");
        synchronized (NATIVE_CRASH_CONTEXT_LOCK) {
            try {
                Files.createDirectories(logDir.toPath());
                deletePreviousNativeCrashSnapshot(snapshotDir.toPath(), marker.toPath());
                Files.createDirectories(snapshotDir.toPath());
                Files.writeString(marker.toPath(),
                        "Best-effort DuckDB EXPORT DATABASE snapshot captured before VP native execution."
                                + System.lineSeparator(),
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
                try (Statement statement = state.getConnection().createStatement()) {
                    statement.execute(
                            "EXPORT DATABASE '" + escapeLiteral(snapshotDir.getAbsolutePath()) + "' (FORMAT PARQUET)");
                }
                return snapshotDir.getAbsolutePath();
            } catch (Throwable e) {
                return "<unavailable: " + getErrorText(e) + ">";
            }
        }
    }

    private void deletePreviousNativeCrashSnapshot(Path snapshotDir, Path marker) throws IOException {
        if (!Files.exists(snapshotDir)) {
            return;
        }
        if (!Files.exists(marker)) {
            throw new IOException("refusing to replace unmarked snapshot directory: " + snapshotDir);
        }
        try (Stream<Path> paths = Files.walk(snapshotDir)) {
            List<Path> toDelete = paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
            for (Path path : toDelete) {
                Files.deleteIfExists(path);
            }
        }
    }

    private boolean shouldWriteNativeCrashContext() {
        DuckDBOptions options = state.getDbmsSpecificOptions();
        return options.verticalPartitioningDirectedData || options.verticalPartitioningDeepExploration
                || options.verticalPartitioningAdvancedQueryFeatures || options.verticalPartitioningParquetRoundTrip
                || options.verticalPartitioningStorageBoundaryTests;
    }

    private QueryResultMultiset getResultMultisetRaw(String query) throws SQLException {
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(query);
        }
        // ExpectedErrors must be empty here. Otherwise SQLancer converts a query error to a
        // null result before this oracle can determine whether the other side succeeded.
        SQLQueryAdapter adapter = new SQLQueryAdapter(query, new ExpectedErrors(), false,
                state.getOptions().canonicalizeSqlString());
        Map<String, Integer> rows = new LinkedHashMap<>();
        Map<String, List<ApproximateKind>> approximateColumns = new LinkedHashMap<>();
        List<String> orderedRows = new ArrayList<>();
        List<List<ApproximateKind>> orderedApproximateColumns = new ArrayList<>();
        SQLancerResultSet rs = null;
        Main.nrQueries.addAndGet(1);
        try {
            rs = adapter.executeAndGet(state);
            if (rs == null) {
                throw new SQLException("DuckDB returned no result set");
            }
            int columnCount = rs.getColumnCount();
            List<String> columnTypes = getColumnTypes(rs, columnCount);
            while (rs.next()) {
                String row = canonicalizeRow(rs, columnCount);
                List<ApproximateKind> approximate = getApproximateColumns(rs, columnCount);
                orderedRows.add(row);
                orderedApproximateColumns.add(approximate);
                rows.put(row, rows.getOrDefault(row, 0) + 1);
                approximateColumns.putIfAbsent(row, approximate);
            }
            return new QueryResultMultiset(rows, approximateColumns, orderedRows, orderedApproximateColumns,
                    columnTypes);
        } finally {
            if (rs != null && !rs.isClosed()) {
                rs.close();
            }
        }
    }

    private void setPairExecutionSettings(boolean splitSide, OptimizerMode optimizerMode) throws SQLException {
        try (Statement statement = state.getConnection().createStatement()) {
            if (splitSide) {
                int splitThreads = Math.max(1, state.getDbmsSpecificOptions().verticalPartitioningSplitThreads);
                statement.execute("SET threads TO " + splitThreads);
                statement.execute("SET disabled_optimizers = '" + optimizerMode.disabledOptimizers + "'");
            } else {
                statement.execute("SET threads TO 1");
                statement.execute("SET disabled_optimizers = ''");
            }
        }
    }

    private OptimizerMode nextOptimizerMode() {
        List<OptimizerMode> modes = configuredOptimizerModes();
        return modes.get((int) Math.floorMod(OPTIMIZER_MODE_CURSOR.getAndIncrement(), modes.size()));
    }

    private List<OptimizerMode> configuredOptimizerModes() {
        String configured = state.getDbmsSpecificOptions().verticalPartitioningOptimizerModes;
        if (configured == null || configured.trim().isEmpty() || configured.equalsIgnoreCase("all")) {
            return List.of(OptimizerMode.DEFAULT, OptimizerMode.DISABLED_BOTH, OptimizerMode.DISABLED_FILTER,
                    OptimizerMode.DISABLED_JOIN);
        }
        List<OptimizerMode> modes = new ArrayList<>();
        for (String token : configured.split(",")) {
            switch (token.trim().toLowerCase(Locale.ROOT)) {
            case "default":
                modes.add(OptimizerMode.DEFAULT);
                break;
            case "disabled":
            case "disabled_both":
                modes.add(OptimizerMode.DISABLED_BOTH);
                break;
            case "bare_filter":
            case "disabled_filter":
                modes.add(OptimizerMode.DISABLED_FILTER);
                break;
            case "bare_join":
            case "disabled_join":
                modes.add(OptimizerMode.DISABLED_JOIN);
                break;
            default:
                break;
            }
        }
        return modes.isEmpty() ? List.of(OptimizerMode.DEFAULT) : modes;
    }

    private static void recordOptimizerMode(OptimizerMode mode) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            increment(OPTIMIZER_MODE_COUNTS, mode);
        }
        maybePersistVpStats();
    }

    private void resetPairExecutionSettings() {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute("SET threads TO 1");
            statement.execute("SET disabled_optimizers = ''");
        } catch (SQLException ignored) {
            // The query error is the useful signal; cleanup is best effort.
        }
    }

    private boolean bothAreSkippableDuckDBErrors(String left, String right) {
        return isSkippableQueryErrorText(normalizeDuckDBQueryError(left))
                && isSkippableQueryErrorText(normalizeDuckDBQueryError(right));
    }

    private boolean bothFailedWithSameDuckDBError(String left, String right) {
        String normalizedLeft = normalizeDuckDBQueryError(left);
        String normalizedRight = normalizeDuckDBQueryError(right);
        return normalizedLeft != null && normalizedLeft.equals(normalizedRight);
    }

    private String normalizeDuckDBQueryError(String error) {
        if (error == null || error.isEmpty()) {
            return null;
        }
        String normalized = error.replace("\r\n", "\n");
        // JDBC wraps DuckDB assertion failures with a second exception and a native
        // stack trace. The first INTERNAL Error line is the stable diagnostic; the
        // addresses and stack frames are execution artifacts and must not create a
        // VP difference when both plans fail for the same reason.
        int internalError = normalized.indexOf("INTERNAL Error:");
        if (internalError >= 0) {
            int end = normalized.indexOf('\n', internalError);
            return normalized.substring(internalError, end >= 0 ? end : normalized.length()).trim();
        }
        String[] markers = { "Parser Error:", "Binder Error:", "Conversion Error:", "Catalog Error:",
                "Not implemented Error:", "Invalid Input Error:", "Out of Range Error:", "Invalid Error:" };
        int markerStart = -1;
        for (String marker : markers) {
            int candidate = normalized.indexOf(marker);
            while (candidate >= 0 && candidate > 0 && normalized.charAt(candidate - 1) != '\n') {
                candidate = normalized.indexOf(marker, candidate + marker.length());
            }
            if (candidate >= 0) {
                markerStart = candidate;
                break;
            }
        }
        if (markerStart >= 0) {
            normalized = normalized.substring(markerStart);
        }
        int firstLine = normalized.indexOf('\n');
        if (firstLine >= 0) {
            normalized = normalized.substring(0, firstLine);
        }
        return normalized.trim();
    }

    private void recordExecutedAdvancedFeatures(VpQueryPair pair) {
        if (usesNativeRollup(pair.singleQuery) && usesNativeRollup(pair.splitQuery)) {
            recordAdvancedFeature(AdvancedFeature.ROLLUP);
        }
    }

    private boolean usesNativeRollup(String query) {
        return query.toUpperCase(Locale.ROOT).contains("GROUP BY ROLLUP (");
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

    private boolean suppressKnownExecutionBug(VpQueryPair pair, KnownVpBug knownBug, QueryExecution single,
            QueryExecution split) throws SQLException {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns || knownBug == null) {
            return false;
        }
        logKnownExecutionBug(pair, knownBug, single, split);
        recordKnownBugSuppressed(knownBug);
        return true;
    }

    private KnownVpBug classifyKnownExecutionBug(VpQueryPair pair, QueryExecution single, QueryExecution split) {
        if (isKnownEmptyFilterBindingExecutionBug(pair, single, split)) {
            return KnownVpBug.DUCKDB_NATIVE_EMPTY_FILTER_BINDING_DEEP_VP_EXECUTION;
        }
        if (!deepExplorationEnabled() || !isKnownBasicStringNullVersion() || !single.succeeded || split.succeeded
                || !isKnownBasicStringNullError(split.error)) {
            return null;
        }
        String source = pair.singleQuery.toUpperCase(Locale.ROOT);
        String rewritten = pair.splitQuery.toUpperCase(Locale.ROOT);
        // This is only a known regression in the versions where it was observed,
        // and only for a deep reconstructed-relation LIMIT 0 shape. The failure
        // reaches the same native path through UNION, anti-join, LATERAL, IN,
        // or a correlated scalar subquery.
        if (!source.matches("(?s).*\\bLIMIT\\s+0\\b.*") || !rewritten.matches("(?s).*\\bLIMIT\\s+0\\b.*")
                || !rewritten.contains("FROM (") || !hasKnownBasicStringNullReconstructionShape(rewritten)) {
            return null;
        }
        return KnownVpBug.DUCKDB_NATIVE_BASIC_STRING_NULL_DEEP_VP_EXECUTION;
    }

    /**
     * These patterns are filtered before either side is sent through the JDBC native bridge. They are confirmed fatal
     * on DuckDB 1.5.5.x: unlike an ordinary expected SQL error, allowing one occurrence to execute can terminate the
     * whole SQLancer process. Keep the predicates deliberately structural and narrow so LIMIT 0, window functions,
     * joins, and scalar subqueries remain available in all other combinations.
     */
    private KnownVpBug classifyKnownNativeCrashGenerationPattern(VpQueryPair pair, OptimizerMode optimizerMode) {
        if (!isKnownBasicStringNullVersion()) {
            return null;
        }
        String split = pair.splitQuery.toUpperCase(Locale.ROOT);
        if (!split.contains(" FROM (")) {
            return null;
        }
        int splitThreads = Math.max(1, state.getDbmsSpecificOptions().verticalPartitioningSplitThreads);
        long sourceRows = materializedSourceRowCount(pair.schema);
        if (pair.reconstructionStrategy == ReconstructionStrategy.EXISTS_FILTER
                && isKnownExistsScalarRowNumberCrashShape(split, optimizerMode.disabledOptimizers, splitThreads,
                        sourceRows)) {
            return KnownVpBug.DUCKDB_NATIVE_LIMIT0_VP_CRASH;
        }
        String source = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!hasLimitZeroOrderedQuery(source) || !hasLimitZeroOrderedQuery(split)) {
            return null;
        }
        if (isKnownLimitZeroVpCrashShape(split)) {
            return KnownVpBug.DUCKDB_NATIVE_LIMIT0_VP_CRASH;
        }
        if (pair.reconstructionStrategy == ReconstructionStrategy.SCALAR_SUBQUERY
                && isKnownScalarSubqueryLimitZeroCrashShape(split, optimizerMode.disabledOptimizers, splitThreads,
                        sourceRows)) {
            return KnownVpBug.DUCKDB_NATIVE_LIMIT0_VP_CRASH;
        }
        if (pair.reconstructionStrategy == ReconstructionStrategy.CTE
                && isKnownCteInnerJoinLimitZeroCrashShape(split, optimizerMode.disabledOptimizers, splitThreads,
                        sourceRows)) {
            return KnownVpBug.DUCKDB_NATIVE_LIMIT0_VP_CRASH;
        }
        return null;
    }

    private boolean hasLimitZeroOrderedQuery(String query) {
        return query.matches("(?s).*\\bORDER\\s+BY\\b.*\\bLIMIT\\s+0\\b.*");
    }

    private boolean isKnownLimitZeroVpCrashShape(String split) {
        // The three confirmed reports share a reconstructed VP relation and
        // the LIMIT 0 native execution path, but differ in the inner plan:
        // LEFT_DELIM/MARK JOIN (#24794), correlated scalar lookups (#25601),
        // or rank/aggregate reconstruction (#25663). Match these narrowly
        // while leaving ordinary LIMIT 0/window/join queries available.
        if (isKnownCorrelatedLimitZeroCrashShape(split) || isKnownRankAggregateLimitZeroCrashShape(split)) {
            return true;
        }
        // #24794: NOT EXISTS over the reconstructed left/right join, which
        // DuckDB lowers to LEFT_DELIM_JOIN/MARK JOIN. Do not match a generic
        // NOT EXISTS query unless it is visibly a VP reconstruction.
        return split.contains("NOT EXISTS") && split.contains("JOIN") && split.contains("VP_ROWID")
                && (split.contains("LEFT_DELIM_JOIN") || split.contains("MARK JOIN")
                        || split.matches("(?s).*NOT\\s+EXISTS.*VP_ROWID.*VP_ROWID.*"));
    }

    private boolean isKnownCorrelatedLimitZeroCrashShape(String split) {
        // duckdb811/0ducnot.log: row-id IN subquery plus repeated correlated
        // scalar lookups in the reconstructed relation.
        int scalarSubqueries = countOccurrences(split, "(SELECT");
        return scalarSubqueries >= 3 && split.matches("(?s).*\\bIN\\s*\\(\\s*SELECT\\b.*") && split.contains("FROM")
                && split.contains("WHERE") && split.contains("VP_ROWID");
    }

    private boolean isKnownRankAggregateLimitZeroCrashShape(String split) {
        // duckdb812/0bug.log: RANK window over a reconstructed relation whose
        // right partition is grouped by row id and joined back.
        return split.contains("RANK() OVER") && split.contains("COUNT(*)")
                && split.matches("(?s).*\\bGROUP\\s+BY\\s+[^)]*VP_ROWID.*") && countWord(split, "JOIN") >= 2
                && split.matches("(?s).*\\bON\\b.*VP_ROWID.*");
    }

    static boolean isKnownCteInnerJoinLimitZeroCrashShape(String split, String disabledOptimizers, int splitThreads,
            long sourceRows) {
        // The failure is repetition-sensitive, but the native state is shared by
        // earlier queries. Waiting for a local occurrence count is not safe, so
        // suppress the first matching generated pair.
        if (split == null || disabledOptimizers == null || splitThreads != 1) {
            return false;
        }
        String normalized = split.toUpperCase(Locale.ROOT);
        boolean cteJoin = normalized.contains("FROM (WITH L AS (SELECT * FROM ")
                && normalized.contains("), R AS (SELECT * FROM ")
                && normalized.matches("(?s).*\\bFROM\\s+L\\s+(?:INNER\\s+)?JOIN\\s+R\\s+ON\\s+.*")
                && normalized.matches("(?s).*\\bL\\.\"?VP_ROWID\"?\\s*=\\s*R\\.\"?VP_ROWID\"?.*");
        boolean rowGroupMode = sourceRows >= LIMIT0_CTE_CRASH_MIN_ROWS
                && VP_SPLIT_DISABLED_OPTIMIZERS.equals(disabledOptimizers);
        boolean smallObservedMode = sourceRows >= LIMIT0_CTE_SMALL_CRASH_MIN_ROWS
                && sourceRows < LIMIT0_CTE_CRASH_MIN_ROWS && "filter_pushdown".equals(disabledOptimizers);
        return cteJoin && (rowGroupMode || smallObservedMode);
    }

    static boolean isKnownScalarSubqueryLimitZeroCrashShape(String split, String disabledOptimizers, int splitThreads,
            long sourceRows) {
        if (split == null || disabledOptimizers == null || splitThreads != 1 || sourceRows < LIMIT0_CTE_CRASH_MIN_ROWS
                || !(disabledOptimizers.equals("filter_pushdown")
                        || disabledOptimizers.equals(VP_SPLIT_DISABLED_OPTIMIZERS))) {
            return false;
        }
        String normalized = split.toUpperCase(Locale.ROOT);
        return countOccurrences(normalized, "(SELECT R.") >= 2
                && normalized.matches("(?s).*\\(SELECT\\s+R\\..*\\bFROM\\b.*\\sR\\s+WHERE\\s+R\\.\"?VP_ROWID\"?"
                        + "\\s*=\\s*L\\.\"?VP_ROWID\"?.*");
    }

    static boolean isKnownExistsScalarRowNumberCrashShape(String split, String disabledOptimizers, int splitThreads,
            long sourceRows) {
        if (split == null || disabledOptimizers == null || splitThreads != 1 || sourceRows < 4096
                || sourceRows >= LIMIT0_CTE_CRASH_MIN_ROWS
                || !(disabledOptimizers.equals("filter_pushdown")
                        || disabledOptimizers.equals(VP_SPLIT_DISABLED_OPTIMIZERS))) {
            return false;
        }
        String normalized = split.toUpperCase(Locale.ROOT);
        return normalized.contains("ROW_NUMBER() OVER")
                && normalized.matches("(?s).*\\bWHERE\\s+EXISTS\\s*\\(\\s*SELECT\\b.*")
                && countOccurrences(normalized, "(SELECT R.") >= 4
                && normalized.matches("(?s).*\\(SELECT\\s+R\\..*\\bFROM\\b.*\\sR\\s+WHERE\\s+R\\.\"?VP_ROWID\"?"
                        + "\\s*=\\s*L\\.\"?VP_ROWID\"?.*");
    }

    private static int countOccurrences(String text, String needle) {
        int count = 0;
        int from = 0;
        while ((from = text.indexOf(needle, from)) >= 0) {
            count++;
            from += needle.length();
        }
        return count;
    }

    private static int countWord(String text, String word) {
        Matcher matcher = Pattern.compile("\\b" + Pattern.quote(word) + "\\b").matcher(text);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    private boolean hasKnownBasicStringNullReconstructionShape(String rewrittenQuery) {
        boolean unionReconstruction = rewrittenQuery.contains("UNION ALL");
        boolean antiJoinReconstruction = rewrittenQuery.contains("NOT EXISTS") && rewrittenQuery.contains("JOIN");
        boolean lateralReconstruction = rewrittenQuery.contains("JOIN LATERAL");
        boolean crossJoinReconstruction = rewrittenQuery.contains("CROSS JOIN");
        // The same v1.5.5 native failure is also reachable through the
        // ordinary LEFT/INNER JOIN reconstructions used by EVEN_SPLIT and
        // DERIVED_TABLE. Keep this guarded by the exact known error, version,
        // deep LIMIT 0 shape, and an actual reconstructed relation.
        boolean ordinaryJoinReconstruction = rewrittenQuery.matches("(?s).*\\bJOIN\\b.*")
                && rewrittenQuery.contains("ON ");
        boolean inFilterReconstruction = rewrittenQuery.matches("(?s).*\\bIN\\s*\\(.*");
        boolean correlatedScalarReconstruction = rewrittenQuery.matches("(?s).*\\(SELECT\\s+.*")
                && rewrittenQuery.contains("WHERE");
        return unionReconstruction || antiJoinReconstruction || lateralReconstruction || inFilterReconstruction
                || crossJoinReconstruction || ordinaryJoinReconstruction || correlatedScalarReconstruction;
    }

    private boolean isKnownBasicStringNullVersion() {
        try {
            String version = state.getConnection().getDatabaseVersion();
            return "v1.5.4".equals(version) || "v1.5.5".equals(version);
        } catch (Exception ignored) {
            return false;
        }
    }

    private boolean isKnownEmptyFilterBindingExecutionBug(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        if (!deepExplorationEnabled() || !single.succeeded || split.succeeded || !isKnownDuckDB154()
                || !isKnownEmptyFilterBindingError(split.error)) {
            return false;
        }
        String source = pair.singleQuery.toUpperCase(Locale.ROOT);
        String rewritten = pair.splitQuery.toUpperCase(Locale.ROOT);
        return hasEmptyFilterBindingShape(source, false) && hasEmptyFilterBindingShape(rewritten, true);
    }

    private boolean hasEmptyFilterBindingShape(String query, boolean requireJoin) {
        int firstFilteredCount = query.indexOf("COUNT(*) FILTER");
        int secondFilteredCount = firstFilteredCount < 0 ? -1 : query.indexOf("COUNT(", firstFilteredCount + 6);
        return query.matches("(?s).*\\b(?:VP_)?EMPTY_COUNT\\b.*") && firstFilteredCount >= 0 && secondFilteredCount >= 0
                && query.contains("WHERE FALSE") && (!requireJoin || query.contains("JOIN"));
    }

    private boolean isKnownDuckDB154() {
        try {
            return "v1.5.4".equals(state.getConnection().getDatabaseVersion());
        } catch (Exception ignored) {
            return false;
        }
    }

    private boolean isKnownBasicStringNullError(String error) {
        return "Invalid Error: basic_string: construction from null is not valid"
                .equals(normalizeDuckDBQueryError(error));
    }

    private boolean isKnownEmptyFilterBindingError(String error) {
        String normalized = normalizeDuckDBQueryError(error);
        return normalized != null && normalized.matches(
                "INTERNAL Error: Failed to bind column reference \"(?:vp_)?empty_count\" \\[17\\.4\\]: inequal types \\(BIGINT != HUGEINT\\)");
    }

    private KnownVpBug classifyKnownBug(VpQueryPair pair, QueryResultMultiset singleRows, QueryResultMultiset splitRows)
            throws SQLException {
        if (isParquetIntervalMillisecondPrecisionDifference(pair, singleRows, splitRows)) {
            return KnownVpBug.DUCKDB_PARQUET_INTERVAL_MILLISECOND_PRECISION;
        }
        if (isParquetIntervalUint32Overflow(pair, singleRows, splitRows)) {
            return KnownVpBug.DUCKDB_PARQUET_INTERVAL_UINT32_MILLISECONDS_OVERFLOW;
        }
        if (isGroupByAllNoCaseMismatch(pair, singleRows, splitRows)) {
            return KnownVpBug.DUCKDB_GROUP_BY_ALL_NOCASE_COLLATION_MISMATCH;
        }
        if (isHistogramNonFiniteOrderMismatch(pair, singleRows, splitRows)) {
            return KnownVpBug.DUCKDB_HISTOGRAM_NONFINITE_ORDER_DEPENDENCE;
        }
        return null;
    }

    private boolean isParquetIntervalMillisecondPrecisionDifference(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!isParquetPair(pair) || !hasIntervalColumn(pair.schema)) {
            return false;
        }
        Map<String, List<String>> source = decodedRowsByRowId(singleRows);
        Map<String, List<String>> split = decodedRowsByRowId(splitRows);
        if (source == null || split == null || !source.keySet().equals(split.keySet())) {
            return false;
        }
        boolean foundDifference = false;
        for (Map.Entry<String, List<String>> entry : source.entrySet()) {
            List<String> left = entry.getValue();
            List<String> right = split.get(entry.getKey());
            if (left.size() != right.size() || left.size() != pair.schema.columns.size() + 1) {
                return false;
            }
            for (int i = 1; i < left.size(); i++) {
                if (Objects.equals(left.get(i), right.get(i))) {
                    continue;
                }
                if (pair.schema.columns.get(i - 1).type.getPrimitiveDataType() != DuckDBDataType.INTERVAL
                        || left.get(i) == null || right.get(i) == null
                        || !intervalDifferenceLessThanOneMillisecond(left.get(i), right.get(i))) {
                    return false;
                }
                foundDifference = true;
            }
        }
        return foundDifference;
    }

    private boolean isParquetIntervalUint32Overflow(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!isParquetPair(pair) || !hasIntervalColumn(pair.schema)) {
            return false;
        }
        Map<String, List<String>> source = decodedRowsByRowId(singleRows);
        Map<String, List<String>> split = decodedRowsByRowId(splitRows);
        if (source == null || split == null || !source.keySet().equals(split.keySet())) {
            return false;
        }
        boolean foundDifference = false;
        for (Map.Entry<String, List<String>> entry : source.entrySet()) {
            List<String> left = entry.getValue();
            List<String> right = split.get(entry.getKey());
            if (left.size() != right.size() || left.size() != pair.schema.columns.size() + 1) {
                return false;
            }
            for (int i = 1; i < left.size(); i++) {
                if (Objects.equals(left.get(i), right.get(i))) {
                    continue;
                }
                if (pair.schema.columns.get(i - 1).type.getPrimitiveDataType() != DuckDBDataType.INTERVAL
                        || left.get(i) == null || right.get(i) == null
                        || (!intervalDifferenceLessThanOneMillisecond(left.get(i), right.get(i))
                                && !matchesParquetIntervalUint32Milliseconds(left.get(i), right.get(i)))) {
                    return false;
                }
                foundDifference = true;
            }
        }
        return foundDifference;
    }

    private boolean isParquetPair(VpQueryPair pair) {
        return pair.description.contains("parquet=PARQUET_RIGHT_PARTITION_RECONSTRUCTION");
    }

    private boolean hasIntervalColumn(VpSchema schema) {
        return schema.columns.stream()
                .anyMatch(column -> column.type.getPrimitiveDataType() == DuckDBDataType.INTERVAL);
    }

    private Map<String, List<String>> decodedRowsByRowId(QueryResultMultiset result) {
        Map<String, List<String>> decoded = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : result.rows.entrySet()) {
            if (entry.getValue() != 1) {
                return null;
            }
            List<String> values = decodeCanonicalRow(entry.getKey());
            if (values.isEmpty() || values.get(0) == null || decoded.put(values.get(0), values) != null) {
                return null;
            }
        }
        return decoded;
    }

    private boolean intervalDifferenceLessThanOneMillisecond(String left, String right) {
        BigInteger leftMicros = parseIntervalMicros(left);
        BigInteger rightMicros = parseIntervalMicros(right);
        return leftMicros != null && rightMicros != null
                && leftMicros.subtract(rightMicros).abs().compareTo(BigInteger.valueOf(1000)) < 0;
    }

    private boolean matchesParquetIntervalUint32Milliseconds(String source, String parquet) {
        BigInteger sourceMicros = parseIntervalMicros(source);
        BigInteger parquetMicros = parseIntervalMicros(parquet);
        if (sourceMicros == null || parquetMicros == null
                || !sourceMicros.remainder(BigInteger.valueOf(1000)).equals(BigInteger.ZERO)
                || !parquetMicros.remainder(BigInteger.valueOf(1000)).equals(BigInteger.ZERO)) {
            return false;
        }
        BigInteger sourceMillis = sourceMicros.divide(BigInteger.valueOf(1000));
        BigInteger parquetMillis = parquetMicros.divide(BigInteger.valueOf(1000));
        return sourceMillis.mod(PARQUET_UINT32_MODULUS).equals(parquetMillis);
    }

    private BigInteger parseIntervalMicros(String value) {
        Matcher matcher = INTERVAL_HMS_PATTERN.matcher(value);
        if (!matcher.matches()) {
            return null;
        }
        BigInteger hours = new BigInteger(matcher.group(2));
        BigInteger minutes = new BigInteger(matcher.group(3));
        BigInteger seconds = new BigInteger(matcher.group(4));
        String fraction = matcher.group(5);
        BigInteger micros = hours.multiply(BigInteger.valueOf(3600)).add(minutes.multiply(BigInteger.valueOf(60)))
                .add(seconds).multiply(BigInteger.valueOf(1_000_000));
        if (fraction != null) {
            micros = micros.add(new BigInteger((fraction + "000000").substring(0, 6)));
        }
        return "-".equals(matcher.group(1)) ? micros.negate() : micros;
    }

    /**
     * DuckDB 1.5.4's histogram map can assign NaN to a different non-finite bucket when the same rows arrive in a
     * different physical order. Keep this matcher deliberately narrow: it must be the QUANTILE_HISTOGRAM feature, a
     * FLOAT/DOUBLE source column, and the histogram column must be the only changed result value.
     */
    private boolean isHistogramNonFiniteOrderMismatch(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!pair.description.contains("friendly=QUANTILE_HISTOGRAM")) {
            return false;
        }
        String source = pair.singleQuery.toUpperCase(Locale.ROOT);
        String split = pair.splitQuery.toUpperCase(Locale.ROOT);
        if (!source.contains("HISTOGRAM(") || !split.contains("HISTOGRAM(") || !source.contains("QUANTILE(")
                || !split.contains("QUANTILE(")) {
            return false;
        }
        if (!pair.schema.columns.stream().anyMatch(column -> {
            DuckDBDataType type = column.type.getPrimitiveDataType();
            return type == DuckDBDataType.FLOAT && column.type.toSQLType().toUpperCase(Locale.ROOT)
                    .matches("(?:REAL|FLOAT|DOUBLE)(?:\\s*\\(.*\\))?");
        })) {
            return false;
        }
        if (singleRows.rows.size() != 1 || splitRows.rows.size() != 1) {
            return false;
        }
        Map.Entry<String, Integer> singleEntry = singleRows.rows.entrySet().iterator().next();
        Map.Entry<String, Integer> splitEntry = splitRows.rows.entrySet().iterator().next();
        if (!Objects.equals(singleEntry.getValue(), splitEntry.getValue())) {
            return false;
        }
        List<String> singleValues = decodeCanonicalRow(singleEntry.getKey());
        List<String> splitValues = decodeCanonicalRow(splitEntry.getKey());
        if (singleValues.size() != splitValues.size()) {
            return false;
        }
        int differingColumns = 0;
        boolean nonFiniteHistogramDifference = false;
        for (int i = 0; i < singleValues.size(); i++) {
            if (Objects.equals(singleValues.get(i), splitValues.get(i))) {
                continue;
            }
            differingColumns++;
            String left = singleValues.get(i);
            String right = splitValues.get(i);
            if (containsNonFiniteHistogramValue(left) || containsNonFiniteHistogramValue(right)) {
                nonFiniteHistogramDifference = true;
            }
        }
        if (differingColumns != 1) {
            return false;
        }
        // A non-finite input can perturb the finite histogram bucket selected
        // by DuckDB's order-dependent implementation, so the output need not
        // contain a visible NaN/Inf key. Require evidence in either the
        // materialized input or the changed histogram output; the FLOAT type
        // alone is not enough to classify an arbitrary histogram mismatch.
        return nonFiniteHistogramDifference
                || pair.extraSetupStatements.stream().anyMatch(this::containsNonFiniteHistogramInput);
    }

    @SuppressWarnings("unused")
    private boolean isPlanIndependentInvalidTimestampBooleanPredicate(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        if (!single.succeeded || split.succeeded) {
            return false;
        }
        String error = normalizeDuckDBQueryError(split.error);
        if (error == null || (!error.contains("TIMESTAMP -> BOOLEAN")
                && !error.contains("unsuccessful or closed pending query"))) {
            return false;
        }
        String source = pair.singleQuery.toUpperCase(Locale.ROOT);
        String rewritten = pair.splitQuery.toUpperCase(Locale.ROOT);
        return source.contains("TIMESTAMP '") && rewritten.contains("TIMESTAMP '") && source.contains(" BETWEEN ")
                && rewritten.contains(" BETWEEN ") && source.contains(" OR ") && rewritten.contains(" OR ");
    }

    @SuppressWarnings("unused")
    private boolean isPlanIndependentInvalidDateBooleanPredicate(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        if (!single.succeeded || split.succeeded) {
            return false;
        }
        String error = normalizeDuckDBQueryError(split.error);
        if (error == null || !error.contains("DATE -> BOOLEAN")) {
            return false;
        }
        String source = pair.singleQuery.toUpperCase(Locale.ROOT);
        String rewritten = pair.splitQuery.toUpperCase(Locale.ROOT);
        return source.contains("DATE '") && rewritten.contains("DATE '") && source.contains("AND(NULL)")
                && rewritten.contains("AND(NULL)");
    }

    @SuppressWarnings("unused")
    private boolean isPlanIndependentInvalidBooleanIntervalInPredicate(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        if (!single.succeeded || split.succeeded) {
            return false;
        }
        String error = normalizeDuckDBQueryError(split.error);
        if (error == null || !error.contains("BOOLEAN -> INTERVAL")) {
            return false;
        }
        String source = pair.singleQuery.toUpperCase(Locale.ROOT);
        String rewritten = pair.splitQuery.toUpperCase(Locale.ROOT);
        return source.contains(" IN (") && rewritten.contains(" IN (") && source.contains("INTERVAL '")
                && rewritten.contains("INTERVAL '") && source.contains("BETWEEN") && rewritten.contains("BETWEEN")
                && source.contains("VP_ROWID") && rewritten.contains("VP_ROWID");
    }

    @SuppressWarnings("unused")
    private boolean isPlanIndependentInvalidSourcePredicate(VpQueryPair pair, QueryExecution single,
            QueryExecution split) {
        if (single.succeeded || !split.succeeded) {
            return false;
        }
        String error = normalizeDuckDBQueryError(single.error);
        if (error == null || !error.startsWith("Conversion Error: Unimplemented type for cast (")) {
            return false;
        }
        String source = pair.singleQuery.toUpperCase(Locale.ROOT);
        String rewritten = pair.splitQuery.toUpperCase(Locale.ROOT);
        // Keep this deliberately narrow: the expression must be present in
        // both forms, and the failing source column must be the synthetic
        // BIGINT row id used by VP reconstruction.
        if (!source.contains("VP_ROWID") || !rewritten.contains("VP_ROWID") || !source.contains(" IN (")
                || !rewritten.contains(" IN (")) {
            return false;
        }
        if (error.contains("BIGINT -> TIME")) {
            return source.contains("TIME '") && rewritten.contains("TIME '");
        }
        return false;
    }

    private boolean containsNonFiniteHistogramValue(String value) {
        if (value == null || !(value.contains("{") || value.startsWith("MAP;"))) {
            return false;
        }
        String upper = value.toUpperCase(Locale.ROOT);
        return upper.contains("NAN") || upper.contains("INF");
    }

    private boolean containsNonFiniteHistogramInput(String value) {
        if (value == null) {
            return false;
        }
        return Pattern.compile("(?i)(?<![A-Z0-9_])(NAN|INF(?:INITY)?)(?![A-Z0-9_])").matcher(value).find();
    }

    private KnownVpBug classifyKnownOrderedBug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        return null;
    }

    private boolean isGroupByAllNoCaseMismatch(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) throws SQLException {
        String source = pair.singleQuery.toUpperCase(Locale.ROOT);
        String split = pair.splitQuery.toUpperCase(Locale.ROOT);
        if (!pair.description.toUpperCase(Locale.ROOT).contains("FRIENDLY=GROUP BY ALL")
                || !source.contains("GROUP BY ALL") || !hasGroupByAllCountProjection(source)
                || split.contains("GROUP BY ALL") || !hasMatchingExplicitGroupBy(source, split)) {
            return false;
        }
        boolean hasNoCaseGroupColumn = hasNoCaseGroupColumn(pair.schema, source)
                || hasNoCaseGroupColumnFromSetup(pair.schema, source)
                || hasNoCaseGroupColumnFromMaterializedDdl(pair.schema, source);
        if (!hasNoCaseGroupColumn) {
            return false;
        }

        Map<String, List<String>> spellings = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : singleRows.rows.entrySet()) {
            List<String> values = decodeCanonicalRow(entry.getKey());
            if (values.size() != 2 || values.get(0) == null) {
                continue;
            }
            String folded = values.get(0).toLowerCase(Locale.ROOT);
            List<String> existing = spellings.computeIfAbsent(folded, ignored -> new ArrayList<>());
            if (!existing.contains(values.get(0))) {
                existing.add(values.get(0));
            }
        }
        boolean hasCaseVariantGroup = spellings.values().stream().anyMatch(values -> values.size() > 1);
        if (!hasCaseVariantGroup) {
            return false;
        }

        // The split side may preserve a different representative key after
        // the explicit GROUP BY rewrite, but the case-folded group counts
        // must remain identical. This prevents unrelated result changes from
        // being absorbed as the NOCASE defect.
        Map<String, Integer> sourceFolded = foldGroupKeys(singleRows);
        Map<String, Integer> splitFolded = foldGroupKeys(splitRows);
        return sourceFolded != null && sourceFolded.equals(splitFolded);
    }

    private boolean hasGroupByAllCountProjection(String source) {
        return Pattern.compile("(?s)SELECT\\s+\"[^\"]+\"\\s*,\\s*COUNT\\s*\\(\\s*\\*\\s*\\)\\s+AS\\s+VP_COUNT")
                .matcher(source).find();
    }

    private boolean hasExplicitGroupBy(String query) {
        return Pattern.compile("(?s)\\bGROUP\\s+BY\\s+\"[^\"]+\"").matcher(query).find();
    }

    private boolean hasMatchingExplicitGroupBy(String source, String split) {
        Matcher groupColumn = Pattern
                .compile("(?s)SELECT\\s+\"([^\"]+)\"\\s*,\\s*COUNT\\s*\\(\\s*\\*\\s*\\)\\s+AS\\s+VP_COUNT")
                .matcher(source);
        if (!groupColumn.find()) {
            return false;
        }
        String quotedColumn = quote(groupColumn.group(1)).toUpperCase(Locale.ROOT);
        return hasExplicitGroupBy(split)
                && Pattern.compile("(?s)\\bGROUP\\s+BY\\s+" + Pattern.quote(quotedColumn)).matcher(split).find();
    }

    private Map<String, Integer> foldGroupKeys(QueryResultMultiset result) {
        Map<String, Integer> folded = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : result.rows.entrySet()) {
            List<String> values = decodeCanonicalRow(entry.getKey());
            if (values.size() != 2) {
                return null;
            }
            // SQL NULL is a valid GROUP BY key and must contribute to the
            // folded-count comparison without being confused with text.
            String key = values.get(0) == null ? "\\u0000NULL"
                    : "\\u0000VALUE:" + values.get(0).toLowerCase(Locale.ROOT);
            // Compare the aggregate cardinality, not the number of result
            // rows in the JDBC multiset: A:1 + a:1 must equal A:2.
            int groupCount;
            try {
                groupCount = Integer.parseInt(values.get(1));
            } catch (NumberFormatException e) {
                return null;
            }
            folded.put(key, folded.getOrDefault(key, 0) + groupCount * entry.getValue());
        }
        return folded;
    }

    private boolean hasNoCaseGroupColumn(VpSchema schema, String source) {
        Matcher groupColumn = Pattern
                .compile("(?s)SELECT\\s+\"([^\"]+)\"\\s*,\\s*COUNT\\s*\\(\\s*\\*\\s*\\)\\s+AS\\s+VP_COUNT")
                .matcher(source);
        if (!groupColumn.find()) {
            return false;
        }
        String quotedColumn = quote(groupColumn.group(1)).toUpperCase(Locale.ROOT);
        boolean setupDeclaresNoCase = schema.setupStatements.stream().map(setup -> setup.toUpperCase(Locale.ROOT))
                .anyMatch(setup -> setup.contains(quotedColumn) && setup.contains("COLLATE NOCASE"));
        if (setupDeclaresNoCase) {
            return true;
        }
        return schema.columnCollationClauses.entrySet().stream()
                .anyMatch(entry -> entry.getKey().equalsIgnoreCase(groupColumn.group(1))
                        && entry.getValue().toUpperCase(Locale.ROOT).contains("NOCASE"));
    }

    private boolean hasNoCaseGroupColumnFromSetup(VpSchema schema, String source) {
        Matcher groupColumn = Pattern
                .compile("(?s)SELECT\\s+\"([^\"]+)\"\\s*,\\s*COUNT\\s*\\(\\s*\\*\\s*\\)\\s+AS\\s+VP_COUNT")
                .matcher(source);
        if (!groupColumn.find()) {
            return false;
        }
        String column = quote(groupColumn.group(1)).toUpperCase(Locale.ROOT);
        return schema.setupStatements.stream().map(setup -> setup.toUpperCase(Locale.ROOT))
                .anyMatch(setup -> setup.contains("COLLATE NOCASE") && setup.contains(column));
    }

    private boolean hasNoCaseGroupColumnFromMaterializedDdl(VpSchema schema, String source) throws SQLException {
        Matcher groupColumn = Pattern
                .compile("(?s)SELECT\\s+\"([^\"]+)\"\\s*,\\s*COUNT\\s*\\(\\s*\\*\\s*\\)\\s+AS\\s+VP_COUNT")
                .matcher(source);
        if (!groupColumn.find()) {
            return false;
        }
        String ddl = getCreateTable(schema.sourceTable).toUpperCase(Locale.ROOT);
        String column = quote(groupColumn.group(1)).toUpperCase(Locale.ROOT);
        return ddl.contains(column) && ddl.contains("COLLATE NOCASE");
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
        File dir = new File(Main.LOG_DIRECTORY, "duckdb");
        try {
            Files.createDirectories(dir.toPath());
            Path target = nextKnownBugLogPath(dir);
            Files.writeString(target, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void logKnownExecutionBug(VpQueryPair pair, KnownVpBug knownBug, QueryExecution single,
            QueryExecution split) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("Known VP bug: ").append(knownBug).append(System.lineSeparator()).append(System.lineSeparator());
        sb.append(formatErrorBugReport(pair, single, split));
        File dir = new File(Main.LOG_DIRECTORY, "duckdb");
        try {
            Files.createDirectories(dir.toPath());
            Path target = nextKnownBugLogPath(dir);
            Files.writeString(target, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void logKnownGenerationSuppression(VpQueryPair pair, KnownVpBug knownBug, OptimizerMode optimizerMode) {
        StringBuilder sb = new StringBuilder();
        sb.append("Known VP native crash pattern suppressed before execution: ").append(knownBug)
                .append(System.lineSeparator()).append(System.lineSeparator());
        sb.append("Database: ").append(state.getDatabaseName()).append(System.lineSeparator());
        sb.append("DuckDB version: ").append(getSingleStringSafe("SELECT version()")).append(System.lineSeparator());
        sb.append("Reconstruction strategy: ").append(pair.reconstructionStrategy).append(System.lineSeparator());
        sb.append("Optimizer mode: ").append(optimizerMode).append(System.lineSeparator());
        sb.append("Split threads: ")
                .append(Math.max(1, state.getDbmsSpecificOptions().verticalPartitioningSplitThreads))
                .append(System.lineSeparator());
        sb.append("Source rows: ").append(materializedSourceRowCount(pair.schema)).append(System.lineSeparator());
        sb.append("Description: ").append(pair.description).append(System.lineSeparator());
        sb.append("Single query: ").append(pair.singleQuery).append(System.lineSeparator());
        sb.append("Split query: ").append(pair.splitQuery).append(System.lineSeparator());
        File dir = new File(Main.LOG_DIRECTORY, "duckdb");
        try {
            Files.createDirectories(dir.toPath());
            Path target = nextKnownBugLogPath(dir);
            Files.writeString(target, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        } catch (IOException ignored) {
            // Keep the suppression decision independent of metadata logging.
        }
    }

    private String getSingleStringSafe(String sql) {
        try {
            return getSingleString(sql);
        } catch (SQLException e) {
            return "<unavailable>";
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
        if (!left.columnTypes.equals(right.columnTypes) || left.orderedRows.size() != right.orderedRows.size()) {
            return false;
        }
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

    private List<String> getColumnTypes(SQLancerResultSet rs, int columnCount) throws SQLException {
        List<String> types = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            types.add(rs.getType(i));
        }
        return types;
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
        List<String> columnTypes = List.of();
        Main.nrQueries.addAndGet(1);
        try {
            rs = adapter.executeAndGet(state);
            if (rs == null) {
                throw new IgnoreMeException();
            }
            int columnCount = rs.getColumnCount();
            columnTypes = getColumnTypes(rs, columnCount);
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
        return new QueryResultMultiset(rows, approximateColumns, orderedRows, orderedApproximateColumns,
                columnTypes);
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
        String lowerMsg = msg.toLowerCase(Locale.ROOT);
        return msg.contains("Invalid length") && msg.contains("for type DATE") || lowerMsg.contains("malformed json")
                || lowerMsg.contains("bad json path") || lowerMsg.contains("json cannot hold blob values")
                || lowerMsg.contains("json_object() labels must be text")
                || lowerMsg.contains("unable to use function") && lowerMsg.contains("requested context")
                || lowerMsg.contains("table does not support scanning")
                || lowerMsg.contains("unrecognized configuration parameter")
                || lowerMsg.contains("no support for that alter table option");
    }

    private boolean isSkippableQueryError(Throwable e) {
        return isSkippableQueryErrorText(getErrorText(e));
    }

    private boolean isSkippableQueryErrorText(String msg) {
        if (msg == null || msg.isEmpty()) {
            return false;
        }
        String lowerMsg = msg.toLowerCase(Locale.ROOT);
        return msg.contains("doesn't yet support") || lowerMsg.contains("parser error")
                || lowerMsg.contains("binder error") || lowerMsg.contains("conversion error")
                || lowerMsg.contains("catalog error") || lowerMsg.contains("invalid input error")
                || lowerMsg.contains("out of range error") || lowerMsg.contains("syntax error")
                || lowerMsg.contains("not implemented") || lowerMsg.contains("unimplemented")
                || lowerMsg.contains("unsupported") || lowerMsg.contains("does not support")
                || msg.contains("Invalid JSON") || lowerMsg.contains("malformed json")
                || lowerMsg.contains("bad json path") || lowerMsg.contains("json cannot hold blob values")
                || lowerMsg.contains("json_object() labels must be text")
                || lowerMsg.contains("unable to use function") && lowerMsg.contains("requested context")
                || lowerMsg.contains("table does not support scanning")
                || lowerMsg.contains("no support for that alter table option")
                || lowerMsg.contains("unrecognized configuration parameter");
    }

    private boolean bothFailedWithSameExpectedResourceFailure(String firstError, String secondError) {
        String firstKind = expectedResourceFailureKind(firstError);
        return firstKind != null && firstKind.equals(expectedResourceFailureKind(secondError));
    }

    private String expectedResourceFailureKind(String error) {
        if (error == null || error.isEmpty()) {
            return null;
        }
        String lower = error.toLowerCase(Locale.ROOT);
        if (lower.contains("out of memory error")) {
            return "OUT_OF_MEMORY";
        }
        if (lower.contains("could not read enough bytes from file")) {
            return "TEMP_FILE_SHORT_READ";
        }
        if (lower.contains("failed to pin block of size")) {
            return "PIN_BLOCK_FAILURE";
        }
        if (lower.contains("failed to allocate data of size")) {
            return "ALLOCATION_FAILURE";
        }
        return null;
    }

    private static boolean isSkippableDuckDBError(Throwable throwable) {
        String lowerMsg = getErrorText(throwable).toLowerCase(Locale.ROOT);
        return lowerMsg.contains("table does not support scanning") || lowerMsg.contains("not authorized");
    }

    private static String getErrorText(Throwable throwable) {
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
            String type = rs.getType(i);
            boolean structured = DuckDBValueCanonicalizer.isStructuredType(type);
            Object value = structured ? rs.getObject(i) : rs.getString(i);
            if (value == null) {
                sb.append("N;");
            } else {
                String canonical = structured ? DuckDBValueCanonicalizer.canonicalize(value, type)
                        : canonicalizeValue((String) value, type);
                sb.append("S:").append(canonical.length()).append(":").append(canonical).append(";");
            }
        }
        return sb.toString();
    }

    private String canonicalizeValue(String value, String typeName) {
        if (!isCanonicalNumericType(typeName)) {
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

    private static boolean isCanonicalNumericType(String typeName) {
        if (typeName == null) {
            return false;
        }
        String normalizedType = typeName.trim().toUpperCase(Locale.ROOT);
        int parameterStart = normalizedType.indexOf('(');
        if (parameterStart >= 0) {
            normalizedType = normalizedType.substring(0, parameterStart).trim();
        }
        switch (normalizedType) {
        case "TINYINT":
        case "SMALLINT":
        case "INTEGER":
        case "INT":
        case "BIGINT":
        case "HUGEINT":
        case "UTINYINT":
        case "USMALLINT":
        case "UINTEGER":
        case "UBIGINT":
        case "DECIMAL":
        case "NUMERIC":
        case "REAL":
        case "FLOAT":
        case "DOUBLE":
            return true;
        default:
            return false;
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
        sb.append("DuckDB version: ").append(getSingleString("SELECT version()")).append(System.lineSeparator());
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
        sb.append("Single column types: ").append(singleRows.columnTypes).append(System.lineSeparator());
        sb.append("Split column types: ").append(splitRows.columnTypes).append(System.lineSeparator());
        sb.append("JDBC row counts: single=").append(singleRows.orderedRows.size()).append(", split=")
                .append(splitRows.orderedRows.size()).append(System.lineSeparator());
        sb.append("Single result sample: ").append(sample(singleRows.rows)).append(System.lineSeparator());
        sb.append("Split result sample: ").append(sample(splitRows.rows)).append(System.lineSeparator());
        sb.append("Only in single: ").append(diff(singleRows, splitRows)).append(System.lineSeparator());
        sb.append("Only in split: ").append(diff(splitRows, singleRows)).append(System.lineSeparator());
        return sb.toString();
    }

    private String formatErrorBugReport(VpQueryPair pair, QueryExecution single, QueryExecution split) {
        StringBuilder sb = new StringBuilder();
        sb.append("[VERTICAL PARTITIONING EXECUTION BUG CANDIDATE]").append(System.lineSeparator());
        if (deepExplorationEnabled()) {
            sb.append("Execution mode: deep plan divergence").append(System.lineSeparator());
            sb.append("Source execution: threads=1, disabled_optimizers=''").append(System.lineSeparator());
            sb.append("Split execution: threads=")
                    .append(Math.max(1, state.getDbmsSpecificOptions().verticalPartitioningSplitThreads))
                    .append(", disabled_optimizers='").append(VP_SPLIT_DISABLED_OPTIMIZERS).append("'")
                    .append(System.lineSeparator());
        } else {
            sb.append("Execution mode: default DuckDB session settings").append(System.lineSeparator());
        }
        sb.append("Strategy summary: split=").append(pair.schema.splitStrategy).append(", reconstruction=")
                .append(pair.reconstructionStrategy).append(", shape=").append(pair.schema.baseShape)
                .append(System.lineSeparator());
        sb.append("Source status: ").append(single.succeeded ? "SUCCESS" : "ERROR").append(System.lineSeparator());
        if (!single.succeeded) {
            sb.append(single.error).append(System.lineSeparator());
        }
        sb.append("Split status: ").append(split.succeeded ? "SUCCESS" : "ERROR").append(System.lineSeparator());
        if (!split.succeeded) {
            sb.append(split.error).append(System.lineSeparator());
        }
        sb.append("Single query: ").append(pair.singleQuery).append(System.lineSeparator());
        sb.append("Split query: ").append(pair.splitQuery).append(System.lineSeparator());
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
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(
                "SELECT sql FROM duckdb_tables() WHERE table_name = '" + escapeLiteral(table) + "' LIMIT 1")) {
            if (rs.next()) {
                String sql = rs.getString(1);
                return sql == null ? "<unavailable>" : sql;
            }
            return "<unavailable>";
        } catch (SQLException ignored) {
            return "<unavailable>";
        }
    }

    private String extractEngineFromCreateTable(String createTable) {
        String upper = createTable.toUpperCase(Locale.ROOT);
        List<String> shape = new ArrayList<>();
        if (upper.contains("VIRTUAL TABLE")) {
            shape.add("VIRTUAL");
        }
        if (upper.contains("WITHOUT ROWID")) {
            shape.add("WITHOUT_ROWID");
        }
        if (upper.contains("STRICT")) {
            shape.add("STRICT");
        }
        return shape.isEmpty() ? "ROWID" : String.join("+", shape);
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
        return List.of("checkpoint_threshold").stream().map(this::capturePragma).collect(Collectors.joining(", "));
    }

    private String capturePragma(String pragmaName) {
        try {
            return pragmaName + "=" + getSingleString("PRAGMA " + pragmaName);
        } catch (SQLException e) {
            return pragmaName + "=<unavailable: " + e.getMessage() + ">";
        }
    }

    private String captureSessionSetStatements() {
        return "PRAGMA checkpoint_threshold='1 byte';" + System.lineSeparator();
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
        executeNoSchemaRefresh("DROP TABLE IF EXISTS " + quote(vp.tempTable));
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

    private String toSQLType(DuckDBCompositeDataType type) {
        return type.toSQLType();
        /*
         * switch (type) { case INT: return "BIGINT"; case VARCHAR: return "VARCHAR"; case FLOAT: return "DOUBLE"; case
         * BOOLEAN: return "BOOLEAN"; case DATE: return "DATE"; case TIMESTAMP: return "TIMESTAMP"; case NULL: return
         * "VARCHAR"; default: throw new AssertionError(type); }
         */
    }

    private String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private String escapeLiteral(String value) {
        return value.replace("'", "''");
    }

    private String sanitizeIdentifier(String identifier) {
        return identifier.replaceAll("[^A-Za-z0-9_]", "_");
    }
}
