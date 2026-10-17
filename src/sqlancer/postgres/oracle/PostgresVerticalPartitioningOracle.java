package sqlancer.postgres.oracle;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
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
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import sqlancer.IgnoreMeException;
import sqlancer.ExecutionTimer;
import sqlancer.Main;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.StateToReproduce.OracleRunReproductionState;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresProvider;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresIndex;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTable.TableType;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresColumnReference;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresAggregate;
import sqlancer.postgres.ast.PostgresAggregate.PostgresAggregateFunction;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.ast.PostgresTableReference;
import sqlancer.postgres.ast.PostgresText;
import sqlancer.postgres.gen.PostgresCommon;
import sqlancer.postgres.gen.PostgresExpressionGenerator;

public class PostgresVerticalPartitioningOracle implements TestOracle<PostgresGlobalState> {

    private static final AtomicLong ORACLE_RUN_ID = new AtomicLong();
    private static final String ROW_ID = "vp_rowid";
    private static final int MAX_DIFF_ROWS = 8;
    private static final int MAX_RESULT_SAMPLE = 12;
    private static final long MAX_ORDERED_VALUE_AGGREGATE_ROWS = 256;
    private static final double FLOAT_ABSOLUTE_TOLERANCE = 1e-6;
    private static final double FLOAT_RELATIVE_TOLERANCE = 1e-5;
    private static final double DOUBLE_ABSOLUTE_TOLERANCE = 1e-12;
    private static final double DOUBLE_RELATIVE_TOLERANCE = 1e-9;
    private static final double AGGREGATE_ABSOLUTE_TOLERANCE = 1e-12;
    private static final double AGGREGATE_RELATIVE_TOLERANCE = 1e-9;
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
    private static final Map<String, Integer> SCHEMA_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> CANDIDATE_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SOURCE_ROW_COUNT_BUCKETS = new LinkedHashMap<>();
    private static final Map<String, Integer> CREATED_BASE_TABLE_PROFILE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SELECTED_BASE_TABLE_PROFILE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> STRESS_BASE_TABLE_PROFILE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> PROFILE_ATTEMPTS = new LinkedHashMap<>();
    private static final Map<String, Integer> PROFILE_COLUMNS_CREATED = new LinkedHashMap<>();
    private static final Map<String, Long> PROFILE_NONNULL_ROWS = new LinkedHashMap<>();
    private static final Map<String, Integer> PROFILE_QUERY_USES = new LinkedHashMap<>();
    private static final Map<String, String> BASE_TABLE_PROFILES = new LinkedHashMap<>();
    private static long sourceRowCountTotal;
    private static long sourceRowCountMinimum = Long.MAX_VALUE;
    private static long sourceRowCountMaximum;
    private static final Map<String, Integer> SCHEMA_COPY_SKIP_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> MVCC_AUXILIARY_CONNECTION_FAILURE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> MVCC_SETUP_SKIP_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SKIPPED_ERROR_COUNTS = new LinkedHashMap<>();
    private static final Map<String, List<SkippedErrorSample>> SKIPPED_ERROR_SAMPLES = new LinkedHashMap<>();
    private static final Map<String, Integer> PLAN_FINGERPRINT_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> PLAN_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> PLAN_DIVERGENCE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SESSION_GUC_PROFILE_COUNTS = new LinkedHashMap<>();
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean();
    private static final AtomicLong KNOWN_BUG_LOG_ID = new AtomicLong();
    private static final AtomicLong UNEXPECTED_ERROR_LOG_ID = new AtomicLong();
    private static final AtomicLong DATABASE_BUG_LOG_ID = new AtomicLong();
    private static final AtomicLong GUC_DIFFERENTIAL_PROFILE_CURSOR = new AtomicLong();
    private static final AtomicLong STATS_DIRTY_EVENTS = new AtomicLong();
    private static final File STATS_FILE = new File(new File(Main.LOG_DIRECTORY, "postgres"), "vp-stats.log");
    private static final long STATS_FLUSH_INTERVAL = 64;
    private static final int MAX_SKIPPED_ERROR_COUNT_GROUPS = 200;
    private static final int MAX_SKIPPED_ERROR_SAMPLE_GROUPS = 50;
    private static final int MAX_SKIPPED_ERROR_SAMPLES_PER_GROUP = 2;
    private static final int MAX_PRIORITY_SKIPPED_ERROR_SAMPLES_PER_GROUP = 3;
    private static final int MAX_PLAN_FINGERPRINTS = 128;
    private static final int MAX_DIRECTED_PLAN_FINGERPRINTS = 128;
    private static final int MAX_CANONICAL_VALUE_LENGTH = 1_000_000;
    private static final int MAX_CANONICAL_ROW_LENGTH = 4_000_000;
    private static final String CANONICAL_DECIMAL_TOO_LARGE = "VP canonical decimal expansion too large";
    private static final String SUBPLAN_TARGET_LIST_ERROR = "variable not found in subplan target list";
    private static final String DISTINCT_ORDER_BY_NOT_IN_SELECT_LIST_ERROR = "for SELECT DISTINCT, ORDER BY expressions must appear in select list";
    private static final List<String> APPROXIMATE_AGGREGATE_PREFIXES = List.of("AVG(", "STDDEV(", "VARIANCE(",
            "SQRDIFF(", "PERCENTILE_CONT(", "CORR(", "COVAR_POP(", "COVAR_SAMP(", "REGR_");
    private static final List<String> JSON_AGGREGATE_PREFIXES = List.of("JSON_AGG(", "JSONB_AGG(");
    private static final List<String> JSON_AGGREGATE_PREFIXES_WITHOUT_EQUALITY = List.of("JSON_AGG(");
    private static int completedOracleRuns;
    private static int completedQueryPairs;
    private static int directedPlanFingerprintSamples;
    private static int completedAttempts;
    private static int schemaShapeSamples;
    private static int candidateShapeSamples;
    private static int planFingerprintSamples;
    private static final ObjectMapper PLAN_JSON_MAPPER = new ObjectMapper();

    private static final class SkippedErrorSample {
        private final String stage;
        private final String sql;

        private SkippedErrorSample(String stage, String sql) {
            this.stage = stage;
            this.sql = sql;
        }
    }

    private static final class PlanCapture {
        private final String fingerprint;
        private final Set<String> shapes;

        private PlanCapture(String fingerprint, Set<String> shapes) {
            this.fingerprint = fingerprint;
            this.shapes = shapes;
        }
    }

    private final PostgresGlobalState state;
    private final ExpectedErrors errors = new ExpectedErrors();
    private final List<String> vpReproductionStatements = new ArrayList<>();
    private boolean vpReproductionStateFlushed;
    private boolean vpDatabaseErrorLogged;
    private String activeSessionGucProfile = SessionGucProfile.BASELINE.name();

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
        NATURAL_OR_USING, STRAIGHT_JOIN, LATERAL_DERIVED, DIRECTED_MATERIALIZED_JOIN, DIRECTED_PARTITION_JOIN
    }

    private enum SessionGucProfile {
        BASELINE, NO_HASH_JOIN, NO_MERGE_JOIN, NO_NESTED_LOOP, PARALLEL_AGGRESSIVE, HASH_SPILL
    }

    private enum PairStage {
        GENERATED, EXTRA_SETUP_OK, SINGLE_OK, SPLIT_OK, COMPARED, MISMATCH
    }

    private enum KnownVpBug {
        PG18_PARTITIONED_GROUPING_SETS_SUBPLAN_TARGET_LIST
    }

    public enum AdvancedFeature {
        ROLLUP, CORRELATED_SUBQUERY, GROUP_CONCAT, ASYMMETRIC_INDEX, CHARSET_COLLATION_COLUMN, BOUNDARY_MUTATION,
        PARTITION_KEY_MUTATION, PARTITION_ATTACH_DETACH, UNION_ALL, MVCC_SNAPSHOT, MVCC_SAVEPOINT, MVCC_MAINTENANCE,
        FILTER_AGGREGATE, GROUPING_SETS, CUBE, DISTINCT_ON, ORDERED_SET_AGGREGATE, STATISTICAL_AGGREGATE,
        ARRAY_JSON_AGGREGATE, LATERAL_UNNEST, RECURSIVE_CTE, RANGE_OPERATOR, EXPLICIT_NULL_ORDERING,
        OUTER_NULL_EXTENSION, LOCAL_FK_TOPOLOGY, GENERATED_COLUMN_INDEX, WINDOW_EXCLUDE, GROUP_BY_DISTINCT,
        PARTITIONED_HELPER_TABLE, PARTITION_PRUNING, PARTITIONWISE_JOIN, POSTGRES_TYPE_COLUMNS, ARRAY_PROFILE,
        RANGE_PROFILE, TEXT_SEARCH_PROFILE, NETWORK_PROFILE, JSON_PROFILE, COLLATION_PROFILE, CTE_MATERIALIZATION,
        JSONB_LATERAL_RECORD, JSONB_LATERAL_EACH, JSONB_OPERATORS, GENERATE_SERIES_LATERAL
    }

    private static final class RowIdSnapshot {
        private final long count;
        private final long sum;
        private final Long min;
        private final Long max;

        private RowIdSnapshot(long count, long sum, Long min, Long max) {
            this.count = count;
            this.sum = sum;
            this.min = min;
            this.max = max;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof RowIdSnapshot)) {
                return false;
            }
            RowIdSnapshot that = (RowIdSnapshot) other;
            return count == that.count && sum == that.sum && Objects.equals(min, that.min)
                    && Objects.equals(max, that.max);
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, sum, min, max);
        }
    }

    private static final class VpColumn {
        private final String name;
        private final PostgresDataType type;
        private final String sqlType;
        private final boolean notNull;
        private final String defaultExpression;
        private final String generatedExpression;
        private final String baseCollation;

        private VpColumn(String name, PostgresDataType type) {
            this(name, type, null, false, null, null, null);
        }

        private VpColumn(String name, PostgresDataType type, String sqlType, boolean notNull, String defaultExpression,
                String generatedExpression, String baseCollation) {
            this.name = name;
            this.type = type;
            this.sqlType = sqlType;
            this.notNull = notNull;
            this.defaultExpression = defaultExpression;
            this.generatedExpression = generatedExpression;
            this.baseCollation = baseCollation;
        }

        private boolean isGenerated() {
            return generatedExpression != null;
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
        private final Map<String, String> sourceColumnCollationClauses = new LinkedHashMap<>();
        private final Map<String, String> leftColumnCollationClauses = new LinkedHashMap<>();
        private final Map<String, String> rightColumnCollationClauses = new LinkedHashMap<>();
        private String deepCollationColumn;
        private String sourceEngine;
        private String leftEngine;
        private String rightEngine;
        private boolean partitionedHelpers;
        private boolean leftPartitioned;
        private boolean rightPartitioned;
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
        private final List<ApproximateKind> forcedApproximateColumns;
        private final boolean deepCollationComparison;

        private VpQueryPair(VpSchema schema, ReconstructionStrategy reconstructionStrategy, String singleQuery,
                String splitQuery, List<String> extraSetupStatements, String description,
                boolean orderedComparisonEnabled, List<ApproximateKind> forcedApproximateColumns,
                boolean deepCollationComparison) {
            this.schema = schema;
            this.reconstructionStrategy = reconstructionStrategy;
            this.singleQuery = singleQuery;
            this.splitQuery = splitQuery;
            this.extraSetupStatements = extraSetupStatements;
            this.description = description;
            this.orderedComparisonEnabled = orderedComparisonEnabled;
            this.forcedApproximateColumns = forcedApproximateColumns;
            this.deepCollationComparison = deepCollationComparison;
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
        NONE(0, 0), DOUBLE(DOUBLE_ABSOLUTE_TOLERANCE, DOUBLE_RELATIVE_TOLERANCE),
        AGGREGATE(AGGREGATE_ABSOLUTE_TOLERANCE, AGGREGATE_RELATIVE_TOLERANCE),
        FLOAT(FLOAT_ABSOLUTE_TOLERANCE, FLOAT_RELATIVE_TOLERANCE),
        // Aggregate results have the wider absolute tolerance, while float results
        // have the wider relative tolerance. Preserve both when the annotations mix.
        WIDEST(Math.max(AGGREGATE_ABSOLUTE_TOLERANCE, FLOAT_ABSOLUTE_TOLERANCE),
                Math.max(AGGREGATE_RELATIVE_TOLERANCE, FLOAT_RELATIVE_TOLERANCE));

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
            if (left == NONE) {
                return right;
            }
            if (right == NONE) {
                return left;
            }
            if (left == right) {
                return left;
            }
            if (left == WIDEST || right == WIDEST || (left == AGGREGATE && right == FLOAT)
                    || (left == FLOAT && right == AGGREGATE)) {
                return WIDEST;
            }
            return left.absoluteTolerance >= right.absoluteTolerance ? left : right;
        }
    }

    public PostgresVerticalPartitioningOracle(PostgresGlobalState state) {
        this.state = state;
        initializeStatsLogging();
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonFetchErrors(errors);
        errors.add("syntax error");
        errors.add("unknown signature");
        errors.add("unsupported comparison operator");
        errors.add("unsupported binary operator");
        errors.add("cannot be used in this context");
        errors.add("is not in select list");
        errors.add("must appear in the GROUP BY clause or be used in an aggregate function");
        errors.add("non-integer constant in ORDER BY");
        errors.add("cannot order by");
        errors.add("can't order by column type jsonb");
        errors.add("invalid cast");
        errors.add("cannot convert NaN to integer");
        errors.add("cannot convert NaN to bigint");
        errors.add("could not parse");
        errors.add("violates foreign key constraint");
        errors.add("violates unique constraint");
        errors.add("failed to satisfy CHECK constraint");
        errors.add("field position must not be zero");
    }

    @Override
    public void check() throws Exception {
        vpReproductionStatements.clear();
        vpReproductionStateFlushed = false;
        vpDatabaseErrorLogged = false;
        try {
            resetSessionForVerticalPartitioningOracle();
            PostgresProvider.ensureVerticalPartitioningBaseTables(state, 2);
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
                            recordPairGenerated(pair);
                            checkedPairs += comparePair(pair);
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
            PostgresProvider.replaceVerticalPartitioningTableAfterSuccessfulCheck(state, baseTable.tableName);
        } catch (SQLException | AssertionError error) {
            if (isDatabaseInternalError(error) && !vpDatabaseErrorLogged) {
                logUnexpectedDatabaseError("oracle", error);
            }
            flushVpReproductionState();
            throw error;
        }
    }

    private BaseTableInfo selectBaseTable() throws Exception {
        state.updateSchema();
        List<PostgresTable> candidates = state.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> !t.isPartitionChild()).filter(t -> !t.getName().startsWith("vp_"))
                .filter(t -> !t.getColumns().isEmpty()).collect(Collectors.toList());
        if (candidates.isEmpty()) {
            throw new IgnoreMeException();
        }
        return createBaseTableInfo(selectProfileWeightedBaseTable(candidates));
    }

    /**
     * Choose a profile first and a table second. Selecting directly from all tables makes LARGE tables
     * progressively less likely as the mutation phase accumulates SMALL tables.
     */
    private PostgresTable selectProfileWeightedBaseTable(List<PostgresTable> candidates) throws IgnoreMeException {
        Map<String, List<PostgresTable>> byProfile = new LinkedHashMap<>();
        synchronized (STATS_LOCK) {
            for (PostgresTable candidate : candidates) {
                String profile = BASE_TABLE_PROFILES.getOrDefault(
                        baseTableProfileKey(state.getDatabaseName(), candidate.getName()), "UNTRACKED");
                byProfile.computeIfAbsent(profile, ignored -> new ArrayList<>()).add(candidate);
            }
        }
        int totalWeight = byProfile.keySet().stream().mapToInt(PostgresVerticalPartitioningOracle::profileWeight).sum();
        if (totalWeight <= 0) {
            throw new IgnoreMeException();
        }
        int ticket = state.getRandomly().getInteger(0, totalWeight);
        for (Map.Entry<String, List<PostgresTable>> entry : byProfile.entrySet()) {
            ticket -= profileWeight(entry.getKey());
            if (ticket < 0) {
                return Randomly.fromList(entry.getValue());
            }
        }
        return Randomly.fromList(candidates);
    }

    private static int profileWeight(String profile) {
        switch (profile) {
        case "LARGE":
            return 8;
        case "UNDERFILLED":
            return 4;
        case "SMALL":
            return 2;
        case "EMPTY":
            return 1;
        default:
            return 1;
        }
    }

    private BaseTableInfo createBaseTableInfo(PostgresTable base) throws Exception {
        List<VpColumn> sourceColumns = getVpColumns(base);
        long rowCount = getRowCount(base.getName());
        recordSourceRowCount(rowCount);
        recordSelectedBaseTableProfile(state.getDatabaseName(), base.getName());
        BaseShape shape = rowCount == 0 ? BaseShape.EMPTY
                : sourceColumns.size() == 1 ? BaseShape.SINGLE_COLUMN : BaseShape.MULTI_COLUMN;
        incrementSelectedBaseShape(shape);
        recordCandidateShape(shape, rowCount, sourceColumns.size());
        String stableOrder = getStableOrder(base, sourceColumns);
        return new BaseTableInfo(base.getName(), sourceColumns, shape, stableOrder, rowCount, "N/A");
    }

    private List<VpColumn> getVpColumns(PostgresTable base) throws SQLException {
        Map<String, PostgresDataType> abstractTypes = base.getColumns().stream()
                .collect(Collectors.toMap(PostgresColumn::getName, PostgresColumn::getType));
        List<VpColumn> columns = new ArrayList<>();
        String sql = "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull, "
                + "pg_get_expr(ad.adbin, ad.adrelid), " + (supportsPostgresAtLeast(12) ? "a.attgenerated" : "''") + ", "
                + "quote_ident(collns.nspname) || '.' || quote_ident(coll.collname) "
                + "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
                + "LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum "
                + "LEFT JOIN pg_collation coll ON coll.oid = a.attcollation "
                + "LEFT JOIN pg_namespace collns ON collns.oid = coll.collnamespace " + "WHERE c.oid = to_regclass("
                + quoteLiteral(base.getName()) + ") " + "AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum";
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                String name = rs.getString(1);
                PostgresDataType type = abstractTypes.get(name);
                if (type == null) {
                    continue;
                }
                String expression = rs.getString(4);
                String generated = "s".equals(rs.getString(5)) ? expression : null;
                String defaultExpression = generated == null ? expression : null;
                columns.add(new VpColumn(name, type, rs.getString(2), rs.getBoolean(3), defaultExpression, generated,
                        rs.getString(6)));
            }
        }
        if (columns.size() == base.getColumns().size()) {
            return columns;
        }
        recordSkippedError("helper-column-catalog", base.getName(), "incomplete-column-metadata");
        return base.getColumns().stream()
                .map(c -> new VpColumn(c.getName(), c.getType(), c.getCatalogType(), false, null, null, null))
                .collect(Collectors.toList());
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

    private void resetSessionForVerticalPartitioningOracle() throws Exception {
        executeOptional("ROLLBACK");
        executeOptional("RESET ALL");
        executeOptional("SET TIME ZONE 'UTC'");
        executeOptional("SET statement_timeout = 15000");
        executeOptional("SET extra_float_digits = 0");
        applySessionGucProfile(SessionGucProfile.BASELINE, false);
        recordVpReproductionStatements(captureSessionSetStatements());
    }

    private void applySessionGucProfile(SessionGucProfile profile, boolean recordComparison) throws Exception {
        // Make the pair's plan environment explicit before applying the one
        // perturbation that is being tested against the baseline.
        executeNoSchemaRefresh("SET enable_hashjoin = on");
        executeNoSchemaRefresh("SET enable_mergejoin = on");
        executeNoSchemaRefresh("SET enable_nestloop = on");
        executeNoSchemaRefresh("SET max_parallel_workers_per_gather = 0");
        executeOptional("SET jit = on");
        switch (profile) {
        case NO_HASH_JOIN:
            executeNoSchemaRefresh("SET enable_hashjoin = off");
            break;
        case NO_MERGE_JOIN:
            executeNoSchemaRefresh("SET enable_mergejoin = off");
            break;
        case NO_NESTED_LOOP:
            executeNoSchemaRefresh("SET enable_nestloop = off");
            break;
        case PARALLEL_AGGRESSIVE:
            executeNoSchemaRefresh("SET max_parallel_workers_per_gather = 4");
            executeNoSchemaRefresh("SET min_parallel_table_scan_size = 0");
            executeNoSchemaRefresh("SET min_parallel_index_scan_size = 0");
            executeNoSchemaRefresh("SET parallel_setup_cost = 0");
            executeNoSchemaRefresh("SET parallel_tuple_cost = 0");
            executeOptional("SET enable_parallel_append = on");
            executeOptional("SET enable_parallel_hash = on");
            break;
        case HASH_SPILL:
            executeNoSchemaRefresh("SET work_mem = '64kB'");
            executeOptional("SET jit = off");
            break;
        case BASELINE:
            break;
        default:
            throw new AssertionError(profile);
        }
        if (supportsPartitionwiseJoin()) {
            executeNoSchemaRefresh("SET enable_partitionwise_join = on");
            executeOptional("SET enable_partitionwise_aggregate = on");
            executeNoSchemaRefresh("SET enable_partition_pruning = on");
        }
        activeSessionGucProfile = profile.name();
        if (recordComparison) {
            synchronized (STATS_LOCK) {
                incrementString(SESSION_GUC_PROFILE_COUNTS, activeSessionGucProfile);
            }
            markVpStatsDirty();
        }
    }

    private List<SessionGucProfile> gucProfilesForPair() {
        List<SessionGucProfile> profiles = new ArrayList<>();
        profiles.add(SessionGucProfile.BASELINE);
        List<SessionGucProfile> stressProfiles = List.of(SessionGucProfile.NO_HASH_JOIN,
                SessionGucProfile.NO_MERGE_JOIN, SessionGucProfile.NO_NESTED_LOOP,
                SessionGucProfile.PARALLEL_AGGRESSIVE, SessionGucProfile.HASH_SPILL);
        long index = GUC_DIFFERENTIAL_PROFILE_CURSOR.getAndIncrement();
        profiles.add(stressProfiles.get((int) Math.floorMod(index, stressProfiles.size())));
        return profiles;
    }

    private void resetSessionForGucDifferentialProfile(SessionGucProfile profile) throws Exception {
        executeOptional("ROLLBACK");
        executeOptional("RESET ALL");
        executeOptional("SET TIME ZONE 'UTC'");
        executeOptional("SET statement_timeout = 15000");
        executeOptional("SET extra_float_digits = 0");
        applySessionGucProfile(profile, true);
        recordVpReproductionStatements(captureSessionSetStatements());
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
                useForeignKey, base.stableOrderStrategy, base.rowCount, base.engine);
        try {
            initializeColumnCollations(vp);
            vp.partitionedHelpers = shouldUsePartitionedHelpers(vp);
            dropArtifacts(vp);
            createSourceTable(vp, helperEngineCandidate(base));
            validateGeneratedColumnDependencies(vp);
            populateSourceTable(vp, base.stableOrderStrategy);
            copyBaseSchemaObjectsToSource(vp);
            createPartitionTables(vp, helperEngineCandidate(base));
            populatePartitionTables(vp);
            copyBaseSchemaObjectsToTarget(vp, vp.leftTable, vp.leftColumns, "left");
            copyBaseSchemaObjectsToTarget(vp, vp.rightTable, vp.rightColumns, "right");
            maybeCreateHelperIndexes(vp);
            maybeCreateGeneratedColumnIndexes(vp);
            if (useForeignKey && hasUniqueRowIdKey(vp.leftTable)) {
                tryExecuteSetup(vp,
                        "ALTER TABLE " + quote(vp.rightTable) + " ADD CONSTRAINT " + quote(vp.prefix + "fk_r_l")
                                + " FOREIGN KEY (" + quote(ROW_ID) + ") REFERENCES " + quote(vp.leftTable) + "("
                                + quote(ROW_ID) + ")");
            }
            maybeCreateLocalColumnForeignKey(vp);
            return vp;
        } catch (Exception | AssertionError e) {
            executeOptional("DROP VIEW IF EXISTS " + quote(vp.viewName));
            executeOptional("DROP TABLE IF EXISTS " + quote(vp.tempTable));
            executeOptional("DROP TABLE IF EXISTS " + quote(vp.rightTable));
            executeOptional("DROP TABLE IF EXISTS " + quote(vp.leftTable));
            executeOptional("DROP TABLE IF EXISTS " + quote(vp.sourceTable));
            throw e;
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

    private String getStableOrder(PostgresTable base, List<VpColumn> columns) {
        List<String> primary = base.getColumns().stream().filter(PostgresColumn::isPrimaryKey)
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

    private long getRowCount(String tableName) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + quote(tableName);
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (!rs.next()) {
                throw new AssertionError("COUNT(*) returned no row for " + tableName);
            }
            return rs.getLong(1);
        }
    }

    private boolean isStableOrderType(PostgresDataType type) {
        switch (type) {
        case FLOAT:
        case REAL:
            return false;
        default:
            return true;
        }
    }

    private boolean isNumericType(PostgresDataType type) {
        switch (type) {
        case INT:
        case FLOAT:
        case REAL:
        case DECIMAL:
        case MONEY:
            return true;
        default:
            return false;
        }
    }

    private boolean isIndexableType(PostgresDataType type) {
        return true;
    }

    private String helperEngineCandidate(BaseTableInfo base) {
        return "N/A";
    }

    private void createSourceTable(VpSchema vp, String preferredEngine) throws Exception {
        vp.sourceEngine = createTableWithEngineFallback(vp, vp.sourceTable, sourceColumnDefs(vp), preferredEngine);
    }

    private void validateGeneratedColumnDependencies(VpSchema vp) throws SQLException {
        long expected = vp.columns.stream().filter(VpColumn::isGenerated).count();
        if (!copiesSourceSchemaObjects() || expected == 0) {
            return;
        }
        String sql = "SELECT COUNT(*) FROM pg_attribute WHERE attrelid = to_regclass(" + quoteLiteral(vp.sourceTable)
                + ") AND attnum > 0 AND NOT attisdropped"
                + (supportsPostgresAtLeast(12) ? " AND attgenerated = 's'" : " AND false");
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            if (!result.next() || result.getLong(1) != expected) {
                recordSkippedError("generated-column-dependency", vp.sourceTable,
                        "source-generated-column-count-mismatch expected=" + expected);
                throw new IgnoreMeException();
            }
        }
    }

    private void maybeCreateGeneratedColumnIndexes(VpSchema vp) {
        for (VpColumn column : vp.columns) {
            if (!column.isGenerated()) {
                continue;
            }
            String suffix = Integer.toHexString(Math.abs(column.name.hashCode()));
            createGeneratedColumnIndex(vp, vp.sourceTable, column, "src", suffix);
            if (containsColumn(vp.leftColumns, column.name)) {
                createGeneratedColumnIndex(vp, vp.leftTable, column, "l", suffix);
            }
            if (containsColumn(vp.rightColumns, column.name)) {
                createGeneratedColumnIndex(vp, vp.rightTable, column, "r", suffix);
            }
        }
    }

    private void createGeneratedColumnIndex(VpSchema vp, String table, VpColumn column, String side, String suffix) {
        String sql = "CREATE INDEX " + quote(vp.prefix + "gen_" + side + "_" + suffix) + " ON " + quote(table) + " ("
                + quote(column.name) + ")";
        try {
            executeSetup(vp, sql);
            recordAdvancedFeature(AdvancedFeature.GENERATED_COLUMN_INDEX);
        } catch (AssertionError | Exception ignored) {
            recordSchemaCopySkip("generated-column-index", sql, "index-creation-failed");
        }
    }

    private void initializeColumnCollations(VpSchema vp) {
        List<String> safeCollations = getSafeCollations();
        for (VpColumn column : vp.columns) {
            String clause = "";
            if (isCollatable(column) && column.baseCollation != null && !"default".equals(column.baseCollation)) {
                clause = " COLLATE " + column.baseCollation;
            }
            vp.sourceColumnCollationClauses.put(column.name, clause);
            vp.leftColumnCollationClauses.put(column.name, clause);
            vp.rightColumnCollationClauses.put(column.name, clause);
        }
        List<VpColumn> collatable = vp.columns.stream().filter(this::isCollatable).collect(Collectors.toList());
        if (deepExplorationEnabled() && !collatable.isEmpty() && !safeCollations.isEmpty()
                && Randomly.getBooleanWithRatherLowProbability()) {
            VpColumn column = Randomly.fromList(collatable);
            String baseCollation = normalizedCollationName(column.baseCollation);
            List<String> splitCollations = safeCollations.stream().filter(collation -> !collation.equals(baseCollation))
                    .collect(Collectors.toList());
            if (splitCollations.isEmpty()) {
                return;
            }
            String splitCollation = Randomly.fromList(splitCollations);
            vp.leftColumnCollationClauses.put(column.name, " COLLATE " + splitCollation);
            vp.rightColumnCollationClauses.put(column.name, " COLLATE " + splitCollation);
            vp.deepCollationColumn = column.name;
        }
    }

    private String normalizedCollationName(String collation) {
        return collation == null || collation.isBlank() || "default".equals(collation) ? "" : collation;
    }

    private boolean isCollatable(VpColumn column) {
        String sqlType = sqlTypeFor(column).toLowerCase(Locale.ROOT);
        return sqlType.equals("text") || sqlType.equals("citext") || sqlType.startsWith("character")
                || sqlType.startsWith("varchar");
    }

    private List<String> getSafeCollations() {
        String sql = "SELECT quote_ident(n.nspname) || '.' || quote_ident(c.collname) "
                + "FROM pg_collation c JOIN pg_namespace n ON n.oid = c.collnamespace "
                + "WHERE c.collencoding IN (-1, pg_char_to_encoding(current_setting('server_encoding'))) "
                + (supportsDeterministicCollationCatalog() ? "AND c.collisdeterministic " : "")
                + "AND c.collname <> 'default' " + "ORDER BY (c.collprovider = 'i') DESC, n.nspname, c.collname";
        List<String> collations = new ArrayList<>();
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                collations.add(rs.getString(1));
            }
        } catch (SQLException error) {
            if (isDatabaseInternalError(error)) {
                recordVpReproductionStatement(sql);
                throw new AssertionError("PostgreSQL internal error while reading the VP collation catalog", error);
            }
            recordSkippedError("collation-catalog", sql, getErrorText(error));
        }
        return collations;
    }

    private boolean supportsDeterministicCollationCatalog() {
        return supportsPostgresAtLeast(12);
    }

    private List<String> sourceColumnDefs(VpSchema vp) {
        List<String> columnDefs = new ArrayList<>();
        columnDefs.add(quote(ROW_ID) + " INT8 NOT NULL PRIMARY KEY");
        for (VpColumn column : vp.columns) {
            columnDefs.add(sourceColumnDef(vp, column));
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
        List<VpColumn> writableColumns = vp.columns.stream()
                .filter(c -> !copiesSourceSchemaObjects() || !c.isGenerated()).collect(Collectors.toList());
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

    private void createPartitionTables(VpSchema vp, String preferredEngine) throws Exception {
        vp.leftEngine = createPartitionTable(vp, vp.leftTable, vp.leftColumns, preferredEngine);
        vp.rightEngine = createPartitionTable(vp, vp.rightTable, vp.rightColumns, preferredEngine);
    }

    private String createPartitionTable(VpSchema vp, String tableName, List<VpColumn> columns, String preferredEngine)
            throws Exception {
        Map<String, String> collationClauses = tableName.equals(vp.leftTable) ? vp.leftColumnCollationClauses
                : vp.rightColumnCollationClauses;
        if (vp.partitionedHelpers) {
            try {
                createPartitionedHelperTable(vp, tableName, columns, collationClauses);
                if (tableName.equals(vp.leftTable)) {
                    vp.leftPartitioned = true;
                } else {
                    vp.rightPartitioned = true;
                }
                recordAdvancedFeature(AdvancedFeature.PARTITIONED_HELPER_TABLE);
                return preferredEngine;
            } catch (AssertionError | SQLException e) {
                executeOptional("DROP TABLE IF EXISTS " + quote(tableName) + " CASCADE");
            }
        }
        return createTableWithEngineFallback(vp, tableName, partitionColumnDefs(vp, columns, collationClauses),
                preferredEngine);
    }

    private boolean shouldUsePartitionedHelpers(VpSchema vp) {
        // Keep both vertical fragments on the same range layout so that an
        // equality join on vp_rowid is eligible for partitionwise planning.
        return advancedFeaturesEnabled() && vp.baseRowCount >= 2;
    }

    private void createPartitionedHelperTable(VpSchema vp, String tableName, List<VpColumn> columns,
            Map<String, String> collationClauses) throws Exception {
        List<String> defs = partitionedHelperColumnDefs(vp, columns, collationClauses);
        String parent = quote(tableName);
        executeSetup(vp, "CREATE TABLE " + parent + " (" + String.join(", ", defs) + ") PARTITION BY RANGE ("
                + quote(ROW_ID) + ")");
        long rows = Math.max(0,
                Math.min(vp.baseRowCount, state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows > 0
                        ? state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows : vp.baseRowCount));
        if (rows == 0) {
            throw new IgnoreMeException();
        }
        long split = rows / 2;
        if (split < 1 || split >= rows) {
            createRangePartition(vp, tableName, "p1", " FOR VALUES FROM (1) TO (" + (rows + 1) + ")");
        } else {
            createRangePartition(vp, tableName, "p1", " FOR VALUES FROM (1) TO (" + (split + 1) + ")");
            createRangePartition(vp, tableName, "p2", " FOR VALUES FROM (" + (split + 1) + ") TO (" + (rows + 1) + ")");
        }
        if (!isPartitionedHelperTable(tableName)) {
            throw new AssertionError("partitioned helper table was not created: " + tableName);
        }
    }

    private void createRangePartition(VpSchema vp, String tableName, String suffix, String bound) throws Exception {
        String child = quote(tableName + "_" + suffix);
        executeSetup(vp, "CREATE TABLE " + child + " PARTITION OF " + quote(tableName) + bound);
    }

    private boolean isPartitionedHelperTable(String tableName) throws SQLException {
        String sql = "SELECT EXISTS (SELECT 1 FROM pg_partitioned_table p JOIN pg_class c ON c.oid = p.partrelid "
                + "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND c.relname = "
                + quoteLiteral(tableName) + ")";
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            return result.next() && result.getBoolean(1);
        }
    }

    private List<String> partitionColumnDefs(VpSchema vp, List<VpColumn> columns,
            Map<String, String> collationClauses) {
        List<String> defs = new ArrayList<>();
        defs.add(quote(ROW_ID) + " BIGINT NOT NULL PRIMARY KEY");
        for (VpColumn column : columns) {
            defs.add(partitionColumnDef(vp, column, collationClauses));
        }
        return defs;
    }

    private List<String> partitionedHelperColumnDefs(VpSchema vp, List<VpColumn> columns,
            Map<String, String> collationClauses) {
        List<String> defs = new ArrayList<>();
        // Declarative partitioned-table primary keys are supported from PG11.
        // Keep the PG10 fallback because PG10 rejects the parent constraint.
        defs.add(quote(ROW_ID) + " BIGINT NOT NULL" + (supportsPostgresAtLeast(11) ? " PRIMARY KEY" : ""));
        for (VpColumn column : columns) {
            defs.add(partitionColumnDef(vp, column, collationClauses));
        }
        return defs;
    }

    private boolean copiesSourceSchemaObjects() {
        return state.getDbmsSpecificOptions().verticalPartitioningSourceCopySchemaObjects;
    }

    private String sqlTypeFor(VpColumn column) {
        if (column.sqlType != null && !column.sqlType.isBlank()) {
            return column.sqlType;
        }
        // An opaque catalog type is safe only while its exact format_type is
        // available. Guessing TEXT here would silently change VP semantics.
        if (column.type == PostgresDataType.OTHER) {
            throw new IgnoreMeException();
        }
        return toSQLType(column.type);
    }

    private String sourceColumnDef(VpSchema vp, VpColumn column) {
        StringBuilder definition = new StringBuilder(quote(column.name)).append(" ").append(sqlTypeFor(column))
                .append(vp.sourceColumnCollationClauses.getOrDefault(column.name, ""));
        if (copiesSourceSchemaObjects()) {
            if (column.isGenerated()) {
                definition.append(" GENERATED ALWAYS AS (").append(column.generatedExpression).append(") STORED");
            } else if (column.defaultExpression != null) {
                definition.append(" DEFAULT ").append(column.defaultExpression);
            }
            if (column.notNull) {
                definition.append(" NOT NULL");
            }
        } else {
            definition.append(" NULL");
        }
        return definition.toString();
    }

    private String partitionColumnDef(VpSchema vp, VpColumn column) {
        return partitionColumnDef(vp, column, vp.sourceColumnCollationClauses);
    }

    private String partitionColumnDef(VpSchema vp, VpColumn column, Map<String, String> collationClauses) {
        return quote(column.name) + " " + sqlTypeFor(column) + collationClauses.getOrDefault(column.name, "") + " NULL";
    }

    private String plainColumnDef(VpColumn column) {
        return quote(column.name) + " " + sqlTypeFor(column) + " NULL";
    }

    private String createTableWithEngineFallback(VpSchema vp, String tableName, List<String> columnDefs,
            String preferredEngine) throws Exception {
        try {
            executeSetup(vp, createTableSQL(tableName, columnDefs, preferredEngine));
            return preferredEngine;
        } catch (AssertionError e) {
            if (deepExplorationEnabled() && containsCharsetCollation(columnDefs)) {
                List<String> plainDefs = plainColumnDefs(tableName, columnDefs, vp);
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

    private List<String> plainColumnDefs(String tableName, List<String> columnDefs, VpSchema vp) {
        if (tableName.equals(vp.sourceTable)) {
            List<String> defs = new ArrayList<>();
            defs.add(quote(ROW_ID) + " INT8 NOT NULL PRIMARY KEY");
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
        return "CREATE TABLE " + quote(tableName) + " (" + String.join(", ", columnDefs) + ")";
    }

    private boolean canFallbackHelperEngine(String engine) {
        return false;
    }

    private String recreateHelperTableWithInnoDB(VpSchema vp, String tableName, List<String> columnDefs)
            throws Exception {
        executeSetup(vp, "DROP TABLE IF EXISTS " + quote(tableName));
        executeSetup(vp, createTableSQL(tableName, columnDefs, "N/A"));
        return "N/A";
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
            Map<String, String> collationClauses = tableName.equals(vp.leftTable) ? vp.leftColumnCollationClauses
                    : vp.rightColumnCollationClauses;
            String fallbackEngine = recreateHelperTableWithInnoDB(vp, tableName,
                    partitionColumnDefs(vp, columns, collationClauses));
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
        return columns.stream().filter(c -> isIndexableType(c.type)).collect(Collectors.toList());
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
            executeInternalStatement("optional-setup", sql, false);
            vp.setupStatements.add(sql);
        } catch (Exception | AssertionError ignored) {
        }
    }

    private void copyBaseSchemaObjectsToSource(VpSchema vp) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSourceCopySchemaObjects) {
            return;
        }
        copyBaseIndexesToSource(vp);
        copyBaseCheckConstraintsToSource(vp);
        copyBaseNonPrimaryConstraintsToSource(vp);
        copyBaseExtendedStatisticsToSource(vp);
        recordBaseForeignKeyTopology(vp);
    }

    private void copyBaseSchemaObjectsToTarget(VpSchema vp, String target, List<VpColumn> targetColumns,
            String targetKind) {
        if (!copiesSourceSchemaObjects()) {
            return;
        }
        copyBaseColumnSemanticsToTarget(vp, target, targetColumns, targetKind);
        copyBaseTargetIndexes(vp, target, targetColumns, targetKind);
        copyBaseTargetConstraints(vp, target, targetColumns, targetKind);
        copyBaseTargetStatistics(vp, target, targetColumns, targetKind);
    }

    private void copyBaseColumnSemanticsToTarget(VpSchema vp, String target, List<VpColumn> targetColumns,
            String targetKind) {
        List<String> names = targetColumns.stream().map(c -> c.name).collect(Collectors.toList());
        for (VpColumn column : targetColumns) {
            if (column.isGenerated()) {
                recordSchemaCopySkip("schema-copy-" + targetKind + "-column", column.name,
                        "generated-expression-kept-materialized");
                continue;
            }
            if (column.notNull) {
                tryExecuteSchemaCopySetup(vp, "schema-copy-" + targetKind + "-not-null",
                        "ALTER TABLE " + quote(target) + " ALTER COLUMN " + quote(column.name) + " SET NOT NULL");
            }
            if (column.defaultExpression != null
                    && defaultReferencesOnly(column.defaultExpression, names, vp.columns)) {
                tryExecuteSchemaCopySetup(vp, "schema-copy-" + targetKind + "-default", "ALTER TABLE " + quote(target)
                        + " ALTER COLUMN " + quote(column.name) + " SET DEFAULT " + column.defaultExpression);
            } else if (column.defaultExpression != null) {
                recordSchemaCopySkip("schema-copy-" + targetKind + "-default", column.name,
                        "default-references-unavailable-column");
            }
        }
    }

    private boolean defaultReferencesOnly(String expression, List<String> availableColumns, List<VpColumn> allColumns) {
        for (VpColumn column : allColumns) {
            if (!availableColumns.contains(column.name)
                    && expression.matches("(?s).*\\b" + java.util.regex.Pattern.quote(column.name) + "\\b.*")) {
                return false;
            }
        }
        return true;
    }

    private void maybeCreateLocalColumnForeignKey(VpSchema vp) {
        if (!deepExplorationEnabled() || vp.overlapColumns.isEmpty()
                || !Randomly.getBooleanWithRatherLowProbability()) {
            return;
        }
        VpColumn column = Randomly.fromList(vp.overlapColumns);
        String index = "CREATE UNIQUE INDEX " + quote(vp.prefix + "fk_key_idx") + " ON " + quote(vp.leftTable) + " ("
                + quote(column.name) + ")";
        if (!tryExecuteSchemaCopySetup(vp, "schema-copy-local-fk-index", index)) {
            recordSchemaCopySkip("schema-copy-local-fk", column.name, "referenced-column-not-unique");
            return;
        }
        String fk = "ALTER TABLE " + quote(vp.rightTable) + " ADD CONSTRAINT " + quote(vp.prefix + "fk_col")
                + " FOREIGN KEY (" + quote(column.name) + ") REFERENCES " + quote(vp.leftTable) + " ("
                + quote(column.name) + ")";
        if (tryExecuteSchemaCopySetup(vp, "schema-copy-local-fk", fk)) {
            recordAdvancedFeature(AdvancedFeature.LOCAL_FK_TOPOLOGY);
        }
    }

    private void copyBaseTargetIndexes(VpSchema vp, String target, List<VpColumn> targetColumns, String targetKind) {
        String sql = "SELECT pg_get_indexdef(ix.indexrelid), ix.indisprimary, ix.indisunique, ix.indexprs IS NOT NULL, "
                + "ix.indpred IS NOT NULL, ix.indimmediate, ix.indisvalid, am.amname, "
                + "array_agg(a.attname ORDER BY keys.ord), bool_and(a.attname IS NOT NULL) "
                + "FROM pg_index ix JOIN pg_class t ON t.oid = ix.indrelid JOIN pg_namespace n ON n.oid = t.relnamespace "
                + "JOIN pg_class i ON i.oid = ix.indexrelid JOIN pg_am am ON am.oid = i.relam "
                + "LEFT JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS keys(attnum, ord) ON TRUE "
                + "LEFT JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = keys.attnum "
                + "WHERE n.nspname = current_schema() AND t.relname = " + quoteLiteral(vp.baseTable)
                + " GROUP BY ix.indexrelid, ix.indisprimary, ix.indisunique, ix.indexprs, ix.indpred, "
                + "ix.indimmediate, ix.indisvalid, am.amname";
        int copied = 0;
        List<String> names = targetColumns.stream().map(c -> c.name).collect(Collectors.toList());
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                String definition = rs.getString(1);
                String reason = null;
                if (definition == null || rs.getBoolean(2)) {
                    reason = "primary";
                } else if (rs.getBoolean(3)) {
                    reason = "unique";
                } else if (rs.getBoolean(4)) {
                    reason = "expression";
                } else if (rs.getBoolean(5)) {
                    reason = "partial";
                } else if (!rs.getBoolean(6) || !rs.getBoolean(7)) {
                    reason = "deferred-or-invalid";
                } else if (!"btree".equalsIgnoreCase(rs.getString(8))) {
                    reason = "access-method";
                } else if (!rs.getBoolean(10)) {
                    reason = "columns-not-in-target";
                } else if (!allColumnsPresent(rs.getArray(9), names)) {
                    reason = "columns-not-in-target";
                }
                if (reason != null) {
                    recordSchemaCopySkip("schema-copy-" + targetKind + "-index",
                            definition == null ? vp.baseTable : definition, reason);
                    continue;
                }
                String rewritten = rewritePostgresIndexForTarget(vp, definition, target, copied++);
                if (rewritten == null) {
                    recordSchemaCopySkip("schema-copy-" + targetKind + "-index", definition, "rewrite-failed");
                } else {
                    tryExecuteSchemaCopySetup(vp, "schema-copy-" + targetKind + "-index", rewritten);
                }
            }
        } catch (SQLException error) {
            recordSchemaCopySkip("schema-copy-" + targetKind + "-index-catalog", vp.baseTable, getErrorText(error));
        }
    }

    private String rewritePostgresIndexForTarget(VpSchema vp, String indexDef, String target, int ordinal) {
        String newName = quote(vp.prefix + target + "_idx_" + ordinal);
        return indexDef.replaceFirst(
                "(?i)^CREATE\\s+(UNIQUE\\s+)?INDEX\\s+(?:\"[^\"]+\"|\\S+)\\s+ON\\s+"
                        + "(?:ONLY\\s+)?(?:\"[^\"]+\"\\.)?(?:\"[^\"]+\"|\\S+)",
                "CREATE $1INDEX " + newName + " ON " + quote(target));
    }

    private void copyBaseTargetConstraints(VpSchema vp, String target, List<VpColumn> targetColumns,
            String targetKind) {
        String sql = "SELECT c.conname, c.contype, pg_get_constraintdef(c.oid), c.convalidated, c.condeferrable, "
                + "c.condeferred, array_agg(a.attname ORDER BY keys.ord), bool_and(a.attname IS NOT NULL) "
                + "FROM pg_constraint c LEFT JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS keys(attnum, ord) ON TRUE "
                + "LEFT JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = keys.attnum WHERE c.conrelid = "
                + quoteLiteral(vp.baseTable) + "::regclass AND c.contype IN ('c', 'u', 'x') "
                + "GROUP BY c.oid, c.conname, c.contype, c.convalidated, c.condeferrable, c.condeferred";
        List<String> names = targetColumns.stream().map(c -> c.name).collect(Collectors.toList());
        int ordinal = 0;
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                String definition = rs.getString(3);
                String kind = rs.getString(2).equals("c") ? "check"
                        : rs.getString(2).equals("u") ? "unique" : "exclusion";
                String reason = null;
                if (definition == null) {
                    reason = "missing-definition";
                } else if (!rs.getBoolean(4)) {
                    reason = "not-validated";
                } else if (rs.getBoolean(5) || rs.getBoolean(6)) {
                    reason = "deferrable";
                } else if (!rs.getBoolean(8) || !allColumnsPresent(rs.getArray(7), names)) {
                    reason = "columns-not-in-target";
                }
                if (reason != null) {
                    recordSchemaCopySkip("schema-copy-" + targetKind + "-" + kind,
                            definition == null ? vp.baseTable : definition, reason);
                    continue;
                }
                String name = quote(vp.prefix + targetKind + "_" + kind + "_" + ordinal++);
                tryExecuteSchemaCopySetup(vp, "schema-copy-" + targetKind + "-" + kind,
                        "ALTER TABLE " + quote(target) + " ADD CONSTRAINT " + name + " " + definition);
            }
        } catch (SQLException error) {
            recordSchemaCopySkip("schema-copy-" + targetKind + "-constraint-catalog", vp.baseTable,
                    getErrorText(error));
        }
    }

    private void copyBaseTargetStatistics(VpSchema vp, String target, List<VpColumn> targetColumns, String targetKind) {
        if (targetColumns.size() < 2) {
            return;
        }
        String sql = "SELECT array_to_string(s.stxkind, ','), string_agg(quote_ident(a.attname), ', ' ORDER BY keys.ord), "
                + "array_agg(a.attname ORDER BY keys.ord), bool_and(a.attname IS NOT NULL), s.stxexprs IS NULL "
                + "FROM pg_statistic_ext s LEFT JOIN LATERAL unnest(s.stxkeys::int2[]) WITH ORDINALITY AS keys(attnum, ord) ON TRUE "
                + "LEFT JOIN pg_attribute a ON a.attrelid = s.stxrelid AND a.attnum = keys.attnum WHERE s.stxrelid = "
                + quoteLiteral(vp.baseTable) + "::regclass GROUP BY s.oid, s.stxkind, s.stxexprs";
        List<String> names = targetColumns.stream().map(c -> c.name).collect(Collectors.toList());
        int ordinal = 0;
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                if (!rs.getBoolean(5)) {
                    recordSchemaCopySkip("schema-copy-" + targetKind + "-statistics", vp.baseTable,
                            "expression-statistics");
                    continue;
                }
                if (!rs.getBoolean(4) || !allColumnsPresent(rs.getArray(3), names)) {
                    recordSchemaCopySkip("schema-copy-" + targetKind + "-statistics", vp.baseTable,
                            "columns-not-in-target");
                    continue;
                }
                String kinds = statisticsKindsClause(rs.getString(1));
                if (kinds == null || rs.getString(2) == null) {
                    recordSchemaCopySkip("schema-copy-" + targetKind + "-statistics", vp.baseTable,
                            "unrecognized-definition");
                    continue;
                }
                String statement = "CREATE STATISTICS " + quote(vp.prefix + targetKind + "_stat_" + ordinal++) + kinds
                        + " ON " + rs.getString(2) + " FROM " + quote(target);
                tryExecuteSchemaCopySetup(vp, "schema-copy-" + targetKind + "-statistics", statement);
                tryExecuteSchemaCopySetup(vp, "schema-copy-" + targetKind + "-statistics-analyze",
                        "ANALYZE " + quote(target));
            }
        } catch (SQLException error) {
            recordSchemaCopySkip("schema-copy-" + targetKind + "-statistics-catalog", vp.baseTable,
                    getErrorText(error));
        }
    }

    private boolean tryExecuteSchemaCopySetup(VpSchema vp, String stage, String sql) {
        boolean success = false;
        try (Statement s = state.getConnection().createStatement()) {
            if (state.getOptions().printAllStatements()) {
                System.out.println(sql);
            }
            if (state.getOptions().logEachSelect()) {
                state.getLogger().writeCurrent(sql);
            }
            s.execute(sql);
            vp.setupStatements.add(sql);
            success = true;
            recordVpReproductionStatement(sql);
            return true;
        } catch (SQLException | AssertionError error) {
            if (isDatabaseInternalError(error)) {
                recordVpReproductionStatement(sql);
                throw new AssertionError("PostgreSQL internal error during " + stage, error);
            }
            recordSchemaCopySkip(stage, sql, getErrorText(error));
            return false;
        } finally {
            if (success) {
                Main.nrSuccessfulActions.addAndGet(1);
            } else {
                Main.nrUnsuccessfulActions.addAndGet(1);
            }
        }
    }

    private void copyBaseIndexesToSource(VpSchema vp) {
        String sql = "SELECT pg_get_indexdef(ix.indexrelid), ix.indisprimary, ix.indisunique, "
                + "ix.indexprs IS NOT NULL, ix.indpred IS NOT NULL, ix.indimmediate, ix.indisvalid, am.amname, "
                + "EXISTS (SELECT 1 FROM unnest(ix.indclass) c(opclassoid) JOIN pg_opclass op "
                + "ON op.oid = c.opclassoid WHERE NOT op.opcdefault) "
                + "FROM pg_index ix JOIN pg_class t ON t.oid = ix.indrelid JOIN pg_namespace n ON n.oid = t.relnamespace "
                + "JOIN pg_class i ON i.oid = ix.indexrelid JOIN pg_am am ON am.oid = i.relam "
                + "WHERE n.nspname = current_schema() AND t.relname = " + quoteLiteral(vp.baseTable);
        int copied = 0;
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                String indexDef = rs.getString(1);
                String skipReason = null;
                if (indexDef == null || rs.getBoolean(2)) {
                    skipReason = "primary";
                } else if (rs.getBoolean(3)) {
                    skipReason = "unique";
                } else if (rs.getBoolean(4)) {
                    skipReason = "expression";
                } else if (rs.getBoolean(5)) {
                    skipReason = "partial";
                } else if (!rs.getBoolean(6) || !rs.getBoolean(7)) {
                    skipReason = "deferred-or-invalid";
                } else if (!"btree".equalsIgnoreCase(rs.getString(8))) {
                    skipReason = "access-method";
                } else if (rs.getBoolean(9)) {
                    skipReason = "operator-class";
                }
                if (skipReason != null) {
                    recordSchemaCopySkip("schema-copy-index", indexDef == null ? vp.baseTable : indexDef, skipReason);
                    continue;
                }
                String rewritten = rewritePostgresIndexForSource(vp, indexDef, copied++);
                if (rewritten != null) {
                    tryExecuteSchemaCopySetup(vp, "schema-copy-index", rewritten);
                }
            }
        } catch (SQLException error) {
            recordSchemaCopySkip("schema-copy-index-catalog", vp.baseTable, getErrorText(error));
        }
    }

    private String rewritePostgresIndexForSource(VpSchema vp, String indexDef, int ordinal) {
        String newName = quote(vp.prefix + "src_idx_" + ordinal);
        return indexDef.replaceFirst(
                "(?i)^CREATE\\s+(UNIQUE\\s+)?INDEX\\s+(?:\"[^\"]+\"|\\S+)\\s+ON\\s+"
                        + "(?:ONLY\\s+)?(?:\"[^\"]+\"\\.)?(?:\"[^\"]+\"|\\S+)",
                "CREATE $1INDEX " + newName + " ON " + quote(vp.sourceTable));
    }

    private void copyBaseCheckConstraintsToSource(VpSchema vp) {
        String sql = "SELECT conname, pg_get_constraintdef(oid), convalidated FROM pg_constraint WHERE conrelid = "
                + quoteLiteral(vp.baseTable) + "::regclass AND contype = 'c'";
        int copied = 0;
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                String constraintDef = rs.getString(2);
                if (constraintDef == null || !rs.getBoolean(3)) {
                    recordSchemaCopySkip("schema-copy-check", vp.baseTable,
                            constraintDef == null ? "missing-definition" : "not-validated");
                    continue;
                }
                String name = quote(vp.prefix + "src_chk_" + copied++);
                tryExecuteSchemaCopySetup(vp, "schema-copy-check",
                        "ALTER TABLE " + quote(vp.sourceTable) + " ADD CONSTRAINT " + name + " " + constraintDef);
            }
        } catch (SQLException error) {
            recordSchemaCopySkip("schema-copy-check-catalog", vp.baseTable, getErrorText(error));
        }
    }

    private void copyBaseNonPrimaryConstraintsToSource(VpSchema vp) {
        String sql = "SELECT c.conname, c.contype, pg_get_constraintdef(c.oid), c.convalidated, c.condeferrable, "
                + "c.condeferred, array_agg(a.attname ORDER BY keys.ord), bool_and(a.attname IS NOT NULL) "
                + "FROM pg_constraint c LEFT JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS keys(attnum, ord) "
                + "ON TRUE LEFT JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = keys.attnum "
                + "WHERE c.conrelid = " + quoteLiteral(vp.baseTable) + "::regclass AND c.contype IN ('u', 'x') "
                + "GROUP BY c.oid, c.conname, c.contype, c.convalidated, c.condeferrable, c.condeferred";
        int copied = 0;
        List<String> helperColumns = vp.columns.stream().map(c -> c.name).collect(Collectors.toList());
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                String constraintDef = rs.getString(3);
                String kind = "u".equals(rs.getString(2)) ? "unique" : "exclusion";
                String skipReason = null;
                if (constraintDef == null) {
                    skipReason = "missing-definition";
                } else if (!rs.getBoolean(4)) {
                    skipReason = "not-validated";
                } else if (rs.getBoolean(5) || rs.getBoolean(6)) {
                    skipReason = "deferrable";
                } else if (!rs.getBoolean(8)) {
                    skipReason = "expression-or-system-column";
                } else if (!allColumnsPresent(rs.getArray(7), helperColumns)) {
                    skipReason = "columns-not-in-helper-source";
                }
                if (skipReason != null) {
                    recordSchemaCopySkip("schema-copy-" + kind, constraintDef == null ? vp.baseTable : constraintDef,
                            skipReason);
                    continue;
                }
                String name = quote(vp.prefix + "src_" + kind + "_" + copied++);
                String copySql = "ALTER TABLE " + quote(vp.sourceTable) + " ADD CONSTRAINT " + name + " "
                        + constraintDef;
                tryExecuteSchemaCopySetup(vp, "schema-copy-" + kind, copySql);
            }
        } catch (SQLException error) {
            recordSchemaCopySkip("schema-copy-constraint-catalog", vp.baseTable, getErrorText(error));
        }
    }

    private void copyBaseExtendedStatisticsToSource(VpSchema vp) {
        String sql = "SELECT pg_get_statisticsobjdef(s.oid), s.stxexprs IS NULL, array_to_string(s.stxkind, ','), "
                + "string_agg(quote_ident(a.attname), ', ' ORDER BY keys.ord), array_agg(a.attname ORDER BY keys.ord), "
                + "bool_and(a.attname IS NOT NULL) "
                + "FROM pg_statistic_ext s LEFT JOIN LATERAL unnest(s.stxkeys::int2[]) WITH ORDINALITY "
                + "AS keys(attnum, ord) ON TRUE LEFT JOIN pg_attribute a ON a.attrelid = s.stxrelid "
                + "AND a.attnum = keys.attnum WHERE s.stxrelid = " + quoteLiteral(vp.baseTable)
                + "::regclass GROUP BY s.oid, s.stxexprs, s.stxkind";
        int copied = 0;
        boolean createdStatistics = false;
        List<String> helperColumns = vp.columns.stream().map(c -> c.name).collect(Collectors.toList());
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                String statisticsDef = rs.getString(1);
                if (statisticsDef == null) {
                    recordSchemaCopySkip("schema-copy-statistics", vp.baseTable, "missing-definition");
                    continue;
                }
                if (!rs.getBoolean(2)) {
                    recordSchemaCopySkip("schema-copy-statistics", statisticsDef, "expression-statistics");
                    continue;
                }
                if (!rs.getBoolean(6)) {
                    recordSchemaCopySkip("schema-copy-statistics", statisticsDef, "unknown-column");
                    continue;
                }
                if (!allColumnsPresent(rs.getArray(5), helperColumns)) {
                    recordSchemaCopySkip("schema-copy-statistics", statisticsDef, "columns-not-in-helper-source");
                    continue;
                }
                String rewritten = createPostgresStatisticsForSource(vp, rs.getString(3), rs.getString(4), copied++);
                if (rewritten == null) {
                    recordSchemaCopySkip("schema-copy-statistics", statisticsDef, "unrecognized-definition");
                    continue;
                }
                try {
                    createdStatistics |= tryExecuteSchemaCopySetup(vp, "schema-copy-statistics", rewritten);
                } catch (AssertionError error) {
                    recordSchemaCopySkip("schema-copy-statistics", rewritten, getErrorText(error));
                }
            }
        } catch (SQLException error) {
            recordSchemaCopySkip("schema-copy-statistics-catalog", vp.baseTable, getErrorText(error));
        }
        if (createdStatistics) {
            tryExecuteSchemaCopySetup(vp, "schema-copy-statistics-analyze", "ANALYZE " + quote(vp.sourceTable));
        }
    }

    private boolean allColumnsPresent(Array columnArray, List<String> helperColumns) throws SQLException {
        if (columnArray == null) {
            return false;
        }
        Object rawArray = columnArray.getArray();
        if (!(rawArray instanceof Object[])) {
            return false;
        }
        Object[] columns = (Object[]) rawArray;
        if (columns.length == 0) {
            return false;
        }
        for (Object column : columns) {
            if (!(column instanceof String) || !helperColumns.contains(column)) {
                return false;
            }
        }
        return true;
    }

    private String createPostgresStatisticsForSource(VpSchema vp, String rawKinds, String quotedColumns, int ordinal) {
        String kinds = statisticsKindsClause(rawKinds);
        if (kinds == null || quotedColumns == null || quotedColumns.isBlank()) {
            return null;
        }
        return "CREATE STATISTICS " + quote(vp.prefix + "src_stat_" + ordinal) + kinds + " ON " + quotedColumns
                + " FROM " + quote(vp.sourceTable);
    }

    private String statisticsKindsClause(String rawKinds) {
        if (rawKinds == null || rawKinds.isBlank()) {
            return "";
        }
        List<String> kinds = new ArrayList<>();
        for (String rawKind : rawKinds.split(",")) {
            switch (rawKind) {
            case "d":
                kinds.add("ndistinct");
                break;
            case "f":
                kinds.add("dependencies");
                break;
            case "m":
                kinds.add("mcv");
                break;
            default:
                return null;
            }
        }
        return kinds.isEmpty() ? "" : " (" + String.join(", ", kinds) + ")";
    }

    private void recordBaseForeignKeyTopology(VpSchema vp) {
        String sql = "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = "
                + quoteLiteral(vp.baseTable) + "::regclass AND contype = 'f'";
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                recordSchemaCopySkip("schema-copy-foreign-key", rs.getString(1),
                        "isolated-helper-source; local-vp-rowid-topology");
            }
        } catch (SQLException error) {
            recordSchemaCopySkip("schema-copy-foreign-key-catalog", vp.baseTable, getErrorText(error));
        }
    }

    private VpQueryPair generateRandomRewritePair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        if (strategy == ReconstructionStrategy.DIRECTED_MATERIALIZED_JOIN) {
            return generateDirectedMaterializedJoinPair(vp);
        }
        if (strategy == ReconstructionStrategy.DIRECTED_PARTITION_JOIN) {
            return generateDirectedPartitionJoinPair(vp);
        }
        if (vp.deepCollationColumn != null) {
            return generateDeepCollationPair(vp, strategy);
        }
        SourceAst sourceAst = createSourceAst(vp);
        /*
         * The generic expression generator is only type-aware at the target expression level. It can still combine two
         * columns of unrelated PostgreSQL types while building predicates (for example daterange versus cidr). Those
         * columns have dedicated probes below; keep them out of the generic pool so an invalid generated statement does
         * not consume a VP comparison.
         */
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(state)
                .setColumns(genericExpressionColumns(sourceAst));
        if (advancedFeaturesEnabled() && hasJsonbColumn(vp) && useAdvancedFeature()) {
            return generateJsonbColumnPair(vp, strategy);
        }
        if (advancedFeaturesEnabled() && (hasArrayColumn(vp) || hasRangeColumn(vp)) && useAdvancedFeature()) {
            if (hasArrayColumn(vp) && (!hasRangeColumn(vp) || Randomly.getBoolean())) {
                return generateLateralUnnestPair(vp, strategy);
            }
            if (hasRangeColumn(vp)) {
                return generateRangeOperatorPair(vp, strategy);
            }
        }
        if (advancedFeaturesEnabled() && useAdvancedFeature()) {
            if (hasJsonbColumn(vp) && Randomly.getBoolean()) {
                return Randomly.getBoolean() ? generateJsonbLateralRecordPair(vp, strategy)
                        : generateJsonbLateralEachPair(vp, strategy);
            }
            return generateGenerateSeriesLateralPair(vp, strategy);
        }
        if (advancedFeaturesEnabled() && useAdvancedFeature()) {
            return generateCorrelatedSubqueryPair(vp, strategy, sourceAst);
        }
        PostgresSelect select = new PostgresSelect();
        boolean useAggregate = Randomly.getBoolean();
        boolean useDistinctOn = advancedFeaturesEnabled() && !useAggregate && useAdvancedFeature();
        select.setSelectOption(useDistinctOn || Randomly.getBoolean() ? PostgresSelect.SelectType.DISTINCT
                : PostgresSelect.SelectType.ALL);
        int fetchColumns = Math.max(1, Randomly.smallNumber() + 1);
        boolean hasWindowExpression = false;
        boolean useSyntheticRollup = false;
        boolean useNativeRollup = false;
        boolean useGroupingSetOrCube = false;
        boolean useUnionAll = false;
        boolean unionAllBranchUsesWhere = false;
        List<PostgresExpression> selected = new ArrayList<>();
        List<PostgresExpression> nonAggregateExpressions = new ArrayList<>();
        List<PostgresExpression> aggregateExpressions = new ArrayList<>();
        List<PostgresExpression> literalExpressions = new ArrayList<>();
        for (int i = 0; i < fetchColumns; i++) {
            PostgresExpression expr;
            if (!useAggregate && !useDistinctOn && advancedFeaturesEnabled() && useAdvancedFeature()) {
                expr = generateAdvancedSelectExpression(sourceAst, gen);
                hasWindowExpression = true;
            } else if (useAggregate && Randomly.getBoolean()) {
                expr = generateVpAggregate(sourceAst.columns, effectiveSourceRowCount(vp));
                aggregateExpressions.add(expr);
            } else {
                expr = randomExpression(gen);
                if (isLiteralExpression(expr)) {
                    literalExpressions.add(expr);
                } else {
                    nonAggregateExpressions.add(expr);
                }
            }
            selected.add(expr);
        }
        if (selected.isEmpty()) {
            PostgresExpression expr = new PostgresColumnReference(Randomly.fromList(sourceAst.columns));
            selected.add(expr);
            nonAggregateExpressions.add(expr);
        }
        select.setFromList(List.of(new PostgresTableReference(sourceAst.table)));
        if (!useNativeRollup && Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression(0, PostgresDataType.BOOLEAN));
        }
        if (!useDistinctOn
                && (useAggregate || (!hasWindowExpression && Randomly.getBooleanWithRatherLowProbability()))) {
            List<PostgresExpression> groupByExpressions = freezeExpressions(
                    chooseGroupByExpressions(nonAggregateExpressions));
            select.setGroupByExpressions(groupByExpressions);
            selected = buildGroupedFetchColumns(groupByExpressions, aggregateExpressions, literalExpressions);
            if (useAggregate && groupByExpressions.isEmpty() && aggregateExpressions.isEmpty()) {
                selected.add(freezeExpression(generateVpAggregate(sourceAst.columns, effectiveSourceRowCount(vp))));
            }
            if (selected.isEmpty()) {
                if (!groupByExpressions.isEmpty()) {
                    selected.add(Randomly.fromList(groupByExpressions));
                } else {
                    selected.add(freezeExpression(generateVpAggregate(sourceAst.columns, effectiveSourceRowCount(vp))));
                }
            }
            if (Randomly.getBoolean() && !groupByExpressions.isEmpty()) {
                select.setHavingClause(generateGroupByHavingExpression(groupByExpressions, sourceAst.columns));
            }
            if (advancedFeaturesEnabled() && !hasWindowExpression && !groupByExpressions.isEmpty()
                    && useAdvancedFeature()) {
                recordAdvancedFeature(AdvancedFeature.ROLLUP);
                select.setGroupByExpressions(List.of(new PostgresText("ROLLUP ("
                        + groupByExpressions.stream().map(PostgresVisitor::asString).collect(Collectors.joining(", "))
                        + ")")));
                useNativeRollup = true;
            } else if (advancedFeaturesEnabled() && !hasWindowExpression && !groupByExpressions.isEmpty()
                    && useAdvancedFeature()) {
                boolean cube = Randomly.getBoolean();
                recordAdvancedFeature(cube ? AdvancedFeature.CUBE : AdvancedFeature.GROUPING_SETS);
                String groupingExpressions = groupByExpressions.stream().map(PostgresVisitor::asString)
                        .collect(Collectors.joining(", "));
                boolean groupingDistinct = supportsPostgresAtLeast(16) && !cube && useAdvancedFeature();
                String groupingClause;
                if (groupingDistinct) {
                    // PostgreSQL 16 de-duplicates rows produced by repeated grouping
                    // sets. Duplicating the same rollup keeps the query deterministic
                    // while exercising the DISTINCT grouping-set implementation.
                    groupingClause = "DISTINCT ROLLUP (" + groupingExpressions + "), ROLLUP (" + groupingExpressions
                            + ")";
                    recordAdvancedFeature(AdvancedFeature.GROUP_BY_DISTINCT);
                } else {
                    groupingClause = cube ? "CUBE (" + groupingExpressions + ")"
                            : "GROUPING SETS ((" + groupingExpressions + "), ())";
                }
                select.setGroupByExpressions(List.of(new PostgresText(groupingClause)));
                select.clearHavingClause();
                useGroupingSetOrCube = true;
            }
        } else if (isDistinct(select) && !useDistinctOn) {
            selected = freezeExpressions(selected);
        } else if (Randomly.getBooleanWithRatherLowProbability()) {
            selected = freezeExpressions(selected);
        }
        // PostgreSQL json has no equality operator, so SELECT DISTINCT over json_agg
        // is invalid even though the aggregate itself is valid. Check after grouped
        // fetch construction, where generated aggregate expressions enter selected.
        if (containsJsonAggregate(selected)) {
            select.setSelectOption(PostgresSelect.SelectType.ALL);
        }
        if (useDistinctOn) {
            PostgresColumn distinctColumn = Randomly.fromList(sourceAst.columns);
            PostgresExpression distinctExpression = new PostgresColumnReference(distinctColumn);
            String distinctSql = PostgresVisitor.asString(distinctExpression);
            if (selected.stream().noneMatch(e -> PostgresVisitor.asString(e).equals(distinctSql))) {
                selected.add(0, distinctExpression);
            } else {
                selected = selected.stream().sorted((left, right) -> {
                    boolean leftMatches = PostgresVisitor.asString(left).equals(distinctSql);
                    boolean rightMatches = PostgresVisitor.asString(right).equals(distinctSql);
                    return Boolean.compare(rightMatches, leftMatches);
                }).collect(Collectors.toCollection(ArrayList::new));
            }
            select.setSelectOption(PostgresSelect.SelectType.DISTINCT);
            select.setDistinctOnClause(distinctExpression);
            // DISTINCT ON without ORDER BY is intentionally nondeterministic:
            // source and split relations may choose different physical rows
            // for a duplicate key. Make the representative row part of the
            // query contract while retaining the PostgreSQL DISTINCT ON path.
            select.setOrderByClauses(List.of(distinctExpression, new PostgresColumnReference(sourceAst.rowIdColumn)));
            recordAdvancedFeature(AdvancedFeature.DISTINCT_ON);
        }
        select.setFetchColumns(selected);
        boolean orderedComparisonEnabled = isRowPreservingQuery(useAggregate, select)
                && hasVpRowIdTieBreaker(select, sourceAst.rowIdColumn);
        if (useNativeRollup || useGroupingSetOrCube || useDistinctOn) {
            orderedComparisonEnabled = false;
        }
        if (!useNativeRollup && !useGroupingSetOrCube && Randomly.getBoolean()) {
            select.setOrderByClauses(chooseLegalOrderBys(select, nonAggregateExpressions, sourceAst.rowIdColumn,
                    isRowPreservingQuery(useAggregate, select)));
            orderedComparisonEnabled = isRowPreservingQuery(useAggregate, select)
                    && hasVpRowIdTieBreaker(select, sourceAst.rowIdColumn);
        }
        if (!useNativeRollup && !useGroupingSetOrCube && Randomly.getBoolean()) {
            boolean rowPreservingQuery = isRowPreservingQuery(useAggregate, select);
            List<PostgresExpression> stableOrderBys = chooseStableLimitOrderBys(select, sourceAst.rowIdColumn,
                    rowPreservingQuery, useAggregate);
            if (!stableOrderBys.isEmpty() || isSingleRowAggregate(useAggregate, select)) {
                select.setOrderByClauses(stableOrderBys);
                orderedComparisonEnabled = rowPreservingQuery && hasVpRowIdTieBreaker(select, sourceAst.rowIdColumn);
                select.setLimitClause(PostgresConstant.createIntConstant(Randomly.getNotCachedInteger(0, 25)));
                if (Randomly.getBooleanWithRatherLowProbability()) {
                    select.setOffsetClause(PostgresConstant.createIntConstant(Randomly.getNotCachedInteger(0, 5)));
                }
            }
        }
        if (useNativeRollup || useGroupingSetOrCube || useDistinctOn) {
            orderedComparisonEnabled = false;
        }
        List<ApproximateKind> forcedApproximateColumns = approximateColumnsForSelect(selected);
        String sourceRelation = sourceRelationForStrategy(vp, strategy);
        String singleQuery = renderSelectWithRelation(select, sourceRelation);
        if (useSyntheticRollup && select.getLimitClause() == null && select.getOffsetClause() == null) {
            singleQuery = "(" + singleQuery + ") UNION ALL (" + createSyntheticRollupBranches(sourceAst, selected,
                    select.getGroupByExpressions(), select.getWhereClause(), sourceRelation) + ")";
            orderedComparisonEnabled = false;
        }
        if (advancedFeaturesEnabled() && !useAggregate && select.getOrderByClauses().isEmpty()
                && select.getLimitClause() == null && select.getOffsetClause() == null
                && strategy != ReconstructionStrategy.TEMP_TABLE && useAdvancedFeature()) {
            recordAdvancedFeature(AdvancedFeature.UNION_ALL);
            useUnionAll = true;
            unionAllBranchUsesWhere = Randomly.getBoolean();
            singleQuery = "(" + singleQuery + ") UNION ALL ("
                    + createUnionAllBranch(sourceAst, selected, select, sourceRelation, unionAllBranchUsesWhere) + ")";
            orderedComparisonEnabled = false;
        }
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        String splitQuery = renderSelectWithRelation(select, relation);
        if (useSyntheticRollup && select.getLimitClause() == null && select.getOffsetClause() == null) {
            splitQuery = "(" + splitQuery + ") UNION ALL (" + createSyntheticRollupBranches(sourceAst, selected,
                    select.getGroupByExpressions(), select.getWhereClause(), relation) + ")";
        }
        if (useUnionAll) {
            splitQuery = "(" + splitQuery + ") UNION ALL ("
                    + createUnionAllBranch(sourceAst, selected, select, relation, unionAllBranchUsesWhere) + ")";
        }
        return new VpQueryPair(vp, strategy, singleQuery, splitQuery, extraSetup,
                "shape=" + vp.baseShape + ", split=" + vp.splitStrategy + ", reconstruction=" + strategy,
                orderedComparisonEnabled, forcedApproximateColumns, false);
    }

    private VpQueryPair generateDeepCollationPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        String alias = quote(vp.sourceTable);
        String qualifiedColumn = alias + "." + quote(vp.deepCollationColumn);
        int variant = (int) Randomly.getNotCachedInteger(0, 5);
        String sourceCollation = vp.sourceColumnCollationClauses.getOrDefault(vp.deepCollationColumn, "");
        String splitCollation = vp.leftColumnCollationClauses.getOrDefault(vp.deepCollationColumn, "");
        String singleQuery = deepCollationQueryForRelation(variant, qualifiedColumn, sourceCollation,
                sourceRelationForStrategy(vp, strategy));
        List<String> extraSetup = new ArrayList<>();
        String reconstruction = buildReconstructionRelation(vp, strategy, extraSetup);
        String splitQuery = deepCollationQueryForRelation(variant, qualifiedColumn, splitCollation, reconstruction);
        return new VpQueryPair(
                vp, strategy, singleQuery, splitQuery, extraSetup, "shape=" + vp.baseShape + ", split="
                        + vp.splitStrategy + ", reconstruction=" + strategy + ", deep=collation, variant=" + variant,
                false, List.of(), true);
    }

    private String deepCollationQueryForRelation(int variant, String qualifiedColumn, String collationClause,
            String relation) {
        String coercedColumn = "(" + qualifiedColumn + collationClause + ")";
        switch (variant) {
        case 0:
            return "SELECT " + coercedColumn + " AS value, (" + coercedColumn
                    + " IS DISTINCT FROM NULL) AS distinct_from_null, (" + coercedColumn + " IS NOT NULL) AS matches"
                    + " FROM " + relation;
        case 1:
            return "SELECT " + coercedColumn + " AS value FROM " + relation;
        case 2:
            return "SELECT (" + coercedColumn + " IS NOT NULL) AS matches, COUNT(*) FROM " + relation + " GROUP BY 1";
        case 3:
            return "SELECT " + coercedColumn + " AS value, (" + coercedColumn + " IS NULL) AS is_null FROM " + relation;
        case 4:
            return "SELECT (" + coercedColumn + " IS NULL) AS is_null, octet_length(" + coercedColumn
                    + ") AS byte_length FROM " + relation;
        default:
            throw new AssertionError(variant);
        }
    }

    private List<ApproximateKind> approximateColumnsForSelect(List<PostgresExpression> selected) {
        List<ApproximateKind> approximateColumns = new ArrayList<>();
        for (PostgresExpression expression : selected) {
            approximateColumns.add(
                    isApproximateAggregateExpression(expression) ? ApproximateKind.AGGREGATE : ApproximateKind.NONE);
        }
        return approximateColumns;
    }

    private boolean isApproximateAggregateExpression(PostgresExpression expression) {
        String normalized = PostgresVisitor.asString(expression).stripLeading().toUpperCase(Locale.ROOT);
        return startsWithAny(normalized, APPROXIMATE_AGGREGATE_PREFIXES);
    }

    private String createSyntheticRollupBranches(SourceAst sourceAst, List<PostgresExpression> selected,
            List<PostgresExpression> groupByExpressions, PostgresExpression whereClause, String relation) {
        List<String> branches = new ArrayList<>();
        for (int prefixLength = groupByExpressions.size() - 1; prefixLength >= 0; prefixLength--) {
            branches.add(createSyntheticRollupBranch(sourceAst, selected, groupByExpressions, prefixLength, whereClause,
                    relation));
        }
        return branches.stream().map(branch -> "(" + branch + ")").collect(Collectors.joining(" UNION ALL "));
    }

    private String createSyntheticRollupBranch(SourceAst sourceAst, List<PostgresExpression> selected,
            List<PostgresExpression> groupByExpressions, int prefixLength, PostgresExpression whereClause,
            String relation) {
        List<String> retainedGroupKeys = groupByExpressions.subList(0, prefixLength).stream()
                .map(PostgresVisitor::asString).collect(Collectors.toList());
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < selected.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(syntheticRollupExpression(selected.get(i), retainedGroupKeys));
        }
        sb.append(" FROM ");
        sb.append(relation);
        if (whereClause != null) {
            sb.append(" WHERE ");
            sb.append(PostgresVisitor.asString(whereClause));
        }
        if (!retainedGroupKeys.isEmpty()) {
            sb.append(" GROUP BY ");
            sb.append(String.join(", ", retainedGroupKeys));
        }
        return sb.toString();
    }

    private String syntheticRollupExpression(PostgresExpression expression, List<String> retainedGroupKeys) {
        String sql = PostgresVisitor.asString(expression);
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

    private boolean isAggregateExpression(PostgresExpression expression) {
        if (expression instanceof PostgresAggregate) {
            return true;
        }
        String sql = PostgresVisitor.asString(expression);
        String normalized = sql.stripLeading().toUpperCase(Locale.ROOT);
        return normalized.startsWith("COUNT(") || normalized.startsWith("COUNT_ROWS(") || normalized.startsWith("SUM(")
                || normalized.startsWith("SUM_INT(") || startsWithAny(normalized, APPROXIMATE_AGGREGATE_PREFIXES)
                || normalized.startsWith("MIN(") || normalized.startsWith("MAX(") || normalized.startsWith("BIT_AND(")
                || normalized.startsWith("BIT_OR(") || normalized.startsWith("XOR_AGG(")
                || normalized.startsWith("BOOL_AND(") || normalized.startsWith("BOOL_OR(")
                || normalized.startsWith("STRING_AGG(") || normalized.startsWith("CONCAT_AGG(")
                || normalized.startsWith("PERCENTILE_DISC(") || normalized.startsWith("MODE()")
                || normalized.startsWith("ARRAY_AGG(") || startsWithAny(normalized, JSON_AGGREGATE_PREFIXES);
    }

    private boolean containsJsonAggregate(List<PostgresExpression> expressions) {
        return expressions.stream().map(PostgresVisitor::asString)
                .map(sql -> sql.stripLeading().toUpperCase(Locale.ROOT))
                .anyMatch(sql -> startsWithAny(sql, JSON_AGGREGATE_PREFIXES_WITHOUT_EQUALITY));
    }

    private static boolean startsWithAny(String sql, List<String> prefixes) {
        return prefixes.stream().anyMatch(sql::startsWith);
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
        int variant = (int) Randomly.getNotCachedInteger(0, 3);
        String predicateSource = variant == 2 ? minMaxInputForSQL(sourceAst.columns)
                : simpleColumnOrRowIdForSQL(sourceAst.columns);
        String predicateColumn = qualifySelectedExpression(predicateSource, subAlias);
        String singleQuery = renderCorrelatedSubquery(variant, outerColumn, predicateColumn,
                relationWithAlias(sourceRelationForStrategy(vp, strategy), vp.sourceTable, sourceAlias),
                relationWithAlias(sourceRelationForStrategy(vp, strategy), vp.sourceTable, subAlias), sourceAlias,
                subAlias);
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        String splitQuery = renderCorrelatedSubquery(variant, outerColumn, predicateColumn,
                relationWithAlias(relation, vp.sourceTable, sourceAlias),
                relationWithAlias(relation, vp.sourceTable, subAlias), sourceAlias, subAlias);
        return new VpQueryPair(
                vp, strategy, singleQuery, splitQuery, extraSetup, "shape=" + vp.baseShape + ", split="
                        + vp.splitStrategy + ", reconstruction=" + strategy + ", advanced=correlated-subquery",
                false, List.of(), false);
    }

    private String renderCorrelatedSubquery(int variant, String outerColumn, String predicateColumn,
            String outerRelation, String subqueryRelation, String outerAlias, String subqueryAlias) {
        switch (variant) {
        case 0:
            return "SELECT " + outerColumn + " FROM " + outerRelation + " WHERE EXISTS (SELECT 1 FROM "
                    + subqueryRelation + " WHERE " + subqueryAlias + "." + quote(ROW_ID) + " <= " + outerAlias + "."
                    + quote(ROW_ID) + " AND " + predicateColumn + " IS NOT NULL)";
        case 1:
            return "SELECT " + outerColumn + " FROM " + outerRelation + " WHERE " + outerAlias + "." + quote(ROW_ID)
                    + " IN (SELECT " + subqueryAlias + "." + quote(ROW_ID) + " FROM " + subqueryRelation + " WHERE "
                    + subqueryAlias + "." + quote(ROW_ID) + " <= " + outerAlias + "." + quote(ROW_ID) + " AND "
                    + predicateColumn + " IS NOT NULL)";
        case 2:
            return "SELECT " + outerColumn + ", (SELECT MIN(" + predicateColumn + ") FROM " + subqueryRelation
                    + " WHERE " + subqueryAlias + "." + quote(ROW_ID) + " <= " + outerAlias + "." + quote(ROW_ID)
                    + ") FROM " + outerRelation;
        default:
            throw new AssertionError(variant);
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

    private boolean isLiteralExpression(PostgresExpression expression) {
        return expression instanceof PostgresConstant;
    }

    private PostgresExpression freezeExpression(PostgresExpression expression) {
        if (isLiteralExpression(expression)) {
            return expression;
        }
        if (expression instanceof PostgresColumnReference) {
            return expression;
        }
        if (expression instanceof PostgresText) {
            return expression;
        }
        return new PostgresText(PostgresVisitor.asString(expression));
    }

    private List<PostgresExpression> freezeExpressions(List<PostgresExpression> expressions) {
        return expressions.stream().map(this::freezeExpression).collect(Collectors.toCollection(ArrayList::new));
    }

    private List<PostgresExpression> buildGroupedFetchColumns(List<PostgresExpression> groupByExpressions,
            List<PostgresExpression> aggregateExpressions, List<PostgresExpression> literalExpressions) {
        List<PostgresExpression> grouped = new ArrayList<>();
        grouped.addAll(groupByExpressions);
        grouped.addAll(freezeExpressions(aggregateExpressions));
        grouped.addAll(literalExpressions);
        return grouped;
    }

    private PostgresExpression randomExpression(PostgresExpressionGenerator gen) {
        return gen.generateExpression(PostgresDataType.getRandomType());
    }

    private List<PostgresExpression> chooseGroupByExpressions(List<PostgresExpression> nonAggregateExpressions) {
        List<PostgresExpression> groupable = nonAggregateExpressions.stream().filter(e -> !isLiteralExpression(e))
                .collect(Collectors.toList());
        if (!advancedFeaturesEnabled() || groupable.size() <= 1) {
            return groupable;
        }
        Collections.shuffle(groupable);
        return groupable.subList(0, state.getRandomly().getInteger(1, Math.min(3, groupable.size()) + 1));
    }

    private List<PostgresExpression> chooseLegalOrderBys(PostgresSelect select,
            List<PostgresExpression> nonAggregateExpressions, PostgresColumn rowIdColumn, boolean rowPreservingQuery) {
        if (!select.getGroupByExpressions().isEmpty()) {
            return addExplicitNullOrdering(stableGroupByOrderBys(select), rowIdColumn, false);
        }
        if (isDistinct(select)) {
            return addExplicitNullOrdering(stableSelectedOrderBys(select), rowIdColumn, false);
        }
        if (rowPreservingQuery) {
            List<PostgresExpression> orderBys = nonAggregateExpressions.stream().filter(e -> !isLiteralExpression(e))
                    .collect(Collectors.toCollection(ArrayList::new));
            if (orderBys.isEmpty()) {
                orderBys.add(new PostgresColumnReference(rowIdColumn));
            }
            appendVpRowIdTieBreaker(orderBys, rowIdColumn);
            return addExplicitNullOrdering(orderBys, rowIdColumn, true);
        }
        return List.of();
    }

    private List<PostgresExpression> chooseStableLimitOrderBys(PostgresSelect select, PostgresColumn rowIdColumn,
            boolean rowPreservingQuery, boolean useAggregate) {
        if (isSingleRowAggregate(useAggregate, select)) {
            return List.of();
        }
        if (!select.getGroupByExpressions().isEmpty()) {
            return addExplicitNullOrdering(stableGroupByOrderBys(select), rowIdColumn, false);
        }
        if (isDistinct(select)) {
            return addExplicitNullOrdering(stableSelectedOrderBys(select), rowIdColumn, false);
        }
        if (rowPreservingQuery) {
            List<PostgresExpression> orderBys = new ArrayList<>(select.getOrderByClauses());
            if (orderBys.isEmpty()) {
                orderBys.add(new PostgresColumnReference(rowIdColumn));
            }
            appendVpRowIdTieBreaker(orderBys, rowIdColumn);
            return addExplicitNullOrdering(orderBys, rowIdColumn, true);
        }
        return List.of();
    }

    private List<PostgresExpression> stableSelectedOrderBys(PostgresSelect select) {
        List<PostgresExpression> orderBys = new ArrayList<>();
        List<PostgresExpression> fetchColumns = select.getFetchColumns();
        for (int i = 0; i < fetchColumns.size(); i++) {
            orderBys.add(new PostgresText(Integer.toString(i + 1)));
        }
        return orderBys;
    }

    private List<PostgresExpression> stableGroupByOrderBys(PostgresSelect select) {
        return select.getGroupByExpressions().stream().filter(e -> !isLiteralExpression(e))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private void appendVpRowIdTieBreaker(List<PostgresExpression> orderBys, PostgresColumn rowIdColumn) {
        if (!hasVpRowIdTieBreaker(orderBys, rowIdColumn)) {
            orderBys.add(new PostgresColumnReference(rowIdColumn));
        }
    }

    private List<PostgresExpression> addExplicitNullOrdering(List<PostgresExpression> orderBys,
            PostgresColumn rowIdColumn, boolean keepRowIdLast) {
        if (!advancedFeaturesEnabled() || orderBys.isEmpty() || !Randomly.getBoolean()) {
            return orderBys;
        }
        List<PostgresExpression> ordered = new ArrayList<>();
        boolean added = false;
        for (PostgresExpression expression : orderBys) {
            String rendered = PostgresVisitor.asString(expression);
            String normalized = rendered.toUpperCase(Locale.ROOT).stripTrailing();
            if (normalized.endsWith(" NULLS FIRST") || normalized.endsWith(" NULLS LAST")) {
                ordered.add(expression);
                added = true;
                continue;
            }
            boolean isRowId = expression instanceof PostgresColumnReference
                    && ((PostgresColumnReference) expression).getColumn().getName().equals(rowIdColumn.getName());
            if (keepRowIdLast && isRowId) {
                continue;
            }
            String nulls = Randomly.getBoolean() ? " NULLS FIRST" : " NULLS LAST";
            String orderExpression = rendered.matches("[0-9]+") ? rendered : "(" + rendered + ")";
            ordered.add(new PostgresText(orderExpression + nulls));
            added = true;
        }
        if (keepRowIdLast) {
            ordered.add(new PostgresColumnReference(rowIdColumn));
        }
        if (added) {
            recordAdvancedFeature(AdvancedFeature.EXPLICIT_NULL_ORDERING);
            return ordered;
        }
        return orderBys;
    }

    private PostgresExpression generateGroupByHavingExpression(List<PostgresExpression> groupByExpressions,
            List<PostgresColumn> columns) {
        List<String> predicates = new ArrayList<>();
        predicates.add("COUNT(*) > 0");
        predicates.add("SUM(" + numericExpressionForSQL(columns) + ") IS NOT NULL");
        String minMaxInput = minMaxInputForSQL(columns);
        predicates.add("MIN(" + minMaxInput + ") IS NOT DISTINCT FROM MAX(" + minMaxInput + ")");
        for (PostgresExpression groupByExpression : groupByExpressions) {
            predicates.add("(" + PostgresVisitor.asString(groupByExpression) + ") IS NOT NULL");
        }
        return new PostgresText(Randomly.fromList(predicates));
    }

    private PostgresExpression generateAdvancedSelectExpression(SourceAst sourceAst, PostgresExpressionGenerator gen) {
        String order = simpleOrderExpressionForSQL(sourceAst.columns) + ", " + quote(ROW_ID);
        String value = simpleColumnOrRowIdForSQL(sourceAst.columns);
        String numeric = numericExpressionForSQL(sourceAst.columns);
        if (supportsPostgresAtLeast(14) && useAdvancedFeature()) {
            recordAdvancedFeature(AdvancedFeature.WINDOW_EXCLUDE);
            String frameType = Randomly.fromOptions("ROWS", "GROUPS");
            String exclusion = Randomly.fromOptions("CURRENT ROW", "GROUP", "TIES", "NO OTHERS");
            return new PostgresText("SUM(" + numeric + ") OVER (ORDER BY " + quote(ROW_ID) + " " + frameType
                    + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE " + exclusion + ")");
        }
        return new PostgresText(Randomly.fromOptions("ROW_NUMBER() OVER (ORDER BY " + order + ")",
                "RANK() OVER (ORDER BY " + order + ")", "LAG(" + value + ") OVER (ORDER BY " + quote(ROW_ID) + ")",
                "SUM(" + numeric + ") OVER (ORDER BY " + quote(ROW_ID) + ")"));
    }

    private boolean supportsPostgresAtLeast(int majorVersion) {
        try {
            String version = state.getConnection().getDatabaseVersion();
            java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("^(\\d+)").matcher(version);
            return matcher.find() && Integer.parseInt(matcher.group(1)) >= majorVersion;
        } catch (Exception ignored) {
            return false;
        }
    }

    private String createUnionAllBranch(SourceAst sourceAst, List<PostgresExpression> selected,
            PostgresSelect leftSelect, String relation, boolean includeWhereClause) {
        PostgresSelect right = new PostgresSelect();
        right.setSelectOption(PostgresSelect.SelectType.ALL);
        right.setFetchColumns(selected);
        right.setFromList(List.of(new PostgresText(relation)));
        if (includeWhereClause) {
            right.setWhereClause(leftSelect.getWhereClause());
        }
        return PostgresVisitor.asString(right);
    }

    private boolean isRowPreservingQuery(boolean useAggregate, PostgresSelect select) {
        return !useAggregate && select.getGroupByExpressions().isEmpty() && !isDistinct(select);
    }

    private boolean isDistinct(PostgresSelect select) {
        return select.getSelectOption() == PostgresSelect.SelectType.DISTINCT;
    }

    private boolean isSingleRowAggregate(boolean useAggregate, PostgresSelect select) {
        return useAggregate && select.getGroupByExpressions().isEmpty();
    }

    private boolean hasVpRowIdTieBreaker(PostgresSelect select, PostgresColumn rowIdColumn) {
        return hasVpRowIdTieBreaker(select.getOrderByClauses(), rowIdColumn);
    }

    private boolean hasVpRowIdTieBreaker(List<PostgresExpression> orderBys, PostgresColumn rowIdColumn) {
        if (orderBys.isEmpty()) {
            return false;
        }
        PostgresExpression last = orderBys.get(orderBys.size() - 1);
        return last instanceof PostgresColumnReference
                && ((PostgresColumnReference) last).getColumn().getName().equals(rowIdColumn.getName());
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

    private void incrementSelectedBaseShape(BaseShape value) {
        synchronized (STATS_LOCK) {
            increment(SELECTED_BASE_SHAPE_COUNTS, value);
        }
    }

    private static void recordSourceRowCount(long rowCount) {
        String bucket;
        if (rowCount == 0) {
            bucket = "ZERO";
        } else if (rowCount == 1) {
            bucket = "ONE";
        } else if (rowCount <= 3) {
            bucket = "TWO_TO_THREE";
        } else if (rowCount <= 7) {
            bucket = "FOUR_TO_SEVEN";
        } else if (rowCount <= 31) {
            bucket = "EIGHT_TO_31";
        } else if (rowCount <= 191) {
            bucket = "32_TO_191";
        } else if (rowCount <= 999) {
            bucket = "192_TO_999";
        } else if (rowCount <= 9999) {
            bucket = "1000_TO_9999";
        } else {
            bucket = "10000_OR_MORE";
        }
        synchronized (STATS_LOCK) {
            SOURCE_ROW_COUNT_BUCKETS.put(bucket, SOURCE_ROW_COUNT_BUCKETS.getOrDefault(bucket, 0) + 1);
            sourceRowCountTotal += rowCount;
            sourceRowCountMinimum = Math.min(sourceRowCountMinimum, rowCount);
            sourceRowCountMaximum = Math.max(sourceRowCountMaximum, rowCount);
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

    private static void recordCandidateShape(BaseShape shape, long rowCount, int columnCount) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            candidateShapeSamples++;
            incrementString(CANDIDATE_SHAPE_COUNTS, "candidate-total");
            incrementString(CANDIDATE_SHAPE_COUNTS, "candidate-" + shape.name().toLowerCase(Locale.ROOT));
            incrementString(CANDIDATE_SHAPE_COUNTS, rowCount == 0 ? "candidate-empty" : "candidate-nonempty");
            incrementString(CANDIDATE_SHAPE_COUNTS,
                    columnCount == 1 ? "candidate-single-column" : "candidate-multi-column");
        }
        markVpStatsDirty();
    }

    public static void recordSkippedError(String stage, String sql, String error) {
        registerVpStatsShutdownHook();
        String normalized = normalizeSkippedError(error);
        synchronized (STATS_LOCK) {
            SKIPPED_ERROR_COUNTS.put(normalized, SKIPPED_ERROR_COUNTS.getOrDefault(normalized, 0) + 1);
            List<SkippedErrorSample> samples = SKIPPED_ERROR_SAMPLES.get(normalized);
            boolean prioritySample = isPrioritySkippedError(normalized);
            if (samples == null && (prioritySample || SKIPPED_ERROR_SAMPLES.size() < MAX_SKIPPED_ERROR_SAMPLE_GROUPS)) {
                samples = new ArrayList<>();
                SKIPPED_ERROR_SAMPLES.put(normalized, samples);
            }
            int sampleLimit = prioritySample ? MAX_PRIORITY_SKIPPED_ERROR_SAMPLES_PER_GROUP
                    : MAX_SKIPPED_ERROR_SAMPLES_PER_GROUP;
            if (samples != null && samples.size() < sampleLimit) {
                samples.add(new SkippedErrorSample(normalizeSkippedStage(stage), normalizeSkippedSql(sql)));
            }
        }
        markVpStatsDirty();
    }

    private static void recordSchemaCopySkip(String stage, String sql, String error) {
        registerVpStatsShutdownHook();
        String key = normalizeSkippedStage(stage) + ":" + normalizeSchemaCopySkipReason(error);
        synchronized (STATS_LOCK) {
            SCHEMA_COPY_SKIP_COUNTS.put(key, SCHEMA_COPY_SKIP_COUNTS.getOrDefault(key, 0) + 1);
        }
        markVpStatsDirty();
    }

    private static String normalizeSchemaCopySkipReason(String error) {
        String normalized = normalizeSkippedError(error);
        String lower = normalized.toLowerCase(Locale.ROOT);
        if (lower.contains("is violated by some row")) {
            return "copied-definition-violated";
        }
        if (lower.contains("could not create unique index") || lower.contains("duplicate key value violates unique")) {
            return "copied-unique-conflict";
        }
        if (lower.contains("conflicting key value violates exclusion constraint")
                || lower.contains("could not create exclusion constraint")) {
            return "copied-exclusion-conflict";
        }
        if (lower.contains("foreign key")) {
            return "isolated-helper-foreign-key-not-copied";
        }
        return normalized.replaceAll("vp_[A-Za-z0-9_]+", "vp_<helper>").replaceAll("\"vp_[^\"]+\"", "\"vp_<helper>\"");
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
        normalized = normalized.replaceAll(" Position: \\d+", "");
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

    public static void recordProfileAttempt(String profile) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            incrementString(PROFILE_ATTEMPTS, profile);
        }
        markVpStatsDirty();
    }

    public static void recordProfileColumnCreated(String profile) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            incrementString(PROFILE_COLUMNS_CREATED, profile);
        }
        markVpStatsDirty();
    }

    public static void recordProfileNonnullRows(String profile, long rows) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            PROFILE_NONNULL_ROWS.merge(profile, rows, Long::sum);
        }
        markVpStatsDirty();
    }

    private static void recordProfileQueryUse(String profile) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            incrementString(PROFILE_QUERY_USES, profile);
        }
        markVpStatsDirty();
    }

    public static void recordCreatedBaseTableProfile(String databaseName, String tableName, String profile) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            BASE_TABLE_PROFILES.put(baseTableProfileKey(databaseName, tableName), profile);
            incrementString(CREATED_BASE_TABLE_PROFILE_COUNTS, profile);
        }
        markVpStatsDirty();
    }

    public static String getCreatedBaseTableProfile(String databaseName, String tableName) {
        synchronized (STATS_LOCK) {
            return BASE_TABLE_PROFILES.getOrDefault(baseTableProfileKey(databaseName, tableName), "UNTRACKED");
        }
    }

    public static void recordBaseTableStressProfile(String profile) {
        synchronized (STATS_LOCK) {
            incrementString(STRESS_BASE_TABLE_PROFILE_COUNTS, profile);
        }
        markVpStatsDirty();
    }

    private static void recordSelectedBaseTableProfile(String databaseName, String tableName) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            incrementString(SELECTED_BASE_TABLE_PROFILE_COUNTS,
                    BASE_TABLE_PROFILES.getOrDefault(baseTableProfileKey(databaseName, tableName), "UNTRACKED"));
        }
        markVpStatsDirty();
    }

    private static String baseTableProfileKey(String databaseName, String tableName) {
        return databaseName + "\u0000" + tableName;
    }

    private static String formatVpStats(String event) {
        StringBuilder sb = new StringBuilder();
        sb.append("-- [vp-stats] event=").append(event).append(" dbms=postgres");
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] oracle-runs completed=").append(completedOracleRuns);
        sb.append(" checkedPairs=").append(completedQueryPairs);
        sb.append(" attempts=").append(completedAttempts);
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] schema-shape-global samples=").append(schemaShapeSamples);
        for (Map.Entry<String, Integer> entry : SCHEMA_SHAPE_COUNTS.entrySet()) {
            sb.append(" ").append(entry.getKey()).append("=").append(entry.getValue());
        }
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] schema-shape samples=").append(candidateShapeSamples);
        for (Map.Entry<String, Integer> entry : CANDIDATE_SHAPE_COUNTS.entrySet()) {
            sb.append(" ").append(entry.getKey()).append("=").append(entry.getValue());
        }
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] split-counts ")
                .append(formatEnumCounts(SplitStrategy.values(), SPLIT_STRATEGY_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] selected-shape-counts ")
                .append(formatEnumCounts(BaseShape.values(), SELECTED_BASE_SHAPE_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] candidate-shape samples=").append(candidateShapeSamples);
        for (Map.Entry<String, Integer> entry : CANDIDATE_SHAPE_COUNTS.entrySet()) {
            sb.append(" ").append(entry.getKey()).append("=").append(entry.getValue());
        }
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] source-row-count-buckets ").append(formatCountPercentages(SOURCE_ROW_COUNT_BUCKETS));
        sb.append(System.lineSeparator());
        long rowSamples;
        long rowTotal;
        long rowMinimum;
        long rowMaximum;
        synchronized (STATS_LOCK) {
            rowSamples = SOURCE_ROW_COUNT_BUCKETS.values().stream().mapToLong(Integer::longValue).sum();
            rowTotal = sourceRowCountTotal;
            rowMinimum = rowSamples == 0 ? 0 : sourceRowCountMinimum;
            rowMaximum = sourceRowCountMaximum;
        }
        sb.append("-- [vp-stats] source-row-count samples=").append(rowSamples).append(" total=").append(rowTotal)
                .append(" min=").append(rowMinimum).append(" max=").append(rowMaximum).append(" average=")
                .append(rowSamples == 0 ? 0.0 : (double) rowTotal / rowSamples);
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] created-base-table-profiles ").append(
                formatStringCounts(CREATED_BASE_TABLE_PROFILE_COUNTS, CREATED_BASE_TABLE_PROFILE_COUNTS.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] selected-base-table-profiles ").append(
                formatStringCounts(SELECTED_BASE_TABLE_PROFILE_COUNTS, SELECTED_BASE_TABLE_PROFILE_COUNTS.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] stress-base-table-profiles ").append(
                formatStringCounts(STRESS_BASE_TABLE_PROFILE_COUNTS, STRESS_BASE_TABLE_PROFILE_COUNTS.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] profile-attempts ")
                .append(formatStringCounts(PROFILE_ATTEMPTS, PROFILE_ATTEMPTS.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] profile-columns-created ")
                .append(formatStringCounts(PROFILE_COLUMNS_CREATED, PROFILE_COLUMNS_CREATED.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] profile-nonnull-rows ").append(formatLongCounts(PROFILE_NONNULL_ROWS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] profile-query-uses ")
                .append(formatStringCounts(PROFILE_QUERY_USES, PROFILE_QUERY_USES.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] session-guc-profiles ")
                .append(formatStringCounts(SESSION_GUC_PROFILE_COUNTS, SESSION_GUC_PROFILE_COUNTS.size()));
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
        sb.append("-- [vp-stats] schema-copy-skip-counts ")
                .append(formatStringCounts(SCHEMA_COPY_SKIP_COUNTS, MAX_SKIPPED_ERROR_COUNT_GROUPS));
        sb.append(System.lineSeparator());
        if (SCHEMA_COPY_SKIP_COUNTS.size() > MAX_SKIPPED_ERROR_COUNT_GROUPS) {
            sb.append("-- [vp-stats] schema-copy-skip-counts-truncated distinctReasons=");
            sb.append(SCHEMA_COPY_SKIP_COUNTS.size());
            sb.append(" shown=").append(MAX_SKIPPED_ERROR_COUNT_GROUPS);
            sb.append(System.lineSeparator());
        }
        sb.append("-- [vp-stats] mvcc-auxiliary-connection-failures ").append(formatStringCounts(
                MVCC_AUXILIARY_CONNECTION_FAILURE_COUNTS, MVCC_AUXILIARY_CONNECTION_FAILURE_COUNTS.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] mvcc-savepoint-setup-skips ")
                .append(formatStringCounts(MVCC_SETUP_SKIP_COUNTS, MVCC_SETUP_SKIP_COUNTS.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] plan-shape-counts ")
                .append(formatStringCounts(PLAN_SHAPE_COUNTS, PLAN_SHAPE_COUNTS.size()));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] plan-divergence-counts ")
                .append(formatStringCounts(PLAN_DIVERGENCE_COUNTS, PLAN_DIVERGENCE_COUNTS.size()));
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

    private boolean useAdvancedFeature() {
        if (deepExplorationEnabled()) {
            return Randomly.getBoolean() || Randomly.getBooleanWithRatherLowProbability();
        }
        return Randomly.getBoolean();
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

    private static String formatStringCounts(Map<String, Integer> counts, int limit) {
        if (counts.isEmpty()) {
            return "NONE=0";
        }
        return counts.entrySet().stream().sorted((l, r) -> Integer.compare(r.getValue(), l.getValue())).limit(limit)
                .map(e -> quoteStatsKey(e.getKey()) + "=" + e.getValue()).collect(Collectors.joining(", "));
    }

    private static String formatLongCounts(Map<String, Long> counts) {
        if (counts.isEmpty()) {
            return "NONE=0";
        }
        return counts.entrySet().stream().sorted((l, r) -> Long.compare(r.getValue(), l.getValue()))
                .map(e -> quoteStatsKey(e.getKey()) + "=" + e.getValue()).collect(Collectors.joining(", "));
    }

    private static String formatCountPercentages(Map<String, Integer> counts) {
        long total = counts.values().stream().mapToLong(Integer::longValue).sum();
        if (counts.isEmpty()) {
            return "NONE=0 (0.00%)";
        }
        return counts.entrySet().stream().map(entry -> {
            double percentage = total == 0 ? 0.0 : entry.getValue() * 100.0 / total;
            return entry.getKey() + "=" + entry.getValue() + " (" + String.format(Locale.ROOT, "%.2f", percentage)
                    + "%)";
        }).collect(Collectors.joining(", "));
    }

    private static String quoteStatsKey(String key) {
        return statsQuote(key);
    }

    public static void logSchemaShapeStats(PostgresGlobalState state, String phase) throws Exception {
        registerVpStatsShutdownHook();
        int emptySingle = 0;
        int emptyMulti = 0;
        int nonEmptySingle = 0;
        int nonEmptyMulti = 0;
        state.updateSchema();
        for (PostgresTable table : state.getSchema().getDatabaseTables()) {
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

    /** Creates an immediately durable checkpoint before VP database generation begins. */
    public static void initializeStatsLogging() {
        registerVpStatsShutdownHook();
        writeVpStatsSnapshot();
    }

    private static void registerVpStatsShutdownHook() {
        if (SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(PostgresVerticalPartitioningOracle::writeFinalVpStats));
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
                System.err.println("Could not write Postgres VP stats: " + e.getMessage());
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

    private PostgresExpression generateVpAggregate(List<PostgresColumn> columns, long rowCount) {
        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            return new PostgresText(randomAdvancedAggregateSQL(columns, rowCount));
        }
        PostgresAggregateFunction func = Randomly.fromOptions(PostgresAggregateFunction.values());
        String numeric = numericExpressionForSQL(columns);
        String integer = integerExpressionForSQL(columns);
        boolean useFilter = advancedFeaturesEnabled() && useAdvancedFeature();
        String filter = useFilter ? " FILTER (WHERE " + aggregateFilterForSQL(columns) + ")" : "";
        if (useFilter) {
            recordAdvancedFeature(AdvancedFeature.FILTER_AGGREGATE);
        }
        switch (func) {
        case SUM:
            return new PostgresText("SUM(" + numeric + ")" + filter);
        case AVG:
            return new PostgresText("AVG(" + numeric + ")" + filter);
        case BIT_AND:
            return new PostgresText("BIT_AND(" + integer + ")" + filter);
        case BIT_OR:
            return new PostgresText("BIT_OR(" + integer + ")" + filter);
        case BOOL_AND:
            return new PostgresText("BOOL_AND(" + booleanAggregateInputForSQL(columns) + ")" + filter);
        case BOOL_OR:
            return new PostgresText("BOOL_OR(" + booleanAggregateInputForSQL(columns) + ")" + filter);
        case COUNT:
            return new PostgresText("COUNT(*)" + filter);
        case EVERY:
            return new PostgresText("EVERY(" + booleanAggregateInputForSQL(columns) + ")" + filter);
        case MIN:
            return new PostgresText("MIN(" + minMaxInputForSQL(columns) + ")" + filter);
        case MAX:
            return new PostgresText("MAX(" + minMaxInputForSQL(columns) + ")" + filter);
        default:
            throw new AssertionError(func);
        }
    }

    private String aggregateFilterForSQL(List<PostgresColumn> columns) {
        List<PostgresColumn> booleanColumns = columns.stream().filter(c -> c.getType() == PostgresDataType.BOOLEAN)
                .collect(Collectors.toList());
        if (!booleanColumns.isEmpty() && Randomly.getBoolean()) {
            return quote(Randomly.fromList(booleanColumns).getName());
        }
        return quote(ROW_ID) + " IS NOT NULL";
    }

    private String randomAdvancedAggregateSQL(List<PostgresColumn> columns, long rowCount) {
        boolean boundedValueAggregate = rowCount <= MAX_ORDERED_VALUE_AGGREGATE_ROWS;
        if (boundedValueAggregate && advancedFeaturesEnabled() && useAdvancedFeature()) {
            recordAdvancedFeature(AdvancedFeature.GROUP_CONCAT);
            return "STRING_AGG((" + groupConcatInputForSQL(columns) + ")::TEXT, ',' ORDER BY " + quote(ROW_ID) + ")";
        }
        if (advancedFeaturesEnabled() && useAdvancedFeature()) {
            return randomPostgresSpecificAggregateSQL(columns, boundedValueAggregate);
        }
        String numeric = numericExpressionForSQL(columns);
        String integer = integerExpressionForSQL(columns);
        return Randomly.fromOptions("AVG(" + numeric + ")", "BIT_AND(" + integer + ")", "BIT_OR(" + integer + ")",
                "STDDEV(" + numeric + ")", "VARIANCE(" + numeric + ")");
    }

    private long effectiveSourceRowCount(VpSchema vp) {
        int maxSourceRows = state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows;
        if (maxSourceRows > 0) {
            return Math.min(vp.baseRowCount, maxSourceRows);
        }
        return vp.baseRowCount;
    }

    private String randomPostgresSpecificAggregateSQL(List<PostgresColumn> columns, boolean boundedValueAggregate) {
        int family = (int) Randomly.getNotCachedInteger(0, boundedValueAggregate ? 3 : 2);
        switch (family) {
        case 0:
            recordAdvancedFeature(AdvancedFeature.ORDERED_SET_AGGREGATE);
            String orderedInput = minMaxInputForSQL(columns);
            return Randomly.fromOptions(
                    "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (" + numericExpressionForSQL(columns)
                            + ")::DOUBLE PRECISION)",
                    "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY " + orderedInput + ")",
                    "MODE() WITHIN GROUP (ORDER BY " + orderedInput + ")");
        case 1:
            recordAdvancedFeature(AdvancedFeature.STATISTICAL_AGGREGATE);
            String y = numericExpressionForSQL(columns);
            String x = numericExpressionForSQL(columns);
            // These aggregates return floating-point values and are sensitive
            // to input order. Source and split relations can legitimately use
            // different scan/join plans, so stabilize the transition order
            // with the VP row id before comparing their results.
            String orderedStatArgs = y + ", " + x + " ORDER BY " + quote(ROW_ID);
            return Randomly.fromOptions("corr(" + orderedStatArgs + ")", "covar_pop(" + orderedStatArgs + ")",
                    "covar_samp(" + orderedStatArgs + ")", "regr_slope(" + orderedStatArgs + ")",
                    "regr_intercept(" + orderedStatArgs + ")", "regr_r2(" + orderedStatArgs + ")");
        case 2:
            recordAdvancedFeature(AdvancedFeature.ARRAY_JSON_AGGREGATE);
            String input = simpleColumnOrRowIdForSQL(columns);
            // Aggregate order is otherwise plan-dependent: source and split
            // relations commonly use different scan/join paths. A stable row
            // id order is required for VP comparison, and PostgreSQL rejects
            // NULL array values as array_agg inputs.
            String orderedAggregateInput = input + " ORDER BY " + quote(ROW_ID);
            String nonNullArrayInput = input + " IS NOT NULL";
            return Randomly.fromOptions(
                    "array_agg(" + orderedAggregateInput + ") FILTER (WHERE " + nonNullArrayInput + ")",
                    "json_agg(" + orderedAggregateInput + ")", "jsonb_agg(" + orderedAggregateInput + ")");
        default:
            throw new AssertionError(family);
        }
    }

    private String groupConcatInputForSQL(List<PostgresColumn> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        PostgresColumn column = Randomly.fromList(columns);
        if (column.getType() == PostgresDataType.TEXT || Randomly.getBoolean()) {
            return "CAST(" + quote(column.getName()) + " AS TEXT)";
        }
        return quote(column.getName());
    }

    private String numericExpressionForSQL(List<PostgresColumn> columns) {
        List<PostgresColumn> numericColumns = columns.stream().filter(c -> isNumericType(c.getType()))
                .collect(Collectors.toList());
        if (!numericColumns.isEmpty() && Randomly.getBoolean()) {
            PostgresColumn column = Randomly.fromList(numericColumns);
            if (column.getType() == PostgresDataType.MONEY) {
                return "CAST(" + quote(column.getName()) + " AS NUMERIC)";
            }
            return quote(column.getName());
        }
        return quote(ROW_ID);
    }

    private String integerExpressionForSQL(List<PostgresColumn> columns) {
        List<PostgresColumn> integerColumns = columns.stream().filter(c -> c.getType() == PostgresDataType.INT)
                .collect(Collectors.toList());
        if (!integerColumns.isEmpty() && Randomly.getBoolean()) {
            return quote(Randomly.fromList(integerColumns).getName());
        }
        return quote(ROW_ID);
    }

    private String simpleColumnOrRowIdForSQL(List<PostgresColumn> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        return quote(Randomly.fromList(columns).getName());
    }

    private String simpleOrderExpressionForSQL(List<PostgresColumn> columns) {
        if (columns.isEmpty()) {
            return quote(ROW_ID);
        }
        return simpleColumnOrRowIdForSQL(columns);
    }

    private String booleanAggregateInputForSQL(List<PostgresColumn> columns) {
        List<PostgresColumn> boolColumns = columns.stream().filter(c -> c.getType() == PostgresDataType.BOOLEAN)
                .collect(Collectors.toList());
        if (!boolColumns.isEmpty() && Randomly.getBoolean()) {
            return quote(Randomly.fromList(boolColumns).getName());
        }
        return quote(ROW_ID) + " IS NOT NULL";
    }

    private String minMaxInputForSQL(List<PostgresColumn> columns) {
        // MIN/MAX has no PostgreSQL overload for bit, range, or the opaque
        // types that can reach a VP schema through catalog fallback. Use an
        // explicit allow-list so a newly mapped/unknown type cannot leak into
        // an invalid aggregate query.
        List<PostgresColumn> supportedColumns = columns.stream().filter(c -> {
            switch (c.getType()) {
            case INT:
            case TEXT:
            case DECIMAL:
            case FLOAT:
            case REAL:
            case MONEY:
            case INET:
                return true;
            default:
                return false;
            }
        }).collect(Collectors.toList());
        if (!supportedColumns.isEmpty() && Randomly.getBoolean()) {
            return quote(Randomly.fromList(supportedColumns).getName());
        }
        return quote(ROW_ID);
    }

    private static final class SourceAst {
        private final PostgresTable table;
        private final PostgresColumn rowIdColumn;
        private final List<PostgresColumn> columns;

        private SourceAst(PostgresTable table, PostgresColumn rowIdColumn, List<PostgresColumn> columns) {
            this.table = table;
            this.rowIdColumn = rowIdColumn;
            this.columns = columns;
        }
    }

    private SourceAst createSourceAst(VpSchema vp) {
        List<PostgresColumn> astColumns = new ArrayList<>();
        PostgresColumn rowId = new PostgresColumn(ROW_ID, PostgresDataType.INT);
        astColumns.add(rowId);
        for (VpColumn column : vp.columns) {
            astColumns.add(new PostgresColumn(column.name, column.type));
        }
        PostgresTable table = new PostgresTable(vp.sourceTable, astColumns, List.<PostgresIndex> of(),
                TableType.STANDARD, List.of(), false, true);
        for (PostgresColumn column : astColumns) {
            column.setTable(table);
        }
        return new SourceAst(table, rowId, astColumns);
    }

    private List<PostgresColumn> genericExpressionColumns(SourceAst sourceAst) {
        List<PostgresColumn> columns = sourceAst.columns.stream()
                .filter(column -> isGenericExpressionType(column.getType()))
                .collect(Collectors.toCollection(ArrayList::new));
        if (columns.isEmpty()) {
            columns.add(sourceAst.rowIdColumn);
        }
        return columns;
    }

    private boolean isGenericExpressionType(PostgresDataType type) {
        switch (type) {
        case INT:
        case BOOLEAN:
        case TEXT:
        case DECIMAL:
        case FLOAT:
        case REAL:
        case MONEY:
        case BIT:
            return true;
        default:
            return false;
        }
    }

    @SuppressWarnings("unused") // Compatibility fallback for externally supplied VP query text.
    private String replaceSourceFrom(String singleQuery, String sourceTable, String relation) {
        String unquotedNeedle = " FROM " + sourceTable;
        if (singleQuery.contains(unquotedNeedle)) {
            return singleQuery.replace(unquotedNeedle, " FROM " + relation);
        }
        String quotedNeedle = " FROM " + quote(sourceTable);
        if (singleQuery.contains(quotedNeedle)) {
            return singleQuery.replace(quotedNeedle, " FROM " + relation);
        }
        throw new IgnoreMeException();
    }

    private String sourceRelation(VpSchema vp) {
        return quote(vp.sourceTable) + " " + quote(vp.sourceTable);
    }

    private String sourceRelationForStrategy(VpSchema vp, ReconstructionStrategy strategy) {
        /*
         * A VP oracle must compare a query over the original logical relation with the same query over the
         * reconstructed relation. Applying the reconstruction strategy to the single side as well turns this into a
         * comparison of two independently transformed relations and can hide both implementation bugs and row-loss
         * bugs. Strategy-specific stress belongs exclusively in the split side.
         */
        return sourceRelation(vp);
    }

    private boolean rightJoinForVp(VpSchema vp) {
        if (vp.leftColumns.isEmpty()) {
            return !vp.rightColumns.isEmpty();
        }
        if (vp.rightColumns.isEmpty()) {
            return false;
        }
        return (vp.splitStrategy.ordinal() & 1) == 0;
    }

    private String renderSelectWithRelation(PostgresSelect select, String relation) {
        select.setFromList(List.of(new PostgresText(relation)));
        return PostgresVisitor.asString(select);
    }

    private String buildReconstructionRelation(VpSchema vp, ReconstructionStrategy strategy, List<String> extraSetup)
            throws Exception {
        switch (strategy) {
        case JOIN:
            return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false), " FROM " + quote(vp.leftTable)
                    + " l JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID));
        case LEFT_RIGHT_JOIN:
            return outerJoinRelation(vp, rightJoinForVp(vp));
        case FULL_OUTER_SIMULATION:
            return reconstructedSubquery(vp, fullOuterProjection(vp),
                    " FROM " + quote(vp.leftTable) + " l FULL OUTER JOIN " + quote(vp.rightTable) + " r ON l."
                            + quote(ROW_ID) + " = r." + quote(ROW_ID));
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
            return antiReconstructionRelation(vp);
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
            return hintedJoinRelation(vp);
        case LATERAL_DERIVED:
            return lateralRelation(vp);
        case DIRECTED_MATERIALIZED_JOIN:
            return directedMaterializedJoinRelation(vp);
        case DIRECTED_PARTITION_JOIN:
            return directedPartitionJoinRelation(vp);
        default:
            throw new AssertionError(strategy);
        }
    }

    private String reconstructedSubquery(VpSchema vp, String projection, String fromAndWhere) {
        return "(SELECT " + projection + fromAndWhere + ") " + quote(vp.sourceTable);
    }

    private String createTemporaryReconstructionTableSQL(VpSchema vp) {
        List<String> defs = new ArrayList<>();
        defs.add(quote(ROW_ID) + " INT8 NOT NULL PRIMARY KEY");
        for (VpColumn column : vp.columns) {
            defs.add(partitionColumnDef(vp, column));
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
            String alias = columnAlias(vp, column, leftAlias, rightAlias);
            parts.add(alias + "." + quote(column.name) + " AS " + quote(column.name));
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
            } else {
                parts.add("r." + quote(column.name) + " AS " + quote(column.name));
            }
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
        String leftMaterialization = cteMaterializationClause(Randomly.getBoolean());
        String rightMaterialization = cteMaterializationClause(Randomly.getBoolean());
        if (advancedFeaturesEnabled() && useAdvancedFeature()) {
            recordAdvancedFeature(AdvancedFeature.RECURSIVE_CTE);
            return "(WITH RECURSIVE walk(" + quote(ROW_ID) + ", depth) AS (SELECT " + quote(ROW_ID) + ", 0 FROM "
                    + quote(vp.leftTable) + " UNION ALL SELECT w." + quote(ROW_ID)
                    + ", w.depth + 1 FROM walk w WHERE w.depth < 2), l AS" + leftMaterialization + " (SELECT * FROM "
                    + quote(vp.leftTable) + "), r AS" + rightMaterialization + " (SELECT * FROM " + quote(vp.rightTable)
                    + ") SELECT " + joinProjection(vp, "l", "r", false) + " FROM l JOIN r ON l." + quote(ROW_ID)
                    + " = r." + quote(ROW_ID) + " WHERE EXISTS (SELECT 1 FROM walk w WHERE w." + quote(ROW_ID) + " = l."
                    + quote(ROW_ID) + ")) " + quote(vp.sourceTable);
        }
        return "(WITH l AS" + leftMaterialization + " (SELECT * FROM " + quote(vp.leftTable) + "), r AS"
                + rightMaterialization + " (SELECT * FROM " + quote(vp.rightTable) + ") SELECT "
                + joinProjection(vp, "l", "r", false) + " FROM l JOIN r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID)
                + ") " + quote(vp.sourceTable);
    }

    private String aggregateThenMergeRelation(VpSchema vp) {
        String rowId = quote(ROW_ID);
        String aggregate = "(SELECT (" + rowId + " % 2) AS vp_bucket, COUNT(*) AS vp_count, SUM(" + rowId
                + ") AS vp_sum FROM " + quote(vp.rightTable) + " GROUP BY (" + rowId + " % 2))";
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN " + aggregate + " rg ON (l." + rowId
                        + " % 2) = rg.vp_bucket AND rg.vp_count > 0 AND rg.vp_sum IS NOT NULL JOIN "
                        + quote(vp.rightTable) + " r ON r." + rowId + " = l." + rowId);
    }

    private String antiReconstructionRelation(VpSchema vp) {
        String rowId = quote(ROW_ID);
        // With unique, non-null row IDs, excluding left rows that lack a right match is equivalent to
        // EXISTS(right match). Testing for neighbouring IDs instead drops valid reconstructed rows.
        return reconstructedSubquery(vp, leftProjectionWithRightLookups(vp),
                " FROM " + quote(vp.leftTable) + " l WHERE NOT EXISTS (SELECT 1 FROM " + quote(vp.leftTable)
                        + " lx WHERE lx." + rowId + " = l." + rowId + " AND NOT EXISTS (SELECT 1 FROM "
                        + quote(vp.rightTable) + " rx WHERE rx." + rowId + " = lx." + rowId + "))");
    }

    private String outerJoinRelation(VpSchema vp, boolean rightJoin) {
        String rowId = quote(ROW_ID);
        // Both helpers contain the source row IDs. Augmenting the preserved side of an outer join adds
        // an unmatched row to the reconstruction that never existed in the logical source relation.
        String left = quote(vp.leftTable);
        String right = quote(vp.rightTable);
        String projection = outerProjection(vp, "l", "r", rightJoin);
        return "(SELECT " + projection + " FROM " + left + " l " + (rightJoin ? "RIGHT JOIN " : "LEFT JOIN ") + right
                + " r ON l." + rowId + " = r." + rowId + ") " + quote(vp.sourceTable);
    }

    private String outerProjection(VpSchema vp, String leftAlias, String rightAlias, boolean rightJoin) {
        String preservedAlias = rightJoin ? rightAlias : leftAlias;
        List<String> parts = new ArrayList<>();
        parts.add(preservedAlias + "." + quote(ROW_ID) + " AS " + quote(ROW_ID));
        for (VpColumn column : vp.columns) {
            String alias;
            if (rightJoin) {
                alias = containsColumn(vp.rightColumns, column.name) ? rightAlias : leftAlias;
            } else {
                alias = containsColumn(vp.leftColumns, column.name) ? leftAlias : rightAlias;
            }
            parts.add(alias + "." + quote(column.name) + " AS " + quote(column.name));
        }
        return String.join(", ", parts);
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
                " FROM " + quote(vp.leftTable) + " l JOIN (SELECT " + quote(ROW_ID) + ", jsonb_build_object('vp', "
                        + quote(ROW_ID) + ") AS payload FROM " + quote(vp.rightTable) + ") jp ON l." + quote(ROW_ID)
                        + " = jp." + quote(ROW_ID) + " JOIN " + quote(vp.rightTable) + " r ON r." + quote(ROW_ID)
                        + " = jp." + quote(ROW_ID));
    }

    private String stringPackRelation(VpSchema vp) {
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN (SELECT " + quote(ROW_ID) + ", concat('vp:', " + quote(ROW_ID)
                        + "::TEXT) AS payload FROM " + quote(vp.rightTable) + ") sp ON concat('vp:', l." + quote(ROW_ID)
                        + "::TEXT) = sp.payload JOIN " + quote(vp.rightTable) + " r ON r." + quote(ROW_ID) + " = sp."
                        + quote(ROW_ID));
    }

    private String hintedJoinRelation(VpSchema vp) {
        return "(WITH l AS" + cteMaterializationClause() + " (SELECT * FROM " + quote(vp.leftTable) + " ORDER BY "
                + quote(ROW_ID) + "), r AS" + cteMaterializationClause() + " (SELECT * FROM " + quote(vp.rightTable)
                + " ORDER BY " + quote(ROW_ID) + ") SELECT " + joinProjection(vp, "l", "r", false)
                + " FROM l JOIN r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID) + ") " + quote(vp.sourceTable);
    }

    private VpQueryPair generateDirectedMaterializedJoinPair(VpSchema vp) throws SQLException {
        long upperBound = Math.max(1, Math.min(getRowCount(vp.sourceTable), 8));
        String alias = quote(vp.sourceTable);
        String predicate = alias + "." + quote(ROW_ID) + " <= " + upperBound;
        String projection = sourceColumnList(vp);
        String single = "SELECT " + projection + " FROM " + sourceRelation(vp) + " WHERE " + predicate + " ORDER BY "
                + alias + "." + quote(ROW_ID);
        String split = "SELECT " + projection + " FROM " + directedMaterializedJoinRelation(vp) + " WHERE " + predicate
                + " ORDER BY " + alias + "." + quote(ROW_ID);
        return new VpQueryPair(vp, ReconstructionStrategy.DIRECTED_MATERIALIZED_JOIN, single, split, List.of(),
                "shape=" + vp.baseShape + ", split=" + vp.splitStrategy
                        + ", reconstruction=DIRECTED_MATERIALIZED_JOIN, directed-plan=index-vs-materialized-join",
                true, List.of(), false);
    }

    private String directedMaterializedJoinRelation(VpSchema vp) {
        return "(WITH l AS" + cteMaterializationClause() + " (SELECT * FROM " + quote(vp.leftTable) + "), r AS"
                + cteMaterializationClause() + " (SELECT * FROM " + quote(vp.rightTable) + ") SELECT "
                + joinProjection(vp, "l", "r", false) + " FROM l JOIN r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID)
                + ") " + quote(vp.sourceTable);
    }

    private VpQueryPair generateDirectedPartitionJoinPair(VpSchema vp) throws SQLException {
        if (!vp.leftPartitioned || !vp.rightPartitioned || getRowCount(vp.sourceTable) < 2) {
            throw new IgnoreMeException();
        }
        String alias = quote(vp.sourceTable);
        String key = alias + "." + quote(ROW_ID);
        long rowCount = getRowCount(vp.sourceTable);
        long upperBound = Math.max(1, Math.min(rowCount / 2, 32));
        String predicate;
        switch ((int) Randomly.getNotCachedInteger(0, 3)) {
        case 0:
            predicate = key + " < 0";
            break;
        case 1:
            predicate = key + " BETWEEN 1 AND " + upperBound;
            break;
        case 2:
            predicate = key + " IS NULL";
            break;
        default:
            predicate = key + " IN (0, 1, " + (upperBound + 1) + ")";
            break;
        }
        recordAdvancedFeature(AdvancedFeature.PARTITION_PRUNING);
        String projection = sourceColumnList(vp);
        String single = "SELECT " + projection + " FROM " + sourceRelation(vp) + " WHERE " + predicate + " ORDER BY "
                + alias + "." + quote(ROW_ID);
        List<String> extraSetup = new ArrayList<>();
        if (supportsPartitionwiseJoin()) {
            extraSetup.add("SET enable_partitionwise_join = on");
        }
        String split = "SELECT " + projection + " FROM " + directedPartitionJoinRelation(vp) + " WHERE " + predicate
                + " ORDER BY " + alias + "." + quote(ROW_ID);
        return new VpQueryPair(vp, ReconstructionStrategy.DIRECTED_PARTITION_JOIN, single, split, extraSetup,
                "shape=" + vp.baseShape + ", split=" + vp.splitStrategy
                        + ", reconstruction=DIRECTED_PARTITION_JOIN, directed-plan=partition-key-predicate-join",
                true, List.of(), false);
    }

    private String directedPartitionJoinRelation(VpSchema vp) {
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false), " FROM " + quote(vp.leftTable)
                + " l JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID));
    }

    private String cteMaterializationClause() {
        // PostgreSQL 10 materializes CTEs by default but does not accept the
        // MATERIALIZED keyword, which was introduced in PostgreSQL 12.
        return supportsMaterializedCteKeyword() ? " MATERIALIZED" : "";
    }

    private String cteMaterializationClause(boolean materialized) {
        if (!supportsMaterializedCteKeyword()) {
            return "";
        }
        recordAdvancedFeature(AdvancedFeature.CTE_MATERIALIZATION);
        return materialized ? " MATERIALIZED" : " NOT MATERIALIZED";
    }

    private boolean supportsMaterializedCteKeyword() {
        return supportsPostgresAtLeast(12);
    }

    private boolean supportsPartitionwiseJoin() {
        return supportsPostgresAtLeast(11);
    }

    private boolean hasUniqueRowIdKey(String tableName) throws SQLException {
        String sql = "SELECT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = " + quoteLiteral(tableName)
                + "::regclass AND i.indisunique AND i.indnatts = 1 AND pg_get_indexdef(i.indexrelid) LIKE '%(\""
                + ROW_ID + "\"%')";
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            return result.next() && result.getBoolean(1);
        }
    }

    private String lateralRelation(VpSchema vp) {
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN LATERAL (SELECT * FROM " + quote(vp.rightTable)
                        + " rr WHERE rr." + quote(ROW_ID) + " = l." + quote(ROW_ID) + ") r ON TRUE");
    }

    private boolean hasArrayColumn(VpSchema vp) {
        return vp.columns.stream().anyMatch(c -> c.type == PostgresDataType.ARRAY_INT
                || c.type == PostgresDataType.ARRAY_TEXT || c.type == PostgresDataType.ARRAY_NUMERIC);
    }

    private boolean hasRangeColumn(VpSchema vp) {
        return vp.columns.stream().anyMatch(c -> c.type == PostgresDataType.RANGE);
    }

    private boolean hasJsonbColumn(VpSchema vp) {
        return vp.columns.stream().anyMatch(c -> c.type == PostgresDataType.JSONB);
    }

    private VpColumn randomArrayColumn(VpSchema vp) {
        return Randomly
                .fromList(vp.columns
                        .stream().filter(c -> c.type == PostgresDataType.ARRAY_INT
                                || c.type == PostgresDataType.ARRAY_TEXT || c.type == PostgresDataType.ARRAY_NUMERIC)
                        .collect(Collectors.toList()));
    }

    private VpQueryPair generateLateralUnnestPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        VpColumn column = randomArrayColumn(vp);
        recordProfileQueryUse("ARRAY_PROFILE:" + column.type.name());
        String sourceAlias = quote(vp.sourceTable);
        String source = sourceRelationForStrategy(vp, strategy);
        List<String> extraSetup = new ArrayList<>();
        String reconstruction = buildReconstructionRelation(vp, strategy, extraSetup);
        String rowId = sourceAlias + "." + quote(ROW_ID);
        String array = sourceAlias + "." + quote(column.name);
        String single = "SELECT " + rowId + ", u.value FROM " + source + " CROSS JOIN LATERAL unnest(" + array
                + ") AS u(value) ORDER BY " + rowId + ", u.value NULLS LAST";
        String split = "SELECT " + rowId + ", u.value FROM " + reconstruction + " CROSS JOIN LATERAL unnest(" + array
                + ") AS u(value) ORDER BY " + rowId + ", u.value NULLS LAST";
        recordAdvancedFeature(AdvancedFeature.LATERAL_UNNEST);
        return new VpQueryPair(
                vp, strategy, single, split, extraSetup, "shape=" + vp.baseShape + ", split=" + vp.splitStrategy
                        + ", reconstruction=" + strategy + ", lateral=unnest, column=" + column.name,
                true, List.of(), false);
    }

    private VpQueryPair generateJsonbLateralRecordPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        String alias = quote(vp.sourceTable);
        String rowId = alias + "." + quote(ROW_ID);
        VpColumn column = randomJsonbColumn(vp);
        recordProfileQueryUse("JSON_PROFILE:" + column.type.name());
        String json = alias + "." + quote(column.name);
        String lateral = " CROSS JOIN LATERAL jsonb_to_record(jsonb_build_object('value', " + json
                + ")) AS j(value JSONB)";
        List<String> extraSetup = new ArrayList<>();
        String reconstruction = buildReconstructionRelation(vp, strategy, extraSetup);
        String single = "SELECT " + rowId + ", j.value FROM " + sourceRelationForStrategy(vp, strategy) + lateral
                + " ORDER BY " + rowId;
        String split = "SELECT " + rowId + ", j.value FROM " + reconstruction + lateral + " ORDER BY " + rowId;
        recordAdvancedFeature(AdvancedFeature.JSONB_LATERAL_RECORD);
        return new VpQueryPair(vp, strategy, single, split, extraSetup, "shape=" + vp.baseShape + ", split="
                + vp.splitStrategy + ", reconstruction=" + strategy + ", lateral=jsonb_to_record", true, List.of(),
                false);
    }

    private VpQueryPair generateJsonbLateralEachPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        String alias = quote(vp.sourceTable);
        String rowId = alias + "." + quote(ROW_ID);
        VpColumn column = randomJsonbColumn(vp);
        recordProfileQueryUse("JSON_PROFILE:" + column.type.name());
        String json = alias + "." + quote(column.name);
        String lateral = " CROSS JOIN LATERAL jsonb_each(CASE WHEN jsonb_typeof(" + json + ") = 'object' THEN " + json
                + " ELSE jsonb_build_object('value', " + json + ") END) AS j(key, value)";
        List<String> extraSetup = new ArrayList<>();
        String reconstruction = buildReconstructionRelation(vp, strategy, extraSetup);
        String single = "SELECT " + rowId + ", j.key, j.value FROM " + sourceRelationForStrategy(vp, strategy) + lateral
                + " ORDER BY " + rowId + ", j.key";
        String split = "SELECT " + rowId + ", j.key, j.value FROM " + reconstruction + lateral + " ORDER BY " + rowId
                + ", j.key";
        recordAdvancedFeature(AdvancedFeature.JSONB_LATERAL_EACH);
        return new VpQueryPair(vp, strategy, single, split, extraSetup, "shape=" + vp.baseShape + ", split="
                + vp.splitStrategy + ", reconstruction=" + strategy + ", lateral=jsonb_each", true, List.of(), false);
    }

    private VpColumn randomJsonbColumn(VpSchema vp) {
        return Randomly.fromList(
                vp.columns.stream().filter(c -> c.type == PostgresDataType.JSONB).collect(Collectors.toList()));
    }

    private VpQueryPair generateJsonbColumnPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        VpColumn column = Randomly.fromList(
                vp.columns.stream().filter(c -> c.type == PostgresDataType.JSONB).collect(Collectors.toList()));
        recordProfileQueryUse("JSON_PROFILE:" + column.type.name());
        String alias = quote(vp.sourceTable);
        String rowId = alias + "." + quote(ROW_ID);
        String json = alias + "." + quote(column.name);
        String key = Randomly.fromOptions("n", "k", "nested", "items");
        String path = Randomly.fromOptions("$." + key, "$.nested." + key, "$[*]");
        String probe = Randomly.fromOptions("jsonb_build_object('" + key + "', 0)",
                "jsonb_build_object('nested', jsonb_build_object('" + key + "', 0))",
                "jsonb_build_object('" + key + "', jsonb_build_array(0, 1))");
        String predicate = Randomly.fromOptions(json + " @> " + probe, json + " ? '" + key + "'");
        String selected = Randomly.getBoolean() ? predicate : "jsonb_path_query_first(" + json + ", '" + path + "')";
        List<String> extraSetup = new ArrayList<>();
        String reconstruction = buildReconstructionRelation(vp, strategy, extraSetup);
        String single = "SELECT " + rowId + ", " + selected + " FROM " + sourceRelationForStrategy(vp, strategy)
                + " ORDER BY " + rowId;
        String split = "SELECT " + rowId + ", " + selected + " FROM " + reconstruction + " ORDER BY " + rowId;
        recordAdvancedFeature(AdvancedFeature.JSONB_OPERATORS);
        return new VpQueryPair(vp, strategy, single, split, extraSetup, "shape=" + vp.baseShape + ", split="
                + vp.splitStrategy + ", reconstruction=" + strategy + ", jsonb-column=" + column.name, true, List.of(),
                false);
    }

    private VpQueryPair generateGenerateSeriesLateralPair(VpSchema vp, ReconstructionStrategy strategy)
            throws Exception {
        String alias = quote(vp.sourceTable);
        String rowId = alias + "." + quote(ROW_ID);
        String lateral = " CROSS JOIN LATERAL generate_series(0, (" + rowId + " % 3)::INTEGER) AS g(value)";
        List<String> extraSetup = new ArrayList<>();
        String reconstruction = buildReconstructionRelation(vp, strategy, extraSetup);
        String single = "SELECT " + rowId + ", g.value FROM " + sourceRelationForStrategy(vp, strategy) + lateral
                + " ORDER BY " + rowId + ", g.value";
        String split = "SELECT " + rowId + ", g.value FROM " + reconstruction + lateral + " ORDER BY " + rowId
                + ", g.value";
        recordAdvancedFeature(AdvancedFeature.GENERATE_SERIES_LATERAL);
        return new VpQueryPair(vp, strategy, single, split, extraSetup, "shape=" + vp.baseShape + ", split="
                + vp.splitStrategy + ", reconstruction=" + strategy + ", lateral=generate_series", true, List.of(),
                false);
    }

    private VpQueryPair generateRangeOperatorPair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        VpColumn column = Randomly.fromList(
                vp.columns.stream().filter(c -> c.type == PostgresDataType.RANGE).collect(Collectors.toList()));
        recordProfileQueryUse("RANGE_PROFILE:" + column.type.name());
        String alias = quote(vp.sourceTable);
        String range = alias + "." + quote(column.name);
        String other = "'[0,100]'::int4range";
        String predicate = Randomly.fromOptions(range + " && " + other, range + " @> 42", range + " <@ " + other,
                range + " -|- " + other, "isempty(" + range + ")");
        List<String> extraSetup = new ArrayList<>();
        String reconstruction = buildReconstructionRelation(vp, strategy, extraSetup);
        String single = "SELECT " + alias + "." + quote(ROW_ID) + ", (" + predicate + ") AS range_test FROM "
                + sourceRelationForStrategy(vp, strategy) + " ORDER BY " + alias + "." + quote(ROW_ID);
        String split = "SELECT " + alias + "." + quote(ROW_ID) + ", (" + predicate + ") AS range_test FROM "
                + reconstruction + " ORDER BY " + alias + "." + quote(ROW_ID);
        recordAdvancedFeature(AdvancedFeature.RANGE_OPERATOR);
        return new VpQueryPair(vp, strategy, single, split, extraSetup, "shape=" + vp.baseShape + ", split="
                + vp.splitStrategy + ", reconstruction=" + strategy + ", range-operator=" + column.name, true,
                List.of(), false);
    }

    private int comparePair(VpQueryPair pair) throws Exception {
        for (String setup : pair.extraSetupStatements) {
            try {
                executeNoSchemaRefresh(setup);
                pair.schema.setupStatements.add(setup);
            } catch (IgnoreMeException e) {
                executeOptional("DROP VIEW IF EXISTS " + quote(pair.schema.viewName));
                executeOptional("DROP TABLE IF EXISTS " + quote(pair.schema.tempTable));
                throw e;
            } catch (SQLException | AssertionError e) {
                if (isExpectedQueryError(e)) {
                    recordSkippedError("extra-setup", setup, getErrorText(e));
                    executeOptional("DROP VIEW IF EXISTS " + quote(pair.schema.viewName));
                    executeOptional("DROP TABLE IF EXISTS " + quote(pair.schema.tempTable));
                    throw new IgnoreMeException();
                }
                flushVpReproductionState();
                logUnexpectedQueryError(pair, "extra-setup", setup, e);
                throw e;
            }
        }
        if (pair.reconstructionStrategy == ReconstructionStrategy.TEMP_TABLE && copiesSourceSchemaObjects()) {
            copyBaseSchemaObjectsToTarget(pair.schema, pair.schema.tempTable, pair.schema.columns, "temp");
        }
        recordPairStage(pair, PairStage.EXTRA_SETUP_OK);
        if (deepExplorationEnabled() && Randomly.getBooleanWithSmallProbability()) {
            verifyDeepMvccBehavior(pair.schema);
        }
        int comparisons = 0;
        for (SessionGucProfile profile : gucProfilesForPair()) {
            resetSessionForGucDifferentialProfile(profile);
            try {
                // Both sides must observe exactly one database snapshot, including concurrent maintenance sessions.
                executeNoSchemaRefresh("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
                QueryResultMultiset singleRows;
                try {
                    singleRows = getResultMultiset("single-query", pair.singleQuery, pair.forcedApproximateColumns);
                } catch (SQLException | AssertionError e) {
                    KnownVpBug knownBug = classifyKnownQueryError(pair, "single-query", pair.singleQuery, e);
                    rollbackBeforeUnexpectedReport();
                    if (suppressKnownQueryError(pair, "single-query", pair.singleQuery, e, knownBug)) {
                        throw new IgnoreMeException();
                    }
                    flushVpReproductionState();
                    logUnexpectedQueryError(pair, "single-query", pair.singleQuery, e);
                    throw e;
                }
                recordPairStage(pair, PairStage.SINGLE_OK);
                QueryResultMultiset splitRows;
                try {
                    splitRows = getResultMultiset("split-query", pair.splitQuery, pair.forcedApproximateColumns);
                } catch (SQLException | AssertionError e) {
                    KnownVpBug knownBug = classifyKnownQueryError(pair, "split-query", pair.splitQuery, e);
                    rollbackBeforeUnexpectedReport();
                    if (suppressKnownQueryError(pair, "split-query", pair.splitQuery, e, knownBug)) {
                        throw new IgnoreMeException();
                    }
                    flushVpReproductionState();
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
                capturePlanFingerprints(pair);
                if (pair.deepCollationComparison) {
                    recordAdvancedFeature(AdvancedFeature.CHARSET_COLLATION_COLUMN);
                }
                comparisons++;
            } finally {
                executeOptional("ROLLBACK");
            }
        }
        return comparisons;
    }

    private void verifyDeepMvccBehavior(VpSchema vp) throws Exception {
        boolean readCommitted = verifySnapshotIsolation(vp, "READ COMMITTED");
        boolean repeatableRead = verifySnapshotIsolation(vp, "REPEATABLE READ");
        if (readCommitted && repeatableRead) {
            recordAdvancedFeature(AdvancedFeature.MVCC_SNAPSHOT);
        }
        if (verifySavepointRollback(vp)) {
            recordAdvancedFeature(AdvancedFeature.MVCC_SAVEPOINT);
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            if (verifyAutocommitMaintenance(vp)) {
                recordAdvancedFeature(AdvancedFeature.MVCC_MAINTENANCE);
            }
        }
    }

    private boolean verifySnapshotIsolation(VpSchema vp, String isolationLevel) throws Exception {
        try (Connection reader = openVpAuxiliaryConnection(); Connection writer = openVpAuxiliaryConnection();
                Statement readerStatement = reader.createStatement();
                Statement writerStatement = writer.createStatement()) {
            reader.setAutoCommit(false);
            writer.setAutoCommit(false);
            readerStatement.execute("BEGIN ISOLATION LEVEL " + isolationLevel + " READ ONLY");
            RowIdSnapshot beforeSource = readRowIdSnapshot(readerStatement, quote(vp.sourceTable));
            writerStatement.execute("BEGIN");
            // Hold a real, uncommitted write transaction while the reader
            // takes both sides of the VP snapshot. Updating the key to itself
            // changes tuple visibility without changing the logical relation.
            writerStatement
                    .execute("UPDATE " + quote(vp.sourceTable) + " SET " + quote(ROW_ID) + " = " + quote(ROW_ID));
            writerStatement.execute("UPDATE " + quote(vp.leftTable) + " SET " + quote(ROW_ID) + " = " + quote(ROW_ID));
            writerStatement.execute("UPDATE " + quote(vp.rightTable) + " SET " + quote(ROW_ID) + " = " + quote(ROW_ID));
            RowIdSnapshot beforeSplit = readRowIdSnapshot(readerStatement, splitSnapshotRelation(vp));
            writer.commit();
            RowIdSnapshot afterSource = readRowIdSnapshot(readerStatement, quote(vp.sourceTable));
            RowIdSnapshot afterSplit = readRowIdSnapshot(readerStatement, splitSnapshotRelation(vp));
            reader.rollback();
            if (!beforeSource.equals(beforeSplit) || !afterSource.equals(afterSplit)) {
                throw new AssertionError("VP MVCC snapshot mismatch at " + isolationLevel + ": source="
                        + beforeSource.count + "/" + beforeSource.sum + ", split=" + beforeSplit.count + "/"
                        + beforeSplit.sum + ", after=" + afterSource.count + "/" + afterSource.sum + " vs "
                        + afterSplit.count + "/" + afterSplit.sum);
            }
            return true;
        } catch (SQLException error) {
            if (isDatabaseInternalError(error)) {
                throw new AssertionError("PostgreSQL internal error during VP MVCC snapshot probe", error);
            }
            if (isAuxiliaryConnectionFailure(error)) {
                recordMvccAuxiliaryConnectionFailure("deep-mvcc-snapshot", error);
            } else {
                recordSkippedError("deep-mvcc-snapshot", vp.sourceTable, getErrorText(error));
            }
            return false;
        }
    }

    private boolean verifySavepointRollback(VpSchema vp) throws Exception {
        try (Connection connection = openVpAuxiliaryConnection(); Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.execute("BEGIN");
            long before = readRowIdSnapshot(statement, quote(vp.leftTable)).count;
            if (before == 0) {
                recordMvccSavepointSetupSkip("empty-helper");
                connection.rollback();
                return false;
            }
            try (ResultSet constraints = statement
                    .executeQuery("SELECT EXISTS (SELECT 1 FROM pg_constraint " + "WHERE conrelid = "
                            + quoteLiteral(vp.leftTable) + "::regclass " + "AND contype IN ('u', 'x'))")) {
                if (constraints.next() && constraints.getBoolean(1)) {
                    recordMvccSavepointSetupSkip("non-primary-unique-or-exclusion-constraint");
                    connection.rollback();
                    return false;
                }
            }
            // The local-FK topology deliberately uses a unique index on the
            // overlap column. It is not represented by pg_constraint, but a
            // copied row can still violate it during the savepoint probe.
            try (ResultSet indexes = statement
                    .executeQuery("SELECT EXISTS (SELECT 1 FROM pg_index " + "WHERE indrelid = "
                            + quoteLiteral(vp.leftTable) + "::regclass " + "AND indisunique AND NOT indisprimary)")) {
                if (indexes.next() && indexes.getBoolean(1)) {
                    recordMvccSavepointSetupSkip("non-primary-unique-index");
                    connection.rollback();
                    return false;
                }
            }
            statement.execute("SAVEPOINT vp_helper_cleanup");
            // PG11+ partitioned helpers own a primary key on vp_rowid. Updating
            // one tuple to its current key exercises savepoint rollback without
            // inventing a rowid that either collides or falls outside a partition.
            statement.execute("UPDATE " + quote(vp.leftTable) + " SET " + quote(ROW_ID) + " = " + quote(ROW_ID)
                    + " WHERE ctid = (SELECT ctid FROM " + quote(vp.leftTable) + " ORDER BY " + quote(ROW_ID)
                    + " LIMIT 1)");
            long duringSavepoint = readRowIdSnapshot(statement, quote(vp.leftTable)).count;
            statement.execute("ROLLBACK TO SAVEPOINT vp_helper_cleanup");
            long restored = readRowIdSnapshot(statement, quote(vp.leftTable)).count;
            connection.rollback();
            if (duringSavepoint != before || restored != before) {
                throw new AssertionError("VP SAVEPOINT helper cleanup mismatch: before=" + before + ", during="
                        + duringSavepoint + ", restored=" + restored);
            }
            return true;
        } catch (SQLException error) {
            if (isDatabaseInternalError(error)) {
                throw new AssertionError("PostgreSQL internal error during VP savepoint probe", error);
            }
            if (isAuxiliaryConnectionFailure(error)) {
                recordMvccAuxiliaryConnectionFailure("deep-mvcc-savepoint", error);
            } else {
                recordSkippedError("deep-mvcc-savepoint", vp.leftTable, getErrorText(error));
            }
            return false;
        }
    }

    private boolean verifyAutocommitMaintenance(VpSchema vp) throws Exception {
        String indexName = quote(vp.prefix + "mvcc_idx");
        try (Connection connection = openVpAuxiliaryConnection(); Statement statement = connection.createStatement()) {
            connection.setAutoCommit(true);
            statement.execute("SET statement_timeout = 15000");
            statement.execute("CREATE INDEX CONCURRENTLY " + indexName + " ON " + quote(vp.sourceTable) + " ("
                    + quote(ROW_ID) + ")");
            statement.execute("VACUUM (ANALYZE) " + quote(vp.sourceTable));
            statement.execute("DROP INDEX CONCURRENTLY IF EXISTS " + indexName);
            return true;
        } catch (SQLException error) {
            if (isDatabaseInternalError(error)) {
                throw new AssertionError("PostgreSQL internal error during VP maintenance probe", error);
            }
            if (isAuxiliaryConnectionFailure(error)) {
                recordMvccAuxiliaryConnectionFailure("deep-mvcc-maintenance", error);
            } else {
                recordSkippedError("deep-mvcc-maintenance", vp.sourceTable, getErrorText(error));
            }
            try (Connection cleanupConnection = openVpAuxiliaryConnection();
                    Statement cleanupStatement = cleanupConnection.createStatement()) {
                cleanupConnection.setAutoCommit(true);
                cleanupStatement.execute("DROP INDEX CONCURRENTLY IF EXISTS " + indexName);
            } catch (SQLException cleanupError) {
                if (isAuxiliaryConnectionFailure(cleanupError)) {
                    recordMvccAuxiliaryConnectionFailure("deep-mvcc-maintenance-cleanup", cleanupError);
                } else {
                    recordSkippedError("deep-mvcc-maintenance-cleanup", vp.sourceTable, getErrorText(cleanupError));
                }
            }
            return false;
        }
    }

    private Connection openVpAuxiliaryConnection() throws SQLException {
        SQLException first = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                return openVpAuxiliaryConnectionOnce();
            } catch (SQLException error) {
                first = error;
                if (!isAuxiliaryConnectionFailure(error) || attempt == 1) {
                    throw error;
                }
            }
        }
        throw first;
    }

    private Connection openVpAuxiliaryConnectionOnce() throws SQLException {
        String url = state.getDbmsSpecificOptions().connectionURL;
        if (url.startsWith("jdbc:")) {
            url = url.substring("jdbc:".length());
        }
        String username = state.getOptions().getUserName();
        String password = state.getOptions().getPassword();
        try {
            URI uri = new URI(url);
            String userInfo = uri.getUserInfo();
            if (userInfo != null) {
                if (userInfo.contains(":")) {
                    String[] parts = userInfo.split(":", 2);
                    username = parts[0];
                    password = parts[1];
                } else {
                    username = userInfo;
                    password = null;
                }
            }
            String host = state.getOptions().getHost() == null ? uri.getHost() : state.getOptions().getHost();
            int port = state.getOptions().getPort() == MainOptions.NO_SET_PORT ? uri.getPort()
                    : state.getOptions().getPort();
            String authority = host == null ? "" : host;
            if (port != MainOptions.NO_SET_PORT && port >= 0) {
                authority += ":" + port;
            }
            String databaseUrl = uri.getScheme() + "://" + authority + "/" + state.getDatabaseName();
            if (uri.getQuery() != null) {
                databaseUrl += "?" + uri.getQuery();
            }
            return DriverManager.getConnection("jdbc:" + databaseUrl, username, password);
        } catch (URISyntaxException error) {
            throw new SQLException("invalid PostgreSQL connection URL: " + url, error);
        }
    }

    private static boolean isAuxiliaryConnectionFailure(SQLException error) {
        SQLException current = error;
        while (current != null) {
            String state = current.getSQLState();
            if (state != null && state.startsWith("08")) {
                return true;
            }
            if (current instanceof SQLTransientConnectionException) {
                return true;
            }
            String message = current.getMessage();
            if (message != null && message.toLowerCase(Locale.ROOT).contains("connection attempt failed")) {
                return true;
            }
            Throwable cause = current.getCause();
            current = cause instanceof SQLException ? (SQLException) cause : null;
        }
        return false;
    }

    private void recordMvccAuxiliaryConnectionFailure(String stage, SQLException error) {
        registerVpStatsShutdownHook();
        String key = normalizeSkippedStage(stage) + ":" + normalizeSkippedError(getErrorText(error));
        synchronized (STATS_LOCK) {
            MVCC_AUXILIARY_CONNECTION_FAILURE_COUNTS.put(key,
                    MVCC_AUXILIARY_CONNECTION_FAILURE_COUNTS.getOrDefault(key, 0) + 1);
        }
        markVpStatsDirty();
    }

    private static void recordMvccSavepointSetupSkip(String reason) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            MVCC_SETUP_SKIP_COUNTS.put(reason, MVCC_SETUP_SKIP_COUNTS.getOrDefault(reason, 0) + 1);
        }
        markVpStatsDirty();
    }

    private RowIdSnapshot readRowIdSnapshot(Statement statement, String relation) throws SQLException {
        String sql = "SELECT COUNT(*), COALESCE(SUM(" + quote(ROW_ID) + "), 0), MIN(" + quote(ROW_ID) + "), MAX("
                + quote(ROW_ID) + ") FROM " + relation;
        try (ResultSet rs = statement.executeQuery(sql)) {
            if (!rs.next()) {
                throw new AssertionError("missing VP snapshot result");
            }
            return new RowIdSnapshot(rs.getLong(1), rs.getLong(2), (Long) rs.getObject(3), (Long) rs.getObject(4));
        }
    }

    private String splitSnapshotRelation(VpSchema vp) {
        return "(SELECT l." + quote(ROW_ID) + " AS " + quote(ROW_ID) + " FROM " + quote(vp.leftTable) + " l JOIN "
                + quote(vp.rightTable) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID) + ") vp_snapshot";
    }

    private void capturePlanFingerprints(VpQueryPair pair) {
        boolean directed = pair.reconstructionStrategy == ReconstructionStrategy.DIRECTED_MATERIALIZED_JOIN
                || pair.reconstructionStrategy == ReconstructionStrategy.DIRECTED_PARTITION_JOIN;
        if (!reservePlanFingerprintSample(directed)) {
            return;
        }
        try {
            PlanCapture sourcePlan = capturePlan(pair.singleQuery, pair.schema.prefix);
            PlanCapture splitPlan = capturePlan(pair.splitQuery, pair.schema.prefix);
            if (pair.schema.leftPartitioned || pair.schema.rightPartitioned) {
                if (sourcePlan.shapes.contains("partition-pruning") || splitPlan.shapes.contains("partition-pruning")
                        || sourcePlan.shapes.contains("partition-scan")
                        || splitPlan.shapes.contains("partition-scan")) {
                    recordAdvancedFeature(AdvancedFeature.PARTITION_PRUNING);
                }
                if (sourcePlan.shapes.contains("partition-aware") || splitPlan.shapes.contains("partition-aware")
                        || sourcePlan.shapes.contains("partitionwise-join")
                        || splitPlan.shapes.contains("partitionwise-join")) {
                    recordAdvancedFeature(AdvancedFeature.PARTITIONWISE_JOIN);
                }
            }
            String key = "strategy=" + pair.reconstructionStrategy + " source=" + sourcePlan.fingerprint + " split="
                    + splitPlan.fingerprint;
            synchronized (STATS_LOCK) {
                incrementString(PLAN_FINGERPRINT_COUNTS, key);
                recordPlanShapes("single", sourcePlan.shapes);
                recordPlanShapes("split", splitPlan.shapes);
                recordPlanDivergence(pair.reconstructionStrategy, directed, sourcePlan.shapes, splitPlan.shapes);
            }
            markVpStatsDirty();
        } catch (SQLException error) {
            String explain = "EXPLAIN (FORMAT JSON, COSTS FALSE, SUMMARY FALSE) " + pair.singleQuery;
            if (isDatabaseInternalError(error)) {
                recordVpReproductionStatement(explain);
                logUnexpectedQueryError(pair, "plan-fingerprint", explain, error);
                throw new AssertionError("PostgreSQL internal error while capturing a VP plan", error);
            }
            recordSkippedError("plan-fingerprint", explain, getErrorText(error));
        } catch (IOException error) {
            recordSkippedError("plan-fingerprint", "EXPLAIN (FORMAT JSON) " + pair.singleQuery, getErrorText(error));
        }
    }

    private static boolean reservePlanFingerprintSample(boolean directed) {
        synchronized (STATS_LOCK) {
            if (directed) {
                if (directedPlanFingerprintSamples >= MAX_DIRECTED_PLAN_FINGERPRINTS) {
                    return false;
                }
                directedPlanFingerprintSamples++;
                return true;
            }
            if (planFingerprintSamples >= MAX_PLAN_FINGERPRINTS) {
                return false;
            }
            planFingerprintSamples++;
            return true;
        }
    }

    private PlanCapture capturePlan(String query, String vpPrefix) throws SQLException, IOException {
        String planJson;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("EXPLAIN (FORMAT JSON, COSTS FALSE, SUMMARY FALSE) " + query)) {
            if (!rs.next()) {
                throw new SQLException("PostgreSQL EXPLAIN returned no plan");
            }
            planJson = rs.getString(1);
        }
        JsonNode root = PLAN_JSON_MAPPER.readTree(planJson);
        JsonNode plan = root.isArray() && !root.isEmpty() ? root.get(0).path("Plan") : root.path("Plan");
        if (plan.isMissingNode() || plan.isNull()) {
            throw new SQLException("PostgreSQL EXPLAIN JSON did not contain Plan");
        }
        Set<String> shapes = new LinkedHashSet<>();
        String normalized = normalizePlanNode(plan, vpPrefix, shapes);
        return new PlanCapture(Integer.toUnsignedString(normalized.hashCode(), 16), shapes);
    }

    private static String normalizePlanNode(JsonNode plan, String vpPrefix, Set<String> shapes) {
        String nodeType = plan.path("Node Type").asText("unknown");
        shapes.add(normalizePlanShape(nodeType));
        StringBuilder normalized = new StringBuilder(nodeType);
        appendPlanField(normalized, plan, "Relation Name", vpPrefix);
        appendPlanField(normalized, plan, "Index Name", vpPrefix);
        appendPlanField(normalized, plan, "Join Type", vpPrefix);
        appendPlanField(normalized, plan, "Strategy", vpPrefix);
        appendPlanField(normalized, plan, "Scan Direction", vpPrefix);
        appendPlanField(normalized, plan, "Operation", vpPrefix);
        appendPlanField(normalized, plan, "Command", vpPrefix);
        appendPlanField(normalized, plan, "Subplans Removed", vpPrefix);
        appendPlanField(normalized, plan, "Partition Pruning", vpPrefix);
        appendPlanField(normalized, plan, "Partitionwise Join", vpPrefix);
        if (plan.has("Subplans Removed")) {
            shapes.add("partition-pruning");
        }
        if (plan.has("Partition Pruning") || plan.has("Partitionwise Join")) {
            shapes.add("partition-aware");
        }
        if ("Append".equals(nodeType) || "Merge Append".equals(nodeType)
                || "Nested Loop".equals(nodeType) && plan.has("Join Type")) {
            if (plan.has("Subplans Removed") || plan.has("Partition Pruning")) {
                shapes.add("partitioned-append");
            }
        }
        JsonNode children = plan.path("Plans");
        if (children.isArray()) {
            normalized.append("[");
            for (JsonNode child : children) {
                normalized.append(normalizePlanNode(child, vpPrefix, shapes)).append(",");
            }
            normalized.append("]");
            if (("Append".equals(nodeType) || "Merge Append".equals(nodeType)) && containsJoinPlan(children)) {
                shapes.add("partitionwise-join");
            }
            // Static pruning removes child partitions during planning. Unlike
            // executor-time pruning it has no "Subplans Removed" field, but
            // a one-child Append over a generated partition child is an
            // unambiguous signature for the directed VP range predicate.
            if (("Append".equals(nodeType) || "Merge Append".equals(nodeType)) && children.size() == 1
                    && isVpPartitionChild(children.get(0))) {
                shapes.add("partition-pruning");
            }
        }
        return normalized.toString();
    }

    private static boolean isVpPartitionChild(JsonNode plan) {
        String relationName = plan.path("Relation Name").asText("");
        return relationName.matches(".*_(p1|p2)$");
    }

    private static boolean containsJoinPlan(JsonNode plans) {
        if (!plans.isArray()) {
            return false;
        }
        for (JsonNode child : plans) {
            String childType = child.path("Node Type").asText("");
            if (childType.endsWith("Join") || childType.equals("Nested Loop")) {
                return true;
            }
            if (containsJoinPlan(child.path("Plans"))) {
                return true;
            }
        }
        return false;
    }

    private static void appendPlanField(StringBuilder normalized, JsonNode plan, String field, String vpPrefix) {
        JsonNode value = plan.get(field);
        if (value != null && !value.isNull()) {
            normalized.append("|").append(field).append("=").append(normalizePlanIdentifier(value.asText(), vpPrefix));
        }
    }

    private static String normalizePlanIdentifier(String value, String vpPrefix) {
        return value.replace(vpPrefix, "vp_").replaceAll("[0-9]+", "#").replaceAll("\\s+", " ").trim();
    }

    private static String normalizePlanShape(String nodeType) {
        return nodeType.toLowerCase(Locale.ROOT).replace('-', ' ').replace('_', ' ').replaceAll("\\s+", "-");
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

    private static void recordPlanDivergence(ReconstructionStrategy strategy, boolean directed,
            Set<String> singleShapes, Set<String> splitShapes) {
        incrementString(PLAN_DIVERGENCE_COUNTS,
                "all:" + (singleShapes.equals(splitShapes) ? "same-shape" : "different-shape"));
        if (!directed) {
            return;
        }
        incrementString(PLAN_DIVERGENCE_COUNTS, "directed:sampled");
        if (strategy == ReconstructionStrategy.DIRECTED_PARTITION_JOIN) {
            boolean splitPartitionScan = splitShapes.contains("partition-scan")
                    || splitShapes.contains("partition-pruning");
            boolean splitJoin = hasPlanShape(splitShapes, "hash-join", "merge-join", "nested-loop");
            incrementString(PLAN_DIVERGENCE_COUNTS, "partition:split-pruning=" + splitPartitionScan);
            incrementString(PLAN_DIVERGENCE_COUNTS, "partition:split-join=" + splitJoin);
            incrementString(PLAN_DIVERGENCE_COUNTS,
                    splitPartitionScan && splitJoin ? "partition:pruning-join" : "partition:target-not-met");
            return;
        }
        boolean singleIndex = hasPlanShape(singleShapes, "index-scan", "index-only-scan", "bitmap-heap-scan");
        boolean splitCte = hasPlanShape(splitShapes, "cte-scan");
        boolean splitJoin = hasPlanShape(splitShapes, "hash-join", "merge-join", "nested-loop");
        incrementString(PLAN_DIVERGENCE_COUNTS, "directed:single-index=" + singleIndex);
        incrementString(PLAN_DIVERGENCE_COUNTS, "directed:split-cte=" + splitCte);
        incrementString(PLAN_DIVERGENCE_COUNTS, "directed:split-join=" + splitJoin);
        incrementString(PLAN_DIVERGENCE_COUNTS, singleIndex && splitCte && splitJoin
                ? "directed:index-to-materialized-join" : "directed:target-not-met");
    }

    private static boolean hasPlanShape(Set<String> shapes, String... candidates) {
        for (String candidate : candidates) {
            if (shapes.contains(candidate)) {
                return true;
            }
        }
        return false;
    }

    private KnownVpBug classifyKnownBug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        return null;
    }

    private KnownVpBug classifyKnownQueryError(VpQueryPair pair, String stage, String query, Throwable error) {
        boolean observedGroupingSetsFailure = isObservedGroupingSetsReconstruction(pair.reconstructionStrategy)
                && containsGroupingSets(query);
        if (!"split-query".equals(stage) || !supportsPostgresAtLeast(18)
                || pair.schema.splitStrategy == SplitStrategy.EMPTY_SPLIT || !pair.schema.leftPartitioned
                || !pair.schema.rightPartitioned || !observedGroupingSetsFailure
                || !getErrorText(error).contains(SUBPLAN_TARGET_LIST_ERROR)) {
            return null;
        }
        return KnownVpBug.PG18_PARTITIONED_GROUPING_SETS_SUBPLAN_TARGET_LIST;
    }

    private static boolean isObservedGroupingSetsReconstruction(ReconstructionStrategy strategy) {
        // Keep this known rule evidence-based. These are the reconstruction
        // shapes already observed with this exact PG18 planner failure;
        // unseen shapes must remain actionable vp-bug reports.
        return strategy == ReconstructionStrategy.JOIN || strategy == ReconstructionStrategy.NATURAL_OR_USING
                || strategy == ReconstructionStrategy.DERIVED_TABLE || strategy == ReconstructionStrategy.VIEW
                || strategy == ReconstructionStrategy.LATERAL_DERIVED
                || strategy == ReconstructionStrategy.NOT_EXISTS_ANTI || strategy == ReconstructionStrategy.JSON_PACK
                || strategy == ReconstructionStrategy.AGGREGATE_THEN_MERGE;
    }

    private static boolean containsGroupingSets(String query) {
        String normalized = query.toUpperCase(Locale.ROOT);
        return normalized.contains("GROUP BY ROLLUP") || normalized.contains("GROUP BY CUBE")
                || normalized.contains("GROUP BY GROUPING SETS") || normalized.contains("GROUP BY DISTINCT ROLLUP");
    }

    private boolean suppressKnownQueryError(VpQueryPair pair, String stage, String failingSql, Throwable error,
            KnownVpBug knownBug) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns || knownBug == null) {
            return false;
        }
        logKnownQueryError(pair, knownBug, stage, failingSql, error);
        recordKnownBugSuppressed(knownBug);
        return true;
    }

    private void logKnownBug(VpQueryPair pair, KnownVpBug knownBug, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("Known VP bug: ").append(knownBug).append(System.lineSeparator()).append(System.lineSeparator());
        sb.append(formatBugReport(pair, singleRows, splitRows));
        File dir = new File(Main.LOG_DIRECTORY, "postgres");
        try {
            Files.createDirectories(dir.toPath());
            Path target = nextKnownBugLogPath(dir);
            Files.writeString(target, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void logKnownQueryError(VpQueryPair pair, KnownVpBug knownBug, String stage, String failingSql,
            Throwable error) {
        String report;
        try {
            report = "Known VP query error: " + knownBug + System.lineSeparator() + System.lineSeparator()
                    + formatUnexpectedQueryErrorReport(pair, stage, failingSql, error);
        } catch (Throwable formattingError) {
            report = "Known VP query error: " + knownBug + System.lineSeparator() + "Stage: " + stage
                    + System.lineSeparator() + "Failing SQL: " + failingSql + System.lineSeparator() + "Original error:"
                    + System.lineSeparator() + stackTrace(error) + System.lineSeparator() + "Report formatting error:"
                    + System.lineSeparator() + stackTrace(formattingError);
        }
        File dir = new File(Main.LOG_DIRECTORY, "postgres");
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
        boolean databaseInternalError = isDatabaseInternalError(error);
        if (databaseInternalError) {
            vpDatabaseErrorLogged = true;
        }
        try {
            report = databaseInternalError
                    ? "[VERTICAL PARTITIONING DATABASE INTERNAL ERROR]" + System.lineSeparator()
                            + formatUnexpectedQueryErrorReport(pair, stage, failingSql, error)
                    : formatUnexpectedQueryErrorReport(pair, stage, failingSql, error);
        } catch (Throwable formattingError) {
            report = "[VERTICAL PARTITIONING UNEXPECTED QUERY ERROR]" + System.lineSeparator() + "Stage: " + stage
                    + System.lineSeparator() + "Failing SQL: " + failingSql + System.lineSeparator() + "Original error:"
                    + System.lineSeparator() + stackTrace(error) + System.lineSeparator() + "Report formatting error:"
                    + System.lineSeparator() + stackTrace(formattingError);
        }
        File dir = new File(Main.LOG_DIRECTORY, "postgres");
        try {
            Files.createDirectories(dir.toPath());
            Path target = databaseInternalError ? nextDatabaseBugLogPath(dir) : nextUnexpectedErrorLogPath(dir);
            Files.writeString(target, report, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
            if (state.getOptions().logEachSelect()) {
                state.getLogger().writeCurrent("-- Postgres VP query error dumped to " + target.toAbsolutePath());
            }
        } catch (IOException e) {
            System.err.println("Could not write Postgres VP unexpected query error: " + e.getMessage());
        }
    }

    private void rollbackBeforeUnexpectedReport() {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute("ROLLBACK");
        } catch (SQLException ignored) {
            // Preserve the original query error if the connection itself is
            // already unusable; report formatting is best-effort.
        }
    }

    private Path nextUnexpectedErrorLogPath(File dir) {
        Path target;
        do {
            target = new File(dir, "vp-unexpected" + UNEXPECTED_ERROR_LOG_ID.incrementAndGet() + ".log").toPath();
        } while (Files.exists(target));
        return target;
    }

    private void logUnexpectedDatabaseError(String stage, Throwable error) {
        StringBuilder report = new StringBuilder("[VERTICAL PARTITIONING DATABASE INTERNAL ERROR]")
                .append(System.lineSeparator()).append("Stage: ").append(stage).append(System.lineSeparator())
                .append("Error:").append(System.lineSeparator()).append(stackTrace(error))
                .append(System.lineSeparator()).append("VP reproduction statements:").append(System.lineSeparator());
        for (String statement : vpReproductionStatements) {
            report.append(statement);
            if (!statement.endsWith(";")) {
                report.append(';');
            }
            report.append(System.lineSeparator());
        }
        File dir = new File(Main.LOG_DIRECTORY, "postgres");
        try {
            Files.createDirectories(dir.toPath());
            Files.writeString(nextDatabaseBugLogPath(dir), report.toString(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW);
            vpDatabaseErrorLogged = true;
        } catch (IOException e) {
            System.err.println("Could not write Postgres VP database error: " + e.getMessage());
        }
    }

    private Path nextDatabaseBugLogPath(File dir) {
        Path target;
        do {
            target = new File(dir, "vp-bug" + DATABASE_BUG_LOG_ID.incrementAndGet() + ".log").toPath();
        } while (Files.exists(target));
        return target;
    }

    private static boolean isDatabaseInternalError(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof SQLException) {
                String sqlState = ((SQLException) current).getSQLState();
                if (sqlState != null && sqlState.startsWith("XX")) {
                    return true;
                }
            }
            String message = current.getMessage();
            if (message != null && message.toLowerCase(Locale.ROOT).contains("internal error")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
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
            BigDecimal l = new BigDecimal(left);
            BigDecimal r = new BigDecimal(right);
            BigDecimal difference = l.subtract(r).abs();
            BigDecimal absoluteTolerance = BigDecimal.valueOf(approximateKind.absoluteTolerance);
            BigDecimal relativeTolerance = BigDecimal.valueOf(approximateKind.relativeTolerance);
            BigDecimal scale = l.abs().max(r.abs());
            return difference.compareTo(absoluteTolerance) <= 0
                    || difference.compareTo(relativeTolerance.multiply(scale)) <= 0;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    private QueryResultMultiset getResultMultiset(String stage, String query,
            List<ApproximateKind> forcedApproximateColumns) throws SQLException {
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
                List<ApproximateKind> rowApproximateColumns = getApproximateColumns(rs, columnCount,
                        forcedApproximateColumns);
                orderedRows.add(row);
                orderedApproximateColumns.add(rowApproximateColumns);
                rows.put(row, rows.getOrDefault(row, 0) + 1);
                approximateColumns.putIfAbsent(row, rowApproximateColumns);
            }
            completed = true;
        } catch (SQLException e) {
            if (isExpectedQueryError(e)) {
                recordSkippedError(stage, query, getErrorText(e));
                throw new IgnoreMeException();
            }
            recordVpReproductionStatement(query);
            throw e;
        } catch (AssertionError e) {
            if (isSkippableQueryError(e)) {
                recordSkippedError(stage, query, getErrorText(e));
                throw new IgnoreMeException();
            }
            recordVpReproductionStatement(query);
            throw e;
        } finally {
            if (completed) {
                Main.nrSuccessfulActions.addAndGet(1);
                recordVpReproductionStatement(query);
            } else {
                Main.nrUnsuccessfulActions.addAndGet(1);
            }
        }
        return new QueryResultMultiset(rows, approximateColumns, orderedRows, orderedApproximateColumns);
    }

    private List<ApproximateKind> getApproximateColumns(SQLancerResultSet rs, int columnCount,
            List<ApproximateKind> forcedApproximateColumns) throws SQLException {
        List<ApproximateKind> approximate = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            String columnType = rs.getType(i);
            ApproximateKind columnApproximation = getApproximateColumnType(columnType);
            if (forcedApproximateColumns != null && i - 1 < forcedApproximateColumns.size()) {
                ApproximateKind forcedApproximation = forcedApproximateColumns.get(i - 1);
                if (isExactNumericType(columnType)) {
                    forcedApproximation = ApproximateKind.NONE;
                }
                columnApproximation = ApproximateKind.looser(columnApproximation, forcedApproximation);
            }
            approximate.add(columnApproximation);
        }
        return approximate;
    }

    private ApproximateKind getApproximateColumnType(String type) {
        if (type == null) {
            return ApproximateKind.NONE;
        }
        String lower = type.toLowerCase(Locale.ROOT);
        if (lower.equals("float4") || lower.equals("real")) {
            return ApproximateKind.FLOAT;
        }
        if (lower.equals("float8") || lower.equals("double precision") || lower.contains("double")) {
            return ApproximateKind.DOUBLE;
        }
        if (lower.contains("float")) {
            return ApproximateKind.DOUBLE;
        }
        return ApproximateKind.NONE;
    }

    private boolean isExactNumericType(String type) {
        if (type == null) {
            return false;
        }
        switch (type.toLowerCase(Locale.ROOT)) {
        case "int2":
        case "int4":
        case "int8":
        case "smallint":
        case "integer":
        case "bigint":
        case "numeric":
        case "decimal":
            return true;
        default:
            return false;
        }
    }

    private boolean isSkippableQueryError(AssertionError e) {
        String msg = getErrorText(e);
        if (msg.isEmpty()) {
            return false;
        }
        return isUnsupportedWindowError(msg) || isUnsupportedLateralError(msg)
                || msg.contains("VP result value too large") || msg.contains("VP result row too large")
                || msg.contains(CANONICAL_DECIMAL_TOO_LARGE) || isMissingFunctionError(msg);
    }

    private boolean isExpectedQueryError(Throwable throwable) {
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

    private boolean isUnsupportedWindowError(String msg) {
        String normalized = msg.toLowerCase(Locale.ROOT);
        return normalized.contains("window")
                && (normalized.contains("not allowed") || normalized.contains("cannot be used"));
    }

    private boolean isUnsupportedLateralError(String msg) {
        String normalized = msg.toLowerCase(Locale.ROOT);
        return normalized.contains("lateral")
                && (normalized.contains("not supported") || normalized.contains("not allowed"));
    }

    private boolean isMissingFunctionError(String msg) {
        String normalized = msg.toLowerCase(Locale.ROOT);
        return normalized.contains("function ") && normalized.contains(" does not exist");
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
                if (value.length() > MAX_CANONICAL_VALUE_LENGTH) {
                    throw new AssertionError("VP result value too large for canonical comparison: " + value.length());
                }
                String canonical = canonicalizeValue(value, rs.getType(i));
                sb.append("S:").append(canonical.length()).append(":").append(canonical).append(";");
                if (sb.length() > MAX_CANONICAL_ROW_LENGTH) {
                    throw new AssertionError("VP result row too large for canonical comparison: " + sb.length());
                }
            }
        }
        return sb.toString();
    }

    private String canonicalizeValue(String value, String typeName) {
        if (isCanonicalByteaType(typeName)) {
            return value.toLowerCase(Locale.ROOT);
        }
        if (!isCanonicalNumericType(typeName)) {
            return value;
        }
        try {
            BigDecimal decimal = new BigDecimal(value);
            long plainLength = decimal.scale() < 0 ? (long) decimal.precision() - decimal.scale()
                    : (long) decimal.precision() + decimal.scale() + 1;
            if (plainLength > MAX_CANONICAL_VALUE_LENGTH) {
                throw new AssertionError(CANONICAL_DECIMAL_TOO_LARGE + ": " + plainLength);
            }
            String plain = decimal.stripTrailingZeros().toPlainString();
            if (plain.length() > MAX_CANONICAL_VALUE_LENGTH) {
                throw new AssertionError(CANONICAL_DECIMAL_TOO_LARGE + ": " + plain.length());
            }
            return Objects.equals(plain, "-0") ? "0" : plain;
        } catch (NumberFormatException ignored) {
            return value;
        }
    }

    private static boolean isCanonicalNumericType(String typeName) {
        if (typeName == null) {
            return false;
        }
        switch (typeName.toLowerCase(Locale.ROOT)) {
        case "int2":
        case "int4":
        case "int8":
        case "smallint":
        case "integer":
        case "bigint":
        case "numeric":
        case "decimal":
        case "float4":
        case "float8":
        case "real":
        case "double precision":
        case "money":
            return true;
        default:
            return false;
        }
    }

    private static boolean isCanonicalByteaType(String typeName) {
        return typeName != null && "bytea".equals(typeName.toLowerCase(Locale.ROOT));
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
        sb.append("Postgres version: ").append(getSingleString("SELECT version()")).append(System.lineSeparator());
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
        sb.append("VP session GUC profile: ").append(activeSessionGucProfile).append(System.lineSeparator());
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
        sb.append("Postgres version: ").append(getSingleString("SELECT version()")).append(System.lineSeparator());
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
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(quote(table)).append(" (");
        List<String> columns = new ArrayList<>();
        String columnSql = "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                + "WHERE table_schema = current_schema() AND table_name = " + quoteLiteral(table)
                + " ORDER BY ordinal_position";
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(columnSql)) {
            while (rs.next()) {
                columns.add(quote(rs.getString(1)) + " " + rs.getString(2)
                        + ("NO".equals(rs.getString(3)) ? " NOT NULL" : ""));
            }
        }
        sb.append(String.join(", ", columns)).append(")");
        List<String> indexes = new ArrayList<>();
        String indexSql = "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename = "
                + quoteLiteral(table);
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(indexSql)) {
            while (rs.next()) {
                indexes.add(rs.getString(1));
            }
        }
        if (!indexes.isEmpty()) {
            sb.append("; indexes=[").append(String.join("; ", indexes)).append("]");
        }
        return sb.toString();
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
        List<String> vars = List.of("TimeZone", "statement_timeout", "extra_float_digits", "enable_hashjoin",
                "enable_mergejoin", "enable_nestloop", "enable_partitionwise_join", "enable_partition_pruning",
                "enable_partitionwise_aggregate", "enable_parallel_append", "enable_parallel_hash",
                "max_parallel_workers_per_gather", "min_parallel_table_scan_size", "parallel_setup_cost",
                "parallel_tuple_cost", "work_mem", "jit");
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
        List<String> vars = List.of("TimeZone", "statement_timeout", "extra_float_digits", "enable_hashjoin",
                "enable_mergejoin", "enable_nestloop", "enable_partitionwise_join", "enable_partition_pruning",
                "enable_partitionwise_aggregate", "enable_parallel_append", "enable_parallel_hash",
                "max_parallel_workers_per_gather", "min_parallel_table_scan_size", "parallel_setup_cost",
                "parallel_tuple_cost", "work_mem", "jit");
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
        String explain = "EXPLAIN (FORMAT TEXT) " + query;
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
        executeInternalStatement("setup", sql, sql.startsWith("CREATE TABLE"));
        vp.setupStatements.add(sql);
    }

    private void tryExecuteSetup(VpSchema vp, String sql) throws Exception {
        try {
            executeInternalStatement("optional-setup", sql, sql.startsWith("CREATE TABLE"));
            vp.setupStatements.add(sql);
        } catch (IgnoreMeException e) {
            // executeInternalStatement already recorded the expected SQL error.
        } catch (AssertionError e) {
            if (!isSkippableQueryError(e)) {
                throw e;
            }
            recordSkippedError("setup", sql, getErrorText(e));
        }
    }

    private void executeOptional(String sql) {
        try {
            executeInternalStatement("cleanup", sql, false);
        } catch (Exception | AssertionError ignored) {
        }
    }

    private void executeNoSchemaRefresh(String sql) throws Exception {
        executeInternalStatement("setup", sql, false);
    }

    private void executeInternalStatement(String stage, String sql, boolean couldAffectSchema) throws Exception {
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
        try (Statement s = state.getConnection().createStatement()) {
            s.execute(query.getQueryString());
            success = true;
        } catch (SQLException e) {
            if (isExpectedQueryError(e)) {
                recordSkippedError(stage, query.getQueryString(), getErrorText(e));
                throw new IgnoreMeException();
            } else {
                if (!"cleanup".equals(stage)) {
                    recordVpReproductionStatement(query.getQueryString());
                }
                throw e;
            }
        } finally {
            if (success) {
                Main.nrSuccessfulActions.addAndGet(1);
                if (!"cleanup".equals(stage)) {
                    recordVpReproductionStatement(query.getQueryString());
                }
            } else {
                Main.nrUnsuccessfulActions.addAndGet(1);
            }
        }
        if (state.getOptions().logExecutionTime()) {
            state.getLogger().writeCurrent(" -- " + timer.end().asString());
        }
        if (success && couldAffectSchema) {
            state.updateSchema();
        }
    }

    private void recordVpReproductionStatements(String sqlBatch) {
        for (String statement : sqlBatch.split("\\R")) {
            if (!statement.isBlank() && !statement.startsWith("--")) {
                recordVpReproductionStatement(statement);
            }
        }
    }

    private void recordVpReproductionStatement(String sql) {
        if (sql != null && !sql.isBlank()) {
            vpReproductionStatements.add(sql);
        }
    }

    private void flushVpReproductionState() {
        if (vpReproductionStateFlushed) {
            return;
        }
        OracleRunReproductionState localState = state.getState().getLocalState();
        if (localState == null) {
            return;
        }
        for (String statement : vpReproductionStatements) {
            localState.log(statement);
        }
        vpReproductionStateFlushed = true;
    }

    private String toSQLType(PostgresDataType type) {
        switch (type) {
        case INT:
            return "BIGINT";
        case BOOLEAN:
            return "BOOLEAN";
        case TEXT:
            return "TEXT";
        case DECIMAL:
            return "NUMERIC";
        case FLOAT:
            return "DOUBLE PRECISION";
        case REAL:
            return "REAL";
        case RANGE:
            return "INT4RANGE";
        case MONEY:
            return "MONEY";
        case BIT:
            return "BIT VARYING";
        case INET:
            return "INET";
        case UUID:
            return "UUID";
        case BYTEA:
            return "BYTEA";
        case MACADDR:
            return "MACADDR";
        case TSVECTOR:
            return "TSVECTOR";
        case TSQUERY:
            return "TSQUERY";
        case ARRAY_INT:
            return "BIGINT[]";
        case ARRAY_TEXT:
            return "TEXT[]";
        case ARRAY_NUMERIC:
            return "NUMERIC[]";
        case JSONB:
            return "JSONB";
        case OTHER:
            throw new IgnoreMeException();
        default:
            throw new AssertionError(type);
        }
    }

    private String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private String quoteLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private String sanitizeIdentifier(String identifier) {
        return identifier.replaceAll("[^A-Za-z0-9_]", "_");
    }
}
