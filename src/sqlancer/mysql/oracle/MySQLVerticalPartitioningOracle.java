package sqlancer.mysql.oracle;

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

import sqlancer.IgnoreMeException;
import sqlancer.ExecutionTimer;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLProvider;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLDataType;
import sqlancer.mysql.MySQLSchema.MySQLIndex;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLSchema.MySQLTable.MySQLEngine;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLConstant;
import sqlancer.mysql.ast.MySQLAggregate;
import sqlancer.mysql.ast.MySQLAggregate.MySQLAggregateFunction;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLTableReference;
import sqlancer.mysql.ast.MySQLText;
import sqlancer.mysql.gen.MySQLExpressionGenerator;

public class MySQLVerticalPartitioningOracle implements TestOracle<MySQLGlobalState> {

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
    private static final File STATS_FILE = new File(new File(Main.LOG_DIRECTORY, "mysql"), "vp-stats.log");
    private static int completedOracleRuns;
    private static int completedQueryPairs;
    private static int completedAttempts;
    private static int schemaShapeSamples;

    private final MySQLGlobalState state;
    private final ExpectedErrors errors = new ExpectedErrors();

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
        MYSQL_EMPTY_TABLE_LEFT_RIGHT_CONSTANT_AGG_DIFF, MYSQL_TEXT_DECIMAL_AGG_OVERFLOW_DIFF,
        MYSQL_SINGLE_COVERING_INDEX_GROUP_OR_DISTINCT_LOSS, MYSQL_ROW_NUMBER_ALIGNMENT_NULL_MARKER_CORRUPTION,
        MYSQL_ZERO_ROWS_SHORTCUT_WRONG_RESULT, SCALAR_SUBQUERY_DEDUP_NULLIFICATION,
        MYSQL_NOT_EXISTS_ANTI_IFNULL_PROJECTION_NULLIFICATION, MYSQL_LATERAL_DERIVED_IFNULL_PROJECTION_NULLIFICATION,
        MYSQL_STRAIGHT_JOIN_IFNULL_GROUP_OR_DISTINCT_LOSS, VIEW_TEMP_MATERIALIZE_NULLIFICATION,
        MYSQL_STRING_PACK_GROUP_KEY_COLLAPSE_TO_NULL, MYSQL_JSON_PACK_DISTINCT_AGG_DROPS_NON_NULL,
        MYSQL_AGGREGATE_THEN_MERGE_EXPR_COLUMN_MISBIND_TO_NULL, MYSQL_LEFT_RIGHT_JOIN_FILTER_MISBINDS_NULLABLE_COLUMN,
        MYSQL_COLLATION_MISMATCH_TEXT_COMPARISON_FALSE_POSITIVE, MYSQL_ROW_NUMBER_ALIGNMENT_PARTIAL_NULLIFICATION,
        MYSQL_TEMP_TABLE_IFNULL_GROUP_OR_DISTINCT_LOSS, MYSQL_EXISTS_IN_DERIVED_JOIN_IFNULL_NULLIFICATION,
        MYSQL_ROLLUP_ZERO_INPUT_SHORTCUT_INCONSISTENCY, MYSQL_FULL_OUTER_SIMULATION_IFNULL_NULL_TO_ZERO,
        MYSQL_STRING_PACK_WINDOW_PROJECTION_NULLIFICATION, MYSQL_LEFT_RIGHT_JOIN_IFNULL_SUM_ZERO_FLIP,
        MYSQL_NOT_EXISTS_ANTI_COUNT_IFNULL_ZERO_FLIP, MYSQL_LEFT_RIGHT_JOIN_COVERING_IFNULL_BIT_OR_ROW_LOSS,
        MYSQL_LATERAL_DERIVED_DEDUP_CAST_IFNULL_ROW_GAIN, MYSQL_AGGREGATE_THEN_MERGE_ROLLUP_IFNULL_KEY_LEAK,
        MYSQL_STRING_PACK_DISTINCT_CASE_FLOAT_SUM_ROUNDING, MYSQL_AGGREGATE_THEN_MERGE_ROLLUP_CONSTANT_XOR_KEY_NULLING,
        MYSQL_ROW_NUMBER_ALIGNMENT_DECIMAL_STRING_INT_INDEX_FALSE_MATCH,
        MYSQL_NATURAL_USING_DISTINCT_GROUP_PARTIAL_DEDUP_ROW_LOSS,
        MYSQL_NOT_EXISTS_ANTI_IFNULL_CASE_AGG_VALUE_CORRUPTION, MYSQL_VIEW_DISTINCT_IFNULL_TEMP_DEDUP_NULLIFICATION,
        MYSQL_120429_DERIVED_DECIMAL_DESC_INDEX_CASE_IFNULL_FILTER_AGG,
        MYSQL_120429_RIGHT_JOIN_DECIMAL_IFNULL_FALLBACK_NULLIFICATION,
        MYSQL_120429_AGGREGATE_THEN_MERGE_DECIMAL_IFNULL_CASE_FILTER_LOSS,
        MYSQL_120429_JSON_PACK_DECIMAL_IFNULL_GROUP_KEY_NULL_COLLAPSE,
        MYSQL_120429_DISTINCT_DECIMAL_IFNULL_COVERING_DEDUP_NULL_COLLAPSE,
        MYSQL_120429_FULL_OUTER_DECIMAL_IFNULL_XOR_SUM_ZERO
    }

    public enum AdvancedFeature {
        ROLLUP, CORRELATED_SUBQUERY, GROUP_CONCAT, ASYMMETRIC_INDEX, CHARSET_COLLATION_COLUMN, BOUNDARY_MUTATION,
        PARTITION_RANGE, PARTITION_LIST, PARTITION_HASH, PARTITION_KEY, PARTITION_PRUNING, EXCHANGE_PARTITION,
        INNODB_FOREIGN_KEY_CASCADE, INNODB_UNIQUE_KEY, TRANSACTION_ISOLATION
    }

    private static final class VpColumn {
        private final String name;
        private final MySQLDataType type;

        private VpColumn(String name, MySQLDataType type) {
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
        private final MySQLEngine engine;

        private BaseTableInfo(String tableName, List<VpColumn> columns, BaseShape shape, String stableOrderStrategy,
                long rowCount, MySQLEngine engine) {
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
        private final MySQLEngine baseEngine;
        private final Map<String, String> columnCollationClauses = new LinkedHashMap<>();
        private MySQLEngine sourceEngine;
        private MySQLEngine leftEngine;
        private MySQLEngine rightEngine;
        private final List<String> setupStatements = new ArrayList<>();

        private VpSchema(String prefix, String baseTable, String sourceTable, String leftTable, String rightTable,
                String viewName, String tempTable, List<VpColumn> columns, List<VpColumn> leftColumns,
                List<VpColumn> rightColumns, List<VpColumn> overlapColumns, BaseShape baseShape,
                SplitStrategy splitStrategy, boolean usesPhysicalForeignKey, String stableOrderStrategy,
                long baseRowCount, MySQLEngine baseEngine) {
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
            this.sourceEngine = MySQLEngine.INNO_DB;
            this.leftEngine = MySQLEngine.INNO_DB;
            this.rightEngine = MySQLEngine.INNO_DB;
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

    public MySQLVerticalPartitioningOracle(MySQLGlobalState state) {
        this.state = state;
        registerVpStatsShutdownHook();
        MySQLErrors.addExpressionErrors(errors);
        errors.add("This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'");
        errors.add("You have an error in your SQL syntax");
        errors.add("doesn't yet support");
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
        errors.add("Unknown collation");
        errors.add("COLLATION");
        errors.add("Unknown character set");
    }

    @Override
    public void check() throws Exception {
        resetSessionForVerticalPartitioningOracle();
        MySQLProvider.ensureVerticalPartitioningBaseTables(state, 2);
        BaseTableInfo baseTable = selectBaseTable();
        int queryVariantsPerCombination = Math.max(1,
                state.getDbmsSpecificOptions().verticalPartitioningQueryVariantsPerCombination);
        int checkedPairs = 0;
        int attempts = 0;
        List<SplitStrategy> splitSchedule = createSplitSchedule(baseTable.shape);
        boolean deepFeatureChecksCompleted = false;
        for (SplitStrategy splitStrategy : splitSchedule) {
            VpSchema vp = materializeTable(baseTable, splitStrategy);
            incrementSplitStrategy(vp.splitStrategy);
            if (deepExplorationEnabled() && !deepFeatureChecksCompleted) {
                runDeepMySQLFeatureChecks(vp);
                deepFeatureChecksCompleted = true;
            }
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
        MySQLProvider.replaceVerticalPartitioningTableAfterSuccessfulCheck(state, baseTable.tableName);
    }

    private BaseTableInfo selectBaseTable() throws Exception {
        state.updateSchema();
        List<MySQLTable> candidates = state.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> !t.getName().startsWith("vp_")).filter(t -> !t.getColumns().isEmpty())
                .collect(Collectors.toList());
        if (candidates.isEmpty()) {
            throw new IgnoreMeException();
        }
        return createBaseTableInfo(Randomly.fromList(candidates));
    }

    private BaseTableInfo createBaseTableInfo(MySQLTable base) throws Exception {
        List<VpColumn> sourceColumns = base.getColumns().stream().map(c -> new VpColumn(c.getName(), c.getType()))
                .collect(Collectors.toList());
        long rowCount = getRowCount(base.getName());
        BaseShape shape = rowCount == 0 ? BaseShape.EMPTY
                : sourceColumns.size() == 1 ? BaseShape.SINGLE_COLUMN : BaseShape.MULTI_COLUMN;
        incrementSelectedBaseShape(shape);
        String stableOrder = getStableOrder(base, sourceColumns);
        return new BaseTableInfo(base.getName(), sourceColumns, shape, stableOrder, rowCount, base.getEngine());
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
        executeNoSchemaRefresh("SET SESSION sql_mode = DEFAULT");
        executeNoSchemaRefresh("SET SESSION time_zone = '+00:00'");
        executeNoSchemaRefresh("SET SESSION sql_auto_is_null = OFF");
        executeNoSchemaRefresh("SET SESSION group_concat_max_len = 1048576");
        executeNoSchemaRefresh("SET SESSION tmp_table_size = DEFAULT");
        executeNoSchemaRefresh("SET SESSION max_heap_table_size = DEFAULT");
        executeNoSchemaRefresh("SET SESSION join_buffer_size = DEFAULT");
        executeNoSchemaRefresh("SET SESSION sort_buffer_size = DEFAULT");
        executeNoSchemaRefresh("SET SESSION read_buffer_size = DEFAULT");
        executeNoSchemaRefresh("SET SESSION read_rnd_buffer_size = DEFAULT");
        executeNoSchemaRefresh("SET SESSION optimizer_switch = DEFAULT");
        executeNoSchemaRefresh("SET SESSION unique_checks = DEFAULT");
        executeNoSchemaRefresh("SET SESSION foreign_key_checks = DEFAULT");
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
    }

    /*
     * These probes deliberately use small, deterministic tables. Randomly partitioning normal SQLancer tables is
     * usually invalid because MySQL requires every unique key to include the partitioning columns.
     */
    private void runDeepMySQLFeatureChecks(VpSchema vp) throws Exception {
        // Random SQLancer SET actions may have changed GLOBAL defaults. These checks require enforced FK semantics
        // and a transaction which does not silently start a chained successor after a statement completes.
        executeNoSchemaRefresh("SET SESSION foreign_key_checks = 1");
        executeNoSchemaRefresh("SET SESSION completion_type = NO_CHAIN");
        executeNoSchemaRefresh("SET SESSION optimizer_prune_level = 1");
        try {
            runPartitionFeatureChecks(vp);
            runInnoDBConstraintChecks(vp);
            runTransactionIsolationChecks(vp);
        } finally {
            executeNoSchemaRefresh("SET SESSION foreign_key_checks = DEFAULT");
            executeNoSchemaRefresh("SET SESSION completion_type = DEFAULT");
            executeNoSchemaRefresh("SET SESSION optimizer_prune_level = DEFAULT");
        }
    }

    private void runPartitionFeatureChecks(VpSchema vp) throws Exception {
        String range = vp.prefix + "partition_range";
        String list = vp.prefix + "partition_list";
        String hash = vp.prefix + "partition_hash";
        String key = vp.prefix + "partition_key";
        String exchange = vp.prefix + "partition_exchange";
        try {
            executeDeepStatement("CREATE TABLE " + quote(range)
                    + " (id INT NOT NULL, payload INT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB "
                    + "PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), "
                    + "PARTITION p1 VALUES LESS THAN (20), PARTITION pmax VALUES LESS THAN MAXVALUE)");
            executeDeepStatement("INSERT INTO " + quote(range) + " VALUES (1, 101), (12, 112), (25, 125)");
            recordAdvancedFeature(AdvancedFeature.PARTITION_RANGE);
            if (verifyPartitionAccessAndObservePruning(range, "id = 12", "p1")) {
                recordAdvancedFeature(AdvancedFeature.PARTITION_PRUNING);
            }

            executeDeepStatement("CREATE TABLE " + quote(exchange)
                    + " (id INT NOT NULL, payload INT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB");
            executeDeepStatement("INSERT INTO " + quote(exchange) + " VALUES (15, 115)");
            executeDeepStatement(
                    "ALTER TABLE " + quote(range) + " EXCHANGE PARTITION p1 WITH TABLE " + quote(exchange));
            if (queryLong("SELECT COUNT(*) FROM " + quote(range) + " WHERE id = 15") != 1
                    || queryLong("SELECT COUNT(*) FROM " + quote(exchange) + " WHERE id = 12") != 1) {
                throw new AssertionError("VP deep EXCHANGE PARTITION changed row ownership");
            }
            recordAdvancedFeature(AdvancedFeature.EXCHANGE_PARTITION);

            executeDeepStatement("CREATE TABLE " + quote(list)
                    + " (id INT NOT NULL, bucket INT NOT NULL, PRIMARY KEY (id, bucket)) ENGINE=InnoDB "
                    + "PARTITION BY LIST (bucket) (PARTITION p0 VALUES IN (0, 2), PARTITION p1 VALUES IN (1, 3))");
            executeDeepStatement("INSERT INTO " + quote(list) + " VALUES (1, 0), (2, 1)");
            recordAdvancedFeature(AdvancedFeature.PARTITION_LIST);
            if (verifyPartitionAccessAndObservePruning(list, "bucket = 1", "p1")) {
                recordAdvancedFeature(AdvancedFeature.PARTITION_PRUNING);
            }

            executeDeepStatement("CREATE TABLE " + quote(hash)
                    + " (id INT NOT NULL, payload INT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB "
                    + "PARTITION BY HASH (id) PARTITIONS 4");
            executeDeepStatement("INSERT INTO " + quote(hash) + " VALUES (1, 1), (2, 2)");
            recordAdvancedFeature(AdvancedFeature.PARTITION_HASH);
            observePartitionPlan(hash, "id = 1");

            executeDeepStatement("CREATE TABLE " + quote(key)
                    + " (id INT NOT NULL, payload INT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB "
                    + "PARTITION BY KEY (id) PARTITIONS 4");
            executeDeepStatement("INSERT INTO " + quote(key) + " VALUES (1, 1), (2, 2)");
            recordAdvancedFeature(AdvancedFeature.PARTITION_KEY);
            observePartitionPlan(key, "id = 1");
        } catch (SQLException | AssertionError e) {
            if (e instanceof AssertionError) {
                throw (AssertionError) e;
            }
            // Older MySQL builds can disable partitioning or EXCHANGE PARTITION. Keep the ordinary VP run useful.
        } finally {
            dropDeepTable(exchange);
            dropDeepTable(key);
            dropDeepTable(hash);
            dropDeepTable(list);
            dropDeepTable(range);
        }
    }

    private void runInnoDBConstraintChecks(VpSchema vp) throws Exception {
        String parent = vp.prefix + "innodb_parent";
        String child = vp.prefix + "innodb_child";
        try {
            executeDeepStatement(
                    "CREATE TABLE " + quote(parent) + " (id INT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB");
            executeDeepStatement("CREATE TABLE " + quote(child)
                    + " (id INT NOT NULL, parent_id INT NOT NULL, token INT NOT NULL, PRIMARY KEY (id), "
                    + "UNIQUE KEY " + quote(vp.prefix + "uq_child") + " (parent_id, token), " + "CONSTRAINT "
                    + quote(vp.prefix + "fk_child_parent") + " FOREIGN KEY (parent_id) REFERENCES " + quote(parent)
                    + " (id) ON DELETE CASCADE) ENGINE=InnoDB");
            executeDeepStatement("INSERT INTO " + quote(parent) + " VALUES (1)");
            executeDeepStatement("INSERT INTO " + quote(child) + " VALUES (1, 1, 7)");
            try {
                executeDeepStatement("INSERT INTO " + quote(child) + " VALUES (2, 1, 7)");
                throw new AssertionError("VP deep InnoDB UNIQUE KEY accepted a duplicate");
            } catch (SQLException expectedDuplicate) {
                recordAdvancedFeature(AdvancedFeature.INNODB_UNIQUE_KEY);
            }
            executeDeepStatement("DELETE FROM " + quote(parent) + " WHERE id = 1");
            if (queryLong("SELECT COUNT(*) FROM " + quote(child)) != 0) {
                throw new AssertionError("VP deep InnoDB ON DELETE CASCADE did not remove child rows");
            }
            recordAdvancedFeature(AdvancedFeature.INNODB_FOREIGN_KEY_CASCADE);
        } catch (SQLException | AssertionError e) {
            if (e instanceof AssertionError) {
                throw (AssertionError) e;
            }
        } finally {
            dropDeepTable(child);
            dropDeepTable(parent);
        }
    }

    private void runTransactionIsolationChecks(VpSchema vp) throws Exception {
        String reconstructedCount = "SELECT COUNT(*) FROM " + quote(vp.leftTable) + " l JOIN " + quote(vp.rightTable)
                + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID);
        try (Statement statement = state.getConnection().createStatement()) {
            for (String isolation : List.of("READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE")) {
                statement.execute("SET SESSION TRANSACTION ISOLATION LEVEL " + isolation);
                statement.execute("START TRANSACTION READ ONLY");
                long sourceCount = queryLong(statement, "SELECT COUNT(*) FROM " + quote(vp.sourceTable));
                long splitCount = queryLong(statement, reconstructedCount);
                statement.execute("ROLLBACK");
                if (sourceCount != splitCount) {
                    throw new AssertionError(
                            "VP deep " + isolation + " transaction saw different source and split counts");
                }
                recordAdvancedFeature(AdvancedFeature.TRANSACTION_ISOLATION);
            }
        } finally {
            executeNoSchemaRefresh("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        }
    }

    private boolean verifyPartitionAccessAndObservePruning(String table, String predicate, String expectedPartition)
            throws SQLException {
        long normalRows = queryLong("SELECT COUNT(*) FROM " + quote(table) + " WHERE " + predicate);
        long explicitPartitionRows = queryLong("SELECT COUNT(*) FROM " + quote(table) + " PARTITION ("
                + quote(expectedPartition) + ") WHERE " + predicate);
        if (normalRows != explicitPartitionRows) {
            throw new AssertionError(
                    "VP deep explicit partition access disagrees with the unpartitioned scan for " + expectedPartition);
        }
        String partitions = explainPartitions(table, predicate);
        return expectedPartition.equalsIgnoreCase(partitions);
    }

    private void observePartitionPlan(String table, String predicate) throws SQLException {
        explainPartitions(table, predicate);
    }

    private String explainPartitions(String table, String predicate) throws SQLException {
        try (Statement statement = state.getConnection().createStatement(); ResultSet rs = statement
                .executeQuery("EXPLAIN FORMAT=TRADITIONAL SELECT * FROM " + quote(table) + " WHERE " + predicate)) {
            int partitionsColumn = -1;
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                if ("partitions".equalsIgnoreCase(rs.getMetaData().getColumnLabel(i))) {
                    partitionsColumn = i;
                    break;
                }
            }
            if (partitionsColumn < 0 || !rs.next()) {
                return null;
            }
            return rs.getString(partitionsColumn);
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

    private String getStableOrder(MySQLTable base, List<VpColumn> columns) {
        List<String> primary = base.getColumns().stream().filter(MySQLColumn::isPrimaryKey).map(c -> quote(c.getName()))
                .collect(Collectors.toList());
        if (!primary.isEmpty()) {
            return String.join(", ", primary);
        }
        List<String> comparable = columns.stream()
                .filter(c -> c.type != MySQLDataType.FLOAT && c.type != MySQLDataType.DOUBLE).map(c -> quote(c.name))
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

    private MySQLEngine helperEngineCandidate(BaseTableInfo base) {
        if (state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine) {
            return base.engine;
        }
        return MySQLEngine.INNO_DB;
    }

    private void createSourceTable(VpSchema vp, MySQLEngine preferredEngine) throws Exception {
        vp.sourceEngine = createTableWithEngineFallback(vp, vp.sourceTable, sourceColumnDefs(vp), preferredEngine);
    }

    private void initializeColumnCollations(VpSchema vp) {
        for (VpColumn column : vp.columns) {
            String clause = "";
            if (deepExplorationEnabled() && column.type == MySQLDataType.VARCHAR && Randomly.getBoolean()) {
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

    private void createPartitionTables(VpSchema vp, MySQLEngine preferredEngine) throws Exception {
        vp.leftEngine = createPartitionTable(vp, vp.leftTable, vp.leftColumns, preferredEngine);
        vp.rightEngine = createPartitionTable(vp, vp.rightTable, vp.rightColumns, preferredEngine);
    }

    private MySQLEngine createPartitionTable(VpSchema vp, String tableName, List<VpColumn> columns,
            MySQLEngine preferredEngine) throws Exception {
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

    private MySQLEngine createTableWithEngineFallback(VpSchema vp, String tableName, List<String> columnDefs,
            MySQLEngine preferredEngine) throws Exception {
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

    private String createTableSQL(String tableName, List<String> columnDefs, MySQLEngine engine) {
        return "CREATE TABLE " + quote(tableName) + " (" + String.join(", ", columnDefs) + ") ENGINE="
                + engineName(engine);
    }

    private boolean canFallbackHelperEngine(MySQLEngine engine) {
        return state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine && engine != MySQLEngine.INNO_DB;
    }

    private MySQLEngine recreateHelperTableWithInnoDB(VpSchema vp, String tableName, List<String> columnDefs)
            throws Exception {
        executeSetup(vp, "DROP TABLE IF EXISTS " + quote(tableName));
        executeSetup(vp, createTableSQL(tableName, columnDefs, MySQLEngine.INNO_DB));
        return MySQLEngine.INNO_DB;
    }

    private void populatePartitionTables(VpSchema vp) throws Exception {
        vp.leftEngine = populatePartitionTable(vp, vp.leftTable, vp.leftColumns, vp.leftEngine);
        vp.rightEngine = populatePartitionTable(vp, vp.rightTable, vp.rightColumns, vp.rightEngine);
    }

    private MySQLEngine populatePartitionTable(VpSchema vp, String tableName, List<VpColumn> columns,
            MySQLEngine engine) throws Exception {
        try {
            executeSetup(vp, populatePartitionSQL(tableName, columns, vp.sourceTable));
            return engine;
        } catch (AssertionError e) {
            if (!canFallbackHelperEngine(engine)) {
                throw e;
            }
            MySQLEngine fallbackEngine = recreateHelperTableWithInnoDB(vp, tableName, partitionColumnDefs(vp, columns));
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
        return columns.stream().filter(c -> c.type != MySQLDataType.VARCHAR).collect(Collectors.toList());
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
        MySQLExpressionGenerator gen = new MySQLExpressionGenerator(state).setColumns(sourceAst.columns);
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()) {
            return generateCorrelatedSubqueryPair(vp, strategy, sourceAst);
        }
        MySQLSelect select = new MySQLSelect();
        select.setSelectType(Randomly.fromOptions(MySQLSelect.SelectType.values()));
        int fetchColumns = Math.max(1, Randomly.smallNumber() + 1);
        boolean useAggregate = Randomly.getBoolean();
        boolean hasWindowExpression = false;
        List<MySQLExpression> selected = new ArrayList<>();
        List<MySQLExpression> nonAggregateExpressions = new ArrayList<>();
        for (int i = 0; i < fetchColumns; i++) {
            MySQLExpression expr;
            if (!useAggregate && advancedFeaturesEnabled() && Randomly.getBooleanWithRatherLowProbability()) {
                expr = generateAdvancedSelectExpression(sourceAst, gen);
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
            selected.add(new MySQLColumnReference(Randomly.fromList(sourceAst.columns), null));
        }
        select.setFetchColumns(selected);
        select.setFromList(List.of(new MySQLTableReference(sourceAst.table)));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpression());
        }
        if (useAggregate || (!hasWindowExpression && Randomly.getBooleanWithRatherLowProbability())) {
            List<MySQLExpression> groupByExpressions = chooseGroupByExpressions(nonAggregateExpressions);
            select.setGroupByExpressions(groupByExpressions);
            if (Randomly.getBoolean() && !groupByExpressions.isEmpty()) {
                select.setHavingClause(generateHavingExpression(gen, sourceAst.columns));
            }
            if (advancedFeaturesEnabled() && !hasWindowExpression && !groupByExpressions.isEmpty()
                    && Randomly.getBooleanWithSmallProbability()) {
                recordAdvancedFeature(AdvancedFeature.ROLLUP);
                select.setGroupByExpressions(appendRollup(groupByExpressions));
            }
        }
        if (Randomly.getBoolean()) {
            List<MySQLExpression> orderBys = nonAggregateExpressions.isEmpty()
                    ? List.of(new MySQLColumnReference(sourceAst.rowIdColumn, null)) : gen.generateOrderBys();
            select.setOrderByClauses(orderBys);
        }
        boolean orderedComparisonEnabled = isRowPreservingQuery(useAggregate, select)
                && hasVpRowIdTieBreaker(select, sourceAst.rowIdColumn);
        if (Randomly.getBoolean()) {
            boolean rowPreservingQuery = isRowPreservingQuery(useAggregate, select);
            if (select.getOrderByClauses().isEmpty()) {
                select.setOrderByClauses(List.of(new MySQLColumnReference(sourceAst.rowIdColumn, null)));
                orderedComparisonEnabled = rowPreservingQuery;
            } else if (rowPreservingQuery) {
                List<MySQLExpression> orderBys = new ArrayList<>(select.getOrderByClauses());
                orderBys.add(new MySQLColumnReference(sourceAst.rowIdColumn, null));
                select.setOrderByClauses(orderBys);
                orderedComparisonEnabled = true;
            } else {
                select.setOrderByClauses(List.of(new MySQLColumnReference(sourceAst.rowIdColumn, null)));
                orderedComparisonEnabled = false;
            }
            select.setSelectType(MySQLSelect.SelectType.ALL);
            select.setLimitClause(MySQLConstant.createIntConstant(Randomly.getNotCachedInteger(0, 25)));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                select.setOffsetClause(MySQLConstant.createIntConstant(Randomly.getNotCachedInteger(0, 5)));
            }
        }
        String singleQuery = MySQLVisitor.asString(select);
        if (advancedFeaturesEnabled() && !useAggregate && select.getOrderByClauses().isEmpty()
                && select.getLimitClause() == null && select.getOffsetClause() == null
                && strategy != ReconstructionStrategy.TEMP_TABLE && Randomly.getBooleanWithSmallProbability()) {
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

    private List<MySQLExpression> appendRollup(List<MySQLExpression> groupByExpressions) {
        List<MySQLExpression> rollup = new ArrayList<>(groupByExpressions);
        int last = rollup.size() - 1;
        rollup.set(last, new MySQLText(MySQLVisitor.asString(rollup.get(last)) + " WITH ROLLUP"));
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

    private boolean isGroupByOrdinalLiteral(MySQLExpression expression) {
        return expression instanceof MySQLConstant && ((MySQLConstant) expression).isInt();
    }

    private List<MySQLExpression> chooseGroupByExpressions(List<MySQLExpression> nonAggregateExpressions) {
        List<MySQLExpression> groupable = nonAggregateExpressions.stream().filter(e -> !isGroupByOrdinalLiteral(e))
                .collect(Collectors.toList());
        if (!advancedFeaturesEnabled() || groupable.size() <= 1) {
            return groupable;
        }
        Collections.shuffle(groupable);
        return groupable.subList(0, state.getRandomly().getInteger(1, Math.min(3, groupable.size()) + 1));
    }

    private MySQLExpression generateHavingExpression(MySQLExpressionGenerator gen, List<MySQLColumn> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            return new MySQLText(
                    Randomly.fromOptions("COUNT(*) > 0", "SUM(" + numericExpressionForSQL(columns) + ") IS NOT NULL",
                            "MIN(" + simpleColumnOrRowIdForSQL(columns) + ") <=> MAX("
                                    + simpleColumnOrRowIdForSQL(columns) + ")"));
        }
        return gen.generateExpression();
    }

    private MySQLExpression generateAdvancedSelectExpression(SourceAst sourceAst, MySQLExpressionGenerator gen) {
        String order = simpleOrderExpressionForSQL(sourceAst.columns) + ", " + quote(ROW_ID);
        String value = simpleColumnOrRowIdForSQL(sourceAst.columns);
        String numeric = numericExpressionForSQL(sourceAst.columns);
        return new MySQLText(Randomly.fromOptions("ROW_NUMBER() OVER (ORDER BY " + order + ")",
                "RANK() OVER (ORDER BY " + order + ")", "LAG(" + value + ") OVER (ORDER BY " + quote(ROW_ID) + ")",
                "SUM(" + numeric + ") OVER (ORDER BY " + quote(ROW_ID) + ")"));
    }

    private String createUnionAllBranch(SourceAst sourceAst, MySQLExpressionGenerator gen, int fetchColumns) {
        MySQLSelect right = new MySQLSelect();
        right.setSelectType(MySQLSelect.SelectType.ALL);
        List<MySQLExpression> selected = new ArrayList<>();
        for (int i = 0; i < fetchColumns; i++) {
            selected.add(Randomly.getBooleanWithRatherLowProbability()
                    ? generateAdvancedSelectExpression(sourceAst, gen) : gen.generateExpression());
        }
        right.setFetchColumns(selected);
        right.setFromList(List.of(new MySQLTableReference(sourceAst.table)));
        if (Randomly.getBoolean()) {
            right.setWhereClause(gen.generateExpression());
        }
        return MySQLVisitor.asString(right);
    }

    private boolean isRowPreservingQuery(boolean useAggregate, MySQLSelect select) {
        return !useAggregate && select.getGroupByExpressions().isEmpty()
                && select.getFromOptions() == MySQLSelect.SelectType.ALL;
    }

    private boolean hasVpRowIdTieBreaker(MySQLSelect select, MySQLColumn rowIdColumn) {
        List<MySQLExpression> orderBys = select.getOrderByClauses();
        if (orderBys.isEmpty()) {
            return false;
        }
        MySQLExpression last = orderBys.get(orderBys.size() - 1);
        return last instanceof MySQLColumnReference
                && ((MySQLColumnReference) last).getColumn().getName().equals(rowIdColumn.getName());
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
        sb.append("-- [vp-stats] event=process-final dbms=mysql");
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

    public static void logSchemaShapeStats(MySQLGlobalState state, String phase) throws Exception {
        registerVpStatsShutdownHook();
        int emptySingle = 0;
        int emptyMulti = 0;
        int nonEmptySingle = 0;
        int nonEmptyMulti = 0;
        state.updateSchema();
        for (MySQLTable table : state.getSchema().getDatabaseTables()) {
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
            Runtime.getRuntime().addShutdownHook(new Thread(MySQLVerticalPartitioningOracle::writeFinalVpStats));
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

    private MySQLExpression generateVpAggregate(MySQLExpressionGenerator gen, List<MySQLColumn> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            return new MySQLText(randomAdvancedAggregateSQL(columns));
        }
        MySQLAggregateFunction func = Randomly.fromOptions(MySQLAggregateFunction.values());
        if (func == MySQLAggregateFunction.SUM || func == MySQLAggregateFunction.SUM_DISTINCT) {
            return new MySQLAggregate(List.of(generateSumInputExpression(gen, columns)), func);
        }
        return gen.generateAggregate();
    }

    private MySQLExpression generateSumInputExpression(MySQLExpressionGenerator gen, List<MySQLColumn> columns) {
        if (Randomly.getBoolean()) {
            MySQLColumn column = Randomly.fromList(columns);
            MySQLColumnReference ref = new MySQLColumnReference(column, null);
            if (column.getType() == MySQLDataType.VARCHAR) {
                if (state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns
                        || advancedFeaturesEnabled()) {
                    if (state.getRandomly().getInteger(0, 100) < 70) {
                        return new MySQLText("CAST(" + MySQLVisitor.asString(ref) + " AS " + randomSumCastType() + ")");
                    }
                    return gen.generateExpression();
                }
                return new MySQLText("CAST(" + MySQLVisitor.asString(ref) + " AS DECIMAL(65, 30))");
            }
            return ref;
        }
        return gen.generateExpression();
    }

    private String randomSumCastType() {
        return Randomly.fromOptions("SIGNED", "UNSIGNED", "DOUBLE", "DECIMAL(10, 2)", "DECIMAL(20, 5)",
                "DECIMAL(65, 30)");
    }

    private String randomAdvancedAggregateSQL(List<MySQLColumn> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()) {
            recordAdvancedFeature(AdvancedFeature.GROUP_CONCAT);
            return "GROUP_CONCAT(" + groupConcatInputForSQL(columns) + " ORDER BY " + quote(ROW_ID) + " SEPARATOR ',')";
        }
        String numeric = numericExpressionForSQL(columns);
        String integer = "CAST(" + numeric + " AS SIGNED)";
        return Randomly.fromOptions("AVG(" + numeric + ")", "BIT_AND(" + integer + ")", "BIT_OR(" + integer + ")",
                "BIT_XOR(" + integer + ")", "STD(" + numeric + ")", "VAR_SAMP(" + numeric + ")");
    }

    private String groupConcatInputForSQL(List<MySQLColumn> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        MySQLColumn column = Randomly.fromList(columns);
        if (column.getType() == MySQLDataType.VARCHAR || Randomly.getBoolean()) {
            return "CAST(" + quote(column.getName()) + " AS CHAR)";
        }
        return quote(column.getName());
    }

    private String numericExpressionForSQL(List<MySQLColumn> columns) {
        List<MySQLColumn> numericColumns = columns.stream().filter(c -> c.getType().isNumeric())
                .collect(Collectors.toList());
        if (!numericColumns.isEmpty() && Randomly.getBoolean()) {
            return quote(Randomly.fromList(numericColumns).getName());
        }
        return quote(ROW_ID);
    }

    private String simpleColumnOrRowIdForSQL(List<MySQLColumn> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        return quote(Randomly.fromList(columns).getName());
    }

    private String simpleOrderExpressionForSQL(List<MySQLColumn> columns) {
        if (columns.isEmpty()) {
            return quote(ROW_ID);
        }
        return simpleColumnOrRowIdForSQL(columns);
    }

    private static final class SourceAst {
        private final MySQLTable table;
        private final MySQLColumn rowIdColumn;
        private final List<MySQLColumn> columns;

        private SourceAst(MySQLTable table, MySQLColumn rowIdColumn, List<MySQLColumn> columns) {
            this.table = table;
            this.rowIdColumn = rowIdColumn;
            this.columns = columns;
        }
    }

    private SourceAst createSourceAst(VpSchema vp) {
        List<MySQLColumn> astColumns = new ArrayList<>();
        MySQLColumn rowId = new MySQLColumn(ROW_ID, MySQLDataType.INT, true, 64);
        astColumns.add(rowId);
        for (VpColumn column : vp.columns) {
            astColumns.add(new MySQLColumn(column.name, column.type, false, 0));
        }
        MySQLTable table = new MySQLTable(vp.sourceTable, astColumns, List.<MySQLIndex> of(), false, vp.sourceEngine);
        for (MySQLColumn column : astColumns) {
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
            extraSetup.add("CREATE TEMPORARY TABLE " + quote(vp.tempTable) + " AS SELECT "
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
            if (state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns && knownBug != null) {
                logKnownBug(pair, knownBug, singleRows, splitRows);
                recordKnownBugSuppressed(knownBug);
                throw new IgnoreMeException();
            }
            throw new AssertionError(formatBugReport(pair, singleRows, splitRows));
        }
        if (pair.orderedComparisonEnabled && !orderedRowsMatch(singleRows, splitRows)) {
            throw new AssertionError(formatOrderedBugReport(pair, singleRows, splitRows));
        }
    }

    private KnownVpBug classifyKnownBug(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (isTextDecimalAggregate(pair.singleQuery) || isTextDecimalAggregate(pair.splitQuery)) {
            return KnownVpBug.MYSQL_TEXT_DECIMAL_AGG_OVERFLOW_DIFF;
        }
        if (pair.schema.baseShape == BaseShape.EMPTY
                && (pair.reconstructionStrategy == ReconstructionStrategy.LEFT_RIGHT_JOIN
                        || pair.reconstructionStrategy == ReconstructionStrategy.FULL_OUTER_SIMULATION)
                && containsAggregate(pair.singleQuery)
                && aggregateArgumentsLookConstant(pair.singleQuery, pair.schema.sourceTable)) {
            return KnownVpBug.MYSQL_EMPTY_TABLE_LEFT_RIGHT_CONSTANT_AGG_DIFF;
        }
        if (isZeroRowsShortcutWrongResult(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_ZERO_ROWS_SHORTCUT_WRONG_RESULT;
        }
        if (isSingleCoveringIndexGroupOrDistinctLoss(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_SINGLE_COVERING_INDEX_GROUP_OR_DISTINCT_LOSS;
        }
        if (isRowNumberAlignmentNullMarkerCorruption(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_ROW_NUMBER_ALIGNMENT_NULL_MARKER_CORRUPTION;
        }
        if (isRowNumberAlignmentDecimalStringIntIndexFalseMatch(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_ROW_NUMBER_ALIGNMENT_DECIMAL_STRING_INT_INDEX_FALSE_MATCH;
        }
        if (isMysql120429DerivedDecimalDescIndexCaseIfNullFilterAggregate(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_120429_DERIVED_DECIMAL_DESC_INDEX_CASE_IFNULL_FILTER_AGG;
        }
        if (isMysql120429RightJoinDecimalIfNullFallbackNullification(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_120429_RIGHT_JOIN_DECIMAL_IFNULL_FALLBACK_NULLIFICATION;
        }
        if (isMysql120429AggregateThenMergeDecimalIfNullCaseFilterLoss(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_120429_AGGREGATE_THEN_MERGE_DECIMAL_IFNULL_CASE_FILTER_LOSS;
        }
        if (isMysql120429JsonPackDecimalIfNullGroupKeyNullCollapse(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_120429_JSON_PACK_DECIMAL_IFNULL_GROUP_KEY_NULL_COLLAPSE;
        }
        if (isMysql120429DistinctDecimalIfNullCoveringDedupNullCollapse(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_120429_DISTINCT_DECIMAL_IFNULL_COVERING_DEDUP_NULL_COLLAPSE;
        }
        if (isMysql120429FullOuterDecimalIfNullXorSumZero(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_120429_FULL_OUTER_DECIMAL_IFNULL_XOR_SUM_ZERO;
        }
        if (isScalarSubqueryDedupNullification(pair, singleRows, splitRows)) {
            return KnownVpBug.SCALAR_SUBQUERY_DEDUP_NULLIFICATION;
        }
        if (isNotExistsAntiIfNullProjectionNullification(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_NOT_EXISTS_ANTI_IFNULL_PROJECTION_NULLIFICATION;
        }
        if (isNotExistsAntiIfNullCaseAggregateValueCorruption(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_NOT_EXISTS_ANTI_IFNULL_CASE_AGG_VALUE_CORRUPTION;
        }
        if (isLateralDerivedIfNullProjectionNullification(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_LATERAL_DERIVED_IFNULL_PROJECTION_NULLIFICATION;
        }
        if (isStraightJoinIfNullGroupOrDistinctLoss(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_STRAIGHT_JOIN_IFNULL_GROUP_OR_DISTINCT_LOSS;
        }
        if (isViewTempMaterializeNullification(pair, singleRows, splitRows)) {
            return KnownVpBug.VIEW_TEMP_MATERIALIZE_NULLIFICATION;
        }
        if (isViewDistinctIfNullTempDedupNullification(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_VIEW_DISTINCT_IFNULL_TEMP_DEDUP_NULLIFICATION;
        }
        if (isStringPackGroupKeyCollapseToNull(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_STRING_PACK_GROUP_KEY_COLLAPSE_TO_NULL;
        }
        if (isStringPackDistinctCaseFloatSumRounding(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_STRING_PACK_DISTINCT_CASE_FLOAT_SUM_ROUNDING;
        }
        if (isJsonPackDistinctAggDropsNonNull(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_JSON_PACK_DISTINCT_AGG_DROPS_NON_NULL;
        }
        if (isAggregateThenMergeRollupIfNullKeyLeak(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_AGGREGATE_THEN_MERGE_ROLLUP_IFNULL_KEY_LEAK;
        }
        if (isAggregateThenMergeRollupConstantXorKeyNulling(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_AGGREGATE_THEN_MERGE_ROLLUP_CONSTANT_XOR_KEY_NULLING;
        }
        if (isAggregateThenMergeExprColumnMisbindToNull(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_AGGREGATE_THEN_MERGE_EXPR_COLUMN_MISBIND_TO_NULL;
        }
        if (isLeftRightJoinFilterMisbindsNullableColumn(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_LEFT_RIGHT_JOIN_FILTER_MISBINDS_NULLABLE_COLUMN;
        }
        if (isCollationMismatchTextComparisonFalsePositive(pair)) {
            return KnownVpBug.MYSQL_COLLATION_MISMATCH_TEXT_COMPARISON_FALSE_POSITIVE;
        }
        if (isRowNumberAlignmentPartialNullification(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_ROW_NUMBER_ALIGNMENT_PARTIAL_NULLIFICATION;
        }
        if (isTempTableIfNullGroupOrDistinctLoss(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_TEMP_TABLE_IFNULL_GROUP_OR_DISTINCT_LOSS;
        }
        if (isExistsInDerivedJoinIfNullNullification(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_EXISTS_IN_DERIVED_JOIN_IFNULL_NULLIFICATION;
        }
        if (isRollupZeroInputShortcutInconsistency(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_ROLLUP_ZERO_INPUT_SHORTCUT_INCONSISTENCY;
        }
        if (isFullOuterSimulationIfNullNullToZero(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_FULL_OUTER_SIMULATION_IFNULL_NULL_TO_ZERO;
        }
        if (isStringPackWindowProjectionNullification(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_STRING_PACK_WINDOW_PROJECTION_NULLIFICATION;
        }
        if (isLeftRightJoinIfNullSumZeroFlip(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_LEFT_RIGHT_JOIN_IFNULL_SUM_ZERO_FLIP;
        }
        if (isNotExistsAntiCountIfNullZeroFlip(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_NOT_EXISTS_ANTI_COUNT_IFNULL_ZERO_FLIP;
        }
        if (isLeftRightJoinCoveringIfNullBitOrRowLoss(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_LEFT_RIGHT_JOIN_COVERING_IFNULL_BIT_OR_ROW_LOSS;
        }
        if (isLateralDerivedDedupCastIfNullRowGain(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_LATERAL_DERIVED_DEDUP_CAST_IFNULL_ROW_GAIN;
        }
        if (isNaturalUsingDistinctGroupPartialDedupRowLoss(pair, singleRows, splitRows)) {
            return KnownVpBug.MYSQL_NATURAL_USING_DISTINCT_GROUP_PARTIAL_DEDUP_ROW_LOSS;
        }
        return null;
    }

    private boolean isTextDecimalAggregate(String query) {
        String normalized = query.toUpperCase(Locale.ROOT);
        return containsAggregate(query) && normalized.contains(" AS DECIMAL");
    }

    private boolean isZeroRowsShortcutWrongResult(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!singlePlan.contains("ZERO ROWS") || splitRows.rows.isEmpty()) {
            return false;
        }
        if (!(pair.reconstructionStrategy == ReconstructionStrategy.VIEW
                || pair.reconstructionStrategy == ReconstructionStrategy.SCALAR_SUBQUERY
                || pair.reconstructionStrategy == ReconstructionStrategy.LEFT_RIGHT_JOIN
                || pair.reconstructionStrategy == ReconstructionStrategy.FULL_OUTER_SIMULATION)) {
            return false;
        }
        return containsGroupBy(pair.singleQuery) && !splitPlan.contains("ZERO ROWS");
    }

    private boolean isSingleCoveringIndexGroupOrDistinctLoss(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!singleRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                || !splitRows.rows.keySet().stream().allMatch(this::isSingleValueRow)) {
            return false;
        }
        if (!(pair.reconstructionStrategy == ReconstructionStrategy.STRING_PACK
                || pair.reconstructionStrategy == ReconstructionStrategy.EXISTS_FILTER
                || pair.reconstructionStrategy == ReconstructionStrategy.NOT_EXISTS_ANTI)) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!singlePlan.contains("COVERING INDEX SCAN")) {
            return false;
        }
        if (!(containsGroupBy(pair.singleQuery) || containsDistinct(pair.singleQuery))) {
            return false;
        }
        if (splitPlan.contains("COVERING INDEX SCAN ON SRC USING") || splitRows.rows.size() <= singleRows.rows.size()) {
            return false;
        }
        return unmatchedRowsAfterFuzzyMatching(splitRows, singleRows).size() > 0
                && unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).isEmpty();
    }

    private boolean isRowNumberAlignmentNullMarkerCorruption(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.ROW_NUMBER_ALIGNMENT) {
            return false;
        }
        if (!pair.splitQuery.toUpperCase(Locale.ROOT).contains("ROW_NUMBER() OVER")) {
            return false;
        }
        if (!queryMentionsAny(pair.singleQuery, "LEAST(", "IFNULL(")) {
            return false;
        }
        int singleNonNull = countNonNullSingleValueRows(singleRows);
        int splitNonNull = countNonNullSingleValueRows(splitRows);
        return singleNonNull > 0 && splitNonNull == 0 && splitRows.rows.size() == singleRows.rows.size();
    }

    private boolean isRowNumberAlignmentDecimalStringIntIndexFalseMatch(VpQueryPair pair,
            QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.ROW_NUMBER_ALIGNMENT) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(normalized.contains("+(IF('',") && normalized.contains(" IN (")
                && hasColumnType(pair.schema, MySQLDataType.INT))) {
            return false;
        }
        String splitQuery = pair.splitQuery.toUpperCase(Locale.ROOT);
        if (!splitQuery.contains("ROW_NUMBER() OVER")) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(singlePlan.contains("ZERO ROWS") && splitPlan.contains("INDEX LOOKUP")
                && splitPlan.contains("<AUTO_KEY0>") && splitPlan.contains("RN =") && splitPlan.contains("= IF("))) {
            return false;
        }
        return singleRows.rows.isEmpty() && !splitRows.rows.isEmpty();
    }

    private boolean isScalarSubqueryDedupNullification(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.SCALAR_SUBQUERY) {
            return false;
        }
        if (!singleRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                || !splitRows.rows.keySet().stream().allMatch(this::isSingleValueRow)) {
            return false;
        }
        if (!containsDistinct(pair.singleQuery)) {
            return false;
        }
        if (!queryMentionsAny(pair.singleQuery, "IFNULL(", "BIT_COUNT(", "CAST(")) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!singlePlan.contains("COVERING INDEX SCAN")) {
            return false;
        }
        if (!splitPlan.contains("TEMPORARY TABLE WITH DEDUPLICATION")) {
            return false;
        }
        return countNonNullValues(singleRows) > countNonNullValues(splitRows) && countNonNullValues(splitRows) == 0;
    }

    private boolean isMysql120429DerivedDecimalDescIndexCaseIfNullFilterAggregate(VpQueryPair pair,
            QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.DERIVED_TABLE
                || !hasColumnType(pair.schema, MySQLDataType.DECIMAL)) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(normalized.contains("VAR_SAMP(") && normalized.contains("STD(") && normalized.contains("CASE IFNULL(NULL")
                && normalized.contains(" ELSE -"))) {
            return false;
        }
        if (!sourceTableDdlMentions(pair.schema, " DESC")) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(singlePlan.contains("AGGREGATE: VAR_SAMP") && singlePlan.contains("COVERING INDEX SCAN")
                && splitPlan.contains("NESTED LOOP INNER JOIN") && splitPlan.contains("SINGLE-ROW INDEX LOOKUP"))) {
            return false;
        }
        return rowCount(singleRows) == 1 && rowCount(splitRows) == 1 && allRowsHaveValueCount(singleRows, 3)
                && allRowsHaveValueCount(splitRows, 3) && countNonNullValues(splitRows) > countNonNullValues(singleRows)
                && !unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).isEmpty();
    }

    private boolean isMysql120429RightJoinDecimalIfNullFallbackNullification(VpQueryPair pair,
            QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.LEFT_RIGHT_JOIN
                || !hasColumnType(pair.schema, MySQLDataType.DECIMAL)) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(normalized.contains("IFNULL(") && normalized.contains(" ORDER BY ")
                && !containsAggregate(pair.singleQuery) && !containsDistinct(pair.singleQuery)
                && !containsGroupBy(pair.singleQuery))) {
            return false;
        }
        if (!pair.splitQuery.toUpperCase(Locale.ROOT).contains(" RIGHT JOIN ")) {
            return false;
        }
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(splitPlan.contains("LEFT HASH JOIN") && splitPlan.contains("COVERING INDEX SCAN")
                && splitPlan.contains("SORT:"))) {
            return false;
        }
        return singleRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                && splitRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                && rowCount(singleRows) == rowCount(splitRows) && countNonNullSingleValueRows(singleRows) > 0
                && countNonNullSingleValueRows(splitRows) == 0
                && !unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).isEmpty();
    }

    private boolean isMysql120429AggregateThenMergeDecimalIfNullCaseFilterLoss(VpQueryPair pair,
            QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.AGGREGATE_THEN_MERGE
                || !hasColumnType(pair.schema, MySQLDataType.DECIMAL)) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(normalized.contains("AVG(") && normalized.contains("IFNULL((NULL) XOR") && normalized.contains("CASE")
                && normalized.contains("WHEN NULL"))) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(singlePlan.contains("AGGREGATE: AVG") && singlePlan.contains("COVERING INDEX SCAN")
                && splitPlan.contains("NESTED LOOP INNER JOIN") && splitPlan.contains("MATERIALIZE")
                && splitPlan.contains("GROUP AGGREGATE: COUNT(0)"))) {
            return false;
        }
        return rowCount(singleRows) == 1 && rowCount(splitRows) == 1
                && singleRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                && splitRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                && countNonNullSingleValueRows(singleRows) == 0 && countNonNullSingleValueRows(splitRows) == 1
                && !unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).isEmpty();
    }

    private boolean isMysql120429JsonPackDecimalIfNullGroupKeyNullCollapse(VpQueryPair pair,
            QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.JSON_PACK
                || !hasColumnType(pair.schema, MySQLDataType.DECIMAL)) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(containsGroupBy(pair.singleQuery) && normalized.contains("IFNULL(")
                && !containsAggregate(pair.singleQuery) && pair.splitQuery.contains("JSON_OBJECT('vp'"))) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(singlePlan.contains("TEMPORARY TABLE WITH DEDUPLICATION")
                && splitPlan.contains("TEMPORARY TABLE WITH DEDUPLICATION")
                && splitPlan.contains("NESTED LOOP INNER JOIN"))) {
            return false;
        }
        Map<String, Integer> onlySingle = unmatchedRowsAfterFuzzyMatching(singleRows, splitRows);
        Map<String, Integer> onlySplit = unmatchedRowsAfterFuzzyMatching(splitRows, singleRows);
        boolean nonNullGroupLost = !onlySingle.isEmpty()
                && countNonNullValues(singleRows) > countNonNullValues(splitRows)
                && rowCount(singleRows) > rowCount(splitRows);
        boolean extraNullifiedGroup = !onlySplit.isEmpty() && anyRowHasNullFirstAndLaterNonNull(onlySplit)
                && rowCount(splitRows) > rowCount(singleRows);
        return nonNullGroupLost || extraNullifiedGroup;
    }

    private boolean isMysql120429DistinctDecimalIfNullCoveringDedupNullCollapse(VpQueryPair pair,
            QueryResultMultiset singleRows, QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.STRAIGHT_JOIN
                || !hasColumnType(pair.schema, MySQLDataType.DECIMAL)) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(containsDistinct(pair.singleQuery) && !containsGroupBy(pair.singleQuery)
                && normalized.contains("CAST(IFNULL(NULL") && normalized.contains(" AS SIGNED"))) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(singlePlan.contains("TEMPORARY TABLE WITH DEDUPLICATION") && singlePlan.contains("COVERING INDEX SCAN")
                && splitPlan.contains("TEMPORARY TABLE WITH DEDUPLICATION") && splitPlan.contains("INNER HASH JOIN"))) {
            return false;
        }
        return singleRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                && splitRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                && hasOnlyNullSingleValue(singleRows) && containsNonNullAndNullSingleValues(splitRows)
                && !unmatchedRowsAfterFuzzyMatching(splitRows, singleRows).isEmpty();
    }

    private boolean isMysql120429FullOuterDecimalIfNullXorSumZero(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.FULL_OUTER_SIMULATION
                || !hasColumnType(pair.schema, MySQLDataType.DECIMAL)) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(normalized.contains("SUM(((") && normalized.contains("IFNULL(NULL") && normalized.contains(" XOR ")
                && normalized.contains(" IS FALSE"))) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(singlePlan.contains("AGGREGATE: SUM") && singlePlan.contains("COVERING INDEX SCAN")
                && splitPlan.contains("UNION ALL MATERIALIZE") && splitPlan.contains("NESTED LOOP LEFT JOIN")
                && splitPlan.contains("NESTED LOOP ANTIJOIN"))) {
            return false;
        }
        return rowCount(singleRows) == 1 && rowCount(splitRows) == 1
                && allRowsAreSingleValueLiteral(singleRows.rows, "1")
                && allRowsAreSingleValueLiteral(splitRows.rows, "0");
    }

    private boolean isViewTempMaterializeNullification(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.VIEW) {
            return false;
        }
        if (!singleRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                || !splitRows.rows.keySet().stream().allMatch(this::isSingleValueRow)) {
            return false;
        }
        if (!(queryMentionsAny(pair.singleQuery, "IFNULL(") && queryMentionsAny(pair.singleQuery, "CAST("))) {
            return false;
        }
        if (countNonNullValues(singleRows) == 0 || countNonNullValues(splitRows) != 0) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!singlePlan.contains("COVERING INDEX SCAN")) {
            return false;
        }
        if (containsDistinct(pair.singleQuery) || splitPlan.contains("TEMPORARY TABLE WITH DEDUPLICATION")) {
            return false;
        }
        return splitPlan.contains("NESTED LOOP INNER JOIN")
                && (splitRows.rows.size() == singleRows.rows.size() || splitRows.rows.size() == 1);
    }

    private boolean isViewDistinctIfNullTempDedupNullification(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.VIEW) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(containsDistinct(pair.singleQuery) && !containsGroupBy(pair.singleQuery)
                && normalized.contains("IFNULL("))) {
            return false;
        }
        if (!(normalized.contains("COALESCE(NULL")
                || (normalized.contains("EXISTS (SELECT 1 WHERE FALSE") && normalized.contains(" XOR ")))) {
            return false;
        }
        if (!pair.splitQuery.contains(quote(pair.schema.viewName))) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(singlePlan.contains("TEMPORARY TABLE WITH DEDUPLICATION")
                && singlePlan.contains("COVERING INDEX SCAN"))) {
            return false;
        }
        if (!(splitPlan.contains("TEMPORARY TABLE WITH DEDUPLICATION") && splitPlan.contains("NESTED LOOP INNER JOIN")
                && splitPlan.contains("SINGLE-ROW INDEX LOOKUP ON R USING PRIMARY"))) {
            return false;
        }
        return countNonNullValues(singleRows) > countNonNullValues(splitRows)
                && unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).size() > 0;
    }

    private boolean isNotExistsAntiIfNullProjectionNullification(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.NOT_EXISTS_ANTI) {
            return false;
        }
        if (!containsDistinct(pair.singleQuery) || containsGroupBy(pair.singleQuery)) {
            return false;
        }
        if (!queryMentionsAny(pair.singleQuery, "IFNULL(", "CAST(")) {
            return false;
        }
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(splitPlan.contains("TEMPORARY TABLE WITH DEDUPLICATION") && splitPlan.contains("NESTED LOOP ANTIJOIN"))) {
            return false;
        }
        return countNonNullValues(singleRows) > countNonNullValues(splitRows) && countNonNullValues(splitRows) > 0
                && splitRows.rows.size() == singleRows.rows.size();
    }

    private boolean isNotExistsAntiIfNullCaseAggregateValueCorruption(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.NOT_EXISTS_ANTI) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(containsAggregate(pair.singleQuery) && !containsGroupBy(pair.singleQuery) && normalized.contains("CASE")
                && normalized.contains("IFNULL(NULL"))) {
            return false;
        }
        if (!(normalized.contains("BIT_XOR(CAST(") || normalized.contains("MIN(DISTINCT (CASE"))) {
            return false;
        }
        if (!singleRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                || !splitRows.rows.keySet().stream().allMatch(this::isSingleValueRow) || rowCount(singleRows) != 1
                || rowCount(splitRows) != 1) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(singlePlan.contains("AGGREGATE:") && singlePlan.contains("COVERING INDEX SCAN"))) {
            return false;
        }
        if (!(splitPlan.contains("AGGREGATE:") && splitPlan.contains("NESTED LOOP ANTIJOIN")
                && !splitPlan.contains("TEMPORARY TABLE WITH DEDUPLICATION"))) {
            return false;
        }
        return countNonNullSingleValueRows(singleRows) == 1
                && (hasOnlyNullSingleValue(splitRows) || allRowsAreSingleValueLiteral(splitRows.rows, "0"))
                && unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).size() > 0;
    }

    private boolean isLateralDerivedIfNullProjectionNullification(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.LATERAL_DERIVED) {
            return false;
        }
        if (!singleRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                || !splitRows.rows.keySet().stream().allMatch(this::isSingleValueRow)) {
            return false;
        }
        if ((containsDistinct(pair.singleQuery) || containsGroupBy(pair.singleQuery))
                || !queryMentionsAny(pair.singleQuery, "IFNULL(")) {
            return false;
        }
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(splitPlan.contains("NESTED LOOP INNER JOIN")
                && splitPlan.contains("COVERING INDEX SCAN ON RR USING PRIMARY"))) {
            return false;
        }
        int singleNonNull = countNonNullSingleValueRows(singleRows);
        int splitNonNull = countNonNullSingleValueRows(splitRows);
        return singleNonNull > 0 && splitNonNull == 0 && splitRows.rows.size() == singleRows.rows.size();
    }

    private boolean isStraightJoinIfNullGroupOrDistinctLoss(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.STRAIGHT_JOIN) {
            return false;
        }
        if (!queryMentionsAny(pair.singleQuery, "IFNULL(")) {
            return false;
        }
        if (!(containsGroupBy(pair.singleQuery) || containsDistinct(pair.singleQuery))) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!singlePlan.contains("COVERING INDEX SCAN")) {
            return false;
        }
        if (!splitPlan.contains("NESTED LOOP INNER JOIN")) {
            return false;
        }
        if (splitRows.rows.size() >= singleRows.rows.size()) {
            return false;
        }
        return unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).size() > 0
                && unmatchedRowsAfterFuzzyMatching(splitRows, singleRows).isEmpty();
    }

    private boolean isStringPackGroupKeyCollapseToNull(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.STRING_PACK) {
            return false;
        }
        if (!(containsGroupBy(pair.singleQuery) || containsDistinct(pair.singleQuery))) {
            return false;
        }
        if (!pair.splitQuery.contains("CONCAT('vp:'")) {
            return false;
        }
        int singleNonNull = countNonNullValues(singleRows);
        int splitNonNull = countNonNullValues(splitRows);
        return singleRows.rows.size() >= 2 && splitRows.rows.size() == 1 && singleNonNull > 0 && splitNonNull == 0;
    }

    private boolean isStringPackDistinctCaseFloatSumRounding(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.STRING_PACK) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(normalized.contains("SUM(DISTINCT CASE") && normalized.contains(" ELSE ")
                && hasColumnType(pair.schema, MySQLDataType.FLOAT) && pair.splitQuery.contains("CONCAT('vp:'"))) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(singlePlan.contains("SUM(DISTINCT (CASE") && splitPlan.contains("SUM(DISTINCT TMP_FIELD)")
                && splitPlan.contains("GROUP AGGREGATE") && splitPlan.contains("INNER HASH JOIN"))) {
            return false;
        }
        return unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).size() > 0
                && unmatchedRowsAfterFuzzyMatching(splitRows, singleRows).size() > 0;
    }

    private boolean isJsonPackDistinctAggDropsNonNull(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.JSON_PACK) {
            return false;
        }
        String normalizedSingle = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(normalizedSingle.contains("MIN(DISTINCT IFNULL(NULL")
                || normalizedSingle.contains("MIN(DISTINCT IFNULL("))) {
            return false;
        }
        if (!pair.splitQuery.contains("JSON_OBJECT('vp'")) {
            return false;
        }
        return hasExactlyOneNonNullSingleValue(singleRows) && hasOnlyNullSingleValue(splitRows);
    }

    private boolean isAggregateThenMergeExprColumnMisbindToNull(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.AGGREGATE_THEN_MERGE) {
            return false;
        }
        if (!queryMentionsAny(pair.singleQuery, "IFNULL(", "XOR", "GROUP BY")) {
            return false;
        }
        return countNonNullValues(singleRows) > countNonNullValues(splitRows)
                && containsNonNullAndNullSingleValues(singleRows) && hasOnlyNullInSecondColumn(splitRows);
    }

    private boolean isAggregateThenMergeRollupIfNullKeyLeak(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.AGGREGATE_THEN_MERGE) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(normalized.contains(" WITH ROLLUP") && normalized.contains("BIT_AND(CAST(")
                && normalized.contains("IFNULL(") && normalized.contains(" NOT IN "))) {
            return false;
        }
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(splitPlan.contains("GROUP AGGREGATE WITH ROLLUP: BIT_AND(TMP_FIELD)") && splitPlan.contains("MATERIALIZE")
                && splitPlan.contains("GROUP AGGREGATE: COUNT(0)"))) {
            return false;
        }
        return unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).size() > 0
                && unmatchedRowsAfterFuzzyMatching(splitRows, singleRows).size() > 0;
    }

    private boolean isAggregateThenMergeRollupConstantXorKeyNulling(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.AGGREGATE_THEN_MERGE) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(normalized.contains(" WITH ROLLUP") && normalized.contains("COUNT(EXISTS(")
                && normalized.contains("WHERE FALSE") && normalized.contains("COALESCE((NULL) XOR")
                && normalized.contains("CAST("))) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(singlePlan.contains("GROUP AGGREGATE WITH ROLLUP: COUNT(EXISTS") && singlePlan.contains("ZERO ROWS")
                && splitPlan.contains("GROUP AGGREGATE WITH ROLLUP: COUNT(EXISTS") && splitPlan.contains("MATERIALIZE")
                && splitPlan.contains("GROUP AGGREGATE: COUNT(0)"))) {
            return false;
        }
        return rowCount(singleRows) == rowCount(splitRows)
                && countNonNullValues(splitRows) > countNonNullValues(singleRows)
                && unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).size() > 0;
    }

    private boolean isLeftRightJoinFilterMisbindsNullableColumn(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.LEFT_RIGHT_JOIN) {
            return false;
        }
        String normalizedSingle = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(normalizedSingle.contains("WHERE") && normalizedSingle.contains("IFNULL(")
                && containsAggregate(pair.singleQuery))) {
            return false;
        }
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!splitPlan.contains("NESTED LOOP LEFT JOIN")) {
            return false;
        }
        return countNonNullValues(singleRows) > 0 && splitRows.rows.keySet().stream().allMatch(this::isAllNullRow);
    }

    private boolean isCollationMismatchTextComparisonFalsePositive(VpQueryPair pair) {
        if (!isPlainTextJoinScanShape(pair)) {
            return false;
        }
        for (VpColumn column : pair.schema.columns) {
            if (column.type != MySQLDataType.VARCHAR) {
                continue;
            }
            String partitionTable = singleOwningPartitionTable(pair.schema, column.name);
            if (partitionTable == null) {
                continue;
            }
            if (!queryUsesColumnInCollationSensitiveContext(pair.singleQuery, column.name)) {
                continue;
            }
            String sourceCollation = getColumnCollation(pair.schema.sourceTable, column.name);
            String partitionCollation = getColumnCollation(partitionTable, column.name);
            if (sourceCollation != null && partitionCollation != null && !sourceCollation.equals(partitionCollation)) {
                return true;
            }
        }
        return false;
    }

    private boolean isRowNumberAlignmentPartialNullification(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.ROW_NUMBER_ALIGNMENT) {
            return false;
        }
        if (!pair.splitQuery.toUpperCase(Locale.ROOT).contains("ROW_NUMBER() OVER")) {
            return false;
        }
        if (!queryMentionsAny(pair.singleQuery, "IFNULL(", "GREATEST(", "LEAST(")) {
            return false;
        }
        if (singleRows.rows.size() != splitRows.rows.size()) {
            return false;
        }
        int singleNonNull = countNonNullValues(singleRows);
        int splitNonNull = countNonNullValues(splitRows);
        return singleNonNull > splitNonNull && splitNonNull >= 0
                && unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).size() > 0;
    }

    private boolean isTempTableIfNullGroupOrDistinctLoss(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.TEMP_TABLE) {
            return false;
        }
        if (!queryMentionsAny(pair.singleQuery, "IFNULL(", "COALESCE(")) {
            return false;
        }
        if (!(containsGroupBy(pair.singleQuery) || containsDistinct(pair.singleQuery))) {
            return false;
        }
        if (pair.singleQuery.toUpperCase(Locale.ROOT).contains(" WITH ROLLUP")) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!singlePlan.contains("COVERING INDEX SCAN")) {
            return false;
        }
        if (!(splitPlan.contains("TABLE SCAN ON VP_DATABASE") || splitPlan.contains("TABLE SCAN ON VP_")
                || splitPlan.contains("TABLE SCAN ON `VP_") || splitPlan.contains("TABLE SCAN ON VP_DATABASE"))) {
            return false;
        }
        return countNonNullValues(singleRows) > countNonNullValues(splitRows)
                && (splitRows.rows.isEmpty() || unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).size() > 0);
    }

    private boolean isExistsInDerivedJoinIfNullNullification(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!(pair.reconstructionStrategy == ReconstructionStrategy.EXISTS_FILTER
                || pair.reconstructionStrategy == ReconstructionStrategy.IN_FILTER
                || pair.reconstructionStrategy == ReconstructionStrategy.DERIVED_TABLE
                || pair.reconstructionStrategy == ReconstructionStrategy.CTE
                || pair.reconstructionStrategy == ReconstructionStrategy.JOIN
                || pair.reconstructionStrategy == ReconstructionStrategy.NATURAL_OR_USING)) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!normalized.contains("IFNULL(")) {
            return false;
        }
        if (normalized.contains(" WITH ROLLUP")) {
            return false;
        }
        if (pair.reconstructionStrategy == ReconstructionStrategy.NATURAL_OR_USING
                && (containsGroupBy(pair.singleQuery) || containsDistinct(pair.singleQuery))) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!singlePlan.contains("COVERING INDEX SCAN")) {
            return false;
        }
        if (!(splitPlan.contains("NESTED LOOP INNER JOIN") || splitPlan.contains("INNER HASH JOIN")
                || splitPlan.contains("MATERIALIZE"))) {
            return false;
        }
        if (countNonNullValues(singleRows) <= countNonNullValues(splitRows)) {
            return false;
        }
        if (splitRows.rows.size() > singleRows.rows.size()) {
            return false;
        }
        return unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).size() > 0;
    }

    private boolean isRollupZeroInputShortcutInconsistency(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (!pair.singleQuery.toUpperCase(Locale.ROOT).contains(" WITH ROLLUP")) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        boolean singleZero = singlePlan.contains("ZERO INPUT ROWS") || singlePlan.contains("ZERO ROWS");
        boolean splitZero = splitPlan.contains("ZERO INPUT ROWS") || splitPlan.contains("ZERO ROWS");
        if (singleZero == splitZero) {
            return false;
        }
        if (singleRows.rows.size() != 1 || splitRows.rows.size() != 1) {
            return false;
        }
        return !singlePlan.equals(splitPlan);
    }

    private boolean isFullOuterSimulationIfNullNullToZero(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.FULL_OUTER_SIMULATION) {
            return false;
        }
        if (!queryMentionsAny(pair.singleQuery, "IFNULL(") || containsAggregate(pair.singleQuery)
                || containsGroupBy(pair.singleQuery) || containsDistinct(pair.singleQuery)) {
            return false;
        }
        if (!singleRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                || !splitRows.rows.keySet().stream().allMatch(this::isSingleValueRow)) {
            return false;
        }
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!splitPlan.contains("UNION ALL MATERIALIZE")) {
            return false;
        }
        Map<String, Integer> onlySingle = unmatchedRowsAfterFuzzyMatching(singleRows, splitRows);
        Map<String, Integer> onlySplit = unmatchedRowsAfterFuzzyMatching(splitRows, singleRows);
        return singleRows.rows.size() == splitRows.rows.size()
                && countNonNullSingleValueRows(splitRows) > countNonNullSingleValueRows(singleRows)
                && allRowsAreSingleValueNull(onlySingle) && allRowsAreSingleValueLiteral(onlySplit, "0");
    }

    private boolean isStringPackWindowProjectionNullification(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.STRING_PACK) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!pair.splitQuery.contains("CONCAT('vp:'") || !normalized.contains("OVER (ORDER BY `VP_ROWID`)")
                || !normalized.contains("IFNULL(") || containsGroupBy(pair.singleQuery)
                || containsDistinct(pair.singleQuery)) {
            return false;
        }
        if (singleRows.orderedRows.size() != splitRows.orderedRows.size() || singleRows.orderedRows.isEmpty()) {
            return false;
        }
        boolean sawNullification = false;
        for (int i = 0; i < singleRows.orderedRows.size(); i++) {
            List<String> singleValues = decodeCanonicalRow(singleRows.orderedRows.get(i));
            List<String> splitValues = decodeCanonicalRow(splitRows.orderedRows.get(i));
            if (singleValues.size() != 3 || splitValues.size() != 3) {
                return false;
            }
            if (!Objects.equals(singleValues.get(0), splitValues.get(0))
                    || !Objects.equals(singleValues.get(2), splitValues.get(2))) {
                return false;
            }
            String singleProjection = singleValues.get(1);
            String splitProjection = splitValues.get(1);
            if (singleProjection != null && splitProjection == null) {
                sawNullification = true;
                continue;
            }
            if (!Objects.equals(singleProjection, splitProjection)) {
                return false;
            }
        }
        return sawNullification;
    }

    private boolean isLeftRightJoinIfNullSumZeroFlip(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.LEFT_RIGHT_JOIN) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(normalized.contains("SUM(IFNULL(") && !containsDistinct(pair.singleQuery)
                && !containsGroupBy(pair.singleQuery))) {
            return false;
        }
        if (!singleRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                || !splitRows.rows.keySet().stream().allMatch(this::isSingleValueRow) || singleRows.rows.size() != 1
                || splitRows.rows.size() != 1) {
            return false;
        }
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!splitPlan.contains("NESTED LOOP LEFT JOIN")) {
            return false;
        }
        return allRowsAreSingleValueLiteral(singleRows.rows, "1") && allRowsAreSingleValueLiteral(splitRows.rows, "0");
    }

    private boolean isNotExistsAntiCountIfNullZeroFlip(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.NOT_EXISTS_ANTI) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(normalized.contains("COUNT(IFNULL(NULL") && !containsGroupBy(pair.singleQuery))) {
            return false;
        }
        if (!singleRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                || !splitRows.rows.keySet().stream().allMatch(this::isSingleValueRow) || singleRows.rows.size() != 1
                || splitRows.rows.size() != 1) {
            return false;
        }
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!splitPlan.contains("NESTED LOOP ANTIJOIN")) {
            return false;
        }
        return allRowsAreSingleValueLiteral(singleRows.rows, "1") && allRowsAreSingleValueLiteral(splitRows.rows, "0");
    }

    private boolean isLeftRightJoinCoveringIfNullBitOrRowLoss(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.LEFT_RIGHT_JOIN) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(normalized.contains("BIT_OR(CAST(") && normalized.contains("WHERE") && normalized.contains("IFNULL(")
                && !containsGroupBy(pair.singleQuery))) {
            return false;
        }
        if (!singleRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                || !splitRows.rows.keySet().stream().allMatch(this::isSingleValueRow) || singleRows.rows.size() != 1
                || splitRows.rows.size() != 1) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!singlePlan.contains("COVERING INDEX SCAN")) {
            return false;
        }
        if (!(splitPlan.contains("NESTED LOOP LEFT JOIN")
                && splitPlan.contains("SINGLE-ROW COVERING INDEX LOOKUP ON R USING PRIMARY"))) {
            return false;
        }
        Map<String, Integer> onlySingle = unmatchedRowsAfterFuzzyMatching(singleRows, splitRows);
        Map<String, Integer> onlySplit = unmatchedRowsAfterFuzzyMatching(splitRows, singleRows);
        return countNonNullSingleValueRows(singleRows) == 1 && countNonNullSingleValueRows(splitRows) == 1
                && !onlySingle.isEmpty() && !onlySplit.isEmpty();
    }

    private boolean isLateralDerivedDedupCastIfNullRowGain(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.LATERAL_DERIVED) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(containsDistinct(pair.singleQuery) && containsGroupBy(pair.singleQuery) && normalized.contains("CAST(")
                && normalized.contains("IFNULL(NULL"))) {
            return false;
        }
        String singlePlan = explainTreeOrEmpty(pair.singleQuery).toUpperCase(Locale.ROOT);
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(singlePlan.contains("TEMPORARY TABLE WITH DEDUPLICATION")
                && singlePlan.contains("COVERING INDEX SCAN"))) {
            return false;
        }
        if (!(splitPlan.contains("TEMPORARY TABLE WITH DEDUPLICATION") && splitPlan.contains("NESTED LOOP INNER JOIN")
                && splitPlan.contains("SINGLE-ROW COVERING INDEX LOOKUP ON RR USING PRIMARY"))) {
            return false;
        }
        if (!singleRows.rows.keySet().stream().allMatch(this::isSingleValueRow)
                || !splitRows.rows.keySet().stream().allMatch(this::isSingleValueRow)) {
            return false;
        }
        return singleRows.rows.isEmpty() && countNonNullSingleValueRows(splitRows) > 0;
    }

    private boolean isNaturalUsingDistinctGroupPartialDedupRowLoss(VpQueryPair pair, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) {
        if (pair.reconstructionStrategy != ReconstructionStrategy.NATURAL_OR_USING) {
            return false;
        }
        String normalized = pair.singleQuery.toUpperCase(Locale.ROOT);
        if (!(normalized.contains("DISTINCTROW") && containsGroupBy(pair.singleQuery)
                && normalized.contains("COUNT(DISTINCT") && normalized.contains(" XOR ")
                && normalized.contains(" IS NOT TRUE"))) {
            return false;
        }
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        if (!(splitPlan.contains("SORT WITH DUPLICATE REMOVAL") && splitPlan.contains("GROUP AGGREGATE")
                && splitPlan.contains("SINGLE-ROW COVERING INDEX LOOKUP"))) {
            return false;
        }
        return rowCount(singleRows) > rowCount(splitRows)
                && unmatchedRowsAfterFuzzyMatching(splitRows, singleRows).isEmpty()
                && unmatchedRowsAfterFuzzyMatching(singleRows, splitRows).size() > 0;
    }

    private boolean isPlainTextJoinScanShape(VpQueryPair pair) {
        if (!(pair.reconstructionStrategy == ReconstructionStrategy.JOIN
                || pair.reconstructionStrategy == ReconstructionStrategy.STRAIGHT_JOIN
                || pair.reconstructionStrategy == ReconstructionStrategy.NATURAL_OR_USING)) {
            return false;
        }
        String splitPlan = explainTreeOrEmpty(pair.splitQuery).toUpperCase(Locale.ROOT);
        return splitPlan.contains("NESTED LOOP") && !queryMentionsAny(splitPlan, "TEMPORARY TABLE", "DEDUPLICATION",
                "MATERIALIZE", "WINDOW", "AGGREGATE", "UNION", "INTERSECT", "EXCEPT");
    }

    private String singleOwningPartitionTable(VpSchema schema, String columnName) {
        boolean inLeft = schema.leftColumns.stream().anyMatch(c -> c.name.equals(columnName));
        boolean inRight = schema.rightColumns.stream().anyMatch(c -> c.name.equals(columnName));
        if (inLeft == inRight) {
            return null;
        }
        return inLeft ? schema.leftTable : schema.rightTable;
    }

    private boolean queryUsesColumnInCollationSensitiveContext(String query, String columnName) {
        String normalized = query.toUpperCase(Locale.ROOT);
        if (columnAppearsInOrderBy(normalized, columnName) || columnAppearsInMinOrMax(normalized, columnName)) {
            return true;
        }
        return columnAppearsInTextComparison(normalized, columnName);
    }

    private boolean columnAppearsInOrderBy(String normalizedQuery, String columnName) {
        int orderByIndex = normalizedQuery.indexOf(" ORDER BY ");
        if (orderByIndex < 0) {
            return false;
        }
        return containsColumnReference(normalizedQuery.substring(orderByIndex), columnName);
    }

    private boolean columnAppearsInMinOrMax(String normalizedQuery, String columnName) {
        return aggregateArgumentMentionsColumn(normalizedQuery, "MIN(", columnName)
                || aggregateArgumentMentionsColumn(normalizedQuery, "MAX(", columnName);
    }

    private boolean aggregateArgumentMentionsColumn(String normalizedQuery, String aggregate, String columnName) {
        int searchFrom = 0;
        while (true) {
            int start = normalizedQuery.indexOf(aggregate, searchFrom);
            if (start < 0) {
                return false;
            }
            int argStart = start + aggregate.length();
            int argEnd = findMatchingParen(normalizedQuery, argStart - 1);
            if (argEnd > argStart && containsColumnReference(normalizedQuery.substring(argStart, argEnd), columnName)) {
                return true;
            }
            searchFrom = argStart;
        }
    }

    private boolean columnAppearsInTextComparison(String normalizedQuery, String columnName) {
        String columnRef = columnReferenceRegex(columnName);
        Pattern leftSide = Pattern.compile("\\(*\\s*" + columnRef + "\\s*\\)*\\s*(?:<=|>=|<|>)");
        Pattern rightSide = Pattern.compile("(?:<=|>=|<|>)\\s*\\(*\\s*" + columnRef + "\\s*\\)*");
        return leftSide.matcher(normalizedQuery).find() || rightSide.matcher(normalizedQuery).find();
    }

    private boolean containsColumnReference(String text, String columnName) {
        return Pattern.compile(columnReferenceRegex(columnName)).matcher(text).find();
    }

    private String columnReferenceRegex(String columnName) {
        String upper = columnName.toUpperCase(Locale.ROOT);
        String bare = "\\b" + Pattern.quote(upper) + "\\b";
        String quoted = "`" + Pattern.quote(upper) + "`";
        String qualifiedBare = "(?:\\b[A-Z0-9_]+\\.)?" + bare;
        String qualifiedQuoted = "(?:`?[A-Z0-9_]+`?\\.)?" + quoted;
        return "(?:" + qualifiedQuoted + "|" + qualifiedBare + ")";
    }

    private String getColumnCollation(String tableName, String columnName) {
        String sql = "SELECT COLLATION_NAME FROM information_schema.columns WHERE table_schema = '"
                + state.getDatabaseName() + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName
                + "'";
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (!rs.next()) {
                return null;
            }
            return rs.getString(1);
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    private boolean containsAggregate(String query) {
        String normalized = query.toUpperCase(Locale.ROOT);
        return normalized.contains("SUM(") || normalized.contains("AVG(") || normalized.contains("MIN(")
                || normalized.contains("MAX(") || normalized.contains("COUNT(") || normalized.contains("BIT_AND(")
                || normalized.contains("BIT_OR(") || normalized.contains("BIT_XOR(") || normalized.contains("STD(")
                || normalized.contains("VAR_SAMP(");
    }

    private boolean containsDistinct(String query) {
        return query.toUpperCase(Locale.ROOT).contains("DISTINCT");
    }

    private boolean containsGroupBy(String query) {
        return query.toUpperCase(Locale.ROOT).contains(" GROUP BY ");
    }

    private boolean queryMentionsAny(String query, String... patterns) {
        String normalized = query.toUpperCase(Locale.ROOT);
        for (String pattern : patterns) {
            if (normalized.contains(pattern.toUpperCase(Locale.ROOT))) {
                return true;
            }
        }
        return false;
    }

    private boolean hasColumnType(VpSchema schema, MySQLDataType type) {
        return schema.columns.stream().anyMatch(c -> c.type == type);
    }

    private boolean sourceTableDdlMentions(VpSchema schema, String text) {
        try {
            return getCreateTable(schema.sourceTable).toUpperCase(Locale.ROOT).contains(text.toUpperCase(Locale.ROOT));
        } catch (SQLException ignored) {
            return false;
        }
    }

    private int rowCount(QueryResultMultiset rows) {
        return rows.rows.values().stream().mapToInt(Integer::intValue).sum();
    }

    private String explainTreeOrEmpty(String query) {
        return formatExplainTree(query);
    }

    private int countNonNullValues(QueryResultMultiset rows) {
        int count = 0;
        for (String row : rows.rows.keySet()) {
            for (String value : decodeCanonicalRow(row)) {
                if (value != null) {
                    count++;
                }
            }
        }
        return count;
    }

    private int countNonNullSingleValueRows(QueryResultMultiset rows) {
        int count = 0;
        for (String row : rows.rows.keySet()) {
            List<String> values = decodeCanonicalRow(row);
            if (values.size() == 1 && values.get(0) != null) {
                count += rows.rows.getOrDefault(row, 0);
            }
        }
        return count;
    }

    private boolean hasExactlyOneNonNullSingleValue(QueryResultMultiset rows) {
        int distinctNonNull = 0;
        for (String row : rows.rows.keySet()) {
            List<String> values = decodeCanonicalRow(row);
            if (values.size() == 1 && values.get(0) != null) {
                distinctNonNull++;
            }
        }
        return distinctNonNull == 1;
    }

    private boolean hasOnlyNullSingleValue(QueryResultMultiset rows) {
        if (rows.rows.size() != 1) {
            return false;
        }
        String row = rows.rows.keySet().iterator().next();
        List<String> values = decodeCanonicalRow(row);
        return values.size() == 1 && values.get(0) == null;
    }

    private boolean containsNonNullAndNullSingleValues(QueryResultMultiset rows) {
        boolean hasNull = false;
        boolean hasNonNull = false;
        for (String row : rows.rows.keySet()) {
            List<String> values = decodeCanonicalRow(row);
            if (values.isEmpty()) {
                continue;
            }
            if (values.get(0) == null) {
                hasNull = true;
            } else {
                hasNonNull = true;
            }
        }
        return hasNull && hasNonNull;
    }

    private boolean hasOnlyNullInSecondColumn(QueryResultMultiset rows) {
        for (String row : rows.rows.keySet()) {
            List<String> values = decodeCanonicalRow(row);
            if (values.size() < 2) {
                return false;
            }
            if (values.get(1) != null) {
                return false;
            }
        }
        return !rows.rows.isEmpty();
    }

    private boolean isAllNullRow(String row) {
        List<String> values = decodeCanonicalRow(row);
        return !values.isEmpty() && values.stream().allMatch(Objects::isNull);
    }

    private boolean isSingleValueRow(String row) {
        return decodeCanonicalRow(row).size() == 1;
    }

    private boolean allRowsAreSingleValueNull(Map<String, Integer> rows) {
        return !rows.isEmpty() && rows.keySet().stream().allMatch(row -> {
            List<String> values = decodeCanonicalRow(row);
            return values.size() == 1 && values.get(0) == null;
        });
    }

    private boolean allRowsAreSingleValueLiteral(Map<String, Integer> rows, String literal) {
        return !rows.isEmpty() && rows.keySet().stream().allMatch(row -> {
            List<String> values = decodeCanonicalRow(row);
            return values.size() == 1 && literal.equals(values.get(0));
        });
    }

    private boolean allRowsHaveValueCount(QueryResultMultiset rows, int valueCount) {
        return !rows.rows.isEmpty()
                && rows.rows.keySet().stream().allMatch(row -> decodeCanonicalRow(row).size() == valueCount);
    }

    private boolean anyRowHasNullFirstAndLaterNonNull(Map<String, Integer> rows) {
        for (String row : rows.keySet()) {
            List<String> values = decodeCanonicalRow(row);
            if (values.size() < 2 || values.get(0) != null) {
                continue;
            }
            for (int i = 1; i < values.size(); i++) {
                if (values.get(i) != null) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean aggregateArgumentsLookConstant(String query, String sourceTable) {
        String normalized = query.toUpperCase(Locale.ROOT);
        String sourceAlias = sourceTable.toUpperCase(Locale.ROOT) + ".";
        for (String aggregate : List.of("SUM(", "AVG(", "MIN(", "MAX(", "COUNT(", "BIT_AND(", "BIT_OR(", "BIT_XOR(",
                "STD(", "VAR_SAMP(")) {
            int searchFrom = 0;
            while (true) {
                int start = normalized.indexOf(aggregate, searchFrom);
                if (start < 0) {
                    break;
                }
                int argStart = start + aggregate.length();
                int argEnd = findMatchingParen(query, argStart - 1);
                if (argEnd > argStart) {
                    String argument = normalized.substring(argStart, argEnd);
                    if (!argument.contains(sourceAlias)
                            && !argument.contains("`" + ROW_ID.toUpperCase(Locale.ROOT) + "`")
                            && !argument.contains(ROW_ID.toUpperCase(Locale.ROOT))) {
                        return true;
                    }
                }
                searchFrom = argStart;
            }
        }
        return false;
    }

    private int findMatchingParen(String text, int openParenIndex) {
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inBacktick = false;
        for (int i = openParenIndex; i < text.length(); i++) {
            char ch = text.charAt(i);
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
                    if (depth == 0) {
                        return i;
                    }
                }
            }
        }
        return -1;
    }

    private void logKnownBug(VpQueryPair pair, KnownVpBug knownBug, QueryResultMultiset singleRows,
            QueryResultMultiset splitRows) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("Known VP bug: ").append(knownBug).append(System.lineSeparator()).append(System.lineSeparator());
        sb.append(formatBugReport(pair, singleRows, splitRows));
        File dir = new File(Main.LOG_DIRECTORY, "mysql");
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
                || msg.contains("Out of sort memory") || msg.contains("sort buffer size")
                || msg.contains("Operand should contain") || msg.contains("Subquery returns more than 1 row");
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
        sb.append("MySQL version: ").append(getSingleString("SELECT VERSION()")).append(System.lineSeparator());
        sb.append("Base table: ").append(pair.schema.baseTable).append(System.lineSeparator());
        sb.append("Base table DDL: ").append(getCreateTable(pair.schema.baseTable)).append(System.lineSeparator());
        sb.append("Base table engine: ").append(engineName(pair.schema.baseEngine)).append(System.lineSeparator());
        sb.append("VP source engine: ").append(engineName(pair.schema.sourceEngine)).append(System.lineSeparator());
        sb.append("VP left engine: ").append(engineName(pair.schema.leftEngine)).append(System.lineSeparator());
        sb.append("VP right engine: ").append(engineName(pair.schema.rightEngine)).append(System.lineSeparator());
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

    private String getSingleString(String sql) throws SQLException {
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getString(1);
            }
            return "<none>";
        }
    }

    private String captureSessionVars() {
        List<String> vars = List.of("sql_mode", "time_zone", "character_set_connection", "collation_connection",
                "optimizer_switch", "big_tables", "sql_buffer_result", "max_sort_length", "tmp_table_size",
                "max_heap_table_size", "internal_tmp_mem_storage_engine", "sort_buffer_size", "read_rnd_buffer_size",
                "join_buffer_size");
        return vars.stream().map(this::captureSessionVar).collect(Collectors.joining(", "));
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
        String explain = "EXPLAIN FORMAT=TREE " + query;
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
        executeInternalStatement(sql, sql.startsWith("CREATE TABLE"));
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
        executeInternalStatement(sql, false);
    }

    private void executeInternalStatement(String sql, boolean couldAffectSchema) throws Exception {
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
        boolean success = query.execute(state);
        if (state.getOptions().logExecutionTime()) {
            state.getLogger().writeCurrent(" -- " + timer.end().asString());
        }
        if (success && couldAffectSchema) {
            state.updateSchema();
        }
    }

    private String toSQLType(MySQLDataType type) {
        switch (type) {
        case INT:
            return "BIGINT";
        case VARCHAR:
            return "TEXT";
        case FLOAT:
            return "FLOAT";
        case DOUBLE:
            return "DOUBLE";
        case DECIMAL:
            return "DECIMAL(65, 30)";
        default:
            throw new AssertionError(type);
        }
    }

    private String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private String engineName(MySQLEngine engine) {
        switch (engine) {
        case INNO_DB:
            return "InnoDB";
        case MY_ISAM:
            return "MyISAM";
        case MEMORY:
            return "MEMORY";
        case HEAP:
            return "HEAP";
        case CSV:
            return "CSV";
        case MERGE:
            return "MERGE";
        case ARCHIVE:
            return "ARCHIVE";
        case FEDERATED:
            return "FEDERATED";
        default:
            throw new AssertionError(engine);
        }
    }

    private String sanitizeIdentifier(String identifier) {
        return identifier.replaceAll("[^A-Za-z0-9_]", "_");
    }
}
