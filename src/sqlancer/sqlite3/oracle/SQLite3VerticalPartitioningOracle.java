package sqlancer.sqlite3.oracle;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Provider;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Constant;
import sqlancer.sqlite3.ast.SQLite3Aggregate;
import sqlancer.sqlite3.ast.SQLite3Aggregate.SQLite3AggregateFunction;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3TableReference;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3Text;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.schema.SQLite3DataType;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column.SQLite3CollateSequence;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;

public class SQLite3VerticalPartitioningOracle implements TestOracle<SQLite3GlobalState> {

    private static final AtomicLong ORACLE_RUN_ID = new AtomicLong();
    private static final String ROW_ID = "vp_rowid";
    private static final int MAX_DIFF_ROWS = 8;
    private static final int MAX_RESULT_SAMPLE = 12;
    private static final int MAX_CANONICAL_VALUE_LENGTH = 1 << 20;
    private static final int MAX_CANONICAL_ROW_LENGTH = 4 << 20;
    private static final double FLOAT_ABSOLUTE_TOLERANCE = 1e-6;
    private static final double FLOAT_RELATIVE_TOLERANCE = 1e-5;
    private static final double DOUBLE_ABSOLUTE_TOLERANCE = 1e-12;
    private static final double DOUBLE_RELATIVE_TOLERANCE = 1e-9;
    private static final Object STATS_LOCK = new Object();
    private static final Map<SplitStrategy, Integer> SPLIT_STRATEGY_COUNTS = new EnumMap<>(SplitStrategy.class);
    private static final Map<BaseShape, Integer> SELECTED_BASE_SHAPE_COUNTS = new EnumMap<>(BaseShape.class);
    private static final Map<ReconstructionStrategy, Integer> RECONSTRUCTION_STRATEGY_COUNTS = new EnumMap<>(
            ReconstructionStrategy.class);
    private static final Map<ReconstructionStrategy, EnumMap<PairStage, Integer>> PAIR_STAGE_COUNTS = new EnumMap<>(
            ReconstructionStrategy.class);
    private static final Map<String, Integer> SKIPPED_STAGE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SKIPPED_ERROR_COUNTS = new LinkedHashMap<>();
    private static final Map<String, String> SKIPPED_ERROR_SAMPLES = new LinkedHashMap<>();
    private static final Map<KnownVpBug, Integer> KNOWN_BUG_COUNTS = new EnumMap<>(KnownVpBug.class);
    private static final Map<AdvancedFeature, Integer> ADVANCED_FEATURE_COUNTS = new EnumMap<>(AdvancedFeature.class);
    private static final Map<String, Integer> SELECTED_BASE_ENGINE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SCHEMA_COPY_SKIP_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SCHEMA_COPY_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> HELPER_INDEX_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SCHEMA_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SOURCE_ROW_COUNT_BUCKETS = new LinkedHashMap<>();
    private static final AtomicBoolean SHUTDOWN_HOOK_REGISTERED = new AtomicBoolean();
    private static final AtomicLong KNOWN_BUG_LOG_ID = new AtomicLong();
    private static final AtomicInteger CORRUPTION_SNAPSHOT_ID = new AtomicInteger();
    private static final File STATS_FILE = new File(new File(Main.LOG_DIRECTORY, "sqlite3"), "vp-stats.log");
    private static final Pattern SQLITE_CONTEXT_SENSITIVE_FUNCTION = Pattern
            .compile("(?i)(^|[^A-Z0-9_])(CHANGES|TOTAL_CHANGES|LAST_INSERT_ROWID|LOAD_EXTENSION)\\s*\\(");
    private static int completedOracleRuns;
    private static int completedQueryPairs;
    private static int completedAttempts;
    private static int observedOracleRuns;
    private static int observedQueryPairs;
    private static int observedAttempts;
    private static int incompleteOracleRuns;
    private static int schemaShapeSamples;

    private final SQLite3GlobalState state;
    private final ExpectedErrors errors = new ExpectedErrors();
    /*
     * Pair execution must not use the generic SQLite error umbrella. In particular, "SQL error or missing database"
     * also matches missing helper tables and would turn a broken VP materialization into a silent IgnoreMeException.
     */
    private final ExpectedErrors pairErrors = new ExpectedErrors();

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
        NATURAL_OR_USING, STRAIGHT_JOIN
    }

    private enum PairStage {
        GENERATED, EXTRA_SETUP_OK, SINGLE_OK, SPLIT_OK, COMPARED, MISMATCH
    }

    private enum KnownVpBug {
        ;
    }

    public enum AdvancedFeature {
        ROLLUP, CORRELATED_SUBQUERY, GROUP_CONCAT, ASYMMETRIC_INDEX, CHARSET_COLLATION_COLUMN, BOUNDARY_MUTATION,
        GENERATED_COLUMN_SNAPSHOT, VIRTUAL_TABLE_SNAPSHOT, FTS_SNAPSHOT, RTREE_SNAPSHOT, INTEGER_PRIMARY_KEY_ROWID,
        CASE_IMPLICIT_TYPE_CONVERSION, NUMERIC_AFFINITY
    }

    private static final List<ReconstructionStrategy> SUPPORTED_RECONSTRUCTION_STRATEGIES = List.of(
            ReconstructionStrategy.JOIN, ReconstructionStrategy.LEFT_RIGHT_JOIN,
            ReconstructionStrategy.FULL_OUTER_SIMULATION, ReconstructionStrategy.SCALAR_SUBQUERY,
            ReconstructionStrategy.EXISTS_FILTER, ReconstructionStrategy.IN_FILTER,
            ReconstructionStrategy.NOT_EXISTS_ANTI, ReconstructionStrategy.DERIVED_TABLE, ReconstructionStrategy.CTE,
            ReconstructionStrategy.VIEW, ReconstructionStrategy.TEMP_TABLE, ReconstructionStrategy.AGGREGATE_THEN_MERGE,
            ReconstructionStrategy.ROW_NUMBER_ALIGNMENT, ReconstructionStrategy.JSON_PACK,
            ReconstructionStrategy.STRING_PACK, ReconstructionStrategy.NATURAL_OR_USING,
            ReconstructionStrategy.STRAIGHT_JOIN);

    private static final class VpColumn {
        private final String name;
        private final SQLite3DataType type;
        private final SQLite3CollateSequence collate;
        private final boolean generated;
        private final String declaredType;
        private final boolean numericAffinity;

        private VpColumn(String name, SQLite3DataType type, SQLite3CollateSequence collate, boolean generated,
                String declaredType) {
            this.name = name;
            this.type = type;
            this.collate = collate;
            this.generated = generated;
            this.declaredType = declaredType;
            this.numericAffinity = hasNumericAffinity(declaredType, type);
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
        private final boolean hasGeneratedColumns;
        private final boolean virtualSnapshot;

        private BaseTableInfo(String tableName, List<VpColumn> columns, BaseShape shape, String stableOrderStrategy,
                long rowCount, String engine, String rowIdExpression, boolean hasGeneratedColumns,
                boolean virtualSnapshot) {
            this.tableName = tableName;
            this.columns = columns;
            this.shape = shape;
            this.stableOrderStrategy = stableOrderStrategy;
            this.rowCount = rowCount;
            this.engine = engine;
            this.rowIdExpression = rowIdExpression;
            this.hasGeneratedColumns = hasGeneratedColumns;
            this.virtualSnapshot = virtualSnapshot;
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
        private final boolean hasGeneratedColumns;
        private final boolean virtualSnapshot;
        private final Map<String, String> columnCollationClauses = new LinkedHashMap<>();
        private String sourceEngine;
        private String leftEngine;
        private String rightEngine;
        private final List<String> sourceIndexes = new ArrayList<>();
        private final List<String> setupStatements = new ArrayList<>();

        private VpSchema(String prefix, String baseTable, String sourceTable, String leftTable, String rightTable,
                String viewName, String tempTable, List<VpColumn> columns, List<VpColumn> leftColumns,
                List<VpColumn> rightColumns, List<VpColumn> overlapColumns, BaseShape baseShape,
                SplitStrategy splitStrategy, boolean usesPhysicalForeignKey, String stableOrderStrategy,
                long baseRowCount, String baseEngine, String rowIdExpression, boolean hasGeneratedColumns,
                boolean virtualSnapshot) {
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
            this.hasGeneratedColumns = hasGeneratedColumns;
            this.virtualSnapshot = virtualSnapshot;
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

    private static final class CopiedSchemaObject {
        private final String name;
        private final String sql;

        private CopiedSchemaObject(String name, String sql) {
            this.name = name;
            this.sql = sql;
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

    public SQLite3VerticalPartitioningOracle(SQLite3GlobalState state) {
        this.state = state;
        registerVpStatsShutdownHook();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        errors.add("This version of SQLite3 doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'");
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
        errors.add("Unsupported modify column");
        errors.add("COLLATION");

        SQLite3Errors.getExpectedExpressionErrors().stream()
                .filter(error -> !error.equals("[SQLITE_ERROR] SQL error or missing database"))
                .forEach(pairErrors::add);
    }

    @Override
    public void check() throws Exception {
        int checkedPairs = 0;
        int attempts = 0;
        boolean observedRun = false;
        boolean completedRun = false;
        try {
            resetSessionForVerticalPartitioningOracle();
            SQLite3Provider.ensureVerticalPartitioningBaseTables(state, 2);
            BaseTableInfo baseTable = selectBaseTable();
            observedRun = true;
            int queryVariantsPerCombination = Math.max(1,
                    state.getDbmsSpecificOptions().verticalPartitioningQueryVariantsPerCombination);
            List<SplitStrategy> splitSchedule = createSplitSchedule(baseTable.shape);
            for (SplitStrategy splitStrategy : splitSchedule) {
                VpSchema vp = null;
                boolean cleanupArtifacts = true;
                try {
                    vp = materializeTable(baseTable, splitStrategy);
                    incrementSplitStrategy(vp.splitStrategy);
                    List<ReconstructionStrategy> reconstructionSchedule = createReconstructionSchedule();
                    for (ReconstructionStrategy strategy : reconstructionSchedule) {
                        for (int i = 0; i < queryVariantsPerCombination; i++) {
                            attempts++;
                            VpQueryPair pair;
                            try {
                                pair = generateRandomRewritePair(vp, strategy);
                            } catch (IgnoreMeException ignored) {
                                recordSkippedStage(strategy, "GENERATE");
                                recordSkippedError("generate", strategy.name(), ignored);
                                continue;
                            }
                            recordPairStage(pair, PairStage.GENERATED);
                            try {
                                comparePair(pair);
                                incrementReconstructionStrategy(pair.reconstructionStrategy);
                                checkedPairs++;
                            } catch (IgnoreMeException ignored) {
                                // comparePair records the precise setup/single/split stage.
                            }
                        }
                    }
                } catch (IgnoreMeException ignored) {
                    recordSkippedStage(splitStrategy, "MATERIALIZE");
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
                completedRun = true;
            } else {
                throw new IgnoreMeException();
            }
            SQLite3Provider.replaceVerticalPartitioningTableAfterSuccessfulCheck(state, baseTable.tableName);
        } catch (AssertionError | SQLException e) {
            preserveCorruptionSnapshot("vp", e);
            throw e;
        } finally {
            if (observedRun) {
                recordVpRunStatistics(checkedPairs, attempts, completedRun);
            }
        }
    }

    private BaseTableInfo selectBaseTable() throws Exception {
        state.updateSchema();
        List<SQLite3Table> candidates = state.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> !t.getName().startsWith("vp_")).filter(t -> !t.getColumns().isEmpty())
                .filter(this::isSupportedVpInputTable).collect(Collectors.toList());
        if (candidates.isEmpty()) {
            throw new IgnoreMeException();
        }
        // FTS/RTree corruption is an independent SQLite finding. Preserve a
        // snapshot, but do not let one already-corrupt virtual table prevent VP
        // exploration of the remaining healthy tables in this database.
        Collections.shuffle(candidates);
        for (SQLite3Table candidate : candidates) {
            try {
                return createBaseTableInfo(candidate);
            } catch (SQLException e) {
                if (!isCorruptionThrowable(e)) {
                    throw e;
                }
                preserveCorruptionSnapshot("vp-probe", e);
            }
        }
        throw new IgnoreMeException();
    }

    private boolean isSupportedVpInputTable(SQLite3Table table) {
        if (table.isView() || table.isSystemTable() || table.isReadOnly()) {
            return false;
        }
        return canReadBaseColumns(table);
    }

    private boolean canReadBaseColumns(SQLite3Table table) {
        String columnList = table.getColumns().stream().map(c -> quote(c.getName())).collect(Collectors.joining(", "));
        if (columnList.isEmpty()) {
            return false;
        }
        try (Statement s = state.getConnection().createStatement(); ResultSet ignored = s
                .executeQuery("SELECT " + columnList + " FROM " + quote(table.getName()) + " LIMIT 1")) {
            while (ignored.next()) {
            }
            return true;
        } catch (SQLException ignored) {
            return false;
        }
    }

    private BaseTableInfo createBaseTableInfo(SQLite3Table base) throws Exception {
        List<VpColumn> sourceColumns = new ArrayList<>();
        for (SQLite3Column column : base.getColumns()) {
            sourceColumns.add(new VpColumn(column.getName(), column.getType(), column.getCollateSequence(),
                    column.isGenerated(), getDeclaredColumnType(base.getName(), column.getName())));
        }
        long rowCount = getRowCount(base.getName());
        BaseShape shape = rowCount == 0 ? BaseShape.EMPTY
                : sourceColumns.size() == 1 ? BaseShape.SINGLE_COLUMN : BaseShape.MULTI_COLUMN;
        incrementSelectedBaseShape(shape);
        String stableOrder = getStableOrder(base, sourceColumns);
        String rowIdExpression = getRowIdExpression(base, stableOrder);
        String createTable = getCreateTable(base.getName());
        String engine = extractEngineFromCreateTable(createTable, base);
        incrementSelectedBaseEngine(engine);
        boolean hasGeneratedColumns = sourceColumns.stream().anyMatch(c -> c.generated);
        if (sourceColumns.stream().anyMatch(c -> isExplicitNumericAffinity(c.declaredType))) {
            recordAdvancedFeature(AdvancedFeature.NUMERIC_AFFINITY);
        }
        boolean virtualSnapshot = base.isVirtual();
        if (hasGeneratedColumns) {
            recordAdvancedFeature(AdvancedFeature.GENERATED_COLUMN_SNAPSHOT);
        }
        if (virtualSnapshot) {
            recordAdvancedFeature(AdvancedFeature.VIRTUAL_TABLE_SNAPSHOT);
            if (engine.contains("FTS")) {
                recordAdvancedFeature(AdvancedFeature.FTS_SNAPSHOT);
            }
            if (engine.contains("RTREE")) {
                recordAdvancedFeature(AdvancedFeature.RTREE_SNAPSHOT);
            }
        }
        probeSpecializedBaseTable(base, sourceColumns, createTable);
        return new BaseTableInfo(base.getName(), sourceColumns, shape, stableOrder, rowCount, engine, rowIdExpression,
                hasGeneratedColumns, virtualSnapshot);
    }

    private void probeSpecializedBaseTable(SQLite3Table base, List<VpColumn> columns, String createTable)
            throws SQLException {
        if (base.isVirtual() && base.getName().startsWith("vt")) {
            String lowerCreateTable = createTable.toLowerCase(Locale.ROOT);
            if (lowerCreateTable.contains("detail=column") || lowerCreateTable.contains("detail=none")) {
                executeProbe("SELECT COUNT(*) FROM " + quote(base.getName()));
            } else {
                executeProbe("SELECT COUNT(*) FROM " + quote(base.getName()) + " WHERE " + quote(base.getName())
                        + " MATCH 'vp_probe'");
            }
        } else if (base.isVirtual() && base.getName().startsWith("rt")) {
            executeProbe("SELECT rtreecheck(" + quote(base.getName()) + ")");
        }
        if (columns.stream().anyMatch(c -> c.generated)) {
            executeProbe("SELECT " + columns.stream().map(c -> quote(c.name)).collect(Collectors.joining(", "))
                    + " FROM " + quote(base.getName()) + " LIMIT 1");
        }
    }

    private void executeProbe(String sql) throws SQLException {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                result.getString(1);
            }
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
        List<ReconstructionStrategy> shuffled = new ArrayList<>(SUPPORTED_RECONSTRUCTION_STRATEGIES);
        Collections.shuffle(shuffled);
        return shuffled;
    }

    private void resetSessionForVerticalPartitioningOracle() throws Exception {
        tryExecuteNoSchemaRefresh("PRAGMA foreign_keys = OFF");
        tryExecuteNoSchemaRefresh("PRAGMA recursive_triggers = OFF");
        tryExecuteNoSchemaRefresh("PRAGMA temp_store = MEMORY");
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
                base.hasGeneratedColumns, base.virtualSnapshot);
        boolean preserveArtifacts = false;
        try {
            initializeColumnCollations(vp);
            dropArtifacts(vp);
            createSourceTable(vp, helperEngineCandidate(base));
            populateSourceTable(vp, base.stableOrderStrategy);
            validateSourceSnapshot(vp);
            copyBaseSchemaObjectsToSource(vp);
            createPartitionTables(vp, helperEngineCandidate(base));
            populatePartitionTables(vp);
            validatePartitionRowCounts(vp);
            maybeCreateHelperIndexes(vp);
            if (useForeignKey) {
                tryExecuteSetup(vp,
                        "ALTER TABLE " + quote(vp.rightTable) + " ADD CONSTRAINT " + quote(vp.prefix + "fk_r_l")
                                + " FOREIGN KEY (" + quote(ROW_ID) + ") REFERENCES " + quote(vp.leftTable) + "("
                                + quote(ROW_ID) + ")");
            }
            // Keep the materialized tables alive while the caller generates and compares query pairs.
            // They are cleaned up by check() after all reconstruction strategies have run.
            preserveArtifacts = true;
            return vp;
        } catch (AssertionError | SQLException e) {
            // The outer check() preserves these failures as bug/corruption reproducers.
            preserveArtifacts = true;
            throw e;
        } finally {
            if (!preserveArtifacts) {
                // This also covers IgnoreMeException, which never returns to check().
                dropArtifacts(vp);
            }
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
                if (isNumericType(column)) {
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

    private boolean isNumericType(SQLite3DataType type) {
        return type == SQLite3DataType.INT || type == SQLite3DataType.REAL;
    }

    private boolean isNumericType(VpColumn column) {
        return column.numericAffinity || isNumericType(column.type);
    }

    private static boolean hasNumericAffinity(String declaredType, SQLite3DataType fallbackType) {
        if (declaredType == null || declaredType.isBlank()) {
            return fallbackType == SQLite3DataType.INT || fallbackType == SQLite3DataType.REAL;
        }
        String upper = declaredType.toUpperCase(Locale.ROOT);
        return upper.contains("INT") || upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB")
                || (!upper.contains("CHAR") && !upper.contains("CLOB") && !upper.contains("TEXT")
                        && !upper.contains("BLOB"));
    }

    private static boolean isExplicitNumericAffinity(String declaredType) {
        if (declaredType == null || declaredType.isBlank()) {
            return false;
        }
        String upper = declaredType.toUpperCase(Locale.ROOT);
        return !upper.contains("INT") && !upper.contains("REAL") && !upper.contains("FLOA") && !upper.contains("DOUB")
                && !upper.contains("CHAR") && !upper.contains("CLOB") && !upper.contains("TEXT")
                && !upper.contains("BLOB");
    }

    private String getStableOrder(SQLite3Table base, List<VpColumn> columns) {
        List<String> primary = base.getColumns().stream().filter(SQLite3Column::isPrimaryKey)
                .map(c -> quote(c.getName())).collect(Collectors.toList());
        if (!primary.isEmpty()) {
            return String.join(", ", primary);
        }
        List<String> comparable = columns.stream().filter(c -> c.type != SQLite3DataType.REAL).map(c -> quote(c.name))
                .collect(Collectors.toList());
        if (!comparable.isEmpty()) {
            return String.join(", ", comparable);
        }
        return null;
    }

    private String getRowIdExpression(SQLite3Table base, String stableOrder) {
        if (!base.hasWithoutRowid()) {
            List<SQLite3Column> integerPrimaryKeys = base.getColumns().stream()
                    .filter(SQLite3Column::isIntegerPrimaryKey).collect(Collectors.toList());
            if (advancedFeaturesEnabled() && !integerPrimaryKeys.isEmpty()) {
                String integerPrimaryKey = quote(Randomly.fromList(integerPrimaryKeys).getName());
                if (canSelectExpression(base.getName(), integerPrimaryKey)
                        && hasUniqueNonNullValues(base.getName(), integerPrimaryKey)) {
                    recordAdvancedFeature(AdvancedFeature.INTEGER_PRIMARY_KEY_ROWID);
                    return integerPrimaryKey;
                }
            }
            List<String> explicitColumnNames = base.getColumns().stream().map(c -> c.getName().toLowerCase(Locale.ROOT))
                    .collect(Collectors.toList());
            for (String candidate : List.of("rowid", "_rowid_", "oid")) {
                if (!explicitColumnNames.contains(candidate) && canSelectExpression(base.getName(), candidate)
                        && hasUniqueNonNullValues(base.getName(), candidate)) {
                    return candidate;
                }
            }
        }
        return rowNumberExpression(base, stableOrder);
    }

    private String rowNumberExpression(SQLite3Table base, String stableOrder) {
        String order = stableOrder;
        if (order == null || order.isEmpty()) {
            order = base.getColumns().stream().map(c -> quote(c.getName())).collect(Collectors.joining(", "));
        }
        if (order == null || order.isEmpty()) {
            order = "(SELECT 1)";
        }
        return "ROW_NUMBER() OVER (ORDER BY " + order + ")";
    }

    private boolean hasUniqueNonNullValues(String tableName, String expression) {
        String sql = "SELECT COUNT(*) = COUNT(" + expression + ") AND COUNT(*) = COUNT(DISTINCT " + expression
                + ") FROM " + quote(tableName);
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() && rs.getInt(1) != 0;
        } catch (SQLException ignored) {
            return false;
        }
    }

    private boolean canSelectExpression(String tableName, String expression) {
        try (Statement s = state.getConnection().createStatement();
                ResultSet ignored = s.executeQuery("SELECT " + expression + " FROM " + quote(tableName) + " LIMIT 1")) {
            while (ignored.next()) {
            }
            return true;
        } catch (SQLException ignored) {
            return false;
        }
    }

    private String getDeclaredColumnType(String tableName, String columnName) throws SQLException {
        String pragma = "PRAGMA table_xinfo(" + quote(tableName) + ")";
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(pragma)) {
            while (rs.next()) {
                if (columnName.equals(rs.getString("name"))) {
                    String type = rs.getString("type");
                    return type == null ? "" : type.trim();
                }
            }
        }
        return "";
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
        if (advancedFeaturesEnabled() && (isWithoutRowIdEngine(base.engine) || isStrictEngine(base.engine))) {
            return base.engine;
        }
        if (state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine && !isWithoutRowIdEngine(base.engine)
                && !isStrictEngine(base.engine)) {
            return base.engine;
        }
        return "ROWID";
    }

    private void createSourceTable(VpSchema vp, String preferredEngine) throws Exception {
        vp.sourceEngine = createTableWithEngineFallback(vp, vp.sourceTable, sourceColumnDefs(vp), preferredEngine);
    }

    private void initializeColumnCollations(VpSchema vp) {
        for (VpColumn column : vp.columns) {
            String clause = inheritedCollationClause(column);
            if (!clause.isEmpty()) {
                recordAdvancedFeature(AdvancedFeature.CHARSET_COLLATION_COLUMN);
            }
            vp.columnCollationClauses.put(column.name, clause);
        }
    }

    private String inheritedCollationClause(VpColumn column) {
        if (column.collate == null || column.collate == SQLite3CollateSequence.BINARY) {
            return "";
        }
        return " COLLATE " + column.collate.name();
    }

    private List<String> sourceColumnDefs(VpSchema vp) {
        List<String> columnDefs = new ArrayList<>();
        columnDefs.add(quote(ROW_ID) + " INTEGER NOT NULL");
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
        } catch (IgnoreMeException e) {
            if (!canFallbackHelperEngine(vp.sourceEngine)) {
                throw e;
            }
            vp.sourceEngine = recreateHelperTableWithRowId(vp, vp.sourceTable, sourceColumnDefs(vp));
            executeSetup(vp, populateSourceSQL(vp, stableOrder));
        }
    }

    private String populateSourceSQL(VpSchema vp, String stableOrder) {
        String columnList = vp.columns.stream().map(c -> quote(c.name)).collect(Collectors.joining(", "));
        String sql = "INSERT INTO " + quote(vp.sourceTable) + " (" + quote(ROW_ID)
                + (columnList.isEmpty() ? "" : ", " + columnList) + ") SELECT " + vp.rowIdExpression
                + (columnList.isEmpty() ? "" : ", " + columnList) + " FROM " + quote(vp.baseTable);
        if (stableOrder != null) {
            sql += " ORDER BY " + stableOrder;
        } else {
            sql += " ORDER BY " + vp.rowIdExpression;
        }
        if (state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows > 0) {
            sql += " LIMIT " + state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows;
        }
        return sql;
    }

    private void validateSourceSnapshot(VpSchema vp) throws SQLException {
        // Every reconstruction relies on a one-to-one row-id key. SQLite
        // permits nullable/non-unique values for declarations such as
        // "INTEGER PRIMARY KEY DESC"; using such a column as vp_rowid causes
        // joins (especially STRING_PACK) to multiply rows and creates a false
        // VP mismatch. Reject the pair before executing comparisons.
        // ROW_NUMBER() is generated by the oracle and is unique by
        // construction; it cannot be passed through COUNT(DISTINCT ...)
        // because SQLite rejects window functions in aggregate arguments.
        boolean generatedRowNumbers = vp.rowIdExpression.trim().toUpperCase(Locale.ROOT).startsWith("ROW_NUMBER(");
        if ((!generatedRowNumbers && !hasUniqueNonNullValues(vp.baseTable, vp.rowIdExpression))
                || !hasUniqueNonNullValues(vp.sourceTable, quote(ROW_ID))) {
            throw new IgnoreMeException();
        }
        String baseQuery = snapshotSourceComparisonQuery(vp, vp.baseTable, vp.rowIdExpression, vp.stableOrderStrategy,
                true);
        String sourceQuery = snapshotSourceComparisonQuery(vp, vp.sourceTable, quote(ROW_ID), quote(ROW_ID), false);
        Map<String, String> baseRows = readSnapshotComparison(vp, baseQuery);
        Map<String, String> sourceRows = readSnapshotComparison(vp, sourceQuery);
        if (!baseRows.equals(sourceRows)) {
            if (onlyAffinityStorageClassChanges(baseRows, sourceRows)) {
                throw new IgnoreMeException();
            }
            throw new AssertionError("VP base-to-source snapshot mismatch for " + vp.baseTable + ": baseRows="
                    + baseRows.size() + ", sourceRows=" + sourceRows.size() + ", onlyInBase="
                    + snapshotDifference(baseRows, sourceRows) + ", onlyInSource="
                    + snapshotDifference(sourceRows, baseRows));
        }
    }

    private boolean onlyAffinityStorageClassChanges(Map<String, String> baseRows, Map<String, String> sourceRows) {
        if (!baseRows.keySet().equals(sourceRows.keySet())) {
            return false;
        }
        for (String rowId : baseRows.keySet()) {
            String[] baseFields = baseRows.get(rowId).split("\\|", -1);
            String[] sourceFields = sourceRows.get(rowId).split("\\|", -1);
            if (baseFields.length != sourceFields.length) {
                return false;
            }
            for (int i = 0; i < baseFields.length; i++) {
                if (baseFields[i].equals(sourceFields[i])) {
                    continue;
                }
                String[] baseValue = baseFields[i].split(":", 2);
                String[] sourceValue = sourceFields[i].split(":", 2);
                if (baseValue.length != 2 || sourceValue.length != 2 || !isNumericStorageClass(baseValue[0])
                        || !isNumericStorageClass(sourceValue[0])
                        || !numericStorageValuesEqual(baseValue[1], sourceValue[1])) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isNumericStorageClass(String type) {
        return "integer".equals(type) || "real".equals(type);
    }

    private boolean numericStorageValuesEqual(String left, String right) {
        try {
            return new BigDecimal(left).compareTo(new BigDecimal(right)) == 0;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    private String snapshotSourceComparisonQuery(VpSchema vp, String table, String rowIdExpression,
            String orderExpression, boolean applyLimit) {
        List<String> expressions = new ArrayList<>();
        expressions.add(rowIdExpression);
        expressions.addAll(vp.columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        List<String> selected = new ArrayList<>();
        for (String expression : expressions) {
            selected.add("typeof(" + expression + ")");
            selected.add("quote(" + expression + ")");
        }
        String query = "SELECT " + String.join(", ", selected) + " FROM " + quote(table);
        if (orderExpression != null) {
            query += " ORDER BY " + orderExpression;
        }
        if (applyLimit && state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows > 0) {
            query += " LIMIT " + state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows;
        }
        return query;
    }

    private Map<String, String> readSnapshotComparison(VpSchema vp, String query) throws SQLException {
        Map<String, String> rows = new LinkedHashMap<>();
        int fieldCount = vp.columns.size() + 1;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(query)) {
            while (result.next()) {
                String rowId = result.getString(2);
                StringBuilder row = new StringBuilder();
                for (int field = 0; field < fieldCount; field++) {
                    if (field != 0) {
                        row.append('|');
                    }
                    int typeColumn = field * 2 + 1;
                    row.append(result.getString(typeColumn)).append(':').append(result.getString(typeColumn + 1));
                }
                if (rows.put(rowId, row.toString()) != null) {
                    throw new AssertionError("Duplicate VP row id in snapshot: " + rowId);
                }
            }
        }
        return rows;
    }

    private String snapshotDifference(Map<String, String> left, Map<String, String> right) {
        return left.entrySet().stream().filter(entry -> !Objects.equals(entry.getValue(), right.get(entry.getKey())))
                .limit(MAX_DIFF_ROWS).map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(", "));
    }

    private void createPartitionTables(VpSchema vp, String preferredEngine) throws Exception {
        vp.leftEngine = createPartitionTable(vp, vp.leftTable, vp.leftColumns, preferredEngine);
        vp.rightEngine = createPartitionTable(vp, vp.rightTable, vp.rightColumns, preferredEngine);
        validateHelperCollations(vp);
    }

    /**
     * A VP helper must preserve the base column collation. Older SQLite VP runs silently dropped declarations such as
     * COLLATE RTRIM, which changes ORDER BY/window results and produces a false VP mismatch.
     */
    private void validateHelperCollations(VpSchema vp) throws SQLException {
        for (String table : List.of(vp.leftTable, vp.rightTable)) {
            String ddl = null;
            try (Statement s = state.getConnection().createStatement();
                    ResultSet rs = s.executeQuery("SELECT sql FROM sqlite_master WHERE name='"
                            + table.replace("'", "''") + "' UNION ALL SELECT sql FROM sqlite_temp_master WHERE name='"
                            + table.replace("'", "''") + "'")) {
                if (rs.next()) {
                    ddl = rs.getString(1);
                }
            }
            if (ddl == null) {
                continue;
            }
            String lowerDdl = ddl.toLowerCase(Locale.ROOT);
            for (VpColumn column : vp.columns) {
                String clause = vp.columnCollationClauses.getOrDefault(column.name, "").trim();
                if (!clause.isEmpty() && lowerDdl.indexOf(clause.toLowerCase(Locale.ROOT)) < 0) {
                    throw new IgnoreMeException();
                }
            }
        }
    }

    private String createPartitionTable(VpSchema vp, String tableName, List<VpColumn> columns, String preferredEngine)
            throws Exception {
        return createTableWithEngineFallback(vp, tableName, partitionColumnDefs(vp, columns), preferredEngine);
    }

    private List<String> partitionColumnDefs(VpSchema vp, List<VpColumn> columns) {
        List<String> defs = new ArrayList<>();
        defs.add(quote(ROW_ID) + " INTEGER NOT NULL");
        for (VpColumn column : columns) {
            defs.add(columnDef(vp, column));
        }
        return defs;
    }

    private String columnDef(VpSchema vp, VpColumn column) {
        return quote(column.name) + " " + helperColumnType(column)
                + vp.columnCollationClauses.getOrDefault(column.name, "") + " NULL";
    }

    private String plainColumnDef(VpColumn column) {
        return quote(column.name) + " " + helperColumnType(column) + " NULL";
    }

    private String helperColumnType(VpColumn column) {
        return column.declaredType.isEmpty() ? toSQLType(column.type) : column.declaredType;
    }

    private boolean isCharacterLike(SQLite3DataType type) {
        return type == SQLite3DataType.TEXT;
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
            return recreateHelperTableWithRowId(vp, tableName, columnDefs);
        }
    }

    private boolean containsCharsetCollation(List<String> columnDefs) {
        return columnDefs.stream().anyMatch(def -> def.toUpperCase(Locale.ROOT).contains(" COLLATE "));
    }

    private List<String> plainColumnDefs(String tableName, VpSchema vp) {
        if (tableName.equals(vp.sourceTable)) {
            List<String> defs = new ArrayList<>();
            defs.add(quote(ROW_ID) + " INTEGER NOT NULL");
            vp.columns.stream().map(this::plainColumnDef).forEach(defs::add);
            return defs;
        }
        List<VpColumn> columns = tableName.equals(vp.leftTable) ? vp.leftColumns : vp.rightColumns;
        List<String> defs = new ArrayList<>();
        defs.add(quote(ROW_ID) + " INTEGER NOT NULL");
        columns.stream().map(this::plainColumnDef).forEach(defs::add);
        return defs;
    }

    private String createTableSQL(String tableName, List<String> columnDefs, String engine) {
        List<String> definitions = new ArrayList<>(columnDefs);
        if (isWithoutRowIdEngine(engine)) {
            if (definitions.isEmpty()) {
                throw new AssertionError("WITHOUT ROWID helper table has no key column: " + tableName);
            }
            definitions.set(0, definitions.get(0) + " PRIMARY KEY");
        }
        String suffix = "";
        if (isWithoutRowIdEngine(engine) && isStrictEngine(engine)) {
            suffix = " STRICT, WITHOUT ROWID";
        } else if (isWithoutRowIdEngine(engine)) {
            suffix = " WITHOUT ROWID";
        } else if (isStrictEngine(engine)) {
            suffix = " STRICT";
        }
        return "CREATE TABLE " + quote(tableName) + " (" + String.join(", ", definitions) + ")" + suffix;
    }

    private boolean canFallbackHelperEngine(String engine) {
        return engine != null && !"ROWID".equalsIgnoreCase(engine)
                && (state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine
                        || isWithoutRowIdEngine(engine) || isStrictEngine(engine));
    }

    private boolean isWithoutRowIdEngine(String engine) {
        return engine != null && engine.toUpperCase(Locale.ROOT).contains("WITHOUT_ROWID");
    }

    private boolean isStrictEngine(String engine) {
        return engine != null && engine.toUpperCase(Locale.ROOT).contains("STRICT");
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

    private void validatePartitionRowCounts(VpSchema vp) throws Exception {
        long sourceRows = getRowCount(vp.sourceTable);
        long leftRows = getRowCount(vp.leftTable);
        long rightRows = getRowCount(vp.rightTable);
        if (sourceRows != leftRows || sourceRows != rightRows) {
            vp.setupStatements.add("-- skipped VP setup: partition row-count mismatch source=" + sourceRows + " left="
                    + leftRows + " right=" + rightRows);
            throw new IgnoreMeException();
        }
        recordSourceRowCount(sourceRows);
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
        boolean created = false;
        if (!sourceIndexable.isEmpty() && Randomly.getBoolean()) {
            VpColumn c = Randomly.fromList(sourceIndexable);
            String indexName = vp.prefix + "src_asym_" + Math.abs(state.getRandomly().getInteger());
            created |= executeOptionalHelperIndexSetup(vp, vp.sourceTable, indexName, "CREATE INDEX " + quote(indexName)
                    + " ON " + quote(vp.sourceTable) + "(" + quote(c.name) + ", " + quote(ROW_ID) + ")");
        }
        if (!leftIndexable.isEmpty() && Randomly.getBoolean()) {
            VpColumn c = Randomly.fromList(leftIndexable);
            String indexName = vp.prefix + "l_asym_" + Math.abs(state.getRandomly().getInteger());
            created |= executeOptionalHelperIndexSetup(vp, vp.leftTable, indexName, "CREATE INDEX " + quote(indexName)
                    + " ON " + quote(vp.leftTable) + "(" + quote(ROW_ID) + ", " + quote(c.name) + ")");
        }
        if (!rightIndexable.isEmpty() && Randomly.getBoolean()) {
            VpColumn c = Randomly.fromList(rightIndexable);
            String indexName = vp.prefix + "r_asym_" + Math.abs(state.getRandomly().getInteger());
            created |= executeOptionalHelperIndexSetup(vp, vp.rightTable, indexName, "CREATE INDEX " + quote(indexName)
                    + " ON " + quote(vp.rightTable) + "(" + quote(c.name) + ", " + quote(ROW_ID) + ")");
        }
        if (created) {
            recordAdvancedFeature(AdvancedFeature.ASYMMETRIC_INDEX);
        }
    }

    private List<VpColumn> helperIndexableColumns(List<VpColumn> columns) {
        return columns.stream().filter(c -> isSQLite3HelperIndexable(c.type)).collect(Collectors.toList());
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
        String rawIndexName = vp.prefix + "idx_" + suffix + "_" + Math.abs(state.getRandomly().getInteger());
        executeOptionalHelperIndexSetup(vp, tableName, rawIndexName, "CREATE INDEX " + quote(rawIndexName) + " ON "
                + quote(tableName) + "(" + String.join(", ", indexColumns) + ")");
    }

    private boolean executeOptionalHelperIndexSetup(VpSchema vp, String tableName, String indexName, String sql) {
        recordHelperIndexOutcome("attempted");
        try {
            executeSetup(vp, sql);
        } catch (AssertionError ignored) {
            recordHelperIndexOutcome("skipped");
            vp.setupStatements.add("-- skipped helper index: " + sql);
            return false;
        } catch (Exception ignored) {
            recordHelperIndexOutcome("skipped");
            vp.setupStatements.add("-- skipped helper index: " + sql);
            return false;
        }
        recordHelperIndexOutcome("succeeded");
        if (tableName.equals(vp.sourceTable)) {
            vp.sourceIndexes.add(indexName);
            verifySourceIndexConsistency(vp, indexName);
        }
        return true;
    }

    private boolean isSQLite3HelperIndexable(SQLite3DataType type) {
        return type != SQLite3DataType.TEXT && type != SQLite3DataType.BINARY;
    }

    private void verifySourceIndexConsistency(VpSchema vp, String indexName) {
        try {
            String tableScan = sourceAccessQuery(vp, " NOT INDEXED");
            String indexScan = sourceAccessQuery(vp, " INDEXED BY " + quote(indexName));
            QueryResultMultiset tableRows = getResultMultiset(tableScan);
            QueryResultMultiset indexRows = getResultMultiset(indexScan);
            if (!resultMultisetsMatch(tableRows, indexRows)) {
                throw new AssertionError(formatSourceIndexInconsistencyReport(vp, indexName, tableScan, indexScan,
                        tableRows, indexRows));
            }
        } catch (IgnoreMeException ignored) {
            vp.setupStatements
                    .add("-- skipped source index self-check: " + indexName + " reason=index-forced-query-unavailable");
        } catch (SQLException e) {
            if (isNoQuerySolution(e)) {
                vp.setupStatements
                        .add("-- skipped source index self-check: " + indexName + " reason=no-query-solution");
                return;
            }
            throw new AssertionError(formatSourceIndexInconsistencyReport(vp, indexName,
                    sourceAccessQuery(vp, " NOT INDEXED"), sourceAccessQuery(vp, " INDEXED BY " + quote(indexName)),
                    "<unavailable: " + compactErrorReason(e) + ">", "<unavailable: " + compactErrorReason(e) + ">"), e);
        } catch (AssertionError e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError(formatSourceIndexInconsistencyReport(vp, indexName,
                    sourceAccessQuery(vp, " NOT INDEXED"), sourceAccessQuery(vp, " INDEXED BY " + quote(indexName)),
                    "<unavailable: " + compactErrorReason(e) + ">", "<unavailable: " + compactErrorReason(e) + ">"), e);
        }
    }

    private String sourceAccessQuery(VpSchema vp, String accessHint) {
        List<String> columns = new ArrayList<>();
        columns.add(quote(ROW_ID));
        columns.addAll(vp.columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        return "SELECT " + String.join(", ", columns) + " FROM " + quote(vp.sourceTable) + accessHint;
    }

    private String formatSourceIndexInconsistencyReport(VpSchema vp, String indexName, String tableScan,
            String indexScan, Object tableRows, Object indexRows) {
        StringBuilder sb = new StringBuilder();
        sb.append("[VERTICAL PARTITIONING SOURCE INDEX SELF-CHECK FAILURE]").append(System.lineSeparator());
        sb.append("Database: ").append(state.getDatabaseName()).append(System.lineSeparator());
        sb.append("SQLite3 version: ").append(safeSingleString("SELECT sqlite_version()"))
                .append(System.lineSeparator());
        sb.append("Base table: ").append(vp.baseTable).append(System.lineSeparator());
        sb.append("Source table: ").append(vp.sourceTable).append(System.lineSeparator());
        sb.append("Source index: ").append(indexName).append(System.lineSeparator());
        sb.append("Table-scan query: ").append(tableScan).append(System.lineSeparator());
        sb.append("Index-scan query: ").append(indexScan).append(System.lineSeparator());
        sb.append("Table-scan plan:").append(System.lineSeparator()).append(formatQueryPlan(tableScan));
        sb.append("Index-scan plan:").append(System.lineSeparator()).append(formatQueryPlan(indexScan));
        if (tableRows instanceof QueryResultMultiset && indexRows instanceof QueryResultMultiset) {
            QueryResultMultiset table = (QueryResultMultiset) tableRows;
            QueryResultMultiset index = (QueryResultMultiset) indexRows;
            sb.append("Table-scan result sample: ").append(sample(table.rows)).append(System.lineSeparator());
            sb.append("Index-scan result sample: ").append(sample(index.rows)).append(System.lineSeparator());
            sb.append("Only in table scan: ").append(diff(table, index)).append(System.lineSeparator());
            sb.append("Only in index scan: ").append(diff(index, table)).append(System.lineSeparator());
        } else {
            sb.append("Table-scan result sample: ").append(tableRows).append(System.lineSeparator());
            sb.append("Index-scan result sample: ").append(indexRows).append(System.lineSeparator());
        }
        sb.append("Integrity checks:").append(System.lineSeparator()).append(formatIntegrityChecks());
        sb.append("Database snapshot: ").append(saveDatabaseSnapshot(vp)).append(System.lineSeparator());
        return sb.toString();
    }

    private void copyBaseSchemaObjectsToSource(VpSchema vp) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSourceCopySchemaObjects) {
            return;
        }
        int copied = 0;
        for (CopiedSchemaObject index : getBaseIndexCreateStatements(vp.baseTable)) {
            recordSchemaCopyOutcome("attempted");
            String rewritten = rewriteSQLiteIndexForSource(vp, index.sql, copied);
            if (rewritten == null) {
                recordSchemaCopyOutcome("skipped");
                recordSchemaCopySkip("rewrite-unmatched");
                vp.setupStatements.add("-- skipped copied schema object: " + index.name + " reason=rewrite-unmatched");
                continue;
            }
            String copiedIndexName = vp.prefix + "src_idx_copy_" + copied;
            boolean created = false;
            try {
                executeSetup(vp, rewritten);
                recordSchemaCopyOutcome("succeeded");
                created = true;
            } catch (AssertionError e) {
                recordSchemaCopyOutcome("skipped");
                recordSchemaCopySkip("execute-assertion");
                vp.setupStatements.add("-- skipped copied schema object: " + index.name + " reason="
                        + compactErrorReason(e) + " sql=" + rewritten);
            } catch (Exception e) {
                recordSchemaCopyOutcome("skipped");
                recordSchemaCopySkip("execute-exception");
                vp.setupStatements.add("-- skipped copied schema object: " + index.name + " reason="
                        + compactErrorReason(e) + " sql=" + rewritten);
            }
            if (created) {
                vp.sourceIndexes.add(copiedIndexName);
                verifySourceIndexConsistency(vp, copiedIndexName);
                copied++;
            }
        }
    }

    private List<CopiedSchemaObject> getBaseIndexCreateStatements(String baseTable) {
        List<CopiedSchemaObject> result = new ArrayList<>();
        String sql = "SELECT name, sql FROM sqlite_schema WHERE type = 'index' AND tbl_name = " + "'"
                + escapeLiteral(baseTable) + "' AND sql IS NOT NULL AND name NOT LIKE 'sqlite_autoindex_%'";
        try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                result.add(new CopiedSchemaObject(rs.getString(1), rs.getString(2)));
            }
        } catch (SQLException ignored) {
            recordSchemaCopyOutcome("skipped");
            recordSchemaCopySkip("read-schema-failed");
        }
        return result;
    }

    private String rewriteSQLiteIndexForSource(VpSchema vp, String indexSql, int ordinal) {
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

    private VpQueryPair generateRandomRewritePair(VpSchema vp, ReconstructionStrategy strategy) throws Exception {
        SourceAst sourceAst = createSourceAst(vp);
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(state).setColumns(sourceAst.columns)
                .allowFtsFunctions(false);
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()) {
            return generateRollupPair(vp, strategy, sourceAst);
        }
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()) {
            return generateCorrelatedSubqueryPair(vp, strategy, sourceAst);
        }
        SQLite3Select select = new SQLite3Select();
        int fetchColumns = Math.max(1, Randomly.smallNumber() + 1);
        boolean useAggregate = Randomly.getBoolean();
        boolean hasWindowExpression = false;
        List<SQLite3Expression> selected = new ArrayList<>();
        List<SQLite3Expression> aggregateExpressions = new ArrayList<>();
        List<SQLite3Expression> nonAggregateExpressions = new ArrayList<>();
        List<SQLite3Expression> groupByExpressions = new ArrayList<>();
        if (useAggregate) {
            SQLite3Expression aggregate = generateVpAggregate(gen, sourceAst.columns);
            aggregateExpressions.add(aggregate);
            selected.add(aggregate);
            for (int i = 1; i < fetchColumns; i++) {
                SQLite3Expression expr = comparisonStableColumnOrRowIdExpression(sourceAst);
                nonAggregateExpressions.add(expr);
            }
        } else {
            for (int i = 0; i < fetchColumns; i++) {
                SQLite3Expression expr;
                if (advancedFeaturesEnabled() && Randomly.getBooleanWithRatherLowProbability()) {
                    expr = generateAdvancedSelectExpression(sourceAst, gen);
                    hasWindowExpression = true;
                } else {
                    expr = generateComparisonStableExpression(gen);
                    nonAggregateExpressions.add(expr);
                }
                selected.add(expr);
            }
        }
        if (selected.isEmpty()) {
            selected.add(new SQLite3ColumnName(Randomly.fromList(sourceAst.columns), null));
        }
        select.setFromList(List.of(new SQLite3TableReference(sourceAst.table)));
        if (Randomly.getBoolean()) {
            select.setWhereClause(generateComparisonStableExpression(gen));
        }
        if (useAggregate) {
            groupByExpressions = chooseGroupByExpressions(nonAggregateExpressions);
            // SQLite has no native WITH ROLLUP or ROLLUP(...) GROUP BY syntax. Native-looking
            // rollup clauses are generated separately by generateRollupPair using UNION ALL.
            select.setGroupByClause(groupByExpressions);
            selected = new ArrayList<>(aggregateExpressions);
            selected.addAll(groupByExpressions);
        } else if (!hasWindowExpression && Randomly.getBooleanWithRatherLowProbability()) {
            groupByExpressions = chooseGroupByExpressions(nonAggregateExpressions);
            select.setGroupByClause(groupByExpressions);
            if (!groupByExpressions.isEmpty()) {
                selected = new ArrayList<>(groupByExpressions);
            }
            if (Randomly.getBoolean() && !groupByExpressions.isEmpty()) {
                select.setHavingClause(generateHavingExpression(gen, sourceAst.columns));
            }
        }
        if (selected.isEmpty()) {
            selected.add(new SQLite3ColumnName(sourceAst.rowIdColumn, null));
        }
        select.setFetchColumns(selected);
        if (Randomly.getBoolean()) {
            List<SQLite3Expression> orderBys = createStableOrderByExpressions(gen, sourceAst, useAggregate,
                    groupByExpressions, nonAggregateExpressions);
            if (!orderBys.isEmpty()) {
                select.setOrderByClauses(orderBys);
            }
        }
        boolean orderedComparisonEnabled = canCompareOrderedRows(useAggregate, select, sourceAst.rowIdColumn);
        boolean rowPreservingQuery = isRowPreservingQuery(useAggregate, select);
        if (rowPreservingQuery && Randomly.getBoolean()) {
            if (select.getOrderByClauses().isEmpty()) {
                select.setOrderByClauses(List.of(new SQLite3ColumnName(sourceAst.rowIdColumn, null)));
                orderedComparisonEnabled = canCompareOrderedRows(useAggregate, select, sourceAst.rowIdColumn);
            } else if (rowPreservingQuery) {
                List<SQLite3Expression> orderBys = new ArrayList<>(select.getOrderByClauses());
                orderBys.add(new SQLite3ColumnName(sourceAst.rowIdColumn, null));
                select.setOrderByClauses(orderBys);
                orderedComparisonEnabled = canCompareOrderedRows(useAggregate, select, sourceAst.rowIdColumn);
            } else {
                select.setOrderByClauses(List.of(new SQLite3ColumnName(sourceAst.rowIdColumn, null)));
                orderedComparisonEnabled = false;
            }
            select.setLimitClause(SQLite3Constant.createIntConstant(Randomly.getNotCachedInteger(0, 25)));
            if (Randomly.getBooleanWithRatherLowProbability()) {
                select.setOffsetClause(SQLite3Constant.createIntConstant(Randomly.getNotCachedInteger(0, 5)));
            }
        }
        String sourceRelation = sourceRelationForStrategy(vp, strategy);
        String singleQuery = renderSelectFromRelation(select, sourceRelation);
        if (advancedFeaturesEnabled() && !useAggregate && select.getOrderByClauses().isEmpty()
                && select.getLimitClause() == null && select.getOffsetClause() == null
                && Randomly.getBooleanWithSmallProbability()) {
            singleQuery = "(" + singleQuery + ") UNION ALL ("
                    + createUnionAllBranch(sourceAst, gen, selected.size(), sourceRelation) + ")";
            orderedComparisonEnabled = false;
        }
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        if (singleQuery.toUpperCase(Locale.ROOT).contains("GROUP_CONCAT")) {
            sourceRelation = orderedRelationForGroupConcat(sourceRelation, vp.sourceTable);
            relation = orderedRelationForGroupConcat(relation, vp.sourceTable);
            singleQuery = renderSelectFromRelation(select, sourceRelation);
        }
        String splitQuery = renderSelectFromRelation(select, relation);
        return new VpQueryPair(vp, strategy, singleQuery, splitQuery, extraSetup,
                "shape=" + vp.baseShape + ", split=" + vp.splitStrategy + ", reconstruction=" + strategy,
                orderedComparisonEnabled);
    }

    private VpQueryPair generateRollupPair(VpSchema vp, ReconstructionStrategy strategy, SourceAst sourceAst)
            throws Exception {
        recordAdvancedFeature(AdvancedFeature.ROLLUP);
        String groupKey = stableGroupKeySQL(comparisonStableColumnOrRowIdForSQL(sourceAst.columns));
        String aggregate = randomAdvancedAggregateSQL(sourceAst.columns);
        String sourceRelation = sourceRelationForStrategy(vp, strategy);
        String singleQuery = "SELECT " + groupKey + " AS vp_group_key, " + aggregate + " AS vp_agg FROM "
                + sourceRelation + " GROUP BY " + groupKey + " UNION ALL SELECT NULL AS vp_group_key, " + aggregate
                + " AS vp_agg FROM " + sourceRelation;
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        if (singleQuery.toUpperCase(Locale.ROOT).contains("GROUP_CONCAT")) {
            singleQuery = rollupQuery(orderedRelationForGroupConcat(sourceRelation, vp.sourceTable), groupKey,
                    aggregate);
            relation = orderedRelationForGroupConcat(relation, vp.sourceTable);
        }
        String splitQuery = rollupQuery(relation, groupKey, aggregate);
        return new VpQueryPair(
                vp, strategy, singleQuery, splitQuery, extraSetup, "shape=" + vp.baseShape + ", split="
                        + vp.splitStrategy + ", reconstruction=" + strategy + ", advanced=sqlite-rollup-union-all",
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
        String predicateColumn = qualifySelectedExpression(simpleColumnOrRowIdForSQL(sourceAst.columns), subAlias);
        int shape = state.getRandomly().getInteger(0, 3);
        String singleQuery = correlatedSubquerySQL(shape, sourceRelationForStrategy(vp, strategy), sourceAlias,
                subAlias, outerColumn, predicateColumn);
        List<String> extraSetup = new ArrayList<>();
        String relation = buildReconstructionRelation(vp, strategy, extraSetup);
        String splitQuery = correlatedSubquerySQL(shape, relation, sourceAlias, subAlias, outerColumn, predicateColumn);
        return new VpQueryPair(vp, strategy, singleQuery, splitQuery, extraSetup, "shape=" + vp.baseShape + ", split="
                + vp.splitStrategy + ", reconstruction=" + strategy + ", advanced=correlated-subquery", false);
    }

    private String qualifySelectedExpression(String expression, String alias) {
        return expression.replace(quote(ROW_ID), alias + "." + quote(ROW_ID));
    }

    private String correlatedSubquerySQL(int shape, String relation, String outerAlias, String subqueryAlias,
            String outerColumn, String predicateColumn) {
        String outerRelation = relationWithAlias(relation, outerAlias);
        String innerRelation = relationWithAlias(relation, subqueryAlias);
        switch (shape) {
        case 0:
            return "SELECT " + outerColumn + " FROM " + outerRelation + " WHERE EXISTS (SELECT 1 FROM " + innerRelation
                    + " WHERE " + subqueryAlias + "." + quote(ROW_ID) + " = " + outerAlias + "." + quote(ROW_ID)
                    + " AND " + predicateColumn + " IS NOT NULL)";
        case 1:
            return "SELECT " + outerColumn + " FROM " + outerRelation + " WHERE " + outerAlias + "." + quote(ROW_ID)
                    + " IN (SELECT " + subqueryAlias + "." + quote(ROW_ID) + " FROM " + innerRelation + " WHERE "
                    + predicateColumn + " IS NOT NULL)";
        case 2:
            return "SELECT " + outerColumn + ", (SELECT " + orderedAggregateSQL("MIN", predicateColumn) + " FROM "
                    + innerRelation + " WHERE " + subqueryAlias + "." + quote(ROW_ID) + " <= " + outerAlias + "."
                    + quote(ROW_ID) + ") FROM " + outerRelation;
        default:
            throw new AssertionError(shape);
        }
    }

    private String relationWithAlias(String relation, String alias) {
        int split = relation.lastIndexOf(' ');
        if (split < 0) {
            return relation + " " + alias;
        }
        String suffix = relation.substring(split + 1);
        if (!suffix.startsWith("\"")) {
            return relation + " " + alias;
        }
        return relation.substring(0, split) + " " + alias;
    }

    private boolean isGroupByOrdinalLiteral(SQLite3Expression expression) {
        return expression instanceof SQLite3Constant.SQLite3IntConstant;
    }

    private SQLite3Expression generateComparisonStableExpression(SQLite3ExpressionGenerator gen) {
        for (int i = 0; i < 20; i++) {
            SQLite3Expression expression = gen.generateExpression();
            if (isComparisonStableExpression(expression)) {
                if (advancedFeaturesEnabled() && Randomly.getBooleanWithRatherLowProbability()) {
                    recordAdvancedFeature(AdvancedFeature.CASE_IMPLICIT_TYPE_CONVERSION);
                    String condition = SQLite3Visitor.asString(expression);
                    return new SQLite3Text(
                            "CASE WHEN (" + condition + ") IS NULL THEN CAST(0 AS NUMERIC) ELSE '0' END = 0", null);
                }
                return expression;
            }
        }
        return SQLite3Constant.createIntConstant(Randomly.getNotCachedInteger(0, 100));
    }

    private List<SQLite3Expression> createStableOrderByExpressions(SQLite3ExpressionGenerator gen, SourceAst sourceAst,
            boolean useAggregate, List<SQLite3Expression> groupByExpressions,
            List<SQLite3Expression> nonAggregateExpressions) {
        if (useAggregate || !groupByExpressions.isEmpty()) {
            return groupByExpressions.stream().filter(this::isComparisonStableExpression).collect(Collectors.toList());
        }
        List<SQLite3Expression> orderBys = nonAggregateExpressions.isEmpty()
                ? List.of(new SQLite3ColumnName(sourceAst.rowIdColumn, null)) : gen.generateOrderBys().stream()
                        .filter(this::isComparisonStableExpression).collect(Collectors.toList());
        if (orderBys.isEmpty()) {
            orderBys = List.of(new SQLite3ColumnName(sourceAst.rowIdColumn, null));
        }
        return orderBys;
    }

    private boolean isComparisonStableExpression(SQLite3Expression expression) {
        if (expression == null) {
            return false;
        }
        String sql = SQLite3Visitor.asString(expression);
        return !containsContextSensitiveFunction(sql);
    }

    private boolean containsContextSensitiveFunction(String sql) {
        return SQLITE_CONTEXT_SENSITIVE_FUNCTION.matcher(sql).find();
    }

    private List<SQLite3Expression> chooseGroupByExpressions(List<SQLite3Expression> nonAggregateExpressions) {
        // Keep GROUP BY keys tied to source columns so the split and reconstructed relations use the same key.
        List<SQLite3Expression> groupable = nonAggregateExpressions.stream().filter(e -> !isGroupByOrdinalLiteral(e))
                .filter(e -> e instanceof SQLite3ColumnName).filter(this::isComparisonStableExpression)
                .map(this::stableGroupKeyExpression).collect(Collectors.toList());
        if (!advancedFeaturesEnabled() || groupable.size() <= 1) {
            return groupable;
        }
        Collections.shuffle(groupable);
        return groupable.subList(0, state.getRandomly().getInteger(1, Math.min(3, groupable.size()) + 1));
    }

    private SQLite3Expression stableGroupKeyExpression(SQLite3Expression expression) {
        return new SQLite3Text(stableGroupKeySQL(SQLite3Visitor.asString(expression)), null);
    }

    private String stableGroupKeySQL(String expression) {
        return "(CASE WHEN typeof((" + expression + ")) IN ('integer', 'real') THEN CAST((" + expression
                + ") AS REAL) ELSE (" + expression + ") END)";
    }

    private SQLite3Expression generateHavingExpression(SQLite3ExpressionGenerator gen, List<SQLite3Column> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            String groupValue = comparisonStableColumnOrRowIdForSQL(columns);
            return new SQLite3Text(
                    Randomly.fromOptions("COUNT(*) > 0",
                            orderedAggregateSQL("SUM", numericExpressionForSQL(columns)) + " IS NOT NULL",
                            orderedAggregateSQL("MIN", groupValue) + " IS " + orderedAggregateSQL("MAX", groupValue)),
                    null);
        }
        return new SQLite3Text(Randomly.fromOptions("COUNT(*) >= 0",
                "COUNT(" + comparisonStableColumnOrRowIdForSQL(columns) + ") >= 0",
                orderedAggregateSQL("MIN", comparisonStableColumnOrRowIdForSQL(columns)) + " IS "
                        + orderedAggregateSQL("MAX", comparisonStableColumnOrRowIdForSQL(columns))),
                null);
    }

    private SQLite3Expression generateAdvancedSelectExpression(SourceAst sourceAst, SQLite3ExpressionGenerator gen) {
        String order = simpleOrderExpressionForSQL(sourceAst.columns) + ", " + quote(ROW_ID);
        String value = simpleColumnOrRowIdForSQL(sourceAst.columns);
        String numeric = numericExpressionForSQL(sourceAst.columns);
        return new SQLite3Text(Randomly.fromOptions("ROW_NUMBER() OVER (ORDER BY " + order + ")",
                "RANK() OVER (ORDER BY " + order + ")", "LAG(" + value + ") OVER (ORDER BY " + quote(ROW_ID) + ")",
                "SUM(" + numeric + ") OVER (ORDER BY " + quote(ROW_ID) + ")"), null);
    }

    private String createUnionAllBranch(SourceAst sourceAst, SQLite3ExpressionGenerator gen, int fetchColumns,
            String relation) {
        SQLite3Select right = new SQLite3Select();
        List<SQLite3Expression> selected = new ArrayList<>();
        for (int i = 0; i < fetchColumns; i++) {
            selected.add(Randomly.getBooleanWithRatherLowProbability()
                    ? generateAdvancedSelectExpression(sourceAst, gen) : generateComparisonStableExpression(gen));
        }
        right.setFetchColumns(selected);
        right.setFromList(List.of(new SQLite3Text(relation, null)));
        if (Randomly.getBoolean()) {
            right.setWhereClause(generateComparisonStableExpression(gen));
        }
        return SQLite3Visitor.asString(right);
    }

    private boolean isRowPreservingQuery(boolean useAggregate, SQLite3Select select) {
        return !useAggregate && select.getGroupByClause().isEmpty();
    }

    private boolean hasVpRowIdTieBreaker(SQLite3Select select, SQLite3Column rowIdColumn) {
        List<SQLite3Expression> orderBys = select.getOrderByClauses();
        if (orderBys.isEmpty()) {
            return false;
        }
        SQLite3Expression last = orderBys.get(orderBys.size() - 1);
        return last instanceof SQLite3ColumnName
                && ((SQLite3ColumnName) last).getColumn().getName().equals(rowIdColumn.getName());
    }

    private boolean canCompareOrderedRows(boolean useAggregate, SQLite3Select select, SQLite3Column rowIdColumn) {
        if (!isRowPreservingQuery(useAggregate, select) || !hasVpRowIdTieBreaker(select, rowIdColumn)) {
            return false;
        }
        if (!deepExplorationEnabled()) {
            return true;
        }
        List<SQLite3Expression> orderBys = select.getOrderByClauses();
        return orderBys.size() == 1 && orderBys.get(0) instanceof SQLite3ColumnName
                && ((SQLite3ColumnName) orderBys.get(0)).getColumn().getName().equals(rowIdColumn.getName());
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

    private static void recordPairStage(VpQueryPair pair, PairStage stage) {
        synchronized (STATS_LOCK) {
            EnumMap<PairStage, Integer> stages = PAIR_STAGE_COUNTS.computeIfAbsent(pair.reconstructionStrategy,
                    ignored -> new EnumMap<>(PairStage.class));
            increment(stages, stage);
        }
    }

    private static void recordSkippedStage(ReconstructionStrategy strategy, String stage) {
        synchronized (STATS_LOCK) {
            String key = strategy.name() + ":" + stage;
            SKIPPED_STAGE_COUNTS.put(key, SKIPPED_STAGE_COUNTS.getOrDefault(key, 0) + 1);
        }
    }

    private static void recordSkippedStage(SplitStrategy strategy, String stage) {
        synchronized (STATS_LOCK) {
            String key = "SPLIT_" + strategy.name() + ":" + stage;
            SKIPPED_STAGE_COUNTS.put(key, SKIPPED_STAGE_COUNTS.getOrDefault(key, 0) + 1);
        }
    }

    @SuppressWarnings("unused")
    private static void recordSkippedError(String stage, Throwable error) {
        recordSkippedError(stage, null, error);
    }

    private static void recordSkippedError(String stage, String sql, Throwable error) {
        String message = error == null ? "<unspecified>" : error.getMessage();
        if (message == null || message.isEmpty()) {
            message = error == null ? "<unspecified>" : error.getClass().getSimpleName();
        }
        message = message.replaceAll("\\s+", " ");
        if (message.length() > 240) {
            message = message.substring(0, 240);
        }
        synchronized (STATS_LOCK) {
            String key = stage + ":" + message;
            SKIPPED_ERROR_COUNTS.put(key, SKIPPED_ERROR_COUNTS.getOrDefault(key, 0) + 1);
            if (sql != null && SKIPPED_ERROR_SAMPLES.size() < 64) {
                SKIPPED_ERROR_SAMPLES.putIfAbsent(key, sql);
            }
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

    private static void recordSchemaCopySkip(String reason) {
        synchronized (STATS_LOCK) {
            SCHEMA_COPY_SKIP_COUNTS.put(reason, SCHEMA_COPY_SKIP_COUNTS.getOrDefault(reason, 0) + 1);
        }
    }

    private static void recordSchemaCopyOutcome(String outcome) {
        synchronized (STATS_LOCK) {
            SCHEMA_COPY_COUNTS.put(outcome, SCHEMA_COPY_COUNTS.getOrDefault(outcome, 0) + 1);
        }
    }

    private static void recordHelperIndexOutcome(String outcome) {
        synchronized (STATS_LOCK) {
            HELPER_INDEX_COUNTS.put(outcome, HELPER_INDEX_COUNTS.getOrDefault(outcome, 0) + 1);
        }
    }

    private static void recordSourceRowCount(long rowCount) {
        synchronized (STATS_LOCK) {
            String bucket;
            if (rowCount == 0) {
                bucket = "0";
            } else if (rowCount == 1) {
                bucket = "1";
            } else if (rowCount <= 10) {
                bucket = "2-10";
            } else if (rowCount <= 100) {
                bucket = "11-100";
            } else if (rowCount <= 255) {
                bucket = "101-255";
            } else if (rowCount == 256) {
                bucket = "256";
            } else if (rowCount <= 1000) {
                bucket = "257-1000";
            } else {
                bucket = "1001+";
            }
            SOURCE_ROW_COUNT_BUCKETS.put(bucket, SOURCE_ROW_COUNT_BUCKETS.getOrDefault(bucket, 0) + 1);
        }
    }

    private static void recordVpRunStatistics(int checkedPairs, int attempts, boolean completed) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            observedOracleRuns++;
            observedQueryPairs += checkedPairs;
            observedAttempts += attempts;
            if (completed) {
                completedOracleRuns++;
                completedQueryPairs += checkedPairs;
                completedAttempts += attempts;
            } else {
                incompleteOracleRuns++;
            }
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

    private static void incrementSelectedBaseEngine(String engine) {
        registerVpStatsShutdownHook();
        synchronized (STATS_LOCK) {
            SELECTED_BASE_ENGINE_COUNTS.put(engine, SELECTED_BASE_ENGINE_COUNTS.getOrDefault(engine, 0) + 1);
        }
    }

    private static String formatVpStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("-- [vp-stats] event=process-final dbms=sqlite3");
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] oracle-runs completed=").append(completedOracleRuns);
        sb.append(" checkedPairs=").append(completedQueryPairs);
        sb.append(" attempts=").append(completedAttempts);
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] oracle-runs observed=").append(observedOracleRuns);
        sb.append(" checkedPairs=").append(observedQueryPairs);
        sb.append(" attempts=").append(observedAttempts);
        sb.append(" incomplete=").append(incompleteOracleRuns);
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
        sb.append("-- [vp-stats] selected-base-engine-counts ").append(formatStringCounts(SELECTED_BASE_ENGINE_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] source-row-count-buckets ").append(formatSourceRowCountBuckets());
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] reconstruction-counts ")
                .append(formatEnumCounts(ReconstructionStrategy.values(), RECONSTRUCTION_STRATEGY_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] reconstruction-stage-counts ").append(formatPairStageCounts());
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] skipped-stage-counts ").append(formatStringCounts(SKIPPED_STAGE_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] skipped-error-counts ").append(formatStringCounts(SKIPPED_ERROR_COUNTS));
        sb.append(System.lineSeparator());
        for (Map.Entry<String, String> sample : SKIPPED_ERROR_SAMPLES.entrySet()) {
            sb.append("-- [vp-stats] skipped-error-sample error=").append(sample.getKey()).append(" sql=")
                    .append(sample.getValue()).append(System.lineSeparator());
        }
        sb.append("-- [vp-stats] known-suppressed-counts ")
                .append(formatEnumCounts(KnownVpBug.values(), KNOWN_BUG_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] advanced-feature-counts ")
                .append(formatEnumCounts(AdvancedFeature.values(), ADVANCED_FEATURE_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] helper-index-counts ").append(formatOutcomeCounts(HELPER_INDEX_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] schema-copy-counts ").append(formatOutcomeCounts(SCHEMA_COPY_COUNTS));
        sb.append(System.lineSeparator());
        sb.append("-- [vp-stats] schema-copy-skip-counts ").append(formatStringCounts(SCHEMA_COPY_SKIP_COUNTS));
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

    private static String formatStringCounts(Map<String, Integer> counts) {
        return counts.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(", "));
    }

    private static String formatPairStageCounts() {
        return java.util.Arrays.stream(ReconstructionStrategy.values()).map(strategy -> {
            Map<PairStage, Integer> stages = PAIR_STAGE_COUNTS.getOrDefault(strategy, new EnumMap<>(PairStage.class));
            return strategy.name() + "{" + formatEnumCounts(PairStage.values(), stages) + "}";
        }).collect(Collectors.joining(", "));
    }

    private static String formatOutcomeCounts(Map<String, Integer> counts) {
        return List.of("attempted", "succeeded", "skipped").stream()
                .map(outcome -> outcome + "=" + counts.getOrDefault(outcome, 0)).collect(Collectors.joining(", "));
    }

    private static String formatSourceRowCountBuckets() {
        int total = SOURCE_ROW_COUNT_BUCKETS.values().stream().mapToInt(Integer::intValue).sum();
        if (total == 0) {
            return "";
        }
        List<String> buckets = List.of("0", "1", "2-10", "11-100", "101-255", "256", "257-1000", "1001+");
        return buckets.stream().filter(SOURCE_ROW_COUNT_BUCKETS::containsKey).map(bucket -> {
            int count = SOURCE_ROW_COUNT_BUCKETS.get(bucket);
            return bucket + "=" + count + "(" + String.format(Locale.ROOT, "%.2f%%", count * 100.0 / total) + ")";
        }).collect(Collectors.joining(", "));
    }

    public static void logSchemaShapeStats(SQLite3GlobalState state, String phase) throws Exception {
        registerVpStatsShutdownHook();
        int emptySingle = 0;
        int emptyMulti = 0;
        int nonEmptySingle = 0;
        int nonEmptyMulti = 0;
        state.updateSchema();
        for (SQLite3Table table : state.getSchema().getDatabaseTables()) {
            if (table.isView() || table.getName().startsWith("vp_") || table.getColumns().isEmpty()) {
                continue;
            }
            boolean empty;
            try {
                empty = table.getNrRows(state) == 0;
            } catch (AssertionError | RuntimeException e) {
                if (isSkippableSQLiteError(e)) {
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
    }

    private static void registerVpStatsShutdownHook() {
        if (SHUTDOWN_HOOK_REGISTERED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(SQLite3VerticalPartitioningOracle::writeFinalVpStats));
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
            Files.writeString(target, sanitizeForUtf8(log), StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /** Replace unpaired UTF-16 surrogates emitted by random SQLite text values. */
    private static String sanitizeForUtf8(String text) {
        StringBuilder sanitized = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (i + 1 < text.length() && Character.isLowSurrogate(text.charAt(i + 1))) {
                    sanitized.append(c).append(text.charAt(++i));
                } else {
                    sanitized.append('?');
                }
            } else if (Character.isLowSurrogate(c)) {
                sanitized.append('?');
            } else {
                sanitized.append(c);
            }
        }
        return sanitized.toString();
    }

    private SQLite3Expression generateVpAggregate(SQLite3ExpressionGenerator gen, List<SQLite3Column> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
            return new SQLite3Text(randomAdvancedAggregateSQL(columns), null);
        }
        SQLite3AggregateFunction func = Randomly.fromOptions(SQLite3AggregateFunction.values());
        if (func == SQLite3AggregateFunction.COUNT_ALL) {
            List<SQLite3Expression> args = new ArrayList<>();
            args.add(new SQLite3ColumnName(SQLite3Column.createDummy("*"), null));
            return new SQLite3Aggregate(args, func);
        }
        if (func == SQLite3AggregateFunction.SUM) {
            return new SQLite3Text(
                    orderedAggregateSQL("SUM", SQLite3Visitor.asString(generateSumInputExpression(gen, columns))),
                    null);
        }
        if (func == SQLite3AggregateFunction.AVG || func == SQLite3AggregateFunction.TOTAL) {
            String aggregateInput = SQLite3Visitor.asString(generateSumInputExpression(gen, columns));
            return new SQLite3Text(orderedAggregateSQL(func.name(), aggregateInput), null);
        }
        if (func == SQLite3AggregateFunction.MIN || func == SQLite3AggregateFunction.MAX) {
            return new SQLite3Text(
                    orderedAggregateSQL(func.name(),
                            minMaxAggregateInputSQL(SQLite3Visitor.asString(generateComparisonStableExpression(gen)))),
                    null);
        }
        if (func == SQLite3AggregateFunction.GROUP_CONCAT) {
            return new SQLite3Text(orderedAggregateSQL("GROUP_CONCAT", groupConcatInputForSQL(columns)), null);
        }
        List<SQLite3Expression> args = new ArrayList<>();
        args.add(generateComparisonStableExpression(gen));
        return new SQLite3Aggregate(args, func);
    }

    private SQLite3Expression generateSumInputExpression(SQLite3ExpressionGenerator gen, List<SQLite3Column> columns) {
        if (Randomly.getBoolean()) {
            SQLite3Column column = Randomly.fromList(columns);
            SQLite3ColumnName ref = new SQLite3ColumnName(column, null);
            if (column.getType() == SQLite3DataType.TEXT || column.getType() == SQLite3DataType.BINARY) {
                if (advancedFeaturesEnabled()) {
                    if (state.getRandomly().getInteger(0, 100) < 70) {
                        return new SQLite3Text(
                                "CAST(" + SQLite3Visitor.asString(ref) + " AS " + randomSumCastType() + ")", null);
                    }
                    return generateComparisonStableExpression(gen);
                }
                return new SQLite3Text("CAST(" + SQLite3Visitor.asString(ref) + " AS REAL)", null);
            }
            return ref;
        }
        return generateComparisonStableExpression(gen);
    }

    private String randomSumCastType() {
        return Randomly.fromOptions("INTEGER", "REAL", "NUMERIC", "TEXT");
    }

    private String randomAdvancedAggregateSQL(List<SQLite3Column> columns) {
        if (advancedFeaturesEnabled() && Randomly.getBooleanWithSmallProbability()) {
            recordAdvancedFeature(AdvancedFeature.GROUP_CONCAT);
            return orderedGroupConcatSQL(groupConcatInputForSQL(columns), "','");
        }
        String numeric = numericExpressionForSQL(columns);
        return Randomly.fromOptions(orderedAggregateSQL("AVG", numeric), orderedAggregateSQL("SUM", numeric),
                orderedAggregateSQL("TOTAL", numeric), orderedAggregateSQL("MIN", minMaxAggregateInputSQL(numeric)),
                orderedAggregateSQL("MAX", minMaxAggregateInputSQL(numeric)), "COUNT(" + numeric + ")");
    }

    private String orderedAggregateSQL(String function, String expression) {
        if ("MIN".equals(function) || "MAX".equals(function)) {
            // MIN/MAX return one representative input value. After a VP rewrite, SQLite can visit numerically equal
            // INTEGER/REAL inputs through a different plan and preserve a different storage class for that
            // representative. Keep ordinary SELECT result comparison type-strict, but make generated MIN/MAX inputs
            // type-stable so these plan-dependent ties do not become oracle false positives.
            expression = "(" + expression + ") COLLATE BINARY";
        }
        return function + "(" + expression + " ORDER BY " + quote(ROW_ID) + ")";
    }

    private String minMaxAggregateInputSQL(String expression) {
        return "CAST((" + expression + ") AS REAL)";
    }

    private String orderedGroupConcatSQL(String expression, String separator) {
        return "GROUP_CONCAT(" + expression + ", " + separator + " ORDER BY " + quote(ROW_ID) + ")";
    }

    private String groupConcatInputForSQL(List<SQLite3Column> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        SQLite3Column column = Randomly.fromList(columns);
        if (isCharacterLike(column.getType()) || Randomly.getBoolean()) {
            return "CAST(" + quote(column.getName()) + " AS TEXT)";
        }
        return quote(column.getName());
    }

    private String numericExpressionForSQL(List<SQLite3Column> columns) {
        List<SQLite3Column> numericColumns = columns.stream().filter(c -> isNumericType(c.getType()))
                .collect(Collectors.toList());
        if (!numericColumns.isEmpty() && Randomly.getBoolean()) {
            return quote(Randomly.fromList(numericColumns).getName());
        }
        return quote(ROW_ID);
    }

    private String simpleColumnOrRowIdForSQL(List<SQLite3Column> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        return quote(Randomly.fromList(columns).getName());
    }

    private String comparisonStableColumnOrRowIdForSQL(List<SQLite3Column> columns) {
        if (columns.isEmpty() || Randomly.getBoolean()) {
            return quote(ROW_ID);
        }
        return quote(Randomly.fromList(columns).getName());
    }

    private SQLite3Expression comparisonStableColumnOrRowIdExpression(SourceAst sourceAst) {
        if (sourceAst.columns.isEmpty() || Randomly.getBoolean()) {
            return new SQLite3ColumnName(sourceAst.rowIdColumn, null);
        }
        return new SQLite3ColumnName(Randomly.fromList(sourceAst.columns), null);
    }

    private String simpleOrderExpressionForSQL(List<SQLite3Column> columns) {
        if (columns.isEmpty()) {
            return quote(ROW_ID);
        }
        return simpleColumnOrRowIdForSQL(columns);
    }

    private static final class SourceAst {
        private final SQLite3Table table;
        private final SQLite3Column rowIdColumn;
        private final List<SQLite3Column> columns;

        private SourceAst(SQLite3Table table, SQLite3Column rowIdColumn, List<SQLite3Column> columns) {
            this.table = table;
            this.rowIdColumn = rowIdColumn;
            this.columns = columns;
        }
    }

    private SourceAst createSourceAst(VpSchema vp) {
        List<SQLite3Column> astColumns = new ArrayList<>();
        SQLite3Column rowId = new SQLite3Column(ROW_ID, SQLite3DataType.INT, true, false, null);
        astColumns.add(rowId);
        for (VpColumn column : vp.columns) {
            astColumns.add(new SQLite3Column(column.name, column.type, false, false, column.collate));
        }
        SQLite3Table table = new SQLite3Table(vp.sourceTable, astColumns, SQLite3Table.TableKind.MAIN, false, false,
                false, false);
        for (SQLite3Column column : astColumns) {
            column.setTable(table);
        }
        return new SourceAst(table, rowId, astColumns);
    }

    private String renderSelectFromRelation(SQLite3Select select, String relation) {
        SQLite3Select copy = new SQLite3Select(select);
        copy.setFromList(List.of(new SQLite3Text(relation, null)));
        return SQLite3Visitor.asString(copy);
    }

    private String rollupQuery(String relation, String groupKey, String aggregate) {
        return "SELECT " + groupKey + " AS vp_group_key, " + aggregate + " AS vp_agg FROM " + relation + " GROUP BY "
                + groupKey + " UNION ALL SELECT NULL AS vp_group_key, " + aggregate + " AS vp_agg FROM " + relation;
    }

    private String orderedRelationForGroupConcat(String relation, String alias) {
        return "(SELECT * FROM " + relation + " ORDER BY " + quote(ROW_ID) + ") " + quote(alias);
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
            return "(" + "SELECT " + joinProjection(vp, "l", "r", false) + " FROM " + quote(vp.leftTable)
                    + " l LEFT JOIN " + quote(vp.rightTable) + " r ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID)
                    + " UNION ALL SELECT " + joinProjection(vp, "l", "r", true) + " FROM " + quote(vp.rightTable)
                    + " r LEFT JOIN " + quote(vp.leftTable) + " l ON l." + quote(ROW_ID) + " = r." + quote(ROW_ID)
                    + " WHERE NOT EXISTS (SELECT 1 FROM " + quote(vp.leftTable) + " lx WHERE lx." + quote(ROW_ID)
                    + " = r." + quote(ROW_ID) + ")) " + quote(vp.sourceTable);
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
                            + " rx WHERE rx." + quote(ROW_ID) + " > r." + quote(ROW_ID) + ")");
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
            if (deepExplorationEnabled() && hasDeepCollation(vp)) {
                throw new IgnoreMeException();
            }
            extraSetup.add("DROP TABLE IF EXISTS " + quote(vp.tempTable));
            extraSetup.add(createTempReconstructionTableSQL(vp));
            extraSetup.add("DELETE FROM " + quote(vp.tempTable));
            extraSetup.add("INSERT INTO " + quote(vp.tempTable) + " (" + tempReconstructionColumnList(vp) + ") SELECT "
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
        default:
            throw new AssertionError(strategy);
        }
    }

    private String reconstructedSubquery(VpSchema vp, String projection, String fromAndWhere) {
        return "(SELECT " + projection + fromAndWhere + ") " + quote(vp.sourceTable);
    }

    private String sourceRelationForStrategy(VpSchema vp, ReconstructionStrategy strategy) {
        if (strategy == ReconstructionStrategy.NOT_EXISTS_ANTI) {
            return antiSourceRelation(vp);
        }
        if (canCompareBaseTableDirectly(vp)) {
            return baseSnapshotRelation(vp, quote(vp.sourceTable));
        }
        return quote(vp.sourceTable);
    }

    private String antiSourceRelation(VpSchema vp) {
        String rowId = quote(ROW_ID);
        if (canCompareBaseTableDirectly(vp)) {
            return "(SELECT s.* FROM " + baseSnapshotRelation(vp, "s") + " WHERE NOT EXISTS (SELECT 1 FROM "
                    + baseSnapshotRelation(vp, "rx") + " WHERE rx." + rowId + " > s." + rowId + ")) "
                    + quote(vp.sourceTable);
        }
        return "(SELECT s.* FROM " + quote(vp.sourceTable) + " s WHERE NOT EXISTS (SELECT 1 FROM "
                + quote(vp.sourceTable) + " rx WHERE rx." + rowId + " > s." + rowId + ")) " + quote(vp.sourceTable);
    }

    private boolean canCompareBaseTableDirectly(VpSchema vp) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningCompareBaseTable) {
            return false;
        }
        // Generated expressions have no declared result affinity when selected
        // from the base table. Helper columns do have one, so INTEGER/REAL
        // storage-class changes would be reported as VP mismatches even when
        // the generated value is numerically identical.
        if (vp.hasGeneratedColumns) {
            return false;
        }
        // A synthetic ROW_NUMBER() key is recomputed by the base query and by source
        // materialization. On WITHOUT ROWID tables, mixed SQLite values and collations
        // can bind ties differently, creating an oracle mismatch unrelated to VP.
        return !vp.rowIdExpression.trim().toUpperCase(Locale.ROOT).startsWith("ROW_NUMBER(");
    }

    /** Keep the base side logically identical to source while retaining SQLite's base-table path. */
    private String baseSnapshotRelation(VpSchema vp, String alias) {
        List<String> projections = new ArrayList<>();
        projections.add(vp.rowIdExpression + " AS " + quote(ROW_ID));
        projections.addAll(vp.columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        String order = vp.stableOrderStrategy == null || vp.stableOrderStrategy.isEmpty() ? vp.rowIdExpression
                : vp.stableOrderStrategy;
        StringBuilder relation = new StringBuilder("(SELECT ").append(String.join(", ", projections)).append(" FROM ")
                .append(quote(vp.baseTable)).append(" ORDER BY ").append(order);
        if (state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows > 0) {
            relation.append(" LIMIT ").append(state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows);
        }
        return relation.append(") ").append(alias).toString();
    }

    private String createTempReconstructionTableSQL(VpSchema vp) {
        List<String> defs = new ArrayList<>();
        defs.add(quote(ROW_ID) + (isWithoutRowIdEngine(vp.sourceEngine) ? " INTEGER NOT NULL" : " INTEGER NULL"));
        for (VpColumn column : vp.columns) {
            defs.add(columnDef(vp, column));
        }
        return createTemporaryTableSQL(vp.tempTable, defs, vp.sourceEngine);
    }

    private String createTemporaryTableSQL(String tableName, List<String> columnDefs, String engine) {
        String create = createTableSQL(tableName, columnDefs, engine);
        return create.replaceFirst("^CREATE TABLE ", "CREATE TEMPORARY TABLE ");
    }

    private boolean hasDeepCollation(VpSchema vp) {
        return vp.columnCollationClauses.values().stream().anyMatch(clause -> clause != null && !clause.isEmpty());
    }

    private String tempReconstructionColumnList(VpSchema vp) {
        List<String> columns = new ArrayList<>();
        columns.add(quote(ROW_ID));
        columns.addAll(vp.columns.stream().map(c -> quote(c.name)).collect(Collectors.toList()));
        return String.join(", ", columns);
    }

    private String joinProjection(VpSchema vp, String leftAlias, String rightAlias, boolean rightBiasedRowId) {
        List<String> parts = new ArrayList<>();
        parts.add((rightBiasedRowId ? rightAlias : leftAlias) + "." + quote(ROW_ID) + " AS " + quote(ROW_ID));
        for (VpColumn column : vp.columns) {
            String alias = columnAlias(vp, column, leftAlias, rightAlias);
            parts.add(projectColumn(vp, column, alias + "." + quote(column.name)));
        }
        return String.join(", ", parts);
    }

    private String usingProjection(VpSchema vp) {
        List<String> parts = new ArrayList<>();
        parts.add(quote(ROW_ID));
        for (VpColumn column : vp.columns) {
            String table = containsColumn(vp.leftColumns, column.name) ? vp.leftTable : vp.rightTable;
            parts.add(projectColumn(vp, column, quote(table) + "." + quote(column.name)));
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
                parts.add(projectColumn(vp, column, "l." + quote(column.name)));
            } else {
                parts.add(projectColumn(vp, column, "(SELECT r." + quote(column.name) + " FROM " + quote(vp.rightTable)
                        + " r WHERE r." + quote(ROW_ID) + " = l." + quote(ROW_ID) + ")"));
            }
        }
        return String.join(", ", parts);
    }

    private String projectColumn(VpSchema vp, VpColumn column, String expression) {
        return expression + vp.columnCollationClauses.getOrDefault(column.name, "") + " AS " + quote(column.name);
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
                        + quote(ROW_ID) + " AND rg.vp_count > 0" + " JOIN " + quote(vp.rightTable) + " r ON r."
                        + quote(ROW_ID) + " = rg." + quote(ROW_ID));
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
        // Concatenating a dynamically typed SQLite key directly is not injective:
        // for example, TEXT '' and BLOB x'' both become the same empty string.
        // Keep the storage class and byte representation in the transport key.
        String encodedRowId = "typeof(" + quote(ROW_ID) + ") || ':' || hex(CAST(" + quote(ROW_ID) + " AS BLOB))";
        return reconstructedSubquery(vp, joinProjection(vp, "l", "r", false),
                " FROM " + quote(vp.leftTable) + " l JOIN (SELECT " + quote(ROW_ID) + ", " + encodedRowId
                        + " AS payload FROM " + quote(vp.rightTable) + ") sp ON "
                        + encodedRowId.replace(quote(ROW_ID), "l." + quote(ROW_ID)) + " = sp.payload JOIN "
                        + quote(vp.rightTable) + " r ON r." + quote(ROW_ID) + " = sp." + quote(ROW_ID));
    }

    private void comparePair(VpQueryPair pair) throws Exception {
        for (String setup : pair.extraSetupStatements) {
            if (!executePairExtraSetup(setup)) {
                recordSkippedStage(pair.reconstructionStrategy, "EXTRA_SETUP");
                throw new IgnoreMeException();
            }
            if (!pair.schema.setupStatements.contains(setup)) {
                pair.schema.setupStatements.add(setup);
            }
        }
        recordPairStage(pair, PairStage.EXTRA_SETUP_OK);
        QueryResultMultiset singleRows;
        try {
            singleRows = getResultMultiset(pair.singleQuery);
        } catch (IgnoreMeException e) {
            recordSkippedStage(pair.reconstructionStrategy, "SINGLE");
            throw e;
        }
        recordPairStage(pair, PairStage.SINGLE_OK);
        QueryResultMultiset splitRows;
        try {
            splitRows = getResultMultiset(pair.splitQuery);
        } catch (IgnoreMeException e) {
            recordSkippedStage(pair.reconstructionStrategy, "SPLIT");
            throw e;
        }
        recordPairStage(pair, PairStage.SPLIT_OK);
        if (!resultMultisetsMatch(singleRows, splitRows)) {
            recordPairStage(pair, PairStage.MISMATCH);
            KnownVpBug knownBug = classifyKnownBug(pair, singleRows, splitRows);
            if (suppressKnownBug(pair, knownBug, singleRows, splitRows, false)) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(formatBugReport(pair, singleRows, splitRows));
        }
        if (pair.orderedComparisonEnabled && !orderedRowsMatch(singleRows, splitRows)) {
            recordPairStage(pair, PairStage.MISMATCH);
            KnownVpBug knownBug = classifyKnownOrderedBug(pair, singleRows, splitRows);
            if (suppressKnownBug(pair, knownBug, singleRows, splitRows, true)) {
                throw new IgnoreMeException();
            }
            throw new AssertionError(formatOrderedBugReport(pair, singleRows, splitRows));
        }
        recordPairStage(pair, PairStage.COMPARED);
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
        return null;
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
        File dir = new File(Main.LOG_DIRECTORY, "sqlite3");
        try {
            Files.createDirectories(dir.toPath());
            Path target;
            do {
                target = new File(dir, "vp-known" + KNOWN_BUG_LOG_ID.incrementAndGet() + ".log").toPath();
            } while (Files.exists(target));
            Files.writeString(target, sanitizeForUtf8(sb.toString()), StandardCharsets.UTF_8,
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
            // SQLite considers INTEGER and REAL values equal when their exact numeric
            // values are equal. An aggregate such as MIN can return either storage
            // class for an integer/real tie depending on the physical scan order. The
            // query result does not expose that representation unless it explicitly
            // asks for typeof(), so comparing JDBC object classes here is too strict.
            if (numericValuesExactlyEqual(l, r)) {
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
            } else if (row.startsWith("V:", idx)) {
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

    private boolean numericValuesExactlyEqual(String left, String right) {
        String leftPayload = numericPayload(left);
        String rightPayload = numericPayload(right);
        if (leftPayload == null || rightPayload == null) {
            return false;
        }
        try {
            return new BigDecimal(leftPayload).compareTo(new BigDecimal(rightPayload)) == 0;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    private boolean numericValuesApproximatelyEqual(String left, String right, ApproximateKind approximateKind) {
        // Do not let a numeric-looking TEXT/BLOB or an INTEGER cross the runtime-type boundary.
        String leftPayload = numericPayload(left);
        String rightPayload = numericPayload(right);
        if (leftPayload == null || rightPayload == null || !left.startsWith("R:") || !right.startsWith("R:")) {
            return false;
        }
        try {
            double l = Double.parseDouble(leftPayload);
            double r = Double.parseDouble(rightPayload);
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
        Map<String, Integer> rows = new LinkedHashMap<>();
        Map<String, List<ApproximateKind>> approximateColumns = new LinkedHashMap<>();
        List<String> orderedRows = new ArrayList<>();
        List<List<ApproximateKind>> orderedApproximateColumns = new ArrayList<>();
        boolean completed = false;
        Main.nrQueries.addAndGet(1);
        try (Statement statement = state.getConnection().createStatement();
                SQLancerResultSet rs = new SQLancerResultSet(statement.executeQuery(query))) {
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
        } catch (IgnoreMeException e) {
            throw e;
        } catch (AssertionError e) {
            if (isSkippableQueryError(e)) {
                recordSkippedError("result", query, e);
                throw new IgnoreMeException();
            }
            throw e;
        } catch (SQLException e) {
            if (isExpectedPairResultError(e) || isSkippableResultReadError(e)) {
                recordSkippedError("result", query, e);
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

    private boolean isExpectedPairResultError(SQLException error) {
        return pairErrors.errorIsExpected(getErrorText(error));
    }

    private List<ApproximateKind> getApproximateColumns(SQLancerResultSet rs, int columnCount) throws SQLException {
        List<ApproximateKind> approximate = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            approximate.add(getApproximateObjectType(rs.getObject(i)));
        }
        return approximate;
    }

    private ApproximateKind getApproximateObjectType(Object value) {
        if (value instanceof Float) {
            return ApproximateKind.FLOAT;
        }
        if (value instanceof Double) {
            return ApproximateKind.DOUBLE;
        }
        return ApproximateKind.NONE;
    }

    private boolean isSkippableResultReadError(SQLException e) {
        String msg = getErrorText(e);
        String lowerMsg = msg.toLowerCase(Locale.ROOT);
        return msg.contains("Invalid length") && msg.contains("for type DATE")
                // SQLite rejects parenthesized operands of a compound SELECT, e.g.
                // "(SELECT ...) UNION ALL (SELECT ... )". These are generated
                // query-shape limitations, not VP result mismatches.
                || lowerMsg.contains("near \"(\": syntax error") || lowerMsg.contains("malformed json")
                || lowerMsg.contains("bad json path") || lowerMsg.contains("json cannot hold blob values")
                || lowerMsg.contains("json_object() labels must be text")
                || lowerMsg.contains("unable to use function") && lowerMsg.contains("requested context")
                || lowerMsg.contains("table does not support scanning") || lowerMsg.contains("integer overflow")
                || lowerMsg.contains("not authorized");
    }

    private boolean isSkippableQueryError(AssertionError e) {
        String msg = getErrorText(e);
        if (msg.isEmpty()) {
            return false;
        }
        String lowerMsg = msg.toLowerCase(Locale.ROOT);
        return msg.contains("doesn't yet support") || msg.contains("You have an error in your SQL syntax")
                || msg.contains("window") || msg.contains("Incorrect DECIMAL value")
                || msg.contains("Truncated incorrect") || msg.contains("Data truncation")
                || msg.contains("Invalid JSON") || lowerMsg.contains("malformed json")
                || lowerMsg.contains("bad json path") || lowerMsg.contains("json cannot hold blob values")
                || lowerMsg.contains("json_object() labels must be text")
                || lowerMsg.contains("unable to use function") && lowerMsg.contains("requested context")
                || lowerMsg.contains("table does not support scanning") || lowerMsg.contains("integer overflow")
                || lowerMsg.contains("not authorized") || msg.contains("LATERAL")
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

    private static boolean isSkippableSQLiteError(Throwable throwable) {
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
            Object value = rs.getObject(i);
            if (value == null) {
                sb.append("N;");
            } else {
                String canonical = value instanceof byte[] ? canonicalizeBlob(rs.getBytes(i))
                        : canonicalizeObject(value);
                if (canonical.length() > MAX_CANONICAL_VALUE_LENGTH) {
                    throw new IgnoreMeException();
                }
                if (canonical.length() > MAX_CANONICAL_VALUE_LENGTH
                        || sb.length() + canonical.length() > MAX_CANONICAL_ROW_LENGTH) {
                    throw new IgnoreMeException();
                }
                sb.append("V:").append(canonical.length()).append(":").append(canonical).append(";");
            }
        }
        return sb.toString();
    }

    private String canonicalizeObject(Object value) throws SQLException {
        if (value instanceof String || value instanceof Character) {
            return "T:" + value;
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof java.math.BigInteger) {
            return "I:" + value;
        }
        if (value instanceof Float || value instanceof Double || value instanceof BigDecimal) {
            return canonicalizeReal(value);
        }
        if (value instanceof Number) {
            double doubleValue = ((Number) value).doubleValue();
            if (Double.isNaN(doubleValue)) {
                return "R:NaN";
            }
            if (doubleValue == Double.POSITIVE_INFINITY) {
                return "R:+Infinity";
            }
            if (doubleValue == Double.NEGATIVE_INFINITY) {
                return "R:-Infinity";
            }
            return canonicalizeReal(value);
        }
        return "O:" + value.getClass().getName() + ":" + value;
    }

    private String canonicalizeReal(Object value) {
        if (value instanceof Float || value instanceof Double) {
            double doubleValue = ((Number) value).doubleValue();
            if (Double.isNaN(doubleValue)) {
                return "R:NaN";
            }
            if (doubleValue == Double.POSITIVE_INFINITY) {
                return "R:+Infinity";
            }
            if (doubleValue == Double.NEGATIVE_INFINITY) {
                return "R:-Infinity";
            }
        }
        BigDecimal decimal;
        if (value instanceof BigDecimal) {
            decimal = (BigDecimal) value;
        } else {
            decimal = new BigDecimal(value.toString());
        }
        String plain = decimal.stripTrailingZeros().toPlainString();
        return "R:" + (Objects.equals(plain, "-0") ? "0" : plain);
    }

    private String canonicalizeBlob(byte[] value) {
        if (value == null) {
            throw new AssertionError("JDBC returned a BLOB object but null bytes");
        }
        return "B:" + Base64.getEncoder().encodeToString(value);
    }

    private String numericPayload(String value) {
        if (value != null && (value.startsWith("R:") || value.startsWith("I:"))) {
            return value.substring(2);
        }
        return null;
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
        sb.append("SQLite3 version: ").append(getSingleString("SELECT sqlite_version()"))
                .append(System.lineSeparator());
        sb.append("Base table: ").append(pair.schema.baseTable).append(System.lineSeparator());
        sb.append("Base table DDL: ").append(getCreateTable(pair.schema.baseTable)).append(System.lineSeparator());
        sb.append("Base table engine: ").append(pair.schema.baseEngine).append(System.lineSeparator());
        sb.append("Generated column snapshot: ").append(pair.schema.hasGeneratedColumns).append(System.lineSeparator());
        sb.append("Virtual table snapshot: ").append(pair.schema.virtualSnapshot).append(System.lineSeparator());
        sb.append("VP source engine: ").append(pair.schema.sourceEngine).append(System.lineSeparator());
        sb.append("VP left engine: ").append(pair.schema.leftEngine).append(System.lineSeparator());
        sb.append("VP right engine: ").append(pair.schema.rightEngine).append(System.lineSeparator());
        sb.append("Single-table relation mode: ")
                .append(canCompareBaseTableDirectly(pair.schema) ? "BASE_SNAPSHOT" : "MATERIALIZED_SOURCE")
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
                ResultSet rs = s.executeQuery("SELECT sql FROM sqlite_schema WHERE name = '" + escapeLiteral(table)
                        + "' UNION ALL SELECT sql FROM sqlite_temp_schema WHERE name = '" + escapeLiteral(table)
                        + "' LIMIT 1")) {
            if (rs.next()) {
                String sql = rs.getString(1);
                return sql == null ? "<unavailable>" : sql;
            }
            return "<unavailable>";
        }
    }

    private String extractEngineFromCreateTable(String createTable, SQLite3Table table) {
        String upper = createTable.toUpperCase(Locale.ROOT);
        List<String> shape = new ArrayList<>();
        if (upper.contains("VIRTUAL TABLE")) {
            shape.add("VIRTUAL");
            if (upper.contains("USING FTS")) {
                shape.add("FTS");
            }
            if (upper.contains("USING RTREE")) {
                shape.add("RTREE");
            }
        }
        if (upper.contains("WITHOUT ROWID")) {
            shape.add("WITHOUT_ROWID");
        }
        if (upper.contains("STRICT")) {
            shape.add("STRICT");
        }
        if (table.getColumns().stream().anyMatch(SQLite3Column::isGenerated)) {
            shape.add("GENERATED");
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

    private String safeSingleString(String sql) {
        try {
            return getSingleString(sql);
        } catch (SQLException e) {
            return "<unavailable: " + compactErrorReason(e) + ">";
        }
    }

    private boolean isNoQuerySolution(SQLException e) {
        String msg = getErrorText(e).toLowerCase(Locale.ROOT);
        return msg.contains("no query solution");
    }

    private String captureSessionVars() {
        return List.of("foreign_keys", "recursive_triggers", "case_sensitive_like", "temp_store", "encoding").stream()
                .map(this::capturePragma).collect(Collectors.joining(", "));
    }

    private String capturePragma(String pragmaName) {
        try {
            return pragmaName + "=" + getSingleString("PRAGMA " + pragmaName);
        } catch (SQLException e) {
            return pragmaName + "=<unavailable: " + e.getMessage() + ">";
        }
    }

    private String captureSessionSetStatements() {
        return "PRAGMA foreign_keys = OFF;" + System.lineSeparator() + "PRAGMA recursive_triggers = OFF;"
                + System.lineSeparator() + "PRAGMA temp_store = MEMORY;" + System.lineSeparator();
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

    private String formatQueryPlan(String query) {
        String explain = "EXPLAIN QUERY PLAN " + query;
        StringBuilder sb = new StringBuilder();
        sb.append(explain).append(";").append(System.lineSeparator());
        sb.append(executeDiagnosticQuery(explain));
        return sb.toString();
    }

    private String formatIntegrityChecks() {
        StringBuilder sb = new StringBuilder();
        sb.append("PRAGMA quick_check;").append(System.lineSeparator());
        sb.append(executeDiagnosticQuery("PRAGMA quick_check"));
        sb.append("PRAGMA integrity_check;").append(System.lineSeparator());
        sb.append(executeDiagnosticQuery("PRAGMA integrity_check"));
        return sb.toString();
    }

    private String saveDatabaseSnapshot(VpSchema vp) {
        try {
            File dir = new File(Main.LOG_DIRECTORY, "sqlite3");
            Files.createDirectories(dir.toPath());
            String fileName = "snapshot-" + sanitizeIdentifier(state.getDatabaseName()) + "-"
                    + sanitizeIdentifier(vp.prefix) + ".db";
            Path target = new File(dir, fileName).toPath();
            try (Statement s = state.getConnection().createStatement()) {
                s.execute("VACUUM INTO '" + escapeLiteral(target.toAbsolutePath().toString()) + "'");
            }
            return target.toAbsolutePath().toString();
        } catch (SQLException | IOException e) {
            return "<unavailable: " + compactErrorReason(e) + ">";
        }
    }

    private void preserveCorruptionSnapshot(String phase, Throwable failure) {
        if (!isCorruptionThrowable(failure)) {
            return;
        }
        File databaseFile = state.getDatabaseFile();
        if (databaseFile == null) {
            return;
        }
        File dir = new File(Main.LOG_DIRECTORY, "sqlite3");
        try {
            Files.createDirectories(dir.toPath());
            String prefix = "corruption-" + sanitizeIdentifier(state.getDatabaseName()) + "-"
                    + sanitizeIdentifier(phase) + "-" + CORRUPTION_SNAPSHOT_ID.incrementAndGet();
            copyFileIfPresent(databaseFile.toPath(), new File(dir, prefix + ".db").toPath());
            copyFileIfPresent(new File(databaseFile.getAbsolutePath() + "-wal").toPath(),
                    new File(dir, prefix + ".db-wal").toPath());
            copyFileIfPresent(new File(databaseFile.getAbsolutePath() + "-shm").toPath(),
                    new File(dir, prefix + ".db-shm").toPath());
            copyFileIfPresent(new File(databaseFile.getAbsolutePath() + "-journal").toPath(),
                    new File(dir, prefix + ".db-journal").toPath());
            if (state.getLogger() != null) {
                state.getLogger().writeCurrent("-- saved SQLite corruption snapshot: " + new File(dir, prefix + ".db"));
            }
        } catch (IOException ignored) {
        }
    }

    private void copyFileIfPresent(Path source, Path target) throws IOException {
        if (Files.exists(source)) {
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private boolean isCorruptionThrowable(Throwable throwable) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            String message = current.getMessage();
            if (message == null) {
                continue;
            }
            String lower = message.toLowerCase(Locale.ROOT);
            if (lower.contains("malformed database schema") || lower.contains("database disk image is malformed")
                    || lower.contains("sqlite_corrupt") || lower.contains("content in the virtual table is corrupt")
                    || lower.contains("corruption found reading blob") || lower.contains("sqlite_corrupt_vtab")) {
                return true;
            }
        }
        return false;
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
        return columns.stream().map(c -> c.generated ? c.name + "(generated)" : c.name)
                .collect(Collectors.joining(", ", "{", "}"));
    }

    private void dropArtifacts(VpSchema vp) throws Exception {
        tryExecuteNoSchemaRefresh("DROP VIEW IF EXISTS " + quote(vp.viewName));
        tryExecuteNoSchemaRefresh("DROP TABLE IF EXISTS " + quote(vp.tempTable));
        tryExecuteNoSchemaRefresh("DROP TABLE IF EXISTS " + quote(vp.rightTable));
        tryExecuteNoSchemaRefresh("DROP TABLE IF EXISTS " + quote(vp.leftTable));
        tryExecuteNoSchemaRefresh("DROP TABLE IF EXISTS " + quote(vp.sourceTable));
    }

    private void cleanupIfNoBug(VpSchema vp) throws Exception {
        dropArtifacts(vp);
    }

    private void executeSetup(VpSchema vp, String sql) throws Exception {
        boolean success = state.executeStatement(new SQLQueryAdapter(sql, errors, sql.startsWith("CREATE TABLE")));
        if (!success) {
            throw new IgnoreMeException();
        }
        vp.setupStatements.add(sql);
    }

    private void tryExecuteSetup(VpSchema vp, String sql) throws Exception {
        try {
            executeSetup(vp, sql);
        } catch (IgnoreMeException ignored) {
            vp.setupStatements.add("-- skipped optional setup: " + sql);
        } catch (AssertionError e) {
            if (!isSkippableQueryError(e)) {
                throw e;
            }
            vp.setupStatements.add("-- skipped optional setup: " + sql + " reason=" + compactErrorReason(e));
        }
    }

    private boolean executeNoSchemaRefresh(String sql) throws Exception {
        return state.executeStatement(new SQLQueryAdapter(sql, errors, false));
    }

    private boolean executePairExtraSetup(String sql) throws SQLException {
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
            Main.nrSuccessfulActions.addAndGet(1);
            return true;
        } catch (SQLException e) {
            Main.nrUnsuccessfulActions.addAndGet(1);
            if (isExpectedPairResultError(e)) {
                recordSkippedError("extra-setup", sql, e);
                return false;
            }
            throw e;
        }
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

    private String toSQLType(SQLite3DataType type) {
        switch (type) {
        case INT:
            return "INTEGER";
        case TEXT:
            return "TEXT";
        case REAL:
            return "REAL";
        case BINARY:
            return "BLOB";
        case NONE:
            return "BLOB";
        case NULL:
            return "TEXT";
        default:
            throw new AssertionError(type);
        }
    }

    private String quote(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private String escapeLiteral(String value) {
        return value.replace("'", "''");
    }

    private String compactErrorReason(Throwable throwable) {
        String message = throwable.getMessage();
        if (message == null || message.isEmpty()) {
            return throwable.getClass().getSimpleName();
        }
        return message.replace('\n', ' ').replace('\r', ' ');
    }

    private String sanitizeIdentifier(String identifier) {
        return identifier.replaceAll("[^A-Za-z0-9_]", "_");
    }
}
