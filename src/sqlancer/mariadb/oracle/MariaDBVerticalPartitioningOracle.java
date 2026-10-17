package sqlancer.mariadb.oracle;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.regex.Pattern;
import java.util.function.Supplier;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBErrors;
import sqlancer.mariadb.MariaDBProvider;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBColumn;
import sqlancer.mariadb.MariaDBSchema.MariaDBDataType;
import sqlancer.mariadb.MariaDBSchema.MariaDBIndex;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable.MariaDBEngine;
import sqlancer.mariadb.ast.MariaDBColumnName;
import sqlancer.mariadb.ast.MariaDBExpression;
import sqlancer.mariadb.ast.MariaDBSelectStatement;
import sqlancer.mariadb.ast.MariaDBTableReference;
import sqlancer.mariadb.gen.MariaDBExpressionGenerator;

/** MariaDB-specific vertical partitioning equivalence oracle. */
public final class MariaDBVerticalPartitioningOracle implements TestOracle<MariaDBGlobalState> {
    private static final String ROW_ID = "vp_rowid";
    private static final int BASE_TABLES_PER_BATCH = 2;
    private static final int STATS_WRITE_ATTEMPT_INTERVAL = 1000;
    private static final int MAX_DIFF_ROWS = 8;
    private static final int MAX_RESULT_SAMPLE = 12;
    private static final double FLOAT_ABSOLUTE_TOLERANCE = 1e-6;
    private static final double FLOAT_RELATIVE_TOLERANCE = 1e-5;
    private static final int DOUBLE_MAX_ULPS = 4;
    private static final AtomicLong RUN_ID = new AtomicLong();
    private static final AtomicLong FEATURE_CANDIDATE_CURSOR = new AtomicLong();
    private static final AtomicLong KNOWN_BUG_LOG_ID = new AtomicLong();
    private static final AtomicBoolean STATS_HOOK_REGISTERED = new AtomicBoolean();
    private static final Object STATS_LOCK = new Object();
    private static final Object STATS_WRITE_LOCK = new Object();
    private static final File STATS_FILE = new File(new File(Main.LOG_DIRECTORY, "mariadb"), "vp-stats.log");
    private static final Map<SplitStrategy, Integer> SPLIT_COUNTS = new EnumMap<>(SplitStrategy.class);
    private static final Map<String, Integer> SPLIT_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final Map<ReconstructionStrategy, Integer> RECONSTRUCTION_COUNTS = new EnumMap<>(
            ReconstructionStrategy.class);
    private static final Map<String, Integer> RECONSTRUCTION_PLAN_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> KNOWN_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> OPTIMIZER_PROFILE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> OPTIMIZER_PROFILE_MISMATCH_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> OPTIMIZER_PROFILE_SKIP_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SCHEMA_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SCHEMA_TYPE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> NATIVE_TYPE_PROJECTION_COUNTS = new LinkedHashMap<>();
    private static final Map<BaseShape, Integer> SHAPE_COUNTS = new EnumMap<>(BaseShape.class);
    private static final Map<String, Integer> SKIP_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> SETUP_FAILURE_COUNTS = new LinkedHashMap<>();
    private static final Set<String> LOGGED_SETUP_FAILURES = new HashSet<>();
    private static final Set<String> LOGGED_SKIP_SAMPLES = new HashSet<>();
    private static final Map<String, Integer> QUERY_SHAPE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> AST_POLICY_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> ROW_COUNT_BUCKET_COUNTS = new LinkedHashMap<>();
    private static int completedOracleRuns;
    private static int completedQueryPairs;
    private static int completedAttempts;
    private static int schemaCopySuccesses;
    private static int schemaCopyFailures;
    private static int engineFallbacks;
    private static final Map<String, Integer> ENGINE_COMBINATION_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> ENGINE_ROLE_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> ENGINE_FALLBACK_REASON_COUNTS = new LinkedHashMap<>();
    private static final Map<String, Integer> ROW_ID_KEY_COUNTS = new LinkedHashMap<>();
    private static final AtomicInteger ENGINE_COMBINATION_CURSOR = new AtomicInteger();
    private static long rowCountSamples;
    private static long rowCountSum;
    private static long rowCountMin = Long.MAX_VALUE;
    private static long rowCountMax;
    private static int lastStatsWriteAttemptBucket = -1;

    private enum BaseShape {
        EMPTY_SINGLE_COLUMN, EMPTY_MULTI_COLUMN, NON_EMPTY_SINGLE_COLUMN, NON_EMPTY_MULTI_COLUMN
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

    private static final class Column {
        final String name;
        final MariaDBDataType type;
        final String definition;

        @SuppressWarnings("unused")
        Column(String name, MariaDBDataType type) {
            this(name, type, type(type));
        }

        Column(String name, MariaDBDataType type, String definition) {
            this.name = name;
            this.type = type;
            this.definition = definition;
        }
    }

    private static final class Plan {
        final List<Column> all, left, right, overlap;

        Plan(List<Column> all, List<Column> left, List<Column> right, List<Column> overlap) {
            this.all = all;
            this.left = left;
            this.right = right;
            this.overlap = overlap;
        }
    }

    private static final class VpTables {
        final String prefix, source, left, right, view, temp, baseSnapshot;
        final String baseTable;
        final String snapshotOrder;
        final BaseShape shape;
        final SplitStrategy splitStrategy;
        final Plan plan;
        MariaDBEngine sourceEngine, leftEngine, rightEngine, baseEngine;
        long baseRowCount;
        final List<String> setupStatements = new ArrayList<>();

        VpTables(String prefix, MariaDBTable base, Plan plan, BaseShape shape, SplitStrategy splitStrategy,
                long baseRowCount, String snapshotOrder) {
            this.prefix = prefix;
            this.plan = plan;
            this.baseTable = base.getName();
            this.shape = shape;
            this.splitStrategy = splitStrategy;
            this.baseEngine = base.getEngine();
            this.baseRowCount = baseRowCount;
            this.snapshotOrder = snapshotOrder;
            this.sourceEngine = MariaDBEngine.INNO_DB;
            this.leftEngine = MariaDBEngine.INNO_DB;
            this.rightEngine = MariaDBEngine.INNO_DB;
            source = quote(prefix + "source");
            left = quote(prefix + "left");
            right = quote(prefix + "right");
            view = quote(prefix + "view");
            temp = quote(prefix + "temp");
            baseSnapshot = quote(prefix + "base_snapshot");
        }
    }

    private static final class QueryPair {
        final String direct, rewritten, description, actualPlan;
        final VpTables vp;
        final ReconstructionStrategy strategy;
        final boolean orderedComparison;
        final List<String> profileSetup;

        QueryPair(VpTables vp, ReconstructionStrategy strategy, String direct, String rewritten, String description,
                boolean orderedComparison) {
            this(vp, strategy, direct, rewritten, description, orderedComparison, strategy.name());
        }

        QueryPair(VpTables vp, ReconstructionStrategy strategy, String direct, String rewritten, String description,
                boolean orderedComparison, String actualPlan) {
            this(vp, strategy, direct, rewritten, description, orderedComparison, actualPlan, List.of());
        }

        QueryPair(VpTables vp, ReconstructionStrategy strategy, String direct, String rewritten, String description,
                boolean orderedComparison, String actualPlan, List<String> profileSetup) {
            this.vp = vp;
            this.strategy = strategy;
            this.direct = direct;
            this.rewritten = rewritten;
            this.description = description;
            this.orderedComparison = orderedComparison;
            this.actualPlan = actualPlan;
            this.profileSetup = profileSetup;
        }
    }

    private static final class QueryResultMultiset {
        final Map<String, Integer> rows;
        final Map<String, List<ApproximateKind>> approximateColumns;
        final List<String> orderedRows;
        final List<List<ApproximateKind>> orderedApproximateColumns;

        QueryResultMultiset(Map<String, Integer> rows, Map<String, List<ApproximateKind>> approximateColumns,
                List<String> orderedRows, List<List<ApproximateKind>> orderedApproximateColumns) {
            this.rows = rows;
            this.approximateColumns = approximateColumns;
            this.orderedRows = orderedRows;
            this.orderedApproximateColumns = orderedApproximateColumns;
        }
    }

    private static final class QueryOutcome {
        final QueryResultMultiset result;
        final Throwable error;

        QueryOutcome(QueryResultMultiset result, Throwable error) {
            this.result = result;
            this.error = error;
        }

        boolean succeeded() {
            return error == null;
        }
    }

    private static final class DmlStepResult {
        final boolean succeeded;
        final int affectedRows;
        final QueryResultMultiset returnedRows;
        final Throwable error;

        DmlStepResult(boolean succeeded, int affectedRows, QueryResultMultiset returnedRows, Throwable error) {
            this.succeeded = succeeded;
            this.affectedRows = affectedRows;
            this.returnedRows = returnedRows;
            this.error = error;
        }
    }

    private static final class DmlScenarioResult {
        final List<DmlStepResult> steps = new ArrayList<>();
        QueryResultMultiset duringSavepoint;
        QueryResultMultiset afterRollbackToSavepoint;
        QueryResultMultiset beforeCommit;
        QueryResultMultiset afterCommit;
        QueryResultMultiset finalRows;
    }

    private enum DmlScenario {
        INSERT_SELECT, INSERT_IGNORE, ON_DUPLICATE_KEY_UPDATE, REPLACE, SAVEPOINT_ROLLBACK, RETURNING
    }

    private enum ApproximateKind {
        NONE(0, 0, 0), FLOAT(FLOAT_ABSOLUTE_TOLERANCE, FLOAT_RELATIVE_TOLERANCE, 0), DOUBLE(0, 0, DOUBLE_MAX_ULPS);

        final double absoluteTolerance, relativeTolerance;
        final int maxUlps;

        ApproximateKind(double absoluteTolerance, double relativeTolerance, int maxUlps) {
            this.absoluteTolerance = absoluteTolerance;
            this.relativeTolerance = relativeTolerance;
            this.maxUlps = maxUlps;
        }

        boolean isApproximate() {
            return this != NONE;
        }

        static ApproximateKind looser(ApproximateKind left, ApproximateKind right) {
            if (left == FLOAT || right == FLOAT)
                return FLOAT;
            if (left == DOUBLE || right == DOUBLE)
                return DOUBLE;
            return NONE;
        }
    }

    private final MariaDBGlobalState state;
    private final ExpectedErrors errors = new ExpectedErrors();
    private final Map<String, Connection> optimizerProfileConnections = new LinkedHashMap<>();

    public MariaDBVerticalPartitioningOracle(MariaDBGlobalState state) {
        this.state = state;
        MariaDBErrors.addCommonErrors(errors);
        for (String error : List.of("Unknown column", "doesn't exist", "not supported", "not valid", "Data truncated",
                "Invalid JSON", "Duplicate column"))
            errors.add(error);
    }

    @Override
    public void check() throws Exception {
        int checkedPairs = 0;
        int attempts = 0;
        try {
            prepareSession();
            MariaDBProvider.ensureVerticalPartitioningBaseTables(state, BASE_TABLES_PER_BATCH);
            if (state.getDbmsSpecificOptions().verticalPartitioningMariaDBFeatures) {
                runMariaDBFeatureProbes();
            }
            recordSchemaShapeCounts();
            List<MariaDBTable> candidates = candidates();
            if (candidates.isEmpty())
                throw new IgnoreMeException();
            MariaDBTable base = chooseCandidate(candidates);
            List<Column> columns = columns(base);
            BaseShape shape = getShape(base, columns);
            increment(SHAPE_COUNTS, shape);
            for (SplitStrategy split : shuffledSplits(shape)) {
                Plan plan = createPlan(columns, split);
                VpTables vp = new VpTables("vp_mariadb_" + sanitizeIdentifier(state.getDatabaseName()) + "_"
                        + RUN_ID.incrementAndGet() + "_", base, plan, shape, split, count(base.getName()),
                        stableOrder(base, columns));
                try {
                    materialize(base, vp);
                    increment(SPLIT_COUNTS, split);
                    increment(SPLIT_SHAPE_COUNTS, actualSplitShape(plan));
                    if (deepEnabled()) {
                        runDeepFeatureChecks(vp);
                        runDmlTransactionProbes(vp);
                    }
                    for (ReconstructionStrategy strategy : shuffledReconstructions()) {
                        for (int i = 0; i < Math.max(1,
                                state.getDbmsSpecificOptions().verticalPartitioningQueryVariantsPerCombination); i++) {
                            attempts++;
                            try {
                                QueryPair pair = generateQueryPair(vp, strategy);
                                if (compare(pair, split, strategy)) {
                                    checkedPairs++;
                                    increment(RECONSTRUCTION_COUNTS, strategy);
                                    increment(RECONSTRUCTION_PLAN_COUNTS, pair.actualPlan);
                                    recordExplainPlan(pair);
                                }
                                for (QueryPair extra : generateSupplementaryQueryPairs(vp, strategy)) {
                                    attempts++;
                                    if (compare(extra, split, strategy)) {
                                        checkedPairs++;
                                        increment(RECONSTRUCTION_COUNTS, strategy);
                                        increment(RECONSTRUCTION_PLAN_COUNTS, extra.actualPlan);
                                    }
                                }
                            } catch (Throwable e) {
                                if (!isSkippableThrowable(e))
                                    throw e;
                                recordSkip(strategy, e);
                            }
                        }
                    }
                } finally {
                    cleanup(vp);
                }
            }
            if (checkedPairs == 0)
                throw new IgnoreMeException();
            recordCompletedVpRun(checkedPairs, attempts);
            MariaDBProvider.replaceVerticalPartitioningTableAfterSuccessfulCheck(state, base.getName());
        } catch (IgnoreMeException e) {
            recordSkippedVpRun(checkedPairs, attempts, e);
            throw e;
        } finally {
            closeOptimizerProfileConnections();
            writeStats();
        }
    }

    private void recordExplainPlan(QueryPair pair) {
        // Keep a lightweight plan census for the semantic reconstruction
        // families.  This is deliberately best-effort: EXPLAIN failures must
        // not turn a valid VP comparison into a skipped query.
        if (!(pair.strategy == ReconstructionStrategy.SCALAR_SUBQUERY
                || pair.strategy == ReconstructionStrategy.CTE
                || pair.strategy == ReconstructionStrategy.VIEW
                || pair.strategy == ReconstructionStrategy.TEMP_TABLE
                || pair.strategy == ReconstructionStrategy.DERIVED_TABLE)) {
            return;
        }
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("EXPLAIN " + pair.rewritten)) {
            while (rs.next()) {
                String type = rs.getString("type");
                if (type != null)
                    increment(RECONSTRUCTION_PLAN_COUNTS, pair.strategy.name() + ":EXPLAIN_" + type.toUpperCase(Locale.ROOT));
            }
        } catch (SQLException ignored) {
            increment(RECONSTRUCTION_PLAN_COUNTS, pair.strategy.name() + ":EXPLAIN_ERROR");
        }
    }

    private void runMariaDBFeatureProbes() {
        // These probes intentionally do not feed values into the equivalence
        // oracle: temporal snapshots and sequence advancement are stateful.
        try {
            execute("UPDATE `vp_system_versioned` SET `val` = `val` + 1 WHERE `id` = 1");
            try (Statement s = state.getConnection().createStatement();
                    ResultSet rs = s.executeQuery("SELECT `val` FROM `vp_system_versioned` "
                            + "FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP - INTERVAL 1 SECOND WHERE `id` = 1")) {
                if (rs.next()) {
                    logLine("vp-mariadb-features.log", "system-versioned snapshot val=" + rs.getString(1));
                }
            }
        } catch (Exception e) {
            recordFeatureProbeSkip("system-versioned", e);
        }
        try {
            try (Statement s = state.getConnection().createStatement();
                    ResultSet rs = s.executeQuery("SELECT `id`, `s`, `e` FROM `vp_application_period` "
                            + "WHERE DATE '2025-01-01' BETWEEN `s` AND `e`")) {
                int rows = 0;
                while (rs.next())
                    rows++;
                logLine("vp-mariadb-features.log", "application-period rows=" + rows);
            }
        } catch (Exception e) {
            recordFeatureProbeSkip("application-period", e);
        }
        try {
            try (Statement s = state.getConnection().createStatement();
                    ResultSet rs = s.executeQuery("SELECT * FROM `vp_invisible`")) {
                int columns = rs.getMetaData().getColumnCount();
                logLine("vp-mariadb-features.log", "invisible select-star columns=" + columns);
            }
        } catch (Exception e) {
            recordFeatureProbeSkip("invisible-columns", e);
        }
        try {
            try (Statement s = state.getConnection().createStatement();
                    ResultSet rs = s.executeQuery("SELECT NEXT VALUE FOR `vp_sequence`")) {
                if (rs.next())
                    logLine("vp-mariadb-features.log", "sequence next=" + rs.getString(1));
            }
        } catch (Exception e) {
            recordFeatureProbeSkip("sequence", e);
        }
        try {
            runPartitionPhysicalEquivalenceProbe();
        } catch (Exception e) {
            recordFeatureProbeSkip("partition-physical-equivalence", e);
        }
    }

    /**
     * A separate physical-schema metamorphic check. This intentionally does
     * not enter the normal direct/rewrite VP comparison: partitioning is a
     * storage property, while the VP oracle is about relational
     * reconstruction. The probe keeps the same generated/check-constrained
     * schema and data, removes only partitioning in the copy, and compares
     * deterministic read-only projections.
     */
    private void runPartitionPhysicalEquivalenceProbe() throws Exception {
        String suffix = sanitizeIdentifier(state.getDatabaseName()) + "_" + RUN_ID.incrementAndGet();
        String partitioned = quote("vp_partition_probe_" + suffix);
        String unpartitioned = quote("vp_unpartitioned_probe_" + suffix);
        String generatedQuery = "SELECT `id`, `plain`, `v_double`, `p_next` FROM " + partitioned
                + " ORDER BY `id`";
        String explicitQuery = "SELECT `id`, `plain`, `plain` * 2, `plain` + 1 FROM " + partitioned
                + " ORDER BY `id`";
        String copyQuery = "SELECT `id`, `plain`, `v_double`, `p_next` FROM " + unpartitioned
                + " ORDER BY `id`";
        try {
            execute("DROP TABLE IF EXISTS " + partitioned);
            execute("DROP TABLE IF EXISTS " + unpartitioned);
            execute("CREATE TABLE " + partitioned
                    + " (`id` INT NOT NULL, `plain` INT NOT NULL,"
                    + " `v_double` INT GENERATED ALWAYS AS (`plain` * 2) VIRTUAL,"
                    + " `p_next` INT GENERATED ALWAYS AS (`plain` + 1) STORED,"
                    + " CHECK (`plain` BETWEEN -100 AND 100),"
                    + " PRIMARY KEY (`id`, `plain`), KEY `idx_v` (`v_double`),"
                    + " KEY `idx_plain_ignored` (`plain`) IGNORED)"
                    + " ENGINE=InnoDB PARTITION BY RANGE (`id`)"
                    + " (PARTITION p0 VALUES LESS THAN (0),"
                    + " PARTITION p1 VALUES LESS THAN (10),"
                    + " PARTITION pmax VALUES LESS THAN MAXVALUE)");
            execute("INSERT INTO " + partitioned + " (`id`, `plain`) VALUES"
                    + " (-1, -100), (0, 0), (1, 1), (9, 9), (10, 100)");
            execute("CREATE TABLE " + unpartitioned + " LIKE " + partitioned);
            execute("ALTER TABLE " + unpartitioned + " REMOVE PARTITIONING");
            execute("INSERT INTO " + unpartitioned + " (`id`, `plain`) SELECT `id`, `plain` FROM " + partitioned);

            QueryResultMultiset generated = getResultMultiset(generatedQuery);
            QueryResultMultiset explicit = getResultMultiset(explicitQuery);
            QueryResultMultiset copied = getResultMultiset(copyQuery);
            if (!orderedRowsMatch(generated, explicit)) {
                throw new AssertionError("Generated-column versus explicit-expression mismatch"
                        + "\nGenerated: " + generatedQuery + "\nExplicit: " + explicitQuery
                        + "\nGenerated sample: " + sample(generated.rows)
                        + "\nExplicit sample: " + sample(explicit.rows));
            }
            if (!orderedRowsMatch(generated, copied)) {
                throw new AssertionError("Partitioned versus unpartitioned physical mismatch"
                        + "\nPartitioned: " + generatedQuery + "\nUnpartitioned: " + copyQuery
                        + "\nPartitioned sample: " + sample(generated.rows)
                        + "\nUnpartitioned sample: " + sample(copied.rows));
            }
            logLine("vp-mariadb-features.log",
                    "partition-physical-equivalence ok rows=" + generated.rows.values().stream()
                            .mapToInt(Integer::intValue).sum());
        } finally {
            try {
                execute("DROP TABLE IF EXISTS " + unpartitioned);
            } catch (Exception ignored) {
            }
            try {
                execute("DROP TABLE IF EXISTS " + partitioned);
            } catch (Exception ignored) {
            }
        }
    }

    private void recordFeatureProbeSkip(String feature, Throwable throwable) {
        logLine("vp-mariadb-features.log", feature + " skipped=" + errorText(throwable).split("\\R", 2)[0]);
    }

    private List<MariaDBTable> candidates() {
        List<MariaDBTable> result = new ArrayList<>(state.getSchema().getDatabaseTables());
        result.removeIf(t -> t.isView()
                || (t.getName().startsWith("vp_") && !MariaDBProvider.isMariaDBFeatureTable(t.getName())));
        if (!state.getVerticalPartitioningProtectedTables().isEmpty()) {
            result.removeIf(t -> !state.getVerticalPartitioningProtectedTables().contains(t.getName()));
        }
        return result;
    }

    private MariaDBTable chooseCandidate(List<MariaDBTable> candidates) {
        if (state.getDbmsSpecificOptions().verticalPartitioningMariaDBFeatures
                && FEATURE_CANDIDATE_CURSOR.getAndIncrement() % 3 == 0) {
            List<MariaDBTable> featureTables = candidates.stream()
                    .filter(t -> MariaDBProvider.isMariaDBFeatureTable(t.getName())).collect(Collectors.toList());
            if (!featureTables.isEmpty()) {
                return featureTables.get((int) (FEATURE_CANDIDATE_CURSOR.get() % featureTables.size()));
            }
        }
        return Randomly.fromList(candidates);
    }

    private void recordSchemaShapeCounts() throws SQLException {
        int emptySingle = 0, emptyMulti = 0, nonEmptySingle = 0, nonEmptyMulti = 0;
        for (MariaDBTable table : state.getSchema().getDatabaseTables()) {
            if (!state.getVerticalPartitioningProtectedTables().contains(table.getName()) || table.isView())
                continue;
            for (MariaDBColumn column : table.getColumns()) {
                increment(SCHEMA_TYPE_COUNTS, column.getType().name());
            }
            long rowCount = count(table.getName());
            recordRowCount(rowCount);
            boolean empty = rowCount == 0;
            boolean single = table.getColumns().size() == 1;
            if (empty && single)
                emptySingle++;
            else if (empty)
                emptyMulti++;
            else if (single)
                nonEmptySingle++;
            else
                nonEmptyMulti++;
        }
        increment(SCHEMA_SHAPE_COUNTS, "empty-single", emptySingle);
        increment(SCHEMA_SHAPE_COUNTS, "empty-multi", emptyMulti);
        increment(SCHEMA_SHAPE_COUNTS, "nonempty-single", nonEmptySingle);
        increment(SCHEMA_SHAPE_COUNTS, "nonempty-multi", nonEmptyMulti);
    }

    private static void recordRowCount(long rowCount) {
        synchronized (STATS_LOCK) {
            rowCountSamples++;
            rowCountSum += rowCount;
            rowCountMin = Math.min(rowCountMin, rowCount);
            rowCountMax = Math.max(rowCountMax, rowCount);
            String bucket;
            if (rowCount == 0) {
                bucket = "0";
            } else if (rowCount < 8) {
                bucket = "1-7";
            } else if (rowCount <= 32) {
                bucket = "8-32";
            } else if (rowCount <= 256) {
                bucket = "33-256";
            } else {
                bucket = "257+";
            }
            ROW_COUNT_BUCKET_COUNTS.put(bucket, ROW_COUNT_BUCKET_COUNTS.getOrDefault(bucket, 0) + 1);
        }
    }

    private List<Column> columns(MariaDBTable table) {
        List<Column> result = new ArrayList<>();
        for (MariaDBColumn column : table.getColumns()) {
            result.add(new Column(column.getName(), column.getType(), columnDefinition(column)));
        }
        return result;
    }

    private String columnDefinition(MariaDBColumn column) {
        String definition = column.getColumnTypeDefinition();
        if (definition == null || definition.isEmpty()) {
            definition = type(column.getType());
        } else {
            if (column.getCharacterSet() != null)
                definition += " CHARACTER SET " + column.getCharacterSet();
            if (column.getCollation() != null)
                definition += " COLLATE " + column.getCollation();
        }
        if (!column.isNullable())
            definition += " NOT NULL";
        return definition;
    }

    private BaseShape getShape(MariaDBTable table, List<Column> columns) throws SQLException {
        long rows = count(table.getName());
        if (rows == 0) {
            return columns.size() == 1 ? BaseShape.EMPTY_SINGLE_COLUMN : BaseShape.EMPTY_MULTI_COLUMN;
        }
        return columns.size() == 1 ? BaseShape.NON_EMPTY_SINGLE_COLUMN : BaseShape.NON_EMPTY_MULTI_COLUMN;
    }

    private Plan createPlan(List<Column> columns, SplitStrategy strategy) {
        List<Column> left = new ArrayList<>(), right = new ArrayList<>(), overlap = new ArrayList<>();
        switch (strategy) {
        case EMPTY_SPLIT:
            right.addAll(columns);
            break;
        case SINGLE_REPLICATED:
            left.addAll(columns);
            right.addAll(columns);
            overlap.addAll(columns);
            break;
        case SINGLE_RIGHT_ROW_ID_ONLY:
            left.addAll(columns);
            break;
        case SINGLE_LEFT_ROW_ID_ONLY:
            right.addAll(columns);
            break;
        case EVEN_SPLIT:
            splitEven(columns, left, right);
            break;
        case OVERLAPPED_SPLIT:
            splitEven(columns, left, right);
            Column overlapped = Randomly.fromList(columns);
            if (!contains(left, overlapped.name))
                left.add(overlapped);
            if (!contains(right, overlapped.name))
                right.add(overlapped);
            overlap.add(overlapped);
            break;
        case SKEWED_SPLIT:
            for (int i = 0; i < columns.size(); i++)
                (i < Math.max(1, columns.size() / 3) ? left : right).add(columns.get(i));
            break;
        case TYPE_DIRECTED_SPLIT:
            for (Column c : columns)
                (c.type == MariaDBDataType.VARCHAR ? right : left).add(c);
            break;
        default:
            throw new AssertionError(strategy);
        }
        if (left.isEmpty() && right.isEmpty() && !columns.isEmpty())
            right.addAll(columns);
        if (left.isEmpty() && !columns.isEmpty() && strategy != SplitStrategy.SINGLE_LEFT_ROW_ID_ONLY
                && strategy != SplitStrategy.EMPTY_SPLIT)
            left.add(columns.get(0));
        if (right.isEmpty() && !columns.isEmpty() && strategy != SplitStrategy.SINGLE_RIGHT_ROW_ID_ONLY)
            right.add(columns.get(columns.size() - 1));
        return new Plan(columns, unique(left), unique(right), unique(overlap));
    }

    private void splitEven(List<Column> columns, List<Column> left, List<Column> right) {
        for (int i = 0; i < columns.size(); i++)
            (i % 2 == 0 ? left : right).add(columns.get(i));
    }

    private static String actualSplitShape(Plan plan) {
        if (plan.left.isEmpty())
            return "LEFT_ROW_ID_ONLY";
        if (plan.right.isEmpty())
            return "RIGHT_ROW_ID_ONLY";
        if (plan.left.size() == plan.all.size() && plan.right.size() == plan.all.size())
            return "FULLY_REPLICATED";
        if (!plan.overlap.isEmpty())
            return "PARTIAL_OVERLAP";
        return Math.abs(plan.left.size() - plan.right.size()) <= 1 ? "DISJOINT_BALANCED" : "DISJOINT_SKEWED";
    }

    private static List<Column> unique(List<Column> columns) {
        Map<String, Column> map = new LinkedHashMap<>();
        for (Column c : columns)
            map.put(c.name, c);
        return new ArrayList<>(map.values());
    }

    private void materialize(MariaDBTable base, VpTables vp) throws Exception {
        MariaDBEngine preferred = nextHelperEngine(base.getEngine());
        increment(ENGINE_COMBINATION_COUNTS, base.getEngine() + ":" + preferred);
        createSource(base, vp, preferred);
        List<Column> insertable = insertableColumns(base, vp.plan.all);
        String sourceColumns = names(insertable);
        String order = vp.snapshotOrder;
        String sourceInput = quote(base.getName()) + snapshotOrderClause(vp) + snapshotLimit();
        if (insertable.isEmpty()) {
            executeSetup(vp, "INSERT INTO " + vp.source + " (`" + ROW_ID + "`) SELECT ROW_NUMBER() OVER ("
                    + (order.isEmpty() ? "" : "ORDER BY " + order) + ") FROM " + sourceInput);
        } else {
            executeSetup(vp, "INSERT INTO " + vp.source + " (`" + ROW_ID + "`, " + sourceColumns
                    + ") SELECT ROW_NUMBER() OVER (" + (order.isEmpty() ? "" : "ORDER BY " + order) + "), "
                    + sourceColumns + " FROM " + sourceInput);
        }
        vp.leftEngine = createHelper(vp, vp.left, vp.plan.left, preferred, "left");
        vp.rightEngine = createHelper(vp, vp.right, vp.plan.right, preferred, "right");
        increment(ENGINE_ROLE_COUNTS, "base-engine=" + vp.baseEngine);
        increment(ENGINE_ROLE_COUNTS, "source-engine=" + vp.sourceEngine);
        increment(ENGINE_ROLE_COUNTS, "left-engine=" + vp.leftEngine);
        increment(ENGINE_ROLE_COUNTS, "right-engine=" + vp.rightEngine);
        insertProjection(vp.left, vp.plan.left, vp.source);
        insertProjection(vp.right, vp.plan.right, vp.source);
        validateMaterialized(vp);
        maybeCreateHelperIndexes(vp);
    }

    private String snapshotOrderClause(VpTables vp) {
        return vp.snapshotOrder.isEmpty() ? "" : " ORDER BY " + vp.snapshotOrder;
    }

    private String snapshotLimit() {
        return state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows > 0
                ? " LIMIT " + state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows : "";
    }

    private void validateMaterialized(VpTables vp) throws SQLException {
        long sourceRows = count(vp.source);
        long leftRows = count(vp.left);
        long rightRows = count(vp.right);
        if (sourceRows != leftRows || sourceRows != rightRows) {
            throw new SQLException("VP materialization row-count mismatch: source=" + sourceRows + ", left=" + leftRows
                    + ", right=" + rightRows);
        }
        for (String table : List.of(vp.source, vp.left, vp.right)) {
            try (Statement statement = state.getConnection().createStatement(); ResultSet rs = statement
                    .executeQuery("SELECT COUNT(`" + ROW_ID + "`), COUNT(DISTINCT `" + ROW_ID + "`) FROM " + table)) {
                rs.next();
                if (rs.getLong(1) != rs.getLong(2)) {
                    throw new SQLException("VP materialization duplicate rowid in " + table);
                }
            }
            if (!hasUniqueRowIdKey(table)) {
                throw new SQLException("VP materialization has no unique rowid key in " + table);
            }
        }
    }

    private boolean hasUniqueRowIdKey(String table) throws SQLException {
        String name = table.substring(1, table.length() - 1).replace("``", "`");
        String sql = "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='"
                + state.getDatabaseName().replace("'", "''") + "' AND TABLE_NAME='" + name.replace("'", "''")
                + "' AND COLUMN_NAME='" + ROW_ID + "' AND SEQ_IN_INDEX=1 AND NON_UNIQUE=0";
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1) > 0;
        }
    }

    private MariaDBEngine nextHelperEngine(MariaDBEngine base) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningHelperUseBaseEngine
                && !state.getDbmsSpecificOptions().verticalPartitioningMariaDBFeatures) {
            return MariaDBEngine.INNO_DB;
        }
        List<MariaDBEngine> choices;
        switch (base) {
        case INNO_DB:
            choices = List.of(MariaDBEngine.INNO_DB, MariaDBEngine.MY_ISAM, MariaDBEngine.ARIA);
            break;
        case MY_ISAM:
            choices = List.of(MariaDBEngine.MY_ISAM, MariaDBEngine.INNO_DB);
            break;
        case ARIA:
            choices = List.of(MariaDBEngine.ARIA, MariaDBEngine.INNO_DB);
            break;
        default:
            choices = List.of(MariaDBEngine.INNO_DB);
        }
        return choices.get(Math.floorMod(ENGINE_COMBINATION_CURSOR.getAndIncrement(), choices.size()));
    }

    private void createSource(MariaDBTable base, VpTables vp, MariaDBEngine preferred) throws Exception {
        // LIKE copies MariaDB temporal metadata. ALTER TABLE on that copy is
        // rejected for system-versioned tables, and replaying PERIOD metadata
        // would make the source an independent temporal table rather than the
        // current-state snapshot used by this oracle.
        if (MariaDBProvider.isMariaDBFeatureTable(base.getName()) && !"vp_invisible".equalsIgnoreCase(base.getName())) {
            vp.sourceEngine = createHelper(vp, vp.source, vp.plan.all, preferred, "source");
            return;
        }
        if (state.getDbmsSpecificOptions().verticalPartitioningSourceCopySchemaObjects) {
            try {
                executeSetup(vp, "CREATE TABLE " + vp.source + " LIKE " + quote(base.getName()));
                executeSetup(vp, "ALTER TABLE " + vp.source + " ADD COLUMN `" + ROW_ID + "` BIGINT NOT NULL FIRST");
                try {
                    executeSetup(vp, "ALTER TABLE " + vp.source + " ENGINE=" + preferred.getTextRepresentation());
                    vp.sourceEngine = preferred;
                } catch (Throwable e) {
                    if (preferred == MariaDBEngine.INNO_DB || !isEngineKeyLimitFailure(e)) {
                        rethrow(e);
                    }
                    logFallback("source", preferred, rootCauseMessage(e));
                    executeSetup(vp, "ALTER TABLE " + vp.source + " ENGINE=InnoDB");
                    vp.sourceEngine = MariaDBEngine.INNO_DB;
                }
                executeSetup(vp, "CREATE UNIQUE INDEX " + quote(vp.prefix + "source_rowid_uq") + " ON " + vp.source
                        + " (`" + ROW_ID + "`)");
                increment(ROW_ID_KEY_COUNTS, "source");
                recordSchemaCopySuccess();
                return;
            } catch (Exception e) {
                logSchemaCopyFailure(vp.source, "CREATE TABLE LIKE", e.getMessage());
                cleanup(vp);
                try {
                    createSourceFromShowCreate(base.getName(), vp.source, preferred);
                    vp.sourceEngine = preferred;
                    increment(ROW_ID_KEY_COUNTS, "source");
                } catch (Exception ddlFailure) {
                    logSchemaCopyFailure(vp.source, "SHOW CREATE TABLE replay", ddlFailure.getMessage());
                    vp.sourceEngine = createHelper(vp, vp.source, vp.plan.all, preferred, "source");
                    copySchemaObjects(base.getName(), vp.source, vp.prefix + "source");
                }
                return;
            }
        }
        vp.sourceEngine = createHelper(vp, vp.source, vp.plan.all, preferred, "source");
    }

    private void createSourceFromShowCreate(String base, String source, MariaDBEngine preferred) throws Exception {
        String ddl;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("SHOW CREATE TABLE " + quote(base))) {
            if (!rs.next())
                throw new SQLException("SHOW CREATE TABLE returned no row");
            ddl = rs.getString(2);
        }
        int open = ddl.indexOf('(');
        if (open < 0)
            throw new SQLException("SHOW CREATE TABLE has no column list");
        String rewritten = "CREATE TABLE " + source + " (\n`" + ROW_ID + "` BIGINT NOT NULL UNIQUE, "
                + ddl.substring(open + 1);
        rewritten = rewritten.replaceFirst("(?i)ENGINE\\s*=\\s*[A-Za-z0-9_]+",
                "ENGINE=" + preferred.getTextRepresentation());
        execute(rewritten);
        recordSchemaCopySuccess();
    }

    private List<Column> insertableColumns(MariaDBTable base, List<Column> columns) throws SQLException {
        List<Column> result = new ArrayList<>();
        for (Column column : columns) {
            String sql = "SELECT EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '"
                    + state.getDatabaseName().replace("'", "''") + "' AND TABLE_NAME = '"
                    + base.getName().replace("'", "''") + "' AND COLUMN_NAME = '" + column.name.replace("'", "''")
                    + "'";
            try (Statement s = state.getConnection().createStatement(); ResultSet rs = s.executeQuery(sql)) {
                if (!rs.next() || !rs.getString(1).toUpperCase(Locale.ROOT).contains("GENERATED"))
                    result.add(column);
            }
        }
        return result;
    }

    private void insertProjection(String target, List<Column> columns, String source) throws Exception {
        if (columns.isEmpty())
            execute("INSERT INTO " + target + " (`" + ROW_ID + "`) SELECT `" + ROW_ID + "` FROM " + source);
        else
            execute("INSERT INTO " + target + " (`" + ROW_ID + "`, " + names(columns) + ") SELECT `" + ROW_ID + "`, "
                    + qualify("s", columns) + " FROM " + source + " s");
    }

    private MariaDBEngine createHelper(VpTables vp, String table, List<Column> columns, MariaDBEngine preferred,
            String logical) throws Exception {
        String ddl = "CREATE TABLE " + table + " (`" + ROW_ID + "` BIGINT NOT NULL PRIMARY KEY"
                + (columns.isEmpty() ? "" : ", " + definitions(columns)) + ") ENGINE="
                + preferred.getTextRepresentation();
        MariaDBEngine actualEngine;
        try {
            executeSetup(vp, ddl);
            actualEngine = preferred;
        } catch (Throwable e) {
            if (preferred == MariaDBEngine.INNO_DB || !isEngineKeyLimitFailure(e)) {
                rethrow(e);
            }
            executeSetup(vp, ddl.replace("ENGINE=" + preferred.getTextRepresentation(), "ENGINE=InnoDB"));
            logFallback(logical, preferred, rootCauseMessage(e));
            actualEngine = MariaDBEngine.INNO_DB;
        }
        increment(ROW_ID_KEY_COUNTS, logical);
        return actualEngine;
    }

    private boolean isEngineKeyLimitFailure(Throwable throwable) {
        String message = errorText(throwable).toLowerCase(Locale.ROOT);
        return message.contains("specified key was too long") && message.contains("max key length");
    }

    private static void rethrow(Throwable throwable) throws Exception {
        if (throwable instanceof Exception) {
            throw (Exception) throwable;
        }
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        throw new AssertionError(throwable);
    }

    private static String rootCauseMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        String message = current.getMessage();
        return message == null ? "unknown" : message.replaceAll("\\(conn=\\d+\\)\\s*", "").trim();
    }

    private void copySchemaObjects(String base, String source, String prefix) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSourceCopySchemaObjects)
            return;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("SHOW CREATE TABLE " + quote(base))) {
            if (!rs.next())
                return;
            String create = rs.getString(2);
            for (String element : topLevelElements(create)) {
                String normalized = element.trim().toUpperCase(Locale.ROOT);
                if (normalized.startsWith("PRIMARY KEY") || normalized.startsWith("KEY ")
                        || normalized.startsWith("INDEX ") || normalized.startsWith("UNIQUE KEY")
                        || normalized.startsWith("UNIQUE INDEX") || normalized.startsWith("CONSTRAINT")) {
                    String rewritten = rewriteObjectName(element, prefix);
                    try {
                        execute("ALTER TABLE " + source + " ADD " + rewritten);
                    } catch (Exception e) {
                        logSchemaCopyFailure(source, element, e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            logSchemaCopyFailure(source, "SHOW CREATE TABLE", e.getMessage());
        }
    }

    private List<String> topLevelElements(String ddl) {
        int open = ddl.indexOf('('), close = ddl.lastIndexOf(')');
        if (open < 0 || close <= open)
            return List.of();
        String body = ddl.substring(open + 1, close);
        List<String> elements = new ArrayList<>();
        int depth = 0, start = 0;
        char quote = 0;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (quote != 0) {
                if (c == quote && (i == 0 || body.charAt(i - 1) != '\\'))
                    quote = 0;
                continue;
            }
            if (c == '\'' || c == '`' || c == '"') {
                quote = c;
                continue;
            }
            if (c == '(')
                depth++;
            else if (c == ')')
                depth--;
            else if (c == ',' && depth == 0) {
                elements.add(body.substring(start, i).trim());
                start = i + 1;
            }
        }
        elements.add(body.substring(start).trim());
        return elements;
    }

    private String rewriteObjectName(String element, String prefix) {
        String value = element.trim();
        if (value.toUpperCase(Locale.ROOT).startsWith("PRIMARY KEY"))
            return value;
        int open = value.indexOf('(');
        if (open < 0)
            return value;
        String upper = value.toUpperCase(Locale.ROOT);
        String name = "`" + prefix + "_idx_" + Math.abs(value.hashCode()) + "`";
        if (upper.startsWith("CONSTRAINT")) {
            int first = value.indexOf(' '), second = first < 0 ? -1 : value.indexOf(' ', first + 1);
            return second < 0 ? value : "CONSTRAINT " + "`" + prefix + "_chk_" + Math.abs(value.hashCode()) + "`"
                    + value.substring(second);
        }
        if (upper.startsWith("UNIQUE KEY"))
            return "UNIQUE KEY " + name + value.substring(open);
        if (upper.startsWith("UNIQUE INDEX"))
            return "UNIQUE INDEX " + name + value.substring(open);
        if (upper.startsWith("KEY"))
            return "KEY " + name + value.substring(open);
        if (upper.startsWith("INDEX"))
            return "INDEX " + name + value.substring(open);
        return value;
    }

    private String relation(VpTables vp, ReconstructionStrategy strategy, List<String> setup) throws Exception {
        String projection = projection(vp, "l", "r", false);
        String on = "l.`" + ROW_ID + "` = r.`" + ROW_ID + "`";
        switch (strategy) {
        case JOIN:
            return subquery(projection, " FROM " + vp.left + " l JOIN " + vp.right + " r ON " + on, vp.source);
        case LEFT_RIGHT_JOIN:
            return subquery(projection,
                    Randomly.getBoolean() ? " FROM " + vp.left + " l LEFT JOIN " + vp.right + " r ON " + on
                            : " FROM " + vp.left + " l RIGHT JOIN " + vp.right + " r ON " + on,
                    vp.source);
        case FULL_OUTER_SIMULATION:
            // MariaDB has no FULL OUTER JOIN. Materialize its two disjoint
            // branches explicitly. Do not use CTAS here: MariaDB turns ENUM
            // and SET expressions into plain strings, changing later predicate
            // semantics on the reconstructed relation.
            String typedProjection = typedInsertionProjection(vp, "l", "r", false);
            String rightOnly = typedRightOnlyProjection(vp, "r");
            setup.add("DROP TEMPORARY TABLE IF EXISTS " + vp.temp);
            setup.add(temporaryReconstructionTable(vp));
            setup.add("INSERT INTO " + vp.temp + " SELECT " + typedProjection + " FROM " + vp.left + " l LEFT JOIN "
                    + vp.right + " r ON " + on + " UNION ALL SELECT " + rightOnly + " FROM " + vp.right
                    + " r LEFT JOIN " + vp.left + " l ON " + on + " WHERE l.`" + ROW_ID + "` IS NULL");
            return vp.temp + " " + vp.source;
        case SCALAR_SUBQUERY:
            return typedScalarRelation(vp, setup, "");
        case EXISTS_FILTER:
            return typedScalarRelation(vp, setup,
                    " WHERE EXISTS (SELECT 1 FROM " + vp.right + " r WHERE r.`" + ROW_ID + "` = l.`" + ROW_ID + "`)");
        case IN_FILTER:
            return typedScalarRelation(vp, setup,
                    " WHERE l.`" + ROW_ID + "` IN (SELECT `" + ROW_ID + "` FROM " + vp.right + ")");
        case NOT_EXISTS_ANTI:
            setup.add("DROP TEMPORARY TABLE IF EXISTS " + vp.temp);
            setup.add(temporaryReconstructionTable(vp));
            setup.add("INSERT INTO " + vp.temp + " SELECT " + typedInsertionProjection(vp, "l", "r", false) + " FROM "
                    + vp.left + " l JOIN " + vp.right + " r ON " + on + " UNION ALL SELECT "
                    + typedRightOnlyProjection(vp, "r") + " FROM " + vp.right + " r WHERE NOT EXISTS (SELECT 1 FROM "
                    + vp.left + " l WHERE l.`" + ROW_ID + "` = r.`" + ROW_ID + "`)");
            return vp.temp + " " + vp.source;
        case DERIVED_TABLE:
            return subquery(projection,
                    " FROM (SELECT * FROM " + vp.left + ") l JOIN (SELECT * FROM " + vp.right + ") r ON " + on,
                    vp.source);
        case CTE:
            return "(WITH l AS (SELECT * FROM " + vp.left + "), r AS (SELECT * FROM " + vp.right + ") SELECT "
                    + projection + " FROM l JOIN r ON " + on + ") " + vp.source;
        case VIEW:
            setup.add("DROP VIEW IF EXISTS " + vp.view);
            setup.add("CREATE VIEW " + vp.view + " AS SELECT " + projection + " FROM " + vp.left + " l JOIN " + vp.right
                    + " r ON " + on);
            return vp.view + " " + vp.source;
        case TEMP_TABLE:
            setup.add("DROP TEMPORARY TABLE IF EXISTS " + vp.temp);
            setup.add("CREATE TEMPORARY TABLE " + vp.temp + " AS SELECT " + projection + " FROM " + vp.left + " l JOIN "
                    + vp.right + " r ON " + on);
            return vp.temp + " " + vp.source;
        case AGGREGATE_THEN_MERGE:
            return subquery(projection,
                    " FROM " + vp.left + " l JOIN (SELECT `" + ROW_ID + "`, COUNT(*) AS vp_count FROM " + vp.right
                            + " GROUP BY `" + ROW_ID + "`) g ON g.`" + ROW_ID + "` = l.`" + ROW_ID + "` JOIN "
                            + vp.right + " r ON r.`" + ROW_ID + "` = g.`" + ROW_ID + "`",
                    vp.source);
        case ROW_NUMBER_ALIGNMENT:
            return "(WITH l AS (SELECT ROW_NUMBER() OVER (ORDER BY `" + ROW_ID + "`) rn, "
                    + helperSelectList(vp.plan.left) + " FROM " + vp.left
                    + "), r AS (SELECT ROW_NUMBER() OVER (ORDER BY `" + ROW_ID + "`) rn, "
                    + helperSelectList(vp.plan.right) + " FROM " + vp.right + ") SELECT " + projection
                    + " FROM l JOIN r ON l.rn = r.rn) " + vp.source;
        case JSON_PACK:
            return subquery(jsonPackedProjection(vp), " FROM " + vp.left + " l JOIN (SELECT `" + ROW_ID
                    + "`, JSON_OBJECT(" + jsonPackArguments(vp) + ") payload FROM " + vp.right
                    + ") p ON p.`" + ROW_ID + "` = l.`" + ROW_ID + "`", vp.source);
        case STRING_PACK:
            return subquery(projection,
                    " FROM " + vp.left + " l JOIN (SELECT `" + ROW_ID + "`, CONCAT('vp:', `" + ROW_ID
                            + "`) payload FROM " + vp.right + ") p ON p.payload = CONCAT('vp:', l.`" + ROW_ID
                            + "`) JOIN " + vp.right + " r ON r.`" + ROW_ID + "` = p.`" + ROW_ID + "`",
                    vp.source);
        case NATURAL_OR_USING:
            return subquery(usingProjection(vp),
                    " FROM " + vp.left + " l JOIN " + vp.right + " r USING (`" + ROW_ID + "`)", vp.source);
        case STRAIGHT_JOIN:
            return subquery(projection, " FROM " + vp.left + " l STRAIGHT_JOIN " + vp.right + " r ON " + on, vp.source);
        // MariaDB does not accept SQL-standard JOIN LATERAL. Exercise its
        // semijoin-derived equivalent instead of claiming LATERAL syntax.
        case LATERAL_DERIVED:
            return subquery(projection, " FROM " + vp.left + " l JOIN " + vp.right + " r ON " + on
                    + " AND r.`" + ROW_ID + "` = (SELECT MIN(r2.`" + ROW_ID + "`) FROM " + vp.right
                    + " r2 WHERE r2.`" + ROW_ID + "` = l.`" + ROW_ID + "`)", vp.source);
        default:
            throw new AssertionError(strategy);
        }
    }

    private String jsonPackArguments(VpTables vp) {
        List<String> args = new ArrayList<>();
        for (Column c : vp.plan.right) {
            args.add("'" + c.name + "'");
            args.add("r.`" + c.name + "`");
        }
        return args.isEmpty() ? "'vp_dummy', NULL" : String.join(", ", args);
    }

    private String jsonPackedProjection(VpTables vp) {
        List<String> expressions = new ArrayList<>();
        for (Column c : vp.plan.all) {
            String value;
            if (contains(vp.plan.right, c.name)) {
                value = "JSON_UNQUOTE(JSON_EXTRACT(p.payload, '$." + c.name + "'))";
                if (c.type == MariaDBDataType.INT || c.type == MariaDBDataType.REAL
                        || c.type == MariaDBDataType.DECIMAL || c.type == MariaDBDataType.BOOLEAN) {
                    value = "CAST(" + value + " AS DECIMAL(65,10))";
                }
            } else {
                value = "l.`" + c.name + "`";
            }
            expressions.add(value + " AS `" + c.name + "`");
        }
        return expressions.isEmpty() ? "l.`" + ROW_ID + "`" : String.join(", ", expressions);
    }

    private String temporaryReconstructionTable(VpTables vp) {
        return "CREATE TEMPORARY TABLE " + vp.temp + " (`" + ROW_ID + "` BIGINT NOT NULL PRIMARY KEY"
                + (vp.plan.all.isEmpty() ? "" : ", " + definitions(vp.plan.all)) + ")";
    }

    private String typedScalarRelation(VpTables vp, List<String> setup, String filter) {
        // Scalar subqueries expose ENUM/SET values as strings. Inserting the
        // scalar result into the original declared types restores the source
        // column semantics while retaining scalar/semijoin execution.
        setup.add("DROP TEMPORARY TABLE IF EXISTS " + vp.temp);
        setup.add(temporaryReconstructionTable(vp));
        setup.add("INSERT INTO " + vp.temp + " SELECT " + scalarProjection(vp) + " FROM " + vp.left + " l" + filter);
        return vp.temp + " " + vp.source;
    }

    private QueryPair generateQueryPair(VpTables vp, ReconstructionStrategy strategy) throws Exception {
        int setupStart = vp.setupStatements.size();
        List<String> setup = new ArrayList<>();
        String relation = relation(vp, strategy, setup);
        for (String sql : setup)
            executeSetup(vp, sql);
        String actualPlan = actualReconstructionPlan(strategy, relation, setup);
        if (advancedEnabled() && Randomly.getBoolean()) {
            incrementQueryShape("primary-ast-query");
            incrementQueryShape("specialized");
            incrementStrategyQueryShape(strategy);
            QueryPair pair = generateAstQueryPair(vp, strategy, relation, actualPlan);
            return withProfileSetup(pair, new ArrayList<>(vp.setupStatements.subList(setupStart,
                    vp.setupStatements.size())));
        }
        incrementQueryShape("primary-non-ast-query");
        List<Column> selected = new ArrayList<>();
        for (Column c : vp.plan.all) {
            if (selected.isEmpty() || Randomly.getBoolean())
                selected.add(c);
        }
        // Keep the simple projection as a minority path. MariaDB optimizer-sensitive
        // expressions are generated against both relations so equivalence remains testable.
        boolean specialized = state.getRandomly().getInteger(0, 99) < 70;
        String projection = specialized ? mariaDbSpecializedProjection(vp.plan.all, strategy)
                : queryProjection(selected, strategy);
        incrementQueryShape(specialized ? "specialized" : "simple");
        incrementStrategyQueryShape(strategy);
        String direct = "SELECT " + projection + " FROM " + vp.source;
        String rewritten = direct.replace(" FROM " + vp.source, " FROM " + relation);
        if (Randomly.getBoolean()) {
            String predicate = predicate(selected, strategy);
            direct += " WHERE " + predicate;
            rewritten += " WHERE " + predicate;
        }
        if (advancedEnabled() && !selected.isEmpty() && Randomly.getBoolean()) {
            Column group = selected.get(0);
            String groupColumn = queryColumn(group);
            // Selecting a bare grouping key is not deterministic when the column collation
            // considers multiple spellings equal (for example, 'c' and 'C'). Use a
            // deterministic aggregate representative while retaining the grouping test.
            String representative;
            if (group.type == MariaDBDataType.VARCHAR) {
                representative = "MIN(CAST(" + groupColumn + " AS BINARY))";
            } else if (group.type == MariaDBDataType.BIT) {
                // MIN(BIT) is returned as storage bytes for a derived base
                // relation but decimal text for a materialized table. Keep the
                // native BIT grouping key and normalize only its representative.
                representative = "MIN(" + groupColumn + " + 0)";
            } else {
                representative = "MIN(" + groupColumn + ")";
            }
            direct = "SELECT " + representative + ", COUNT(*) FROM " + vp.source + " GROUP BY " + groupColumn
                    + (Randomly.getBoolean() ? " WITH ROLLUP" : "");
            rewritten = direct.replace(" FROM " + vp.source, " FROM " + relation);
            incrementQueryShape("group-by");
        } else if (deepEnabled() && !selected.isEmpty() && state.getRandomly().getInteger(0, 99) < 35) {
            String orderColumn = queryColumn(selected.get(0));
            direct = "SELECT " + orderColumn + ", ROW_NUMBER() OVER (ORDER BY `" + ROW_ID + "`) FROM " + vp.source;
            rewritten = direct.replace(" FROM " + vp.source, " FROM " + relation);
            incrementQueryShape("window");
        }
        if (advancedEnabled() && state.getRandomly().getInteger(0, 99) < 35) {
            direct = "SELECT * FROM (" + direct + " UNION ALL " + direct + ") u";
            rewritten = "SELECT * FROM (" + rewritten + " UNION ALL " + rewritten + ") u";
            incrementQueryShape("union");
        }
        if (advancedEnabled() && Randomly.getBoolean()) {
            direct = "SELECT * FROM (" + direct + ") q ORDER BY 1";
            rewritten = "SELECT * FROM (" + rewritten + ") q ORDER BY 1";
            incrementQueryShape("projection-sort");
        }
        return new QueryPair(vp, strategy, direct, rewritten, strategy.name(), false, actualPlan,
                new ArrayList<>(vp.setupStatements.subList(setupStart, vp.setupStatements.size())));
    }

    private QueryPair withProfileSetup(QueryPair pair, List<String> setup) {
        return new QueryPair(pair.vp, pair.strategy, pair.direct, pair.rewritten, pair.description,
                pair.orderedComparison, pair.actualPlan, setup);
    }

    private List<QueryPair> generateSupplementaryQueryPairs(VpTables vp, ReconstructionStrategy strategy)
            throws Exception {
        if (!advancedEnabled())
            return List.of();
        boolean narrowSort = state.getRandomly().getInteger(0, 100) < 5;
        boolean boundedPad = state.getRandomly().getInteger(0, 100) < 5;
        List<Column> specialColumns = vp.plan.all.stream()
                .filter(c -> c.type == MariaDBDataType.UUID || c.type == MariaDBDataType.INET6)
                .collect(Collectors.toList());
        boolean invalidType = !specialColumns.isEmpty() && state.getRandomly().getInteger(0, 100) < 5;
        boolean nativeFunctions = !specialColumns.isEmpty() && state.getRandomly().getInteger(0, 100) < 5;
        if (!narrowSort && !boundedPad && !invalidType && !nativeFunctions)
            return List.of();
        int setupStart = vp.setupStatements.size();
        List<String> setup = new ArrayList<>();
        String relation = relation(vp, strategy, setup);
        for (String sql : setup)
            executeSetup(vp, sql);
        String actualPlan = actualReconstructionPlan(strategy, relation, setup);
        List<String> profileSetup = new ArrayList<>(vp.setupStatements.subList(setupStart, vp.setupStatements.size()));
        List<QueryPair> pairs = new ArrayList<>();
        if (narrowSort) {
            String projection = "SELECT `" + ROW_ID + "` FROM ";
            String suffix = " ORDER BY `" + ROW_ID + "` LIMIT 32";
            pairs.add(new QueryPair(vp, strategy, projection + vp.source + suffix, projection + relation + suffix,
                    "Additional narrow row-id sort", true, actualPlan, profileSetup));
            incrementQueryShape("additional-narrow-key-sort");
        }
        if (boundedPad) {
            QueryPair pair = generateAstQueryPair(vp, strategy, relation, actualPlan, true);
            pairs.add(withProfileSetup(pair, profileSetup));
            incrementQueryShape("additional-bounded-pad-ast");
        }
        if (invalidType) {
            Column column = Randomly.fromList(specialColumns);
            String projection = "SELECT " + quote(column.name) + " - 1 FROM ";
            pairs.add(new QueryPair(vp, strategy, projection + vp.source, projection + relation,
                    "Additional illegal native-type probe", false, actualPlan, profileSetup));
            recordAstPolicy(strategy, "projection:0", "INTENTIONAL_INVALID_TYPE", column.type.name(),
                    "ADDITIONAL_ERROR_PROBE", projection);
            incrementQueryShape("additional-invalid-type");
        }
        if (nativeFunctions) {
            List<String> expressions = new ArrayList<>();
            for (Column column : specialColumns) {
                MariaDBColumn astColumn = new MariaDBColumn(column.name, column.type, false, 0, column.definition,
                        null, null);
                expressions.add(sqlancer.mariadb.ast.MariaDBVisitor.asString(typeDirectedAstProjection(astColumn)));
            }
            String projection = "SELECT " + String.join(", ", expressions) + " FROM ";
            pairs.add(new QueryPair(vp, strategy, projection + vp.source, projection + relation,
                    "Additional native-type functions", false, actualPlan, profileSetup));
            incrementQueryShape("additional-native-functions");
        }
        return pairs;
    }

    private static void incrementQueryShape(String shape) {
        synchronized (STATS_LOCK) {
            QUERY_SHAPE_COUNTS.put(shape, QUERY_SHAPE_COUNTS.getOrDefault(shape, 0) + 1);
        }
    }

    private static void incrementStrategyQueryShape(ReconstructionStrategy strategy) {
        switch (strategy) {
        case DERIVED_TABLE:
        case LATERAL_DERIVED:
            incrementQueryShape("derived");
            break;
        case SCALAR_SUBQUERY:
            incrementQueryShape("scalar");
            break;
        default:
            break;
        }
    }

    private static String actualReconstructionPlan(ReconstructionStrategy requested, String relation,
            List<String> setup) {
        String actual;
        String upper = (String.join(" ", setup) + " " + relation).toUpperCase(Locale.ROOT);
        if (upper.contains("UNION ALL"))
            actual = requested == ReconstructionStrategy.NOT_EXISTS_ANTI ? "UNION_NOT_EXISTS_ANTI" : "UNION_FULL_OUTER";
        else if (upper.contains("NOT EXISTS"))
            actual = "NOT_EXISTS_FILTERED";
        else if (upper.contains("SELECT R0.*") && upper.contains(" IN (SELECT"))
            actual = "SEMIJOIN_DERIVED";
        else if (upper.contains("SELECT * FROM") && upper.contains("JOIN (SELECT * FROM"))
            actual = "DERIVED_JOIN";
        else if (upper.contains("SELECT R.") && upper.contains("WHERE EXISTS"))
            actual = "SCALAR_EXISTS";
        else if (upper.contains(" IN (SELECT"))
            actual = "IN_FILTER";
        else if (upper.contains(" STRAIGHT_JOIN "))
            actual = "STRAIGHT_JOIN";
        else if (requested == ReconstructionStrategy.LEFT_RIGHT_JOIN && upper.contains(" LEFT JOIN ")) {
            actual = "LEFT_OUTER_JOIN";
        } else if (requested == ReconstructionStrategy.LEFT_RIGHT_JOIN && upper.contains(" RIGHT JOIN ")) {
            actual = "RIGHT_OUTER_JOIN";
        } else if (upper.startsWith("(WITH "))
            actual = "CTE_OR_WINDOW";
        else if (upper.contains("CREATE VIEW"))
            actual = "VIEW";
        else if (requested == ReconstructionStrategy.JSON_PACK && !upper.contains("JSON_OBJECT(")) {
            actual = "PLAIN_JOIN_FALLBACK";
        } else if ((requested == ReconstructionStrategy.SCALAR_SUBQUERY
                || requested == ReconstructionStrategy.EXISTS_FILTER || requested == ReconstructionStrategy.IN_FILTER)
                && upper.contains(" JOIN "))
            actual = "PLAIN_JOIN_FALLBACK";
        else
            actual = requested.name();
        return actual;
    }

    private String mariaDbSpecializedProjection(List<Column> columns, ReconstructionStrategy strategy) {
        List<String> expressions = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            Column c = columns.get(i);
            increment(NATIVE_TYPE_PROJECTION_COUNTS, c.type.name());
            String q = queryColumn(c);
            String expression;
            if (c.type == MariaDBDataType.VARCHAR) {
                expression = Randomly.fromOptions("COALESCE(" + q + ", '')",
                        "NULLIF(TRIM(TRAILING ' ' FROM " + q + "), '')", "CONCAT(COALESCE(" + q + ", ''), _utf8mb4 '')",
                        "CHAR_LENGTH(COALESCE(" + q + ", ''))",
                        "CASE WHEN " + q + " IS NULL THEN '' ELSE RTRIM(" + q + ") END",
                        "CONVERT(COALESCE(" + q + ", '') USING utf8mb4) COLLATE utf8mb4_bin",
                        "CONVERT(COALESCE(" + q + ", ' ') USING utf8mb4) COLLATE utf8mb4_general_ci");
            } else if (c.type == MariaDBDataType.INT || c.type == MariaDBDataType.REAL
                    || c.type == MariaDBDataType.DECIMAL || c.type == MariaDBDataType.BOOLEAN) {
                // Keep numeric projections total for signed and unsigned values. Mixing
                // signed and unsigned BIGINT arithmetic can raise an out-of-range error
                // for ordinary negative values, producing skips instead of oracle data.
                expression = Randomly.fromOptions("COALESCE(" + q + ", 0)", "CAST(" + q + " AS CHAR)",
                        "ROUND(" + q + ", 0)", "(" + q + " << 1) | (" + q + " & 1)",
                        "(" + q + " ^ 1) + (" + q + " & 1)", "IF(" + q + " IS NULL, 0, " + q + ")",
                        "NULLIF(" + q + ", 0)");
            } else if (c.type == MariaDBDataType.JSON) {
                expression = Randomly.fromOptions(q, "JSON_VALID(" + q + ")", "JSON_TYPE(" + q + ")",
                        "JSON_EXTRACT(" + q + ", '$.vp')", "JSON_LENGTH(" + q + ")");
            } else if (c.type == MariaDBDataType.BIT) {
                expression = Randomly.fromOptions(q, "BIT_COUNT(" + q + ")", "CAST(" + q + " AS UNSIGNED)",
                        q + " IS NULL");
            } else if (c.type == MariaDBDataType.ENUM) {
                expression = Randomly.fromOptions(q, "FIELD(" + q + ", 'a', 'b', 'c')", q + " = 'a'",
                        "CHAR_LENGTH(" + q + ")");
            } else if (c.type == MariaDBDataType.SET) {
                expression = Randomly.fromOptions(q, "FIND_IN_SET('a', " + q + ")", q + " = 'a,b'",
                        "CHAR_LENGTH(" + q + ")");
            } else if (c.type == MariaDBDataType.UUID) {
                expression = Randomly.fromOptions(q, q + " IS NULL", q + " = '00000000-0000-4000-8000-000000000001'",
                        "CHAR_LENGTH(" + q + ")");
            } else if (c.type == MariaDBDataType.INET6) {
                expression = Randomly.fromOptions(q, q + " IS NULL", q + " = '2001:db8::1'", "IS_IPV6(" + q + ")");
            } else {
                expression = Randomly.fromOptions(q, q + " IS NULL", "CAST(" + q + " AS CHAR)");
            }
            expressions.add(expression + " AS " + quote("vp_expr_" + i));
        }
        if (expressions.isEmpty()) {
            return "`" + ROW_ID + "`";
        }
        return String.join(", ", expressions);
    }

    private QueryPair generateAstQueryPair(VpTables vp, ReconstructionStrategy strategy, String relation,
            String actualPlan) {
        return generateAstQueryPair(vp, strategy, relation, actualPlan, false);
    }

    private QueryPair generateAstQueryPair(VpTables vp, ReconstructionStrategy strategy, String relation,
            String actualPlan, boolean boundedPadLengths) {
        List<MariaDBColumn> astColumns = new ArrayList<>();
        for (Column c : vp.plan.all) {
            astColumns.add(new MariaDBColumn(c.name, c.type, false, 0, c.definition, null, null));
        }
        List<MariaDBColumn> generalColumns = new ArrayList<>();
        for (MariaDBColumn column : astColumns) {
            String reason = astColumnRestriction(column, usesScalarProjection(strategy));
            if (reason == null) {
                generalColumns.add(column);
            } else {
                recordAstPolicy(strategy, "operand:" + column.getName(), reason, column.getType().name(),
                        "TYPE_DIRECTED", column.getName());
            }
        }
        MariaDBExpressionGenerator gen = new MariaDBExpressionGenerator(state.getRandomly()).setColumns(generalColumns)
                .setStableStringFunctions(true).setTypeDirectedSpecialOperands(true).setBoundedPadLengths(boundedPadLengths);
        MariaDBSelectStatement direct = createAstSelect(vp.source, gen, astColumns, strategy);
        if (!astColumns.isEmpty() && Randomly.getBooleanWithSmallProbability()) {
            probeAstStringMetadata(vp, strategy, relation, Randomly.fromList(astColumns));
        }
        String directSql = direct.asString();
        increment(AST_POLICY_COUNTS, "QUERY_RETAINED:" + strategy);
        String rewrittenSql = directSql.replace(" FROM " + vp.source, " FROM " + relation);
        boolean ordered = false;
        boolean addOuterQuery = advancedEnabled() && Randomly.getBoolean();
        if (addOuterQuery) {
            // MariaDB requires unique names for derived-table columns. Expressions such as
            // c0 and (+ c0) have different text but the same implicit result name.
            String aliasedDirect = aliasAstOutputExpressions(directSql);
            if (aliasedDirect != null) {
                directSql = aliasedDirect;
                rewrittenSql = directSql.replace(" FROM " + vp.source, " FROM " + relation);
            } else {
                addOuterQuery = false;
            }
        }
        if (addOuterQuery) {
            String orderBy = stableAstOrderBy(directSql);
            if (orderBy != null) {
                directSql = "SELECT * FROM (" + directSql + ") q ORDER BY " + orderBy;
                rewrittenSql = "SELECT * FROM (" + rewrittenSql + ") q ORDER BY " + orderBy;
                incrementQueryShape(boundedPadLengths ? "additional-ast-projection-sort" : "ast-projection-sort");
            }
        }
        return new QueryPair(vp, strategy, directSql, rewrittenSql, "AST outer query, reconstruction=" + strategy,
                ordered, actualPlan);
    }

    static String astColumnRestriction(MariaDBColumn column, boolean scalar) {
        if (column.getType() == MariaDBDataType.UUID || column.getType() == MariaDBDataType.INET6)
            return "NATIVE_SPECIAL_TYPE";
        if (column.getColumnTypeDefinition() != null
                && column.getColumnTypeDefinition().toUpperCase(Locale.ROOT).contains("ZEROFILL"))
            return "ZEROFILL_METADATA";
        if (scalar && column.getType() == MariaDBDataType.BIT)
            return "SCALAR_BIT_METADATA";
        return null;
    }

    private void probeAstStringMetadata(VpTables vp, ReconstructionStrategy strategy, String relation,
            MariaDBColumn column) {
        String name = quote(column.getName());
        String expression = Randomly.fromOptions("CONCAT(" + name + ", '')", "LPAD(" + name + ", 8, '0')",
                "RPAD(" + name + ", 8, '0')");
        String direct = "SELECT " + expression + " FROM " + vp.source + " WHERE 1 = 0";
        String rewritten = "SELECT " + expression + " FROM " + relation + " WHERE 1 = 0";
        try {
            String sourceMetadata = astResultMetadata(direct);
            String reconstructedMetadata = astResultMetadata(rewritten);
            String outcome = sourceMetadata.equals(reconstructedMetadata) ? "METADATA_EQUAL" : "METADATA_DIFFERENT";
            recordAstPolicy(strategy, "metadata-probe", outcome, column.getType().name(), "PROBE_ONLY", expression);
            if (!sourceMetadata.equals(reconstructedMetadata)) {
                logLine("vp-ast-metadata.log", "strategy=" + strategy + " expression=" + expression + " type="
                        + column.getColumnTypeDefinition() + " source=" + sourceMetadata + " reconstructed="
                        + reconstructedMetadata + " direct=" + direct + " rewritten=" + rewritten);
            }
        } catch (SQLException e) {
            if (!isSkippableThrowable(e))
                throw new AssertionError("AST metadata probe failed: " + direct + " / " + rewritten, e);
            recordAstPolicy(strategy, "metadata-probe", skipCategory(e.getMessage()), column.getType().name(),
                    "PROBE_ERROR", expression);
        }
    }

    private String astResultMetadata(String query) throws SQLException {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet result = statement.executeQuery(query)) {
            ResultSetMetaData metadata = result.getMetaData();
            return "jdbcType=" + metadata.getColumnType(1) + ",sqlType=" + metadata.getColumnTypeName(1) + ",precision="
                    + metadata.getPrecision(1) + ",scale=" + metadata.getScale(1) + ",displaySize="
                    + metadata.getColumnDisplaySize(1) + ",signed=" + metadata.isSigned(1);
        }
    }

    private void recordAstPolicy(ReconstructionStrategy strategy, String position, String reason, String types,
            String policy, String expression) {
        String key = strategy + ":" + position + ":" + reason + ":" + types + ":" + policy;
        increment(AST_POLICY_COUNTS, key);
        synchronized (LOGGED_SKIP_SAMPLES) {
            if (!LOGGED_SKIP_SAMPLES.add("AST:" + key))
                return;
        }
        logLine("vp-ast-policy.log", key + " expression=" + expression);
    }

    private String aliasAstOutputExpressions(String sql) {
        int selectEnd = sql.indexOf(" FROM ");
        if (selectEnd < 0)
            return null;
        String body = sql.substring("SELECT ".length(), selectEnd).trim();
        List<String> expressions = topLevelElements("(" + body + ")");
        if (expressions.isEmpty() || expressions.stream().anyMatch(e -> e.trim().equals("*"))) {
            return null;
        }
        StringBuilder result = new StringBuilder("SELECT ");
        for (int i = 0; i < expressions.size(); i++) {
            if (i != 0)
                result.append(", ");
            result.append(expressions.get(i)).append(" AS `vp_ast_col_").append(i).append('`');
        }
        return result.append(sql.substring(selectEnd)).toString();
    }

    private String stableAstOrderBy(String sql) {
        int selectEnd = sql.indexOf(" FROM ");
        if (selectEnd < 0)
            return null;
        String body = sql.substring("SELECT ".length(), selectEnd).trim();
        List<String> expressions = topLevelElements("(" + body + ")");
        if (expressions.isEmpty() || expressions.stream().anyMatch(e -> e.trim().equals("*")))
            return null;
        List<String> positions = new ArrayList<>();
        for (int i = 1; i <= expressions.size(); i++)
            positions.add(Integer.toString(i));
        return String.join(", ", positions);
    }

    private MariaDBSelectStatement createAstSelect(String from, MariaDBExpressionGenerator gen,
            List<MariaDBColumn> columns, ReconstructionStrategy strategy) {
        MariaDBSelectStatement select = new MariaDBSelectStatement();
        select.setSelectType(Randomly.fromOptions(MariaDBSelectStatement.MariaDBSelectType.values()));
        MariaDBTable fromTable = new MariaDBTable(from, List.of(), List.<MariaDBIndex> of(), MariaDBEngine.INNO_DB);
        select.setFromList(List.of(new MariaDBTableReference(fromTable)));
        List<MariaDBExpression> fetch = new ArrayList<>();
        Set<String> selectedColumnNames = new HashSet<>();
        int nrExpressions = Math.max(1, Randomly.smallNumber() + 1);
        for (int i = 0; i < nrExpressions; i++) {
            if (columns.isEmpty()) {
                fetch.add(new MariaDBColumnName(MariaDBColumn.createDummy("*")));
            } else if (advancedEnabled() && Randomly.getBoolean()) {
                MariaDBExpression expression = normalizeBitAstProjection(gen.getRandomExpression(), columns);
                fetch.add(expression);
                recordAstExpression(strategy, "projection:" + i, expression, columns);
            } else {
                List<MariaDBColumn> available = new ArrayList<>();
                for (MariaDBColumn column : columns) {
                    if (!selectedColumnNames.contains(column.getName()))
                        available.add(column);
                }
                if (available.isEmpty()) {
                    MariaDBColumn column = Randomly.fromList(columns);
                    fetch.add(astColumnExpression(column));
                } else {
                    MariaDBColumn column = Randomly.fromList(available);
                    selectedColumnNames.add(column.getName());
                    fetch.add(astColumnExpression(column));
                }
                recordAstExpression(strategy, "projection:" + i, fetch.get(fetch.size() - 1), columns);
            }
        }
        select.setFetchColumns(fetch);
        if (!columns.isEmpty() && Randomly.getBoolean()) {
            MariaDBExpression predicate = gen.generateBooleanExpression();
            select.setWhereClause(predicate);
            recordAstExpression(strategy, "predicate", predicate, columns);
        }
        return select;
    }

    private MariaDBExpression typeDirectedAstProjection(MariaDBColumn column) {
        if (column.getType() != MariaDBDataType.UUID && column.getType() != MariaDBDataType.INET6)
            return astColumnExpression(column);
        MariaDBExpression expression = new MariaDBColumnName(column);
        switch (state.getRandomly().getInteger(0, 4)) {
        case 0:
            return expression;
        case 1:
            return new sqlancer.mariadb.ast.MariaDBText(expression, " IS NULL", false);
        case 2:
            return new sqlancer.mariadb.ast.MariaDBText(expression, " IS NOT NULL", false);
        default:
            String function = column.getType() == MariaDBDataType.UUID ? "CHAR_LENGTH(" : "IS_IPV6(";
            return new sqlancer.mariadb.ast.MariaDBText(
                    new sqlancer.mariadb.ast.MariaDBText(expression, function, true), ")", false);
        }
    }

    private void recordAstExpression(ReconstructionStrategy strategy, String position, MariaDBExpression expression,
            List<MariaDBColumn> columns) {
        String sql = sqlancer.mariadb.ast.MariaDBVisitor.asString(expression);
        String upper = sql.toUpperCase(Locale.ROOT);
        String types = columns.stream().filter(c -> referencesIdentifier(upper, c.getName()))
                .map(c -> c.getType().name()).distinct().collect(Collectors.joining("+"));
        for (MariaDBColumn column : columns) {
            if ("ZEROFILL_METADATA".equals(astColumnRestriction(column, false))
                    && referencesIdentifier(upper, column.getName())) {
                recordAstPolicy(strategy, position, "ZEROFILL_PROJECTION", column.getType().name(), "BARE_COLUMN", sql);
            }
        }
        if (upper.contains("LPAD(") || upper.contains("RPAD(") || upper.contains("CONCAT(")) {
            recordAstPolicy(strategy, position, "STRING_RESULT_METADATA", types, "NORMALIZE_INPUTS", sql);
        } else {
            increment(AST_POLICY_COUNTS, "EXPRESSION_RETAINED:" + strategy + ":" + position + ":" + types);
        }
    }

    private MariaDBExpression normalizeBitAstProjection(MariaDBExpression expression, List<MariaDBColumn> columns) {
        String sql = sqlancer.mariadb.ast.MariaDBVisitor.asString(expression).toUpperCase(Locale.ROOT);
        boolean referencesBit = columns.stream().anyMatch(
                column -> column.getType() == MariaDBDataType.BIT && referencesIdentifier(sql, column.getName()));
        if (!referencesBit) {
            return expression;
        }
        // Connector/J can expose the same BIT expression as incompatible text
        // depending on whether its input is a base field or a view/derived
        // field. Normalize only the final result boundary; the expression and
        // predicates still execute with native BIT semantics.
        MariaDBExpression prefixed = new sqlancer.mariadb.ast.MariaDBText(expression, "CAST(", true);
        return new sqlancer.mariadb.ast.MariaDBText(prefixed, " AS UNSIGNED)", false);
    }

    private MariaDBExpression astColumnExpression(MariaDBColumn column) {
        MariaDBExpression expression = new MariaDBColumnName(column);
        if (column.getType() == MariaDBDataType.BIT) {
            expression = new sqlancer.mariadb.ast.MariaDBText(expression, " + 0", false);
        }
        return expression;
    }

    private String queryProjection(List<Column> columns, ReconstructionStrategy strategy) {
        if (columns.isEmpty())
            return "COUNT(*)";
        List<String> result = new ArrayList<>();
        for (Column c : columns) {
            String value = queryColumn(c);
            if (advancedEnabled() && Randomly.getBoolean()) {
                if (c.type == MariaDBDataType.INT || c.type == MariaDBDataType.REAL)
                    value = "COALESCE(" + value + ", 0)";
                else if (c.type == MariaDBDataType.VARCHAR)
                    value = "COALESCE(" + value + ", '')";
            }
            if (deepEnabled() && c.type == MariaDBDataType.VARCHAR && Randomly.getBooleanWithSmallProbability())
                value = "CONVERT(" + value + " USING utf8mb4) COLLATE utf8mb4_bin";
            result.add(value);
        }
        return String.join(", ", result);
    }

    private String predicate(List<Column> columns, ReconstructionStrategy strategy) {
        if (columns.isEmpty())
            return "1 = 1";
        Column c = Randomly.fromList(columns);
        String q = queryColumn(c);
        if (c.type == MariaDBDataType.VARCHAR)
            return q + " IS NULL OR CHAR_LENGTH(" + q + ") >= 0";
        if (c.type == MariaDBDataType.JSON)
            return q + " IS NULL OR JSON_VALID(" + q + ")";
        if (c.type == MariaDBDataType.BIT)
            return q + " IS NULL OR BIT_COUNT(" + q + ") >= 0";
        if (c.type == MariaDBDataType.ENUM || c.type == MariaDBDataType.SET || c.type == MariaDBDataType.UUID
                || c.type == MariaDBDataType.INET6 || c.type == MariaDBDataType.DATE
                || c.type == MariaDBDataType.DATETIME || c.type == MariaDBDataType.TIMESTAMP)
            return q + " IS NULL OR " + q + " = " + q;
        if (deepEnabled() && (c.type == MariaDBDataType.INT || c.type == MariaDBDataType.REAL)
                && Randomly.getBooleanWithSmallProbability())
            return quote(c.name) + " IN (0, 1, -1, 2147483647, -2147483648)";
        return q + " IS NULL OR " + q + " >= 0";
    }

    private String projection(VpTables vp, String leftAlias, String rightAlias, boolean rightBiasedRowId) {
        List<String> result = new ArrayList<>();
        result.add((rightBiasedRowId ? rightAlias : leftAlias) + ".`" + ROW_ID + "` AS `" + ROW_ID + "`");
        for (Column c : vp.plan.all) {
            boolean left = contains(vp.plan.left, c.name), right = contains(vp.plan.right, c.name);
            boolean overlap = contains(vp.plan.overlap, c.name);
            String alias = overlap ? (Randomly.getBoolean() ? leftAlias : rightAlias) : left ? leftAlias : rightAlias;
            result.add(
                    left || right ? alias + "." + quote(c.name) + " AS " + quote(c.name) : "NULL AS " + quote(c.name));
        }
        return String.join(", ", result);
    }

    private String scalarProjection(VpTables vp) {
        List<String> result = new ArrayList<>();
        result.add("l.`" + ROW_ID + "` AS `" + ROW_ID + "`");
        for (Column c : vp.plan.all) {
            if (contains(vp.plan.left, c.name))
                result.add("l." + quote(c.name) + " AS " + quote(c.name));
            else if (contains(vp.plan.right, c.name))
                result.add(scalarSubqueryValue(vp, c) + " AS " + quote(c.name));
            else
                result.add("NULL AS " + quote(c.name));
        }
        return String.join(", ", result);
    }

    private String scalarSubqueryValue(VpTables vp, Column column) {
        String value = "r." + quote(column.name);
        if (column.type == MariaDBDataType.BIT || column.type == MariaDBDataType.ENUM
                || column.type == MariaDBDataType.SET) {
            // A MariaDB scalar BIT subquery reports BIT metadata but sends a
            // decimal text payload through JDBC. ENUM/SET scalar results also
            // lose their ordinal metadata. Insert native numeric values into
            // the explicitly typed reconstruction table to preserve both.
            value = value + " + 0";
        }
        return "(SELECT " + value + " FROM " + vp.right + " r WHERE r.`" + ROW_ID + "` = l.`" + ROW_ID + "`)";
    }

    private boolean usesScalarProjection(ReconstructionStrategy strategy) {
        return strategy == ReconstructionStrategy.SCALAR_SUBQUERY || strategy == ReconstructionStrategy.EXISTS_FILTER
                || strategy == ReconstructionStrategy.IN_FILTER;
    }

    private String queryColumn(Column column) {
        String value = quote(column.name);
        return column.type == MariaDBDataType.BIT ? "(" + value + " + 0)" : value;
    }

    private String typedInsertionProjection(VpTables vp, String leftAlias, String rightAlias,
            boolean rightBiasedRowId) {
        List<String> result = new ArrayList<>();
        result.add((rightBiasedRowId ? rightAlias : leftAlias) + ".`" + ROW_ID + "` AS `" + ROW_ID + "`");
        for (Column c : vp.plan.all) {
            boolean left = contains(vp.plan.left, c.name), right = contains(vp.plan.right, c.name);
            boolean overlap = contains(vp.plan.overlap, c.name);
            String alias = overlap ? (Randomly.getBoolean() ? leftAlias : rightAlias) : left ? leftAlias : rightAlias;
            String value = left || right ? alias + "." + quote(c.name) : "NULL";
            if (c.type == MariaDBDataType.ENUM || c.type == MariaDBDataType.SET) {
                value = "(" + value + " + 0)";
            }
            result.add(value + " AS " + quote(c.name));
        }
        return String.join(", ", result);
    }

    private String typedRightOnlyProjection(VpTables vp, String alias) {
        List<String> result = new ArrayList<>();
        result.add(alias + ".`" + ROW_ID + "` AS `" + ROW_ID + "`");
        for (Column c : vp.plan.all) {
            String value = contains(vp.plan.right, c.name) ? alias + "." + quote(c.name) : "NULL";
            if (c.type == MariaDBDataType.ENUM || c.type == MariaDBDataType.SET) {
                value = "(" + value + " + 0)";
            }
            result.add(value + " AS " + quote(c.name));
        }
        return String.join(", ", result);
    }

    private String usingProjection(VpTables vp) {
        List<String> result = new ArrayList<>();
        result.add("`" + ROW_ID + "`");
        for (Column c : vp.plan.all) {
            if (contains(vp.plan.left, c.name))
                result.add("l." + quote(c.name) + " AS " + quote(c.name));
            else if (contains(vp.plan.right, c.name))
                result.add("r." + quote(c.name) + " AS " + quote(c.name));
            else
                result.add("NULL AS " + quote(c.name));
        }
        return String.join(", ", result);
    }

    private static String subquery(String projection, String from, String alias) {
        return "(SELECT " + projection + from + ") AS " + alias;
    }

    private boolean contains(List<Column> columns, String name) {
        for (Column c : columns)
            if (c.name.equalsIgnoreCase(name))
                return true;
        return false;
    }

    private static String helperSelectList(List<Column> columns) {
        List<String> result = new ArrayList<>();
        result.add(quote(ROW_ID));
        for (Column c : columns)
            result.add(quote(c.name));
        return String.join(", ", result);
    }

    private boolean compare(QueryPair pair, SplitStrategy split, ReconstructionStrategy strategy) throws Exception {
        QueryOutcome directOutcome = executeQueryOutcome(pair.direct);
        QueryOutcome rewrittenOutcome = executeQueryOutcome(pair.rewritten);
        // Profiles are an independent dimension, including when baseline queries fail.
        runOptimizerProfileChecks(pair);
        if (!directOutcome.succeeded() || !rewrittenOutcome.succeeded()) {
            compareQueryOutcomes(pair, directOutcome, rewrittenOutcome);
            return false;
        }
        QueryResultMultiset direct = directOutcome.result;
        QueryResultMultiset rewritten = rewrittenOutcome.result;
        boolean match = queryResultsMatch(pair, direct, rewritten);
        if (!match) {
            String known = classifyKnown(pair, strategy, direct, rewritten);
            // Only suppress known mismatches when explicitly requested. With
            // the option disabled, database*.log remains an audit stream for
            // reviewing even classified patterns.
            if (state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns && known != null) {
                increment(KNOWN_COUNTS, known);
                logKnownBug(pair, known, direct, rewritten);
                return true;
            }
            throw new AssertionError(formatBugReport(pair, direct, rewritten));
        }
        return true;
    }

    @SuppressWarnings("unused")
    private void compareBaseAndSource(QueryPair pair) throws SQLException {
        if (pair.direct.contains("`" + ROW_ID + "`") || pair.direct.contains(ROW_ID)) {
            return;
        }
        String sourceFrom = " FROM " + pair.vp.source;
        if (!pair.direct.contains(sourceFrom)) {
            increment(SKIP_COUNTS, "base-source: unsupported query shape");
            return;
        }
        String baseQuery = pair.direct.replace(sourceFrom, " FROM " + baseSnapshotRelation(pair.vp));
        QueryOutcome base = executeQueryOutcome(baseQuery);
        QueryOutcome source = executeQueryOutcome(pair.direct);
        if (!base.succeeded() || !source.succeeded()) {
            if (handleResourceOutcomes(pair, base, source, () -> executeQueryOutcome(baseQuery),
                    () -> executeQueryOutcome(pair.direct), "base-source")) {
                return;
            }
            if (base.succeeded() != source.succeeded() || !sameSqlException(base.error, source.error)) {
                throw new AssertionError("Base/source query outcome mismatch: base=" + outcomeText(base) + ", source="
                        + outcomeText(source) + "\nBase query: " + baseQuery + "\nSource query: " + pair.direct);
            }
            if (isSkippableThrowable(base.error)) {
                recordSkip(pair.strategy, base.error);
                return;
            }
            throw new AssertionError("Both base/source queries failed: " + outcomeText(base));
        }
        boolean match = pair.orderedComparison ? orderedRowsMatch(base.result, source.result)
                : resultMultisetsMatch(base.result, source.result);
        if (!match && isDistinctQuery(pair.direct)
                && distinctCharacterRepresentativesMatch(pair, base.result, source.result)) {
            // DISTINCT under an accent/case-insensitive, PAD SPACE collation
            // may choose different representatives for the same equivalence
            // class after materialization (e.g. 'Case' vs 'case', ' ' vs
            // ''). This is not a relation mismatch.
            match = true;
        }
        if (!match) {
            // A ZEROFILL column can be rendered with its display padding in
            // the temporary base snapshot and without it after the INSERT
            // into the materialized source. This is a representation change,
            // not a change of the stored value. Keep this check cell-based so
            // an unrelated base/source mismatch is still reported.
            if (hasZeroFillColumnReferenced(pair) && hasOnlyExpectedCellDifferences(pair, base.result, source.result,
                    (left, right, kind, column) -> isZeroFillPaddingDifference(left, right))) {
                return;
            }
            String known = classifyKnownBaseSource(pair, base.result, source.result, baseQuery);
            if (state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns && known != null) {
                increment(KNOWN_COUNTS, known);
                try {
                    logKnownBug(pair, known, base.result, source.result);
                } catch (SQLException e) {
                    throw new AssertionError(e);
                }
                return;
            }
            throw new AssertionError("Base/source snapshot mismatch\nBase query: " + baseQuery + "\nSource query: "
                    + pair.direct + "\nBase sample: " + sample(base.result.rows) + "\nSource sample: "
                    + sample(source.result.rows));
        }
    }

    /**
     * MDEV-41117 also appears before the rewrite comparison, when the
     * temporary base snapshot scans a native scalar column but the materialized
     * source uses a generated ref index for the same quoted string predicate.
     * The known family includes historical BIT/string, temporal/string, and
     * UUID/JSON-like BIT cases. The database7 NOT IN/NULL range-scan case is
     * classified separately as MDEV-38834. Keep this separate from
     * the rewrite classifier: it is only valid when the base/source result sets
     * differ and EXPLAIN proves ALL versus ref.
     */
    private String classifyKnownBaseSource(QueryPair pair, QueryResultMultiset base, QueryResultMultiset source,
            String baseQuery) {
        if (base.rows.isEmpty() && !source.rows.isEmpty()
                && hasIntegerDecimalStringInPredicate(pair)
                && sourceScanMatches(pair, base, null)) {
            return "MARIADB_MDEV_41081_BOOLEAN_IN_INDEX_RANGE";
        }
        if (!base.rows.isEmpty() && source.rows.isEmpty()
                && hasQuotedInPredicate(pair, false) && sourceScanMatches(pair, base, null)) {
            return "MARIADB_MDEV_41117_BIT_STRING_IN_INDEX_RANGE";
        }
        if (!isMdev41117StringIndexBaseSource(pair, base, source, baseQuery)) {
            return null;
        }
        return "MARIADB_MDEV_41117_BIT_STRING_IN_INDEX_RANGE";
    }

    private boolean hasIntegerDecimalStringInPredicate(QueryPair pair) {
        String sql = pair.direct.toUpperCase(Locale.ROOT);
        for (Column column : pair.vp.plan.all) {
            if (isIntegerStorageType(column) && Pattern.compile("`?" + Pattern.quote(column.name.toUpperCase(Locale.ROOT))
                    + "`?\\s+IN\\s*\\(\\s*'[-+]?\\d+\\.\\d+'\\s*\\)").matcher(sql).find()) {
                return true;
            }
        }
        return false;
    }

    private boolean hasQuotedInPredicate(QueryPair pair, boolean bitOnly) {
        String sql = pair.direct.toUpperCase(Locale.ROOT);
        for (Column column : pair.vp.plan.all) {
            if ((bitOnly ? column.type == MariaDBDataType.BIT : isMdev41117StringIndexType(column))
                    && Pattern.compile("`?" + Pattern.quote(column.name.toUpperCase(Locale.ROOT))
                            + "`?\\s+IN\\s*\\([^()]*'[^']+'[^()]*\\)").matcher(sql).find()) {
                return true;
            }
        }
        return false;
    }

    // Confirm the conversion family by changing only indexed access to the source.
    private boolean sourceScanMatches(QueryPair pair, QueryResultMultiset expected, String profile) {
        Connection connection = profileConnection(profile, false);
        if (connection == null) {
            return false;
        }
        List<String> indexes = new ArrayList<>();
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SHOW INDEX FROM " + pair.vp.source)) {
            while (rs.next()) {
                String name = rs.getString("Key_name");
                if (!indexes.contains(name)) {
                    indexes.add(name);
                }
            }
        } catch (SQLException e) {
            return false;
        }
        String from = " FROM " + pair.vp.source;
        if (indexes.isEmpty() || !pair.direct.contains(from)) {
            return false;
        }
        String scan = pair.direct.replace(from, from + " IGNORE INDEX ("
                + indexes.stream().map(MariaDBVerticalPartitioningOracle::quote).collect(Collectors.joining(", ")) + ")");
        try {
            QueryResultMultiset result = getResultMultiset(connection, scan, false);
            return pair.orderedComparison ? orderedRowsMatch(expected, result) : resultMultisetsMatch(expected, result);
        } catch (SQLException e) {
            return false;
        }
    }
    private boolean isMdev41117StringIndexBaseSource(QueryPair pair, QueryResultMultiset base,
            QueryResultMultiset source, String baseQuery) {
        if (base.rows.isEmpty() || !source.rows.isEmpty()) {
            return false;
        }
        String sql = pair.direct.toUpperCase(Locale.ROOT);
        java.util.regex.Matcher matcher = Pattern.compile(
                "(?:'([^']+)'\\s+IN\\s*\\(\\s*`?([A-Z_][A-Z0-9_$]*)`?\\s*\\)"
                        + "|`?([A-Z_][A-Z0-9_$]*)`?\\s+IN\\s*\\(\\s*'([^']+)'\\s*\\))")
                .matcher(sql);
        if (!matcher.find()) {
            return false;
        }
        String columnName = matcher.group(2) != null ? matcher.group(2) : matcher.group(3);
        Column bitColumn = null;
        for (Column column : pair.vp.plan.all) {
            if (column.type == MariaDBDataType.BIT && column.name.equalsIgnoreCase(columnName)
                    && referencesIdentifier(sql, column.name)) {
                bitColumn = column;
                break;
            }
        }
        if (bitColumn == null || !isMdev41117StringIndexType(bitColumn)
                || !hasSourceIndexContainingColumn(pair, bitColumn.name)) {
            return false;
        }
        try (Statement statement = state.getConnection().createStatement();
                ResultSet baseExplain = statement.executeQuery("EXPLAIN " + baseQuery)) {
            boolean baseAll = false;
            while (baseExplain.next()) {
                String table = stripIdentifierQuotes(baseExplain.getString("table"));
                if (table != null && table.equalsIgnoreCase(stripIdentifierQuotes(pair.vp.baseSnapshot))) {
                    baseAll = "ALL".equalsIgnoreCase(baseExplain.getString("type"));
                    break;
                }
            }
            if (!baseAll) {
                return false;
            }
        } catch (SQLException ignored) {
            return false;
        }
        try (Statement statement = state.getConnection().createStatement();
                ResultSet sourceExplain = statement.executeQuery("EXPLAIN " + pair.direct)) {
            boolean sawSourcePlan = false;
            while (sourceExplain.next()) {
                String table = stripIdentifierQuotes(sourceExplain.getString("table"));
                String key = stripIdentifierQuotes(sourceExplain.getString("key"));
                if (table != null && table.equalsIgnoreCase(stripIdentifierQuotes(pair.vp.source))) {
                    sawSourcePlan = true;
                }
                if (table != null && table.equalsIgnoreCase(stripIdentifierQuotes(pair.vp.source))
                        && key != null && key.startsWith(pair.vp.prefix + "idx_src_")) {
                    return true;
                }
            }
            /*
             * MariaDB may fold an impossible temporal literal predicate to a
             * NULL/impossible plan, so EXPLAIN no longer reports the source
             * table even though the source index was considered.  The
             * result-direction check plus the source index metadata is the
             * stable evidence in that form.
             */
            if (!sawSourcePlan && bitColumn.type != MariaDBDataType.BIT) {
                return true;
            }
        } catch (SQLException ignored) {
            return false;
        }
        return false;
    }

    private boolean hasSourceIndexContainingColumn(QueryPair pair, String columnName) {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("SHOW INDEX FROM " + pair.vp.source)) {
            while (rs.next()) {
                String keyName = rs.getString("Key_name");
                String indexedColumn = rs.getString("Column_name");
                if (keyName != null && keyName.startsWith(pair.vp.prefix + "idx_src_")
                        && columnName.equalsIgnoreCase(indexedColumn)) {
                    return true;
                }
            }
        } catch (SQLException ignored) {
            return false;
        }
        return false;
    }

    private boolean hasSourceLeadingIndexContainingColumn(QueryPair pair, String columnName) {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("SHOW INDEX FROM " + pair.vp.source)) {
            while (rs.next()) {
                if (rs.getInt("Seq_in_index") == 1
                        && columnName.equalsIgnoreCase(rs.getString("Column_name"))) {
                    return true;
                }
            }
        } catch (SQLException ignored) {
            return false;
        }
        return false;
    }

    private boolean isMdev41117StringIndexType(Column column) {
        return column.type == MariaDBDataType.BIT || column.type == MariaDBDataType.DATE
                || column.type == MariaDBDataType.DATETIME || column.type == MariaDBDataType.TIMESTAMP
                || column.type == MariaDBDataType.JSON;
    }

    private QueryOutcome executeQueryOutcome(String sql) {
        try {
            return new QueryOutcome(getResultMultiset(sql), null);
        } catch (Throwable throwable) {
            return new QueryOutcome(null, throwable);
        }
    }

    private void compareQueryOutcomes(QueryPair pair, QueryOutcome direct, QueryOutcome rewritten) throws SQLException {
        if (handleResourceOutcomes(pair, direct, rewritten, () -> executeQueryOutcome(pair.direct),
                () -> executeQueryOutcome(pair.rewritten), "baseline")) {
            return;
        }
        if (direct.succeeded() != rewritten.succeeded()) {
            throw new AssertionError(
                    "VP query outcome mismatch: direct=" + outcomeText(direct) + ", rewritten=" + outcomeText(rewritten)
                            + "\nDirect query: " + pair.direct + "\nRewritten query: " + pair.rewritten);
        }
        if (sameSqlException(direct.error, rewritten.error)) {
            if (isSkippableThrowable(direct.error)) {
                recordSkip(pair.strategy, direct.error);
                return;
            }
            throw new AssertionError("Both VP queries failed: " + outcomeText(direct));
        }
        throw new AssertionError("VP query error mismatch: direct=" + outcomeText(direct) + ", rewritten="
                + outcomeText(rewritten) + "\nDirect query: " + pair.direct + "\nRewritten query: " + pair.rewritten);
    }

    private boolean handleResourceOutcomes(QueryPair pair, QueryOutcome direct, QueryOutcome rewritten,
            Supplier<QueryOutcome> repeatDirect, Supplier<QueryOutcome> repeatRewritten, String context) throws SQLException {
        if (resourceLimitError(direct, rewritten) == null)
            return false;
        if (bothSameResourceError(direct, rewritten)) {
            increment(SKIP_COUNTS, pair.strategy + ":" + context + ":BOTH_SAME_RESOURCE_ERROR");
            recordSkip(pair.strategy, direct.error);
            return true;
        }
        increment(SKIP_COUNTS, pair.strategy + ":" + context + ":ASYMMETRIC_RESOURCE_CANDIDATE");
        logLine("vp-resource-candidates.log", "context=" + context + " strategy=" + pair.strategy + " direct="
                + outcomeText(direct) + " rewritten=" + outcomeText(rewritten) + " direct-sql=" + pair.direct
                + " rewritten-sql=" + pair.rewritten);
        for (int retry = 1; retry <= 2; retry++) {
            QueryOutcome repeatedDirect = repeatDirect.get();
            QueryOutcome repeatedRewritten = repeatRewritten.get();
            logLine("vp-resource-candidates.log", "context=" + context + " retry=" + retry + " direct="
                    + outcomeText(repeatedDirect) + " rewritten=" + outcomeText(repeatedRewritten)
                    + " direct-sql=" + pair.direct + " rewritten-sql=" + pair.rewritten);
            if (repeatedDirect.succeeded() && repeatedRewritten.succeeded()
                    && !queryResultsMatch(pair, repeatedDirect.result, repeatedRewritten.result)) {
                throw new AssertionError(formatBugReport(pair, repeatedDirect.result, repeatedRewritten.result));
            }
            if (resourceLimitError(repeatedDirect, repeatedRewritten) == null
                    && (!repeatedDirect.succeeded() || !repeatedRewritten.succeeded())) {
                compareQueryOutcomes(pair, repeatedDirect, repeatedRewritten);
                increment(SKIP_COUNTS, pair.strategy + ":" + context + ":RESOURCE_CANDIDATE_NOT_CONFIRMED");
                return true;
            }
            if (!sameOutcomeStatus(direct, repeatedDirect) || !sameOutcomeStatus(rewritten, repeatedRewritten)) {
                increment(SKIP_COUNTS, pair.strategy + ":" + context + ":RESOURCE_CANDIDATE_NOT_CONFIRMED");
                return true;
            }
        }
        increment(SKIP_COUNTS, pair.strategy + ":" + context + ":RESOURCE_ASYMMETRY_CONFIRMED");
        // A repeatable difference in memory requirements is a resource candidate, not proof of wrong rows.
        // Keep the original SQL, retries and session snapshots; continue testing the remaining profiles.
        String knownResource = classifyKnownResourceAsymmetry(pair, direct, rewritten, context,
                repeatDirect, repeatRewritten);
        if (knownResource != null && state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns) {
            increment(KNOWN_COUNTS, knownResource);
            logLine("vp-known-resource.log", "known=" + knownResource + " context=" + context
                    + " strategy=" + pair.strategy + " direct=" + outcomeText(direct)
                    + " rewritten=" + outcomeText(rewritten) + " direct-sql=" + pair.direct
                    + " rewritten-sql=" + pair.rewritten);
            logLine("vp-resource-candidates.log", "status=KNOWN_RESOURCE_ONLY known=" + knownResource
                    + " context=" + context + " strategy=" + pair.strategy + " direct=" + outcomeText(direct)
                    + " rewritten=" + outcomeText(rewritten) + " direct-sql=" + pair.direct
                    + " rewritten-sql=" + pair.rewritten);
        }
        logLine("vp-resource-candidates.log", "status=CONFIRMED_RESOURCE_ONLY context=" + context
                + " strategy=" + pair.strategy + " direct=" + outcomeText(direct) + " rewritten="
                + outcomeText(rewritten) + " direct-sql=" + pair.direct + " rewritten-sql=" + pair.rewritten);
        return true;
    }

    /**
     * Known MariaDB resource behavior: an empty native/reconstructed relation
     * can choose different empty-table plans. One side is folded to a const
     * empty result while the other still enters DISTINCT/GROUP BY/ORDER BY
     * filesort and raises ER_OUT_OF_SORTMEMORY (1038). This is intentionally
     * narrower than matching every resource error, and is only classified
     * after both retries confirmed the same asymmetric outcome.
     */
    private String classifyKnownResourceAsymmetry(QueryPair pair, QueryOutcome direct, QueryOutcome rewritten,
            String context, Supplier<QueryOutcome> repeatDirect, Supplier<QueryOutcome> repeatRewritten)
            throws SQLException {
        if (!"profile:derived_merge_on".equals(context)
                || pair.vp == null || direct.succeeded() == rewritten.succeeded()
                || !hasOrderOrGroupingSort(pair)
                || (!isOutOfSortMemory(direct.error) && !isOutOfSortMemory(rewritten.error))) {
            return null;
        }
        QueryOutcome success = direct.succeeded() ? direct : rewritten;
        if (success.result == null || !success.result.rows.isEmpty()) {
            return null;
        }
        Connection connection = optimizerProfileConnections.get("derived_merge_on");
        if (connection == null) {
            return null;
        }
        long originalMaxSortLength;
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + pair.vp.source)) {
                if (!rs.next() || rs.getLong(1) != 0) {
                    return null;
                }
            }
            boolean filesort = false;
            String failedSql = direct.succeeded() ? pair.rewritten : pair.direct;
            try (ResultSet rs = statement.executeQuery("EXPLAIN " + failedSql)) {
                while (rs.next()) {
                    String extra = rs.getString("Extra");
                    filesort |= extra != null && extra.toLowerCase(Locale.ROOT).contains("using filesort");
                }
            }
            if (!filesort) {
                return null;
            }
            boolean constEmpty = false;
            String successfulSql = direct.succeeded() ? pair.direct : pair.rewritten;
            try (ResultSet rs = statement.executeQuery("EXPLAIN " + successfulSql)) {
                while (rs.next()) {
                    String extra = rs.getString("Extra");
                    if (extra != null) {
                        String normalized = extra.toLowerCase(Locale.ROOT);
                        constEmpty |= normalized.contains("no matching row in const table")
                                || normalized.contains("impossible where noticed after reading const tables");
                    }
                }
            }
            if (!constEmpty) {
                return null;
            }
            try (ResultSet rs = statement.executeQuery("SELECT @@session.max_sort_length")) {
                if (!rs.next()) {
                    return null;
                }
                originalMaxSortLength = rs.getLong(1);
            }
            if (originalMaxSortLength <= 1024) {
                return null;
            }
            try {
                statement.execute("SET SESSION max_sort_length=1024");
                QueryOutcome smallDirect = repeatDirect.get();
                QueryOutcome smallRewritten = repeatRewritten.get();
                if (smallDirect.succeeded() && smallRewritten.succeeded()
                        && smallDirect.result != null && smallRewritten.result != null
                        && smallDirect.result.rows.isEmpty() && smallRewritten.result.rows.isEmpty()) {
                    logLine("vp-resource-candidates.log", "known-probe=EMPTY_INPUT_FILESORT"
                            + " source-count=0 failed-plan-filesort=true original-max-sort-length="
                            + originalMaxSortLength + " probe-max-sort-length=1024 direct=EMPTY rewritten=EMPTY");
                    return "MARIADB_EMPTY_INPUT_FILESORT_RESOURCE_ASYMMETRY";
                }
            } finally {
                statement.execute("SET SESSION max_sort_length=" + originalMaxSortLength);
            }
        }
        return null;
    }

    private boolean isOutOfSortMemory(Throwable error) {
        SQLException sql = findSqlException(error);
        return sql != null && sql.getErrorCode() == 1038;
    }

    private boolean hasOrderOrGroupingSort(QueryPair pair) {
        String sql = (pair.direct + " " + pair.rewritten).toLowerCase(Locale.ROOT);
        return sql.contains("order by") || sql.contains("group by") || sql.contains("distinct");
    }

    private boolean sameOutcomeStatus(QueryOutcome first, QueryOutcome second) {
        if (first.succeeded() || second.succeeded())
            return first.succeeded() == second.succeeded();
        return sameSqlException(first.error, second.error)
                && isResourceLimitThrowable(first.error) == isResourceLimitThrowable(second.error)
                && (!isResourceLimitThrowable(first.error)
                        || skipCategory(errorText(first.error)).equals(skipCategory(errorText(second.error))));
    }

    private boolean bothSameResourceError(QueryOutcome first, QueryOutcome second) {
        return first.error != null && second.error != null && isResourceLimitThrowable(first.error)
                && isResourceLimitThrowable(second.error) && sameSqlException(first.error, second.error)
                && skipCategory(errorText(first.error)).equals(skipCategory(errorText(second.error)));
    }

    private Throwable resourceLimitError(QueryOutcome first, QueryOutcome second) {
        if (first.error != null && isResourceLimitThrowable(first.error)) {
            return first.error;
        }
        if (second.error != null && isResourceLimitThrowable(second.error)) {
            return second.error;
        }
        return null;
    }

    private boolean sameSqlException(Throwable left, Throwable right) {
        if (left == null || right == null)
            return false;
        SQLException l = findSqlException(left);
        SQLException r = findSqlException(right);
        if (l == null || r == null)
            return left.getClass().equals(right.getClass());
        return Objects.equals(l.getSQLState(), r.getSQLState()) && l.getErrorCode() == r.getErrorCode();
    }

    private SQLException findSqlException(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SQLException)
                return (SQLException) current;
            current = current.getCause();
        }
        return null;
    }

    private String outcomeText(QueryOutcome outcome) {
        return outcome.succeeded() ? "SUCCESS" : errorText(outcome.error).replace('\n', ' ');
    }

    private String classifyKnown(QueryPair pair, ReconstructionStrategy strategy, QueryResultMultiset direct,
            QueryResultMultiset rewritten) {
        return classifyKnown(pair, strategy, direct, rewritten, null);
    }

    private String classifyKnown(QueryPair pair, ReconstructionStrategy strategy, QueryResultMultiset direct,
            QueryResultMultiset rewritten, String optimizerProfileContext) {
        if (isConcatUnaryTextDistinctTruncation(pair, direct, rewritten, optimizerProfileContext)) {
            return "MARIADB_CONCAT_UNARY_TEXT_DISTINCT_TEMP_TRUNCATION";
        }
        if (isRoundNegatedBooleanMaterialization(pair, direct, rewritten, optimizerProfileContext)) {
            return "MARIADB_ROUND_NEGATED_BOOLEAN_MATERIALIZATION_SIGN";
        }
        if (isNullableDecimalHashJoinNullLoss(pair, strategy, direct, rewritten, optimizerProfileContext)) {
            return "MARIADB_NULLABLE_DECIMAL_HASH_JOIN_IS_NULL_LOSS";
        }
        Set<Integer> extremeDoubleColumns = extremeDoubleProjectionColumns(pair);
        if (hasExtremeDoubleRoundDisplayShift(pair, strategy, direct, rewritten) && !extremeDoubleColumns.isEmpty()
                && hasOnlyExpectedCellDifferences(pair, direct, rewritten,
                        (left, right, kind, column) -> extremeDoubleColumns.contains(column)
                                && isExtremeDoubleCellDifference(left, right, kind))) {
            return "MARIADB_EXTREME_DOUBLE_ROUND_DISPLAY_SHIFT";
        }
        Set<Integer> unsignedDoubleColumns = unsignedDoubleProjectionColumns(pair);
        if (hasUnsignedDoubleScalarShift(pair, strategy) && !unsignedDoubleColumns.isEmpty()
                && hasOnlyExpectedCellDifferences(pair, direct, rewritten,
                        (left, right, kind, column) -> unsignedDoubleColumns.contains(column)
                                && isUnsignedDoubleCellDifference(left, right, kind))) {
            return "MARIADB_UNSIGNED_DOUBLE_SCALAR_SHIFT_SEMANTICS";
        }
        Set<Integer> trimOrdColumns = trimOrdProjectionColumns(pair);
        if (hasTrimOrdTemporaryTruncation(pair, strategy) && !trimOrdColumns.isEmpty()
                && hasOnlyExpectedCellDifferences(pair, direct, rewritten, (left, right, kind,
                        column) -> trimOrdColumns.contains(column) && isTrimOrdCellDifference(left, right, kind))) {
            return "MARIADB_TRIM_ORD_TEMPORARY_COLUMN_TRUNCATION";
        }
        if (isMdev41081BooleanInIndexRange(pair, strategy, direct, rewritten)) {
            return "MARIADB_MDEV_41081_BOOLEAN_IN_INDEX_RANGE";
        }
        if (isMdev38834NotInNullRange(pair, direct, rewritten)
                || isMdev38834IsTrueBitwiseNullRange(pair, direct, rewritten, optimizerProfileContext)) {
            return "MARIADB_MDEV_38834_NOT_IN_NULL_RANGE_SCAN";
        }
        if (isMdev41337NotNullDatetimeIsNullDerived(pair, strategy, direct, rewritten, optimizerProfileContext)) {
            return "MARIADB_MDEV_41337_NOT_NULL_DATETIME_IS_NULL_DERIVED";
        }
        if (isMdev41327EnumShiftTemporalDerivedPruning(pair, strategy, direct, rewritten)) {
            return "MARIADB_MDEV_41327_ENUM_SHIFT_TEMPORAL_DERIVED_PRUNING";
        }
        if (isMdev41328BitJsonDerivedComparison(pair, strategy, direct, rewritten)) {
            return "MARIADB_MDEV_41328_BIT_JSON_DERIVED_COMPARISON";
        }
        if (isBitNotNullInZeroDateDerivedPushdown(pair, strategy, direct, rewritten,
                optimizerProfileContext)) {
            return "MARIADB_BIT_NOT_NULL_IN_ZERO_DATE_DERIVED_PUSHDOWN";
        }
        if (isBitLpadInXorDerivedPushdown(pair, strategy, direct, rewritten, optimizerProfileContext)) {
            return "MARIADB_BIT_LPAD_IN_XOR_DERIVED_PUSHDOWN";
        }
        String bitInKnown = classifyBitInPlanMismatch(pair, strategy, direct, rewritten, optimizerProfileContext);
        if (bitInKnown != null) {
            return bitInKnown;
        }
        if (isNestedNotInDatetimeDerivedPushdown(pair, strategy, direct, rewritten,
                optimizerProfileContext)) {
            return "MARIADB_NESTED_NOT_IN_DATETIME_DERIVED_PUSHDOWN";
        }
        if (isConfirmedNativeInIndexConversion(pair, direct, rewritten, optimizerProfileContext)
                || isMdev41117BitStringInIndexRange(pair, strategy, direct, rewritten)
                || (direct.rows.isEmpty() && !rewritten.rows.isEmpty()
                        && isDerivedPlan(strategy, pair.actualPlan) && hasQuotedInPredicate(pair, true)
                        && sourceScanMatches(pair, rewritten, optimizerProfileContext))) {
            return "MARIADB_MDEV_41117_BIT_STRING_IN_INDEX_RANGE";
        }
        if (isMdev41117QuotedComparisonIndexVariant(pair, strategy, direct, rewritten,
                optimizerProfileContext)
                || isMdev41117QuotedInIndexedPlan(pair, strategy, direct, rewritten, optimizerProfileContext)) {
            return "MARIADB_MDEV_41117_BIT_STRING_IN_INDEX_RANGE";
        }
        if (isMdev41117ReconstructionIndexConversion(pair, strategy, direct, rewritten,
                optimizerProfileContext)) {
            return "MARIADB_MDEV_41117_BIT_STRING_IN_INDEX_RANGE";
        }
        if (isMdev41117DerivedWithKeysTemporalConversion(pair, strategy, direct, rewritten,
                optimizerProfileContext)) {
            return "MARIADB_MDEV_41117_BIT_STRING_IN_INDEX_RANGE";
        }
        if (isMdev41117TemporalStringInIndexRange(pair, strategy, direct, rewritten, optimizerProfileContext)) {
            // Temporal/string index conversion is the same MDEV-41117
            // family as the historical BIT/string cases; keep one known-pool
            // bucket so equivalent manifestations are absorbed together.
            return "MARIADB_MDEV_41117_BIT_STRING_IN_INDEX_RANGE";
        }
        // A nullable temporal column can expose the same faulty indexed
        // lookup family through IS NULL rather than a quoted conversion. The
        // source ref/range plan must be present and removing only helper
        // indexes must restore the complete native multiset; this keeps
        // unrelated NULL/pushdown differences visible.
        if (isMdev41117TemporalNullIndexRange(pair, strategy, direct, rewritten,
                optimizerProfileContext)) {
            return "MARIADB_MDEV_41117_BIT_STRING_IN_INDEX_RANGE";
        }
        if (isMariaDBTemporalNullifDerivedTypeFlip(pair, strategy, direct, rewritten)) {
            return "MARIADB_TEMPORAL_NULLIF_DERIVED_TYPE_FLIP";
        }
        if (!hasZeroFillObservable(pair, strategy) || !hasOnlyExpectedCellDifferences(pair, direct, rewritten,
                (left, right, kind, column) -> isZeroFillPaddingDifference(left, right))) {
            return null;
        }
        // The historical MariaDB cases are JDBC display-padding loss only. Do
        // not absorb a mismatch merely because a ZEROFILL column is present.
        return "ZEROFILL_DISPLAY_LOSS";
    }


    // mariadb815/10bug.log and mariadb817/database23: 0a/00 -> 0,
    // asymmetric materialization and a length-only repair probe.
    private boolean isConcatUnaryTextDistinctTruncation(QueryPair pair, QueryResultMultiset direct,
            QueryResultMultiset rewritten, String profile) {
        boolean distinct = Pattern.compile("(?i)\\bSELECT\\s+DISTINCT(?:ROW)?\\s").matcher(pair.direct).find()
                && Pattern.compile("(?i)\\bSELECT\\s+DISTINCT(?:ROW)?\\s").matcher(pair.rewritten).find();
        Set<Integer> columns = projectionColumns(pair.direct, expression -> pair.vp.plan.all.stream()
                .anyMatch(c -> (c.type == MariaDBDataType.JSON || c.definition != null
                        && c.definition.toUpperCase(Locale.ROOT).contains("LONGTEXT"))
                        && (distinct && concatUnaryTextPattern(c.name).matcher(expression).find()
                                || concatFalseUnaryTextPattern(c.name).matcher(expression).find()))
                        || (distinct && pair.vp.plan.all.stream().anyMatch(c -> c.type == MariaDBDataType.REAL
                                && hasRepeatedRoundTextCast(expression, c.name))));
        if (columns.isEmpty()) {
            return false;
        }
        boolean rewrittenTruncated = hasOnlyExpectedCellDifferences(pair, direct, rewritten,
                (a, b, kind, column) -> columns.contains(column) && isConcatMaterializationLoss(a, b));
        boolean directTruncated = !rewrittenTruncated && hasOnlyExpectedCellDifferences(pair, rewritten, direct,
                (a, b, kind, column) -> columns.contains(column) && isConcatMaterializationLoss(a, b));
        if (!rewrittenTruncated && !directTruncated) {
            return false;
        }
        String[] profiles = profile == null ? new String[] { null, null } : profile.split("->", -1);
        Connection directConnection = profile == null ? state.getConnection().getJdbcConnection()
                : optimizerProfileConnections.get(profiles[0]);
        Connection rewrittenConnection = profile == null ? state.getConnection().getJdbcConnection()
                : optimizerProfileConnections.get(profiles[profiles.length - 1]);
        if (directConnection == null || rewrittenConnection == null) {
            return false;
        }
        Connection truncatedConnection = rewrittenTruncated ? rewrittenConnection : directConnection;
        Connection fullConnection = rewrittenTruncated ? directConnection : rewrittenConnection;
        String truncatedSql = rewrittenTruncated ? pair.rewritten : pair.direct;
        String fullSql = rewrittenTruncated ? pair.direct : pair.rewritten;
        try {
            boolean truncatedTemp = distinct ? usesDistinctTemporary(truncatedConnection, truncatedSql)
                    : usesMaterializedDerived(truncatedConnection, truncatedSql);
            boolean fullTemp = distinct ? usesDistinctTemporary(fullConnection, fullSql)
                    : usesMaterializedDerived(fullConnection, fullSql);
            if (!truncatedTemp || fullTemp) {
                return false;
            }
            String repaired = truncatedSql;
            for (Column column : pair.vp.plan.all) {
                java.util.regex.Matcher matcher = concatUnaryTextPattern(column.name).matcher(repaired);
                StringBuffer buffer = new StringBuffer();
                while (matcher.find()) {
                    String replacement = matcher.group().replaceAll("(?i)\\bAS\\s+CHAR\\b", "AS CHAR(32)");
                    matcher.appendReplacement(buffer, java.util.regex.Matcher.quoteReplacement(replacement));
                }
                matcher.appendTail(buffer);
                repaired = buffer.toString();
                matcher = roundTextCastPattern(column.name).matcher(repaired);
                buffer = new StringBuffer();
                while (matcher.find()) {
                    matcher.appendReplacement(buffer, java.util.regex.Matcher.quoteReplacement(
                            matcher.group().replaceAll("(?i)\\bAS\\s+CHAR\\b", "AS CHAR(32)")));
                }
                matcher.appendTail(buffer);
                repaired = buffer.toString();
                matcher = concatFalseUnaryTextPattern(column.name).matcher(repaired);
                buffer = new StringBuffer();
                while (matcher.find()) {
                    matcher.appendReplacement(buffer, java.util.regex.Matcher.quoteReplacement(
                            matcher.group().replaceAll("(?i)\\bAS\\s+CHAR\\b", "AS CHAR(32)")));
                }
                matcher.appendTail(buffer);
                repaired = buffer.toString();
            }
            if (repaired.equals(truncatedSql)) {
                return false;
            }
            QueryOutcome probe = executeProfileQuery(truncatedConnection, repaired);
            boolean matches = probe.succeeded() && queryResultsMatch(pair, rewrittenTruncated ? direct : rewritten, probe.result);
            return matches;
        } catch (SQLException ignored) {
            return false;
        }
    }

    static Pattern concatUnaryTextPattern(String column) {
        String charset = "(?:\\s+CHARACTER\\s+SET\\s+utf8mb4)?";
        String collation = "(?:\\s+COLLATE\\s+utf8mb4_bin)?";
        String suffix = "(?:'a'|CAST\\s*\\(\\s*'a'\\s+AS\\s+CHAR" + charset + "\\s*\\)" + collation + ")";
        return Pattern.compile("\\bCONCAT\\s*\\(\\s*CAST\\s*\\(\\s*\\(?\\s*-\\s*`?"
                + Pattern.quote(column) + "`?\\s*\\)?\\s+AS\\s+CHAR" + charset + "\\s*\\)" + collation
                + "\\s*,\\s*" + suffix + "\\s*\\)", Pattern.CASE_INSENSITIVE);
    }

    static boolean isConcatZeroSuffixLoss(String full, String truncated) {
        return "C:3061".equals(full) && "C:30".equals(truncated);
    }

    public static boolean isConcatMaterializationLoss(String full, String truncated) {
        if (isConcatZeroSuffixLoss(full, truncated) || "C:3030".equals(full) && "C:30".equals(truncated)) {
            return true;
        }
        java.util.regex.Matcher fullCell = Pattern.compile("C:([0-9a-f]+)").matcher(String.valueOf(full));
        java.util.regex.Matcher truncatedCell = Pattern.compile("C:([0-9a-f]+)")
                .matcher(String.valueOf(truncated));
        if (!fullCell.matches() || !truncatedCell.matches()) {
            return false;
        }
        String fullHex = fullCell.group(1);
        String truncatedHex = truncatedCell.group(1);
        return fullHex.equals(truncatedHex + "3030");
    }

    public static boolean hasRepeatedRoundTextCast(String expression, String column) {
        java.util.regex.Matcher matcher = roundTextCastPattern(column).matcher(expression);
        return matcher.find() && matcher.find();
    }

    private static Pattern roundTextCastPattern(String column) {
        return Pattern.compile("CAST\\s*\\(\\s*ROUND\\s*\\(\\s*`?" + Pattern.quote(column)
                + "`?\\s*\\)\\s+AS\\s+CHAR(?:\\s+CHARACTER\\s+SET\\s+utf8mb4)?\\s*\\)",
                Pattern.CASE_INSENSITIVE);
    }

    static Pattern concatFalseUnaryTextPattern(String column) {
        String charset = "(?:\\s+CHARACTER\\s+SET\\s+utf8mb4)?";
        String collation = "(?:\\s+COLLATE\\s+utf8mb4_bin)?";
        return Pattern.compile("\\bCONCAT\\s*\\(\\s*CAST\\s*\\(\\s*FALSE\\s+AS\\s+CHAR" + charset
                + "\\s*\\)" + collation + "\\s*,\\s*CAST\\s*\\(\\s*\\(?\\s*-\\s*`?"
                + Pattern.quote(column) + "`?\\s*\\)?\\s+AS\\s+CHAR" + charset + "\\s*\\)"
                + collation + "\\s*\\)", Pattern.CASE_INSENSITIVE);
    }

    private boolean usesDistinctTemporary(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("EXPLAIN " + sql)) {
            while (rs.next()) {
                String extra = rs.getString("Extra");
                if (extra != null && extra.contains("Using temporary")) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean usesMaterializedDerived(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet rs = statement.executeQuery("EXPLAIN " + sql)) {
            while (rs.next()) {
                String table = rs.getString("table");
                if (table != null && table.startsWith("<derived")) {
                    return true;
                }
            }
        }
        return false;
    }

    // mariadb814/0bug.log: classify only after disabling join cache restores
    // exactly the native result on the same connection; restore the setting.
    private boolean isNullableDecimalHashJoinNullLoss(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten, String profile) {
        if (!(isDerivedPlan(strategy, pair.actualPlan) || strategy == ReconstructionStrategy.NATURAL_OR_USING)
                || direct.rows.isEmpty() || !rewritten.rows.isEmpty()
                || !Pattern.compile("(?i)\\bSELECT\\s+DISTINCT(?:ROW)?\\s").matcher(pair.direct).find()
                || pair.vp.plan.all.stream().noneMatch(c -> c.type == MariaDBDataType.DECIMAL
                        && c.definition != null && !c.definition.toUpperCase(Locale.ROOT).contains("NOT NULL")
                        && hasIsNullPredicateForColumn(pair.direct.toUpperCase(Locale.ROOT), c.name))) {
            return false;
        }
        Connection connection = profileConnection(profile, true);
        if (connection == null) {
            return false;
        }
        try (Statement statement = connection.createStatement()) {
            boolean hash = false;
            try (ResultSet rs = statement.executeQuery("EXPLAIN " + pair.rewritten)) {
                while (rs.next()) {
                    String extra = rs.getString("Extra");
                    hash |= "hash_index".equalsIgnoreCase(rs.getString("type"))
                            && extra != null && extra.contains("BNLH join");
                }
            }
            if (!hash) {
                return false;
            }
            int original;
            try (ResultSet rs = statement.executeQuery("SELECT @@session.join_cache_level")) {
                if (!rs.next()) {
                    return false;
                }
                original = rs.getInt(1);
            }
            try {
                statement.execute("SET SESSION join_cache_level=0");
                QueryOutcome probe = executeProfileQuery(connection, pair.rewritten);
                return probe.succeeded() && queryResultsMatch(pair, direct, probe.result);
            } finally {
                statement.execute("SET SESSION join_cache_level=" + original);
            }
        } catch (SQLException ignored) {
            return false;
        }
    }

    /**
     * A cross-profile comparison is identified as direct-profile->rewritten-profile.
     * Classifiers that execute one side again must use that side's actual session,
     * rather than looking up the combined comparison label as a connection name.
     */
    private Connection profileConnection(String profile, boolean rewrittenSide) {
        if (profile == null) {
            return state.getConnection().getJdbcConnection();
        }
        String[] profiles = profile.split("->", -1);
        String selected = rewrittenSide ? profiles[profiles.length - 1] : profiles[0];
        return optimizerProfileConnections.get(selected);
    }

    static Pattern roundNegatedBooleanPattern() {
        return Pattern.compile("\\bROUND\\s*\\(\\s*\\(?\\s*-\\s*TRUE\\s*\\)?\\s*,\\s*"
                + "(?:'[^']*'|[-+]?\\d+)\\s*\\)", Pattern.CASE_INSENSITIVE);
    }

    // This is a signedness defect, not the extreme-DOUBLE overflow rule.
    // Both sides must recover the exact -1 rows after making ROUND's output signed.
    private boolean isRoundNegatedBooleanMaterialization(QueryPair pair, QueryResultMultiset direct,
            QueryResultMultiset rewritten, String profile) {
        Set<Integer> columns = projectionColumns(pair.direct,
                expression -> roundNegatedBooleanPattern().matcher(expression).find());
        if (columns.isEmpty() || !pair.direct.startsWith("SELECT * FROM (SELECT ")
                || !pair.rewritten.startsWith("SELECT * FROM (SELECT ")) {
            return false;
        }
        boolean rewrittenUnsigned = hasOnlyExpectedCellDifferences(pair, direct, rewritten,
                (a, b, kind, column) -> columns.contains(column) && "-1".equals(a) && "1".equals(b));
        boolean directUnsigned = !rewrittenUnsigned && hasOnlyExpectedCellDifferences(pair, rewritten, direct,
                (a, b, kind, column) -> columns.contains(column) && "-1".equals(a) && "1".equals(b));
        if (!rewrittenUnsigned && !directUnsigned) {
            return false;
        }
        Connection left = profileConnection(profile, false);
        Connection right = profileConnection(profile, true);
        if (left == null || right == null) {
            return false;
        }
        QueryResultMultiset expected = rewrittenUnsigned ? direct : rewritten;
        for (boolean rewrittenSide : List.of(false, true)) {
            String sql = rewrittenSide ? pair.rewritten : pair.direct;
            java.util.regex.Matcher matcher = roundNegatedBooleanPattern().matcher(sql);
            StringBuffer repaired = new StringBuffer();
            while (matcher.find()) {
                matcher.appendReplacement(repaired,
                        java.util.regex.Matcher.quoteReplacement("CAST(" + matcher.group() + " AS SIGNED)"));
            }
            matcher.appendTail(repaired);
            QueryOutcome probe = executeProfileQuery(rewrittenSide ? right : left, repaired.toString());
            if (!probe.succeeded() || !queryResultsMatch(pair, expected, probe.result)) {
                return false;
            }
        }
        return true;
    }

    /**
     * MDEV-41337: with derived_merge=off, DISTINCTROW over a reconstructed
     * relation can disagree with the native table for IS NULL on a
     * DATETIME NOT NULL PRIMARY KEY. The native const plan may retain one
     * row even though the predicate is impossible, while the CTE/derived
     * path correctly returns no rows.
     */
    private boolean isMdev41337NotNullDatetimeIsNullDerived(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten, String profile) {
        if (direct.rows.isEmpty() || !rewritten.rows.isEmpty() || !isDerivedPlan(strategy, pair.actualPlan)) {
            return false;
        }
        String sql = pair.direct.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
        for (Column column : pair.vp.plan.all) {
            if (!isTemporalStringIndexType(column.type) || !referencesIdentifier(sql, column.name)
                    || !hasIsNullPredicateForColumn(sql, column.name)
                    || column.definition == null
                    || !column.definition.toUpperCase(Locale.ROOT).contains("NOT NULL")) {
                continue;
            }
            String zero = column.type == MariaDBDataType.DATE ? "0000-00-00" : "0000-00-00 00:00:00";
            Pattern predicate = Pattern.compile("(?i)(?<![A-Z0-9_$])(`?" + Pattern.quote(column.name)
                    + "`?)(\\s*\\)*)\\s+IS\\s+NULL");
            String repair = "$1$2 = '" + zero + "'";
            try {
                Connection directConnection = profileConnection(profile, false);
                Connection rewrittenConnection = profileConnection(profile, true);
                if (directConnection == null || rewrittenConnection == null) {
                    return false;
                }
                QueryResultMultiset nativeZero = getResultMultiset(directConnection,
                        predicate.matcher(pair.direct).replaceAll(repair), false);
                QueryResultMultiset rewrittenZero = getResultMultiset(rewrittenConnection,
                        predicate.matcher(pair.rewritten).replaceAll(repair), false);
                if (queryResultsMatch(pair, direct, nativeZero) && queryResultsMatch(pair, direct, rewrittenZero)) {
                    return true;
                }
            } catch (SQLException ignored) {
                return false;
            }
        }
        return false;
    }

    private boolean hasIsNullPredicateForColumn(String sql, String columnName) {
        String pattern = "(?is)(?<![A-Z0-9_$])`?" + Pattern.quote(columnName.toUpperCase(Locale.ROOT))
                + "`?\\s*\\)*\\s+IS\\s+NULL";
        return Pattern.compile(pattern).matcher(sql).find();
    }

    /**
     * MDEV-38834: with not_null_range_scan enabled, a nullable indexed
     * expression under a compound NULL-sensitive predicate can discard NULL
     * rows on the direct range path, while a materialized derived path
     * evaluates the original three-valued predicate and keeps them.
     */
    private boolean isMdev38834NotInNullRange(QueryPair pair, QueryResultMultiset direct,
            QueryResultMultiset rewritten) {
        if (!direct.rows.isEmpty() || rewritten.rows.isEmpty()) {
            return false;
        }
        String sql = pair.direct.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
        boolean originalNotInForm = sql.contains("IS NOT NULL")
                && (sql.contains(" NOT IN ") || sql.contains("<>"));
        boolean compoundComparisonForm = isMdev38834CompoundComparison(sql);
        if (!originalNotInForm && !compoundComparisonForm) {
            return false;
        }
        for (Column column : pair.vp.plan.all) {
            if (!isNullableNumericRangeType(column.type) || !referencesIdentifier(sql, column.name)
                    || !sourceHasAnyIndexContainingColumn(pair, column.name)) {
                continue;
            }
            if (!sourceUsesRangeIndex(pair, column.name)) {
                continue;
            }
            return true;
        }
        return false;
    }

    private boolean isMdev38834CompoundComparison(String sql) {
        // database4.log: ((nullable_numeric IS NOT NULL) < (other IN (...))).
        return Pattern.compile(
                "`?[A-Z_][A-Z0-9_$]*`?\\s+IS\\s+NOT\\s+NULL\\s*\\)\\s*[<>=]\\s*\\(.*\\bIN\\s*\\(")
                .matcher(sql).find();
    }

    // mariadb819/database3: IS TRUE is not NULL-propagating under bitwise
    // arithmetic. Require a direct range plan and both complete results to
    // agree after disabling only not_null_range_scan on their actual sessions.
    private boolean isMdev38834IsTrueBitwiseNullRange(QueryPair pair, QueryResultMultiset direct,
            QueryResultMultiset rewritten, String profile) {
        if (direct.rows.isEmpty() || rowCount(direct) >= rowCount(rewritten)
                || !direct.rows.entrySet().stream()
                        .allMatch(e -> rewritten.rows.getOrDefault(e.getKey(), 0) >= e.getValue())) {
            return false;
        }
        int where = pair.direct.toUpperCase(Locale.ROOT).indexOf(" WHERE ");
        if (where < 0) {
            return false;
        }
        String predicate = pair.direct.substring(where);
        if (!predicate.contains("^") || !predicate.contains("&")) {
            return false;
        }
        Connection connection = profileConnection(profile, false);
        Connection rewrittenConnection = profileConnection(profile, true);
        if (connection == null || rewrittenConnection == null) {
            return false;
        }
        for (Column column : pair.vp.plan.all) {
            if (!isNullableNumericRangeType(column.type) || column.definition == null
                    || column.definition.toUpperCase(Locale.ROOT).contains("NOT NULL")
                    || !Pattern.compile("(?i)(?<![A-Z0-9_$])`?" + Pattern.quote(column.name)
                            + "`?\\s+IS\\s+TRUE\\b").matcher(predicate).find()
                    || !sourceUsesRangeIndex(pair, column.name, profile)) {
                continue;
            }
            return notNullRangeOffMatches(pair, connection, pair.direct, rewritten, true)
                    && notNullRangeOffMatches(pair, rewrittenConnection, pair.rewritten, rewritten, false);
        }
        return false;
    }

    private boolean notNullRangeOffMatches(QueryPair pair, Connection connection, String sql,
            QueryResultMultiset expected, boolean requireEnabled) {
        try (Statement statement = connection.createStatement()) {
            String original;
            try (ResultSet rs = statement.executeQuery("SELECT @@session.optimizer_switch")) {
                if (!rs.next()) {
                    return false;
                }
                original = rs.getString(1);
            }
            if (requireEnabled && !original.contains("not_null_range_scan=on")) {
                return false;
            }
            try {
                statement.execute("SET SESSION optimizer_switch='not_null_range_scan=off'");
                QueryOutcome probe = executeProfileQuery(connection, sql);
                return probe.succeeded() && queryResultsMatch(pair, expected, probe.result);
            } finally {
                statement.execute("SET SESSION optimizer_switch='" + original.replace("'", "''") + "'");
            }
        } catch (SQLException ignored) {
            return false;
        }
    }

    private boolean sourceHasAnyIndexContainingColumn(QueryPair pair, String columnName) {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("SHOW INDEX FROM " + pair.vp.source)) {
            while (rs.next()) {
                if (columnName.equalsIgnoreCase(rs.getString("Column_name"))) {
                    return true;
                }
            }
        } catch (SQLException ignored) {
            return false;
        }
        return false;
    }

    private boolean isNullableNumericRangeType(MariaDBDataType type) {
        return type == MariaDBDataType.DECIMAL || type == MariaDBDataType.REAL || type == MariaDBDataType.INT
                || type == MariaDBDataType.BOOLEAN;
    }

    private boolean sourceUsesRangeIndex(QueryPair pair, String columnName) {
        return sourceUsesRangeIndex(pair, columnName, null);
    }

    private boolean sourceUsesRangeIndex(QueryPair pair, String columnName, String profile) {
        Connection connection = profileConnection(profile, false);
        if (connection == null) {
            return false;
        }
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("EXPLAIN " + pair.direct)) {
            while (rs.next()) {
                String table = stripIdentifierQuotes(rs.getString("table"));
                String access = rs.getString("type");
                String key = stripIdentifierQuotes(rs.getString("key"));
                if (table != null && table.equalsIgnoreCase(stripIdentifierQuotes(pair.vp.source))
                        && "range".equalsIgnoreCase(access) && key != null
                        && key.startsWith(pair.vp.prefix + "idx_src_")) {
                    return true;
                }
                if (table != null && table.equalsIgnoreCase(stripIdentifierQuotes(pair.vp.source))
                        && "range".equalsIgnoreCase(access) && key != null
                        && sourceIndexHasColumn(pair, key, columnName)) {
                    return true;
                }
            }
        } catch (SQLException ignored) {
            return false;
        }
        return false;
    }

    private boolean sourceIndexHasColumn(QueryPair pair, String indexName, String columnName) {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("SHOW INDEX FROM " + pair.vp.source)) {
            while (rs.next()) {
                if (indexName.equalsIgnoreCase(rs.getString("Key_name"))
                        && columnName.equalsIgnoreCase(rs.getString("Column_name"))
                        && rs.getInt("Seq_in_index") == 1) {
                    return true;
                }
            }
        } catch (SQLException ignored) {
            return false;
        }
        return false;
    }

    /**
     * MDEV-41327: ENUM numeric coercion through << produces a zero-date match
     * in the native table, but derived_merge=off can prune the ENUM operand
     * during derived materialization, turning the predicate into IN (NULL).
     */
    private boolean isMdev41327EnumShiftTemporalDerivedPruning(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten) {
        if (direct.rows.isEmpty() || !rewritten.rows.isEmpty() || !isDerivedPlan(strategy, pair.actualPlan)) {
            return false;
        }
        String sql = pair.direct.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
        if (!sql.contains(" IN ") || !sql.contains("<<")) {
            return false;
        }
        for (Column temporal : pair.vp.plan.all) {
            if (!isTemporalStringIndexType(temporal.type) || !referencesIdentifier(sql, temporal.name)) {
                continue;
            }
            for (Column enumColumn : pair.vp.plan.all) {
                if (enumColumn.type != MariaDBDataType.ENUM || !referencesIdentifier(sql, enumColumn.name)) {
                    continue;
                }
                String pattern = "(?is)\\b" + Pattern.quote(temporal.name.toUpperCase(Locale.ROOT))
                        + "\\s+IN\\s*\\(\\s*\\(*\\s*" + Pattern.quote(enumColumn.name.toUpperCase(Locale.ROOT))
                        + "\\s*<<\\s*" + Pattern.quote(enumColumn.name.toUpperCase(Locale.ROOT)) + "\\s*\\)*\\s*\\)";
                if (Pattern.compile(pattern).matcher(sql).find()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * MDEV-41328: a BIT IN (JSON) predicate is evaluated with native JSON
     * coercion on the base table but with a string-like materialized JSON
     * column after a CTE/derived join, changing which rows survive.
     */
    private boolean isMdev41328BitJsonDerivedComparison(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten) {
        if (!isDerivedPlan(strategy, pair.actualPlan) || direct.rows.isEmpty() || rewritten.rows.isEmpty()
                || rowCount(direct) <= rowCount(rewritten)) {
            return false;
        }
        String sql = pair.direct.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
        if (!sql.contains(" IN ")) {
            return false;
        }
        boolean hasBit = false;
        boolean hasJson = false;
        for (Column column : pair.vp.plan.all) {
            if (column.type == MariaDBDataType.BIT && referencesIdentifier(sql, column.name)) {
                hasBit = true;
            }
            if (column.type == MariaDBDataType.JSON && referencesIdentifier(sql, column.name)) {
                hasJson = true;
            }
        }
        if (!hasBit || !hasJson) {
            return false;
        }
        return Pattern.compile("(?is)\\b[A-Z_][A-Z0-9_$]*\\s+IN\\s*\\(\\s*[A-Z_][A-Z0-9_$]*\\s*\\)")
                .matcher(sql).find();
    }

    /** database9/2152: derived condition pushdown loses the zero-date row. */
    private boolean isBitNotNullInZeroDateDerivedPushdown(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten, String profile) {
        if (!"derived_merge_off".equals(profile) || !isDerivedPlan(strategy, pair.actualPlan)
                || direct.rows.isEmpty() || !rewritten.rows.isEmpty()
                || direct.rows.keySet().stream().noneMatch(row -> row.contains("0000-00-00"))) {
            return false;
        }
        String rewrittenSql = pair.rewritten.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
        if (!rewrittenSql.contains("WITH ") || !rewrittenSql.contains(" JOIN ")
                || rewrittenSql.split(" AS \\(SELECT \\* FROM ", -1).length != 3) {
            return false;
        }
        String sql = pair.direct.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
        for (Column bit : pair.vp.plan.all) {
            if (bit.type != MariaDBDataType.BIT) {
                continue;
            }
            for (Column date : pair.vp.plan.all) {
                if (date.type != MariaDBDataType.DATE) {
                    continue;
                }
                String predicate = "(?is)\\(\\s*\\(*\\s*`?" + Pattern.quote(bit.name)
                        + "`?\\s+IS\\s+NOT\\s+NULL\\s*\\)*\\s*\\)\\s+IN\\s*\\(\\s*\\(*\\s*`?"
                        + Pattern.quote(date.name) + "`?\\s*\\)*\\s*\\)";
                if (Pattern.compile(predicate, Pattern.CASE_INSENSITIVE).matcher(sql).find()) {
                    return true;
                }
            }
        }
        return false;
    }

    // These BIT/IN cases have different failing plans and repair probes.
    private String classifyBitInPlanMismatch(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten, String profile) {
        if (direct.rows.isEmpty() == rewritten.rows.isEmpty()
                || !(isDerivedPlan(strategy, pair.actualPlan) || strategy == ReconstructionStrategy.STRAIGHT_JOIN
                        || strategy == ReconstructionStrategy.NATURAL_OR_USING)) {
            return null;
        }
        for (Column column : pair.vp.plan.all) {
            if (column.type != MariaDBDataType.BIT
                    || !referencesIdentifier(pair.direct.toUpperCase(Locale.ROOT), column.name)) {
                continue;
            }
            // database10: the source BIT index rejects a nonnumeric string.
            if (direct.rows.isEmpty() && hasNativeInOperand(pair.direct, column.name)
                    && matchesBitIndexInString(pair.direct, column.name)
                    && sourceUsesBitRefIndex(pair, profile, column.name)
                    && sourceScanMatches(pair, rewritten, profile)) {
                return "MARIADB_MDEV_41117_BIT_STRING_IN_INDEX_RANGE";
            }
            // database2: pushing CONCAT(... BIT ...) IN (..., BIT) into the
            // materialized join loses the row; disabling derived pushdown restores it.
            if (!direct.rows.isEmpty() && rewritten.rows.isEmpty()
                    && matchesBitConcatInStringIn(pair.direct, column.name)
                    && pair.rewritten.toUpperCase(Locale.ROOT).contains(" JOIN ")
                    && derivedPushdownRepairMatches(pair, direct, profile)) {
                return "MARIADB_BIT_CONCAT_IN_DERIVED_PUSHDOWN";
            }
        }
        return null;
    }

    // mariadb818/database6: the BIT/LPAD/IN/XOR predicate is correct on the
    // native source, but differs after derived-condition pushdown. Require the
    // complete cast/collation shape and a live pushdown-off repair probe.
    private boolean isBitLpadInXorDerivedPushdown(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten, String profile) {
        if (!isDerivedPlan(strategy, pair.actualPlan) || direct.rows.isEmpty()
                || queryResultsMatch(pair, direct, rewritten)
                || !pair.rewritten.toUpperCase(Locale.ROOT).contains(" JOIN ")) {
            return false;
        }
        for (Column column : pair.vp.plan.all) {
            if (column.type == MariaDBDataType.BIT
                    && matchesBitLpadInXorDerivedPushdown(pair.direct, column.name)
                    && derivedPushdownRepairMatches(pair, direct, profile)) {
                return true;
            }
        }
        return false;
    }

    static boolean matchesBitIndexInString(String sql, String column) {
        String id = "`?" + Pattern.quote(column) + "`?";
        return Pattern.compile("(?is)\\+\\s*\\(\\s*" + id
                + "\\s+IN\\s*\\(\\s*'[^'0-9][^']*'\\s*\\)\\s*\\)").matcher(sql).find();
    }

    static boolean matchesBitConcatInStringIn(String sql, String column) {
        String id = "`?" + Pattern.quote(column) + "`?";
        return Pattern.compile("(?is)CONCAT\\s*\\(\\s*CAST\\s*\\(\\s*" + id
                + "\\s+AS\\s+CHAR.*?\\)\\s+IN\\s*\\(\\s*'[^']+'\\s*,\\s*" + id
                + "\\s*\\)").matcher(sql).find();
    }

    static boolean matchesBitLpadInXorDerivedPushdown(String sql, String column) {
        String id = "`?" + Pattern.quote(column) + "`?";
        String charCast = "CAST\\s*\\(\\s*'[^']*'\\s+AS\\s+CHAR\\s+CHARACTER\\s+SET\\s+utf8mb4\\s*\\)"
                + "\\s+COLLATE\\s+utf8mb4_bin";
        String bitCast = "CAST\\s*\\(\\s*" + id
                + "\\s+AS\\s+CHAR\\s+CHARACTER\\s+SET\\s+utf8mb4\\s*\\)"
                + "\\s+COLLATE\\s+utf8mb4_bin";
        return Pattern.compile("(?is)LPAD\\s*\\(\\s*" + charCast + "\\s*,\\s*'[^']*'\\s*,\\s*"
                + bitCast + "\\s*\\)\\s+IN\\s*\\(\\s*" + id + "\\s*,\\s*'[^']+'\\s*\\)"
                + "\\s*\\)*\\s*XOR\\s+" + id).matcher(sql).find();
    }

    static boolean matchesNestedNotInDatetimePredicate(String sql, String innerColumn, String datetimeColumn) {
        String inner = "`?" + Pattern.quote(innerColumn) + "`?";
        String datetime = "`?" + Pattern.quote(datetimeColumn) + "`?";
        return Pattern.compile("(?is)'[^']+'\\s+NOT\\s+IN\\s*\\(\\s*'[^']+'\\s*,\\s*" + inner
                + "\\s*\\)\\s*\\)\\s+IN\\s*\\(\\s*" + datetime + "\\s*\\)")
                .matcher(sql).find();
    }

    private boolean sourceUsesBitRefIndex(QueryPair pair, String profile, String columnName) {
        Connection connection = profileConnection(profile, false);
        if (connection == null) {
            return false;
        }
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("EXPLAIN " + pair.direct)) {
            while (rs.next()) {
                if (stripIdentifierQuotes(pair.vp.source).equalsIgnoreCase(stripIdentifierQuotes(rs.getString("table")))
                        && "ref".equalsIgnoreCase(rs.getString("type"))) {
                    String key = stripIdentifierQuotes(rs.getString("key"));
                    if (key != null && sourceIndexContainsColumn(pair, key, columnName)) {
                        return true;
                    }
                }
            }
        } catch (SQLException ignored) {
            return false;
        }
        return false;
    }

    private boolean sourceIndexContainsColumn(QueryPair pair, String indexName, String columnName) {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("SHOW INDEX FROM " + pair.vp.source)) {
            while (rs.next()) {
                if (indexName.equalsIgnoreCase(rs.getString("Key_name"))
                        && columnName.equalsIgnoreCase(rs.getString("Column_name"))) {
                    return true;
                }
            }
        } catch (SQLException ignored) {
            return false;
        }
        return false;
    }

    private boolean isNestedNotInDatetimeDerivedPushdown(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten, String profile) {
        if (direct.rows.isEmpty() || !rewritten.rows.isEmpty() || !isDerivedPlan(strategy, pair.actualPlan)
                || pair.vp.plan.all.stream().noneMatch(c -> c.type == MariaDBDataType.DATETIME
                        && hasNativeInOperand(pair.direct, c.name))) {
            return false;
        }
        for (Column inner : pair.vp.plan.all) {
            if (inner.type != MariaDBDataType.INT) {
                continue;
            }
            for (Column datetime : pair.vp.plan.all) {
                if (datetime.type == MariaDBDataType.DATETIME
                        && matchesNestedNotInDatetimePredicate(pair.direct, inner.name, datetime.name)
                        && pair.rewritten.toUpperCase(Locale.ROOT).contains("WITH L AS")
                        && pair.rewritten.toUpperCase(Locale.ROOT).contains(" JOIN ")
                        && derivedPushdownRepairMatches(pair, direct, profile)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean derivedPushdownRepairMatches(QueryPair pair, QueryResultMultiset expected, String profile) {
        Connection connection = profileConnection(profile, true);
        if (connection == null) {
            return false;
        }
        try (Statement statement = connection.createStatement()) {
            String original;
            try (ResultSet rs = statement.executeQuery("SELECT @@session.optimizer_switch")) {
                if (!rs.next()) {
                    return false;
                }
                original = rs.getString(1);
            }
            if (!original.contains("derived_merge=off") || !original.contains("condition_pushdown_for_derived=on")) {
                return false;
            }
            try {
                statement.execute("SET SESSION optimizer_switch='condition_pushdown_for_derived=off'");
                QueryOutcome probe = executeProfileQuery(connection, pair.rewritten);
                return probe.succeeded() && queryResultsMatch(pair, expected, probe.result);
            } finally {
                statement.execute("SET SESSION optimizer_switch='" + original.replace("'", "''") + "'");
            }
        } catch (SQLException ignored) {
            return false;
        }
    }

    private boolean isDerivedPlan(ReconstructionStrategy strategy, String actualPlan) {
        return strategy == ReconstructionStrategy.JOIN || strategy == ReconstructionStrategy.CTE
                || strategy == ReconstructionStrategy.DERIVED_TABLE
                || strategy == ReconstructionStrategy.LEFT_RIGHT_JOIN || strategy == ReconstructionStrategy.VIEW
                || strategy == ReconstructionStrategy.TEMP_TABLE
                || strategy == ReconstructionStrategy.ROW_NUMBER_ALIGNMENT
                || "JOIN".equals(actualPlan) || "CTE_OR_WINDOW".equals(actualPlan) || "DERIVED_JOIN".equals(actualPlan)
                || "LEFT_OUTER_JOIN".equals(actualPlan) || "RIGHT_OUTER_JOIN".equals(actualPlan)
                || "VIEW".equals(actualPlan) || "TEMP_TABLE".equals(actualPlan)
                || "SEMIJOIN_DERIVED".equals(actualPlan)
                || "ROW_NUMBER_ALIGNMENT".equals(actualPlan);
    }

    private int rowCount(QueryResultMultiset result) {
        int count = 0;
        for (Integer value : result.rows.values()) {
            count += value;
        }
        return count;
    }

    /**
     * MDEV-41081 is the integer-index conversion family captured in mariadb808/12bug.log, mariadb809/11bug.log, and
     * mariadb810/database10.log. A quoted non-integral decimal is rejected by the direct path, while a helper ref
     * lookup rounds it to an integer key. The reconstruction shape varies, so key the rule to the actual indexed access
     * instead.
     */
    private boolean isMdev41081BooleanInIndexRange(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten) {
        if (!((strategy == ReconstructionStrategy.JOIN
                || strategy == ReconstructionStrategy.LEFT_RIGHT_JOIN
                && ("LEFT_OUTER_JOIN".equals(pair.actualPlan) || "RIGHT_OUTER_JOIN".equals(pair.actualPlan)))
                || (strategy == ReconstructionStrategy.STRAIGHT_JOIN && "STRAIGHT_JOIN".equals(pair.actualPlan))
                || (strategy == ReconstructionStrategy.VIEW && "VIEW".equals(pair.actualPlan)))
                || !direct.rows.isEmpty() || rewritten.rows.isEmpty()) {
            return false;
        }
        String sql = pair.direct.toUpperCase(Locale.ROOT);
        java.util.regex.Matcher matcher = Pattern
                .compile("(?:'[-+]?\\d+\\.\\d+'\\s+IN\\s*\\(\\s*`?([A-Z_][A-Z0-9_$]*)`?\\s*\\)"
                        + "|`?([A-Z_][A-Z0-9_$]*)`?\\s+IN\\s*\\(\\s*'[-+]?\\d+\\.\\d+'\\s*\\))")
                .matcher(sql);
        if (!matcher.find())
            return false;
        String columnName = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
        Column predicateColumn = null;
        for (Column column : pair.vp.plan.all) {
            if (column.name.equalsIgnoreCase(columnName)) {
                predicateColumn = column;
                break;
            }
        }
        if (predicateColumn == null || !isIntegerStorageType(predicateColumn)
                || !referencesIdentifier(sql, predicateColumn.name)) {
            return false;
        }
        if (helperUsesLeadingRefIndex(pair, predicateColumn)) {
            return true;
        }
        return strategy == ReconstructionStrategy.VIEW && helperUsesRefIndexContainingColumn(pair, predicateColumn);
    }

    private boolean isIntegerStorageType(Column column) {
        return column.type == MariaDBDataType.INT || column.type == MariaDBDataType.BOOLEAN;
    }

    /**
     * MDEV-41117 is the string/index conversion family captured in
     * mariadb809/16bug.log and mariadb810/database16.log. The rewrite-side
     * classifier remains limited to BIT columns; temporal and JSON-like
     * base/source cases are handled by isMdev41117StringIndexBaseSource().
     */
    private boolean isMdev41117BitStringInIndexRange(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten) {
        if (!isDerivedPlan(strategy, pair.actualPlan)
                || direct.rows.isEmpty() || !rewritten.rows.isEmpty()) {
            return false;
        }
        String sql = pair.direct.toUpperCase(Locale.ROOT);
        java.util.regex.Matcher matcher = Pattern.compile(
                "(?:'((?:[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12})|(?:\\{\\\"VP\\\":\\d+\\}))'"
                        + "\\s+IN\\s*\\(\\s*`?([A-Z_][A-Z0-9_$]*)`?\\s*\\)"
                        + "|`?([A-Z_][A-Z0-9_$]*)`?\\s+IN\\s*\\(\\s*'((?:[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12})|(?:\\{\\\"VP\\\":\\d+\\}))'\\s*\\))")
                .matcher(sql);
        if (!matcher.find()) {
            return false;
        }
        String columnName = matcher.group(2) != null ? matcher.group(2) : matcher.group(3);
        for (Column column : pair.vp.plan.all) {
            if (column.type != MariaDBDataType.BIT || !column.name.equalsIgnoreCase(columnName)
                    || !referencesIdentifier(sql, column.name)) {
                continue;
            }
            if (helperUsesLeadingRefIndex(pair, column)
                    || hasSourceIndexContainingColumn(pair, column.name)) {
                return true;
            }
        }
        return false;
    }

    /**
     * MDEV-41117 also appears through comparison operators and unary wrappers.
     * For those less syntactically stable forms, only classify the mismatch
     * after an index-free source scan reproduces the rewritten rows exactly.
     */
    private boolean isMdev41117QuotedComparisonIndexVariant(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten, String profile) {
        if (!isDerivedPlan(strategy, pair.actualPlan) || direct.rows.isEmpty() == rewritten.rows.isEmpty()) {
            return false;
        }
        String sql = pair.direct.toUpperCase(Locale.ROOT);
        boolean nullBooleanIn = Pattern.compile("(?is)IS\\s+NOT\\s+NULL\\s*\\)\\s+IN\\s*\\(").matcher(sql).find();
        for (Column column : pair.vp.plan.all) {
            boolean temporalNull = isTemporalStringIndexType(column.type)
                    && hasIsNullPredicateForColumn(sql, column.name);
            if (!isMdev41117StringIndexType(column) || !referencesIdentifier(sql, column.name)
                    || (!pair.direct.contains("'") && !nullBooleanIn && !temporalNull)) {
                continue;
            }
            if (sourceHasAnyIndexContainingColumn(pair, column.name)
                    && sourceScanMatches(pair, rewritten, profile)) {
                return true;
            }
            if ((hasQuotedComparisonForColumn(sql, column.name)
                    || (isTemporalStringIndexType(column.type)
                            && hasIsNullPredicateForColumn(sql, column.name)))
                    && helperScanMatches(pair, direct, profile)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Preserve source-index conversion cases where the scan repair is unstable across saved optimizer sessions.
     * Require a quoted IN predicate on the indexed BIT/temporal column and EXPLAIN evidence of the faulty access.
     */
    private boolean isMdev41117QuotedInIndexedPlan(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten, String profile) {
        if (direct.rows.isEmpty() == rewritten.rows.isEmpty()
                || !(isDerivedPlan(strategy, pair.actualPlan) || strategy == ReconstructionStrategy.STRAIGHT_JOIN
                        || strategy == ReconstructionStrategy.NATURAL_OR_USING)) {
            return false;
        }
        Connection connection = profileConnection(profile, false);
        if (connection == null) {
            return false;
        }
        for (Column column : pair.vp.plan.all) {
            if (!(column.type == MariaDBDataType.BIT || isTemporalStringIndexType(column.type))
                    || !hasQuotedInPredicateForColumn(pair.direct, column.name)
                    || !sourceHasAnyIndexContainingColumn(pair, column.name)) {
                continue;
            }
            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("EXPLAIN " + pair.direct)) {
                while (rs.next()) {
                    String table = stripIdentifierQuotes(rs.getString("table"));
                    String key = stripIdentifierQuotes(rs.getString("key"));
                    String access = rs.getString("type");
                    String extra = rs.getString("Extra");
                    boolean impossible = extra != null && extra.contains("Impossible WHERE noticed after reading const tables");
                    boolean indexedLookup = table != null
                            && table.equalsIgnoreCase(stripIdentifierQuotes(pair.vp.source))
                            && key != null && key.startsWith(pair.vp.prefix + "idx_src_")
                            && ("ref".equalsIgnoreCase(access) || "range".equalsIgnoreCase(access));
                    if (impossible || indexedLookup) {
                        // With a nonempty direct result, an EXPLAIN-only
                        // impossible plan does not explain the empty rewrite.
                        // Require a live repair rather than suppressing any
                        // empty VIEW result paired with this source plan.
                        return direct.rows.isEmpty() || helperScanMatches(pair, direct, profile)
                                || sourceScanMatches(pair, rewritten, profile);
                    }
                }
            } catch (SQLException ignored) {
                return false;
            }
        }
        return false;
    }

    /**
     * Saved mariadb819 cases 1, 10 and 17 are the same quoted-string
     * conversion defect, but the optimizer exposes it through different
     * plans: const-table pruning for temporal columns and a helper ref/hash
     * lookup for BIT columns. Keep the rule limited to a typed BIT/temporal
     * IN predicate and an actual reconstructed JOIN plan.
     */
    private boolean isMdev41117ReconstructionIndexConversion(QueryPair pair,
            ReconstructionStrategy strategy, QueryResultMultiset direct,
            QueryResultMultiset rewritten, String profile) {
        if (!isDerivedPlan(strategy, pair.actualPlan) || direct.rows.isEmpty() == rewritten.rows.isEmpty()) {
            return false;
        }
        Connection connection = profileConnection(profile, true);
        if (connection == null) {
            return false;
        }
        for (Column column : pair.vp.plan.all) {
            if (!(column.type == MariaDBDataType.BIT || isTemporalStringIndexType(column.type))
                    || !hasQuotedInPredicateForColumn(pair.direct, column.name)) {
                continue;
            }
            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("EXPLAIN " + pair.rewritten)) {
                boolean conversionPlan = false;
                while (rs.next()) {
                    String extra = rs.getString("Extra");
                    String key = stripIdentifierQuotes(rs.getString("key"));
                    String access = rs.getString("type");
                    conversionPlan |= extra != null && (extra.contains("no matching row in const table")
                            || extra.contains("Impossible WHERE noticed after reading const tables"));
                    conversionPlan |= key != null && key.contains(pair.vp.prefix + "idx_")
                            && ("ref".equalsIgnoreCase(access) || "range".equalsIgnoreCase(access)
                                    || "hash_index".equalsIgnoreCase(access));
                }
                if (conversionPlan) {
                    return helperScanMatches(pair, direct, profile)
                            || sourceScanMatches(pair, rewritten, profile);
                }
            } catch (SQLException ignored) {
                return false;
            }
        }
        return false;
    }

    static boolean hasQuotedInPredicateForColumn(String sql, String column) {
        String id = "`?" + Pattern.quote(column) + "`?";
        String wrappers = "(?:\\(\\s*\\+?\\s*)*" + id + "(?:\\s*\\))*";
        Pattern columnOnLeft = Pattern.compile("(?is)" + wrappers
                + "\\s+IN\\s*\\([^)]*'[^']+'[^)]*\\)");
        Pattern columnOnRight = Pattern.compile("(?is)'[^']+'\\s+IN\\s*\\(\\s*" + wrappers);
        return columnOnLeft.matcher(sql).find() || columnOnRight.matcher(sql).find();
    }

    private boolean helperScanMatches(QueryPair pair, QueryResultMultiset expected, String profile) {
        Connection connection = profileConnection(profile, true);
        if (connection == null) {
            return false;
        }
        // A VIEW hides its helper references from IGNORE INDEX insertion.
        // Expand only our saved generated definition, leaving the real view
        // untouched, so the repair probe also covers early impossible ranges.
        String scan = expandHelperScanView(pair);
        try {
            for (String helper : List.of(pair.vp.left, pair.vp.right)) {
                List<String> indexes = new ArrayList<>();
                try (Statement statement = connection.createStatement();
                        ResultSet rs = statement.executeQuery("SHOW INDEX FROM " + helper)) {
                    while (rs.next()) {
                        String key = rs.getString("Key_name");
                        if (!indexes.contains(key)) {
                            indexes.add(key);
                        }
                    }
                }
                if (indexes.isEmpty()) {
                    continue;
                }
                String hint = " IGNORE INDEX (" + indexes.stream().map(MariaDBVerticalPartitioningOracle::quote)
                        .collect(Collectors.joining(", ")) + ")";
                scan = scan.replaceAll("(" + Pattern.quote(helper) + ")( [lr])?",
                        "$1$2" + java.util.regex.Matcher.quoteReplacement(hint));
            }
            return scan.contains(" IGNORE INDEX (")
                    && queryResultsMatch(pair, expected, getResultMultiset(connection, scan, false));
        } catch (SQLException ignored) {
            return false;
        }
    }

    private String expandHelperScanView(QueryPair pair) {
        String createPrefix = "CREATE VIEW " + pair.vp.view + " AS ";
        for (String sql : pair.vp.setupStatements) {
            if (sql.startsWith(createPrefix)) {
                String definition = sql.substring(createPrefix.length()).replaceFirst(";\\s*$", "");
                return pair.rewritten.replace(pair.vp.view, "(" + definition + ")");
            }
        }
        return pair.rewritten;
    }

    static boolean hasNativeInOperand(String sql, String column) {
        int where = sql.toUpperCase(Locale.ROOT).indexOf(" WHERE ");
        if (where < 0) {
            return false;
        }
        String identifier = "(?<![A-Z0-9_$])`?" + Pattern.quote(column) + "`?(?![A-Z0-9_$])";
        return Pattern.compile("(?i)(?:" + identifier + "\\s+IN\\s*\\(|(?<!NOT\\s)\\bIN\\s*\\(\\s*"
                + identifier + "\\s*\\))").matcher(sql.substring(where)).find();
    }

    // Cover boolean/arithmetic IN operands and composite helper keys only when
    // the actual failing profile uses that key and removing indexes repairs all rows.
    private boolean isConfirmedNativeInIndexConversion(QueryPair pair, QueryResultMultiset direct,
            QueryResultMultiset rewritten, String profile) {
        if (direct.rows.isEmpty() == rewritten.rows.isEmpty()) {
            return false;
        }
        Connection connection = profileConnection(profile, true);
        if (connection == null) {
            return false;
        }
        for (Column column : pair.vp.plan.all) {
            if (!(column.type == MariaDBDataType.BIT || isTemporalStringIndexType(column.type))
                    || !hasNativeInOperand(pair.direct, column.name)) {
                continue;
            }
            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery("EXPLAIN " + pair.rewritten)) {
                boolean indexed = false;
                boolean pruned = false;
                while (rs.next()) {
                    String extra = rs.getString("Extra");
                    pruned |= extra != null && (extra.contains("no matching row in const table")
                            || extra.contains("Impossible WHERE noticed after reading const tables"));
                    String key = stripIdentifierQuotes(rs.getString("key"));
                    String access = rs.getString("type");
                    if (key == null || !("ref".equalsIgnoreCase(access) || "range".equalsIgnoreCase(access)
                            || "hash_index".equalsIgnoreCase(access))) {
                        continue;
                    }
                    for (String suffix : List.of("l", "r")) {
                        String indexPrefix = pair.vp.prefix + "idx_" + suffix + "_";
                        int start = key.indexOf(indexPrefix);
                        if (start >= 0) {
                            String index = key.substring(start).split(":", 2)[0];
                            indexed |= helperIndexContainsColumn(pair,
                                    suffix.equals("l") ? pair.vp.left : pair.vp.right, index, column.name);
                        }
                    }
                }
                boolean generatedPredicateIndex = pair.vp.setupStatements.stream().anyMatch(sql ->
                        Pattern.compile("(?is)^CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+`?"
                                + Pattern.quote(pair.vp.prefix) + "idx_[lr]_[^`\\s]+`?\\s+ON\\s+[^()]+\\([^)]*`?"
                                + Pattern.quote(column.name) + "`?\\s*[,)]").matcher(sql).find());
                if ((indexed || pruned && generatedPredicateIndex) && helperScanMatches(pair, direct, profile)) {
                    return true;
                }
            } catch (SQLException ignored) {
                return false;
            }
        }
        return false;
    }

    private boolean hasQuotedComparisonForColumn(String sql, String columnName) {
        String identifier = "`?" + Pattern.quote(columnName.toUpperCase(Locale.ROOT)) + "`?";
        String wrappedIdentifier = "\\(*\\s*\\+?\\s*" + identifier + "\\s*\\)*";
        String quoted = "'[^']+'";
        return Pattern.compile("(?is)(?:" + quoted + "\\s*(?:IN|[<>=]+)\\s*\\(?(?:" + wrappedIdentifier
                + ")|(?:" + wrappedIdentifier + ")\\s*(?:IN|[<>=]+)\\s*\\(?" + quoted + ")").matcher(sql).find();
    }

    /**
     * MDEV-41117 also affects DATE/DATETIME/TIMESTAMP columns when a quoted
     * value is converted through an indexed direct table access but the same
     * predicate is evaluated after the VP relation has been reconstructed.
     * Keep this restricted to quoted string IN predicates, a temporal column,
     * and a source/helper index access. Multi-literal IN lists are included
     * because the same conversion issue can affect one element of the list.
     * Other temporal mismatches (arithmetic, NULL handling, or non-indexed
     * expressions) must remain visible.
     */
    private boolean isMdev41117TemporalStringInIndexRange(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten, String optimizerProfileContext) {
        if (direct.rows.isEmpty() == rewritten.rows.isEmpty()
                || strategy == ReconstructionStrategy.SCALAR_SUBQUERY
                || strategy == ReconstructionStrategy.IN_FILTER
                || strategy == ReconstructionStrategy.EXISTS_FILTER
                || strategy == ReconstructionStrategy.NOT_EXISTS_ANTI) {
            return false;
        }
        String sql = pair.direct.toUpperCase(Locale.ROOT);
        // Empty strings take the temporal index-conversion path too. Require
        // an index-free source scan to reproduce the rewrite's exact rows.
        for (Column column : pair.vp.plan.all) {
            if (isTemporalStringIndexType(column.type)
                    && Pattern.compile("(?i)`?" + Pattern.quote(column.name)
                            + "`?\\s+IN\\s*\\(\\s*''\\s*\\)").matcher(pair.direct).find()
                    && hasSourceLeadingIndexContainingColumn(pair, column.name)
                    && sourceScanMatches(pair, rewritten, optimizerProfileContext)) {
                return true;
            }
        }
        // mariadb814/database3: mixed constant IN lists can contain nested
        // boolean expressions. Confirm this additional form on the same
        // profile connection by removing only the source index access.
        if (direct.rows.isEmpty() && !rewritten.rows.isEmpty()) {
            for (Column column : pair.vp.plan.all) {
                if (isTemporalStringIndexType(column.type)
                        && hasNestedStringInList(sql, column.name)
                        && hasSourceLeadingIndexContainingColumn(pair, column.name)
                        && sourceScanMatches(pair, rewritten, optimizerProfileContext)) {
                    return true;
                }
            }
        }
        java.util.regex.Matcher matcher = Pattern.compile(
                "(?:'([^']+)'\\s+IN\\s*\\(\\s*`?([A-Z_][A-Z0-9_$]*)`?\\s*\\)"
                        + "|`?([A-Z_][A-Z0-9_$]*)`?\\s+IN\\s*\\(\\s*'([^']+)'\\s*\\))")
                .matcher(sql);
        String columnName;
        if (matcher.find()) {
            columnName = matcher.group(2) != null ? matcher.group(2) : matcher.group(3);
        } else {
            java.util.regex.Matcher listMatcher = Pattern.compile(
                    "`?([A-Z_][A-Z0-9_$]*)`?\\s+IN\\s*\\(([^()]*)\\)")
                    .matcher(sql);
            String matchedColumn = null;
            while (listMatcher.find()) {
                if (listMatcher.group(2).contains("'")) {
                    matchedColumn = listMatcher.group(1);
                    break;
                }
            }
            if (matchedColumn != null) {
                columnName = matchedColumn;
            } else {
            /*
             * MDEV-41117 also manifests when the temporal comparison value is
             * a boolean OR expression, e.g. c3 IN (('date' OR c2)). The
             * direct full scan and the materialized derived ref lookup can
             * coerce the same expression differently.
             */
            java.util.regex.Matcher booleanMatcher = Pattern.compile(
                    "`?([A-Z_][A-Z0-9_$]*)`?\\s+IN\\s*\\(\\s*\\(\\s*'([^']+)'\\s+OR\\s+`?([A-Z_][A-Z0-9_$]*)`?\\s*\\)\\s*\\)")
                    .matcher(sql);
            if (!booleanMatcher.find()) {
                return false;
            }
            columnName = booleanMatcher.group(1);
            String booleanColumnName = booleanMatcher.group(3);
            boolean hasBooleanOperand = pair.vp.plan.all.stream().anyMatch(column -> column.type == MariaDBDataType.BOOLEAN
                    && column.name.equalsIgnoreCase(booleanColumnName)
                    && referencesIdentifier(sql, column.name));
            if (!hasBooleanOperand) {
                return false;
            }
            }
        }
        for (Column column : pair.vp.plan.all) {
            if (!column.name.equalsIgnoreCase(columnName) || !referencesIdentifier(sql, column.name)
                    || !isTemporalStringIndexType(column.type)) {
                continue;
            }
            if (helperUsesLeadingRefIndex(pair, column)
                    || helperUsesRefIndexContainingColumn(pair, column)
                    || hasSourceLeadingIndexContainingColumn(pair, column.name)
                    || isSemijoinOffAggregateIndexVariant(pair, strategy, column, optimizerProfileContext)) {
                return sourceScanMatches(pair, rewritten, optimizerProfileContext)
                        || helperScanMatches(pair, direct, optimizerProfileContext);
            }
        }
        return false;
    }

    private boolean isMdev41117TemporalNullIndexRange(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten, String profile) {
        if (!isDerivedPlan(strategy, pair.actualPlan) || direct.rows.isEmpty() || !rewritten.rows.isEmpty()) {
            return false;
        }
        String sql = pair.direct.toUpperCase(Locale.ROOT);
        for (Column column : pair.vp.plan.all) {
            if (!isTemporalStringIndexType(column.type)
                    || !hasIsNullPredicateForColumn(sql, column.name)
                    || !hasSourceIndexContainingColumn(pair, column.name)) {
                continue;
            }
            try {
                Connection connection = profileConnection(profile, false);
                if (connection == null) {
                    return false;
                }
                boolean indexedLookup = false;
                try (Statement statement = connection.createStatement();
                        ResultSet rs = statement.executeQuery("EXPLAIN " + pair.direct)) {
                    while (rs.next()) {
                        String key = stripIdentifierQuotes(rs.getString("key"));
                        String access = rs.getString("type");
                        indexedLookup |= key != null && key.startsWith(pair.vp.prefix + "idx_src_")
                                && ("ref".equalsIgnoreCase(access) || "range".equalsIgnoreCase(access));
                    }
                }
                if (indexedLookup && helperScanMatches(pair, direct, profile)) {
                    return true;
                }
            } catch (SQLException ignored) {
                return false;
            }
        }
        return false;
    }

    /**
     * mariadb820/database5 and database6: derived_with_keys creates an
     * implicit key0 on a materialized helper relation. Temporal values in an
     * IN expression are then converted differently from the native scan,
     * losing the zero-date row. Require a live derived_with_keys-off repair;
     * a key0 EXPLAIN alone is not sufficient evidence.
     */
    private boolean isMdev41117DerivedWithKeysTemporalConversion(QueryPair pair,
            ReconstructionStrategy strategy, QueryResultMultiset direct,
            QueryResultMultiset rewritten, String profile) {
        if (!isDerivedPlan(strategy, pair.actualPlan) || direct.rows.isEmpty() || !rewritten.rows.isEmpty()
                || !pair.direct.toUpperCase(Locale.ROOT).contains(" IN ")) {
            return false;
        }
        Connection connection = profileConnection(profile, true);
        if (connection == null) {
            return false;
        }
        boolean temporal = false;
        for (Column column : pair.vp.plan.all) {
            if (isTemporalStringIndexType(column.type) && referencesIdentifier(pair.direct, column.name)) {
                temporal = true;
                break;
            }
        }
        if (!temporal) {
            return false;
        }
        try (Statement statement = connection.createStatement()) {
            String original;
            try (ResultSet rs = statement.executeQuery("SELECT @@session.optimizer_switch")) {
                if (!rs.next()) {
                    return false;
                }
                original = rs.getString(1);
            }
            if (!original.contains("derived_with_keys=on")) {
                return false;
            }
            try {
                statement.execute("SET SESSION optimizer_switch='derived_with_keys=off'");
                QueryOutcome probe = executeProfileQuery(connection, pair.rewritten);
                return probe.succeeded() && queryResultsMatch(pair, direct, probe.result);
            } finally {
                statement.execute("SET SESSION optimizer_switch='" + original.replace("'", "''") + "'");
            }
        } catch (SQLException ignored) {
            return false;
        }
    }

    /**
     * Under semijoin=off the aggregate-then-merge form of MDEV-41117 can use
     * the helper's PRIMARY key for the final eq_ref join even though the
     * temporal predicate was planned using the generated leading index. The
     * ordinary classifier re-runs EXPLAIN on the oracle connection, whose
     * semijoin setting is different, so requiring an observable ref access
     * there misses this profile-only variant (database7_913 in mariadb813).
     * Keep the fallback specific to that profile and reconstruction shape.
     */
    private static boolean hasNestedStringInList(String sql, String columnName) {
        java.util.regex.Matcher matcher = Pattern.compile("(?i)(?<![A-Z0-9_$])`?"
                + Pattern.quote(columnName) + "`?\\s+IN\\s*\\(").matcher(sql);
        while (matcher.find()) {
            int depth = 1;
            char quoted = 0;
            boolean nested = false;
            boolean string = false;
            for (int i = matcher.end(); i < sql.length(); i++) {
                char c = sql.charAt(i);
                if (quoted != 0) {
                    if (c == '\\') {
                        i++;
                    } else if (c == quoted) {
                        if (i + 1 < sql.length() && sql.charAt(i + 1) == quoted) {
                            i++;
                        } else {
                            quoted = 0;
                        }
                    }
                } else if (c == '\'' || c == '"' || c == '`') {
                    quoted = c;
                    string |= c == '\'';
                } else if (c == '(') {
                    depth++;
                    nested = true;
                } else if (c == ')' && --depth == 0) {
                    if (nested && string) {
                        return true;
                    }
                    break;
                }
            }
        }
        return false;
    }

    private boolean isSemijoinOffAggregateIndexVariant(QueryPair pair, ReconstructionStrategy strategy,
            Column column, String optimizerProfileContext) {
        if (optimizerProfileContext == null || !optimizerProfileContext.contains("semijoin_off")
                || strategy != ReconstructionStrategy.AGGREGATE_THEN_MERGE) {
            return false;
        }
        if (contains(pair.vp.plan.left, column.name)
                && hasLeadingHelperIndex(pair, pair.vp.left, pair.vp.prefix + "idx_l_", column.name)) {
            return true;
        }
        return contains(pair.vp.plan.right, column.name)
                && hasLeadingHelperIndex(pair, pair.vp.right, pair.vp.prefix + "idx_r_", column.name);
    }

    private boolean isTemporalStringIndexType(MariaDBDataType type) {
        return type == MariaDBDataType.DATE || type == MariaDBDataType.DATETIME
                || type == MariaDBDataType.TIMESTAMP;
    }

    /**
     * MariaDB 12.3.2 can evaluate NULLIF((- TRUE), temporal_column) differently after the temporal column flows through
     * a derived/CTE reconstruction. The direct table path returns -1, while the reconstructed path can return 1 for the
     * same rows. Keep the classifier narrow and cell-based so unrelated temporal NULLIF mismatches still surface.
     */
    private boolean isMariaDBTemporalNullifDerivedTypeFlip(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten) {
        if (!(strategy == ReconstructionStrategy.CTE || strategy == ReconstructionStrategy.DERIVED_TABLE
                || strategy == ReconstructionStrategy.LEFT_RIGHT_JOIN || strategy == ReconstructionStrategy.VIEW)
                || direct.rows.isEmpty() || rewritten.rows.isEmpty()) {
            return false;
        }
        String sql = pair.direct.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
        if (!sql.contains("DISTINCTROW")) {
            return false;
        }
        boolean hasTemporalNullif = false;
        for (Column column : pair.vp.plan.all) {
            if ((column.type == MariaDBDataType.DATE || column.type == MariaDBDataType.DATETIME
                    || column.type == MariaDBDataType.TIMESTAMP)
                    && hasMinusTrueNullifForColumn(sql, column.name)) {
                hasTemporalNullif = true;
                break;
            }
        }
        if (!hasTemporalNullif || !isDerivedTemporalNullifPlan(pair.actualPlan)) {
            return false;
        }
        return hasOnlyExpectedCellDifferencesInSingleColumn(pair, direct, rewritten,
                (left, right, kind, column) -> isMinusOneOneFlip(left, right));
    }

    private boolean hasMinusTrueNullifForColumn(String sql, String columnName) {
        String quotedColumn = Pattern.quote(columnName.toUpperCase(Locale.ROOT));
        // The generated expression is commonly rendered as
        // NULLIF((- true), c0): one parenthesis belongs to NULLIF's argument
        // and another to the unary expression itself. Accept repeated
        // balanced-looking wrappers, but keep the operand and column strict.
        String pattern = "(?is)\\bNULLIF\\s*\\(\\s*\\(*\\s*-\\s*TRUE\\s*\\)*\\s*,\\s*`?"
                + quotedColumn + "`?\\s*\\)";
        return Pattern.compile(pattern).matcher(sql).find();
    }

    private boolean isDerivedTemporalNullifPlan(String actualPlan) {
        return "CTE_OR_WINDOW".equals(actualPlan) || "DERIVED_JOIN".equals(actualPlan)
                || "LEFT_OUTER_JOIN".equals(actualPlan) || "RIGHT_OUTER_JOIN".equals(actualPlan)
                || "VIEW".equals(actualPlan);
    }

    private boolean isMinusOneOneFlip(String left, String right) {
        return (isCanonicalScalar(left, "-1") && isCanonicalScalar(right, "1"))
                || (isCanonicalScalar(left, "1") && isCanonicalScalar(right, "-1"));
    }

    private boolean isCanonicalScalar(String value, String expected) {
        if (value == null) {
            return false;
        }
        int colon = value.lastIndexOf(':');
        return colon >= 0 ? expected.equals(value.substring(colon + 1)) : expected.equals(value);
    }

    /**
     * Require a mismatch-producing side to use a generated helper index whose first key part is the predicate column,
     * with EXPLAIN access=ref. A replicated column can legitimately have an index on either side.
     */
    private boolean helperUsesLeadingRefIndex(QueryPair pair, Column column) {
        boolean inLeft = contains(pair.vp.plan.left, column.name);
        boolean inRight = contains(pair.vp.plan.right, column.name);
        if (!inLeft && !inRight)
            return false;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("EXPLAIN " + pair.rewritten)) {
            List<String> indexedAliases = new ArrayList<>();
            if (inLeft)
                indexedAliases.add("l");
            if (inRight)
                indexedAliases.add("r");
            while (rs.next()) {
                String table = stripIdentifierQuotes(rs.getString("table"));
                String access = rs.getString("type");
                String key = stripIdentifierQuotes(rs.getString("key"));
                if (!"ref".equalsIgnoreCase(access) || key == null)
                    continue;
                for (String alias : indexedAliases) {
                    String suffix = alias.equals("l") ? "l" : "r";
                    String helper = alias.equals("l") ? pair.vp.left : pair.vp.right;
                    String expectedIndexPrefix = pair.vp.prefix + "idx_" + suffix + "_";
                    if (!hasLeadingHelperIndex(pair, helper, expectedIndexPrefix, column.name)
                            || (!alias.equalsIgnoreCase(table)
                                    && !stripIdentifierQuotes(helper).equalsIgnoreCase(table))) {
                        continue;
                    }
                    if (key.startsWith(expectedIndexPrefix))
                        return true;
                }
            }
        } catch (SQLException ignored) {
            return false;
        }
        return false;
    }

    private boolean helperUsesRefIndexContainingColumn(QueryPair pair, Column column) {
        boolean inLeft = contains(pair.vp.plan.left, column.name);
        boolean inRight = contains(pair.vp.plan.right, column.name);
        if (!inLeft && !inRight) {
            return false;
        }
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("EXPLAIN " + pair.rewritten)) {
            while (rs.next()) {
                String access = rs.getString("type");
                String key = stripIdentifierQuotes(rs.getString("key"));
                if (!"ref".equalsIgnoreCase(access) || key == null || !key.startsWith(pair.vp.prefix + "idx_")) {
                    continue;
                }
                String helper = key.startsWith(pair.vp.prefix + "idx_l_") ? pair.vp.left : pair.vp.right;
                if (helperIndexContainsColumn(pair, helper, key, column.name)) {
                    return true;
                }
            }
        } catch (SQLException ignored) {
            return false;
        }
        return false;
    }

    private boolean hasLeadingHelperIndex(QueryPair pair, String helper, String expectedIndexPrefix,
            String columnName) {
        String pattern = "(?is)^\\s*CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+`?" + Pattern.quote(expectedIndexPrefix)
                + "[^`\\s]*`?\\s+ON\\s+`?" + Pattern.quote(stripIdentifierQuotes(helper)) + "`?\\s*\\(\\s*`?"
                + Pattern.quote(columnName) + "`?(?:\\s*[,\\)])";
        Pattern indexPattern = Pattern.compile(pattern);
        return pair.vp.setupStatements.stream().anyMatch(indexPattern.asPredicate());
    }

    private boolean helperIndexContainsColumn(QueryPair pair, String helper, String indexName, String columnName) {
        String pattern = "(?is)^\\s*CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+`?" + Pattern.quote(indexName)
                + "`?\\s+ON\\s+`?" + Pattern.quote(stripIdentifierQuotes(helper)) + "`?\\s*\\([^)]*`?"
                + Pattern.quote(columnName) + "`?(?:\\s*[,\\)])";
        Pattern indexPattern = Pattern.compile(pattern);
        return pair.vp.setupStatements.stream().anyMatch(indexPattern.asPredicate());
    }

    private String stripIdentifierQuotes(String identifier) {
        if (identifier == null)
            return null;
        String result = identifier.trim();
        if (result.length() >= 2 && result.charAt(0) == '`' && result.charAt(result.length() - 1) == '`') {
            return result.substring(1, result.length() - 1).replace("``", "`");
        }
        return result;
    }

    @FunctionalInterface
    private interface CellDifferencePredicate {
        boolean accepts(String left, String right, ApproximateKind kind, int column);
    }

    /**
     * Known-pattern suppression is deliberately cell-based. SQL text and column definitions identify a candidate, but
     * cannot prove that an unrelated result column did not also change.
     */
    private boolean hasOnlyExpectedCellDifferences(QueryPair pair, QueryResultMultiset left, QueryResultMultiset right,
            CellDifferencePredicate predicate) {
        if (pair.orderedComparison) {
            if (left.orderedRows.size() != right.orderedRows.size())
                return false;
            boolean differs = false;
            for (int i = 0; i < left.orderedRows.size(); i++) {
                List<String> l = decodeCanonicalRow(left.orderedRows.get(i));
                List<String> r = decodeCanonicalRow(right.orderedRows.get(i));
                if (l.size() != r.size())
                    return false;
                for (int column = 0; column < l.size(); column++) {
                    if (!Objects.equals(l.get(column), r.get(column))) {
                        differs = true;
                        if (!predicate.accepts(l.get(column), r.get(column),
                                ApproximateKind.looser(getApproximateKind(orderedApprox(left, i), column),
                                        getApproximateKind(orderedApprox(right, i), column)),
                                column))
                            return false;
                    }
                }
            }
            return differs;
        }

        Map<String, Integer> remaining = new LinkedHashMap<>(right.rows);
        boolean differs = false;
        for (Map.Entry<String, Integer> entry : left.rows.entrySet()) {
            for (int count = 0; count < entry.getValue(); count++) {
                String matched = null;
                for (Map.Entry<String, Integer> candidate : remaining.entrySet()) {
                    if (candidate.getValue() <= 0)
                        continue;
                    if (rowsHaveOnlyExpectedDifferences(entry.getKey(), left.approximateColumns.get(entry.getKey()),
                            candidate.getKey(), right.approximateColumns.get(candidate.getKey()), predicate)) {
                        matched = candidate.getKey();
                        break;
                    }
                }
                if (matched == null)
                    return false;
                if (!entry.getKey().equals(matched))
                    differs = true;
                int remainingCount = remaining.get(matched);
                if (remainingCount == 1)
                    remaining.remove(matched);
                else
                    remaining.put(matched, remainingCount - 1);
            }
        }
        return differs && remaining.isEmpty();
    }

    private boolean hasOnlyExpectedCellDifferencesInSingleColumn(QueryPair pair, QueryResultMultiset left,
            QueryResultMultiset right, CellDifferencePredicate predicate) {
        if (pair.orderedComparison) {
            if (left.orderedRows.size() != right.orderedRows.size()) {
                return false;
            }
            Set<Integer> differenceColumns = new HashSet<>();
            boolean differs = false;
            for (int i = 0; i < left.orderedRows.size(); i++) {
                List<String> l = decodeCanonicalRow(left.orderedRows.get(i));
                List<String> r = decodeCanonicalRow(right.orderedRows.get(i));
                if (l.size() != r.size()) {
                    return false;
                }
                for (int column = 0; column < l.size(); column++) {
                    if (!Objects.equals(l.get(column), r.get(column))) {
                        differs = true;
                        differenceColumns.add(column);
                        if (!predicate.accepts(l.get(column), r.get(column),
                                ApproximateKind.looser(getApproximateKind(orderedApprox(left, i), column),
                                        getApproximateKind(orderedApprox(right, i), column)), column)) {
                            return false;
                        }
                    }
                }
            }
            return differs && differenceColumns.size() == 1;
        }

        Map<String, Integer> remaining = new LinkedHashMap<>(right.rows);
        Set<Integer> differenceColumns = new HashSet<>();
        boolean differs = false;
        for (Map.Entry<String, Integer> entry : left.rows.entrySet()) {
            for (int count = 0; count < entry.getValue(); count++) {
                String matched = null;
                for (Map.Entry<String, Integer> candidate : remaining.entrySet()) {
                    if (candidate.getValue() <= 0 || !rowsHaveOnlyExpectedDifferences(
                            entry.getKey(), left.approximateColumns.get(entry.getKey()), candidate.getKey(),
                            right.approximateColumns.get(candidate.getKey()), predicate)) {
                        continue;
                    }
                    matched = candidate.getKey();
                    break;
                }
                if (matched == null) {
                    return false;
                }
                List<String> l = decodeCanonicalRow(entry.getKey());
                List<String> r = decodeCanonicalRow(matched);
                for (int column = 0; column < l.size(); column++) {
                    if (!Objects.equals(l.get(column), r.get(column))) {
                        differs = true;
                        differenceColumns.add(column);
                    }
                }
                int remainingCount = remaining.get(matched);
                if (remainingCount == 1) {
                    remaining.remove(matched);
                } else {
                    remaining.put(matched, remainingCount - 1);
                }
            }
        }
        return differs && remaining.isEmpty() && differenceColumns.size() == 1;
    }

    private boolean rowsHaveOnlyExpectedDifferences(String left, List<ApproximateKind> leftKinds, String right,
            List<ApproximateKind> rightKinds, CellDifferencePredicate predicate) {
        List<String> l = decodeCanonicalRow(left), r = decodeCanonicalRow(right);
        if (l.size() != r.size())
            return false;
        for (int i = 0; i < l.size(); i++) {
            if (!Objects.equals(l.get(i), r.get(i)) && !predicate.accepts(l.get(i), r.get(i),
                    ApproximateKind.looser(getApproximateKind(leftKinds, i), getApproximateKind(rightKinds, i)), i)) {
                return false;
            }
        }
        return true;
    }

    private boolean isZeroFillPaddingDifference(String left, String right) {
        return left != null && right != null && !left.equals(right)
                && zeroFillCanonical(left).equals(zeroFillCanonical(right));
    }

    private String zeroFillCanonical(String value) {
        return value.replaceAll("(^|[^0-9])0+(?=[0-9])", "$1");
    }

    private boolean isExtremeDoubleCellDifference(String left, String right, ApproximateKind kind) {
        return left != null && right != null && isExtremeRoundToken(left) && isExtremeRoundToken(right)
                && (kind == ApproximateKind.DOUBLE || isMaxDoubleToPowerOfTenShift(left, right));
    }

    private boolean isMaxDoubleToPowerOfTenShift(String left, String right) {
        String max = "S:309:179769313486231570" + "0".repeat(291);
        String power = "S:18:100000000000000000";
        return max.equals(left) && power.equals(right) || max.equals(right) && power.equals(left);
    }

    private boolean isExtremeRoundToken(String value) {
        return value.contains("9223372036854776000") || value.contains("100000000000000000")
                || value.contains("179769313486231570");
    }

    private boolean isUnsignedDoubleCellDifference(String left, String right, ApproximateKind kind) {
        return kind == ApproximateKind.DOUBLE && left != null && right != null && !left.equals(right) && isNumeric(left)
                && isNumeric(right);
    }

    private boolean isTrimOrdCellDifference(String left, String right, ApproximateKind ignored) {
        if (left == null || right == null || left.equals(right) || !isNumeric(left) || !isNumeric(right))
            return false;
        return left.startsWith(right) || right.startsWith(left);
    }

    private boolean isNumeric(String value) {
        try {
            return Double.isFinite(Double.parseDouble(value));
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    private Set<Integer> extremeDoubleProjectionColumns(QueryPair pair) {
        Set<String> names = new HashSet<>();
        for (Column column : pair.vp.plan.all) {
            if (column.definition != null && column.definition.toUpperCase(Locale.ROOT).contains("DOUBLE")) {
                names.add(column.name);
            }
        }
        return projectionColumns(pair.direct, expression -> expression.contains("ROUND(")
                && (names.stream().anyMatch(name -> referencesIdentifier(expression, name))
                        || isBigintRoundLeftProjection(pair, expression)
                        || Pattern.compile("(?i)\\bROUND\\s*\\(\\s*'[^']+'\\s*\\)").matcher(expression).find()));
    }

    private boolean isBigintRoundLeftProjection(QueryPair pair, String expression) {
        return pair.vp.plan.all.stream().anyMatch(c -> c.definition != null
                && c.definition.toUpperCase(Locale.ROOT).contains("BIGINT")
                && Pattern.compile("(?i)\\bROUND\\s*\\(\\s*LEFT\\s*\\(\\s*`?"
                        + Pattern.quote(c.name) + "`?\\s*,").matcher(expression).find());
    }

    private Set<Integer> unsignedDoubleProjectionColumns(QueryPair pair) {
        Set<String> names = new HashSet<>();
        for (Column column : pair.vp.plan.all) {
            String definition = column.definition == null ? "" : column.definition.toUpperCase(Locale.ROOT);
            if (definition.contains("DOUBLE") && definition.contains("UNSIGNED"))
                names.add(column.name);
        }
        return projectionColumns(pair.direct, expression -> expression.contains("<<")
                && names.stream().anyMatch(name -> referencesIdentifier(expression, name)));
    }

    private Set<Integer> trimOrdProjectionColumns(QueryPair pair) {
        Set<String> names = new HashSet<>();
        for (Column column : pair.vp.plan.all) {
            if (column.type == MariaDBDataType.VARCHAR && isUtf8mb4Uca1400(column.definition))
                names.add(column.name);
        }
        return projectionColumns(pair.direct, expression -> names.stream().anyMatch(name -> {
            String identifier = Pattern.quote(name.toUpperCase(Locale.ROOT));
            return Pattern.compile("\\bTRIM\\s*\\(\\s*ORD\\s*\\(\\s*`?" + identifier + "`?\\s*\\)\\s*\\)")
                    .matcher(expression).find();
        }));
    }

    private Set<Integer> projectionColumns(String sql, java.util.function.Predicate<String> predicate) {
        String upper = sql.toUpperCase(Locale.ROOT);
        // ORDER BY wrappers preserve the inner projection positions. Inspect those positions rather than '*'.
        java.util.regex.Matcher wrapper = Pattern.compile(
                "(?s)^SELECT \\* FROM \\((SELECT .*)\\) [A-Z_][A-Z_0-9]*(?: ORDER BY [0-9, ]+)?$")
                .matcher(upper);
        if (wrapper.matches())
            return projectionColumns(wrapper.group(1), predicate);
        if (!upper.startsWith("SELECT "))
            return Set.of();
        int from = topLevelFrom(upper);
        if (from < 0)
            return Set.of();
        List<String> expressions = topLevelElements("(" + upper.substring("SELECT ".length(), from) + ")");
        if (expressions.stream().anyMatch(expression -> expression.trim().equals("*")))
            return Set.of();
        Set<Integer> result = new HashSet<>();
        for (int i = 0; i < expressions.size(); i++)
            if (predicate.test(expressions.get(i)))
                result.add(i);
        return result;
    }

    private int topLevelFrom(String sql) {
        int depth = 0;
        char quoteCharacter = 0;
        for (int i = "SELECT ".length(); i <= sql.length() - " FROM ".length(); i++) {
            char current = sql.charAt(i);
            if (quoteCharacter != 0) {
                if (current == quoteCharacter && (i == 0 || sql.charAt(i - 1) != '\\'))
                    quoteCharacter = 0;
                continue;
            }
            if (current == '\'' || current == '`' || current == '"')
                quoteCharacter = current;
            else if (current == '(')
                depth++;
            else if (current == ')')
                depth--;
            else if (depth == 0 && sql.startsWith(" FROM ", i))
                return i;
        }
        return -1;
    }

    /**
     * MariaDB 12.3 materialization changes ROUND() of DOUBLE near 2^63 from 9223372036854776000 to
     * 100000000000000000. This is a known value/display boundary defect, not ordinary floating-point tolerance.
     * Restrict this suppression to the exact documented boundary values so unrelated numeric mismatches remain
     * actionable candidates.
     */
    private boolean hasExtremeDoubleRoundDisplayShift(QueryPair pair, ReconstructionStrategy strategy,
            QueryResultMultiset direct, QueryResultMultiset rewritten) {
        if (!isJoinMaterializingStrategy(strategy) || !pair.direct.toUpperCase(Locale.ROOT).contains("ROUND(")) {
            return false;
        }
        boolean hasDouble = pair.vp.plan.all.stream()
                .anyMatch(c -> c.definition != null && c.definition.toUpperCase(Locale.ROOT).contains("DOUBLE"));
        if (!hasDouble && projectionColumns(pair.direct,
                expression -> isBigintRoundLeftProjection(pair, expression)
                        || Pattern.compile("(?i)\\bROUND\\s*\\(\\s*'[^']+'\\s*\\)").matcher(expression).find())
                .isEmpty()) {
            return false;
        }
        return hasExtremeRoundToken(direct.rows) && hasExtremeRoundToken(rewritten.rows);
    }

    private boolean hasExtremeRoundToken(Map<String, Integer> rows) {
        for (String row : rows.keySet()) {
            if (isExtremeRoundToken(row)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasUnsignedDoubleScalarShift(QueryPair pair, ReconstructionStrategy strategy) {
        if (strategy != ReconstructionStrategy.SCALAR_SUBQUERY) {
            return false;
        }
        String direct = pair.direct.toUpperCase(Locale.ROOT);
        String rewritten = pair.rewritten.toUpperCase(Locale.ROOT);
        if (!direct.contains("<<") || !rewritten.contains("SELECT R.")) {
            return false;
        }
        for (Column column : pair.vp.plan.all) {
            if (column.definition == null) {
                continue;
            }
            String definition = column.definition.toUpperCase(Locale.ROOT);
            if (definition.contains("DOUBLE") && definition.contains("UNSIGNED")
                    && referencesIdentifier(direct, column.name) && referencesIdentifier(rewritten, column.name)) {
                return true;
            }
        }
        return false;
    }

    private boolean hasTrimOrdTemporaryTruncation(QueryPair pair, ReconstructionStrategy strategy) {
        if (!isJoinMaterializingStrategy(strategy)) {
            return false;
        }

        String sql = pair.direct.toUpperCase(Locale.ROOT);
        for (Column column : pair.vp.plan.all) {
            if (column.type != MariaDBDataType.VARCHAR || !isUtf8mb4Uca1400(column.definition)) {
                continue;
            }
            String identifier = Pattern.quote(column.name.toUpperCase(Locale.ROOT));
            String expression = "\\bTRIM\\s*\\(\\s*ORD\\s*\\(\\s*`?" + identifier + "`?\\s*\\)\\s*\\)";
            if (Pattern.compile(expression).matcher(sql).find()) {
                return true;
            }
        }
        return false;
    }

    private boolean isJoinMaterializingStrategy(ReconstructionStrategy strategy) {
        switch (strategy) {
        case JOIN:
        case LEFT_RIGHT_JOIN:
        case FULL_OUTER_SIMULATION:
        case DERIVED_TABLE:
        case CTE:
        case VIEW:
        case TEMP_TABLE:
        case AGGREGATE_THEN_MERGE:
        case SCALAR_SUBQUERY:
        case EXISTS_FILTER:
        case IN_FILTER:
        case NOT_EXISTS_ANTI:
        case ROW_NUMBER_ALIGNMENT:
        case STRING_PACK:
        case JSON_PACK:
        case NATURAL_OR_USING:
        case STRAIGHT_JOIN:
        case LATERAL_DERIVED:
            return true;
        default:
            return false;
        }
    }

    private boolean isUtf8mb4Uca1400(String definition) {
        if (definition == null) {
            return false;
        }
        String normalized = definition.toUpperCase(Locale.ROOT);
        return normalized.contains("UTF8MB4") && normalized.contains("UCA1400_AI_CI");
    }

    private boolean hasZeroFillObservable(QueryPair pair, ReconstructionStrategy strategy) {
        String sql = pair.direct.toUpperCase(Locale.ROOT);
        /*
         * Profile checks reuse the original QueryPair but may execute a
         * relation produced by a different reconstruction plan (for example
         * a VIEW while the pair's selected strategy is AGGREGATE_THEN_MERGE).
         * The known rule is still safe here because the caller additionally
         * requires every changed cell to be a pure zero-fill padding change.
         * Keep the strategy argument for source compatibility, but do not
         * reject an otherwise exact ZEROFILL display-only mismatch merely
         * because the profile relation differs from the pair's plan.
         */
        if (strategy == null) {
            return false;
        }
        return hasZeroFillColumnReferenced(pair, sql);
    }

    private boolean hasZeroFillColumnReferenced(QueryPair pair) {
        return hasZeroFillColumnReferenced(pair, pair.direct.toUpperCase(Locale.ROOT));
    }

    private boolean hasZeroFillColumnReferenced(QueryPair pair, String sql) {
        // Returning a ZEROFILL column itself is observable through JDBC: the
        // confirmed cases project such a column through a materializing relation.
        return pair.vp.plan.all.stream()
                .anyMatch(column -> column.definition != null
                        && column.definition.toUpperCase(Locale.ROOT).contains("ZEROFILL")
                        && referencesIdentifier(sql, column.name));
    }

    private boolean referencesIdentifier(String sql, String name) {
        String identifier = Pattern.quote(name.toUpperCase(Locale.ROOT));
        return Pattern.compile("(?<![A-Z0-9_$])" + identifier + "(?![A-Z0-9_$])").matcher(sql).find();
    }

    private QueryResultMultiset getResultMultiset(String sql) throws SQLException {
        return getResultMultiset(state.getConnection().getJdbcConnection(), sql, true);
    }

    private QueryResultMultiset getResultMultiset(Connection connection, String sql, boolean accountQuery)
            throws SQLException {
        Map<String, Integer> rows = new LinkedHashMap<>();
        Map<String, List<ApproximateKind>> approximateColumns = new LinkedHashMap<>();
        List<String> orderedRows = new ArrayList<>();
        List<List<ApproximateKind>> orderedApproximateColumns = new ArrayList<>();
        // These two VP SELECTs are issued through JDBC directly because the result
        // set must be consumed twice for tolerant comparison. Keep SQLancer's
        // global query progress counter consistent with the actual work performed.
        if (accountQuery) {
            state.getManager().incrementSelectQueryCount();
        }
        // This path intentionally uses JDBC directly (the result set is consumed
        // with metadata for tolerant comparison), so log the SELECT explicitly;
        // otherwise cur.log contains only setup DDL/DML and hides the actual VP probes.
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent(sql);
        }
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int count = meta.getColumnCount();
            while (rs.next()) {
                String row = canonicalizeRow(rs, meta, count);
                List<ApproximateKind> approx = approximateColumns(meta, count);
                orderedRows.add(row);
                orderedApproximateColumns.add(approx);
                rows.put(row, rows.getOrDefault(row, 0) + 1);
                approximateColumns.putIfAbsent(row, approx);
            }
        } catch (SQLException e) {
            logLine("vp-query-errors.log", "sql=" + sql + " error=" + e.getMessage());
            if (isResourceLimitThrowable(e)) {
                logResourceQueryError(connection, sql, e);
            }
            throw e;
        }
        return new QueryResultMultiset(rows, approximateColumns, orderedRows, orderedApproximateColumns);
    }

    private void logResourceQueryError(Connection connection, String sql, SQLException error) {
        String configuration;
        try {
            configuration = readResourceSessionState(connection).toString();
        } catch (SQLException snapshotError) {
            // Diagnostics must preserve the original query failure.
            configuration = "unavailable: " + snapshotError.getMessage();
        }
        logLine("vp-resource-errors.log", "error=" + error.getMessage() + " session=" + configuration + " sql=" + sql);
    }

    static Map<String, String> readResourceSessionState(Connection connection) throws SQLException {
        Map<String, String> values = new LinkedHashMap<>();
        String query = "SELECT CONNECTION_ID() AS connection_id, DATABASE() AS database_name, "
                + "@@session.sort_buffer_size AS sort_buffer_size, @@session.max_sort_length AS max_sort_length, "
                + "@@session.max_length_for_sort_data AS max_length_for_sort_data, "
                + "@@session.tmp_table_size AS tmp_table_size, @@session.max_heap_table_size AS max_heap_table_size, "
                + "@@session.max_allowed_packet AS max_allowed_packet, @@session.join_buffer_size AS join_buffer_size, "
                + "@@session.join_buffer_space_limit AS join_buffer_space_limit, @@session.sql_mode AS sql_mode, "
                + "@@session.character_set_connection AS character_set_connection, "
                + "@@session.collation_connection AS collation_connection, @@session.optimizer_switch AS optimizer_switch";
        try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(query)) {
            if (result.next()) {
                ResultSetMetaData metadata = result.getMetaData();
                for (int i = 1; i <= metadata.getColumnCount(); i++) {
                    values.put(metadata.getColumnLabel(i), result.getString(i));
                }
            }
        }
        return values;
    }

    private String canonicalizeRow(ResultSet rs, ResultSetMetaData meta, int count) throws SQLException {
        StringBuilder row = new StringBuilder();
        for (int i = 1; i <= count; i++) {
            byte[] binary = isBinaryResult(meta.getColumnType(i)) ? rs.getBytes(i) : null;
            byte[] character = isCharacterResult(meta.getColumnType(i)) ? rs.getBytes(i) : null;
            String value = binary == null ? rs.getString(i) : null;
            if (binary != null) {
                // MariaDB's JDBC driver renders a base BIT column as b'0101'
                // but a scalar BIT expression as decimal-looking text, even
                // when both carry the same bytes. Compare binary SQL results
                // by their actual payload without changing the tested SQL.
                row.append("B:").append(binary.length).append(':');
                for (byte item : binary)
                    row.append(String.format(Locale.ROOT, "%02x", item & 0xff));
                row.append(';');
            } else if (character != null) {
                /*
                 * MariaDB Connector/J can expose the same CHAR/VARCHAR value
                 * differently through MyISAM temporary snapshots and
                 * materialized InnoDB/MyISAM source tables (notably trailing
                 * spaces and case under a *_ai_ci collation).  Compare the
                 * returned character payload, rather than getString()'s
                 * display representation, so this transport artifact cannot
                 * become a VP mismatch.
                 */
                row.append("C:").append(character.length).append(':');
                for (byte item : character)
                    row.append(String.format(Locale.ROOT, "%02x", item & 0xff));
                row.append(';');
            } else if (value == null) {
                row.append("N;");
            } else {
                // Preserve the exact JDBC display value. In MariaDB this is
                // observable for ZEROFILL and other display-sensitive types;
                // numeric tolerance is applied later, per result-column.
                row.append("S:").append(value.length()).append(":").append(value).append(";");
            }
        }
        return row.toString();
    }

    private boolean isBinaryResult(int jdbcType) {
        return jdbcType == java.sql.Types.BIT || jdbcType == java.sql.Types.BINARY
                || jdbcType == java.sql.Types.VARBINARY || jdbcType == java.sql.Types.LONGVARBINARY;
    }

    private boolean isCharacterResult(int jdbcType) {
        return jdbcType == java.sql.Types.CHAR || jdbcType == java.sql.Types.VARCHAR
                || jdbcType == java.sql.Types.LONGVARCHAR || jdbcType == java.sql.Types.NCHAR
                || jdbcType == java.sql.Types.NVARCHAR || jdbcType == java.sql.Types.LONGNVARCHAR;
    }

    private List<ApproximateKind> approximateColumns(ResultSetMetaData meta, int count) throws SQLException {
        List<ApproximateKind> result = new ArrayList<>();
        for (int i = 1; i <= count; i++)
            result.add(approximateKind(meta.getColumnTypeName(i)));
        return result;
    }

    private ApproximateKind approximateKind(String type) {
        if (type == null)
            return ApproximateKind.NONE;
        String upper = type.toUpperCase(Locale.ROOT);
        if (upper.contains("FLOAT") || upper.contains("REAL"))
            return ApproximateKind.FLOAT;
        if (upper.contains("DOUBLE"))
            return ApproximateKind.DOUBLE;
        return ApproximateKind.NONE;
    }

    private boolean resultMultisetsMatch(QueryResultMultiset left, QueryResultMultiset right) {
        if (left.rows.equals(right.rows))
            return true;
        Map<String, Integer> remaining = new LinkedHashMap<>(right.rows);
        for (Map.Entry<String, Integer> entry : left.rows.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                String match = findApproximateRowMatch(entry.getKey(), left.approximateColumns.get(entry.getKey()),
                        remaining, right.approximateColumns);
                if (match == null)
                    return false;
                int count = remaining.get(match);
                if (count == 1)
                    remaining.remove(match);
                else
                    remaining.put(match, count - 1);
            }
        }
        return remaining.isEmpty();
    }

    private boolean isDistinctQuery(String sql) {
        return sql != null && sql.toUpperCase(Locale.ROOT).contains("DISTINCT");
    }

    private boolean queryResultsMatch(QueryPair pair, QueryResultMultiset left, QueryResultMultiset right) {
        boolean exact = pair.orderedComparison ? orderedRowsMatch(left, right) : resultMultisetsMatch(left, right);
        return exact || (!pair.orderedComparison && isDistinctQuery(pair.direct)
                && distinctCharacterRepresentativesMatch(pair, left, right));
    }

    private boolean distinctCharacterRepresentativesMatch(QueryPair pair, QueryResultMultiset left,
            QueryResultMultiset right) {
        if (left.orderedRows.size() != right.orderedRows.size() || left.rows.isEmpty()) {
            return false;
        }
        try {
            List<String> collations = distinctOutputCollations(pair.direct);
            if (collations.isEmpty()) {
                return false;
            }
            return normalizedDistinctRows(collations, left.rows).equals(normalizedDistinctRows(collations, right.rows));
        } catch (SQLException e) {
            return false;
        }
    }

    private List<String> distinctOutputCollations(String sql) throws SQLException {
        if (sql.startsWith("SELECT * FROM (")) {
            int end = sql.lastIndexOf(") q ORDER BY ");
            if (end < 0) {
                return List.of();
            }
            sql = sql.substring("SELECT * FROM (".length(), end);
        }
        if (!sql.matches("(?is)^SELECT\\s+DISTINCT(?:ROW)?\\s+.*")) {
            return List.of();
        }
        int from = topLevelFrom(sql);
        if (from < 0) {
            return List.of();
        }
        String projection = sql.substring("SELECT ".length(), from)
                .replaceFirst("(?is)^DISTINCT(?:ROW)?\\s+", "");
        List<String> expressions = topLevelElements("(" + projection + ")");
        List<String> aliases = new ArrayList<>();
        List<String> probes = new ArrayList<>();
        for (int i = 0; i < expressions.size(); i++) {
            String expression = expressions.get(i).replaceFirst("(?is)\\s+AS\\s+`vp_ast_col_\\d+`$", "");
            if (expression.equals("*")) {
                return List.of();
            }
            String alias = "vp_collation_" + i;
            aliases.add(expression + " AS " + quote(alias));
            probes.add("COLLATION(" + quote(alias) + ")");
        }
        String probe = "SELECT " + String.join(", ", probes) + " FROM (SELECT DISTINCT "
                + String.join(", ", aliases) + sql.substring(from) + ") vp_collations LIMIT 1";
        List<String> result = new ArrayList<>();
        try (Statement statement = state.getConnection().createStatement(); ResultSet rs = statement.executeQuery(probe)) {
            if (rs.next()) {
                for (int i = 1; i <= expressions.size(); i++) {
                    result.add(rs.getString(i));
                }
            }
        }
        return result;
    }

    private Map<String, Integer> normalizedDistinctRows(List<String> collations, Map<String, Integer> rows)
            throws SQLException {
        Map<String, Integer> result = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : rows.entrySet()) {
            StringBuilder normalized = new StringBuilder();
            List<String> values = decodeCanonicalRow(entry.getKey());
            for (int index = 0; index < values.size(); index++) {
                String value = values.get(index);
                if (value == null) {
                    normalized.append("N;");
                } else if (value.startsWith("C:")) {
                    if (index >= collations.size() || collations.get(index) == null
                            || !collations.get(index).matches("[a-zA-Z0-9_]+")) {
                        throw new SQLException("Missing output collation");
                    }
                    String collation = collations.get(index);
                    if (collation.equalsIgnoreCase("binary")) {
                        normalized.append(value).append(';');
                        continue;
                    }
                    String charset = collation.substring(0, collation.indexOf('_'));
                    String expression = "CONVERT(UNHEX('" + value.substring(2) + "') USING " + charset + ")";
                    // PAD SPACE ignores ASCII spaces only. NOPAD and binary outputs retain their bytes.
                    if (!collation.toLowerCase(Locale.ROOT).contains("nopad")) {
                        expression = "RTRIM(" + expression + ")";
                    }
                    try (Statement statement = state.getConnection().createStatement();
                            ResultSet rs = statement.executeQuery("SELECT HEX(WEIGHT_STRING(" + expression
                                    + " COLLATE " + collation + "))")) {
                        if (!rs.next()) {
                            throw new SQLException("Missing collation weight");
                        }
                        normalized.append("C:").append(rs.getString(1)).append(';');
                    }
                } else {
                    normalized.append(value).append(';');
                }
            }
            result.put(normalized.toString(), result.getOrDefault(normalized.toString(), 0) + entry.getValue());
        }
        return result;
    }

    private String findApproximateRowMatch(String row, List<ApproximateKind> leftApproximateColumns,
            Map<String, Integer> candidates, Map<String, List<ApproximateKind>> candidateApproximateColumns) {
        Integer exact = candidates.get(row);
        if (exact != null && exact > 0)
            return row;
        for (Map.Entry<String, Integer> candidate : candidates.entrySet()) {
            if (candidate.getValue() > 0 && rowsApproximatelyEqual(row, leftApproximateColumns, candidate.getKey(),
                    candidateApproximateColumns.get(candidate.getKey())))
                return candidate.getKey();
        }
        return null;
    }

    private boolean rowsApproximatelyEqual(String left, List<ApproximateKind> leftApproximateColumns, String right,
            List<ApproximateKind> rightApproximateColumns) {
        List<String> leftValues = decodeCanonicalRow(left);
        List<String> rightValues = decodeCanonicalRow(right);
        if (leftValues.size() != rightValues.size())
            return false;
        for (int i = 0; i < leftValues.size(); i++) {
            String l = leftValues.get(i), r = rightValues.get(i);
            if (Objects.equals(l, r))
                continue;
            ApproximateKind kind = ApproximateKind.looser(getApproximateKind(leftApproximateColumns, i),
                    getApproximateKind(rightApproximateColumns, i));
            if (!kind.isApproximate() || l == null || r == null || !numericValuesApproximatelyEqual(l, r, kind)) {
                return false;
            }
        }
        return true;
    }

    private ApproximateKind getApproximateKind(List<ApproximateKind> approximateColumns, int index) {
        if (approximateColumns == null || index >= approximateColumns.size())
            return ApproximateKind.NONE;
        return approximateColumns.get(index);
    }

    private List<String> decodeCanonicalRow(String row) {
        List<String> values = new ArrayList<>();
        int idx = 0;
        while (idx < row.length()) {
            if (row.startsWith("N;", idx)) {
                values.add(null);
                idx += 2;
            } else if (row.startsWith("S:", idx) || row.startsWith("B:", idx) || row.startsWith("C:", idx)) {
                boolean binary = row.charAt(idx) == 'B';
                boolean character = row.charAt(idx) == 'C';
                int lengthStart = idx + 2;
                int lengthEnd = row.indexOf(':', lengthStart);
                if (lengthEnd < 0)
                    throw new AssertionError("Malformed VP row encoding: " + row);
                int length = Integer.parseInt(row.substring(lengthStart, lengthEnd));
                int valueStart = lengthEnd + 1, valueEnd = valueStart + ((binary || character) ? length * 2 : length);
                if (valueEnd >= row.length() || row.charAt(valueEnd) != ';') {
                    throw new AssertionError("Malformed VP row encoding: " + row);
                }
                values.add((binary ? "B:" : character ? "C:" : "") + row.substring(valueStart, valueEnd));
                idx = valueEnd + 1;
            } else {
                throw new AssertionError("Malformed VP row encoding: " + row);
            }
        }
        return values;
    }

    private boolean numericValuesApproximatelyEqual(String left, String right, ApproximateKind kind) {
        try {
            double l = Double.parseDouble(left), r = Double.parseDouble(right);
            if (!Double.isFinite(l) || !Double.isFinite(r))
                return false;
            if (kind == ApproximateKind.DOUBLE) {
                if (l == r)
                    return true;
                double difference = Math.abs(l - r);
                double ulp = Math.max(Math.ulp(l), Math.ulp(r));
                return difference <= kind.maxUlps * ulp;
            }
            double diff = Math.abs(l - r);
            double scale = Math.max(Math.abs(l), Math.abs(r));
            return diff <= kind.absoluteTolerance || diff <= kind.relativeTolerance * scale;
        } catch (NumberFormatException ignored) {
            return false;
        }
    }

    private boolean orderedRowsMatch(QueryResultMultiset left, QueryResultMultiset right) {
        if (left.orderedRows.size() != right.orderedRows.size())
            return false;
        for (int i = 0; i < left.orderedRows.size(); i++) {
            if (left.orderedRows.get(i).equals(right.orderedRows.get(i)))
                continue;
            if (!rowsApproximatelyEqual(left.orderedRows.get(i), orderedApprox(left, i), right.orderedRows.get(i),
                    orderedApprox(right, i)))
                return false;
        }
        return true;
    }

    private List<ApproximateKind> orderedApprox(QueryResultMultiset rows, int index) {
        if (index >= rows.orderedApproximateColumns.size())
            return List.of();
        return rows.orderedApproximateColumns.get(index);
    }

    private void cleanup(VpTables vp) {
        for (String sql : List.of("DROP VIEW IF EXISTS " + vp.view, "DROP TEMPORARY TABLE IF EXISTS " + vp.temp,
                "DROP TEMPORARY TABLE IF EXISTS " + vp.baseSnapshot, "DROP TABLE IF EXISTS " + vp.source,
                "DROP TABLE IF EXISTS " + vp.left, "DROP TABLE IF EXISTS " + vp.right)) {
            try {
                execute(sql);
            } catch (Exception ignored) {
            }
        }
    }

    private void prepareSession() {
        // Preserve the session environment produced by MariaDBSetGenerator.
        // Each named optimizer assignment below changes only that switch, so
        // unrelated randomized switches, sql_mode, time_zone, and buffer sizes
        // remain part of the tested execution environment.
        if (state.getDbmsSpecificOptions().verticalPartitioningMariaDBFeatures) {
            String[] switches = { "rowid_filter", "condition_pushdown_for_derived", "hash_join_cardinality",
                    "outer_join_with_cache", "split_materialized", "mrr", "mrr_cost_based" };
            for (String name : switches) {
                String value = Randomly.getBoolean() ? "on" : "off";
                try {
                    execute("SET SESSION optimizer_switch = '" + name + "=" + value + "'");
                } catch (Exception ignored) {
                    // MariaDB minor versions differ in supported switches.
                }
            }
        }
    }

    static Map<String, String> optimizerProfiles() {
        Map<String, String> profiles = new LinkedHashMap<>();
        for (String name : List.of("derived_merge", "semijoin", "materialization",
                "condition_pushdown_for_derived", "index_merge")) {
            profiles.put(name + "_on", name);
            profiles.put(name + "_off", name);
        }
        return profiles;
    }

    /**
     * Compare one query under a baseline session and a small set of optimizer
     * profiles. Each profile uses a separate JDBC session, with the relevant
     * session environment copied from the oracle connection. Unsupported
     * switches are skipped after checking @@optimizer_switch.
     */
    private void runOptimizerProfileChecks(QueryPair pair) throws Exception {
        if (!state.getDbmsSpecificOptions().verticalPartitioningOptimizerProfiles) {
            return;
        }
        String switchState;
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("SELECT @@optimizer_switch")) {
            if (!rs.next()) {
                return;
            }
            switchState = rs.getString(1);
        }
        Map<String, Boolean> available = parseOptimizerSwitches(switchState);
        Map<String, String> sessionState = readProfileSessionState();
        List<ProfileRun> successfulProfiles = new ArrayList<>();
        for (Map.Entry<String, String> profile : optimizerProfiles().entrySet()) {
            if (!available.containsKey(profile.getValue())) {
                increment(OPTIMIZER_PROFILE_COUNTS, profile.getKey() + "=unsupported");
                continue;
            }
            increment(OPTIMIZER_PROFILE_COUNTS, profile.getKey());
            ProfileComparison profiled = executeOptimizerProfilePair(profile.getKey(), pair, switchState, profile.getValue(),
                    profile.getKey().endsWith("_on"), sessionState);
            if (profiled.setupError != null) {
                String reason = profileSkipReason(profiled.setupError);
                increment(OPTIMIZER_PROFILE_SKIP_COUNTS, profile.getKey() + ":" + reason);
                logLine("vp-optimizer-profile-skips.log",
                        "profile=" + profile.getKey() + " stage=setup error=" + errorText(profiled.setupError)
                                .replace('\n', ' ') + " direct=" + pair.direct + " rewritten=" + pair.rewritten);
                logLine("vp-optimizer-profile-skips.log",
                        "profile=" + profile.getKey() + " setup-sql=" + profileSetupStatements(pair).stream()
                                .collect(Collectors.joining(" || ")));
                continue;
            }
            if (!profiled.direct.succeeded() || !profiled.rewritten.succeeded()) {
                Connection profileConnection = optimizerProfileConnection(profile.getKey());
                if (handleResourceOutcomes(pair, profiled.direct, profiled.rewritten,
                        () -> executeProfileQuery(profileConnection, pair.direct),
                        () -> executeProfileQuery(profileConnection, pair.rewritten), "profile:" + profile.getKey())) {
                    increment(OPTIMIZER_PROFILE_SKIP_COUNTS, profile.getKey() + ":resource");
                    continue;
                }
                if (isProfileInfrastructureError(profiled.direct.error)
                        || isProfileInfrastructureError(profiled.rewritten.error)) {
                    increment(OPTIMIZER_PROFILE_SKIP_COUNTS, profile.getKey());
                    logLine("vp-optimizer-profile-skips.log",
                            "profile=" + profile.getKey() + " stage=query error="
                                    + errorText(profiled.direct.error != null ? profiled.direct.error
                                            : profiled.rewritten.error).replace('\n', ' '));
                    continue;
                }
                if (profiled.direct.succeeded() != profiled.rewritten.succeeded()
                        || !sameSqlException(profiled.direct.error, profiled.rewritten.error)) {
                    increment(OPTIMIZER_PROFILE_MISMATCH_COUNTS, profile.getKey() + ":error");
                    throw new AssertionError("Optimizer profile VP outcome mismatch\nProfile: " + profile.getKey()
                            + "\nDirect: " + outcomeText(profiled.direct) + "\nRewritten: "
                            + outcomeText(profiled.rewritten)
                            + "\noptimizer_switch baseline: " + switchState);
                }
                String reason = profileSkipReason(profiled.direct.error != null ? profiled.direct.error
                        : profiled.rewritten.error);
                increment(OPTIMIZER_PROFILE_SKIP_COUNTS, profile.getKey() + ":" + reason);
                continue;
            }
            increment(OPTIMIZER_PROFILE_COUNTS, profile.getKey() + ":checked");
            successfulProfiles.add(new ProfileRun(profile.getKey(), profiled));
            boolean same = queryResultsMatch(pair, profiled.direct.result, profiled.rewritten.result);
            if (!same) {
                String known = classifyKnown(pair, pair.strategy, profiled.direct.result, profiled.rewritten.result,
                        profile.getKey());
                if (state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns && known != null) {
                    increment(KNOWN_COUNTS, known);
                    increment(OPTIMIZER_PROFILE_MISMATCH_COUNTS, profile.getKey() + ":known");
                    logKnownBug(pair, known, profiled.direct.result, profiled.rewritten.result);
                    continue;
                }
                increment(OPTIMIZER_PROFILE_MISMATCH_COUNTS, profile.getKey());
                throw new AssertionError("Optimizer profile VP mismatch\nProfile: " + profile.getKey()
                        + "\nDirect query: " + pair.direct + "\nRewritten query: " + pair.rewritten
                        + "\nDirect sample: " + sample(profiled.direct.result.rows)
                        + "\nRewritten sample: " + sample(profiled.rewritten.result.rows)
                        + "\noptimizer_switch baseline: " + switchState
                        + "\nApplied optimizer_switch: "
                        + optimizerProfileSwitch(switchState, profile.getValue(), profile.getKey().endsWith("_on"))
                        + "\nDirect EXPLAIN: " + explainProfileSummary(profile.getKey(), pair.direct)
                        + "\nRewritten EXPLAIN: " + explainProfileSummary(profile.getKey(), pair.rewritten));
            }
        }
        if (state.getDbmsSpecificOptions().verticalPartitioningOptimizerCrossProfile) {
            for (ProfileRun directProfile : successfulProfiles) {
                for (ProfileRun rewrittenProfile : successfulProfiles) {
                    if (directProfile == rewrittenProfile)
                        continue;
                    String comparison = directProfile.name + "->" + rewrittenProfile.name;
                    increment(OPTIMIZER_PROFILE_COUNTS, "cross:" + comparison + ":checked");
                    if (!queryResultsMatch(pair, directProfile.comparison.direct.result,
                            rewrittenProfile.comparison.rewritten.result)) {
                        String known = classifyKnown(pair, pair.strategy, directProfile.comparison.direct.result,
                                rewrittenProfile.comparison.rewritten.result, comparison);
                        if (state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns && known != null) {
                            increment(KNOWN_COUNTS, known);
                            increment(OPTIMIZER_PROFILE_MISMATCH_COUNTS, "cross-known:" + comparison);
                            logKnownBug(pair, known, directProfile.comparison.direct.result,
                                    rewrittenProfile.comparison.rewritten.result);
                            continue;
                        }
                        increment(OPTIMIZER_PROFILE_MISMATCH_COUNTS, "cross:" + comparison);
                        throw new AssertionError("Cross-profile VP mismatch\nDirect profile: " + directProfile.name
                                + "\nRewritten profile: " + rewrittenProfile.name + "\nDirect query: " + pair.direct
                                + "\nRewritten query: " + pair.rewritten
                                + "\nDirect sample: " + sample(directProfile.comparison.direct.result.rows)
                                + "\nRewritten sample: " + sample(rewrittenProfile.comparison.rewritten.result.rows)
                                + "\noptimizer_switch baseline: " + switchState
                                + "\nDirect optimizer_switch: " + optimizerProfileSwitch(switchState,
                                        optimizerProfiles().get(directProfile.name), directProfile.name.endsWith("_on"))
                                + "\nRewritten optimizer_switch: " + optimizerProfileSwitch(switchState,
                                        optimizerProfiles().get(rewrittenProfile.name), rewrittenProfile.name.endsWith("_on"))
                                + "\nProfile session state: " + sessionState
                                + "\nDirect EXPLAIN: " + explainProfileSummary(directProfile.name, pair.direct)
                                + "\nRewritten EXPLAIN: " + explainProfileSummary(rewrittenProfile.name, pair.rewritten));
                    }
                }
            }
        }
    }

    private static final class ProfileComparison {
        final QueryOutcome direct;
        final QueryOutcome rewritten;
        final Throwable setupError;

        ProfileComparison(QueryOutcome direct, QueryOutcome rewritten, Throwable setupError) {
            this.direct = direct;
            this.rewritten = rewritten;
            this.setupError = setupError;
        }
    }

    private static final class ProfileRun {
        final String name;
        final ProfileComparison comparison;

        ProfileRun(String name, ProfileComparison comparison) {
            this.name = name;
            this.comparison = comparison;
        }
    }

    private Map<String, Boolean> parseOptimizerSwitches(String value) {
        Map<String, Boolean> result = new LinkedHashMap<>();
        for (String token : value.split(",")) {
            String[] parts = token.trim().split("=", 2);
            if (parts.length == 2) {
                result.put(parts[0].trim().toLowerCase(Locale.ROOT), "on".equalsIgnoreCase(parts[1].trim()));
            }
        }
        return result;
    }

    private Map<String, String> readProfileSessionState() throws SQLException {
        return readProfileSessionState(state.getConnection().getJdbcConnection());
    }

    static Map<String, String> readProfileSessionState(Connection connection) throws SQLException {
        Map<String, String> values = new LinkedHashMap<>();
        List<String> variables = List.of("sql_mode", "time_zone", "lc_time_names", "character_set_client",
                "character_set_results", "character_set_connection", "collation_connection", "sort_buffer_size",
                "max_sort_length", "max_length_for_sort_data", "tmp_table_size", "max_heap_table_size",
                "join_buffer_size", "join_buffer_space_limit", "join_cache_level", "optimizer_prune_level",
                "optimizer_search_depth", "sql_buffer_result", "sql_auto_is_null", "eq_range_index_dive_limit",
                "max_seeks_for_key");
        String query = "SELECT " + variables.stream().map(v -> "@@session." + v).collect(Collectors.joining(", "));
        try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(query)) {
            if (result.next()) {
                for (int i = 0; i < variables.size(); i++) {
                    values.put(variables.get(i), result.getString(i + 1));
                }
            }
        }
        return values;
    }

    static void applyProfileSessionState(Connection connection, Map<String, String> values) throws SQLException {
        Set<String> textVariables = Set.of("sql_mode", "time_zone", "lc_time_names", "character_set_client",
                "character_set_results", "character_set_connection", "collation_connection");
        try (Statement statement = connection.createStatement()) {
            for (Map.Entry<String, String> entry : values.entrySet()) {
                String value = entry.getValue();
                String literal;
                if (value == null) {
                    literal = "NULL";
                } else if (textVariables.contains(entry.getKey())) {
                    literal = "'" + value.replace("'", "''") + "'";
                } else if (value.matches("[0-9]+")) {
                    literal = value;
                } else {
                    throw new SQLException("Invalid numeric profile session value: " + entry.getKey());
                }
                statement.execute("SET SESSION " + entry.getKey() + "=" + literal);
            }
        }
    }

    private ProfileComparison executeOptimizerProfilePair(String profileName, QueryPair pair, String baselineSwitch,
            String targetSwitch, boolean enabled, Map<String, String> sessionState) {
        Connection connection = null;
        try {
            connection = optimizerProfileConnection(profileName);
            applyProfileSessionState(connection, sessionState);
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET SESSION optimizer_switch = '"
                        + optimizerProfileSwitch(baselineSwitch, targetSwitch, enabled).replace("'", "''") + "'");
                try {
                    for (String setup : profileSetupStatements(pair)) {
                        if (!setup.startsWith("--")) {
                            statement.execute(setup);
                        }
                    }
                } catch (Throwable setupFailure) {
                    QueryOutcome failed = new QueryOutcome(null, setupFailure);
                    return new ProfileComparison(failed, failed, setupFailure);
                }
            }
            QueryOutcome direct = executeProfileQuery(connection, pair.direct);
            QueryOutcome rewritten = executeProfileQuery(connection, pair.rewritten);
            return new ProfileComparison(direct, rewritten, null);
        } catch (Throwable throwable) {
            if (connection != null && isProfileInfrastructureError(throwable)) {
                closeOptimizerProfileConnection(profileName);
            }
            QueryOutcome failed = new QueryOutcome(null, throwable);
            return new ProfileComparison(failed, failed, throwable);
        }
    }

    private Connection optimizerProfileConnection(String profileName) throws SQLException {
        Connection connection = optimizerProfileConnections.get(profileName);
        if (connection != null && !connection.isClosed() && connection.isValid(1)) {
            return connection;
        }
        closeOptimizerProfileConnection(profileName);
        String host = state.getOptions().getHost();
        int port = state.getOptions().getPort();
        if (host == null) {
            host = sqlancer.mariadb.MariaDBOptions.DEFAULT_HOST;
        }
        if (port == sqlancer.MainOptions.NO_SET_PORT) {
            port = sqlancer.mariadb.MariaDBOptions.DEFAULT_PORT;
        }
        String url = String.format(Locale.ROOT, "jdbc:mariadb://%s:%d/%s", host, port, state.getDatabaseName());
        connection = DriverManager.getConnection(url, state.getOptions().getUserName(),
                state.getOptions().getPassword());
        optimizerProfileConnections.put(profileName, connection);
        return connection;
    }

    private void closeOptimizerProfileConnections() {
        for (String profileName : new ArrayList<>(optimizerProfileConnections.keySet())) {
            closeOptimizerProfileConnection(profileName);
        }
    }

    private void closeOptimizerProfileConnection(String profileName) {
        Connection connection = optimizerProfileConnections.remove(profileName);
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
            }
        }
    }

    private List<String> profileSetupStatements(QueryPair pair) {
        String baseSnapshot = pair.vp.baseSnapshot;
        String source = pair.vp.source;
        String left = pair.vp.left;
        String right = pair.vp.right;
        String temp = pair.vp.temp;
        String view = pair.vp.view;
        List<String> result = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (String sql : pair.profileSetup) {
            if (!seen.add(sql))
                continue;
            // A profile connection is independent from the oracle connection.
            // It must receive the complete reconstructed relation, including
            // source/left/right table DDL and population statements.  The old
            // filter only copied snapshot/temp/view statements, leaving the
            // partition tables empty (or absent) and producing systematic
            // false VP mismatches in optimizer-profile checks.
            if (sql.contains(baseSnapshot) || sql.contains(source) || sql.contains(left)
                    || sql.contains(right) || sql.contains(temp) || sql.contains(view)
                    || sql.startsWith("DROP TEMPORARY TABLE") || sql.startsWith("CREATE TEMPORARY TABLE")
                    || sql.startsWith("DROP VIEW") || sql.startsWith("CREATE VIEW")) {
                result.add(sql);
            }
        }
        return result;
    }

    private boolean isProfileInfrastructureError(Throwable throwable) {
        if (throwable == null)
            return false;
        String message = errorText(throwable).toLowerCase(Locale.ROOT);
        return message.contains("doesn't exist") || message.contains("unknown database")
                || message.contains("connection") || message.contains("communications link")
                || message.contains("optimizer_switch") || message.contains("access denied");
    }

    private String profileSkipReason(Throwable throwable) {
        if (throwable == null)
            return "unknown";
        String message = errorText(throwable).toLowerCase(Locale.ROOT);
        if (message.contains("doesn't exist"))
            return "missing_object";
        if (message.contains("optimizer_switch"))
            return "optimizer_switch";
        if (message.contains("connection") || message.contains("communications link"))
            return "connection";
        if (isResourceLimitThrowable(throwable))
            return "resource";
        if (message.contains("syntax") || message.contains("parse"))
            return "syntax";
        return "other";
    }

    private QueryOutcome executeProfileQuery(Connection connection, String sql) {
        try {
            return new QueryOutcome(getResultMultiset(connection, sql, false), null);
        } catch (Throwable throwable) {
            return new QueryOutcome(null, throwable);
        }
    }

    static String optimizerProfileSwitch(String baselineSwitch, String targetSwitch, boolean enabled) {
        Map<String, String> values = new LinkedHashMap<>();
        for (String token : baselineSwitch.split(",")) {
            String trimmed = token.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            String[] parts = trimmed.split("=", 2);
            String name = parts[0].trim();
            values.put(name.toLowerCase(Locale.ROOT), parts.length == 2 ? parts[1].trim() : "on");
        }
        values.put(targetSwitch.toLowerCase(Locale.ROOT), enabled ? "on" : "off");
        /*
         * MariaDB rejects a session in which both materialization and
         * in_to_exists are disabled.  Randomized session setup can leave
         * in_to_exists=off in the baseline. Fix this companion to ON in both
         * materialization profiles, so their common environment is legal and
         * only the target switch differs.
         */
        if ("materialization".equalsIgnoreCase(targetSwitch)
                && values.containsKey("in_to_exists")) {
            values.put("in_to_exists", "on");
        }
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            if (result.length() > 0) {
                result.append(',');
            }
            result.append(entry.getKey()).append('=').append(entry.getValue());
        }
        return result.toString();
    }

    private String explainProfileSummary(String profileName, String sql) {
        // These sessions belong to optimizerProfileConnections and are closed by its lifecycle.
        // Diagnostics must inspect the existing profile, rather than reconnecting with different settings.
        Connection connection = optimizerProfileConnections.get(profileName);
        return connection == null ? "unavailable:profile session missing" : explainSummary(connection, sql);
    }

    private String explainSummary(Connection connection, String sql) {
        StringBuilder summary = new StringBuilder();
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("EXPLAIN " + sql)) {
            while (rs.next()) {
                if (summary.length() > 0) {
                    summary.append(" | ");
                }
                summary.append(rs.getString("table")).append(':').append(rs.getString("type")).append(':')
                        .append(rs.getString("key"));
            }
        } catch (SQLException e) {
            return "unavailable:" + errorText(e).replace('\n', ' ');
        }
        return summary.toString();
    }

    private boolean advancedEnabled() {
        return state.getDbmsSpecificOptions().verticalPartitioningAdvancedQueryFeatures
                || state.getDbmsSpecificOptions().verticalPartitioningDeepExploration;
    }

    private boolean deepEnabled() {
        return state.getDbmsSpecificOptions().verticalPartitioningDeepExploration;
    }

    private boolean isSkippableThrowable(Throwable throwable) {
        if (throwable instanceof IgnoreMeException)
            return true;
        if (!(throwable instanceof SQLException))
            return false;
        String message = errorText(throwable).toLowerCase(Locale.ROOT);
        // Never hide server, connection, transaction, or assertion failures.
        // These indicate infrastructure/database health problems, not an
        // invalid generated query.
        if (message.contains("server has gone away") || message.contains("lost connection")
                || message.contains("connection reset") || message.contains("connection refused")
                || message.contains("communications link failure") || message.contains("deadlock")
                || message.contains("lock wait timeout") || message.contains("lock wait abort")
                || message.contains("internal error") || message.contains("assertion") || message.contains("crash")
                || message.contains("broken pipe") || message.contains("connection is closed")
                || message.contains("query timeout")) {
            return false;
        }
        return message.contains("syntax") || message.contains("not supported") || message.contains("unknown column")
                || message.contains("doesn't exist") || message.contains("invalid json")
                || message.contains("data truncated") || message.contains("too long")
                || message.contains("duplicate column name")
                // Only errors with an explicit conversion/encoding context are
                // generator failures. A generic "out of range" can be a real
                // type/overflow semantic difference and must be surfaced.
                || message.contains("bigint value is out of range")
                || message.contains("unsigned value is out of range")
                || message.contains("value is out of range in 'cast")
                || message.contains("integer value is out of range") || message.contains("invalid integer")
                || message.contains("division by 0") || message.contains("regex error")
                || message.contains("quantifier does not follow") || message.contains("could not create a join buffer")
                || message.contains("illegal mix of collations") || message.contains("not in select list")
                || message.contains("invalid use of group function")
                || message.contains("subquery returns more than 1 row") || message.contains("out of sort memory")
                || message.contains("illegal parameter data type") || message.contains("illegal parameter data types")
                || message.contains("lateral") && (message.contains("syntax") || message.contains("not supported"))
                || message.contains("window") && (message.contains("not allowed") || message.contains("invalid use"))
                || message.contains("can't reopen table") || message.contains("operand should contain")
                || message.contains("doesn't support savepoint") || message.contains("does not support savepoint");
    }

    private boolean isResourceLimitThrowable(Throwable throwable) {
        if (findSqlException(throwable) == null) {
            return false;
        }
        String message = errorText(throwable).toLowerCase(Locale.ROOT);
        return message.contains("out of sort memory") || message.contains("could not create a join buffer");
    }

    private String errorText(Throwable throwable) {
        StringBuilder sb = new StringBuilder();
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null)
                sb.append(current.getMessage()).append('\n');
            current = current.getCause();
        }
        return sb.toString();
    }

    private void recordSkip(ReconstructionStrategy strategy, Throwable throwable) {
        String text = errorText(throwable);
        String reason = text.isEmpty() ? throwable.getClass().getSimpleName() : text.split("\\R", 2)[0];
        String category = throwable instanceof IgnoreMeException ? "IgnoreMeException" : skipCategory(reason);
        increment(SKIP_COUNTS, strategy + ": " + category);
        if (!(throwable instanceof IgnoreMeException))
            logSkipSample(strategy + " reason=" + reason, category);
    }

    private String skipCategory(String reason) {
        String normalized = reason.toLowerCase(Locale.ROOT);
        if (normalized.contains("bigint value is out of range"))
            return "BIGINT_OUT_OF_RANGE";
        if (normalized.contains("regex error"))
            return "REGEX_ERROR";
        if (normalized.contains("duplicate column"))
            return "DUPLICATE_COLUMN";
        if (normalized.contains("unknown column"))
            return "UNKNOWN_COLUMN";
        if (normalized.contains("out of sort memory"))
            return "SORT_MEMORY";
        if (normalized.contains("illegal parameter data type"))
            return "INVALID_PARAMETER_TYPE";
        if (normalized.contains("doesn't exist"))
            return "MISSING_OBJECT";
        if (normalized.contains("not supported") || normalized.contains("syntax"))
            return "UNSUPPORTED_SQL";
        if (normalized.contains("data truncated"))
            return "DATA_TRUNCATED";
        if (normalized.contains("out of range"))
            return "VALUE_OUT_OF_RANGE";
        return throwableClassReason(reason);
    }

    private String throwableClassReason(String reason) {
        int separator = reason.indexOf(':');
        return separator > 0 ? reason.substring(0, separator).trim().toUpperCase(Locale.ROOT) : "OTHER";
    }

    private void logSkipSample(String line, String category) {
        synchronized (LOGGED_SKIP_SAMPLES) {
            if (!LOGGED_SKIP_SAMPLES.add(category))
                return;
        }
        logLine("vp-skips.log", "sample " + line);
    }

    private void recordCompletedVpRun(int checkedPairs, int attempts) {
        synchronized (STATS_LOCK) {
            completedOracleRuns++;
            completedQueryPairs += checkedPairs;
            completedAttempts += attempts;
        }
    }

    private void recordSkippedVpRun(int checkedPairs, int attempts, IgnoreMeException throwable) {
        synchronized (STATS_LOCK) {
            completedQueryPairs += checkedPairs;
            completedAttempts += attempts;
        }
        increment(SKIP_COUNTS, "oracle: " + throwable.getClass().getSimpleName());
        // IgnoreMeException is the normal "this generated case is not applicable"
        // path. Keep its aggregate count in vp-stats.log without writing one line
        // for every skipped oracle run.
    }

    private static void recordSchemaCopySuccess() {
        synchronized (STATS_LOCK) {
            schemaCopySuccesses++;
        }
    }

    private static void recordSchemaCopyFailure() {
        synchronized (STATS_LOCK) {
            schemaCopyFailures++;
        }
    }

    private static void recordEngineFallback() {
        synchronized (STATS_LOCK) {
            engineFallbacks++;
        }
    }

    private long count(String table) throws SQLException {
        String relation = isQuotedIdentifier(table) ? table : quote(table);
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM " + relation)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private boolean isQuotedIdentifier(String value) {
        return value.length() >= 2 && value.charAt(0) == '`' && value.charAt(value.length() - 1) == '`';
    }

    private String stableOrder(MariaDBTable base, List<Column> columns) {
        List<String> order = new ArrayList<>();
        for (MariaDBColumn column : base.getColumns()) {
            if (column.isPrimaryKey())
                order.add(quote(column.getName()));
        }
        // Sorting by every column makes wide JSON/LONGTEXT tables exceed
        // MariaDB's small sort buffer before the oracle can run. A primary
        // key gives deterministic row numbering; without one, preserve the
        // engine's row order and let the single materialized snapshot define
        // the correspondence.
        return String.join(", ", order);
    }

    private String baseSnapshotRelation(VpTables vp) {
        return vp.baseSnapshot;
    }

    private void maybeCreateHelperIndexes(VpTables vp) {
        if (!advancedEnabled())
            return;
        maybeCreateHelperIndex(vp, vp.source, vp.plan.all, "src");
        maybeCreateHelperIndex(vp, vp.left, vp.plan.left, "l");
        maybeCreateHelperIndex(vp, vp.right, vp.plan.right, "r");
    }

    private void maybeCreateHelperIndex(VpTables vp, String table, List<Column> columns, String suffix) {
        List<Column> indexable = columns.stream().filter(c -> c.type != MariaDBDataType.VARCHAR)
                .collect(Collectors.toList());
        if (indexable.isEmpty() || !Randomly.getBoolean())
            return;
        Collections.shuffle(indexable);
        List<String> parts = new ArrayList<>();
        if (Randomly.getBoolean())
            parts.add(quote(ROW_ID));
        parts.add(quote(indexable.get(0).name));
        if (!parts.contains(quote(ROW_ID)) && Randomly.getBoolean())
            parts.add(quote(ROW_ID));
        String sql = "CREATE INDEX "
                + quote(vp.prefix + "idx_" + suffix + "_" + Math.abs(state.getRandomly().getInteger())) + " ON " + table
                + "(" + String.join(", ", parts) + ")";
        try {
            executeSetup(vp, sql);
        } catch (Throwable e) {
            vp.setupStatements.add("-- skipped helper index: " + sql + " reason=" + errorText(e).replace('\n', ' '));
            recordSkip(ReconstructionStrategy.JOIN, e);
        }
    }

    private void runDeepFeatureChecks(VpTables vp) {
        List<Column> varcharColumns = vp.plan.all.stream().filter(c -> c.type == MariaDBDataType.VARCHAR)
                .collect(Collectors.toList());
        if (!varcharColumns.isEmpty()) {
            Column c = Randomly.fromList(varcharColumns);
            try {
                compare(new QueryPair(vp, ReconstructionStrategy.JOIN,
                        // PAD SPACE collations treat values such as '' and ' ' as equal. Use a binary
                        // sort key so this ordered probe does not report input-order differences.
                        "SELECT " + quote(c.name) + " COLLATE utf8mb4_bin FROM " + vp.source + " ORDER BY CAST("
                                + quote(c.name) + " AS BINARY)",
                        "SELECT " + quote(c.name) + " COLLATE utf8mb4_bin FROM "
                                + relation(vp, ReconstructionStrategy.JOIN, new ArrayList<>()) + " ORDER BY CAST("
                                + quote(c.name) + " AS BINARY)",
                        "deep collation probe", true), vp.splitStrategy, ReconstructionStrategy.JOIN);
            } catch (Throwable e) {
                if (!isSkippableThrowable(e))
                    throw new AssertionError(e);
                recordSkip(ReconstructionStrategy.JOIN, e);
            }
        }
        try {
            compare(new QueryPair(vp, ReconstructionStrategy.ROW_NUMBER_ALIGNMENT,
                    "SELECT ROW_NUMBER() OVER (ORDER BY `" + ROW_ID + "`) FROM " + vp.source,
                    "SELECT ROW_NUMBER() OVER (ORDER BY `" + ROW_ID + "`) FROM "
                            + relation(vp, ReconstructionStrategy.ROW_NUMBER_ALIGNMENT, new ArrayList<>()),
                    "deep window probe", true), vp.splitStrategy, ReconstructionStrategy.ROW_NUMBER_ALIGNMENT);
        } catch (Throwable e) {
            if (!isSkippableThrowable(e))
                throw new AssertionError(e);
            recordSkip(ReconstructionStrategy.ROW_NUMBER_ALIGNMENT, e);
        }
    }

    /**
     * Run write-side VP probes without changing the read oracle. The source and
     * reconstructed relations are read from the already materialized fixture,
     * while every mutation is directed at a fresh temporary target and rolled
     * back or dropped before the next probe.
     */
    private void runDmlTransactionProbes(VpTables vp) {
        List<DmlScenario> scenarios = List.of(DmlScenario.INSERT_SELECT, DmlScenario.INSERT_IGNORE,
                DmlScenario.ON_DUPLICATE_KEY_UPDATE, DmlScenario.REPLACE, DmlScenario.SAVEPOINT_ROLLBACK,
                DmlScenario.RETURNING);
        String completionType = null;
        try {
            try {
                completionType = singleString("SELECT @@session.completion_type");
                execute("SET SESSION completion_type='NO_CHAIN'");
            } catch (Throwable e) {
                if (!isSkippableDmlFailure(e))
                    throw new AssertionError("MariaDB VP DML transaction setup failed", e);
                recordSkip(ReconstructionStrategy.CTE, e);
                return;
            }
            for (DmlScenario scenario : scenarios) {
                if (scenario == DmlScenario.SAVEPOINT_ROLLBACK && !supportsSavepoints(vp.sourceEngine)) {
                    increment(SKIP_COUNTS, ReconstructionStrategy.CTE + ": SAVEPOINT_UNSUPPORTED_ENGINE");
                    continue;
                }
                try {
                    DmlScenarioResult direct = runDmlScenario(vp, vp.source, scenario, "d");
                    DmlScenarioResult rewritten = runDmlScenario(vp,
                            relation(vp, ReconstructionStrategy.CTE, new ArrayList<>()), scenario, "r");
                    compareDmlScenario(vp, scenario, direct, rewritten);
                } catch (Throwable e) {
                    if (!isSkippableDmlFailure(e))
                        throw new AssertionError("MariaDB VP DML/transaction probe failed: " + scenario, e);
                    recordSkip(ReconstructionStrategy.CTE, e);
                }
            }
        } finally {
            if (completionType != null) {
                try {
                    execute("SET SESSION completion_type='" + completionType.replace("'", "''") + "'");
                } catch (Throwable ignored) {
                }
            }
        }
    }

    private boolean supportsSavepoints(MariaDBEngine engine) {
        return engine == MariaDBEngine.INNO_DB;
    }

    private boolean isSkippableDmlFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (isSkippableThrowable(current))
                return true;
            current = current.getCause();
        }
        return false;
    }

    private DmlScenarioResult runDmlScenario(VpTables vp, String relation, DmlScenario scenario, String side)
            throws Exception {
        String target = quote(vp.prefix + "dml_" + scenario.name().toLowerCase(Locale.ROOT) + "_" + side);
        String columns = dmlColumnList(vp);
        DmlScenarioResult result = new DmlScenarioResult();
        boolean transactionOpen = false;
        try {
            execute("DROP TEMPORARY TABLE IF EXISTS " + target);
            execute("CREATE TEMPORARY TABLE " + target + " LIKE " + vp.source);
            try (Statement statement = state.getConnection().createStatement()) {
                statement.execute("START TRANSACTION");
                transactionOpen = true;
                if (scenario == DmlScenario.INSERT_SELECT || scenario == DmlScenario.RETURNING) {
                    result.steps.add(executeDmlStep(statement,
                            "INSERT INTO " + target + " (" + columns + ") SELECT " + columns + " FROM " + relation
                                    + (scenario == DmlScenario.RETURNING ? " RETURNING *" : "")));
                } else {
                    result.steps.add(executeDmlStep(statement,
                            "INSERT INTO " + target + " (" + columns + ") SELECT " + columns + " FROM " + relation));
                    if (scenario == DmlScenario.SAVEPOINT_ROLLBACK) {
                        statement.execute("SAVEPOINT vp_dml_probe");
                        result.steps.add(executeDmlStep(statement,
                                "INSERT IGNORE INTO " + target + " (" + columns + ") SELECT " + columns + " FROM "
                                        + relation));
                        result.duringSavepoint = getResultMultiset(
                                "SELECT * FROM " + target + " ORDER BY `" + ROW_ID + "`");
                        statement.execute("ROLLBACK TO SAVEPOINT vp_dml_probe");
                        result.afterRollbackToSavepoint = getResultMultiset(
                                "SELECT * FROM " + target + " ORDER BY `" + ROW_ID + "`");
                    } else {
                        result.steps.add(executeDmlStep(statement, dmlMutation(target, relation, columns, scenario)));
                    }
                }
                result.beforeCommit = getResultMultiset("SELECT * FROM " + target + " ORDER BY `" + ROW_ID + "`");
                result.afterCommit = result.beforeCommit;
                statement.execute("COMMIT");
                transactionOpen = false;
                result.afterCommit = getResultMultiset("SELECT * FROM " + target + " ORDER BY `" + ROW_ID + "`");
            } finally {
                if (transactionOpen) {
                    try (Statement rollback = state.getConnection().createStatement()) {
                        rollback.execute("ROLLBACK");
                    }
                }
            }
            result.finalRows = getResultMultiset("SELECT * FROM " + target + " ORDER BY `" + ROW_ID + "`");
            return result;
        } finally {
            try {
                execute("DROP TEMPORARY TABLE IF EXISTS " + target);
            } catch (Throwable ignored) {
            }
        }
    }

    private String dmlMutation(String target, String relation, String columns, DmlScenario scenario) {
        switch (scenario) {
        case INSERT_IGNORE:
            return "INSERT IGNORE INTO " + target + " (" + columns + ") SELECT " + columns + " FROM " + relation;
        case ON_DUPLICATE_KEY_UPDATE:
            return "INSERT INTO " + target + " (" + columns + ") SELECT " + columns + " FROM " + relation
                    + " ON DUPLICATE KEY UPDATE `" + ROW_ID + "`=VALUES(`" + ROW_ID + "`)";
        case REPLACE:
            return "REPLACE INTO " + target + " (" + columns + ") SELECT " + columns + " FROM " + relation;
        default:
            throw new AssertionError("No mutation for " + scenario);
        }
    }

    private DmlStepResult executeDmlStep(Statement statement, String sql) {
        try {
            boolean hasResult = statement.execute(sql);
            QueryResultMultiset returned = null;
            if (hasResult) {
                returned = readCurrentResultSet(statement);
            }
            return new DmlStepResult(true, statement.getUpdateCount(), returned, null);
        } catch (Throwable e) {
            return new DmlStepResult(false, -1, null, e);
        }
    }

    private QueryResultMultiset readCurrentResultSet(Statement statement) throws SQLException {
        try (ResultSet rs = statement.getResultSet()) {
            if (rs == null)
                return new QueryResultMultiset(new LinkedHashMap<>(), new LinkedHashMap<>(), new ArrayList<>(),
                        new ArrayList<>());
            return readResultSet(rs);
        }
    }

    private QueryResultMultiset readResultSet(ResultSet rs) throws SQLException {
        Map<String, Integer> rows = new LinkedHashMap<>();
        Map<String, List<ApproximateKind>> approximateColumns = new LinkedHashMap<>();
        List<String> orderedRows = new ArrayList<>();
        List<List<ApproximateKind>> orderedApproximateColumns = new ArrayList<>();
        ResultSetMetaData meta = rs.getMetaData();
        int count = meta.getColumnCount();
        while (rs.next()) {
            String row = canonicalizeRow(rs, meta, count);
            List<ApproximateKind> approx = approximateColumns(meta, count);
            orderedRows.add(row);
            orderedApproximateColumns.add(approx);
            rows.put(row, rows.getOrDefault(row, 0) + 1);
            approximateColumns.putIfAbsent(row, approx);
        }
        return new QueryResultMultiset(rows, approximateColumns, orderedRows, orderedApproximateColumns);
    }

    private void compareDmlScenario(VpTables vp, DmlScenario scenario, DmlScenarioResult direct,
            DmlScenarioResult rewritten) {
        if (direct.steps.size() != rewritten.steps.size())
            throw new AssertionError("VP DML step count mismatch: " + scenario);
        for (int i = 0; i < direct.steps.size(); i++) {
            DmlStepResult left = direct.steps.get(i);
            DmlStepResult right = rewritten.steps.get(i);
            if (left.succeeded != right.succeeded) {
                throw new AssertionError("VP DML success mismatch: " + scenario + " step=" + i);
            }
            if (!left.succeeded) {
                if (!sameSqlException(left.error, right.error))
                    throw new AssertionError("VP DML error mismatch: " + scenario + " step=" + i
                            + " direct=" + errorText(left.error) + " rewritten=" + errorText(right.error));
                continue;
            }
            if (left.affectedRows != right.affectedRows)
                throw new AssertionError("VP DML affected-row mismatch: " + scenario + " step=" + i
                        + " direct=" + left.affectedRows + " rewritten=" + right.affectedRows);
            if (!sameOptionalResult(left.returnedRows, right.returnedRows))
                throw new AssertionError("VP DML RETURNING mismatch: " + scenario + " step=" + i);
        }
        if (!resultMultisetsMatch(direct.beforeCommit, rewritten.beforeCommit)
                || !resultMultisetsMatch(direct.afterCommit, rewritten.afterCommit)
                || !resultMultisetsMatch(direct.finalRows, rewritten.finalRows)) {
            throw new AssertionError("VP DML final-state mismatch: " + scenario);
        }
        if (scenario == DmlScenario.SAVEPOINT_ROLLBACK
                && (!resultMultisetsMatch(direct.duringSavepoint, rewritten.duringSavepoint)
                        || !resultMultisetsMatch(direct.afterRollbackToSavepoint, rewritten.afterRollbackToSavepoint))) {
            throw new AssertionError("VP DML savepoint-state mismatch: " + scenario);
        }
    }

    private boolean sameOptionalResult(QueryResultMultiset left, QueryResultMultiset right) {
        if (left == null || right == null)
            return left == right;
        return resultMultisetsMatch(left, right);
    }

    private String dmlColumnList(VpTables vp) {
        List<String> columns = new ArrayList<>();
        columns.add(quote(ROW_ID));
        columns.addAll(vp.plan.all.stream().map(column -> quote(column.name)).collect(Collectors.toList()));
        return String.join(", ", columns);
    }

    private String formatBugReport(QueryPair pair, QueryResultMultiset direct, QueryResultMultiset rewritten)
            throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("[VERTICAL PARTITIONING BUG CANDIDATE]").append(System.lineSeparator());
        sb.append("Mode: RANDOM_TABLE_REWRITE").append(System.lineSeparator());
        sb.append("MariaDB version: ").append(singleString("SELECT VERSION()")).append(System.lineSeparator());
        sb.append("Database: ").append(state.getDatabaseName()).append(System.lineSeparator());
        sb.append("Strategy summary: split=").append(pair.vp.splitStrategy).append(", reconstruction=")
                .append(pair.strategy).append(", shape=").append(pair.vp.shape).append(", prefix=")
                .append(pair.vp.prefix).append(System.lineSeparator());
        sb.append("Base table: ").append(pair.vp.baseTable).append(System.lineSeparator());
        sb.append("Base table DDL: ").append(createTable(pair.vp.baseTable)).append(System.lineSeparator());
        sb.append("Base engine: ").append(pair.vp.baseEngine).append(System.lineSeparator());
        sb.append("VP engines: source=").append(pair.vp.sourceEngine).append(", left=").append(pair.vp.leftEngine)
                .append(", right=").append(pair.vp.rightEngine).append(System.lineSeparator());
        sb.append("Base row count: ").append(pair.vp.baseRowCount).append(System.lineSeparator());
        sb.append("Left columns: ").append(columnNames(pair.vp.plan.left)).append(System.lineSeparator());
        sb.append("Right columns: ").append(columnNames(pair.vp.plan.right)).append(System.lineSeparator());
        sb.append("Overlap columns: ").append(columnNames(pair.vp.plan.overlap)).append(System.lineSeparator());
        sb.append("Description: ").append(pair.description).append(System.lineSeparator());
        sb.append("Setup statements:").append(System.lineSeparator());
        for (String setup : pair.vp.setupStatements)
            sb.append(setup).append(";").append(System.lineSeparator());
        sb.append("Direct query: ").append(pair.direct).append(System.lineSeparator());
        sb.append("Rewritten query: ").append(pair.rewritten).append(System.lineSeparator());
        sb.append("Direct plan:").append(System.lineSeparator()).append(explain(pair.direct));
        sb.append("Rewritten plan:").append(System.lineSeparator()).append(explain(pair.rewritten));
        sb.append("Direct sample: ").append(sample(direct.rows)).append(System.lineSeparator());
        sb.append("Rewritten sample: ").append(sample(rewritten.rows)).append(System.lineSeparator());
        sb.append("Only in direct: ").append(diff(direct, rewritten)).append(System.lineSeparator());
        sb.append("Only in rewritten: ").append(diff(rewritten, direct)).append(System.lineSeparator());
        return sb.toString();
    }

    private String sample(Map<String, Integer> rows) {
        return rows.entrySet().stream().limit(MAX_RESULT_SAMPLE).map(e -> e.getKey() + " x" + e.getValue())
                .collect(Collectors.joining(", "));
    }

    private String diff(QueryResultMultiset left, QueryResultMultiset right) {
        Map<String, Integer> remaining = new LinkedHashMap<>(left.rows);
        Map<String, Integer> other = new LinkedHashMap<>(right.rows);
        for (Map.Entry<String, Integer> entry : new ArrayList<>(remaining.entrySet())) {
            int exact = Math.min(entry.getValue(), other.getOrDefault(entry.getKey(), 0));
            if (exact > 0) {
                decrement(remaining, entry.getKey(), exact);
                decrement(other, entry.getKey(), exact);
            }
        }
        return remaining.entrySet().stream().filter(e -> e.getValue() > 0).limit(MAX_DIFF_ROWS)
                .map(e -> e.getKey() + " x" + e.getValue()).collect(Collectors.joining(", "));
    }

    private void decrement(Map<String, Integer> map, String key, int amount) {
        int count = map.getOrDefault(key, 0) - amount;
        if (count <= 0)
            map.remove(key);
        else
            map.put(key, count);
    }

    private String explain(String sql) {
        StringBuilder sb = new StringBuilder();
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("EXPLAIN " + sql)) {
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columns; i++) {
                    if (i != 1)
                        sb.append(" | ");
                    sb.append(rs.getString(i));
                }
                sb.append(System.lineSeparator());
            }
        } catch (Exception e) {
            sb.append("<explain failed: ").append(e.getMessage()).append(">").append(System.lineSeparator());
        }
        return sb.toString();
    }

    private String singleString(String sql) throws SQLException {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "<none>";
        }
    }

    private String createTable(String table) throws SQLException {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("SHOW CREATE TABLE " + quote(table))) {
            return rs.next() ? rs.getString(2) : "<missing>";
        }
    }

    private String columnNames(List<Column> columns) {
        return columns.stream().map(c -> c.name).collect(Collectors.joining(", "));
    }

    private void executeSetup(VpTables vp, String sql) throws Exception {
        vp.setupStatements.add(sql);
        if (execute(sql)) {
            return;
        }
        // SQLancer's expected-error adapter returns false without exposing the
        // SQL error. Re-run only this failed statement to record its cause;
        // successful setup still uses the usual logging and schema refresh.
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
            throw new AssertionError("VP setup returned false, but replay succeeded: " + sql);
        } catch (SQLException e) {
            String stage = setupStage(vp, sql);
            String category = stage + ":" + e.getErrorCode() + ":" + e.getSQLState();
            increment(SETUP_FAILURE_COUNTS, category);
            synchronized (LOGGED_SETUP_FAILURES) {
                if (LOGGED_SETUP_FAILURES.add(category)) {
                    logLine("vp-setup-errors.log", "stage=" + stage + " sql=" + sql + " error=" + e.getMessage());
                }
            }
            if (isSkippableThrowable(e)) {
                throw new IgnoreMeException();
            }
            throw e;
        }
    }

    private String setupStage(VpTables vp, String sql) {
        if (sql.startsWith("INSERT INTO " + vp.source) || sql.startsWith("CREATE TABLE " + vp.source)
                || sql.startsWith("ALTER TABLE " + vp.source)) {
            return "SOURCE";
        }
        if (sql.contains(vp.baseSnapshot)) {
            return "BASE_SNAPSHOT";
        }
        if (sql.contains(vp.left) || sql.contains(vp.right)) {
            return "HELPER_OR_RECONSTRUCTION";
        }
        return "RECONSTRUCTION";
    }

    private static String qualify(String alias, List<Column> columns) {
        List<String> result = new ArrayList<>();
        for (Column c : columns)
            result.add(alias + "." + quote(c.name));
        return String.join(", ", result);
    }

    private static String names(List<Column> columns) {
        List<String> result = new ArrayList<>();
        for (Column c : columns)
            result.add(quote(c.name));
        return String.join(", ", result);
    }

    private static String definitions(List<Column> columns) {
        List<String> result = new ArrayList<>();
        for (Column c : columns)
            result.add(quote(c.name) + " " + c.definition);
        return String.join(", ", result);
    }

    private static String type(MariaDBDataType type) {
        switch (type) {
        case INT:
            return "BIGINT";
        case REAL:
            return "DOUBLE";
        case BOOLEAN:
            return "BOOLEAN";
        default:
            return "VARCHAR(255)";
        }
    }

    private static String quote(String value) {
        return "`" + value.replace("`", "``") + "`";
    }

    private static String sanitizeIdentifier(String value) {
        return value.replaceAll("[^A-Za-z0-9_]", "_");
    }

    private static <K> void increment(Map<K, Integer> map, K key) {
        synchronized (STATS_LOCK) {
            map.put(key, map.getOrDefault(key, 0) + 1);
        }
    }

    private static <K> void increment(Map<K, Integer> map, K key, int amount) {
        if (amount == 0)
            return;
        synchronized (STATS_LOCK) {
            map.put(key, map.getOrDefault(key, 0) + amount);
        }
    }

    private static List<SplitStrategy> shuffledSplits(BaseShape shape) {
        List<SplitStrategy> result = new ArrayList<>();
        if (shape == BaseShape.EMPTY_SINGLE_COLUMN || shape == BaseShape.NON_EMPTY_SINGLE_COLUMN) {
            result.addAll(List.of(SplitStrategy.SINGLE_REPLICATED, SplitStrategy.SINGLE_RIGHT_ROW_ID_ONLY,
                    SplitStrategy.SINGLE_LEFT_ROW_ID_ONLY));
        } else {
            result.addAll(List.of(SplitStrategy.EMPTY_SPLIT, SplitStrategy.EVEN_SPLIT, SplitStrategy.OVERLAPPED_SPLIT,
                    SplitStrategy.SKEWED_SPLIT, SplitStrategy.TYPE_DIRECTED_SPLIT));
        }
        Collections.shuffle(result);
        return result;
    }

    private static List<ReconstructionStrategy> shuffledReconstructions() {
        // Keep the strategy pool limited to rewrites whose SQL actually
        // changes the reconstruction semantics.  The former JSON_PACK,
        // STRING_PACK, LATERAL_DERIVED, AGGREGATE_THEN_MERGE and
        // ROW_NUMBER_ALIGNMENT branches only added redundant expressions or
        // joins (and LATERAL_DERIVED was not correlated at all).  Selecting
        // them advertised coverage that the generated SQL did not provide.
        List<ReconstructionStrategy> result = new ArrayList<>(List.of(
                ReconstructionStrategy.JOIN, ReconstructionStrategy.LEFT_RIGHT_JOIN,
                ReconstructionStrategy.FULL_OUTER_SIMULATION, ReconstructionStrategy.SCALAR_SUBQUERY,
                ReconstructionStrategy.EXISTS_FILTER, ReconstructionStrategy.IN_FILTER,
                ReconstructionStrategy.NOT_EXISTS_ANTI, ReconstructionStrategy.DERIVED_TABLE,
                ReconstructionStrategy.CTE, ReconstructionStrategy.VIEW, ReconstructionStrategy.TEMP_TABLE,
                ReconstructionStrategy.NATURAL_OR_USING, ReconstructionStrategy.STRAIGHT_JOIN));
        Collections.shuffle(result);
        return result;
    }

    private boolean execute(String sql) throws Exception {
        return state.executeStatement(new SQLQueryAdapter(sql, errors, true));
    }

    private static void logLine(String name, String value) {
        try {
            File dir = new File(Main.LOG_DIRECTORY, "mariadb");
            dir.mkdirs();
            Files.writeString(new File(dir, name).toPath(), value + System.lineSeparator(), StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (Exception ignored) {
        }
    }

    private void logFallback(String logical, MariaDBEngine from, String reason) {
        recordEngineFallback();
        String normalized = reason == null ? "unknown" : reason.split("\\R", 2)[0];
        increment(ENGINE_FALLBACK_REASON_COUNTS, from + "->InnoDB: " + normalized);
        logLine("vp-engine-fallback.log", logical + " from=" + from + " to=InnoDB reason=" + normalized);
    }

    private void logSchemaCopyFailure(String table, String object, String reason) {
        recordSchemaCopyFailure();
        logLine("vp-schema-copy-failures.log", table + " object=" + object + " reason=" + reason);
    }

    private static String formatCounts(Map<?, Integer> counts) {
        return counts.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(", "));
    }

    private static String formatStats(String event) {
        StringBuilder sb = new StringBuilder();
        sb.append("-- [vp-stats] event=").append(event).append(" dbms=mariadb\n");
        sb.append("-- [vp-stats] oracle-runs completed=").append(completedOracleRuns).append(" checkedPairs=")
                .append(completedQueryPairs).append(" attempts=").append(completedAttempts).append('\n');
        sb.append("-- [vp-stats] schema-shape samples=")
                .append(SCHEMA_SHAPE_COUNTS.values().stream().mapToInt(Integer::intValue).sum());
        for (Map.Entry<String, Integer> entry : SCHEMA_SHAPE_COUNTS.entrySet()) {
            sb.append(' ').append(entry.getKey()).append('=').append(entry.getValue());
        }
        sb.append('\n');
        sb.append("-- [vp-stats] detected-column-types ").append(formatCounts(SCHEMA_TYPE_COUNTS)).append('\n');
        sb.append("-- [vp-stats] native-type-projections ").append(formatCounts(NATIVE_TYPE_PROJECTION_COUNTS))
                .append('\n');
        sb.append("-- [vp-stats] successful-requested-split-counts ").append(formatCounts(SPLIT_COUNTS)).append('\n');
        sb.append("-- [vp-stats] actual-split-shapes ").append(formatCounts(SPLIT_SHAPE_COUNTS)).append('\n');
        sb.append("-- [vp-stats] selected-shape-counts ").append(formatCounts(SHAPE_COUNTS)).append('\n');
        sb.append("-- [vp-stats] query-shape-counts ").append(formatCounts(QUERY_SHAPE_COUNTS)).append('\n');
        sb.append("-- [vp-stats] ast-policy-counts ").append(formatCounts(AST_POLICY_COUNTS)).append('\n');
        sb.append(MariaDBProvider.verticalPartitioningCreationStats()).append('\n');
        synchronized (STATS_LOCK) {
            long average = rowCountSamples == 0 ? 0 : rowCountSum / rowCountSamples;
            sb.append("-- [vp-stats] row-count samples=").append(rowCountSamples).append(" min=")
                    .append(rowCountSamples == 0 ? 0 : rowCountMin).append(" max=").append(rowCountMax)
                    .append(" average=").append(average).append('\n');
            sb.append("-- [vp-stats] row-count-buckets ").append(formatCounts(ROW_COUNT_BUCKET_COUNTS)).append('\n');
        }
        sb.append("-- [vp-stats] successful-requested-reconstruction-counts ")
                .append(formatCounts(RECONSTRUCTION_COUNTS)).append('\n');
        sb.append("-- [vp-stats] actual-reconstruction-plans ").append(formatCounts(RECONSTRUCTION_PLAN_COUNTS))
                .append('\n');
        sb.append("-- [vp-stats] known-suppressed-counts ").append(formatCounts(KNOWN_COUNTS)).append('\n');
        sb.append("-- [vp-stats] optimizer-profile-counts ").append(formatCounts(OPTIMIZER_PROFILE_COUNTS))
                .append('\n');
        sb.append("-- [vp-stats] optimizer-profile-mismatches ")
                .append(formatCounts(OPTIMIZER_PROFILE_MISMATCH_COUNTS)).append('\n');
        sb.append("-- [vp-stats] optimizer-profile-skipped ")
                .append(formatCounts(OPTIMIZER_PROFILE_SKIP_COUNTS)).append('\n');
        sb.append("-- [vp-stats] skipped-error-counts ").append(formatCounts(SKIP_COUNTS)).append('\n');
        sb.append("-- [vp-stats] setup-failure-counts ").append(formatCounts(SETUP_FAILURE_COUNTS)).append('\n');
        sb.append("-- [vp-stats] schema-copy-counts successes=").append(schemaCopySuccesses).append(" failures=")
                .append(schemaCopyFailures).append('\n');
        sb.append("-- [vp-stats] engine-fallback-counts ").append(engineFallbacks).append('\n');
        sb.append("-- [vp-stats] engine-combination-counts ").append(formatCounts(ENGINE_COMBINATION_COUNTS))
                .append('\n');
        sb.append("-- [vp-stats] engine-role-counts ").append(formatCounts(ENGINE_ROLE_COUNTS)).append('\n');
        sb.append("-- [vp-stats] rowid-key-counts ").append(formatCounts(ROW_ID_KEY_COUNTS)).append('\n');
        sb.append("-- [vp-stats] engine-fallback-reasons ").append(formatCounts(ENGINE_FALLBACK_REASON_COUNTS))
                .append('\n');
        return sb.toString();
    }

    private static void registerStatsHook() {
        if (STATS_HOOK_REGISTERED.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> writeStatsSnapshot("process-final")));
            writeStatsSnapshot("process-start");
        }
    }

    private static void writeStatsSnapshot(String event) {
        String text;
        synchronized (STATS_LOCK) {
            text = formatStats(event);
        }
        synchronized (STATS_WRITE_LOCK) {
            Path target = STATS_FILE.toPath();
            Path temporary = null;
            try {
                Files.createDirectories(target.getParent());
                temporary = Files.createTempFile(target.getParent(), "vp-stats", ".tmp");
                Files.writeString(temporary, text, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
                try {
                    Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                    Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException ignored) {
                // Statistics must not affect fuzzing when the log directory is unavailable.
            } finally {
                if (temporary != null) {
                    try {
                        Files.deleteIfExists(temporary);
                    } catch (IOException ignored) {
                    }
                }
            }
        }
    }

    private void logKnownBug(QueryPair pair, String known, QueryResultMultiset direct, QueryResultMultiset rewritten)
            throws SQLException {
        StringBuilder text = new StringBuilder();
        text.append("Known VP bug: ").append(known).append("\n\n");
        text.append(formatBugReport(pair, direct, rewritten));
        File dir = new File(Main.LOG_DIRECTORY, "mariadb");
        try {
            Files.createDirectories(dir.toPath());
            Path target;
            do {
                target = new File(dir, "vp-known" + KNOWN_BUG_LOG_ID.incrementAndGet() + ".log").toPath();
            } while (Files.exists(target));
            Files.writeString(target, sanitizeForUtf8(text.toString()), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE_NEW);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /** Random text can contain an unpaired UTF-16 surrogate, which UTF-8 cannot encode. */
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

    private void writeStats() {
        registerStatsHook();
        synchronized (STATS_LOCK) {
            int attemptBucket = completedAttempts / STATS_WRITE_ATTEMPT_INTERVAL;
            if (attemptBucket == lastStatsWriteAttemptBucket)
                return;
            lastStatsWriteAttemptBucket = attemptBucket;
        }
        writeStatsSnapshot("process-checkpoint");
    }
}
