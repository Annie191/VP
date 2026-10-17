package sqlancer.monetdb.oracle;

import java.math.BigDecimal;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.TestOracle;
import sqlancer.monetdb.MonetDBOptions;
import sqlancer.monetdb.MonetDBProvider;
import sqlancer.monetdb.MonetDBProvider.MonetDBGlobalState;
import sqlancer.monetdb.MonetDBSchema.MonetDBColumn;
import sqlancer.monetdb.MonetDBSchema.MonetDBDataType;
import sqlancer.monetdb.MonetDBSchema.MonetDBTable;

/**
 * Result-equivalence oracle for logical vertical partitioning. The small SelectAst below deliberately keeps the outer
 * query structured until the final visitor step; reconstruction relations are substituted only in the FROM clause.
 */
public final class MonetDBVerticalPartitioningOracle implements TestOracle<MonetDBGlobalState> {

    private static final String ROW_ID = "vp_rowid";
    private static final int MAX_DIFF_ROWS = 8;
    private static final AtomicLong RUNS = new AtomicLong();
    private static final AtomicLong PAIRS = new AtomicLong();
    private static final AtomicLong MISMATCHES = new AtomicLong();
    private static final AtomicBoolean SHUTDOWN_HOOK = new AtomicBoolean();
    private static final AtomicLong OBJECT_IDS = new AtomicLong();
    private static final String PROCESS_PREFIX = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    private static final File STATS_FILE = new File("logs/monetdb/vp-stats.log");
    private final MonetDBGlobalState state;
    private final MonetDBMutationFeedback mutations;

    private enum BaseShape {
        EMPTY, SINGLE_COLUMN, MULTI_COLUMN
    }

    private enum PartitionStrategy {
        NONE, RANGE, LIST
    }

    private enum SplitStrategy {
        EMPTY_SPLIT, SINGLE_REPLICATED, SINGLE_RIGHT_ROW_ID_ONLY, SINGLE_LEFT_ROW_ID_ONLY, EVEN_SPLIT, OVERLAPPED_SPLIT,
        SKEWED_SPLIT, TYPE_DIRECTED_SPLIT
    }

    private enum ReconstructionStrategy {
        JOIN, LEFT_RIGHT_JOIN, RIGHT_JOIN, FULL_OUTER_SIMULATION, SCALAR_SUBQUERY, EXISTS_FILTER, IN_FILTER,
        NOT_EXISTS_ANTI, DERIVED_TABLE, CTE, VIEW, TEMP_TABLE, AGGREGATE_THEN_MERGE, ROW_NUMBER_ALIGNMENT, JSON_PACK,
        STRING_PACK, NATURAL_OR_USING, USING_BUSINESS_COLUMNS, RECURSIVE_CTE, STRAIGHT_JOIN, LATERAL_DERIVED
    }

    public enum AdvancedFeature {
        ROLLUP, WINDOW_FUNCTION, CORRELATED_SUBQUERY, LISTAGG, ASYMMETRIC_INDEX, CHARSET_COLLATION_COLUMN,
        BOUNDARY_MUTATION, MERGE_UPSERT, NATURAL_JOIN, USING_JOIN, WINDOW_RANK, WINDOW_DENSE_RANK, WINDOW_NTILE,
        WINDOW_LAG, WINDOW_LEAD, WINDOW_FIRST_VALUE, WINDOW_LAST_VALUE, WINDOW_RANGE_FRAME, WINDOW_ROWS_FRAME,
        WINDOW_FILTER, CTE_NESTED, CTE_MULTI_REFERENCE, CTE_RECURSIVE
    }

    private enum SchemaCopyOutcome {
        SUCCESS, SKIPPED, FAILED
    }

    private static final Map<BaseShape, AtomicLong> BASE_SHAPE_COUNTS = counters(BaseShape.class);
    private static final Map<SplitStrategy, AtomicLong> SPLIT_COUNTS = counters(SplitStrategy.class);
    private static final Map<ReconstructionStrategy, AtomicLong> RECONSTRUCTION_COUNTS = counters(
            ReconstructionStrategy.class);
    private static final Map<AdvancedFeature, AtomicLong> ADVANCED_COUNTS = counters(AdvancedFeature.class);
    private static final Map<SchemaCopyOutcome, AtomicLong> SCHEMA_COPY_COUNTS = counters(SchemaCopyOutcome.class);
    private static final Map<PartitionStrategy, AtomicLong> PARTITIONED_BASE_COUNTS = counters(PartitionStrategy.class);
    private static final Map<String, AtomicLong> COMPARISON_COUNTS = new ConcurrentHashMap<>();
    private static final AtomicLong LARGE_QUERY_PAIRS = new AtomicLong();
    private static final AtomicLong LARGE_SOURCE_MAX = new AtomicLong();
    private static final AtomicLong SAMPLE_ROWS = new AtomicLong();
    private static final AtomicLong PARTITION_PAIRS = new AtomicLong();
    private static final AtomicLong PARTITION_VP_PAIRS = new AtomicLong();
    private static final AtomicLong PRUNING_CONFIRMED = new AtomicLong();
    private static final AtomicLong PRUNING_NOT_OBSERVED = new AtomicLong();
    private static final AtomicLong ROUTING_REJECTIONS = new AtomicLong();
    private static final AtomicLong STORAGE_PROBES = new AtomicLong();
    private static final AtomicLong STORAGE_UNAVAILABLE = new AtomicLong();
    private static final AtomicLong STORAGE_LAYOUT_DIFFERENCES = new AtomicLong();
    private static final Object STORAGE_LOCK = new Object();
    private static final AtomicLong SAMPLE_CASES = new AtomicLong();
    private static final AtomicLong SAMPLE_UNSUPPORTED = new AtomicLong();
    private static final AtomicLong MERGE_UNSUPPORTED = new AtomicLong();
    private static final AtomicLong WINDOW_FILTER_UNSUPPORTED = new AtomicLong();
    private static final Object WINDOW_CAPABILITY_LOCK = new Object();
    private static volatile Boolean WINDOW_FILTER_SUPPORTED;

    private static final class StorageObservation {
        final long columns;
        final String modes;

        StorageObservation(long columns, String modes) {
            this.columns = columns;
            this.modes = modes;
        }
    }

    private static final class Base {
        final String name;
        final List<MonetDBColumn> columns;
        final BaseShape shape;
        final String order;
        final PartitionStrategy partitionStrategy;
        final long rowCount;

        Base(String name, List<MonetDBColumn> columns, BaseShape shape, String order,
                PartitionStrategy partitionStrategy, long rowCount) {
            this.name = name;
            this.columns = columns;
            this.shape = shape;
            this.order = order;
            this.partitionStrategy = partitionStrategy;
            this.rowCount = rowCount;
        }
    }

    private static final class Vp {
        final String prefix;
        final String base;
        final String source;
        final String left;
        final String right;
        final String view;
        final String temp;
        final List<String> transientObjects = new ArrayList<>();
        final List<MonetDBColumn> columns;
        final List<MonetDBColumn> leftColumns;
        final List<MonetDBColumn> rightColumns;
        final Base baseMetadata;
        final SplitStrategy splitStrategy;
        final List<String> setupStatements = new ArrayList<>();
        long baseRowCountAtMaterialization;
        long sourceRowCount;

        Vp(String prefix, Base base, List<MonetDBColumn> left, List<MonetDBColumn> right, SplitStrategy splitStrategy) {
            this.prefix = prefix;
            this.base = base.name;
            this.source = prefix + "source";
            this.left = prefix + "left";
            this.right = prefix + "right";
            this.view = prefix + "view";
            this.temp = prefix + "temp";
            this.columns = base.columns;
            this.leftColumns = left;
            this.rightColumns = right;
            this.baseMetadata = base;
            this.splitStrategy = splitStrategy;
        }
    }

    private static final class SelectAst {
        final List<String> projection = new ArrayList<>();
        String from;
        String where;
        final List<String> groupBy = new ArrayList<>();
        String having;
        final List<String> orderBy = new ArrayList<>();
        Integer limit;
        Integer offset;

        String render() {
            StringBuilder sql = new StringBuilder("SELECT ");
            sql.append(projection.isEmpty() ? "*" : String.join(", ", projection));
            sql.append(" FROM ").append(from);
            if (where != null) {
                sql.append(" WHERE ").append(where);
            }
            if (!groupBy.isEmpty()) {
                sql.append(" GROUP BY ").append(String.join(", ", groupBy));
            }
            if (having != null) {
                sql.append(" HAVING ").append(having);
            }
            if (!orderBy.isEmpty()) {
                sql.append(" ORDER BY ").append(String.join(", ", orderBy));
            }
            if (limit != null) {
                sql.append(" LIMIT ").append(limit);
            }
            if (offset != null) {
                sql.append(" OFFSET ").append(offset);
            }
            return sql.toString();
        }
    }

    private static final class Pair {
        final String source;
        final String reconstructed;
        final ReconstructionStrategy strategy;
        final boolean ordered;
        final Vp vp;
        final List<String> setupStatements;

        Pair(String source, String reconstructed, ReconstructionStrategy strategy, boolean ordered, Vp vp,
                List<String> setupStatements) {
            this.source = source;
            this.reconstructed = reconstructed;
            this.strategy = strategy;
            this.ordered = ordered;
            this.vp = vp;
            this.setupStatements = setupStatements;
        }
    }

    private static final class RowSet {
        final MonetDBResultComparison.Snapshot layers = new MonetDBResultComparison.Snapshot();
        final Map<String, Integer> rows = new LinkedHashMap<>();
        final List<String> ordered = new ArrayList<>();
    }

    public MonetDBVerticalPartitioningOracle(MonetDBGlobalState state) {
        this.state = state;
        this.mutations = new MonetDBMutationFeedback(state);
        if (SHUTDOWN_HOOK.compareAndSet(false, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(MonetDBVerticalPartitioningOracle::writeStats));
        }
    }

    @Override
    public void check() throws Exception {
        RUNS.incrementAndGet();
        Base base = chooseBase();
        BASE_SHAPE_COUNTS.get(base.shape).incrementAndGet();
        PARTITIONED_BASE_COUNTS.get(base.partitionStrategy).incrementAndGet();
        if (base.partitionStrategy != PartitionStrategy.NONE) {
            verifyPartitionedBase(base);
            comparePartitionPaths(base, null);
        }
        if (state.getDbmsSpecificOptions().verticalPartitioningDeepExploration) {
            applyBoundaryMutation(base);
        } else {
            mutations.skipped("UPDATE", base.name, null, "none", base.partitionStrategy.name(), "boundary",
                    "deep_exploration_disabled");
            mutations.skipped("MERGE", base.name, null, "none", base.partitionStrategy.name(), "matched_update_insert",
                    "deep_exploration_disabled");
        }
        int variants = Math.max(1, state.getDbmsSpecificOptions().verticalPartitioningQueryVariantsPerCombination);
        List<SplitStrategy> splitSchedule = new ArrayList<>(splitStrategies(base.shape));
        shuffleWithSeed(splitSchedule);
        boolean firstSplit = true;
        for (SplitStrategy split : splitSchedule) {
            SPLIT_COUNTS.get(split).incrementAndGet();
            Vp vp = materialize(base, split, firstSplit);
            boolean largeProbe = firstSplit;
            firstSplit = false;
            try {
                if (largeProbe) {
                    compareNullOrdering(vp);
                }
                if (largeProbe
                        && vp.sourceRowCount >= state.getDbmsSpecificOptions().verticalPartitioningLargeQueryMinRows) {
                    compareLargeQueries(vp);
                }
                if (base.partitionStrategy != PartitionStrategy.NONE) {
                    comparePartitionPaths(base, vp);
                }
                List<ReconstructionStrategy> reconstructionSchedule = new ArrayList<>(
                        List.of(ReconstructionStrategy.values()));
                shuffleWithSeed(reconstructionSchedule);
                for (ReconstructionStrategy strategy : reconstructionSchedule) {
                    for (int i = 0; i < variants; i++) {
                        try {
                            Pair pair = createPair(vp, strategy);
                            compare(pair);
                            PAIRS.incrementAndGet();
                            RECONSTRUCTION_COUNTS.get(strategy).incrementAndGet();
                        } finally {
                            try {
                                cleanupPairObjects(vp);
                            } catch (SQLException ignored) {
                                // Preserve the query/setup failure; the outer VP cleanup retries all objects.
                            }
                        }
                    }
                }
            } finally {
                cleanup(vp);
            }
        }
    }

    private void applyBoundaryMutation(Base base) throws SQLException {
        if (base.shape == BaseShape.EMPTY || base.columns.isEmpty()) {
            mutations.skipped("UPDATE", base.name, null, "none", base.partitionStrategy.name(), "boundary",
                    "empty_base");
            return;
        }
        List<MonetDBColumn> mutable = base.columns.stream()
                .filter(c -> base.partitionStrategy == PartitionStrategy.NONE || !c.getName().equals("id"))
                .collect(Collectors.toList());
        if (mutable.isEmpty()) {
            mutations.skipped("UPDATE", base.name, null, "none", base.partitionStrategy.name(), "boundary",
                    "no_mutable_column");
            return;
        }
        MonetDBColumn column = Randomly.fromList(mutable);
        String literal;
        switch (column.getType()) {
        case INT:
            literal = Randomly.fromOptions("0", "-1", "1", "2147483647", "-2147483647");
            break;
        case BIGINT:
            literal = Randomly.fromOptions("0", "-1", "1", "2147483647", "-9223372036854775807");
            break;
        case HUGEINT:
            literal = Randomly.fromOptions("CAST('0' AS HUGEINT)", "CAST('-1' AS HUGEINT)",
                    "CAST('999999999999999999' AS HUGEINT)");
            break;
        case DECIMAL:
            literal = Randomly.fromOptions("0.000000", "-1.000000", "999999999.999999");
            break;
        case DOUBLE:
            literal = Randomly.fromOptions("0.0", "-0.0", "1e100", "-1e100");
            break;
        case BOOLEAN:
            literal = Randomly.fromOptions("TRUE", "FALSE");
            break;
        case DATE:
            literal = "DATE " + quoteLiteral(Randomly.fromOptions("1970-01-01", "2000-02-29", "2038-01-19"));
            break;
        case TIME:
            literal = "TIME " + quoteLiteral(Randomly.fromOptions("00:00:00", "12:34:56", "23:59:59"));
            break;
        case TIMETZ:
            literal = "TIME WITH TIME ZONE "
                    + quoteLiteral(Randomly.fromOptions("00:00:00+00:00", "12:34:56+05:00", "23:59:59-08:00"));
            break;
        case TIMESTAMP:
            literal = "TIMESTAMP " + quoteLiteral(
                    Randomly.fromOptions("1970-01-01 00:00:00", "2000-02-29 12:34:56", "2038-01-19 03:14:07"));
            break;
        case TIMESTAMPTZ:
            literal = "TIMESTAMP WITH TIME ZONE " + quoteLiteral(Randomly.fromOptions("1970-01-01 00:00:00+00:00",
                    "2000-02-29 12:34:56+05:00", "2038-01-19 03:14:07-08:00"));
            break;
        case INTERVAL:
            literal = Randomly.fromOptions("INTERVAL '0' SECOND", "INTERVAL '1' DAY", "INTERVAL '-2' DAY");
            break;
        case UUID:
            literal = "UUID " + quoteLiteral(Randomly.fromOptions("00000000-0000-0000-0000-000000000000",
                    "123e4567-e89b-12d3-a456-426614174000"));
            break;
        case INET:
            literal = "INET " + quoteLiteral(Randomly.fromOptions("0.0.0.0", "192.168.1.1", "127.0.0.1/32"));
            break;
        case URL:
            literal = "URL " + quoteLiteral(Randomly.fromOptions("http://localhost/", "https://example.com/a"));
            break;
        case BLOB:
        case VARCHAR:
            literal = quoteLiteral(Randomly.fromOptions("", " ", "0", "AaBb", "abc ", "ß"));
            break;
        default:
            throw new AssertionError(column.getType());
        }
        if (Randomly.getBoolean()) {
            if (applyMergeMutation(base, column, literal)) {
                mutations.skipped("UPDATE", base.name, column.getName(), column.getType().name(),
                        base.partitionStrategy.name(), "boundary", "merge_succeeded");
                return;
            }
        } else {
            mutations.skipped("MERGE", base.name, column.getName(), column.getType().name(),
                    base.partitionStrategy.name(), "matched_update_insert", "not_selected");
        }
        String predicate = "mod(abs(CAST(sys.rand() AS BIGINT)), 100) < " + Randomly.fromOptions("5", "10", "20")
                + " AND " + q(column.getName()) + " IS NOT NULL";
        String assignment = Randomly.getBooleanWithRatherLowProbability() ? "NULL" : literal;
        MonetDBMutationFeedback.Outcome result = mutations.run("UPDATE", base.name, column.getName(),
                column.getType().name(), base.partitionStrategy.name(), "boundary",
                "UPDATE " + q(base.name) + " SET " + q(column.getName()) + " = " + assignment + " WHERE " + predicate);
        if (result.success) {
            increment(AdvancedFeature.BOUNDARY_MUTATION);
        }
    }

    private boolean applyMergeMutation(Base base, MonetDBColumn matchColumn, String matchLiteral) throws SQLException {
        String source = "vp_" + PROCESS_PREFIX + "_merge_" + OBJECT_IDS.incrementAndGet();
        String setupSql = "CREATE TABLE " + q(source) + " (" + definitions(base.columns) + ")";
        boolean mergeAttempted = false;
        try {
            List<String> names = base.columns.stream().map(c -> q(c.getName())).collect(Collectors.toList());
            List<String> values = base.columns.stream()
                    .map(c -> c == matchColumn ? matchLiteral : randomLiteralForMerge(c.getType()))
                    .collect(Collectors.toList());
            execute(setupSql);
            setupSql = "INSERT INTO " + q(source) + " (" + String.join(", ", names) + ") VALUES ("
                    + String.join(", ", values) + ")";
            execute(setupSql);
            String sourceValues = names.stream().map(n -> "src." + n).collect(Collectors.joining(", "));
            String mergeSql = "MERGE INTO " + q(base.name) + " tgt USING " + q(source) + " src ON tgt."
                    + q(matchColumn.getName()) + " = src." + q(matchColumn.getName()) + " WHEN MATCHED THEN UPDATE SET "
                    + q(matchColumn.getName()) + " = src." + q(matchColumn.getName())
                    + " WHEN NOT MATCHED THEN INSERT (" + String.join(", ", names) + ") VALUES (" + sourceValues + ")";
            mergeAttempted = true;
            MonetDBMutationFeedback.Outcome result = mutations.run("MERGE", base.name, matchColumn.getName(),
                    matchColumn.getType().name(), base.partitionStrategy.name(), "matched_update_insert", mergeSql);
            if (result.success) {
                increment(AdvancedFeature.MERGE_UPSERT);
            } else if ("capability".equals(result.failureClass)) {
                MERGE_UNSUPPORTED.incrementAndGet();
            }
            return result.success;
        } catch (SQLException failure) {
            if (mergeAttempted) {
                throw failure; // Already recorded; connection/resource errors must not be hidden.
            }
            SQLException actual = failure.getCause() instanceof SQLException ? (SQLException) failure.getCause()
                    : failure;
            mutations.externalFailure("MERGE_SETUP", base.name, matchColumn.getName(), matchColumn.getType().name(),
                    base.partitionStrategy.name(), "source_create_insert", setupSql, actual);
            mutations.skipped("MERGE", base.name, matchColumn.getName(), matchColumn.getType().name(),
                    base.partitionStrategy.name(), "matched_update_insert", "setup_failure");
            if (MonetDBMutationFeedback.classify(actual).equals("connection")
                    || MonetDBMutationFeedback.classify(actual).equals("resource")) {
                throw failure;
            }
            return false;
        } finally {
            String cleanupSql = "DROP TABLE IF EXISTS " + q(source) + " CASCADE";
            try {
                execute(cleanupSql);
            } catch (SQLException failure) {
                SQLException actual = failure.getCause() instanceof SQLException ? (SQLException) failure.getCause()
                        : failure;
                mutations.externalFailure("MERGE_CLEANUP", base.name, matchColumn.getName(),
                        matchColumn.getType().name(), base.partitionStrategy.name(), "source_drop", cleanupSql, actual);
            }
        }
    }

    private String randomLiteralForMerge(MonetDBDataType type) {
        switch (type) {
        case INT:
            return Integer.toString(state.getRandomly().getInteger(-1000, 1001));
        case BIGINT:
            return "CAST('" + state.getRandomly().getLong(-100000, 100001) + "' AS BIGINT)";
        case HUGEINT:
            return "CAST('" + state.getRandomly().getLong(-100000, 100001) + "' AS HUGEINT)";
        case DECIMAL:
            return "0.000000";
        case DOUBLE:
            return "0.0";
        case BOOLEAN:
            return Randomly.fromOptions("TRUE", "FALSE");
        case DATE:
            return "DATE '2000-01-01'";
        case TIME:
            return "TIME '12:00:00'";
        case TIMETZ:
            return "TIME WITH TIME ZONE '12:00:00+00:00'";
        case TIMESTAMP:
            return "TIMESTAMP '2000-01-01 00:00:00'";
        case TIMESTAMPTZ:
            return "TIMESTAMP WITH TIME ZONE '2000-01-01 00:00:00+00:00'";
        case INTERVAL:
            return "INTERVAL '1' DAY";
        case UUID:
            return "UUID '00000000-0000-0000-0000-000000000001'";
        case INET:
            return "INET '127.0.0.1/32'";
        case URL:
            return "URL 'https://example.com/a'";
        case BLOB:
            return "BLOB ''";
        case VARCHAR:
            return "'merge'";
        default:
            throw new AssertionError(type);
        }
    }

    private static String quoteLiteral(String value) {
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'";
    }

    private Base chooseBase() throws Exception {
        state.updateSchema();
        List<MonetDBTable> tables = state.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> !t.getName().startsWith("vp_")).filter(t -> !t.getColumns().isEmpty())
                .filter(t -> !t.getName().matches("pt_(range|list)_p[0-9]+")).collect(Collectors.toList());
        if (tables.isEmpty()) {
            throw new IgnoreMeException();
        }
        MonetDBTable table = Randomly.fromList(tables);
        long count = scalarLong("SELECT COUNT(*) FROM " + q(table.getName()));
        BaseShape shape = count == 0 ? BaseShape.EMPTY
                : table.getColumns().size() == 1 ? BaseShape.SINGLE_COLUMN : BaseShape.MULTI_COLUMN;
        List<String> comparable = table.getColumns().stream().filter(c -> c.getType() != MonetDBDataType.DOUBLE)
                .map(c -> q(c.getName())).collect(Collectors.toList());
        String order = comparable.isEmpty() ? null : String.join(", ", comparable);
        PartitionStrategy partitionStrategy = table.getName().startsWith("pt_range") ? PartitionStrategy.RANGE
                : table.getName().startsWith("pt_list") ? PartitionStrategy.LIST : PartitionStrategy.NONE;
        if (partitionStrategy != PartitionStrategy.NONE && !verifyPartitionRouting(table.getName())) {
            throw new AssertionError("Invalid partition routing: " + table.getName());
        }
        return new Base(table.getName(), new ArrayList<>(table.getColumns()), shape, order, partitionStrategy, count);
    }

    private List<String> partitionChildren(String mergeTable) {
        return state.getSchema().getDatabaseTables().stream().map(MonetDBTable::getName)
                .filter(n -> n.matches(java.util.regex.Pattern.quote(mergeTable) + "_p[0-9]+")).sorted()
                .collect(Collectors.toList());
    }

    private boolean verifyPartitionRouting(String mergeTable) throws SQLException {
        List<String> children = partitionChildren(mergeTable);
        long total = 0;
        boolean list = mergeTable.startsWith("pt_list");
        int[] bounds = children.size() == 4 ? new int[] { -1000, -1, 0, 1, 1000 } : new int[] { -1000, 0, 1000 };
        for (int i = 0; i < children.size(); i++) {
            String child = q(children.get(i));
            total += scalarLong("SELECT COUNT(*) FROM " + child);
            String violation;
            if (list && children.size() == 4) {
                String[] values = { "-1000,-100,-99,-98,-90,-80,-1", "0", "1,2,9,10,11,99", "101,199,999,1000" };
                violation = q("id") + " NOT IN (" + values[i] + ")";
            } else if (list) {
                violation = q("id") + (i == 0 ? " > 0" : " <= 0");
            } else {
                violation = q("id") + " < " + bounds[i] + " OR " + q("id") + " >= " + bounds[i + 1];
            }
            violation = "(" + violation + ")" + (i == 0 ? "" : " OR " + q("id") + " IS NULL");
            if (scalarLong("SELECT COUNT(*) FROM " + child + " WHERE " + violation) != 0) {
                return false;
            }
        }
        return !children.isEmpty() && scalarLong("SELECT COUNT(*) FROM " + q(mergeTable)) == total;
    }

    /** Exercise routing, pruning-visible predicates, duplicate keys and an in-partition update. */
    private void verifyPartitionedBase(Base base) throws SQLException {
        String key = q("id");
        int first = base.partitionStrategy == PartitionStrategy.LIST ? 0 : -1;
        int second = 1;
        for (int value : new int[] { first, first, second }) {
            partitionMutation(base, "PARTITION_INSERT", "id", "INT", "duplicate_key_insert",
                    "INSERT INTO " + q(base.name) + " (" + key + ") VALUES (" + value + ")");
        }
        MonetDBColumn payload = base.columns.stream().filter(c -> !c.getName().equalsIgnoreCase("id")).findFirst()
                .orElse(null);
        if (payload != null) {
            String name = q(payload.getName());
            partitionMutation(base, "PARTITION_UPDATE", payload.getName(), payload.getType().name(),
                    "same_value_update",
                    "UPDATE " + q(base.name) + " SET " + name + " = " + name + " WHERE " + key + " = " + first);
        }
        partitionMutation(base, "PARTITION_INSERT", "id", "INT", "null_key_insert",
                "INSERT INTO " + q(base.name) + " (" + key + ") VALUES (NULL)");
        expectRoutingRejection(base, "INSERT INTO " + q(base.name + "_p0") + " (" + key + ") VALUES (1)");
        expectRoutingRejection(base,
                "INSERT INTO " + q(partitionChildren(base.name).get(partitionChildren(base.name).size() - 1)) + " ("
                        + key + ") VALUES (-1)");
        expectRoutingRejection(base, "INSERT INTO " + q(base.name) + " (" + key + ") VALUES ("
                + (base.partitionStrategy == PartitionStrategy.RANGE ? "1000" : "1001") + ")");
        if (!verifyPartitionRouting(base.name)) {
            throw new AssertionError("Invalid routing after partition DML: " + base.name);
        }
    }

    private void partitionMutation(Base base, String operation, String column, String dataType, String shape,
            String sql) throws SQLException {
        MonetDBMutationFeedback.Outcome result = mutations.run(operation, base.name, column, dataType,
                base.partitionStrategy.name(), shape, sql);
        if (!result.success) {
            throw new AssertionError("Required partition mutation failed (" + result.failureClass + "): " + sql);
        }
    }

    private void expectRoutingRejection(Base base, String sql) throws SQLException {
        MonetDBMutationFeedback.Outcome result = mutations.run("PARTITION_ROUTING", base.name, "id", "INT",
                base.partitionStrategy.name(), "expected_rejection", sql);
        if (!result.success && "partition_routing".equals(result.failureClass)) {
            ROUTING_REJECTIONS.incrementAndGet();
            return;
        }
        throw new AssertionError("Expected partition routing rejection, got "
                + (result.success ? "accepted" : result.failureClass) + ": " + sql);
    }

    /** Keep the merge relation in the actual result oracle; compare bags, including duplicate and NULL keys. */
    private void comparePartitionPaths(Base base, Vp vp) throws Exception {
        String columns = names(base.columns);
        List<String> children = partitionChildren(base.name);
        String union = partitionUnion(children, columns);
        String reconstructed = vp == null ? null : "(SELECT " + projection(vp, "l", "r") + " FROM " + q(vp.left)
                + " l JOIN " + q(vp.right) + " r ON l." + q(ROW_ID) + " = r." + q(ROW_ID) + ") v";
        List<String> predicates = List.of("TRUE", "id = -1000", "id = -1", "id = 0", "id = 1", "id = 999", "id = 1000",
                "id IS NULL", "id < 0", "id >= 0", "id BETWEEN -1 AND 1",
                "id IN (-1000, -1, 0, 1, 999, 1000) OR id IS NULL");
        for (String predicate : predicates) {
            predicate = predicate.replace("id", q("id"));
            String direct = "SELECT " + columns + " FROM " + q(base.name) + " WHERE " + predicate;
            if (vp != null) {
                comparePartitionQuery(direct, "SELECT " + columns + " FROM " + reconstructed + " WHERE " + predicate,
                        true);
                continue;
            }
            comparePartitionQuery(direct, "SELECT " + columns + " FROM " + union + " WHERE " + predicate, false);
            List<String> related = relatedPartitions(base, children, predicate);
            if (related.size() < children.size()) {
                comparePartitionQuery(direct,
                        "SELECT " + columns + " FROM " + partitionUnion(related, columns) + " WHERE " + predicate,
                        false);
                String plan = explain(direct);
                boolean relevant = plan.contains("table(") && related.stream().anyMatch(c -> plan.contains(q(c)));
                boolean unrelated = children.stream().filter(c -> !related.contains(c))
                        .anyMatch(c -> plan.contains(q(c)));
                (relevant && !unrelated ? PRUNING_CONFIRMED : PRUNING_NOT_OBSERVED).incrementAndGet();
            }
        }
    }

    private String partitionUnion(List<String> children, String columns) {
        return "(" + children.stream().map(c -> "SELECT " + columns + " FROM " + q(c))
                .collect(Collectors.joining(" UNION ALL ")) + ") u";
    }

    private List<String> relatedPartitions(Base base, List<String> children, String predicate) {
        String plain = predicate.replace(q("id"), "id");
        boolean list = base.partitionStrategy == PartitionStrategy.LIST;
        Set<Integer> selected = new HashSet<>();
        if (plain.equals("id IS NULL")) {
            selected.add(0);
        } else if (plain.startsWith("id = ")) {
            int key = Integer.parseInt(plain.substring(5));
            selected.add(children.size() == 4 ? MonetDBProvider.partitionForKey(list, key)
                    : list ? key <= 0 ? 0 : 1 : key < 0 ? 0 : 1);
        } else if (plain.equals("id < 0")) {
            selected.add(0);
            if (!list && children.size() == 4) {
                selected.add(1);
            }
        } else if (plain.equals("id >= 0")) {
            if (children.size() == 4) {
                for (int i = list ? 1 : 2; i < 4; i++) {
                    selected.add(i);
                }
            } else if (!list) {
                selected.add(1);
            } else {
                return children;
            }
        } else {
            return children;
        }
        return selected.stream().sorted().map(children::get).collect(Collectors.toList());
    }

    private void compareNullOrdering(Vp vp) throws Exception {
        MonetDBColumn sortColumn = vp.columns.stream()
                .filter(c -> c.getType().isNumeric() || c.getType() == MonetDBDataType.VARCHAR).findFirst().orElse(null);
        if (sortColumn == null) {
            return;
        }
        String rebuilt = "(SELECT " + projection(vp, "l", "r") + " FROM " + q(vp.left) + " l JOIN "
                + q(vp.right) + " r ON l." + q(ROW_ID) + " = r." + q(ROW_ID) + ") s";
        for (String direction : List.of("ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST")) {
            String query = "SELECT " + semanticProjection(vp, "s") + " FROM %s ORDER BY s."
                    + q(sortColumn.getName()) + " " + direction + ", s." + q(ROW_ID);
            compare(new Pair(String.format(Locale.ROOT, query, q(vp.source) + " s"),
                    String.format(Locale.ROOT, query, rebuilt), ReconstructionStrategy.JOIN, true, vp, Collections.emptyList()));
            COMPARISON_COUNTS.computeIfAbsent("null_order_probes", k -> new AtomicLong()).incrementAndGet();
        }
    }

    /** Full-result workloads: no small LIMIT and deterministic tie-breaking for sorting/window comparisons. */
    private void compareLargeQueries(Vp vp) throws Exception {
        LARGE_SOURCE_MAX.accumulateAndGet(vp.sourceRowCount, Math::max);
        String direct = q(vp.source) + " s";
        String rebuilt = "(SELECT " + projection(vp, "l", "r") + " FROM " + q(vp.left) + " l JOIN " + q(vp.right)
                + " r ON l." + q(ROW_ID) + " = r." + q(ROW_ID) + ") s";
        String key = "s." + q(ROW_ID);
        String columns = semanticProjection(vp, "s");
        List<String> payload = vp.columns.stream().filter(c -> c.getType() != MonetDBDataType.BLOB)
                .map(c -> "s." + q(c.getName())).collect(Collectors.toList());
        MonetDBColumn text = vp.columns.stream().filter(c -> c.getType() == MonetDBDataType.VARCHAR).findFirst()
                .orElse(null);
        String sort = (text == null ? "mod(" + key + ", 17)" : "s." + q(text.getName())) + " DESC, " + key;
        String window = "PARTITION BY mod(" + key + ", 17) ORDER BY " + key;
        List<String> queries = List.of("SELECT " + columns + " FROM %s ORDER BY " + sort,
                "SELECT " + columns + ", ROW_NUMBER() OVER (" + window + ") AS vp_rank, LAG(" + key + ") OVER ("
                        + window + ") AS vp_prev, SUM(" + key + ") OVER (" + window
                        + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS vp_total FROM %s",
                "SELECT mod(" + key + ", 17), COUNT(*), SUM(" + key + "), MIN(" + key + "), MAX(" + key
                        + ") FROM %s GROUP BY mod(" + key + ", 17)",
                "SELECT DISTINCT " + (payload.isEmpty() ? "mod(" + key + ", 17)" : String.join(", ", payload))
                        + " FROM %s");
        for (int i = 0; i < queries.size(); i++) {
            String query = queries.get(i);
            compare(new Pair(String.format(Locale.ROOT, query, direct), String.format(Locale.ROOT, query, rebuilt),
                    ReconstructionStrategy.JOIN, i == 0, vp, Collections.emptyList()));
            LARGE_QUERY_PAIRS.incrementAndGet();
        }
    }

    private void comparePartitionQuery(String direct, String reference, boolean vertical) throws Exception {
        state.getState().logStatement(direct);
        state.getState().logStatement(reference);
        String plans = formatExplain(direct) + formatExplain(reference);
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent("-- PARTITION SOURCE: " + direct);
            state.getLogger().writeCurrent("-- PARTITION RECONSTRUCTED: " + reference);
            state.getLogger().writeCurrent("-- PARTITION PLANS:\n" + plans);
        }
        RowSet actual = read(direct);
        RowSet expected = read(reference);
        MonetDBResultComparison.Report report = compareLayers(actual, expected, direct, reference, false);
        if (report.fails(false, state.getDbmsSpecificOptions().verticalPartitioningStrictRepresentation)) {
            MISMATCHES.incrementAndGet();
            throw new AssertionError("[PARTITION EQUIVALENCE MISMATCH]\n" + direct + "\n" + reference + "\nmerge only: "
                    + diff(actual, expected) + "\nreference only: " + diff(expected, actual) + "\n" + plans);
        }
        (vertical ? PARTITION_VP_PAIRS : PARTITION_PAIRS).incrementAndGet();
    }

    private List<SplitStrategy> splitStrategies(BaseShape shape) {
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

    private Vp materialize(Base base, SplitStrategy strategy, boolean fullCopy) throws Exception {
        List<MonetDBColumn> left = new ArrayList<>();
        List<MonetDBColumn> right = new ArrayList<>();
        List<MonetDBColumn> shuffled = new ArrayList<>(base.columns);
        shuffleWithSeed(shuffled);
        if (base.shape == BaseShape.SINGLE_COLUMN) {
            if (strategy == SplitStrategy.SINGLE_REPLICATED) {
                left.addAll(base.columns);
                right.addAll(base.columns);
            } else if (strategy == SplitStrategy.SINGLE_RIGHT_ROW_ID_ONLY) {
                left.addAll(base.columns);
            } else {
                right.addAll(base.columns);
            }
        } else if (strategy == SplitStrategy.TYPE_DIRECTED_SPLIT) {
            for (MonetDBColumn column : base.columns) {
                (column.getType().isNumeric() ? left : right).add(column);
            }
            if (left.isEmpty() || right.isEmpty()) {
                left.clear();
                right.clear();
                evenSplit(shuffled, left, right);
            }
        } else if (strategy == SplitStrategy.SKEWED_SPLIT) {
            left.add(shuffled.get(0));
            right.addAll(shuffled.subList(1, shuffled.size()));
            if (right.isEmpty()) {
                right.add(shuffled.get(0));
            }
        } else {
            evenSplit(shuffled, left, right);
            if (strategy == SplitStrategy.OVERLAPPED_SPLIT) {
                MonetDBColumn overlap = Randomly.fromList(base.columns);
                if (!left.contains(overlap)) {
                    left.add(overlap);
                }
                if (!right.contains(overlap)) {
                    right.add(overlap);
                }
            }
        }
        // Database schemas can outlive a worker process after an interrupted run. Include a
        // process-local time component so stale VP objects cannot collide with a later run.
        String prefix = "vp_" + PROCESS_PREFIX + "_" + OBJECT_IDS.incrementAndGet() + "_";
        int sampleChance = state.getDbmsSpecificOptions().verticalPartitioningSamplePercent;
        if (sampleChance < 0 || sampleChance > 100) {
            throw new IllegalArgumentException("--vp-sample-percent must be between 0 and 100");
        }
        int samplePercent = !fullCopy && base.partitionStrategy == PartitionStrategy.NONE && base.rowCount > 0
                && state.getRandomly().getInteger(0, 100) < sampleChance ? Randomly.fromOptions(10, 25, 50) : 0;
        Vp vp = new Vp(prefix, base, left, right, strategy);
        boolean declareKeys = Randomly.getBoolean();
        try {
            drop(vp);
            vp.baseRowCountAtMaterialization = scalarLong("SELECT COUNT(*) FROM " + q(base.name));
            executeSetup(vp, "CREATE TABLE " + q(vp.source) + " (" + q(ROW_ID)
                    + " BIGINT GENERATED ALWAYS AS IDENTITY, " + definitions(base.columns) + (declareKeys
                            ? ", CONSTRAINT " + q(vp.prefix + "source_pk") + " PRIMARY KEY (" + q(ROW_ID) + ")" : "")
                    + ")");
            String columns = names(base.columns);
            int sampleRows = samplePercent == 0 ? 0
                    : (int) Math.max(1, vp.baseRowCountAtMaterialization * samplePercent / 100);
            String sampledBase = sampleRows == 0 ? q(base.name)
                    : "(SELECT " + columns + " FROM " + q(base.name) + " SAMPLE " + sampleRows + ") vp_sample";
            try {
                executeSetup(vp,
                        "INSERT INTO " + q(vp.source) + " (" + columns + ") SELECT " + columns + " FROM " + sampledBase
                                + (base.order == null ? "" : " ORDER BY " + base.order)
                                + (base.partitionStrategy == PartitionStrategy.NONE ? limitClause() : ""));
                if (samplePercent != 0) {
                    SAMPLE_CASES.incrementAndGet();
                    SAMPLE_ROWS.addAndGet(sampleRows);
                }
            } catch (SQLException sampleFailure) {
                if (samplePercent == 0) {
                    throw sampleFailure;
                }
                // A failed SAMPLE probe is a genuine capability issue only;
                // preserve the normal full-copy path for that server.
                SAMPLE_UNSUPPORTED.incrementAndGet();
                executeSetup(vp,
                        "INSERT INTO " + q(vp.source) + " (" + columns + ") SELECT " + columns + " FROM " + q(base.name)
                                + (base.order == null ? "" : " ORDER BY " + base.order)
                                + (base.partitionStrategy == PartitionStrategy.NONE ? limitClause() : ""));
            }
            copySchemaObjects(vp);
            executeSetup(vp, "CREATE TABLE " + q(vp.left) + " (" + q(ROW_ID) + " BIGINT"
                    + (left.isEmpty() ? "" : ", " + definitions(left)) + (declareKeys
                            ? ", CONSTRAINT " + q(vp.prefix + "left_pk") + " PRIMARY KEY (" + q(ROW_ID) + ")" : "")
                    + ")");
            executeSetup(vp, "CREATE TABLE " + q(vp.right) + " (" + q(ROW_ID) + " BIGINT"
                    + (right.isEmpty() ? "" : ", " + definitions(right)) + (declareKeys
                            ? ", CONSTRAINT " + q(vp.prefix + "right_pk") + " PRIMARY KEY (" + q(ROW_ID) + ")" : "")
                    + ")");
            populatePart(vp, vp.left, left);
            populatePart(vp, vp.right, right);
            mutateMaterializedParts(vp);
            vp.sourceRowCount = scalarLong("SELECT COUNT(*) FROM " + q(vp.source));
            observeStorage(base.name, vp.source, vp.left, vp.right);
            return vp;
        } catch (Exception failure) {
            try {
                cleanup(vp);
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private String limitClause() {
        return state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows > 0
                ? " LIMIT " + state.getDbmsSpecificOptions().verticalPartitioningMaxSourceRows : "";
    }

    private void evenSplit(List<MonetDBColumn> columns, List<MonetDBColumn> left, List<MonetDBColumn> right) {
        for (int i = 0; i < columns.size(); i++) {
            (i % 2 == 0 ? left : right).add(columns.get(i));
        }
        if (right.isEmpty() && !left.isEmpty()) {
            right.add(left.get(0));
        }
    }

    private Pair createPair(Vp vp, ReconstructionStrategy strategy) throws Exception {
        Pair semantic = semanticPair(vp, strategy);
        if (semantic != null) {
            return semantic;
        }
        SelectAst ast = new SelectAst();
        String rowId = columnReference(vp, ROW_ID);
        boolean aggregate = Randomly.getBoolean();
        if (aggregate) {
            MonetDBColumn groupColumn = advancedFeaturesEnabled() && Randomly.getBoolean()
                    ? Randomly.fromList(vp.columns) : null;
            if (groupColumn != null) {
                ast.projection.add(columnReference(vp, groupColumn.getName()));
                ast.groupBy.add("ROLLUP (" + columnReference(vp, groupColumn.getName()) + ")");
                increment(AdvancedFeature.ROLLUP);
            }
            ast.projection.add(Randomly.fromOptions("COUNT(*)", "COUNT(" + rowId + ")"));
            if (Randomly.getBoolean()) {
                ast.having = Randomly.fromOptions("COUNT(*) > 0", "COUNT(*) = 0", "COUNT(*) >= 2", "COUNT(*) <> 1");
            }
            String exactNumeric = aggregateNumericColumn(vp);
            String approximateNumeric = approximateNumericColumn(vp);
            if ((exactNumeric != null || approximateNumeric != null) && Randomly.getBoolean()) {
                // Vertical reconstruction is allowed to change the physical input order. SUM/AVG over DOUBLE are not
                // associative, so comparing them across the two plans produces false positives (especially after
                // cancellation of values such as -1e100 and +1e100). Keep order-insensitive MIN/MAX coverage for
                // DOUBLE, and reserve SUM/AVG for exact numeric inputs.
                if (exactNumeric == null) {
                    ast.projection.add(
                            Randomly.fromOptions("MIN(" + approximateNumeric + ")", "MAX(" + approximateNumeric + ")"));
                } else {
                    if (Randomly.getBoolean()) {
                        String caseExpr = "CAST(CASE WHEN " + exactNumeric + " IS NULL THEN 0 ELSE mod(" + exactNumeric
                                + ", 1000) END AS DECIMAL(38, 6))";
                        ast.projection.add("SUM(" + caseExpr + ")");
                    }
                    ast.projection
                            .add(Randomly.fromOptions("SUM(CAST(mod(" + exactNumeric + ", 1000) AS DECIMAL(38, 6)))",
                                    "AVG(CAST(mod(" + exactNumeric + ", 1000) AS DECIMAL(38, 6)))",
                                    "MIN(" + exactNumeric + ")", "MAX(" + exactNumeric + ")"));
                }
            }
            if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
                String text = vp.columns.stream().filter(c -> c.getType() == MonetDBDataType.VARCHAR)
                        .map(c -> columnReference(vp, c.getName())).findFirst().orElse(null);
                if (text != null) {
                    ast.projection.add("LISTAGG(" + text + ", ',') WITHIN GROUP (ORDER BY " + rowId + ")");
                    increment(AdvancedFeature.LISTAGG);
                }
            }
        } else {
            ast.projection.add(rowId);
            for (MonetDBColumn column : vp.columns) {
                ast.projection.add(columnReference(vp, column.getName()));
            }
        }
        if (!aggregate && advancedFeaturesEnabled() && Randomly.getBoolean()) {
            if (!vp.columns.isEmpty()) {
                ast.projection.add("(SELECT COUNT(*) FROM " + q(vp.source) + " q WHERE q." + q(ROW_ID) + " <= "
                        + q(vp.source) + "." + q(ROW_ID) + ") AS " + q("vp_correlated"));
                increment(AdvancedFeature.CORRELATED_SUBQUERY);
            }
        }
        if (!aggregate && advancedFeaturesEnabled() && Randomly.getBoolean()) {
            addWindowExpressions(ast, vp);
        }
        ast.from = q(vp.source);
        ast.where = outerPredicate(vp);
        if (!aggregate && Randomly.getBoolean()) {
            ast.orderBy.add(rowId);
            ast.limit = state.getRandomly().getInteger(0, 25);
            if (Randomly.getBoolean()) {
                ast.offset = state.getRandomly().getInteger(0, 5);
            }
        }
        boolean hasWindow = ast.projection.stream().anyMatch(expression -> expression.contains(" OVER ("));
        if (hasWindow) {
            ast.from = windowInput(vp, q(vp.source) + " AS " + q(vp.source));
        }
        String source = ast.render();
        List<String> setup = new ArrayList<>();
        String relation = relation(vp, strategy, setup);
        // Keep the reconstruction relation in the same SELECT plan so WHERE,
        // GROUP BY, HAVING and window operators can interact with the VP
        // joins/CTEs. Physical materialization remains covered by the
        // explicit TEMP_TABLE reconstruction strategy.
        String evaluationRelation = hasWindow ? windowInput(vp, relation) : relation;
        ast.from = evaluationRelation;
        String reconstructed = ast.render();
        if (advancedFeaturesEnabled() && !aggregate && !hasWindow && ast.orderBy.isEmpty() && ast.limit == null
                && Randomly.getBoolean()) {
            SelectAst branch = new SelectAst();
            branch.projection.addAll(ast.projection);
            branch.from = q(vp.source);
            source = "(" + source + " UNION ALL " + branch.render() + ")";
            branch.from = evaluationRelation;
            reconstructed = "(" + reconstructed + " UNION ALL " + branch.render() + ")";
        }
        for (String statement : setup) {
            try {
                execute(statement);
            } catch (SQLException e) {
                if (statement.startsWith("DROP VIEW IF EXISTS") || statement.startsWith("DROP TABLE IF EXISTS")) {
                    // IF EXISTS is not type-agnostic in MonetDB; the other DROP variant follows.
                    continue;
                }
                throw e;
            }
        }
        return new Pair(source, reconstructed, strategy, !ast.orderBy.isEmpty(), vp, new ArrayList<>(setup));
    }

    /** Strategies with changed cardinality need a semantic reference, not the unchanged source projection. */
    private Pair semanticPair(Vp vp, ReconstructionStrategy strategy) {
        String source = q(vp.source) + " s";
        String rebuilt = "(SELECT " + projection(vp, "l", "r") + " FROM " + q(vp.left) + " l JOIN " + q(vp.right)
                + " r ON l." + q(ROW_ID) + " = r." + q(ROW_ID) + ") s";
        String reference;
        String candidate;
        switch (strategy) {
        case LEFT_RIGHT_JOIN:
        case RIGHT_JOIN:
        case FULL_OUTER_SIMULATION:
        case NOT_EXISTS_ANTI:
            // Auxiliary join keys intentionally differ from the unique VP identity. Negative, positive, NULL,
            // repeated and absent keys produce real matches, multiplicity changes and unmatched rows.
            String helper = "(SELECT CAST(-1 AS BIGINT) AS vp_key, 10 AS vp_marker"
                    + " UNION ALL SELECT CAST(-1 AS BIGINT), 11 UNION ALL SELECT CAST(1 AS BIGINT), 12"
                    + " UNION ALL SELECT CAST(NULL AS BIGINT), 13 UNION ALL SELECT CAST(7 AS BIGINT), 14) h";
            String directInput = keyedInput(vp, source);
            String splitInput = keyedInput(vp, rebuilt);
            String on = "x.vp_key = h.vp_key";
            String selected = semanticProjection(vp, "x") + ", h.vp_key, h.vp_marker";
            String matched = "SELECT " + selected + " FROM " + directInput + " JOIN " + helper + " ON " + on;
            String leftOnly = "SELECT " + semanticProjection(vp, "x")
                    + ", CAST(NULL AS BIGINT), CAST(NULL AS INTEGER) FROM " + directInput
                    + " WHERE NOT EXISTS (SELECT 1 FROM " + helper + " WHERE " + on + ")";
            List<String> missing = new ArrayList<>();
            missing.add("CAST(NULL AS BIGINT)");
            for (MonetDBColumn column : vp.columns) {
                missing.add("CAST(NULL AS " + type(column.getType()) + ")");
            }
            String rightOnly = "SELECT " + String.join(", ", missing) + ", h.vp_key, h.vp_marker FROM " + helper
                    + " WHERE NOT EXISTS (SELECT 1 FROM " + directInput + " WHERE " + on + ")";
            if (strategy == ReconstructionStrategy.NOT_EXISTS_ANTI) {
                reference = "SELECT " + semanticProjection(vp, "x") + " FROM " + directInput + " LEFT JOIN " + helper
                        + " ON " + on + " WHERE h.vp_marker IS NULL";
                candidate = "SELECT " + semanticProjection(vp, "x") + " FROM " + splitInput
                        + " WHERE NOT EXISTS (SELECT 1 FROM " + helper + " WHERE " + on + ")";
            } else {
                String kind = strategy == ReconstructionStrategy.LEFT_RIGHT_JOIN ? "LEFT"
                        : strategy == ReconstructionStrategy.RIGHT_JOIN ? "RIGHT" : "FULL OUTER";
                reference = matched + " UNION ALL "
                        + (strategy == ReconstructionStrategy.RIGHT_JOIN ? rightOnly : leftOnly);
                if (strategy == ReconstructionStrategy.FULL_OUTER_SIMULATION) {
                    reference += " UNION ALL " + rightOnly;
                }
                candidate = "SELECT " + selected + " FROM " + splitInput + " " + kind + " JOIN " + helper + " ON " + on;
            }
            break;
        case RECURSIVE_CTE:
            increment(AdvancedFeature.CTE_RECURSIVE);
            reference = "SELECT " + semanticProjection(vp, "s") + ", d.depth FROM " + source
                    + " CROSS JOIN (SELECT 1 AS depth UNION ALL SELECT 2 UNION ALL SELECT 3) d";
            candidate = "WITH RECURSIVE depths(depth) AS (SELECT 1 UNION ALL SELECT depth + 1 FROM depths"
                    + " WHERE depth < 3) SELECT " + semanticProjection(vp, "s") + ", d.depth FROM " + rebuilt
                    + " CROSS JOIN depths d";
            break;
        case AGGREGATE_THEN_MERGE:
            String bucket = "mod(s." + q(ROW_ID) + ", 3)";
            String filter = "mod(t." + q(ROW_ID) + ", 3) = " + bucket;
            String count = "(SELECT COUNT(*) FROM " + q(vp.source) + " t WHERE " + filter + ")";
            String sum = "(SELECT SUM(t." + q(ROW_ID) + ") FROM " + q(vp.source) + " t WHERE " + filter + ")";
            reference = "SELECT " + semanticProjection(vp, "s") + ", " + count + " AS vp_count, " + sum
                    + " AS vp_sum FROM " + source + " WHERE " + count + " >= 2";
            candidate = "SELECT " + semanticProjection(vp, "s") + ", a.vp_count, a.vp_sum FROM " + rebuilt
                    + " JOIN (SELECT mod(" + q(ROW_ID) + ", 3) AS bucket, COUNT(*) AS vp_count, SUM(" + q(ROW_ID)
                    + ") AS vp_sum FROM " + q(vp.right) + " GROUP BY mod(" + q(ROW_ID) + ", 3)) a ON a.bucket = "
                    + bucket + " WHERE a.vp_count >= 2";
            break;
        case NATURAL_OR_USING:
        case USING_BUSINESS_COLUMNS:
            String businessHelper = "(SELECT CAST(-1 AS BIGINT) AS vp_business_key, CAST('dup' AS VARCHAR(32))"
                    + " AS vp_business_label, 10 AS vp_marker UNION ALL SELECT -1, 'dup', 11"
                    + " UNION ALL SELECT 0, 'other', 12 UNION ALL SELECT 1, 'dup', 13"
                    + " UNION ALL SELECT 1, NULL, 14 UNION ALL SELECT NULL, 'dup', 15) h";
            reference = "SELECT " + semanticProjection(vp, "x") + ", h.vp_marker FROM " + businessInput(vp, source)
                    + " JOIN " + businessHelper
                    + " ON x.vp_business_key = h.vp_business_key AND x.vp_business_label = h.vp_business_label";
            String businessJoin = strategy == ReconstructionStrategy.NATURAL_OR_USING ? " NATURAL JOIN " : " JOIN ";
            candidate = "SELECT " + semanticProjection(vp, "x") + ", h.vp_marker FROM " + businessInput(vp, rebuilt)
                    + businessJoin + businessHelper + (strategy == ReconstructionStrategy.USING_BUSINESS_COLUMNS
                            ? " USING (vp_business_key, vp_business_label)" : "");
            increment(strategy == ReconstructionStrategy.NATURAL_OR_USING ? AdvancedFeature.NATURAL_JOIN
                    : AdvancedFeature.USING_JOIN);
            break;
        case ROW_NUMBER_ALIGNMENT:
            // Reverse one side's order within nonunique buckets. Alignment must account for bucket cardinality.
            String rankFilter = "mod(t." + q(ROW_ID) + ", 3) = mod(s." + q(ROW_ID) + ", 3) AND t." + q(ROW_ID)
                    + " <= s." + q(ROW_ID);
            reference = "SELECT " + semanticProjection(vp, "s") + ", (SELECT COUNT(*) FROM " + q(vp.source)
                    + " t WHERE " + rankFilter + ") AS vp_rank FROM " + source;
            String window = "PARTITION BY mod(" + q(ROW_ID) + ", 3) ORDER BY " + q(ROW_ID);
            candidate = "WITH l0 AS (SELECT *, mod(" + q(ROW_ID) + ", 3) AS bucket, ROW_NUMBER() OVER (" + window
                    + " ASC) AS rn FROM " + q(vp.left) + "), r0 AS (SELECT *, mod(" + q(ROW_ID)
                    + ", 3) AS bucket, ROW_NUMBER() OVER (" + window + " DESC) AS rn, COUNT(*) OVER (PARTITION BY mod("
                    + q(ROW_ID) + ", 3)) AS n FROM " + q(vp.right) + ") SELECT " + projection(vp, "l0", "r0")
                    + ", l0.rn AS vp_rank FROM l0 JOIN r0 ON l0.bucket = r0.bucket AND l0.rn + r0.rn = r0.n + 1";
            break;
        default:
            return null;
        }
        return new Pair(reference, candidate, strategy, false, vp, Collections.emptyList());
    }

    private String semanticProjection(Vp vp, String alias) {
        List<String> columns = new ArrayList<>();
        columns.add(alias + "." + q(ROW_ID));
        for (MonetDBColumn column : vp.columns) {
            columns.add(alias + "." + q(column.getName()));
        }
        return String.join(", ", columns);
    }

    private String keyedInput(Vp vp, String input) {
        return "(SELECT " + semanticProjection(vp, "s") + ", CASE WHEN mod(s." + q(ROW_ID)
                + ", 4) = 0 THEN CAST(NULL AS BIGINT) ELSE mod(s." + q(ROW_ID) + ", 3) - 1 END AS vp_key FROM " + input
                + ") x";
    }

    private String businessInput(Vp vp, String input) {
        return "(SELECT " + semanticProjection(vp, "s") + ", CAST(CASE WHEN mod(s." + q(ROW_ID)
                + ", 5) = 0 THEN NULL ELSE mod(s." + q(ROW_ID)
                + ", 3) - 1 END AS INTEGER) AS vp_business_key, CAST(CASE WHEN mod(s." + q(ROW_ID)
                + ", 4) = 0 THEN NULL WHEN mod(s." + q(ROW_ID)
                + ", 4) = 1 THEN 'other' ELSE 'dup' END AS VARCHAR(8)) AS vp_business_label FROM " + input + ") x";
    }

    private String windowInput(Vp vp, String relation) {
        String input = q(vp.source);
        return "(SELECT " + input + ".*, mod(" + input + "." + q(ROW_ID) + ", 3) AS " + q("vp_window_bucket") + " FROM "
                + relation + ") AS " + q(vp.source);
    }

    private void addWindowExpressions(SelectAst ast, Vp vp) {
        // Qualify the row id: JOIN-derived relations can expose more than one
        // vp_rowid to MonetDB's name resolver even when the projection aliases
        // the result back to a single column.
        String rowId = q(vp.source) + "." + q(ROW_ID);
        String value = rowId;
        // Compute the expression in windowInput(). MonetDB 11.55 otherwise
        // loses a derived-relation qualifier nested in a window ORDER BY;
        // using an unqualified row id is ambiguous after join flattening.
        String frameRowId = rowId;
        String bucketOrder = q(vp.source) + "." + q("vp_window_bucket");
        List<String> expressions = new ArrayList<>();
        expressions.add("SUM(" + value + ") OVER (ORDER BY " + rowId
                + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS " + q("vp_window_sum"));
        increment(AdvancedFeature.WINDOW_FUNCTION);
        if (Randomly.getBoolean()) {
            expressions.add("RANK() OVER (ORDER BY " + bucketOrder + ") AS " + q("vp_window_rank"));
            increment(AdvancedFeature.WINDOW_RANK);
        }
        if (Randomly.getBoolean()) {
            expressions.add("DENSE_RANK() OVER (ORDER BY " + bucketOrder + ") AS " + q("vp_window_dense_rank"));
            increment(AdvancedFeature.WINDOW_DENSE_RANK);
        }
        if (Randomly.getBoolean()) {
            expressions.add("NTILE(3) OVER (ORDER BY " + rowId + ") AS " + q("vp_window_ntile"));
            increment(AdvancedFeature.WINDOW_NTILE);
        }
        if (Randomly.getBoolean()) {
            expressions.add("LAG(" + value + ", 1) OVER (ORDER BY " + rowId + ") AS " + q("vp_window_lag"));
            increment(AdvancedFeature.WINDOW_LAG);
        }
        if (Randomly.getBoolean()) {
            expressions.add("LEAD(" + value + ", 1) OVER (ORDER BY " + rowId + ") AS " + q("vp_window_lead"));
            increment(AdvancedFeature.WINDOW_LEAD);
        }
        if (Randomly.getBoolean()) {
            expressions.add("FIRST_VALUE(" + value + ") OVER (ORDER BY " + rowId
                    + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS " + q("vp_window_first"));
            increment(AdvancedFeature.WINDOW_FIRST_VALUE);
        }
        if (Randomly.getBoolean()) {
            expressions.add("LAST_VALUE(" + value + ") OVER (ORDER BY " + rowId
                    + " ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS " + q("vp_window_last"));
            increment(AdvancedFeature.WINDOW_LAST_VALUE);
        }
        if (Randomly.getBoolean()) {
            expressions.add("SUM(" + rowId + ") OVER (ORDER BY " + bucketOrder
                    + " RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS " + q("vp_window_range"));
            increment(AdvancedFeature.WINDOW_RANGE_FRAME);
        } else {
            expressions.add("SUM(" + rowId + ") OVER (ORDER BY " + bucketOrder + ", " + frameRowId
                    + " ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS " + q("vp_window_rows"));
            increment(AdvancedFeature.WINDOW_ROWS_FRAME);
        }
        if (Randomly.getBoolean()) {
            if (supportsWindowFilter()) {
                expressions.add("COUNT(*) FILTER (WHERE mod(" + rowId + ", 2) = 0) OVER (ORDER BY " + rowId
                        + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS " + q("vp_window_filter"));
            } else {
                // MonetDB 11.55 does not implement FILTER on window aggregates. Exercise the same filtered-count
                // semantics with a portable CASE expression instead of dropping this coverage on every query.
                expressions.add("SUM(CASE WHEN mod(" + rowId + ", 2) = 0 THEN 1 ELSE 0 END) OVER (ORDER BY " + rowId
                        + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS " + q("vp_window_filter"));
            }
            increment(AdvancedFeature.WINDOW_FILTER);
        }
        ast.projection.addAll(expressions);
    }

    private boolean supportsWindowFilter() {
        Boolean cached = WINDOW_FILTER_SUPPORTED;
        if (cached != null) {
            return cached;
        }
        synchronized (WINDOW_CAPABILITY_LOCK) {
            if (WINDOW_FILTER_SUPPORTED == null) {
                try (Statement statement = state.getConnection().createStatement();
                        ResultSet ignored = statement.executeQuery("SELECT COUNT(*) FILTER (WHERE TRUE) OVER ()")) {
                    // Consume the single-row capability probe so the JDBC
                    // driver can finish the result cleanly before the
                    // connection is reused for the generated query.
                    ignored.next();
                    WINDOW_FILTER_SUPPORTED = true;
                } catch (SQLException unsupported) {
                    WINDOW_FILTER_SUPPORTED = false;
                    WINDOW_FILTER_UNSUPPORTED.incrementAndGet();
                }
            }
            return WINDOW_FILTER_SUPPORTED;
        }
    }

    private boolean advancedFeaturesEnabled() {
        MonetDBOptions options = state.getDbmsSpecificOptions();
        return options.verticalPartitioningAdvancedQueryFeatures || options.verticalPartitioningDeepExploration;
    }

    private String columnReference(Vp vp, String column) {
        return q(vp.source) + "." + q(column);
    }

    private String outerPredicate(Vp vp) {
        if (vp.columns.isEmpty()) {
            return null;
        }
        List<MonetDBColumn> leftOnly = vp.leftColumns.stream().filter(c -> !vp.rightColumns.contains(c))
                .collect(Collectors.toList());
        List<MonetDBColumn> rightOnly = vp.rightColumns.stream().filter(c -> !vp.leftColumns.contains(c))
                .collect(Collectors.toList());
        MonetDBColumn left = Randomly.fromList(leftOnly.isEmpty() ? vp.columns : leftOnly);
        String first = columnReference(vp, left.getName());
        String predicate = Randomly.fromOptions(first + " IS NULL", first + " IS NOT NULL");
        List<MonetDBColumn> candidates = rightOnly.isEmpty() ? vp.columns : rightOnly;
        List<MonetDBColumn> comparable = candidates.stream()
                .filter(c -> c != left && c.getType() == left.getType()
                        && (c.getType().isNumeric() || c.getType() == MonetDBDataType.VARCHAR))
                .collect(Collectors.toList());
        if (!comparable.isEmpty()) {
            String second = columnReference(vp, Randomly.fromList(comparable).getName());
            String comparison = first + Randomly.fromOptions(" = ", " <> ", " < ", " >= ") + second;
            predicate = Randomly.fromOptions("(" + predicate + " AND " + comparison + ")",
                    "(" + predicate + " OR " + comparison + ")", "NOT (" + predicate + " OR " + comparison + ")");
        } else if (vp.columns.size() > 1) {
            List<MonetDBColumn> others = candidates.stream().filter(c -> c != left).collect(Collectors.toList());
            if (others.isEmpty()) {
                others = vp.columns.stream().filter(c -> c != left).collect(Collectors.toList());
            }
            MonetDBColumn other = Randomly.fromList(others);
            String second = columnReference(vp, other.getName());
            predicate = Randomly.fromOptions("(" + predicate + " AND " + second + " IS NOT NULL)",
                    "(" + predicate + " OR " + second + " IS NULL)",
                    "NOT (" + predicate + " AND " + second + " IS NULL)");
        }
        return Randomly.getBoolean() ? predicate : null;
    }

    private String aggregateNumericColumn(Vp vp) {
        return vp.columns.stream()
                .filter(c -> c.getType() == MonetDBDataType.INT || c.getType() == MonetDBDataType.BIGINT
                        || c.getType() == MonetDBDataType.HUGEINT || c.getType() == MonetDBDataType.DECIMAL)
                .map(c -> columnReference(vp, c.getName())).findFirst().orElse(null);
    }

    private String approximateNumericColumn(Vp vp) {
        return vp.columns.stream().filter(c -> c.getType() == MonetDBDataType.DOUBLE)
                .map(c -> columnReference(vp, c.getName())).findFirst().orElse(null);
    }

    private void increment(AdvancedFeature feature) {
        ADVANCED_COUNTS.get(feature).incrementAndGet();
    }

    private String relation(Vp vp, ReconstructionStrategy strategy, List<String> setup) {
        String projection = projection(vp, "l", "r");
        String join = q(vp.left) + " l JOIN " + q(vp.right) + " r ON l." + q(ROW_ID) + " = r." + q(ROW_ID);
        switch (strategy) {
        case SCALAR_SUBQUERY:
            return "(SELECT " + lookupProjection(vp) + " FROM " + q(vp.left) + " l) AS " + q(vp.source);
        case EXISTS_FILTER:
            return "(SELECT " + lookupProjection(vp) + " FROM " + q(vp.left) + " l WHERE EXISTS (SELECT 1 FROM "
                    + q(vp.right) + " r WHERE r." + q(ROW_ID) + " = l." + q(ROW_ID) + ")) AS " + q(vp.source);
        case IN_FILTER:
            return "(SELECT " + lookupProjection(vp) + " FROM " + q(vp.left) + " l WHERE l." + q(ROW_ID)
                    + " IN (SELECT r." + q(ROW_ID) + " FROM " + q(vp.right) + " r)) AS " + q(vp.source);
        case DERIVED_TABLE:
            join = "(SELECT * FROM " + q(vp.left) + ") l JOIN (SELECT * FROM " + q(vp.right) + ") r ON l." + q(ROW_ID)
                    + " = r." + q(ROW_ID);
            break;
        case CTE:
            String baseCte = "l AS (SELECT * FROM " + q(vp.left) + "), r AS (SELECT * FROM " + q(vp.right)
                    + "), joined AS (SELECT " + projection + " FROM l JOIN r ON l." + q(ROW_ID) + " = r." + q(ROW_ID)
                    + ")";
            if (advancedFeaturesEnabled() && Randomly.getBoolean()) {
                if (Randomly.getBoolean()) {
                    increment(AdvancedFeature.CTE_MULTI_REFERENCE);
                    return "(WITH " + baseCte + " SELECT * FROM joined INTERSECT SELECT * FROM joined) " + q(vp.source);
                }
                increment(AdvancedFeature.CTE_NESTED);
                return "(WITH " + baseCte
                        + ", layer1 AS (SELECT * FROM joined), layer2 AS (SELECT * FROM layer1) SELECT * FROM layer2) "
                        + q(vp.source);
            }
            return "(WITH " + baseCte + " SELECT * FROM joined) AS " + q(vp.source);
        case VIEW:
            // Preserve the real view/catalog path; clear stale objects before creation.
            String viewName = vp.prefix + "view_" + OBJECT_IDS.incrementAndGet();
            vp.transientObjects.add(viewName);
            setup.add("CREATE VIEW " + q(viewName) + " AS SELECT " + projection + " FROM " + join);
            return q(viewName) + " AS " + q(vp.source);
        case TEMP_TABLE:
            String tempName = vp.prefix + "temp_" + OBJECT_IDS.incrementAndGet();
            String tempConstraint = tempName + "_pk";
            vp.transientObjects.add(tempName);
            setup.add("CREATE TEMPORARY TABLE " + q(tempName) + " (" + q(ROW_ID) + " BIGINT, " + definitions(vp.columns)
                    + ", CONSTRAINT " + q(tempConstraint) + " PRIMARY KEY (" + q(ROW_ID)
                    + ")) ON COMMIT PRESERVE ROWS");
            setup.add("INSERT INTO " + q(tempName) + " (" + q(ROW_ID) + ", " + names(vp.columns) + ") SELECT "
                    + projection + " FROM " + join);
            return q(tempName) + " AS " + q(vp.source);
        case JSON_PACK:
            return "(SELECT " + projection + " FROM " + jsonPackedRelation(vp) + ") AS " + q(vp.source);
        case STRING_PACK:
            return "(SELECT " + projection + " FROM " + q(vp.left) + " l JOIN " + q(vp.right) + " r ON CAST(l."
                    + q(ROW_ID) + " AS VARCHAR(64)) = CAST(r." + q(ROW_ID) + " AS VARCHAR(64))) " + q(vp.source);
        case STRAIGHT_JOIN:
            return "(SELECT " + projection + " FROM " + q(vp.left) + " l CROSS JOIN " + q(vp.right) + " r WHERE l."
                    + q(ROW_ID) + " = r." + q(ROW_ID) + ") " + q(vp.source);
        case LATERAL_DERIVED:
            // Keep the lateral path, but project only the join key from the
            // lateral side. SELECT * exposes a second copy of every payload
            // column; MonetDB may then rewrite LATERAL + ROLLUP using those
            // unused columns and produce spurious groups.
            List<String> lateralColumns = new ArrayList<>();
            lateralColumns.add("r0." + q(ROW_ID));
            for (MonetDBColumn column : vp.rightColumns) {
                lateralColumns.add("r0." + q(column.getName()));
            }
            return "(SELECT " + projection + " FROM " + q(vp.left) + " l JOIN LATERAL (SELECT "
                    + String.join(", ", lateralColumns) + " FROM " + q(vp.right) + " r0 WHERE r0." + q(ROW_ID) + " = l."
                    + q(ROW_ID) + ") r ON TRUE) " + q(vp.source);
        case JOIN:
            break;
        default:
            throw new AssertionError(strategy);
        }
        return "(SELECT " + projection + " FROM " + join + ") AS " + q(vp.source);
    }

    private String lookupProjection(Vp vp) {
        List<String> values = new ArrayList<>();
        values.add("l." + q(ROW_ID) + " AS " + q(ROW_ID));
        for (MonetDBColumn column : vp.columns) {
            if (vp.leftColumns.contains(column)) {
                values.add("l." + q(column.getName()) + " AS " + q(column.getName()));
            } else {
                values.add("(SELECT r." + q(column.getName()) + " FROM " + q(vp.right) + " r WHERE r." + q(ROW_ID)
                        + " = l." + q(ROW_ID) + ") AS " + q(column.getName()));
            }
        }
        return String.join(", ", values);
    }

    private String projection(Vp vp, String leftAlias, String rightAlias) {
        List<String> values = new ArrayList<>();
        values.add(leftAlias + "." + q(ROW_ID) + " AS " + q(ROW_ID));
        for (MonetDBColumn column : vp.columns) {
            String alias = vp.leftColumns.contains(column) ? leftAlias
                    : vp.rightColumns.contains(column) ? rightAlias : leftAlias;
            values.add(alias + "." + q(column.getName()) + " AS " + q(column.getName()));
        }
        return String.join(", ", values);
    }

    private void compare(Pair pair) throws Exception {
        // VP setup uses direct JDBC statements, so it is not captured by the
        // normal QueryManager statement history. Keep both SELECTs in the
        // reproduction state as well as the live -cur log; otherwise a
        // failure log contains only the final assertion and cannot be replayed.
        state.getState().logStatement(pair.source);
        state.getState().logStatement(pair.reconstructed);
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent("-- VP SOURCE: " + pair.source);
            state.getLogger().writeCurrent("-- VP RECONSTRUCTED: " + pair.reconstructed);
        }
        RowSet left = read(pair.source);
        RowSet right = read(pair.reconstructed);
        if (state.getOptions().logEachSelect()) {
            state.getLogger()
                    .writeCurrent("-- VP RESULT: strategy=" + pair.strategy + ", input=" + pair.vp.sourceRowCount
                            + ", reference=" + left.ordered.size() + ", reconstructed=" + right.ordered.size());
        }
        MonetDBResultComparison.Report report = compareLayers(left, right, pair.source, pair.reconstructed, pair.ordered);
        if (report.fails(pair.ordered, state.getDbmsSpecificOptions().verticalPartitioningStrictRepresentation)) {
            MISMATCHES.incrementAndGet();
            if (state.getDbmsSpecificOptions().verticalPartitioningSuppressKnownPatterns
                    && classifyKnownPattern(pair, left, right) != null) {
                return;
            }
            throw new AssertionError(report + "\n" + formatBugReport(pair, left, right));
        }
    }

    private MonetDBResultComparison.Report compareLayers(RowSet left, RowSet right, String source, String reconstructed,
            boolean ordered) throws IOException {
        MonetDBOptions options = state.getDbmsSpecificOptions();
        String query = source.toUpperCase(Locale.ROOT);
        boolean scalarAggregate = !query.contains("GROUP BY") && !query.contains(" OVER (")
                && (query.contains("SUM(") || query.contains("AVG("));
        MonetDBResultComparison.Report report = MonetDBResultComparison.compare(left.layers, right.layers, scalarAggregate,
                options.verticalPartitioningAggregateAbsoluteError, options.verticalPartitioningAggregateRelativeError);
        Map<String, Boolean> layers = new LinkedHashMap<>();
        layers.put("exact_bag", report.exactBag);
        layers.put("normalized_bag", report.normalizedBag);
        layers.put("metadata", report.metadata);
        layers.put("null_bag", report.nullBag);
        layers.put("observed_order", report.exactOrder);
        if (ordered) {
            layers.put("required_order", report.normalizedOrder && report.nullOrder);
        }
        layers.forEach((name, equal) -> COMPARISON_COUNTS.computeIfAbsent(name + (equal ? ".equal" : ".different"),
                k -> new AtomicLong()).incrementAndGet());
        if (report.diagnosticDifference()) {
            // Independent of log-each-select; retain raw/normalized/metadata/order evidence even when policy accepts it.
            synchronized (MonetDBResultComparison.class) {
                java.nio.file.Path log = java.nio.file.Path.of("logs/monetdb/vp-comparison-layers.log");
                Files.createDirectories(log.getParent());
                Files.writeString(log, "database=" + state.getDatabaseName() + ", order_required=" + ordered + "\n"
                        + report + "\nSOURCE: " + source + "\nRECONSTRUCTED: " + reconstructed
                        + "\nLEFT_METADATA: " + left.layers.metadata + "\nRIGHT_METADATA: " + right.layers.metadata
                        + "\nLEFT_EXACT: " + left.layers.exact.stream().limit(MAX_DIFF_ROWS).collect(Collectors.toList())
                        + "\nRIGHT_EXACT: " + right.layers.exact.stream().limit(MAX_DIFF_ROWS).collect(Collectors.toList())
                        + "\nLEFT_NULLS: " + left.layers.nulls.stream().limit(MAX_DIFF_ROWS).collect(Collectors.toList())
                        + "\nRIGHT_NULLS: " + right.layers.nulls.stream().limit(MAX_DIFF_ROWS).collect(Collectors.toList())
                        + "\nLAYER_DIFF: " + MonetDBResultComparison.evidence(left.layers, right.layers) + "\n\n",
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        }
        return report;
    }

    private String formatBugReport(Pair pair, RowSet sourceRows, RowSet splitRows) {
        Vp vp = pair.vp;
        StringBuilder sb = new StringBuilder();
        sb.append("[VERTICAL PARTITIONING BUG CANDIDATE]").append(System.lineSeparator());
        sb.append("Mode: RANDOM_TABLE_REWRITE").append(System.lineSeparator());
        sb.append("Database: ").append(state.getDatabaseName()).append(System.lineSeparator());
        sb.append("MonetDB version: ").append(databaseVersion()).append(System.lineSeparator());
        sb.append("Strategy summary: split=").append(vp.splitStrategy).append(", reconstruction=").append(pair.strategy)
                .append(", shape=").append(vp.baseMetadata.shape).append(", prefix=").append(vp.prefix)
                .append(System.lineSeparator());
        sb.append("Base table: ").append(vp.base).append(System.lineSeparator());
        sb.append("Base table DDL: ").append(baseTableDdl(vp.baseMetadata)).append(System.lineSeparator());
        sb.append("Base row count at selection: ").append(vp.baseMetadata.rowCount).append(System.lineSeparator());
        sb.append("Base row count at materialization: ").append(vp.baseRowCountAtMaterialization)
                .append(System.lineSeparator());
        sb.append("Base row count at mismatch: ").append(safeRowCount(vp.base)).append(System.lineSeparator());
        sb.append("VP source row count: ").append(safeRowCount(vp.source)).append(" (materialized=")
                .append(vp.sourceRowCount).append(")").append(System.lineSeparator());
        sb.append("VP left row count: ").append(safeRowCount(vp.left)).append(System.lineSeparator());
        sb.append("VP right row count: ").append(safeRowCount(vp.right)).append(System.lineSeparator());
        sb.append("Base shape: ").append(vp.baseMetadata.shape).append(System.lineSeparator());
        sb.append("Stable row order: ").append(vp.baseMetadata.order == null ? "NONE" : vp.baseMetadata.order)
                .append(System.lineSeparator());
        sb.append("Prefix: ").append(vp.prefix).append(System.lineSeparator());
        sb.append("Split strategy: ").append(vp.splitStrategy).append(System.lineSeparator());
        sb.append("Left columns: ").append(columnNames(vp.leftColumns)).append(System.lineSeparator());
        sb.append("Right columns: ").append(columnNames(vp.rightColumns)).append(System.lineSeparator());
        sb.append("Overlap columns: ").append(columnNames(overlapColumns(vp))).append(System.lineSeparator());
        sb.append("Reconstruction key mode: ").append(reconstructionKeyMode(pair)).append(System.lineSeparator());
        sb.append("Reconstruction key columns: {").append(ROW_ID).append("}").append(System.lineSeparator());
        sb.append("Reconstruction strategy: ").append(pair.strategy).append(System.lineSeparator());
        sb.append("Description: Independent semantic reference compared with vertical-partition reconstruction")
                .append(System.lineSeparator());
        sb.append("Optimizer profile: MonetDB default optimizer pipeline (no VP-specific override)")
                .append(System.lineSeparator());
        sb.append("Time-travel timestamp: N/A (not used by the MonetDB VP oracle)").append(System.lineSeparator());
        sb.append("Session vars: ").append(captureSessionVars()).append(System.lineSeparator());
        sb.append("Session SET statements:").append(System.lineSeparator());
        sb.append("SET SCHEMA ").append(q(MonetDBProvider.schemaName(state.getDatabaseName()))).append(";")
                .append(System.lineSeparator());
        sb.append("Setup statements:").append(System.lineSeparator());
        for (String setup : vp.setupStatements) {
            sb.append(setup).append(";").append(System.lineSeparator());
        }
        for (String setup : pair.setupStatements) {
            sb.append(setup).append(";").append(System.lineSeparator());
        }
        sb.append("VP source snapshot:").append(System.lineSeparator());
        sb.append(formatSnapshot(vp.source, vp.columns));
        sb.append("VP left snapshot:").append(System.lineSeparator());
        sb.append(formatSnapshot(vp.left, vp.leftColumns));
        sb.append("VP right snapshot:").append(System.lineSeparator());
        sb.append(formatSnapshot(vp.right, vp.rightColumns));
        sb.append("Single query: ").append(pair.source).append(System.lineSeparator());
        sb.append("Split query: ").append(pair.reconstructed).append(System.lineSeparator());
        sb.append("Single query plan:").append(System.lineSeparator());
        sb.append(formatExplain(pair.source));
        sb.append("Split query plan:").append(System.lineSeparator());
        sb.append(formatExplain(pair.reconstructed));
        sb.append("Single result sample: ").append(sampleWithCounts(sourceRows)).append(System.lineSeparator());
        sb.append("Split result sample: ").append(sampleWithCounts(splitRows)).append(System.lineSeparator());
        sb.append("Only in single: ").append(diff(sourceRows, splitRows)).append(System.lineSeparator());
        sb.append("Only in split: ").append(diff(splitRows, sourceRows)).append(System.lineSeparator());
        if (pair.ordered && !sourceRows.ordered.equals(splitRows.ordered)) {
            sb.append("Single ordered result sample: ").append(orderedSample(sourceRows))
                    .append(System.lineSeparator());
            sb.append("Split ordered result sample: ").append(orderedSample(splitRows)).append(System.lineSeparator());
            sb.append("First ordered mismatch: ").append(firstOrderedMismatch(sourceRows, splitRows))
                    .append(System.lineSeparator());
        }
        return sb.toString();
    }

    private String databaseVersion() {
        try {
            return state.getConnection().getJdbcConnection().getMetaData().getDatabaseProductVersion();
        } catch (SQLException e) {
            return "<unavailable: " + e.getMessage() + ">";
        }
    }

    private String baseTableDdl(Base base) {
        try {
            DatabaseMetaData metadata = state.getConnection().getJdbcConnection().getMetaData();
            String schema = MonetDBProvider.schemaName(state.getDatabaseName());
            List<String> definitions = new ArrayList<>();
            try (ResultSet columns = metadata.getColumns(null, schema, base.name, "%")) {
                while (columns.next()) {
                    StringBuilder definition = new StringBuilder(q(columns.getString("COLUMN_NAME"))).append(" ")
                            .append(jdbcTypeDefinition(columns));
                    definition.append(
                            columns.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls ? " NOT NULL" : " NULL");
                    String defaultValue = columns.getString("COLUMN_DEF");
                    if (defaultValue != null) {
                        definition.append(" DEFAULT ").append(defaultValue);
                    }
                    definitions.add(definition.toString());
                }
            }
            List<String> primaryKey = new ArrayList<>();
            try (ResultSet keys = metadata.getPrimaryKeys(null, schema, base.name)) {
                while (keys.next()) {
                    primaryKey.add(q(keys.getString("COLUMN_NAME")));
                }
            }
            if (!primaryKey.isEmpty()) {
                definitions.add("PRIMARY KEY (" + String.join(", ", primaryKey) + ")");
            }
            if (!definitions.isEmpty()) {
                boolean partitioned = "pt_range".equalsIgnoreCase(base.name) || "pt_list".equalsIgnoreCase(base.name);
                StringBuilder ddl = new StringBuilder(partitioned ? "CREATE MERGE TABLE " : "CREATE TABLE ")
                        .append(q(base.name)).append(" (").append(String.join(", ", definitions)).append(")");
                if (partitioned) {
                    ddl.append(" PARTITION BY ").append("pt_list".equalsIgnoreCase(base.name) ? "VALUES" : "RANGE")
                            .append(" ON (").append(q("id")).append(")");
                }
                return ddl.toString();
            }
        } catch (SQLException e) {
            // Fall back to the schema model below. A diagnostic failure must not hide the mismatch.
        }
        String fallback = base.columns.stream()
                .map(c -> q(c.getName()) + " " + type(c.getType()) + (c.isNullable() ? " NULL" : " NOT NULL"))
                .collect(Collectors.joining(", "));
        return "CREATE TABLE " + q(base.name) + " (" + fallback + ") /* reconstructed from schema model */";
    }

    private String jdbcTypeDefinition(ResultSet column) throws SQLException {
        String typeName = column.getString("TYPE_NAME");
        if (typeName == null) {
            typeName = "UNKNOWN";
        }
        if (typeName.contains("(")) {
            return typeName;
        }
        int jdbcType = column.getInt("DATA_TYPE");
        int size = column.getInt("COLUMN_SIZE");
        int scale = column.getInt("DECIMAL_DIGITS");
        if (jdbcType == Types.DECIMAL || jdbcType == Types.NUMERIC) {
            return typeName + "(" + size + ", " + scale + ")";
        }
        if ((jdbcType == Types.CHAR || jdbcType == Types.VARCHAR || jdbcType == Types.NCHAR
                || jdbcType == Types.NVARCHAR) && size > 0) {
            return typeName + "(" + size + ")";
        }
        return typeName;
    }

    private String safeRowCount(String table) {
        try {
            return Long.toString(scalarLong("SELECT COUNT(*) FROM " + q(table)));
        } catch (SQLException | AssertionError e) {
            return "<unavailable: " + e.getMessage() + ">";
        }
    }

    private List<MonetDBColumn> overlapColumns(Vp vp) {
        return vp.columns.stream().filter(vp.leftColumns::contains).filter(vp.rightColumns::contains)
                .collect(Collectors.toList());
    }

    private String columnNames(List<MonetDBColumn> columns) {
        return columns.stream().map(MonetDBColumn::getName).collect(Collectors.joining(", ", "{", "}"));
    }

    private String reconstructionKeyMode(Pair pair) {
        switch (pair.strategy) {
        case ROW_NUMBER_ALIGNMENT:
            return "OPPOSITE_ORDER_WITHIN_BUCKETS";
        case NATURAL_OR_USING:
        case USING_BUSINESS_COLUMNS:
            return "TWO_NULLABLE_BUSINESS_COLUMNS";
        case LEFT_RIGHT_JOIN:
        case RIGHT_JOIN:
        case FULL_OUTER_SIMULATION:
        case NOT_EXISTS_ANTI:
            return "NULLABLE_DUPLICATE_SIGNED_AUXILIARY_KEY";
        default:
            return "SYNTHETIC_IDENTITY";
        }
    }

    private String captureSessionVars() {
        List<String> values = new ArrayList<>();
        values.add("current_schema=" + MonetDBProvider.schemaName(state.getDatabaseName()));
        try {
            DatabaseMetaData metadata = state.getConnection().getJdbcConnection().getMetaData();
            values.add("user=" + metadata.getUserName());
            values.add("driver=" + metadata.getDriverName() + " " + metadata.getDriverVersion());
            values.add("auto_commit=" + state.getConnection().getJdbcConnection().getAutoCommit());
        } catch (SQLException e) {
            values.add("jdbc_state=<unavailable: " + e.getMessage() + ">");
        }
        return String.join(", ", values);
    }

    private String formatSnapshot(String table, List<MonetDBColumn> columns) {
        List<String> selected = new ArrayList<>();
        selected.add(q(ROW_ID) + " AS " + q("__vp_row_id"));
        selected.addAll(columns.stream().map(c -> q(c.getName())).collect(Collectors.toList()));
        String sql = "SELECT " + String.join(", ", selected) + " FROM " + q(table) + " ORDER BY " + q(ROW_ID);
        StringBuilder sb = new StringBuilder();
        sb.append(sql).append(";").append(System.lineSeparator());
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            ResultSetMetaData metadata = rs.getMetaData();
            while (rs.next()) {
                List<String> values = new ArrayList<>();
                for (int i = 1; i <= metadata.getColumnCount(); i++) {
                    values.add(metadata.getColumnLabel(i) + "=" + canonical(rs, i));
                }
                sb.append(String.join(" | ", values)).append(System.lineSeparator());
            }
        } catch (SQLException | RuntimeException e) {
            sb.append("<unavailable: ").append(e.getMessage()).append(">").append(System.lineSeparator());
        }
        return sb.toString();
    }

    private String formatExplain(String sql) {
        String plan = explain(sql);
        return "EXPLAIN " + sql + ";" + System.lineSeparator() + plan
                + (plan.endsWith(System.lineSeparator()) ? "" : System.lineSeparator());
    }

    private String sampleWithCounts(RowSet rows) {
        return rows.rows.entrySet().stream().limit(MAX_DIFF_ROWS).map(e -> displayRow(e.getKey()) + " x" + e.getValue())
                .collect(Collectors.joining(", "));
    }

    private String orderedSample(RowSet rows) {
        return rows.ordered.stream().limit(MAX_DIFF_ROWS).map(this::displayRow).collect(Collectors.joining(", "));
    }

    private String firstOrderedMismatch(RowSet left, RowSet right) {
        int common = Math.min(left.ordered.size(), right.ordered.size());
        for (int i = 0; i < common; i++) {
            if (!Objects.equals(left.ordered.get(i), right.ordered.get(i))) {
                return "index=" + i + ", single=" + displayRow(left.ordered.get(i)) + ", split="
                        + displayRow(right.ordered.get(i));
            }
        }
        return "index=" + common + ", single="
                + (common < left.ordered.size() ? displayRow(left.ordered.get(common)) : "<end>") + ", split="
                + (common < right.ordered.size() ? displayRow(right.ordered.get(common)) : "<end>");
    }

    private String diff(RowSet left, RowSet right) {
        Map<String, Integer> remaining = new LinkedHashMap<>(left.rows);
        for (Map.Entry<String, Integer> entry : right.rows.entrySet()) {
            int count = remaining.getOrDefault(entry.getKey(), 0) - entry.getValue();
            if (count > 0) {
                remaining.put(entry.getKey(), count);
            } else {
                remaining.remove(entry.getKey());
            }
        }
        return remaining.entrySet().stream().limit(MAX_DIFF_ROWS).map(e -> displayRow(e.getKey()) + " x" + e.getValue())
                .collect(Collectors.joining(", "));
    }

    private String displayRow(String row) {
        return row.replace('\u001f', '|');
    }

    private String classifyKnownPattern(Pair pair, RowSet source, RowSet reconstructed) {
        // Intentionally empty for the initial MonetDB port. The suppression
        // option is wired, but no unverified mismatch is hidden.
        return null;
    }

    private String explain(String sql) {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery("EXPLAIN " + sql)) {
            StringBuilder plan = new StringBuilder();
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columns; i++) {
                    if (i > 1) {
                        plan.append(" | ");
                    }
                    plan.append(Objects.toString(rs.getObject(i), "<NULL>"));
                }
                plan.append('\n');
            }
            return plan.toString();
        } catch (SQLException e) {
            return "<unavailable: " + e.getMessage() + ">";
        }
    }

    private static void writeStats() {
        try {
            Files.createDirectories(STATS_FILE.toPath().getParent());
            StringBuilder content = new StringBuilder();
            content.append("runs=").append(RUNS.get()).append('\n');
            content.append("pairs=").append(PAIRS.get()).append('\n');
            content.append("mismatches=").append(MISMATCHES.get()).append('\n');
            appendCounters(content, "base_shape", BASE_SHAPE_COUNTS);
            appendCounters(content, "split", SPLIT_COUNTS);
            appendCounters(content, "reconstruction", RECONSTRUCTION_COUNTS);
            appendCounters(content, "advanced", ADVANCED_COUNTS);
            MonetDBMutationFeedback.appendStats(content);
            COMPARISON_COUNTS.keySet().stream().sorted().forEach(key -> content.append("comparison.").append(key)
                    .append('=').append(COMPARISON_COUNTS.get(key).get()).append('\n'));
            appendCounters(content, "schema_copy", SCHEMA_COPY_COUNTS);
            content.append("partitioned_base=" + (PARTITIONED_BASE_COUNTS.get(PartitionStrategy.RANGE).get()
                    + PARTITIONED_BASE_COUNTS.get(PartitionStrategy.LIST).get()) + '\n');
            appendCounters(content, "partition_strategy", PARTITIONED_BASE_COUNTS);
            content.append("partition_query_pairs=").append(PARTITION_PAIRS.get()).append('\n');
            content.append("partition_vp_pairs=").append(PARTITION_VP_PAIRS.get()).append('\n');
            content.append("partition_pruning_confirmed=").append(PRUNING_CONFIRMED.get()).append('\n');
            content.append("partition_pruning_not_observed=").append(PRUNING_NOT_OBSERVED.get()).append('\n');
            content.append("partition_routing_rejections=").append(ROUTING_REJECTIONS.get()).append('\n');
            content.append("storage_probes=").append(STORAGE_PROBES.get()).append('\n');
            content.append("storage_unavailable=").append(STORAGE_UNAVAILABLE.get()).append('\n');
            content.append("storage_layout_differences=").append(STORAGE_LAYOUT_DIFFERENCES.get()).append('\n');
            content.append("large_query_pairs=").append(LARGE_QUERY_PAIRS.get()).append('\n');
            content.append("large_source_max_rows=").append(LARGE_SOURCE_MAX.get()).append('\n');
            content.append("sample_rows=").append(SAMPLE_ROWS.get()).append('\n');
            content.append("sample_cases=").append(SAMPLE_CASES.get()).append('\n');
            content.append("sample_unsupported=").append(SAMPLE_UNSUPPORTED.get()).append('\n');
            content.append("merge_unsupported=").append(MERGE_UNSUPPORTED.get()).append('\n');
            content.append("window_filter_unsupported=").append(WINDOW_FILTER_UNSUPPORTED.get()).append('\n');
            Files.writeString(STATS_FILE.toPath(), content, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException ignored) {
            // Shutdown statistics must never hide the original database result.
        }
    }

    private static <E extends Enum<E>> Map<E, AtomicLong> counters(Class<E> type) {
        Map<E, AtomicLong> counters = new ConcurrentHashMap<>();
        for (E value : type.getEnumConstants()) {
            counters.put(value, new AtomicLong());
        }
        return counters;
    }

    private static <E extends Enum<E>> void appendCounters(StringBuilder output, String prefix,
            Map<E, AtomicLong> counters) {
        for (E key : counters.keySet().stream().sorted().collect(Collectors.toList())) {
            output.append(prefix).append('.').append(key.name().toLowerCase(Locale.ROOT)).append('=')
                    .append(counters.get(key).get()).append('\n');
        }
    }

    private RowSet read(String sql) throws SQLException {
        RowSet result = new RowSet();
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            int count = rs.getMetaData().getColumnCount();
            result.layers.metadata(rs.getMetaData());
            while (rs.next()) {
                result.layers.row(rs);
                List<String> values = new ArrayList<>();
                for (int i = 1; i <= count; i++) {
                    values.add(canonical(rs, i));
                }
                String row = String.join("\u001f", values);
                result.rows.merge(row, 1, Integer::sum);
                result.ordered.add(row);
            }
        } catch (SQLException e) {
            throw new SQLException("Failed query: " + sql, e);
        }
        return result;
    }

    private String canonical(ResultSet rs, int index) throws SQLException {
        Object value = rs.getObject(index);
        if (value == null) {
            return "<NULL>";
        }
        int type = rs.getMetaData().getColumnType(index);
        if (type == Types.TINYINT || type == Types.SMALLINT || type == Types.INTEGER || type == Types.BIGINT
                || type == Types.DECIMAL || type == Types.NUMERIC) {
            BigDecimal decimal = value instanceof BigDecimal ? (BigDecimal) value : new BigDecimal(value.toString());
            return "<NUMERIC:" + decimal.stripTrailingZeros().toPlainString() + ">";
        }
        if (type == Types.FLOAT || type == Types.REAL || type == Types.DOUBLE) {
            try {
                return "<FLOAT:" + Double.toHexString(rs.getDouble(index)) + ">";
            } catch (NumberFormatException ignored) {
                return "<FLOAT:" + value.toString().toLowerCase(Locale.ROOT) + ">";
            }
        }
        if (type == Types.BOOLEAN || type == Types.BIT) {
            return "<BOOLEAN:" + Boolean.toString(rs.getBoolean(index)) + ">";
        }
        if (type == Types.BINARY || type == Types.VARBINARY || type == Types.LONGVARBINARY || type == Types.BLOB) {
            byte[] bytes = rs.getBytes(index);
            StringBuilder hex = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                hex.append(String.format(Locale.ROOT, "%02x", b & 0xff));
            }
            return "<BINARY:" + hex + ">";
        }
        return "<" + type + ":" + Objects.toString(value) + ">";
    }

    private void copySchemaObjects(Vp vp) {
        if (!state.getDbmsSpecificOptions().verticalPartitioningSourceCopySchemaObjects) {
            SCHEMA_COPY_COUNTS.get(SchemaCopyOutcome.SKIPPED).incrementAndGet();
            return;
        }
        try {
            DatabaseMetaData metadata = state.getConnection().getJdbcConnection().getMetaData();
            copyPrimaryKey(metadata, vp);
            String schema = MonetDBProvider.schemaName(state.getDatabaseName());
            try (ResultSet indexes = metadata.getIndexInfo(null, schema, vp.base, false, false)) {
                Map<String, List<String>> indexColumns = new LinkedHashMap<>();
                Map<String, Boolean> uniqueIndexes = new LinkedHashMap<>();
                while (indexes.next()) {
                    String name = indexes.getString("INDEX_NAME");
                    String column = indexes.getString("COLUMN_NAME");
                    if (name == null || column == null) {
                        continue;
                    }
                    if (vp.columns.stream().noneMatch(c -> c.getName().equals(column)))
                        continue;
                    indexColumns.computeIfAbsent(name, k -> new ArrayList<>()).add(q(column));
                    uniqueIndexes.put(name, !indexes.getBoolean("NON_UNIQUE"));
                }
                for (Map.Entry<String, List<String>> entry : indexColumns.entrySet()) {
                    String name = entry.getKey();
                    try {
                        boolean unique = uniqueIndexes.get(name);
                        executeSetup(vp,
                                "CREATE " + (unique ? "UNIQUE " : "") + "INDEX "
                                        + q(vp.prefix + "idx_" + Math.abs(name.hashCode())) + " ON " + q(vp.source)
                                        + " (" + String.join(", ", entry.getValue()) + ")");
                        increment(AdvancedFeature.ASYMMETRIC_INDEX);
                        SCHEMA_COPY_COUNTS.get(SchemaCopyOutcome.SUCCESS).incrementAndGet();
                    } catch (Exception ignored) {
                        SCHEMA_COPY_COUNTS.get(SchemaCopyOutcome.FAILED).incrementAndGet();
                        // Schema fidelity is best effort, but each failed object
                        // remains visible in the VP setup log.
                    }
                }
            }
        } catch (SQLException ignored) {
            SCHEMA_COPY_COUNTS.get(SchemaCopyOutcome.FAILED).incrementAndGet();
        }
    }

    private void copyPrimaryKey(DatabaseMetaData metadata, Vp vp) throws SQLException {
        try (ResultSet keys = metadata.getPrimaryKeys(null, MonetDBProvider.schemaName(state.getDatabaseName()),
                vp.base)) {
            List<String> columns = new ArrayList<>();
            while (keys.next()) {
                String column = keys.getString("COLUMN_NAME");
                if (column != null) {
                    columns.add(q(column));
                }
            }
            if (!columns.isEmpty()) {
                try {
                    executeSetup(vp, "ALTER TABLE " + q(vp.source) + " ADD CONSTRAINT " + q(vp.prefix + "base_key")
                            + " UNIQUE (" + String.join(", ", columns) + ")");
                    SCHEMA_COPY_COUNTS.get(SchemaCopyOutcome.SUCCESS).incrementAndGet();
                } catch (SQLException ignored) {
                    SCHEMA_COPY_COUNTS.get(SchemaCopyOutcome.FAILED).incrementAndGet();
                    // The synthetic identity is already the source key. A
                    // copied base key is still best effort.
                }
            }
        }
    }

    private void populatePart(Vp vp, String target, List<MonetDBColumn> columns) throws SQLException {
        String order = target.hashCode() % 3 == 0 ? " ORDER BY " + q(ROW_ID) : target.hashCode() % 3 == 1
                ? " ORDER BY " + q(ROW_ID) + " DESC" : " ORDER BY mod(" + q(ROW_ID) + ", 17), " + q(ROW_ID) + " DESC";
        if (columns.isEmpty()) {
            executeSetup(vp, "INSERT INTO " + q(target) + " (" + q(ROW_ID) + ") SELECT " + q(ROW_ID) + " FROM "
                    + q(vp.source) + order);
            return;
        }
        String names = names(columns);
        executeSetup(vp, "INSERT INTO " + q(target) + " (" + q(ROW_ID) + ", " + names + ") SELECT " + q(ROW_ID) + ", "
                + names + " FROM " + q(vp.source) + order);
    }

    private void mutateMaterializedParts(Vp vp) throws SQLException {
        MonetDBColumn column = vp.columns.stream().filter(c -> c.getType().isNumeric()).findFirst().orElse(null);
        if (column == null) {
            return;
        }
        String name = q(column.getName());
        for (String table : new String[] { vp.source, vp.left, vp.right }) {
            if ((table.equals(vp.left) && !vp.leftColumns.contains(column))
                    || (table.equals(vp.right) && !vp.rightColumns.contains(column))) {
                continue;
            }
            executeSetup(vp,
                    "UPDATE " + q(table) + " SET " + name + " = " + name + " WHERE mod(" + q(ROW_ID) + ", 10) = 0");
        }
    }

    /** Observe MonetDB's physical column storage without affecting equivalence checks. */
    private void observeStorage(String base, String source, String left, String right) {
        // Read all four relations in one scoped catalog pass. Calling the
        // global storage() function once per relation magnifies catalog work
        // and has triggered MonetDB 11.55 crashes under concurrent workers.
        synchronized (STORAGE_LOCK) {
            Map<String, StorageObservation> observations = storage(base, source, left, right);
            StorageObservation baseStorage = observations.get(base);
            StorageObservation sourceStorage = observations.get(source);
            StorageObservation leftStorage = observations.get(left);
            StorageObservation rightStorage = observations.get(right);
            if (baseStorage == null || sourceStorage == null || leftStorage == null || rightStorage == null) {
                STORAGE_UNAVAILABLE.incrementAndGet();
                return;
            }
            STORAGE_PROBES.incrementAndGet();
            if (leftStorage.columns != rightStorage.columns || !leftStorage.modes.equals(rightStorage.modes)) {
                STORAGE_LAYOUT_DIFFERENCES.incrementAndGet();
            }
        }
    }

    private Map<String, StorageObservation> storage(String... tables) {
        Map<String, Long> columnCounts = new LinkedHashMap<>();
        Map<String, Set<String>> modesByTable = new LinkedHashMap<>();
        for (String table : tables) {
            columnCounts.put(table, 0L);
            modesByTable.put(table, new HashSet<>());
        }
        String tableList = java.util.Arrays.stream(tables).map(MonetDBVerticalPartitioningOracle::quoteLiteral)
                .collect(Collectors.joining(", "));
        String schema = "sqlancer_"
                + Objects.toString(state.getDatabaseName(), "default").replaceAll("[^A-Za-z0-9_]", "_");
        String sql = "SELECT \"table\", mode FROM sys.storage() WHERE \"schema\" = " + quoteLiteral(schema)
                + " AND \"table\" IN (" + tableList + ")";
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                String table = rs.getString(1);
                columnCounts.computeIfPresent(table, (key, count) -> count + 1);
                modesByTable.get(table).add(Objects.toString(rs.getObject(2), "<NULL>"));
            }
            Map<String, StorageObservation> observations = new LinkedHashMap<>();
            for (String table : tables) {
                observations.put(table,
                        new StorageObservation(columnCounts.get(table), String.join(",", modesByTable.get(table))));
            }
            return observations;
        } catch (SQLException e) {
            return Collections.emptyMap();
        }
    }

    private String definitions(List<MonetDBColumn> columns) {
        return columns.stream().map(c -> q(c.getName()) + " " + type(c.getType()) + " NULL")
                .collect(Collectors.joining(", "));
    }

    private String names(List<MonetDBColumn> columns) {
        return columns.stream().map(c -> q(c.getName())).collect(Collectors.joining(", "));
    }

    private String jsonPackedRelation(Vp vp) {
        String left = jsonPackedSide(vp.left, vp.leftColumns, vp.columns, "l");
        String right = jsonPackedSide(vp.right, vp.rightColumns, vp.columns, "r");
        return left + " JOIN " + right + " ON l." + q("vp_json_key") + " = r." + q("vp_json_key");
    }

    private String jsonPackedSide(String table, List<MonetDBColumn> presentColumns, List<MonetDBColumn> allColumns,
            String alias) {
        List<String> selectedColumns = new ArrayList<>();
        List<String> groupedColumns = new ArrayList<>();
        for (MonetDBColumn column : allColumns) {
            String name = q(column.getName());
            if (presentColumns.contains(column)) {
                selectedColumns.add(name);
                groupedColumns.add(name);
            } else {
                selectedColumns.add("CAST(NULL AS " + type(column.getType()) + ") AS " + name);
            }
        }
        return "(SELECT " + q(ROW_ID) + ", " + String.join(", ", selectedColumns) + ", json.tojsonarray(CAST("
                + q(ROW_ID) + " AS VARCHAR(64))) AS " + q("vp_json_key") + " FROM " + q(table) + " GROUP BY "
                + q(ROW_ID) + (groupedColumns.isEmpty() ? "" : ", " + String.join(", ", groupedColumns)) + ") " + alias;
    }

    private String type(MonetDBDataType type) {
        switch (type) {
        case INT:
            return "INTEGER";
        case BIGINT:
            return "BIGINT";
        case HUGEINT:
            return "HUGEINT";
        case DECIMAL:
            return "DECIMAL(38, 18)";
        case DOUBLE:
            return "DOUBLE";
        case VARCHAR:
            return "VARCHAR(16384)";
        case BOOLEAN:
            return "BOOLEAN";
        case DATE:
            return "DATE";
        case TIME:
            return "TIME";
        case TIMETZ:
            return "TIMETZ";
        case TIMESTAMP:
            return "TIMESTAMP";
        case TIMESTAMPTZ:
            return "TIMESTAMPTZ";
        case INTERVAL:
            return "INTERVAL DAY TO SECOND";
        case UUID:
            return "UUID";
        case INET:
            return "INET";
        case URL:
            return "URL";
        case BLOB:
            return "BLOB";
        default:
            throw new AssertionError(type);
        }
    }

    private long scalarLong(String sql) throws SQLException {
        try (Statement statement = state.getConnection().createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            if (!rs.next()) {
                throw new AssertionError(sql);
            }
            return rs.getLong(1);
        }
    }

    private void cleanup(Vp vp) throws SQLException {
        SQLException failure = null;
        try {
            cleanupPairObjects(vp);
        } catch (SQLException e) {
            failure = e;
        }
        try {
            drop(vp);
        } catch (SQLException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void cleanupPairObjects(Vp vp) throws SQLException {
        SQLException failure = null;
        List<String> names = new ArrayList<>(vp.transientObjects);
        for (String name : names) {
            try {
                dropRelation(name);
                vp.transientObjects.remove(name);
            } catch (SQLException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void drop(Vp vp) throws SQLException {
        SQLException failure = null;
        for (String name : new String[] { vp.view, vp.temp, vp.right, vp.left, vp.source }) {
            try {
                dropRelation(name);
            } catch (SQLException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** Drops either kind of relation; stale failures can leave a view where a table is expected, or vice versa. */
    private void dropRelation(String name) throws SQLException {
        SQLException first = null;
        boolean dropped = false;
        for (String kind : new String[] { "VIEW", "TABLE" }) {
            try {
                execute("DROP " + kind + " IF EXISTS " + q(name) + " CASCADE");
                dropped = true;
            } catch (SQLException e) {
                // Once MonetDB closes the connection (for example after a
                // server-side resource failure), retrying every object only
                // creates a large cascade of identical cleanup exceptions.
                if (e instanceof java.sql.SQLNonTransientConnectionException
                        || e.getCause() instanceof java.sql.SQLNonTransientConnectionException) {
                    throw e;
                }
                if (first == null) {
                    first = e;
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        if (!dropped && first != null) {
            throw first;
        }
    }

    private void execute(String sql) throws SQLException {
        // These statements bypass GlobalState.executeStatement(), therefore
        // they would otherwise be absent from databaseN.log. Record the
        // exact DDL/DML before execution so a later failure includes source,
        // left/right partitions, temporary tables/views and cleanup actions.
        state.getState().logStatement(sql);
        if (state.getOptions().logEachSelect()) {
            state.getLogger().writeCurrent("-- VP EXECUTE: " + sql);
        }
        try (Statement statement = state.getConnection().createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new SQLException("Failed statement: " + sql, e);
        }
    }

    private void executeSetup(Vp vp, String sql) throws SQLException {
        execute(sql);
        vp.setupStatements.add(sql);
    }

    private <T> void shuffleWithSeed(List<T> values) {
        for (int i = values.size() - 1; i > 0; i--) {
            int j = state.getRandomly().getInteger(0, i + 1);
            Collections.swap(values, i, j);
        }
    }

    private static String q(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
