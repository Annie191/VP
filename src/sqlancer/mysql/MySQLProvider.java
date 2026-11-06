package sqlancer.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.TableIndex;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.mysql.MySQLMultiEngineMetadata.LogicalTableGroup;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLDataType;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.gen.MySQLExpressionGenerator;
import sqlancer.mysql.gen.MySQLAlterTable;
import sqlancer.mysql.gen.MySQLDeleteGenerator;
import sqlancer.mysql.gen.MySQLDropIndex;
import sqlancer.mysql.gen.MySQLInsertGenerator;
import sqlancer.mysql.gen.MySQLSetGenerator;
import sqlancer.mysql.gen.MySQLTableGenerator;
import sqlancer.mysql.oracle.MySQLVerticalPartitioningOracle;
import sqlancer.mysql.oracle.MySQLVerticalPartitioningOracle.AdvancedFeature;
import sqlancer.mysql.gen.MySQLTruncateTableGenerator;
import sqlancer.mysql.gen.MySQLUpdateGenerator;
import sqlancer.mysql.gen.MySQLViewGenerator;
import sqlancer.mysql.gen.admin.MySQLFlush;
import sqlancer.mysql.gen.admin.MySQLReset;
import sqlancer.mysql.gen.datadef.MySQLIndexGenerator;
import sqlancer.mysql.gen.tblmaintenance.MySQLAnalyzeTable;
import sqlancer.mysql.gen.tblmaintenance.MySQLCheckTable;
import sqlancer.mysql.gen.tblmaintenance.MySQLChecksum;
import sqlancer.mysql.gen.tblmaintenance.MySQLOptimize;
import sqlancer.mysql.gen.tblmaintenance.MySQLRepair;

@AutoService(DatabaseProvider.class)
public class MySQLProvider extends SQLProviderAdapter<MySQLGlobalState, MySQLOptions> {

    private static final int MULTI_ENGINE_MIN_ROWS_PER_TABLE = 192;
    private static final int VP_BASE_TABLES_PER_BATCH = 2;
    private static final int VP_REPLACEMENT_RETRY_COUNT = 10;
    private static final int VP_SINGLE_COLUMN_PERCENT = 40;
    private static final int VP_EMPTY_TABLE_PERCENT = 10;
    private static final int VP_INITIAL_INSERT_ATTEMPTS = 12;
    private static final int VP_INITIAL_MIN_ROWS = 8;

    public MySQLProvider() {
        super(MySQLGlobalState.class, MySQLOptions.class);
    }

    enum Action implements AbstractAction<MySQLGlobalState> {
        SHOW_TABLES((g) -> new SQLQueryAdapter("SHOW TABLES")), //
        INSERT(MySQLInsertGenerator::insertRow), //
        SET_VARIABLE(MySQLSetGenerator::set), //
        REPAIR(MySQLRepair::repair), //
        OPTIMIZE(MySQLOptimize::optimize), //
        CHECKSUM(MySQLChecksum::checksum), //
        CHECK_TABLE(MySQLCheckTable::check), //
        ANALYZE_TABLE(MySQLAnalyzeTable::analyze), //
        FLUSH(MySQLFlush::create), RESET(MySQLReset::create), CREATE_INDEX(MySQLIndexGenerator::create), //
        ALTER_TABLE(MySQLAlterTable::create), //
        TRUNCATE_TABLE(MySQLTruncateTableGenerator::generate), //
        VIEW_GENERATOR(MySQLViewGenerator::getQuery), //
        SELECT_INFO((g) -> new SQLQueryAdapter(
                "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '" + g.getDatabaseName()
                        + "'")), //
        UPDATE(MySQLUpdateGenerator::create), //
        DELETE(MySQLDeleteGenerator::delete), //
        DROP_INDEX(MySQLDropIndex::generate);

        private final SQLQueryProvider<MySQLGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<MySQLGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(MySQLGlobalState globalState) throws Exception {
            return sqlQueryProvider.getQuery(globalState);
        }
    }

    private static int mapBatchActions(MySQLGlobalState globalState, Action a) {
        return mapActions(globalState, a, false, true);
    }

    private static int mapActions(MySQLGlobalState globalState, Action a, boolean allowRowDeletes,
            boolean allowViewGenerator) {
        Randomly r = globalState.getRandomly();
        boolean preserveVerticalPartitioningRows = usesVerticalPartitioningOracle(globalState.getDbmsSpecificOptions())
                && !globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes && !allowRowDeletes;
        int nrPerformed = 0;
        switch (a) {
        case DROP_INDEX:
            nrPerformed = r.getInteger(0, 2);
            break;
        case SHOW_TABLES:
            nrPerformed = r.getInteger(0, 1);
            break;
        case INSERT:
            nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            break;
        case REPAIR:
            nrPerformed = r.getInteger(0, 1);
            break;
        case SET_VARIABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case CREATE_INDEX:
            nrPerformed = r.getInteger(0, 5);
            break;
        case FLUSH:
            nrPerformed = Randomly.getBooleanWithSmallProbability() ? r.getInteger(0, 1) : 0;
            break;
        case OPTIMIZE:
            // seems to yield low CPU utilization
            nrPerformed = Randomly.getBooleanWithSmallProbability() ? r.getInteger(0, 1) : 0;
            break;
        case RESET:
            // affects the global state, so do not execute
            nrPerformed = globalState.getOptions().getNumberConcurrentThreads() == 1 ? r.getInteger(0, 1) : 0;
            break;
        case CHECKSUM:
        case CHECK_TABLE:
        case ANALYZE_TABLE:
            nrPerformed = r.getInteger(0, 2);
            break;
        case ALTER_TABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case TRUNCATE_TABLE:
            nrPerformed = preserveVerticalPartitioningRows ? 0 : r.getInteger(0, 2);
            break;
        case VIEW_GENERATOR:
            nrPerformed = allowViewGenerator ? r.getInteger(0, 2) : 0;
            break;
        case SELECT_INFO:
            nrPerformed = r.getInteger(0, 10);
            break;
        case UPDATE:
            nrPerformed = r.getInteger(0, 10);
            break;
        case DELETE:
            nrPerformed = preserveVerticalPartitioningRows ? 0 : r.getInteger(0, 10);
            break;
        default:
            throw new AssertionError(a);
        }
        return nrPerformed;
    }

    @Override
    public void generateDatabase(MySQLGlobalState globalState) throws Exception {
        if (usesMultiEngineOracle(globalState)) {
            generateMultiEngineDatabase(globalState);
            return;
        }

        boolean usesVerticalPartitioningOracle = usesVerticalPartitioningOracle(globalState);
        if (usesVerticalPartitioningOracle) {
            createVerticalPartitioningTableBatch(globalState);
        } else {
            long initialTables = Randomly.getNotCachedInteger(1, 2);
            while (globalState.getSchema().getDatabaseTables().size() < initialTables) {
                String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                SQLQueryAdapter createTable = generateInitialTable(globalState, tableName);
                globalState.executeStatement(createTable);
            }
        }

        if (!usesVerticalPartitioningOracle) {
            executeRandomActions(globalState);
        }

        if (usesVerticalPartitioningOracle) {
            MySQLVerticalPartitioningOracle.logSchemaShapeStats(globalState, "post-generate");
        }

        if (globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch((o) -> o == MySQLOracleFactory.CERT)) {
            // Enfore statistic collected for all tables
            ExpectedErrors errors = new ExpectedErrors();
            MySQLErrors.addExpressionErrors(errors);
            for (MySQLTable table : globalState.getSchema().getDatabaseTables()) {
                StringBuilder sb = new StringBuilder();
                sb.append("ANALYZE TABLE ");
                sb.append(table.getName());
                sb.append(" UPDATE HISTOGRAM ON ");
                String columns = table.getColumns().stream().map(MySQLColumn::getName)
                        .collect(Collectors.joining(", "));
                sb.append(columns + ";");
                globalState.executeStatement(new SQLQueryAdapter(sb.toString(), errors));
            }
        }
    }

    private boolean usesMultiEngineOracle(MySQLGlobalState globalState) {
        return globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch(o -> o == MySQLOracleFactory.MULTI_ENGINE);
    }

    private boolean usesVerticalPartitioningOracle(MySQLGlobalState globalState) {
        return usesVerticalPartitioningOracle(globalState.getDbmsSpecificOptions());
    }

    private static boolean usesVerticalPartitioningOracle(MySQLOptions options) {
        return options.getTestOracleFactory().stream()
                .anyMatch(o -> o == MySQLOracleFactory.VERTICAL_PARTITIONING || o == MySQLOracleFactory.ECST);
    }

    public static void replaceVerticalPartitioningTableBatch(MySQLGlobalState globalState, List<String> oldTableNames)
            throws Exception {
        replaceVerticalPartitioningTables(globalState, oldTableNames, VP_BASE_TABLES_PER_BATCH);
    }

    public static void replaceVerticalPartitioningTables(MySQLGlobalState globalState, List<String> oldTableNames,
            int replacementCount) throws Exception {
        for (String tableName : oldTableNames) {
            globalState
                    .executeStatement(new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(tableName), true));
        }
        globalState.updateSchema();
        createVerticalPartitioningTables(globalState, replacementCount);
        globalState.updateSchema();
    }

    public static void replaceVerticalPartitioningTableAfterSuccessfulCheck(MySQLGlobalState globalState,
            String oldTableName) throws Exception {
        MySQLTable replacement = createValidVerticalPartitioningBaseTable(globalState);
        if (replacement == null) {
            logVerticalPartitioningReplacementFailure(globalState, oldTableName);
            throw new IgnoreMeException();
        }
        globalState
                .executeStatement(new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(oldTableName), true));
        globalState.updateSchema();
    }

    public static void ensureVerticalPartitioningBaseTables(MySQLGlobalState globalState, int minBaseTables)
            throws Exception {
        globalState.updateSchema();
        while (countVerticalPartitioningBaseTables(globalState) < minBaseTables) {
            MySQLTable table = createValidVerticalPartitioningBaseTable(globalState);
            if (table == null) {
                throw new IgnoreMeException();
            }
            globalState.updateSchema();
        }
    }

    private static void createVerticalPartitioningTableBatch(MySQLGlobalState globalState) throws Exception {
        createVerticalPartitioningTables(globalState, VP_BASE_TABLES_PER_BATCH);
    }

    private static void createVerticalPartitioningTables(MySQLGlobalState globalState, int tableCount)
            throws Exception {
        for (int i = 0; i < tableCount; i++) {
            MySQLTable table = createValidVerticalPartitioningBaseTable(globalState);
            if (table == null) {
                throw new IgnoreMeException();
            }
            globalState.updateSchema();
        }
        globalState.updateSchema();
    }

    private static MySQLTable createValidVerticalPartitioningBaseTable(MySQLGlobalState globalState) throws Exception {
        for (int attempt = 0; attempt < VP_REPLACEMENT_RETRY_COUNT; attempt++) {
            String tableName = getFreeTableName(globalState);
            SQLQueryAdapter createTable = generateVerticalPartitioningTable(globalState, tableName);
            boolean success = globalState.executeStatement(createTable);
            globalState.updateSchema();
            if (!success) {
                continue;
            }
            MySQLTable table = findVerticalPartitioningBaseTable(globalState, tableName);
            if (table != null) {
                initializeVerticalPartitioningRows(globalState, table);
                mutateVerticalPartitioningCandidate(globalState, table);
                globalState.updateSchema();
                table = findVerticalPartitioningBaseTable(globalState, tableName);
                if (table != null && isValidVerticalPartitioningBaseTable(globalState, table)) {
                    return table;
                }
            }
            globalState
                    .executeStatement(new SQLQueryAdapter("DROP TABLE IF EXISTS " + quoteIdentifier(tableName), true));
            globalState.updateSchema();
        }
        return null;
    }

    private static void mutateVerticalPartitioningCandidate(MySQLGlobalState globalState, MySQLTable table)
            throws Exception {
        int actions = Math.max(1, globalState.getOptions().getMaxNumberInserts())
                + globalState.getRandomly().getInteger(1, 11);
        for (int i = 0; i < actions; i++) {
            globalState.updateSchema();
            table = findVerticalPartitioningBaseTable(globalState, table.getName());
            if (table == null) {
                return;
            }
            try {
                globalState.executeStatement(createVerticalPartitioningCandidateMutation(globalState, table));
            } catch (IgnoreMeException ignored) {
            }
        }
    }

    private static boolean initializeVerticalPartitioningRows(MySQLGlobalState globalState, MySQLTable table)
            throws Exception {
        if (globalState.getRandomly().getInteger(0, 100) < VP_EMPTY_TABLE_PERCENT) {
            return true;
        }
        for (int attempt = 0; attempt < VP_INITIAL_INSERT_ATTEMPTS; attempt++) {
            if (countRows(globalState, table) >= VP_INITIAL_MIN_ROWS) {
                break;
            }
            try {
                globalState.executeStatement(MySQLInsertGenerator.insertRow(globalState, table));
            } catch (IgnoreMeException | SQLException ignored) {
                // Failed INSERTs still consume one bounded attempt.
            }
        }
        return countRows(globalState, table) == 0;
    }

    private static boolean hasRows(MySQLGlobalState globalState, MySQLTable table) throws SQLException {
        return countRows(globalState, table) > 0;
    }

    private static long countRows(MySQLGlobalState globalState, MySQLTable table) throws SQLException {
        try (Statement statement = globalState.getConnection().createStatement();
                ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM " + quoteIdentifier(table.getName()))) {
            return result.next() ? result.getLong(1) : 0;
        }
    }

    private static SQLQueryAdapter createVerticalPartitioningCandidateMutation(MySQLGlobalState globalState,
            MySQLTable table) throws Exception {
        List<Function<MySQLTable, SQLQueryAdapter>> actions = new java.util.ArrayList<>();
        // Deliberately empty VP tables must stay empty during subsequent mutations.
        if (hasRows(globalState, table)) {
            actions.add(t -> {
                try {
                    return MySQLInsertGenerator.insertRow(globalState, t);
                } catch (SQLException e) {
                    throw new AssertionError(e);
                }
            });
        }
        actions.add(t -> createCandidateUpdate(globalState, t));
        actions.add(t -> createCandidateIndex(globalState, t));
        actions.add(t -> createCandidateAnalyze(globalState, t));
        actions.add(t -> createCandidateCheck(t));
        actions.add(t -> createCandidateChecksum(t));
        actions.add(t -> createCandidateRepair(t));
        actions.add(t -> createCandidateOptimize(t));
        actions.add(t -> createCandidateAlter(t));
        actions.add(t -> MySQLSetGenerator.set(globalState));
        actions.add(t -> new SQLQueryAdapter(
                "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '"
                        + globalState.getDatabaseName() + "' and TABLE_NAME = '" + t.getName() + "'"));
        actions.add(t -> createCandidateDropIndex(t));
        if (globalState.getDbmsSpecificOptions().verticalPartitioningAllowRowDeletes) {
            actions.add(t -> createCandidateDelete(globalState, t));
            actions.add(t -> new SQLQueryAdapter("TRUNCATE TABLE " + quoteIdentifier(t.getName()),
                    ExpectedErrors.from("doesn't have this option")));
        }
        return Randomly.fromList(actions).apply(table);
    }

    private static SQLQueryAdapter createCandidateUpdate(MySQLGlobalState globalState, MySQLTable table) {
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addInsertUpdateErrors(errors);
        MySQLExpressionGenerator gen = new MySQLExpressionGenerator(globalState).setColumns(table.getColumns());
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(quoteIdentifier(table.getName()));
        sb.append(" SET ");
        List<MySQLColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(quoteIdentifier(columns.get(i).getName()));
            sb.append("=");
            if (shouldUseVpBoundaryLiteral(globalState)) {
                MySQLVerticalPartitioningOracle.recordAdvancedFeature(AdvancedFeature.BOUNDARY_MUTATION);
                sb.append(generateCandidateBoundaryLiteral(columns.get(i)));
            } else if (Randomly.getBoolean()) {
                sb.append(gen.generateConstant());
            } else if (Randomly.getBoolean()) {
                sb.append("DEFAULT");
            } else {
                MySQLErrors.addExpressionErrors(errors);
                sb.append(MySQLVisitor.asString(gen.generateExpression()));
            }
        }
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            MySQLErrors.addExpressionErrors(errors);
            sb.append(MySQLVisitor.asString(gen.generateExpression()));
        }
        errors.add("doesn't have this option");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static boolean shouldUseVpBoundaryLiteral(MySQLGlobalState globalState) {
        if (globalState.getDbmsSpecificOptions().verticalPartitioningDeepExploration) {
            return globalState.getRandomly().getInteger(0, 100) < 40;
        }
        return false;
    }

    private static String generateCandidateBoundaryLiteral(MySQLColumn column) {
        switch (column.getType()) {
        case INT:
            return Randomly.fromOptions("0", "-0", "-1", "1", "2147483647", "-2147483648", "9223372036854775807",
                    "-9223372036854775808", "18446744073709551615");
        case FLOAT:
        case DOUBLE:
            return Randomly.fromOptions("0", "-0.0", "1e308", "-1e308", "0.0000000000000001",
                    "0.000000000000000000000000000001");
        case DECIMAL:
            return Randomly.fromOptions("0", "-1", "99999999999999999999.9999999999",
                    "-99999999999999999999.9999999999", "0.0000000001", "0.000000000000000000000000000001");
        case VARCHAR:
            return Randomly.fromOptions("''", "' '", "'0'", "'0000-00-00'", "'NaN'", "'Infinity'", "'AaBbCc'",
                    "'zzzzzzzzzzzzzzzzzzzz'", "'  '", "'abc '", "'00'", "'-0'", "'1e309'", "'变'", "'é'", "'ß'",
                    "'1970-01-01'", "'2038-01-19'", "'9999-12-31'", "'2024-02-29'", "'00010101'");
        default:
            throw new AssertionError(column.getType());
        }
    }

    private static SQLQueryAdapter createCandidateIndex(MySQLGlobalState globalState, MySQLTable table) {
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);
        StringBuilder sb = new StringBuilder("CREATE ");
        if (Randomly.getBoolean()) {
            sb.append("UNIQUE ");
            errors.add("Duplicate entry");
        }
        sb.append("INDEX ");
        sb.append(globalState.getSchema().getFreeIndexName());
        sb.append(" ON ");
        sb.append(quoteIdentifier(table.getName()));
        sb.append("(");
        List<MySQLColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            MySQLColumn column = columns.get(i);
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(quoteIdentifier(column.getName()));
            if (Randomly.getBoolean() && column.getType() == MySQLDataType.VARCHAR) {
                sb.append("(");
                sb.append(globalState.getRandomly().getInteger(1, 5));
                sb.append(")");
            }
            if (Randomly.getBoolean()) {
                sb.append(" ");
                sb.append(Randomly.fromOptions("ASC", "DESC"));
            }
        }
        sb.append(")");
        errors.add("Specified key was too long");
        errors.add("Data truncation");
        errors.add("Duplicate key name");
        errors.add("used in key specification without a key length");
        errors.add("can't be used in key specification with the used table type");
        errors.add("The storage engine for the table doesn't support descending indexes");
        errors.add("Table handler doesn't support NULL in given index");
        errors.add("A primary key index cannot be invisible");
        errors.add("Functional index on a column is not supported. Consider using a regular index instead.");
        errors.add("Incorrect usage of spatial/fulltext/hash index and explicit index order");
        errors.add("must include all columns");
        errors.add("cannot index the expression");
        errors.add("Data truncation: Truncated incorrect");
        errors.add("a disallowed function.");
        errors.add("Cannot create a functional index on an expression that returns a BLOB or TEXT.");
        errors.add("out of range");
        errors.add("Data truncated for functional index");
        errors.add("Row size too large");
        errors.add("having prefix key part");
        errors.add("in the PARTITION BY KEY() clause is not supported");
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private static SQLQueryAdapter createCandidateDropIndex(MySQLTable table) {
        if (!table.hasIndexes()) {
            throw new IgnoreMeException();
        }
        TableIndex index = table.getRandomIndex();
        StringBuilder sb = new StringBuilder("DROP INDEX ");
        sb.append(index.getIndexName());
        sb.append(" ON ");
        sb.append(quoteIdentifier(table.getName()));
        return new SQLQueryAdapter(sb.toString(),
                ExpectedErrors.from("LOCK=NONE is not supported", "ALGORITHM=INPLACE is not supported",
                        "Data truncation", "Data truncated for functional index",
                        "A primary key index cannot be invisible"));
    }

    private static SQLQueryAdapter createCandidateDelete(MySQLGlobalState globalState, MySQLTable table) {
        ExpectedErrors errors = new ExpectedErrors();
        MySQLExpressionGenerator gen = new MySQLExpressionGenerator(globalState).setColumns(table.getColumns());
        StringBuilder sb = new StringBuilder("DELETE");
        if (Randomly.getBoolean()) {
            sb.append(" LOW_PRIORITY");
        }
        if (Randomly.getBoolean()) {
            sb.append(" QUICK");
        }
        if (Randomly.getBoolean()) {
            sb.append(" IGNORE");
        }
        sb.append(" FROM ");
        sb.append(quoteIdentifier(table.getName()));
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            MySQLErrors.addExpressionErrors(errors);
            sb.append(MySQLVisitor.asString(gen.generateExpression()));
        }
        errors.add("doesn't have this option");
        errors.add("Truncated incorrect");
        errors.add("Data truncated for functional index");
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    private static SQLQueryAdapter createCandidateAnalyze(MySQLGlobalState globalState, MySQLTable table) {
        StringBuilder sb = new StringBuilder("ANALYZE TABLE ");
        sb.append(quoteIdentifier(table.getName()));
        if (Randomly.getBoolean() && !table.getColumns().isEmpty()) {
            sb.append(" UPDATE HISTOGRAM ON ");
            sb.append(table.getRandomNonEmptyColumnSubset().stream().map(c -> quoteIdentifier(c.getName()))
                    .collect(Collectors.joining(", ")));
            if (Randomly.getBoolean()) {
                sb.append(" WITH ");
                sb.append(globalState.getRandomly().getInteger(1, 1024));
                sb.append(" BUCKETS");
            }
        }
        return new SQLQueryAdapter(sb.toString());
    }

    private static SQLQueryAdapter createCandidateCheck(MySQLTable table) {
        StringBuilder sb = new StringBuilder("CHECK TABLE ");
        sb.append(quoteIdentifier(table.getName()));
        List<String> options = Randomly.subset("FOR UPGRADE", "QUICK", "FAST", "MEDIUM", "EXTENDED", "CHANGED");
        if (!options.isEmpty()) {
            sb.append(" ");
            sb.append(options.stream().collect(Collectors.joining(" ")));
        }
        return new SQLQueryAdapter(sb.toString());
    }

    private static SQLQueryAdapter createCandidateChecksum(MySQLTable table) {
        StringBuilder sb = new StringBuilder("CHECKSUM TABLE ");
        sb.append(quoteIdentifier(table.getName()));
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("QUICK", "EXTENDED"));
        }
        return new SQLQueryAdapter(sb.toString());
    }

    private static SQLQueryAdapter createCandidateRepair(MySQLTable table) {
        StringBuilder sb = new StringBuilder("REPAIR TABLE ");
        sb.append(quoteIdentifier(table.getName()));
        if (Randomly.getBoolean()) {
            sb.append(" QUICK");
        }
        if (Randomly.getBoolean()) {
            sb.append(" EXTENDED");
        }
        if (Randomly.getBoolean()) {
            sb.append(" USE_FRM");
        }
        return new SQLQueryAdapter(sb.toString());
    }

    private static SQLQueryAdapter createCandidateOptimize(MySQLTable table) {
        StringBuilder sb = new StringBuilder("OPTIMIZE");
        if (Randomly.getBoolean()) {
            sb.append(" ");
            sb.append(Randomly.fromOptions("NO_WRITE_TO_BINLOG", "LOCAL"));
        }
        sb.append(" TABLE ");
        sb.append(quoteIdentifier(table.getName()));
        return new SQLQueryAdapter(sb.toString());
    }

    private static SQLQueryAdapter createCandidateAlter(MySQLTable table) {
        ExpectedErrors errors = ExpectedErrors.from("does not support the create option", "doesn't have this option",
                "is not supported for this operation", "Data truncation", "Specified key was too long");
        errors.add("Data truncated for functional index ");
        errors.add("Deadlock found when trying to get lock; try restarting transaction");
        errors.add("Lock wait timeout exceeded");
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        sb.append(quoteIdentifier(table.getName()));
        sb.append(" ");
        sb.append(Randomly.fromOptions("FORCE", Randomly.fromOptions("DISABLE", "ENABLE") + " KEYS",
                "CHECKSUM " + Randomly.fromOptions(0, 1), "DELAY_KEY_WRITE " + Randomly.fromOptions(0, 1),
                "ROW_FORMAT "
                        + Randomly.fromOptions("DEFAULT", "DYNAMIC", "FIXED", "COMPRESSED", "REDUNDANT", "COMPACT"),
                "STATS_AUTO_RECALC " + Randomly.fromOptions("0", "1", "DEFAULT"),
                "STATS_PERSISTENT " + Randomly.fromOptions("0", "1", "DEFAULT"),
                "PACK_KEYS " + Randomly.fromOptions("0", "1", "DEFAULT")));
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private static void logVerticalPartitioningReplacementFailure(MySQLGlobalState globalState, String oldTableName) {
        String message = "-- VP replacement failed after " + VP_REPLACEMENT_RETRY_COUNT
                + " attempts; keeping old base table " + oldTableName;
        try {
            globalState.getLogger().writeCurrent(message);
        } catch (UnsupportedOperationException ignored) {
            globalState.getState().logStatement(message);
        }
    }

    private static MySQLTable findVerticalPartitioningBaseTable(MySQLGlobalState globalState, String tableName) {
        return globalState.getSchema().getDatabaseTables().stream().filter(t -> t.getName().equals(tableName))
                .filter(t -> !t.isView()).filter(t -> !t.getName().startsWith("vp_"))
                .filter(t -> !t.getColumns().isEmpty()).findFirst().orElse(null);
    }

    private static boolean isValidVerticalPartitioningBaseTable(MySQLGlobalState globalState, MySQLTable table) {
        if (table.isView() || table.getName().startsWith("vp_") || table.getColumns().isEmpty()) {
            return false;
        }
        String columnList = table.getColumns().stream().map(c -> quoteIdentifier(c.getName()))
                .collect(Collectors.joining(", "));
        try (Statement s = globalState.getConnection().createStatement()) {
            try (ResultSet ignored = s.executeQuery("SELECT COUNT(*) FROM " + quoteIdentifier(table.getName()))) {
                while (ignored.next()) {
                }
            }
            try (ResultSet ignored = s
                    .executeQuery("SELECT " + columnList + " FROM " + quoteIdentifier(table.getName()) + " LIMIT 1")) {
                while (ignored.next()) {
                }
            }
            return true;
        } catch (SQLException ignored) {
            return false;
        }
    }

    private static void executeRandomActions(MySQLGlobalState globalState) throws Exception {
        StatementExecutor<MySQLGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                MySQLProvider::mapBatchActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    private static String getFreeTableName(MySQLGlobalState globalState) {
        for (int i = 0;; i++) {
            String tableName = DBMSCommon.createTableName(i);
            boolean exists = globalState.getSchema().getDatabaseTables().stream()
                    .anyMatch(t -> t.getName().equals(tableName));
            if (!exists) {
                return tableName;
            }
        }
    }

    private static long countVerticalPartitioningBaseTables(MySQLGlobalState globalState) {
        return globalState.getSchema().getDatabaseTables().stream().filter(t -> !t.isView())
                .filter(t -> !t.getName().startsWith("vp_")).filter(t -> !t.getColumns().isEmpty()).count();
    }

    private static String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private SQLQueryAdapter generateInitialTable(MySQLGlobalState globalState, String tableName) {
        if (!usesVerticalPartitioningOracle(globalState)) {
            return MySQLTableGenerator.generate(globalState, tableName);
        }
        return generateVerticalPartitioningTable(globalState, tableName);
    }

    private static SQLQueryAdapter generateVerticalPartitioningTable(MySQLGlobalState globalState, String tableName) {
        int shape = globalState.getRandomly().getInteger(0, 100);
        if (shape < VP_SINGLE_COLUMN_PERCENT) {
            return MySQLTableGenerator.generate(globalState, tableName, 1);
        }
        // Keep a substantial, explicit wide-table population (4 columns or more).
        int nrColumns = shape < VP_SINGLE_COLUMN_PERCENT + 30 ? globalState.getRandomly().getInteger(4, 9)
                : globalState.getRandomly().getInteger(2, 4);
        return MySQLTableGenerator.generate(globalState, tableName, nrColumns);
    }

    private void generateMultiEngineDatabase(MySQLGlobalState globalState) throws Exception {
        MySQLMultiEngineReplicaManager replicaManager = new MySQLMultiEngineReplicaManager(globalState);
        List<LogicalTableGroup> groups = replicaManager
                .createLogicalTableGroups(globalState.getDbmsSpecificOptions().multiEngineLogicalTables);
        populateMultiEngineLogicalTables(globalState, replicaManager, groups);
        replicaManager.analyzeReplicaTables(groups);
        globalState.updateSchema();
        ensureMultiEngineReplicasConsistent(replicaManager, groups);
    }

    private void populateMultiEngineLogicalTables(MySQLGlobalState globalState,
            MySQLMultiEngineReplicaManager replicaManager, List<LogicalTableGroup> groups) throws Exception {
        int maxRowsPerTable = Math.max(MULTI_ENGINE_MIN_ROWS_PER_TABLE,
                globalState.getDbmsSpecificOptions().multiEngineMaxRowsPerTable);
        for (LogicalTableGroup group : groups) {
            replicaManager.insertMirroredRows(group,
                    globalState.getRandomly().getInteger(MULTI_ENGINE_MIN_ROWS_PER_TABLE, maxRowsPerTable + 1));
        }
    }

    private void ensureMultiEngineReplicasConsistent(MySQLMultiEngineReplicaManager replicaManager,
            List<LogicalTableGroup> groups) throws Exception {
        replicaManager.ensureReplicasConsistent(groups);
    }

    @Override
    public SQLConnection createDatabase(MySQLGlobalState globalState) throws SQLException {
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = MySQLOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = MySQLOptions.DEFAULT_PORT;
        }
        String databaseName = globalState.getDatabaseName();
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        globalState.getState().logStatement("USE " + databaseName);
        String url = String.format("jdbc:mysql://%s:%d?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                host, port);
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + databaseName);
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "mysql";
    }

    @Override
    public boolean addRowsToAllTables(MySQLGlobalState globalState) throws Exception {
        List<MySQLTable> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (MySQLTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = MySQLInsertGenerator.insertRow(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }

}
