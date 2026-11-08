package sqlancer.duckdb;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "DuckDB")
public class DuckDBOptions implements DBMSSpecificOptions<DuckDBOracleFactory> {

    @Parameter(names = "--ecst-query-pairs", description = "Comparable DuckDB ECST pairs per invocation")
    public int ecstQueryPairs = 4;

    @Parameter(names = "--test-collate", arity = 1)
    public boolean testCollate = true;

    @Parameter(names = "--test-check", description = "Allow generating CHECK constraints in tables", arity = 1)
    public boolean testCheckConstraints = true;

    @Parameter(names = "--test-default-values", description = "Allow generating DEFAULT values in tables", arity = 1)
    public boolean testDefaultValues = true;

    @Parameter(names = "--test-not-null", description = "Allow generating NOT NULL constraints in tables", arity = 1)
    public boolean testNotNullConstraints = true;

    @Parameter(names = "--test-functions", description = "Allow generating functions in expressions", arity = 1)
    public boolean testFunctions = true;

    @Parameter(names = "--test-casts", description = "Allow generating casts in expressions", arity = 1)
    public boolean testCasts = true;

    @Parameter(names = "--test-between", description = "Allow generating the BETWEEN operator in expressions", arity = 1)
    public boolean testBetween = true;

    @Parameter(names = "--test-in", description = "Allow generating the IN operator in expressions", arity = 1)
    public boolean testIn = true;

    @Parameter(names = "--test-case", description = "Allow generating the CASE operator in expressions", arity = 1)
    public boolean testCase = true;

    @Parameter(names = "--test-binary-logicals", description = "Allow generating AND and OR in expressions", arity = 1)
    public boolean testBinaryLogicals = true;

    @Parameter(names = "--test-int-constants", description = "Allow generating INTEGER constants", arity = 1)
    public boolean testIntConstants = true;

    @Parameter(names = "--test-varchar-constants", description = "Allow generating VARCHAR constants", arity = 1)
    public boolean testStringConstants = true;

    @Parameter(names = "--test-date-constants", description = "Allow generating DATE constants", arity = 1)
    public boolean testDateConstants = true;

    @Parameter(names = "--test-timestamp-constants", description = "Allow generating TIMESTAMP constants", arity = 1)
    public boolean testTimestampConstants = true;

    @Parameter(names = "--test-float-constants", description = "Allow generating floating-point constants", arity = 1)
    public boolean testFloatConstants = true;

    @Parameter(names = "--test-boolean-constants", description = "Allow generating boolean constants", arity = 1)
    public boolean testBooleanConstants = true;

    @Parameter(names = "--test-binary-comparisons", description = "Allow generating binary comparison operators (e.g., >= or LIKE)", arity = 1)
    public boolean testBinaryComparisons = true;

    @Parameter(names = "--test-indexes", description = "Allow explicit (i.e. CREATE INDEX) and implicit (i.e., UNIQUE and PRIMARY KEY) indexes", arity = 1)
    public boolean testIndexes = true;

    @Parameter(names = "--test-rowid", description = "Test tables' rowid columns", arity = 1)
    public boolean testRowid = true;

    @Parameter(names = "--max-num-views", description = "The maximum number of views that can be generated for a database", arity = 1)
    public int maxNumViews = 1;

    @Parameter(names = "--max-num-deletes", description = "The maximum number of DELETE statements that are issued for a database", arity = 1)
    public int maxNumDeletes = 1;

    @Parameter(names = "--max-num-updates", description = "The maximum number of UPDATE statements that are issued for a database", arity = 1)
    public int maxNumUpdates = 5;

    @Parameter(names = {
            "--vertical-partitioning-query-pairs" }, description = "The number of vertical partitioning rewrite pairs checked per materialized table")
    public int verticalPartitioningQueryPairs = 20;

    @Parameter(names = { "--vp-query-variants-per-combination",
            "--vertical-partitioning-query-variants-per-combination" }, description = "Number of random query variants generated for every vertical partitioning split/reconstruction combination")
    public int verticalPartitioningQueryVariantsPerCombination = 5;

    @Parameter(names = "--vp-helper-use-base-engine", description = "Accepted for compatibility; DuckDB has no storage-engine choice for vertical partitioning helper tables", arity = 1)
    public boolean verticalPartitioningHelperUseBaseEngine;

    @Parameter(names = "--vp-source-copy-schema-objects", description = "Try to copy indexes and table constraints from the selected base table to the vertical partitioning source table; failures are skipped", arity = 1)
    public boolean verticalPartitioningSourceCopySchemaObjects;

    @Parameter(names = "--vp-schema-fidelity-mode", description = "VP schema fidelity: drop (plain partition metadata), strict (copy source indexes/constraints), or mixed (randomly alternate)", arity = 1)
    public String verticalPartitioningSchemaFidelityMode = "drop";

    @Parameter(names = "--vp-allow-row-deletes", description = "Allow DELETE during the random mutation phase when the vertical partitioning oracle is enabled", arity = 1)
    public boolean verticalPartitioningAllowRowDeletes;

    @Parameter(names = "--vp-suppress-known-patterns", description = "In vertical partitioning exploration mode, log known high-frequency mismatches separately and continue with later query pairs", arity = 1)
    public boolean verticalPartitioningSuppressKnownPatterns;

    @Parameter(names = "--vp-enable-advanced-query-features", description = "Enable advanced vertical partitioning query features", arity = 1)
    public boolean verticalPartitioningAdvancedQueryFeatures;

    @Parameter(names = "--vp-enable-risky-predicates", description = "Enable targeted BETWEEN/IN/NULL/cast/date predicate profiles in VP queries", arity = 1)
    public boolean verticalPartitioningRiskyPredicates = true;

    @Parameter(names = "--vp-enable-deep-exploration", description = "Enable stronger DuckDB vertical partitioning exploration features; this includes advanced query features and boundary-value/collation exploration", arity = 1)
    public boolean verticalPartitioningDeepExploration;

    @Parameter(names = "--vp-enable-parquet-roundtrip", description = "Independently compare VP source results with COPY TO Parquet and read_parquet results", arity = 1)
    public boolean verticalPartitioningParquetRoundTrip;

    @Parameter(names = "--vp-enable-storage-boundary-tests", description = "Exercise DuckDB checkpoint, compression, vector-size and row-group boundary paths", arity = 1)
    public boolean verticalPartitioningStorageBoundaryTests;

    @Parameter(names = "--vp-storage-boundary-profile", description = "Storage boundary profile: vector, rowgroup, memory, or all")
    public String verticalPartitioningStorageBoundaryProfile = "all";

    @Parameter(names = "--vp-optimizer-modes", description = "Split execution optimizer modes, comma-separated: default,disabled,bare_filter,bare_join, or all")
    public String verticalPartitioningOptimizerModes = "all";

    @Parameter(names = "--vp-split-threads", description = "DuckDB worker threads used by the split side of VP queries; keep at 1 for stable concurrent fuzzing")
    public int verticalPartitioningSplitThreads = 1;

    @Parameter(names = {
            "--vertical-partitioning-max-source-rows" }, description = "The maximum number of rows copied into a vertical partitioning source table; 0 means no limit")
    public int verticalPartitioningMaxSourceRows = 4096;

    @Parameter(names = "--vp-enable-directed-data", description = "Create a DuckDB-specific VP base table with vector/row-group boundary data", arity = 1)
    public boolean verticalPartitioningDirectedData;

    @Parameter(names = "--vp-directed-source-rows", description = "Rows generated for the DuckDB-specific VP base table")
    public int verticalPartitioningDirectedSourceRows = 4096;

    @Parameter(names = "--oracle")
    public List<DuckDBOracleFactory> oracles = Arrays.asList(DuckDBOracleFactory.QUERY_PARTITIONING);

    @Override
    public List<DuckDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
