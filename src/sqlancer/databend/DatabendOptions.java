package sqlancer.databend;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(commandDescription = "Databend")
public class DatabendOptions implements DBMSSpecificOptions<DatabendOracleFactory> {
    @Parameter(names = "--ecst-query-pairs", description = "Comparable Databend ECST pairs per invocation")
    public int ecstQueryPairs = 4;

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3307;

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

    @Parameter(names = "--oracle")
    public List<DatabendOracleFactory> oracles = Arrays.asList(DatabendOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--vp-query-variants-per-combination", arity = 1)
    public int verticalPartitioningQueryVariantsPerCombination = 5;
    @Parameter(names = "--vp-helper-use-base-engine", arity = 1)
    public boolean verticalPartitioningHelperUseBaseEngine = true;
    @Parameter(names = "--vp-mixed-helper-engines", description = "Generate VP helpers with a FUSE/MEMORY engine mix when supported", arity = 1)
    public boolean verticalPartitioningMixedHelperEngines = false;
    @Parameter(names = "--vp-engine-layout-profile", arity = 1)
    public String verticalPartitioningEngineLayoutProfile = "FUSE_FOCUSED";
    @Parameter(names = "--vp-source-copy-schema-objects", arity = 1)
    public boolean verticalPartitioningSourceCopySchemaObjects = true;
    @Parameter(names = "--vp-schema-profile", description = "VP source schema profile: PROJECTION_ONLY, NULL_DEFAULT, or CONSTRAINTS", arity = 1)
    public String verticalPartitioningSchemaProfile = "NULL_DEFAULT";
    @Parameter(names = "--vp-allow-row-deletes", arity = 1)
    public boolean verticalPartitioningAllowRowDeletes = false;
    @Parameter(names = "--vp-suppress-known-patterns", arity = 1)
    public boolean verticalPartitioningSuppressKnownPatterns = true;
    @Parameter(names = "--vp-enable-advanced-query-features", arity = 1)
    public boolean verticalPartitioningAdvancedQueryFeatures = false;
    @Parameter(names = "--vp-enable-deep-exploration", arity = 1, hidden = true)
    public boolean verticalPartitioningDeepExploration = false;
    @Parameter(names = "--vp-max-source-rows", arity = 1, hidden = true)
    public int verticalPartitioningMaxSourceRows = 0;

    @Override
    public List<DatabendOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
