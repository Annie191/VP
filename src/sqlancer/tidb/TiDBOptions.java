package sqlancer.tidb;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "TiDB (default port: " + TiDBOptions.DEFAULT_PORT
        + ", default host: " + TiDBOptions.DEFAULT_HOST + ")")
public class TiDBOptions implements DBMSSpecificOptions<TiDBOracleFactory> {
    public enum StorageConfig {
        TIKV_ONLY, TIFLASH_ONLY, MIXED
    }

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 4000;

    @Parameter(names = { "--max-num-tables" }, description = "The maximum number of tables/views that can be created")
    public int maxNumTables = 10;

    @Parameter(names = { "--max-num-indexes" }, description = "The maximum number of indexes that can be created")
    public int maxNumIndexes = 20;

    @Parameter(names = "--oracle")
    public List<TiDBOracleFactory> oracle = Arrays.asList(TiDBOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--enable-non-prepared-plan-cache")
    public boolean nonPreparePlanCache;

    @Parameter(names = { "--tiflash" }, description = "Enable TiFlash")
    public boolean tiflash;

    @Parameter(names = { "--storage-config" }, description = "Storage mode used by the mixed storage oracle")
    public StorageConfig storageConfig = StorageConfig.MIXED;

    @Parameter(names = {
            "--vertical-partitioning-query-pairs" }, description = "The number of vertical partitioning rewrite pairs checked per materialized table")
    public int verticalPartitioningQueryPairs = 20;

    @Parameter(names = "--ecst-query-pairs", description = "The number of controlled ECST query pairs checked per oracle invocation")
    public int ecstQueryPairs = 4;

    @Parameter(names = "--ecst-max-candidates", description = "Legacy compatibility option; TiDB ECST now retains the full applicable candidate set")
    public int ecstMaxCandidates = 16;


    @Parameter(names = { "--vp-query-variants-per-combination",
            "--vertical-partitioning-query-variants-per-combination" }, description = "Number of random query variants generated for every vertical partitioning split/reconstruction combination")
    public int verticalPartitioningQueryVariantsPerCombination = 5;

    @Parameter(names = "--vp-helper-use-base-engine", description = "Try to create vertical partitioning helper tables with the selected base table's storage engine, falling each helper table back to InnoDB on failure", arity = 1)
    public boolean verticalPartitioningHelperUseBaseEngine;

    @Parameter(names = "--vp-source-copy-schema-objects", description = "Try to copy indexes and table constraints from the selected base table to the vertical partitioning source table; failures are skipped", arity = 1)
    public boolean verticalPartitioningSourceCopySchemaObjects;

    @Parameter(names = "--vp-allow-row-deletes", description = "Allow DELETE and TRUNCATE during the random mutation phase when the vertical partitioning oracle is enabled", arity = 1)
    public boolean verticalPartitioningAllowRowDeletes;

    @Parameter(names = "--vp-suppress-known-patterns", description = "In vertical partitioning exploration mode, log known high-frequency mismatches separately and continue with later query pairs", arity = 1)
    public boolean verticalPartitioningSuppressKnownPatterns;

    @Parameter(names = "--vp-enable-advanced-query-features", description = "Enable first-batch advanced vertical partitioning query features such as window expressions, UNION ALL, diversified aggregates, and helper indexes", arity = 1)
    public boolean verticalPartitioningAdvancedQueryFeatures;

    @Parameter(names = "--vp-enable-deep-exploration", description = "Enable stronger vertical partitioning exploration features; this includes advanced query features and additionally enables collation variants and boundary-value mutations", arity = 1)
    public boolean verticalPartitioningDeepExploration;

    @Parameter(names = {
            "--vertical-partitioning-max-source-rows" }, description = "The maximum number of rows copied into a vertical partitioning source table; 0 means no limit")
    public int verticalPartitioningMaxSourceRows = 256;

    @Override
    public List<TiDBOracleFactory> getTestOracleFactory() {
        return oracle;
    }
}
