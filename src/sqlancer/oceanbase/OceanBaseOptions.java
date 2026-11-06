package sqlancer.oceanbase;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "OceanBase (default port: " + OceanBaseOptions.DEFAULT_PORT
        + ", default host: " + OceanBaseOptions.DEFAULT_HOST + ")")
public class OceanBaseOptions implements DBMSSpecificOptions<OceanBaseOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 2881;

    @Parameter(names = "--oracle")
    public List<OceanBaseOracleFactory> oracles = Arrays.asList(OceanBaseOracleFactory.TLP_WHERE);

    @Parameter(names = "--ecst-query-pairs", description = "Number of comparable ECST query pairs per oracle invocation")
    public int ecstQueryPairs = 4;

    @Parameter(names = { "--query-timeout" }, description = "Query timeout")
    public int queryTimeout = 1000000000;
    @Parameter(names = { "--transaction-timeout" }, description = "Transaction timeout")
    public int trxTimeout = 1000000000;

    @Parameter(names = { "--vp-query-variants-per-combination",
            "--vertical-partitioning-query-variants-per-combination" }, description = "Number of random outer query variants generated for each vertical partitioning split/reconstruction combination")
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

    @Parameter(names = "--vp-pushdown-mode", description = "Controls OceanBase aggregation pushdown during vertical partitioning checks: neutralize disables it when supported, observe keeps the natural optimizer behavior")
    public String verticalPartitioningPushdownMode = "neutralize";

    @Parameter(names = "--vp-ob-storage-profile", description = "OceanBase storage profile for vertical partitioning checks: default keeps the original path, memtable-sstable reruns the same generated source/split query pairs after major freeze and compaction")
    public String verticalPartitioningOceanBaseStorageProfile = "default";

    @Parameter(names = "--vp-ob-partition-profile", description = "OceanBase physical partitioning profile for vertical partitioning helper tables: none keeps normal helper tables, hash partitions source/left/right by vp_rowid")
    public String verticalPartitioningOceanBasePartitionProfile = "none";

    @Parameter(names = "--vp-ob-partition-count", description = "Number of HASH partitions used for OceanBase VP helper tables when --vp-ob-partition-profile=hash")
    public int verticalPartitioningOceanBasePartitionCount = 8;

    @Parameter(names = "--vp-ob-parallel-degree", description = "If greater than 0, add the same PARALLEL(n) hint to both OceanBase VP source and split queries")
    public int verticalPartitioningOceanBaseParallelDegree;

    @Parameter(names = "--vp-ob-column-store-profile", description = "OceanBase column group profile for vertical partitioning helper tables: none, all, each, or all-each")
    public String verticalPartitioningOceanBaseColumnStoreProfile = "none";

    @Parameter(names = "--vp-ob-sql-semantics-profile", description = "OceanBase MySQL-compatible session semantics profile for VP checks: default resets to the normal clean session, fuzz samples sql_mode/charset/collation profiles")
    public String verticalPartitioningOceanBaseSqlSemanticsProfile = "default";

    @Parameter(names = { "--vertical-partitioning-max-source-rows",
            "--vp-max-source-rows" }, description = "Maximum number of rows copied from the base table into a vertical partitioning source table; 0 disables the limit")
    public int verticalPartitioningMaxSourceRows = 256;

    @Override
    public List<OceanBaseOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
