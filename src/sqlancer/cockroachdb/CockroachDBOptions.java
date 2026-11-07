package sqlancer.cockroachdb;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "CockroachDB (default port: " + CockroachDBOptions.DEFAULT_PORT
        + " default host: " + CockroachDBOptions.DEFAULT_HOST + ")")
public class CockroachDBOptions implements DBMSSpecificOptions<CockroachDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 26257;

    @Parameter(names = "--oracle")
    public CockroachDBOracleFactory oracle = CockroachDBOracleFactory.NOREC;

    @Parameter(names = "--ecst-query-pairs", description = "Number of comparable ECST pairs per invocation on a single CockroachDB node")
    public int ecstQueryPairs = 4;

    @Parameter(names = {
            "--test-hash-indexes" }, description = "Test the USING HASH WITH BUCKET_COUNT=n_buckets option in CREATE INDEX")
    public boolean testHashIndexes = true;

    @Parameter(names = { "--test-temp-tables" }, description = "Test TEMPORARY tables")
    public boolean testTempTables; // default: false https://github.com/cockroachdb/cockroach/issues/85388

    @Parameter(names = { "--max-num-tables" }, description = "The maximum number of tables that can be created")
    public int maxNumTables = 10;

    @Parameter(names = { "--max-num-indexes" }, description = "The maximum number of indexes that can be created")
    public int maxNumIndexes = 20;

    @Parameter(names = { "--vp-query-variants-per-combination",
            "--vertical-partitioning-query-variants-per-combination" }, description = "Number of random query variants generated for every vertical partitioning split/reconstruction combination")
    public int verticalPartitioningQueryVariantsPerCombination = 5;

    @Parameter(names = "--vp-helper-use-base-engine", description = "Accepted for vertical partitioning option compatibility; CockroachDB has no per-table storage engine, so this is a no-op", arity = 1)
    public boolean verticalPartitioningHelperUseBaseEngine;

    @Parameter(names = "--vp-source-copy-schema-objects", description = "Try to copy indexes and table constraints from the selected base table to the vertical partitioning source table; failures are skipped", arity = 1)
    public boolean verticalPartitioningSourceCopySchemaObjects;

    @Parameter(names = "--vp-allow-row-deletes", description = "Allow row-removing or table-removing actions during the random mutation phase when the vertical partitioning oracle is enabled", arity = 1)
    public boolean verticalPartitioningAllowRowDeletes;

    @Parameter(names = "--vp-suppress-known-patterns", description = "In vertical partitioning exploration mode, log known result mismatches and known internal query errors separately and continue with later query pairs", arity = 1)
    public boolean verticalPartitioningSuppressKnownPatterns;

    @Parameter(names = "--vp-enable-advanced-query-features", description = "Enable first-batch advanced vertical partitioning query features such as window expressions, UNION ALL, diversified aggregates, and helper indexes", arity = 1)
    public boolean verticalPartitioningAdvancedQueryFeatures;

    @Parameter(names = "--vp-enable-deep-exploration", description = "Enable stronger vertical partitioning exploration features; this includes advanced query features and additionally enables collation variants and boundary-value mutations", arity = 1)
    public boolean verticalPartitioningDeepExploration;

    @Parameter(names = "--vp-enable-time-travel-profile", description = "Run CockroachDB vertical partitioning comparisons against one explicit AS OF SYSTEM TIME snapshot per reconstruction strategy", arity = 1)
    public boolean verticalPartitioningTimeTravelProfile;

    @Parameter(names = "--vp-enable-distributed-profile", description = "Require a multi-node CockroachDB cluster for vertical partitioning, split and scatter VP relations, and collect gateway/range/leaseholder/DistSQL coverage", arity = 1)
    public boolean verticalPartitioningDistributedProfile;

    @Parameter(names = "--vp-enable-row-range-pressure-profile", description = "For CockroachDB vertical partitioning, amplify the VP source table to larger row-count buckets, create more ranges, scatter them, and collect extra DistSQL gateway plan coverage", arity = 1)
    public boolean verticalPartitioningRowRangePressureProfile;

    @Parameter(names = "--vp-row-range-pressure-max-source-rows", description = "Maximum target row count for the CockroachDB VP row/range pressure profile; values above 4096 are capped")
    public int verticalPartitioningRowRangePressureMaxSourceRows = 4096;

    @Parameter(names = "--vp-distributed-gateways", description = "Comma-separated CockroachDB SQL gateway endpoints (host:port) sampled by the distributed VP profile; the initial --host/--port gateway remains usable when omitted")
    public String verticalPartitioningDistributedGateways = "";

    @Parameter(names = "--vp-distributed-min-live-nodes", description = "Minimum number of live CockroachDB nodes required by the distributed VP profile")
    public int verticalPartitioningDistributedMinLiveNodes = 3;

    @Parameter(names = "--vertical-partitioning-max-source-rows", description = "Maximum number of rows copied from a SQLancer-generated table into the random-rewrite vertical partitioning source table; use 0 for no limit")
    public int verticalPartitioningMaxSourceRows = 256;

    @Override
    public List<CockroachDBOracleFactory> getTestOracleFactory() {
        return Arrays.asList(oracle);
    }

}
