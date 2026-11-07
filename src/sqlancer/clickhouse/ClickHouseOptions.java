package sqlancer.clickhouse;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "ClickHouse (default port: " + ClickHouseOptions.DEFAULT_PORT
        + ", default host: " + ClickHouseOptions.DEFAULT_HOST + ")")
public class ClickHouseOptions implements DBMSSpecificOptions<ClickHouseOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8123;

    @Parameter(names = "--oracle")
    public List<ClickHouseOracleFactory> oracle = Arrays.asList(ClickHouseOracleFactory.TLPWhere);

    @Parameter(names = "--ecst-query-pairs", description = "Comparable ECST pairs per invocation on one ClickHouse node")
    public int ecstQueryPairs = 4;

    @Parameter(names = { "--test-joins" }, description = "Allow the generation of JOIN clauses", arity = 1)
    public boolean testJoins = true;

    @Parameter(names = { "--analyzer" }, description = "Enable analyzer in ClickHouse", arity = 1)
    public boolean enableAnalyzer = true;

    @Parameter(names = { "--vp-query-variants-per-combination",
            "--vertical-partitioning-query-variants-per-combination" }, description = "Number of random query variants generated for every vertical partitioning split/reconstruction combination")
    public int verticalPartitioningQueryVariantsPerCombination = 5;

    @Parameter(names = "--vp-enable-reconstruction-extensions", description = "Also exercise aggregate, scalar-subquery, serialization, anti-join and windowed reconstruction extensions after validating unique, complete row identities", arity = 1)
    public boolean verticalPartitioningReconstructionExtensions;

    @Parameter(names = "--vp-helper-use-base-engine", description = "Enable ClickHouse VP base-engine diversity (MergeTree, Memory, or Log) and copy the selected row-preserving engine to helper tables", arity = 1)
    public boolean verticalPartitioningHelperUseBaseEngine;

    @Parameter(names = "--vp-source-copy-schema-objects", description = "Copy compatible ClickHouse source table schema objects into vertical partitioning helpers", arity = 1)
    public boolean verticalPartitioningSourceCopySchemaObjects;

    @Parameter(names = "--vp-allow-row-deletes", description = "Allow completed ClickHouse DELETE mutations during vertical partitioning base-table generation", arity = 1)
    public boolean verticalPartitioningAllowRowDeletes;

    @Parameter(names = "--vp-enable-update-mutations", description = "Apply completed, row-preserving ALTER UPDATE mutations to eligible VP base tables before materialization", arity = 1)
    public boolean verticalPartitioningUpdateMutations;

    @Parameter(names = "--vp-include-normal-workload", description = "Generate the regular ClickHouse tables and INSERT workload before VP setup, and allow VP to select those tables as source relations", arity = 1)
    public boolean verticalPartitioningIncludeNormalWorkload;

    @Parameter(names = "--vp-suppress-known-patterns", description = "Log known ClickHouse vertical partitioning patterns separately and continue with later query pairs", arity = 1)
    public boolean verticalPartitioningSuppressKnownPatterns;

    @Parameter(names = "--vp-enable-advanced-query-features", description = "Enable advanced ClickHouse vertical partitioning query features such as ROLLUP, diversified aggregates, ordering/type/nullability coverage, boundary mutations, and helper schema objects", arity = 1)
    public boolean verticalPartitioningAdvancedQueryFeatures;

    @Parameter(names = "--vp-enable-deep-exploration", description = "Enable the deliberately bounded deep ClickHouse vertical partitioning exploration tier; this includes advanced query features plus windows, UNION ALL, and correlated subqueries", arity = 1)
    public boolean verticalPartitioningDeepExploration;

    @Parameter(names = "--vp-enable-large-data-exploration", description = "Enable large ClickHouse VP base tables and row-id ordered helper tables for granule/index/pushdown exploration", arity = 1)
    public boolean verticalPartitioningLargeDataExploration;

    @Parameter(names = "--vp-enable-query-settings-profiles", description = "Run each ClickHouse VP oracle with one randomized query-level optimizer settings profile", arity = 1)
    public boolean verticalPartitioningQuerySettingsProfiles;

    @Parameter(names = "--vp-enable-optimizer-bug-probes", description = "Run bounded ClickHouse VP probes for known optimizer-sensitive JOIN/filter/ORDER BY/LIMIT combinations", arity = 1)
    public boolean verticalPartitioningOptimizerBugProbes = true;

    @Parameter(names = "--vp-enable-clickhouse-native-relations", description = "Enable ClickHouse-native VP relations such as PREWHERE, computed columns, FINAL, projections, and aggregate combinators", arity = 1)
    public boolean verticalPartitioningNativeRelations;

    @Parameter(names = "--vp-enable-distributed-profile", description = "Enable a ClickHouse distributed VP profile that creates distributed shadow access paths for VP source tables", arity = 1)
    public boolean verticalPartitioningDistributedProfile;

    @Parameter(names = "--vp-enable-distributed-shard-key-profile", description = "Exercise rand, sipHash(row id), business-column, and composite Distributed shard keys", arity = 1)
    public boolean verticalPartitioningDistributedShardKeyProfile;

    @Parameter(names = "--vp-distributed-cluster", description = "ClickHouse cluster name to use for the distributed VP profile; when omitted, the first available cluster is used")
    public String verticalPartitioningDistributedCluster = "";

    @Parameter(names = "--vp-distributed-min-live-nodes", description = "Minimum ClickHouse cluster node count required to enable the distributed VP profile")
    public int verticalPartitioningDistributedMinLiveNodes = 3;

    @Parameter(names = "--vp-enable-distributed-topology-profile", description = "Record ClickHouse shard/replica placement and verify all configured cluster endpoints are reachable", arity = 1)
    public boolean verticalPartitioningDistributedTopologyProfile;

    @Parameter(names = "--vp-enable-distributed-retry-profile", description = "Exercise asynchronous Distributed inserts, queue draining, and post-merge VP equivalence", arity = 1)
    public boolean verticalPartitioningDistributedRetryProfile;

    @Parameter(names = "--vp-enable-distributed-failure-profile", description = "Probe whether the configured cluster has enough replicas for failure failover; never stops nodes automatically", arity = 1)
    public boolean verticalPartitioningDistributedFailureProfile;

    @Parameter(names = "--vp-enable-multi-gateway-profile", description = "Execute the same VP source and Distributed reconstruction through multiple configured ClickHouse gateway endpoints", arity = 1)
    public boolean verticalPartitioningMultiGatewayProfile;

    @Parameter(names = "--vp-enable-replicated-merge-tree-profile", description = "Create temporary ReplicatedMergeTree VP helpers, wait for replication queues to converge, and compare their Distributed reconstruction", arity = 1)
    public boolean verticalPartitioningReplicatedMergeTreeProfile;

    @Parameter(names = "--vp-enable-complex-types", description = "Enable ClickHouse VP complex types: Enum, Array, Map, Tuple, Nested, DateTime64, Decimal256, UUID, IPv4, and IPv6", arity = 1)
    public boolean verticalPartitioningComplexTypes;

    @Parameter(names = "--vp-query-settings-profile-size", description = "Number of optimizer settings randomized per ClickHouse VP profile (3 or 5)", arity = 1)
    public int verticalPartitioningQuerySettingsProfileSize = 3;

    @Parameter(names = "--vp-source-row-limit", description = "Maximum number of rows copied from a normal ClickHouse source table into a VP source table when large-data exploration is disabled; use 0 for no limit")
    public int verticalPartitioningSourceRowLimit = 256;

    @Parameter(names = "--vp-enable-physical-state-profile", description = "Create multiple ordered MergeTree parts for VP source/helpers and compare again after OPTIMIZE FINAL", arity = 1)
    public boolean verticalPartitioningPhysicalStateProfile;

    @Parameter(names = "--vp-enable-part-layout-profile", description = "Enable the MergeTree multi-part/partition layout VP profile", arity = 1)
    public boolean verticalPartitioningPartLayoutProfile;

    @Parameter(names = "--vp-enable-optimize-final-profile", description = "Run OPTIMIZE FINAL and compare VP results after merges", arity = 1)
    public boolean verticalPartitioningOptimizeFinalProfile = true;

    @Parameter(names = "--vp-enable-partition-pruning-profile", description = "Verify partition and data-skipping pruning in the physical layout profile", arity = 1)
    public boolean verticalPartitioningPartitionPruningProfile;

    @Parameter(names = "--vp-enable-auto-merge-profile", description = "Wait for automatic MergeTree background merges before the post-merge comparison", arity = 1)
    public boolean verticalPartitioningAutoMergeProfile;

    @Parameter(names = "--vp-physical-state-insert-parts", description = "Number of ordered INSERT batches used by the VP physical-state profile")
    public int verticalPartitioningPhysicalStateInsertParts = 4;

    @Parameter(names = "--vp-part-inserts-per-table", description = "Number of INSERT batches used per source/helper table in the part-layout profile")
    public int verticalPartitioningPartInsertsPerTable = 3;

    @Override
    public List<ClickHouseOracleFactory> getTestOracleFactory() {
        return oracle;
    }
}
