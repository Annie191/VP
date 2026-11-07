package sqlancer.clickhouse.oracle;

import sqlancer.clickhouse.ClickHouseOptions;

/** Identical, query-scoped settings on the HTTP connection and in standalone replay. */
final class ClickHouseEcstSession {
    static final String SETTINGS = " SETTINGS enable_analyzer=1,enable_materialized_cte=1,join_use_nulls=1,"
            + "join_default_strictness='ALL',join_algorithm='hash',optimize_move_to_prewhere=0,"
            + "use_query_condition_cache=0,use_skip_indexes_on_data_read=0,max_threads=2";

    private ClickHouseEcstSession() {
    }

    static void validateOptions(ClickHouseOptions options) {
        if (options.verticalPartitioningDistributedProfile || options.verticalPartitioningDistributedShardKeyProfile
                || options.verticalPartitioningDistributedTopologyProfile
                || options.verticalPartitioningDistributedRetryProfile
                || options.verticalPartitioningDistributedFailureProfile
                || options.verticalPartitioningMultiGatewayProfile
                || options.verticalPartitioningReplicatedMergeTreeProfile
                || !options.verticalPartitioningDistributedCluster.isBlank()) {
            throw new IllegalArgumentException(
                    "ClickHouse ECST uses one local node; distributed VP profiles are unsupported");
        }
    }
}
