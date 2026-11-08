package sqlancer.mariadb;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "MariaDB (default port: " + MariaDBOptions.DEFAULT_PORT
        + ", default host: " + MariaDBOptions.DEFAULT_HOST + ")")
public class MariaDBOptions implements DBMSSpecificOptions<MariaDBOracleFactory> {
    @Parameter(names = "--ecst-query-pairs", description = "Comparable MariaDB ECST pairs per invocation")
    public int ecstQueryPairs = 4;

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    @Parameter(names = "--oracle")
    public List<MariaDBOracleFactory> oracles = Arrays.asList(MariaDBOracleFactory.NOREC);

    @Parameter(names = "--vp-query-variants-per-combination")
    public int verticalPartitioningQueryVariantsPerCombination = 5;

    @Parameter(names = "--vp-helper-use-base-engine", arity = 1)
    public boolean verticalPartitioningHelperUseBaseEngine;

    @Parameter(names = "--vp-source-copy-schema-objects", arity = 1)
    public boolean verticalPartitioningSourceCopySchemaObjects;

    @Parameter(names = "--vp-allow-row-deletes", arity = 1)
    public boolean verticalPartitioningAllowRowDeletes;

    // When enabled, established known-pool patterns are logged separately from
    // database*.log candidates.
    @Parameter(names = "--vp-suppress-known-patterns", arity = 1)
    public boolean verticalPartitioningSuppressKnownPatterns;

    @Parameter(names = "--vp-enable-advanced-query-features", arity = 1)
    public boolean verticalPartitioningAdvancedQueryFeatures;

    @Parameter(names = "--vp-enable-deep-exploration", arity = 1)
    public boolean verticalPartitioningDeepExploration;

    @Parameter(names = "--vertical-partitioning-max-source-rows")
    public int verticalPartitioningMaxSourceRows = 256;

    @Parameter(names = "--vp-mariadb-features", arity = 1)
    public boolean verticalPartitioningMariaDBFeatures;

    /**
     * Run a small, session-isolated optimizer profile matrix for each VP query. Profiles only toggle switches reported
     * by @@optimizer_switch.
     */
    @Parameter(names = "--vp-optimizer-profiles", arity = 1)
    public boolean verticalPartitioningOptimizerProfiles;

    @Parameter(names = "--vp-optimizer-cross-profile", arity = 1)
    public boolean verticalPartitioningOptimizerCrossProfile;

    @Override
    public List<MariaDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
