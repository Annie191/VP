package sqlancer.postgres;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "PostgreSQL (default port: " + PostgresOptions.DEFAULT_PORT
        + ", default host: " + PostgresOptions.DEFAULT_HOST + ")")
public class PostgresOptions implements DBMSSpecificOptions<PostgresOracleFactory> {
    @Parameter(names = "--ecst-query-pairs", description = "Comparable PostgreSQL ECST query pairs per check")
    public int ecstQueryPairs = 4;

    @Parameter(names = "--ecst-snapshot-rows", description = "Maximum frozen ECST source rows; zero copies all rows")
    public int ecstSnapshotRows = 256;

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 5432;
    private static Boolean defaultTestTablespaces;

    @Parameter(names = "--bulk-insert", description = "Specifies whether INSERT statements should be issued in bulk", arity = 1)
    public boolean allowBulkInsert;

    @Parameter(names = "--oracle", description = "Specifies which test oracle should be used for PostgreSQL")
    public List<PostgresOracleFactory> oracle = Arrays.asList(PostgresOracleFactory.QUERY_PARTITIONING);

    @Parameter(names = "--connection-timeout", description = "Timeout in seconds for connecting to the server", arity = 1)
    public int connectionTimeoutInSeconds;

    @Parameter(names = "--test-collations", description = "Specifies whether to test different collations", arity = 1)
    public boolean testCollations = true;

    @Parameter(names = "--test-tablespaces", description = "Specifies whether to test tablespace creation (default is OS-dependent)", arity = 1)
    public boolean testTablespaces;

    @Parameter(names = "--tablespace-path", description = "Base path for tablespace directories (default is OS-dependent)", arity = 1)
    public String tablespacePath = getDefaultTablespacePath();

    @Parameter(names = "--connection-url", description = "Specifies the URL for connecting to the PostgreSQL server", arity = 1)
    public String connectionURL = String.format("postgresql://%s:%d/test", PostgresOptions.DEFAULT_HOST,
            PostgresOptions.DEFAULT_PORT);

    @Parameter(names = "--extensions", description = "Specifies a comma-separated list of extension names to be created in each test database", arity = 1)
    public String extensions = "";

    @Parameter(names = "--vertical-partitioning-rows", description = "Target rows for the large PostgreSQL VP base-table profile; zero disables large-table population")
    public int verticalPartitioningRows = 10000;

    @Parameter(names = "--vertical-partitioning-query-pairs", description = "Accepted for vertical partitioning option compatibility; split/reconstruction schedules and query variants drive the random-rewrite oracle")
    public int verticalPartitioningQueryPairs = 24;

    @Parameter(names = { "--vp-query-variants-per-combination",
            "--vertical-partitioning-query-variants-per-combination" }, description = "Target number of effective query variants fully compared under all selected GUC profiles for every split/reconstruction combination")
    public int verticalPartitioningQueryVariantsPerCombination = 5;

    @Parameter(names = "--vp-max-attempts-per-query-variant", description = "Attempt multiplier for filling each split/reconstruction combination with effective variants; at least 1")
    public int verticalPartitioningMaxAttemptsPerQueryVariant = 3;

    @Parameter(names = "--vp-query-combination-time-budget-ms", description = "Elapsed time budget per split/reconstruction combination, checked between attempts; 0 disables this time limit")
    public long verticalPartitioningQueryCombinationTimeBudgetMs = 60000;

    @Parameter(names = "--vp-helper-use-base-engine", description = "Accepted for vertical partitioning option compatibility; PostgreSQL has no per-table storage engine, so this is a no-op", arity = 1)
    public boolean verticalPartitioningHelperUseBaseEngine;

    @Parameter(names = "--vp-source-copy-schema-objects", description = "Try to copy indexes and table constraints from the selected base table to the vertical partitioning source table; failures are skipped", arity = 1)
    public boolean verticalPartitioningSourceCopySchemaObjects;

    @Parameter(names = "--vp-allow-row-deletes", description = "Allow row-removing or table-removing actions during the random mutation phase when the vertical partitioning oracle is enabled", arity = 1)
    public boolean verticalPartitioningAllowRowDeletes;

    @Parameter(names = "--vp-suppress-known-patterns", description = "In vertical partitioning exploration mode, log known high-frequency mismatches separately and continue with later query pairs", arity = 1)
    public boolean verticalPartitioningSuppressKnownPatterns;

    @Parameter(names = "--vp-enable-advanced-query-features", description = "Enable first-batch advanced vertical partitioning query features such as window expressions, UNION ALL, diversified aggregates, and helper indexes", arity = 1)
    public boolean verticalPartitioningAdvancedQueryFeatures;

    @Parameter(names = "--vp-enable-deep-exploration", description = "Enable stronger vertical partitioning exploration features; this includes advanced query features and additionally enables conservative PostgreSQL collation variants and boundary-value mutations", arity = 1)
    public boolean verticalPartitioningDeepExploration;

    @Parameter(names = "--vertical-partitioning-max-source-rows", description = "Maximum number of rows copied from a SQLancer-generated table into the random-rewrite vertical partitioning source table; use 0 for no limit")
    public int verticalPartitioningMaxSourceRows = 10000;

    private static boolean determineDefaultTablespaceSupport() {
        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.contains("linux")) {
            System.out.println("[INFO] Linux detected: Enabling tablespace testing by default");
            return true;
        } else if (osName.contains("mac") || osName.contains("darwin")) {
            System.out.println(
                    "[INFO] macOS detected: Disabling tablespace testing by default due to different /tmp handling. Override with --test-tablespaces=true and ensure proper directory permissions.");
            return false;
        } else if (osName.contains("windows")) {
            System.out.println(
                    "[INFO] Windows detected: Disabling tablespace testing by default due to path format differences. Override with --test-tablespaces=true and use --tablespace-path to set a valid Windows path.");
            return false;
        } else {
            System.out.println(
                    "[INFO] Unknown OS detected: Disabling tablespace testing by default for safety. Override with --test-tablespaces=true if your system supports PostgreSQL tablespaces.");
            return false;
        }
    }

    public static String getDefaultTablespacePath() {
        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.contains("windows")) {
            // On Windows, use a path in the temp directory
            return new File(System.getProperty("java.io.tmpdir"), "postgresql" + File.separator + "tablespace")
                    .getAbsolutePath();
        } else {
            // On Unix-like systems, use /tmp
            return "/tmp/postgresql/tablespace";
        }
    }

    @Override
    public List<PostgresOracleFactory> getTestOracleFactory() {
        return oracle;
    }

    public String getTablespacePath() {
        if (tablespacePath == null || tablespacePath.isBlank()) {
            throw new AssertionError("Tablespace path is null or empty. Please configure --tablespace-path");
        }

        File path = new File(tablespacePath);

        // Check if the directory exists or can be created
        if (!path.exists() && !path.mkdirs()) {
            throw new AssertionError("Cannot create tablespace directory: " + tablespacePath
                    + ". Please ensure the parent directory exists and you have write permissions.");
        }

        // Check if it's actually a directory
        if (!path.isDirectory()) {
            throw new AssertionError("Tablespace path is not a directory: " + tablespacePath);
        }

        // Check write permissions
        if (!path.canWrite()) {
            throw new AssertionError("No write permissions for tablespace directory: " + tablespacePath
                    + ". Please ensure you have write permissions to this directory.");
        }

        return tablespacePath;
    }

    public boolean isTestTablespaces() {
        // If the user explicitly set the value via command line, use that
        // Otherwise, use the OS-dependent default
        return testTablespaces || getDefaultTablespaceSupport();
    }

    private static boolean getDefaultTablespaceSupport() {
        if (defaultTestTablespaces == null) {
            defaultTestTablespaces = determineDefaultTablespaceSupport();
        }
        return defaultTestTablespaces;
    }
}
