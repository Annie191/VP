package sqlancer.tidb.oracle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicLong;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstFeature;
import sqlancer.common.oracle.ecst.EcstFailureRecord;
import sqlancer.common.oracle.ecst.EcstHistory;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
import sqlancer.common.oracle.ecst.EcstPhase;
import sqlancer.common.oracle.ecst.EcstRuleLibrary;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.common.oracle.ecst.EcstTransformationConfig;
import sqlancer.tidb.TiDBOptions;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTable;

/**
 * TiDB ECST prototype. The implemented capability set is deliberately smaller than the
 * full ECST product and uses a separate oracle entry point plus the controlled VP seam.
 */
public final class TiDBECSTOracle implements TestOracle<TiDBGlobalState> {
    private static final AtomicLong RUN_IDS = new AtomicLong();
    private final TiDBGlobalState state;
    private final TiDBVerticalPartitioningOracle vp;
    private final TiDBExecutionContextAnalyzer analyzer;
    private final TiDBEcstCapabilities capabilities;
    private final EcstHistory history = new EcstHistory();
    private final List<EcstTarget> targets;
    private volatile String lastSynthesisSummary = "<none>";
    private long semanticPreconditionRejects;
    private int requestedBudget;
    private int completedCases;
    private int attempts;
    private int candidateSetSkips;
    private int creationSkips;
    private int executionFailures;
    private int bugCandidates;
    private int comparisonSkips;
    private int failureRecords;
    private int seedMappingSkips;
    private int noValidTargetSkips;
    private int noConfigurationSkips;
    private int seedRefreshes;
    private long runId;
    private int caseSequence;
    private final List<String> candidateRejectReasons = new ArrayList<>();

    public TiDBECSTOracle(TiDBGlobalState state) {
        this.state = state;
        this.capabilities = new TiDBEcstCapabilities(state);
        this.targets = capabilities.supportedTargets(state);
        this.vp = new TiDBVerticalPartitioningOracle(state, false);
        this.analyzer = new TiDBExecutionContextAnalyzer(state);
    }

    static List<EcstTarget> defaultTargets() {
        return allTargets().stream().filter(target -> !target.getKey().contains("TIFLASH")).collect(Collectors.toList());
    }

    static List<EcstTarget> allTargets() {
        return Arrays.asList(
                new EcstTarget("TIDB_LOCATION_ROOT_TO_COP_TIKV", EcstContextDimension.LOCATION, "ROOT", "COP_TIKV"),
                new EcstTarget("TIDB_LOCATION_ROOT_TO_COP_TIFLASH", EcstContextDimension.LOCATION, "ROOT",
                        "COP_TIFLASH"),
                new EcstTarget("TIDB_LOCATION_COP_TIKV_TO_ROOT", EcstContextDimension.LOCATION, "COP_TIKV", "ROOT"),
                new EcstTarget("TIDB_LOCATION_COP_TIFLASH_TO_ROOT", EcstContextDimension.LOCATION, "COP_TIFLASH",
                        "ROOT"),
                new EcstTarget("TIDB_PUSHDOWN_ROOT_TO_COP", EcstContextDimension.BOUNDARY, "NON_PUSHDOWN",
                        "PUSHDOWN"),
                new EcstTarget("TIDB_PUSHDOWN_COP_TO_ROOT", EcstContextDimension.BOUNDARY, "PUSHDOWN",
                        "NON_PUSHDOWN"),
                new EcstTarget("TIDB_ACCESS_TABLEFULLSCAN_TO_INDEXLOOKUP", EcstContextDimension.ACCESS_PATH,
                        "TABLEFULLSCAN", "INDEXLOOKUP"),
                new EcstTarget("TIDB_ACCESS_INDEXLOOKUP_TO_TABLEFULLSCAN", EcstContextDimension.ACCESS_PATH,
                        "INDEXLOOKUP", "TABLEFULLSCAN"));
    }

    @Override
    public void check() throws Exception {
        try {
            checkInternal();
        } finally {
            writeFeedbackSummary();
        }
    }

    private void checkInternal() throws Exception {
        TiDBOptions options = state.getDbmsSpecificOptions();
        int budget = Math.max(1, options.ecstQueryPairs);
        requestedBudget = budget;
        runId = RUN_IDS.incrementAndGet();
        caseSequence = 0;
        completedCases = 0;
        int completed = 0;
        attempts = 0;
        candidateSetSkips = 0;
        creationSkips = 0;
        executionFailures = 0;
        bugCandidates = 0;
        comparisonSkips = 0;
        failureRecords = 0;
        seedMappingSkips = 0;
        noValidTargetSkips = 0;
        noConfigurationSkips = 0;
        seedRefreshes = 0;
        candidateRejectReasons.clear();
        semanticPreconditionRejects = 0;
        EcstSeedPool seedPool = new EcstSeedPool(vp);
        while (completed < budget && attempts < budget * 4) {
            attempts++;
            TiDBVerticalPartitioningOracle.EcstSeed seed = null;
            EcstContext seedContext;
            TiDBExecutionContextAnalyzer.ParsedContext seedParsed;
            try {
                seed = seedPool.nextSeed();
                seedParsed = analyzer.extractContext(seed.getOriginalQuery(), seed.getOperation());
                seedContext = seedParsed.context();
            } catch (IgnoreMeException | IllegalArgumentException ignored) {
                creationSkips++;
                recordFailure(EcstPhase.SEED_GENERATION, seed, null, null, ignored);
                continue;
            } catch (Exception failure) {
                creationSkips++;
                recordFailure(EcstPhase.SEED_GENERATION, seed, null, null, failure);
                cleanupSeed(seed);
                if (isRecoverable(failure)) continue;
                throw failure;
            } catch (AssertionError failure) {
                creationSkips++;
                recordFailure(EcstPhase.SEED_GENERATION, seed, null, null, failure);
                cleanupSeed(seed);
                throw failure;
            }
            EcstTarget target = null;
            EcstTransformationConfig config = null;
            try {
                List<EcstTarget> validTargets = supportedTargets(seed, seedContext);
                if (seedParsed.status() == TiDBExecutionContextAnalyzer.MatchStatus.NOT_FOUND
                        || seedParsed.status() == TiDBExecutionContextAnalyzer.MatchStatus.AMBIGUOUS) {
                    seedMappingSkips++;
                    writeSeedSkipRecord(seed, seedParsed, validTargets.size(), List.of("seed mapping: "
                            + seedParsed.reason()));
                    cleanupSeed(seed);
                    continue;
                }
                if (validTargets.isEmpty()) {
                    noValidTargetSkips++;
                    writeSeedSkipRecord(seed, seedParsed, 0, List.of("no valid target for seed context"));
                    cleanupSeed(seed);
                    continue;
                }
                Map<String, List<EcstTransformationConfig>> candidateSets = synthesizeCandidateSets(seed, seedContext,
                        validTargets);
                if (candidateSets.isEmpty()) {
                    noConfigurationSkips++;
                    candidateSetSkips++;
                    writeSeedSkipRecord(seed, seedParsed, validTargets.size(), candidateRejectReasons);
                    cleanupSeed(seed);
                    continue;
                }
                target = chooseTarget(candidateSets, seed.features());
                List<EcstTransformationConfig> applicable = candidateSets.get(target.getKey());
                config = chooseConfig(target, applicable, seed);
            } catch (Exception | AssertionError failure) {
                recordFailure(config == null ? EcstPhase.CONFIGURATION_SYNTHESIS : EcstPhase.TARGET_SELECTION,
                        seed, target, config, failure);
                cleanupSeed(seed);
                if (isRecoverable(failure)) continue;
                throw failure;
            }
            EcstFeature feature = featureForSeed(seed, config);
            long started = System.nanoTime();
            TiDBVerticalPartitioningOracle.EcstCase testCase;
            try {
                testCase = vp.applyEcstTransformation(seed, config, target);
            } catch (IgnoreMeException | IllegalArgumentException ignored) {
                creationSkips++;
                recordFailure(EcstPhase.MATERIALIZATION, seed, target, config, ignored);
                cleanupSeed(seed);
                continue;
            } catch (Exception failure) {
                recordFailure(EcstPhase.MATERIALIZATION, seed, target, config, failure);
                cleanupSeed(seed);
                if (isRecoverable(failure)) continue;
                throw failure;
            } catch (AssertionError failure) {
                recordFailure(EcstPhase.MATERIALIZATION, seed, target, config, failure);
                cleanupSeed(seed);
                throw failure;
            }
            try {
                EcstContext refreshedSeedContext = analyzer.extractContext(testCase.getOriginalQuery(),
                        testCase.getOperation()).context();
                if (!sameContext(seedContext, refreshedSeedContext)) {
                    recordFailure(EcstPhase.PLAN_ANALYSIS, seed, target, config, testCase,
                            new IllegalStateException("SEED_CONTEXT_CHANGED: seed=" + seedContext + ", refreshed="
                                    + refreshedSeedContext));
                    cleanupSeed(seed);
                    continue;
                }
            } catch (Exception failure) {
                recordFailure(EcstPhase.PLAN_ANALYSIS, seed, target, config, testCase, failure);
                cleanupSeed(seed);
                if (isRecoverable(failure)) continue;
                throw failure;
            }
            TiDBVerticalPartitioningOracle.EcstSemanticPreconditionResult semantic;
            try {
                semantic = vp.checkSemanticPreconditions(testCase);
            } catch (Exception | AssertionError failure) {
                recordFailure(EcstPhase.PRECONDITION_CHECK, seed, target, config, testCase, failure);
                cleanupSeed(seed);
                if (isRecoverable(failure)) continue;
                throw failure;
            }
            if (!semantic.isValid()) {
                semanticPreconditionRejects++;
                writeCaseRecord(target, config, testCase, null, null, semantic);
                cleanupSeed(seed);
                continue;
            }
            EcstObservation observation;
            try {
                observation = analyzer.analyze(testCase.getOriginalQuery(), testCase.getTransformedQuery(),
                        testCase.getOperation(), target);
            } catch (Exception | AssertionError failure) {
                recordFailure(EcstPhase.PLAN_ANALYSIS, seed, target, config, testCase, failure);
                cleanupSeed(seed);
                if (isRecoverable(failure)) continue;
                throw failure;
            }

            boolean keepArtifacts = false;
            TiDBVerticalPartitioningOracle.EcstComparison comparison = null;
            try {
                comparison = vp.compareEcstCase(testCase);
                if (!comparison.isComparable()) {
                    comparisonSkips++;
                    continue;
                }
                // A comparable result comparison, including a mismatch, is a completed test pair.
                completed++;
                completedCases = completed;
                if (!comparison.isEqual()) {
                    bugCandidates++;
                    keepArtifacts = true;
                    throw new AssertionError(formatBug(target, testCase, observation, comparison));
                }
            } catch (Exception failure) {
                executionFailures++;
                recordFailure(EcstPhase.RESULT_COMPARISON, seed, target, config, testCase, failure);
                if (isRecoverable(failure)) continue;
                throw failure;
            } finally {
                writeCaseRecord(target, config, testCase, observation, comparison, null);
                EcstShiftStatus status = comparison != null && !comparison.isComparable() ? EcstShiftStatus.UNKNOWN
                        : observation.getStatus();
                history.observe(target.getKey(), config.stableKey(), status,
                        observation.getObservedShifts(), System.nanoTime() - started, feature);
                if (!keepArtifacts) {
                    cleanupSeed(seed);
                }
            }
        }
        completedCases = completed;
        if (completed == 0) {
            throw new IgnoreMeException();
        }
    }

    private static boolean sameContext(EcstContext before, EcstContext after) {
        if (before == null || after == null) {
            return false;
        }
        for (EcstContextDimension dimension : EcstContextDimension.values()) {
            String left = before.value(dimension);
            String right = after.value(dimension);
            if ("UNKNOWN".equals(left) && "UNKNOWN".equals(right)) {
                continue;
            }
            if (!left.equals(right)) {
                return false;
            }
        }
        return true;
    }

    private final class EcstSeedPool {
        private static final int REFRESH_INTERVAL = 8;
        private final TiDBVerticalPartitioningOracle backend;
        private int issued;

        private EcstSeedPool(TiDBVerticalPartitioningOracle backend) {
            this.backend = backend;
        }

        private TiDBVerticalPartitioningOracle.EcstSeed nextSeed() throws Exception {
            if (issued > 0 && issued % REFRESH_INTERVAL == 0) {
                backend.refreshEcstBaseRelations();
                seedRefreshes++;
            }
            issued++;
            return backend.createEcstSeed();
        }
    }

    Map<String, List<EcstTransformationConfig>> synthesizeCandidateSets(
            TiDBVerticalPartitioningOracle.EcstSeed seed, EcstContext seedContext, List<EcstTarget> validTargets) {
        Map<String, List<EcstTransformationConfig>> result = new LinkedHashMap<>();
        candidateRejectReasons.clear();
        List<String> synthesis = new ArrayList<>();
        QueryShape shape = seed.getShape();
        for (EcstTarget target : validTargets) {
            int generated = 0;
            int ruleAccepted = 0;
            int legal = 0;
            int applicableCount = 0;
            List<EcstTransformationConfig> applicable = new ArrayList<>();
            for (EcstTransformationConfig config : enumerateConfigurationSpace()) {
                generated++;
                if (!supportsRule(target, config)) {
                    continue;
                }
                ruleAccepted++;
                if (!isLegal(target, config, shape)) {
                    continue;
                }
                legal++;
                if (!isApplicableToSeed(config, shape, seed.getExpressionContext())) {
                    continue;
                }
                if (!isDependencyLayoutApplicable(seed, config)) {
                    continue;
                }
                if (!capabilities.supports(config, target, seed, seedContext)) {
                    continue;
                }
                applicableCount++;
                applicable.add(config);
            }
            if (!applicable.isEmpty()) {
                result.put(target.getKey(), applicable);
            } else {
                candidateRejectReasons.add(target.getKey() + ": no applicable configuration after rule/legal/seed/layout/capability checks");
            }
            synthesis.add(target.getKey() + "{generated=" + generated + ",ruleAccepted=" + ruleAccepted
                    + ",legal=" + legal + ",applicable=" + applicableCount + ",allCandidates=" + applicable.size()
                    + "}");
        }
        lastSynthesisSummary = "capabilities=" + capabilities.describe() + "; queryShape=" + shape
                + "; accessPathPreparation=" + seed.getAccessPathPreparation() + "; "
                + String.join("; ", synthesis);
        return result;
    }

    /** Resolves S_valid from the actual seed execution context, including direction. */
    List<EcstTarget> supportedTargets(TiDBVerticalPartitioningOracle.EcstSeed seed, EcstContext context) {
        if (seed == null || context == null) {
            return List.of();
        }
        List<EcstTarget> valid = new ArrayList<>();
        for (EcstTarget target : targets) {
            String observed = context.value(target.getDimension());
            if ("UNKNOWN".equals(observed) || !capabilities.supportsTarget(target, seed, context)) {
                continue;
            }
            if (target.getFrom().equals(observed)) {
                valid.add(target);
            }
        }
        return valid;
    }

    /** Compatibility helper retained for diagnostics; production synthesis always consumes a seed. */
    QueryShape inspectQueryShape() {
        int tables = 0;
        int maxColumns = 0;
        try {
            state.updateSchema();
            for (TiDBTable table : state.getSchema().getDatabaseTables()) {
                if (!table.isView() && !table.getName().startsWith("vp_")) {
                    tables++;
                    maxColumns = Math.max(maxColumns, table.getColumns().size());
                }
            }
        } catch (Exception ignored) {
            // Candidate synthesis remains conservative if schema metadata is temporarily unavailable.
        }
        // Schema width is only a dependency bound. The controlled ECST generator constructs its own
        // SELECT/HAVING seed queries, so those capabilities must not be inferred from table metadata.
        // JOIN remains disabled until a real target query with a join is supplied.
        return generatorQueryShape(tables > 0, maxColumns);
    }

    static QueryShape generatorQueryShape(boolean hasBaseTable, int maxDependencies) {
        return new QueryShape(hasBaseTable, hasBaseTable, hasBaseTable, false, maxDependencies);
    }

    /** Extracts query-level shape signals without treating column count as an aggregation signal. */
    static QueryShape queryShapeFromSql(String query, int maxDependencies) {
        String normalized = query == null ? "" : query.toLowerCase(java.util.Locale.ROOT);
        boolean hasProjection = normalized.contains("select") && !normalized.matches(".*select\\s+\\*.*");
        boolean hasAggregation = normalized.matches(".*\\b(count|sum|avg|min|max|group\\s+by)\\b.*");
        boolean hasJoin = normalized.matches(".*\\b(join|union)\\b.*");
        boolean hasBaseTable = normalized.contains(" from ");
        return new QueryShape(hasBaseTable, hasProjection, hasAggregation, hasJoin, maxDependencies);
    }

    /** Full bounded product before target, semantic, and TiDB capability filtering. */
    static List<EcstTransformationConfig> enumerateConfigurationSpace() {
        List<EcstTransformationConfig> result = new ArrayList<>();
        for (EcstTransformationConfig.Representation representation : EcstTransformationConfig.Representation.values()) {
            for (EcstTransformationConfig.PartitionLayout layout : EcstTransformationConfig.PartitionLayout.values()) {
                for (EcstTransformationConfig.Reconstruction reconstruction : EcstTransformationConfig.Reconstruction
                        .values()) {
                    for (EcstTransformationConfig.ExpressionContext context : EcstTransformationConfig.ExpressionContext
                            .values()) {
                        result.add(new EcstTransformationConfig(representation, layout, reconstruction, context));
                    }
                }
            }
        }
        return result;
    }

    static List<EcstTransformationConfig> applicableConfigurations(EcstTarget target) {
        List<EcstTransformationConfig> result = new ArrayList<>();
        for (EcstTransformationConfig config : enumerateConfigurationSpace()) {
            if (supportsRule(target, config) && isLegal(target, config, QueryShape.synthetic())
                    && isApplicableToTiDb(config, QueryShape.synthetic())) {
                result.add(config);
            }
        }
        return result;
    }

    private static boolean supportsRule(EcstTarget target, EcstTransformationConfig config) {
        return EcstRuleLibrary.tiDbRules().stream()
                .anyMatch(rule -> rule.getTargetKey().equals(targetFamily(target)) && rule.supports(config));
    }

    private static String targetFamily(EcstTarget target) {
        switch (target.getDimension()) {
        case LOCATION:
            return "TIDB_LOCATION_SHIFT";
        case BOUNDARY:
            return "TIDB_PUSHDOWN_SHIFT";
        case ACCESS_PATH:
            return "TIDB_ACCESS_PATH_SHIFT";
        default:
            return target.getKey();
        }
    }

    private static boolean isLegal(EcstTarget target, EcstTransformationConfig config, QueryShape shape) {
        if (config.getRepresentation() == EcstTransformationConfig.Representation.ORIGINAL) {
            return false;
        }
        if (config.getRepresentation() == EcstTransformationConfig.Representation.VIEW
                && config.getReconstruction() != EcstTransformationConfig.Reconstruction.VIEW) {
            return false;
        }
        if (config.getRepresentation() == EcstTransformationConfig.Representation.REPLICATED
                && config.getReconstruction() == EcstTransformationConfig.Reconstruction.CTE) {
            return false;
        }
        if (config.getPartitionLayout() == EcstTransformationConfig.PartitionLayout.CROSS_SIDE
                && shape.maxDependencies < 2) {
            return false;
        }
        if (config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.ON) {
            return shape.hasJoin && (target.getDimension() == EcstContextDimension.BOUNDARY
                    || target.getDimension() == EcstContextDimension.ACCESS_PATH);
        }
        if (config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.HAVING) {
            return shape.hasAggregation;
        }
        return config.getExpressionContext() != EcstTransformationConfig.ExpressionContext.SELECT || shape.hasProjection;
    }

    /** TiDB currently has a VP materializer for these contexts; other legal configs remain visible as future work. */
    private static boolean isApplicableToSeed(EcstTransformationConfig config, QueryShape shape,
            EcstTransformationConfig.ExpressionContext seedExpressionContext) {
        return config.getExpressionContext() == seedExpressionContext && shape.hasBaseTable;
    }

    private static boolean isDependencyLayoutApplicable(TiDBVerticalPartitioningOracle.EcstSeed seed,
            EcstTransformationConfig config) {
        String actual = seed.getDependencyLayout(config.getPartitionLayout());
        if (config.getPartitionLayout() == EcstTransformationConfig.PartitionLayout.CROSS_SIDE) {
            return "CROSS_SIDE".equals(actual);
        }
        return "SAME_SIDE".equals(actual) || "SINGLE_SIDE".equals(actual);
    }

    private static boolean isApplicableToTiDb(EcstTransformationConfig config, QueryShape shape) {
        return config.getRepresentation() == EcstTransformationConfig.Representation.VERTICAL_PARTITIONED
                && (config.getReconstruction() == EcstTransformationConfig.Reconstruction.JOIN
                        || config.getReconstruction() == EcstTransformationConfig.Reconstruction.DERIVED_TABLE)
                && (config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.WHERE
                        || config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.SELECT
                        || config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.HAVING)
                && shape.hasBaseTable;
    }

    public static final class QueryShape {
        private final boolean hasBaseTable;
        private final boolean hasProjection;
        private final boolean hasAggregation;
        private final boolean hasJoin;
        private final int maxDependencies;

        private QueryShape(boolean hasBaseTable, boolean hasProjection, boolean hasAggregation, boolean hasJoin,
                int maxDependencies) {
            this.hasBaseTable = hasBaseTable;
            this.hasProjection = hasProjection;
            this.hasAggregation = hasAggregation;
            this.hasJoin = hasJoin;
            this.maxDependencies = maxDependencies;
        }

        static QueryShape synthetic() {
            return new QueryShape(true, true, true, false, 2);
        }

        @Override
        public String toString() {
            return "baseTable=" + hasBaseTable + ",projection=" + hasProjection + ",aggregation="
                    + hasAggregation + ",join=" + hasJoin + ",maxDependencies=" + maxDependencies;
        }

        public boolean hasAggregation() {
            return hasAggregation;
        }

        public boolean hasProjection() {
            return hasProjection;
        }

        public boolean hasJoin() {
            return hasJoin;
        }

        public boolean hasBaseTable() {
            return hasBaseTable;
        }

        public int getMaxDependencies() {
            return maxDependencies;
        }
    }

    private EcstTarget chooseTarget(Map<String, List<EcstTransformationConfig>> candidateSets, EcstFeature targetFeature) {
        Map<String, List<String>> keys = new LinkedHashMap<>();
        for (Map.Entry<String, List<EcstTransformationConfig>> entry : candidateSets.entrySet()) {
            keys.put(entry.getKey(), entry.getValue().stream().map(EcstTransformationConfig::stableKey)
                    .collect(Collectors.toList()));
        }
        String chosen = history.chooseTargetDecision(keys, targetFeature).getKey();
        for (EcstTarget target : targets) {
            if (target.getKey().equals(chosen)) {
                return target;
            }
        }
        for (EcstTarget target : targets) {
            if (candidateSets.containsKey(target.getKey())) {
                return target;
            }
        }
        throw new IllegalStateException("Target scheduler selected a target outside S_valid: " + chosen);
    }

    private EcstTransformationConfig chooseConfig(EcstTarget target, List<EcstTransformationConfig> applicable,
            TiDBVerticalPartitioningOracle.EcstSeed seed) {
        if (applicable.isEmpty()) {
            throw new IllegalStateException("No applicable ECST configuration for target " + target.getKey());
        }
        Map<String, EcstFeature> features = new LinkedHashMap<>();
        for (EcstTransformationConfig candidate : applicable) {
            features.put(candidate.stableKey(), featureForSeed(seed, candidate));
        }
        String chosen = history.chooseTransformDecision(target.getKey(), features).getKey();
        for (EcstTransformationConfig candidate : applicable) {
            if (candidate.stableKey().equals(chosen)) {
                return candidate;
            }
        }
        return applicable.get(state.getRandomly().getInteger(0, applicable.size()));
    }

    private EcstFeature featureForSeed(TiDBVerticalPartitioningOracle.EcstSeed seed,
            EcstTransformationConfig config) {
        return new EcstFeature("TiDB", seed.getExpressionType(), config.getExpressionContext().name(),
                seed.getDependencyLayout(config.getPartitionLayout()), seed.getQueryShape());
    }

    private String formatBug(EcstTarget target, TiDBVerticalPartitioningOracle.EcstCase testCase,
            EcstObservation observation,
            TiDBVerticalPartitioningOracle.EcstComparison comparison) {
        return "[ECST BUG CANDIDATE]" + System.lineSeparator() + "target=" + target + System.lineSeparator()
                + "config=" + testCase.getDescription() + System.lineSeparator() + "shiftStatus="
                + observation.getStatus() + System.lineSeparator() + "shiftReason=" + observation.getReason()
                + System.lineSeparator() + "originalContext=" + observation.getOriginal() + System.lineSeparator()
                + "transformedContext=" + observation.getTransformed() + System.lineSeparator() + "originalMatchedNode="
                + observation.getOriginalMatchedNode() + System.lineSeparator() + "transformedMatchedNode="
                + observation.getTransformedMatchedNode() + System.lineSeparator() + "originalEvidence="
                + observation.getOriginalEvidence() + System.lineSeparator() + "transformedEvidence="
                + observation.getTransformedEvidence() + System.lineSeparator() + "originalPlan="
                + observation.getOriginalPlan() + System.lineSeparator() + "transformedPlan="
                + observation.getTransformedPlan() + System.lineSeparator() + "originalQuery="
                + testCase.getOriginalQuery() + System.lineSeparator() + "targetDependencies="
                + testCase.getTargetDependencies() + System.lineSeparator() + "leftDependencies="
                + testCase.getLeftDependencies() + System.lineSeparator() + "rightDependencies="
                + testCase.getRightDependencies() + System.lineSeparator() + "transformedQuery="
                + testCase.getTransformedQuery() + System.lineSeparator() + "originalSample="
                + comparison.getOriginalSample() + System.lineSeparator() + "transformedSample="
                + comparison.getTransformedSample() + System.lineSeparator() + "comparisonReason="
                + comparison.getReason() + System.lineSeparator() + "history="
                + history.formatSummary() + System.lineSeparator() + "generationInfo="
                + testCase.getGenerationInfo() + System.lineSeparator() + "materializationSetup="
                + testCase.getMaterializationSetup();
    }

    private void cleanupSeed(TiDBVerticalPartitioningOracle.EcstSeed seed) {
        if (seed == null) {
            return;
        }
        try {
            vp.cleanupEcstSeed(seed);
        } catch (Exception | AssertionError failure) {
            recordFailure(EcstPhase.CLEANUP, seed, null, null, failure);
        }
    }

    private static boolean isRecoverable(Throwable failure) {
        if (failure instanceof IgnoreMeException) {
            return true;
        }
        String message = failure == null ? "" : String.valueOf(failure.getMessage()).toLowerCase(java.util.Locale.ROOT);
        if (failure instanceof java.sql.SQLException) {
            return isExpectedDatabaseMessage(message);
        }
        if (failure instanceof AssertionError) {
            return false;
        }
        return message.contains("unsupported") || message.contains("syntax") || message.contains("cannot create")
                || message.contains("unknown storage") || message.contains("timeout");
    }

    private static boolean isExpectedDatabaseMessage(String message) {
        return message.contains("unsupported") || message.contains("doesn't support")
                || message.contains("syntax") || message.contains("cannot create")
                || message.contains("unknown storage") || message.contains("unknown column")
                || message.contains("duplicate key") || message.contains("data truncation")
                || message.contains("out of range") || message.contains("division by 0");
    }

    private void recordFailure(EcstPhase phase, TiDBVerticalPartitioningOracle.EcstSeed seed, EcstTarget target,
            EcstTransformationConfig config, Throwable failure) {
        recordFailure(phase, seed, target, config, null, failure);
    }

    private void recordFailure(EcstPhase phase, TiDBVerticalPartitioningOracle.EcstSeed seed, EcstTarget target,
            EcstTransformationConfig config, TiDBVerticalPartitioningOracle.EcstCase testCase, Throwable failure) {
        failureRecords++;
        String stack = stackTrace(failure);
        List<String> setup = testCase == null ? List.of() : testCase.getMaterializationSetup();
        String baseTable = testCase == null ? seed == null ? "" : seed.getBaseTable() : testCase.getBaseTable();
        String original = testCase == null ? seed == null ? "" : seed.getOriginalQuery() : testCase.getOriginalQuery();
        String transformed = testCase == null ? "" : testCase.getTransformedQuery();
        String expression = testCase == null ? seed == null ? "" : seed.getTargetExpression()
                : testCase.getTargetExpression();
        String generationInfo = testCase == null ? "" : testCase.getGenerationInfo();
        EcstFailureRecord record = new EcstFailureRecord(phase, target == null ? "" : target.getKey(),
                config == null ? "" : config.stableKey(), baseTable, original, transformed, expression,
                failure == null ? "" : failure.getMessage(), stack, setup, generationInfo);
        appendJsonLine("ecst-failures.jsonl", record.toJsonLine());
    }

    private void writeCaseRecord(EcstTarget target, EcstTransformationConfig config,
            TiDBVerticalPartitioningOracle.EcstCase testCase, EcstObservation observation,
            TiDBVerticalPartitioningOracle.EcstComparison comparison,
            TiDBVerticalPartitioningOracle.EcstSemanticPreconditionResult precondition) {
        String status = observation == null ? "UNKNOWN" : observation.getStatus().name();
        String reason = observation == null ? "case did not reach plan analysis" : observation.getReason();
        String originalPlan = observation == null ? "" : observation.getOriginalPlan();
        String transformedPlan = observation == null ? "" : observation.getTransformedPlan();
        String shiftStatus = observation == null ? "UNKNOWN" : observation.getStatus().name();
        String comparisonStatus = comparison == null ? "NOT_RUN"
                : comparison.isComparable() ? comparison.isEqual() ? "COMPARABLE_EQUAL" : "COMPARABLE_MISMATCH"
                        : "INCOMPARABLE";
        String preconditionStatus = precondition == null ? "VALID" : precondition.isValid() ? "VALID" : "INVALID";
        if (comparison != null && !comparison.isComparable()) {
            status = "UNKNOWN";
            reason = comparison.getReason();
        }
        String observedShifts = observation == null ? "[]"
                : EcstFailureRecord.jsonArray(new ArrayList<>(observation.getObservedShifts()));
        String lineage = testCase == null || testCase.getOperation() == null ? "{}"
                : EcstFailureRecord.json(String.valueOf(testCase.getOperation().getDependencyLineage()));
        String caseId = runId + "-" + (++caseSequence);
        String line = "{\"type\":\"case\",\"target\":\"" + EcstFailureRecord.json(target.getKey())
                + "\",\"config\":\"" + EcstFailureRecord.json(config.stableKey()) + "\",\"status\":\""
                + EcstFailureRecord.json(status) + "\",\"reason\":\"" + EcstFailureRecord.json(reason)
                + "\",\"runId\":" + runId + ",\"workerId\":\""
                + EcstFailureRecord.json(Thread.currentThread().getName()) + "\",\"caseId\":\"" + caseId
                + "\",\"sourceCommit\":\"" + EcstFailureRecord.json(System.getProperty("git.commit", "UNKNOWN"))
                + "\",\"shiftStatus\":\"" + shiftStatus + "\",\"comparisonStatus\":\""
                + comparisonStatus + "\",\"preconditionStatus\":\"" + preconditionStatus
                + "\",\"observedShifts\":" + observedShifts
                + ",\"originalQuery\":\"" + EcstFailureRecord.json(testCase.getOriginalQuery())
                + "\",\"transformedQuery\":\"" + EcstFailureRecord.json(testCase.getTransformedQuery())
                + "\",\"targetExpression\":\"" + EcstFailureRecord.json(testCase.getTargetExpression())
                + "\",\"dependencies\":" + EcstFailureRecord.jsonArray(testCase.getTargetDependencies())
                + ",\"lineage\":\"" + lineage
                + "\",\"originalPlan\":\"" + EcstFailureRecord.json(originalPlan)
                + "\",\"transformedPlan\":\"" + EcstFailureRecord.json(transformedPlan)
                + "\",\"originalMatchedNode\":\""
                + EcstFailureRecord.json(observation == null ? "" : observation.getOriginalMatchedNode())
                + "\",\"transformedMatchedNode\":\""
                + EcstFailureRecord.json(observation == null ? "" : observation.getTransformedMatchedNode())
                + "\",\"originalEvidence\":\""
                + EcstFailureRecord.json(observation == null ? "" : observation.getOriginalEvidence())
                + "\",\"transformedEvidence\":\""
                + EcstFailureRecord.json(observation == null ? "" : observation.getTransformedEvidence())
                + "\",\"generationInfo\":\"" + EcstFailureRecord.json(testCase.getGenerationInfo())
                + "\",\"setup\":" + EcstFailureRecord.jsonArray(testCase.getMaterializationSetup()) + "}";
        appendJsonLine("ecst-cases.jsonl", line);
    }

    private void writeSeedSkipRecord(TiDBVerticalPartitioningOracle.EcstSeed seed,
            TiDBExecutionContextAnalyzer.ParsedContext parsed, int validTargetCount, List<String> reasons) {
        StringBuilder line = new StringBuilder("{\"type\":\"seed_skip\"");
        line.append(",\"runId\":").append(runId).append(",\"workerId\":\"")
                .append(EcstFailureRecord.json(Thread.currentThread().getName())).append("\",\"caseId\":\"")
                .append(runId).append("-").append(++caseSequence).append("\"");
        line.append(",\"seedClause\":\"").append(EcstFailureRecord.json(seed.getExpressionContext().name()));
        line.append("\",\"seedMatchStatus\":\"").append(EcstFailureRecord.json(parsed.extractionStatus().name()));
        line.append("\",\"seedMatchReason\":\"").append(EcstFailureRecord.json(parsed.reason()));
        line.append("\",\"validTargetCount\":").append(validTargetCount);
        line.append(",\"candidateRejectReasons\":").append(EcstFailureRecord.jsonArray(reasons));
        line.append(",\"targetExpression\":\"").append(EcstFailureRecord.json(seed.getTargetExpression())).append("\"");
        line.append(",\"dependencies\":").append(EcstFailureRecord.jsonArray(seed.getTargetDependencies()));
        line.append(",\"candidateExpressions\":").append(EcstFailureRecord.jsonArray(parsed.candidateExpressions()));
        line.append(",\"originalPlan\":\"").append(EcstFailureRecord.json(parsed.plan())).append("\"");
        line.append(",\"originalQuery\":\"").append(EcstFailureRecord.json(seed.getOriginalQuery())).append("\"}");
        appendJsonLine("ecst-cases.jsonl", line.toString());
    }

    private void appendJsonLine(String fileName, String line) {
        File directory = new File(Main.LOG_DIRECTORY, "tidb");
        if (!directory.exists() && !directory.mkdirs()) {
            return;
        }
        try (FileWriter writer = new FileWriter(new File(directory, fileName), true)) {
            writer.write(line);
            writer.write(System.lineSeparator());
        } catch (IOException ignored) {
            // Diagnostics must never alter oracle behavior.
        }
    }

    private static String stackTrace(Throwable failure) {
        if (failure == null) {
            return "";
        }
        StringWriter text = new StringWriter();
        failure.printStackTrace(new PrintWriter(text));
        return text.toString();
    }

    private void writeFeedbackSummary() {
        File directory = new File(Main.LOG_DIRECTORY, "tidb");
        if (!directory.exists() && !directory.mkdirs()) {
            return;
        }
        File file = new File(directory, "ecst-stats.log");
        try (FileWriter writer = new FileWriter(file, true)) {
            writer.write("-- [ecst-stats]" + System.lineSeparator());
            writer.write("candidateSynthesis=" + lastSynthesisSummary + System.lineSeparator());
            writer.write("semanticPreconditionRejects=" + semanticPreconditionRejects + System.lineSeparator());
            writer.write("requestedBudget=" + requestedBudget + System.lineSeparator());
            writer.write("runId=" + runId + System.lineSeparator());
            writer.write("caseRecordsWritten=" + caseSequence + System.lineSeparator());
            writer.write("completedCases=" + completedCases + System.lineSeparator());
            writer.write("attempts=" + attempts + System.lineSeparator());
            writer.write("candidateSetSkips=" + candidateSetSkips + System.lineSeparator());
            writer.write("creationSkips=" + creationSkips + System.lineSeparator());
            writer.write("executionFailures=" + executionFailures + System.lineSeparator());
            writer.write("comparisonSkips=" + comparisonSkips + System.lineSeparator());
            writer.write("failureRecords=" + failureRecords + System.lineSeparator());
            writer.write("bugCandidates=" + bugCandidates + System.lineSeparator());
            writer.write("seedMappingSkips=" + seedMappingSkips + System.lineSeparator());
            writer.write("noValidTargetSkips=" + noValidTargetSkips + System.lineSeparator());
            writer.write("noConfigurationSkips=" + noConfigurationSkips + System.lineSeparator());
            writer.write("seedRefreshes=" + seedRefreshes + System.lineSeparator());
            writer.write("budgetComplete=" + (completedCases >= requestedBudget) + System.lineSeparator());
            writer.write(history.formatSummary());
        } catch (IOException ignored) {
            // Feedback logging must never turn a completed test into a DBMS failure.
        }
    }
}
