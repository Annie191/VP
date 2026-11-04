package sqlancer.tidb.oracle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
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
import sqlancer.common.oracle.ecst.EcstHistoricalRule;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstPhase;
import sqlancer.common.oracle.ecst.EcstRuleLibrary;
import sqlancer.common.oracle.ecst.EcstShiftStatus;
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
    private static volatile String cachedSourceCommit;
    private final TiDBGlobalState state;
    private final TiDBVerticalPartitioningOracle vp;
    private final TiDBExecutionContextAnalyzer analyzer;
    private final TiDBEcstCapabilities capabilities;
    private final List<EcstHistoricalRule> historicalRules = TiDBEcstRuleLibrary.historicalRules();
    private final EcstHistory history = new EcstHistory();
    private final List<EcstTarget> targets;
    private final EcstSeedPool seedPool;
    private volatile String lastSynthesisSummary = "<none>";
    private long semanticPreconditionRejects;
    private int requestedBudget;
    private int completedCases;
    private int attempts;
    private int candidateSetSkips;
    private int creationSkips;
    private int generationExpectedSkips;
    private int executionFailures;
    private int bugCandidates;
    private int comparisonSkips;
    private int comparableComparisons;
    private int equalComparisons;
    private int incomparableComparisons;
    private int failureRecords;
    private int seedMappingSkips;
    private int seedExtractionAttempts;
    private int seedExtractionMatched;
    private int seedExtractionNotFound;
    private int seedExtractionAbsorbed;
    private int noValidTargetSkips;
    private int noConfigurationSkips;
    private int seedRefreshes;
    private int seedRefreshAttempts;
    private int seedRefreshExpectedSkips;
    private long rootCopCandidateGenerated;
    private long rootCopPreconditionAccepted;
    private long rootCopExplainAnalyzed;
    private long rootCopMigrationSuccess;
    private long runId;
    private int caseSequence;
    private int caseRecordsWritten;
    private int seedSkipRecordsWritten;
    private volatile String lastCommittedCaseId = "";
    private volatile String cachedDbVersion;
    private final List<String> candidateRejectReasons = new ArrayList<>();
    private static final int MAX_CONFIG_ATTEMPTS_PER_SEED = 4;
    private static final int LOW_REACHABILITY_FAILURE_THRESHOLD = 3;
    private static final Object JSON_LOG_LOCK = new Object();
    private final Map<String, Integer> lowReachabilityFailures = new LinkedHashMap<>();
    private final Map<String, Integer> materializationRejectCounts = new LinkedHashMap<>();
    private int lowReachabilitySuppressed;
    private int materializationRejections;
    private int materializationRejectsSuppressed;
    private long seedGenerationCostNanos;
    private long candidateSynthesisCostNanos;
    private long materializationCostNanos;
    private long planAnalysisCostNanos;
    private long resultComparisonCostNanos;

    public TiDBECSTOracle(TiDBGlobalState state) {
        this.state = state;
        this.capabilities = new TiDBEcstCapabilities(state);
        this.targets = capabilities.supportedTargets(state);
        this.vp = new TiDBVerticalPartitioningOracle(state, false, false,
                TiDBVerticalPartitioningOracle.ecstEmptyBaseTableProbabilityForTest());
        this.analyzer = new TiDBExecutionContextAnalyzer(state);
        this.seedPool = new EcstSeedPool(vp);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (runId != 0) {
                writeFeedbackSummary("FINAL_SHUTDOWN");
            }
        }, "tidb-ecst-final-snapshot"));
    }

    static List<EcstTarget> defaultTargets() {
        return targetCatalog().stream().filter(target -> !target.getKey().contains("TIFLASH")).collect(Collectors.toList());
    }

    /** Active target catalog used by target selection; auxiliary shifts remain history observations only. */
    static List<EcstTarget> targetCatalog() {
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
                        "INDEXLOOKUP", "TABLEFULLSCAN"),
                new EcstTarget("TIDB_ACCESS_TABLEFULLSCAN_TO_INDEXREADER", EcstContextDimension.ACCESS_PATH,
                        "TABLEFULLSCAN", "INDEXREADER"),
                new EcstTarget("TIDB_ACCESS_INDEXREADER_TO_TABLEFULLSCAN", EcstContextDimension.ACCESS_PATH,
                        "INDEXREADER", "TABLEFULLSCAN"),
                new EcstTarget("TIDB_OPERATOR_SELECTION_TO_MERGEJOIN", EcstContextDimension.OPERATOR,
                        "SELECTION", "MERGEJOIN"),
                new EcstTarget("TIDB_OPERATOR_MERGEJOIN_TO_SELECTION", EcstContextDimension.OPERATOR,
                        "MERGEJOIN", "SELECTION"));
    }

    static List<EcstTarget> allTargets() {
        return targetCatalog();
    }

    @Override
    public void check() throws Exception {
        try {
            checkInternal();
        } finally {
            writeFeedbackSummary("FINAL");
        }
    }

    private void checkInternal() throws Exception {
        TiDBOptions options = state.getDbmsSpecificOptions();
        int budget = Math.max(1, options.ecstQueryPairs);
        requestedBudget = budget;
        runId = RUN_IDS.incrementAndGet();
        caseSequence = 0;
        caseRecordsWritten = 0;
        seedSkipRecordsWritten = 0;
        lastCommittedCaseId = "";
        completedCases = 0;
        int completed = 0;
        attempts = 0;
        candidateSetSkips = 0;
        creationSkips = 0;
        generationExpectedSkips = 0;
        executionFailures = 0;
        bugCandidates = 0;
        comparisonSkips = 0;
        comparableComparisons = 0;
        equalComparisons = 0;
        incomparableComparisons = 0;
        failureRecords = 0;
        seedMappingSkips = 0;
        seedExtractionAttempts = 0;
        seedExtractionMatched = 0;
        seedExtractionNotFound = 0;
        seedExtractionAbsorbed = 0;
        noValidTargetSkips = 0;
        noConfigurationSkips = 0;
        seedRefreshes = 0;
        seedRefreshAttempts = 0;
        seedRefreshExpectedSkips = 0;
        lowReachabilityFailures.clear();
        materializationRejectCounts.clear();
        lowReachabilitySuppressed = 0;
        materializationRejections = 0;
        materializationRejectsSuppressed = 0;
        seedGenerationCostNanos = 0;
        candidateSynthesisCostNanos = 0;
        materializationCostNanos = 0;
        planAnalysisCostNanos = 0;
        resultComparisonCostNanos = 0;
        candidateRejectReasons.clear();
        semanticPreconditionRejects = 0;
        while (completed < budget && attempts < budget * 4) {
            attempts++;
            long started = System.nanoTime();
            TiDBVerticalPartitioningOracle.EcstSeed seed = null;
            EcstContext seedContext;
            TiDBExecutionContextAnalyzer.ParsedContext seedParsed;
            long seedPhaseStarted = System.nanoTime();
            try {
                seed = seedPool.nextSeed();
                seedParsed = analyzer.extractContext(seed.getOriginalQuery(), seed.getOperation());
                seedContext = seedParsed.context();
                seedExtractionAttempts++;
                switch (seedParsed.extractionStatus()) {
                case MATCHED:
                    seedExtractionMatched++;
                    break;
                case EXPRESSION_NOT_FOUND:
                    seedExtractionNotFound++;
                    break;
                case ABSORBED_IN_INDEX_RANGE:
                    seedExtractionAbsorbed++;
                    break;
                default:
                    break;
                }
            } catch (IgnoreMeException | IllegalArgumentException ignored) {
                creationSkips++;
                generationExpectedSkips++;
                writeExpectedGenerationSkip(seed, ignored);
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
            seedGenerationCostNanos += System.nanoTime() - seedPhaseStarted;
            EcstTarget target = null;
            EcstTransformationConfig config = null;
            Map<String, List<EcstTransformationConfig>> candidateSets = null;
            long synthesisStarted = System.nanoTime();
            try {
                List<EcstTarget> validTargets = supportedTargets(seed, seedContext);
                if (seedParsed.extractionStatus() == TiDBExecutionContextAnalyzer.ExtractionStatus.EXPRESSION_NOT_FOUND
                        || seedParsed.extractionStatus() == TiDBExecutionContextAnalyzer.ExtractionStatus.EXPLAIN_ERROR
                        || seedParsed.extractionStatus() == TiDBExecutionContextAnalyzer.ExtractionStatus.AMBIGUOUS) {
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
                candidateSets = synthesizeCandidateSets(seed, seedContext,
                        validTargets);
                if (candidateSets.isEmpty()) {
                    noConfigurationSkips++;
                    candidateSetSkips++;
                    writeSeedSkipRecord(seed, seedParsed, validTargets.size(), candidateRejectReasons);
                    cleanupSeed(seed);
                    continue;
                }
                target = chooseTarget(candidateSets, featureForSeed(seed, seedContext));
            } catch (Exception | AssertionError failure) {
                recordFailure(config == null ? EcstPhase.CONFIGURATION_SYNTHESIS : EcstPhase.TARGET_SELECTION,
                        seed, target, config, failure);
                cleanupSeed(seed);
                if (isRecoverable(failure)) continue;
                throw failure;
            }
            candidateSynthesisCostNanos += System.nanoTime() - synthesisStarted;
            EcstFeature feature = featureForSeed(seed, seedContext);
            TiDBVerticalPartitioningOracle.EcstCase testCase = null;
            List<EcstTransformationConfig> remaining = new ArrayList<>(candidateSets.get(target.getKey()));
            int candidateAttempts = 0;
            while (testCase == null && !remaining.isEmpty()
                    && candidateAttempts++ < MAX_CONFIG_ATTEMPTS_PER_SEED) {
                config = chooseConfig(target, remaining, feature);
                remaining.remove(config);
                long materializationStarted = System.nanoTime();
                TiDBVerticalPartitioningOracle.EcstCase candidate;
                try {
                    candidate = vp.applyEcstTransformation(seed, config, target);
                } catch (IgnoreMeException | IllegalArgumentException ignored) {
                    creationSkips++;
                    recordMaterializationRejection(seed, target, config, null, ignored, feature,
                            System.nanoTime() - materializationStarted);
                    continue;
                } catch (Exception failure) {
                    if (isRecoverable(failure)) {
                        recordMaterializationRejection(seed, target, config, null, failure, feature,
                                System.nanoTime() - materializationStarted);
                        continue;
                    }
                    recordFailure(EcstPhase.MATERIALIZATION, seed, target, config, failure);
                    cleanupSeed(seed);
                    throw failure;
                } catch (AssertionError failure) {
                    recordFailure(EcstPhase.MATERIALIZATION, seed, target, config, failure);
                    cleanupSeed(seed);
                    throw failure;
                }
                try {
                    // VIEW-backed candidates must exist before metadata and semantic checks
                    // inspect the transformed relation.
                    vp.prepareEcstCase(candidate);
                } catch (Exception | AssertionError failure) {
                    if (isRecoverable(failure)) {
                        recordMaterializationRejection(seed, target, config, candidate, failure, feature,
                                System.nanoTime() - materializationStarted);
                        continue;
                    }
                    recordFailure(EcstPhase.MATERIALIZATION, seed, target, config, candidate, failure);
                    cleanupSeed(seed);
                    throw failure;
                }
                materializationCostNanos += System.nanoTime() - materializationStarted;
                try {
                    EcstContext refreshedSeedContext = analyzer.extractContext(candidate.getOriginalQuery(),
                            candidate.getOperation()).context();
                    boolean rootToCopSelectionTemplate = isRootToCopTarget(target)
                            && seed.getExpressionContext() == EcstTransformationConfig.ExpressionContext.SELECT
                            && "SELECTION_PREDICATE".equals(candidate.getOperation().getExpectedRole())
                            && sameLocationBoundary(seedContext, refreshedSeedContext);
                    if (!sameContext(seedContext, refreshedSeedContext) && !rootToCopSelectionTemplate) {
                        recordFailure(EcstPhase.PLAN_ANALYSIS, seed, target, config,
                                new IllegalStateException("SEED_CONTEXT_CHANGED: seed=" + seedContext + ", refreshed="
                                        + refreshedSeedContext));
                        continue;
                    }
                } catch (Exception failure) {
                    recordFailure(EcstPhase.PLAN_ANALYSIS, seed, target, config, candidate, failure);
                    if (isRecoverable(failure)) continue;
                    cleanupSeed(seed);
                    throw failure;
                }
                TiDBVerticalPartitioningOracle.EcstSemanticPreconditionResult semantic;
                try {
                    semantic = vp.checkSemanticPreconditions(candidate);
                } catch (Exception | AssertionError failure) {
                    recordFailure(EcstPhase.PRECONDITION_CHECK, seed, target, config, candidate, failure);
                    if (isRecoverable(failure)) continue;
                    cleanupSeed(seed);
                    throw failure;
                }
                if (!semantic.isValid()) {
                    semanticPreconditionRejects++;
                    writeCaseRecord(target, config, candidate, null, null, semantic);
                    continue;
                }
                TiDBVerticalPartitioningOracle.EcstSemanticPreconditionResult data = vp
                        .validateDataPreconditions(candidate);
                if (data.getStatus() != TiDBVerticalPartitioningOracle.EcstSemanticPreconditionResult.Status.DATA_VALID) {
                    semanticPreconditionRejects++;
                    writeCaseRecord(target, config, candidate, null, null, data);
                    continue;
                }
                testCase = candidate;
                materializationRejectCounts.remove(materializationKey(target, config, feature));
                if ("TIDB_LOCATION_ROOT_TO_COP_TIKV".equals(target.getKey())) {
                    rootCopPreconditionAccepted++;
                }
            }
            if (testCase == null) {
                noConfigurationSkips++;
                candidateSetSkips++;
                candidateRejectReasons.add(target.getKey() + ": all candidate configurations failed or were exhausted");
                cleanupSeed(seed);
                continue;
            }
            EcstObservation observation;
            long planStarted = System.nanoTime();
            try {
                vp.prepareEcstCase(testCase);
                observation = analyzer.analyze(testCase.getOriginalQuery(), testCase.getTransformedQuery(),
                        testCase.getOperation(), target);
                if ("TIDB_LOCATION_ROOT_TO_COP_TIKV".equals(target.getKey())) {
                    rootCopExplainAnalyzed++;
                }
            } catch (Exception | AssertionError failure) {
                recordFailure(EcstPhase.PLAN_ANALYSIS, seed, target, config, testCase, failure);
                cleanupSeed(seed);
                if (isRecoverable(failure)) continue;
                throw failure;
            }
            planAnalysisCostNanos += System.nanoTime() - planStarted;

            boolean keepArtifacts = false;
            TiDBVerticalPartitioningOracle.EcstComparison comparison = null;
            TiDBVerticalPartitioningOracle.EcstBugValidation bugValidation = null;
            long comparisonStarted = System.nanoTime();
            try {
                comparison = vp.compareEcstCase(testCase);
                if (!comparison.isComparable()) {
                    comparisonSkips++;
                    incomparableComparisons++;
                    continue;
                }
                comparableComparisons++;
                if (comparison.isEqual()) {
                    equalComparisons++;
                }
                // A comparable result comparison, including a mismatch, is a completed test pair.
                completed++;
                completedCases = completed;
                if (!comparison.isEqual()) {
                    keepArtifacts = true;
                    bugValidation = vp.validateBugCandidate(testCase, comparison);
                    if (!bugValidation.isReproduced()) {
                        continue;
                    }
                    bugCandidates++;
                    throw new AssertionError(formatBug(target, testCase, observation, comparison, bugValidation));
                }
            } catch (Exception failure) {
                executionFailures++;
                recordFailure(EcstPhase.RESULT_COMPARISON, seed, target, config, testCase, failure);
                if (isRecoverable(failure)) continue;
                throw failure;
            } finally {
                resultComparisonCostNanos += System.nanoTime() - comparisonStarted;
                writeCaseRecord(target, config, testCase, observation, comparison, null, bugValidation);
                history.observe(target.getKey(), config.stableKey(), observation.getStatus(),
                        observation.getObservedShifts(), System.nanoTime() - started, feature);
                observeReachability(target, config, feature, observation.getStatus());
                if ("TIDB_LOCATION_ROOT_TO_COP_TIKV".equals(target.getKey())
                        && observation.getStatus() == EcstShiftStatus.SUCCESS) {
                    rootCopMigrationSuccess++;
                }
                if (observation.getStatus() == EcstShiftStatus.SUCCESS && comparison != null
                        && comparison.isComparable() && comparison.getOriginalResultCount() > 0
                        && comparison.getTransformedResultCount() > 0) {
                    history.recordNonEmptyCoverage(observation.getObservedShifts());
                }
                lastCommittedCaseId = runId + "-" + caseSequence;
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

    private static boolean sameLocationBoundary(EcstContext before, EcstContext after) {
        return before != null && after != null && before.getLocation().equals(after.getLocation())
                && before.getBoundary().equals(after.getBoundary());
    }

    private static boolean isRootToCopTarget(EcstTarget target) {
        if (target == null) return false;
        if (target.getDimension() == EcstContextDimension.LOCATION) {
            return "ROOT".equals(target.getFrom()) && ("COP_TIKV".equals(target.getTo())
                    || "COP_TIFLASH".equals(target.getTo()));
        }
        return target.getDimension() == EcstContextDimension.BOUNDARY
                && "NON_PUSHDOWN".equals(target.getFrom()) && "PUSHDOWN".equals(target.getTo());
    }

    private boolean isLowReachabilitySuppressed(EcstTarget target,
            EcstTransformationConfig config, TiDBVerticalPartitioningOracle.EcstSeed seed) {
        String key = reachabilityKey(target, config, featureForSeed(seed));
        return lowReachabilityFailures.getOrDefault(key, 0) >= LOW_REACHABILITY_FAILURE_THRESHOLD;
    }

    private boolean isMaterializationSuppressed(EcstTarget target,
            EcstTransformationConfig config, TiDBVerticalPartitioningOracle.EcstSeed seed) {
        String key = materializationKey(target, config, featureForSeed(seed));
        return materializationRejectCounts.getOrDefault(key, 0) >= LOW_REACHABILITY_FAILURE_THRESHOLD;
    }

    private void observeReachability(EcstTarget target, EcstTransformationConfig config, EcstFeature feature,
            EcstShiftStatus status) {
        String key = reachabilityKey(target, config, feature);
        if (status == EcstShiftStatus.SUCCESS) {
            lowReachabilityFailures.remove(key);
            return;
        }
        lowReachabilityFailures.merge(key, 1, Integer::sum);
    }

    private static String reachabilityKey(EcstTarget target, EcstTransformationConfig config,
            EcstFeature feature) {
        return target.getKey() + "|" + config.stableKey() + "|" + feature.key();
    }

    private static String materializationKey(EcstTarget target, EcstTransformationConfig config,
            EcstFeature feature) {
        return target.getKey() + "|" + config.stableKey() + "|" + feature.key();
    }

    private void recordMaterializationRejection(TiDBVerticalPartitioningOracle.EcstSeed seed, EcstTarget target,
            EcstTransformationConfig config, TiDBVerticalPartitioningOracle.EcstCase testCase, Throwable failure,
            EcstFeature feature, long costNanos) {
        materializationRejections++;
        materializationCostNanos += Math.max(0, costNanos);
        materializationRejectCounts.merge(materializationKey(target, config, feature), 1, Integer::sum);
        history.recordMaterializationCost(target.getKey(), config.stableKey(), costNanos, feature);
        recordFailure(EcstPhase.MATERIALIZATION, seed, target, config, testCase, failure, "REJECTED");
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
                seedRefreshAttempts++;
                try {
                    backend.refreshEcstBaseRelations();
                    seedRefreshes++;
                } catch (IgnoreMeException | IllegalArgumentException expected) {
                    seedRefreshExpectedSkips++;
                    generationExpectedSkips++;
                    writeExpectedGenerationSkip(null, expected);
                }
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
            int structuralApplicable = 0;
            int capabilitySupported = 0;
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
                if (!isApplicableToSeed(target, config, shape, seed.getExpressionContext())) {
                    continue;
                }
                if (!isDependencyLayoutApplicable(seed, config)) {
                    continue;
                }
                if (!isStructurallyApplicable(target, config, shape,
                        seed.getDependencyLayout(config.getPartitionLayout()))) {
                    continue;
                }
                if (isLowReachabilitySuppressed(target, config, seed)) {
                    lowReachabilitySuppressed++;
                    continue;
                }
                if (isMaterializationSuppressed(target, config, seed)) {
                    materializationRejectsSuppressed++;
                    continue;
                }
                structuralApplicable++;
                if (!capabilities.supports(config, target, seed, seedContext)) {
                    continue;
                }
                capabilitySupported++;
                applicableCount++;
                applicable.add(config);
            }
            if (!applicable.isEmpty()) {
                result.put(target.getKey(), applicable);
                if ("TIDB_LOCATION_ROOT_TO_COP_TIKV".equals(target.getKey())) {
                    rootCopCandidateGenerated += applicable.size();
                }
            } else {
                candidateRejectReasons.add(target.getKey() + ": no applicable configuration after rule/legal/seed/layout/capability checks");
            }
            synthesis.add(target.getKey() + "{generated=" + generated + ",ruleAccepted=" + ruleAccepted
                    + ",semanticLegal=" + legal + ",structuralApplicable=" + structuralApplicable
                    + ",capabilitySupported=" + capabilitySupported + ",applicable=" + applicableCount
                    + ",allCandidates=" + applicable.size() + "}");
        }
        lastSynthesisSummary = "capabilities=" + capabilities.describe() + "; queryShape=" + shape
                + "; accessPathPreparation=" + seed.getAccessPathPreparation() + "; "
                + "historicalRules=" + historicalRules.size() + ";historicalMatches="
                + historicalRuleMatches(seed, result) + ";historicalMigrations="
                + historicalRuleCount(EcstHistoricalRule.Kind.PRIMARY_TARGET, EcstHistoricalRule.Kind.SECONDARY_MIGRATION)
                + ";historicalStructuralEvidence="
                + historicalRuleCount(EcstHistoricalRule.Kind.STRUCTURAL_PRESERVATION) + "; "
                + String.join("; ", synthesis);
        return result;
    }

    private int historicalRuleCount(EcstHistoricalRule.Kind... kinds) {
        java.util.Set<EcstHistoricalRule.Kind> accepted = java.util.EnumSet.noneOf(EcstHistoricalRule.Kind.class);
        accepted.addAll(java.util.Arrays.asList(kinds));
        return (int) historicalRules.stream().filter(rule -> accepted.contains(rule.getKind())).count();
    }

    private int historicalRuleMatches(TiDBVerticalPartitioningOracle.EcstSeed seed,
            Map<String, List<EcstTransformationConfig>> candidates) {
        int matches = 0;
        for (EcstHistoricalRule rule : historicalRules) {
            List<EcstTransformationConfig> applicable = candidates.get(rule.getTargetKey());
            if (applicable == null || !rule.getConfiguration().contains(seed.getExpressionContext().name())) {
                continue;
            }
            if (applicable.stream().anyMatch(config -> config.stableKey().equals(rule.getConfiguration()))) {
                matches++;
            }
        }
        return matches;
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
        case OPERATOR:
            return "TIDB_OPERATOR_SHIFT";
        default:
            return target.getKey();
        }
    }

    private static boolean isLegal(EcstTarget target, EcstTransformationConfig config, QueryShape shape) {
        if (config.getRepresentation() == EcstTransformationConfig.Representation.ORIGINAL) {
            return false;
        }
        if (config.getRepresentation() == EcstTransformationConfig.Representation.VIEW
                && config.getReconstruction() == EcstTransformationConfig.Reconstruction.JOIN) {
            return false;
        }
        if (config.getRepresentation() == EcstTransformationConfig.Representation.VIEW
                && config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.JOIN_ON) {
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
        if (config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.JOIN_ON) {
            return target.getDimension() == EcstContextDimension.LOCATION
                    || target.getDimension() == EcstContextDimension.BOUNDARY
                    || target.getDimension() == EcstContextDimension.ACCESS_PATH
                    || target.getDimension() == EcstContextDimension.OPERATOR;
        }
        if (config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.HAVING) {
            return shape.hasAggregation;
        }
        return config.getExpressionContext() != EcstTransformationConfig.ExpressionContext.SELECT || shape.hasProjection;
    }

    /** TiDB currently has a VP materializer for these contexts; other legal configs remain visible as future work. */
    private static boolean isApplicableToSeed(EcstTarget target, EcstTransformationConfig config, QueryShape shape,
            EcstTransformationConfig.ExpressionContext seedExpressionContext) {
        if (!shape.hasBaseTable) {
            return false;
        }
        EcstTransformationConfig.ExpressionContext embedded = config.getExpressionContext();
        if (embedded == seedExpressionContext) {
            return true;
        }
        return seedExpressionContext == EcstTransformationConfig.ExpressionContext.WHERE
                && (embedded == EcstTransformationConfig.ExpressionContext.JOIN_ON
                        || embedded == EcstTransformationConfig.ExpressionContext.INNER_WHERE)
                || seedExpressionContext == EcstTransformationConfig.ExpressionContext.SELECT
                        && (isConcreteRootToCopTarget(target)
                                ? embedded == EcstTransformationConfig.ExpressionContext.WHERE
                                        || embedded == EcstTransformationConfig.ExpressionContext.INNER_WHERE
                                : embedded == EcstTransformationConfig.ExpressionContext.INNER_SELECT);
    }

    private static boolean isDependencyLayoutApplicable(TiDBVerticalPartitioningOracle.EcstSeed seed,
            EcstTransformationConfig config) {
        if (config.getRepresentation() == EcstTransformationConfig.Representation.VIEW) {
            return config.getPartitionLayout() != EcstTransformationConfig.PartitionLayout.CROSS_SIDE;
        }
        String actual = seed.getDependencyLayout(config.getPartitionLayout());
        if (config.getPartitionLayout() == EcstTransformationConfig.PartitionLayout.CROSS_SIDE) {
            return "CROSS_SIDE".equals(actual);
        }
        return "SAME_SIDE".equals(actual) || "SINGLE_SIDE".equals(actual);
    }

    /**
     * Checks whether the selected layout can structurally induce the requested
     * direction. Plan analysis remains the final authority on the observed shift.
     */
    private static boolean isStructurallyApplicable(EcstTarget target, EcstTransformationConfig config,
            QueryShape shape, String dependencyLayout) {
        if (target == null || config == null || shape == null || dependencyLayout == null) {
            return false;
        }
        boolean crossSide = "CROSS_SIDE".equals(dependencyLayout);
        if (crossSide && shape.maxDependencies < 2) {
            return false;
        }
        boolean boundaryReconstruction = config.getReconstruction() == EcstTransformationConfig.Reconstruction.JOIN
                || config.getReconstruction() == EcstTransformationConfig.Reconstruction.DERIVED_TABLE
                || config.getReconstruction() == EcstTransformationConfig.Reconstruction.CTE
                || config.getReconstruction() == EcstTransformationConfig.Reconstruction.VIEW;
        if (!boundaryReconstruction) {
            return false;
        }
        switch (target.getDimension()) {
        case LOCATION:
            if (isPushdownLocation(target.getTo())) {
                // Root -> Cop needs a predicate or an inner projection that TiDB can push
                // into one scan. A top-level projection/HAVING/Join-ON shape leaves the
                // expression at the root and is therefore not a useful induction template.
                return !crossSide && isRootToCopInductionContext(target, config.getExpressionContext());
            }
            if (isPushdownLocation(target.getFrom()) && "ROOT".equals(target.getTo())) {
                // A join/derived boundary, including a cross-side dependency, can lift evaluation to Root.
                return boundaryReconstruction;
            }
            return false;
        case BOUNDARY:
            if ("PUSHDOWN".equals(target.getTo())) {
                // Every target input must be available on the pushed-down relation side.
                return !crossSide;
            }
            if ("PUSHDOWN".equals(target.getFrom()) && "NON_PUSHDOWN".equals(target.getTo())) {
                return boundaryReconstruction;
            }
            return false;
        case ACCESS_PATH:
            return !crossSide && (config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.WHERE
                    || config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.INNER_WHERE);
        case OPERATOR:
            return crossSide && (config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.WHERE
                    || config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.JOIN_ON
                    || config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.INNER_WHERE);
        default:
            return false;
        }
    }

    static boolean structurallyApplicableForTest(EcstTarget target, EcstTransformationConfig config,
            String dependencyLayout, QueryShape shape) {
        return isStructurallyApplicable(target, config, shape, dependencyLayout);
    }

    private static boolean isPushdownLocation(String location) {
        return "COP_TIKV".equals(location) || "COP_TIFLASH".equals(location);
    }

    private static boolean isRootToCopInductionContext(
            EcstTransformationConfig.ExpressionContext context) {
        return context == EcstTransformationConfig.ExpressionContext.WHERE
                || context == EcstTransformationConfig.ExpressionContext.INNER_WHERE
                || context == EcstTransformationConfig.ExpressionContext.INNER_SELECT;
    }

    private static boolean isRootToCopInductionContext(EcstTarget target,
            EcstTransformationConfig.ExpressionContext context) {
        if (isConcreteRootToCopTarget(target)) {
            return context == EcstTransformationConfig.ExpressionContext.WHERE
                    || context == EcstTransformationConfig.ExpressionContext.INNER_WHERE;
        }
        return isRootToCopInductionContext(context);
    }

    private static boolean isConcreteRootToCopTarget(EcstTarget target) {
        if (target == null || target.getKey() == null) {
            return false;
        }
        return target.getKey().startsWith("TIDB_LOCATION_ROOT_TO_COP")
                || target.getKey().startsWith("TIDB_PUSHDOWN_ROOT_TO_COP");
    }

    private static boolean isApplicableToTiDb(EcstTransformationConfig config, QueryShape shape) {
        boolean vertical = config.getRepresentation() == EcstTransformationConfig.Representation.VERTICAL_PARTITIONED;
        boolean view = config.getRepresentation() == EcstTransformationConfig.Representation.VIEW;
        return (vertical && (config.getReconstruction() == EcstTransformationConfig.Reconstruction.JOIN
                        || config.getReconstruction() == EcstTransformationConfig.Reconstruction.DERIVED_TABLE
                        || config.getReconstruction() == EcstTransformationConfig.Reconstruction.CTE
                        || config.getReconstruction() == EcstTransformationConfig.Reconstruction.VIEW)
                || view && (config.getReconstruction() == EcstTransformationConfig.Reconstruction.DERIVED_TABLE
                        || config.getReconstruction() == EcstTransformationConfig.Reconstruction.CTE
                        || config.getReconstruction() == EcstTransformationConfig.Reconstruction.VIEW))
                && (config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.WHERE
                        || config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.SELECT
                        || config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.HAVING
                        || config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.JOIN_ON
                        || config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.INNER_WHERE
                        || config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.INNER_SELECT)
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
            EcstFeature feature) {
        if (applicable.isEmpty()) {
            throw new IllegalStateException("No applicable ECST configuration for target " + target.getKey());
        }
        Map<String, EcstFeature> features = new LinkedHashMap<>();
        for (EcstTransformationConfig candidate : applicable) {
            features.put(candidate.stableKey(), feature);
        }
        String chosen = history.chooseTransformDecision(target.getKey(), features).getKey();
        for (EcstTransformationConfig candidate : applicable) {
            if (candidate.stableKey().equals(chosen)) {
                return candidate;
            }
        }
        return applicable.get(state.getRandomly().getInteger(0, applicable.size()));
    }

    private EcstFeature featureForSeed(TiDBVerticalPartitioningOracle.EcstSeed seed) {
        return featureForSeed(seed, null);
    }

    private EcstFeature featureForSeed(TiDBVerticalPartitioningOracle.EcstSeed seed, EcstContext context) {
        String indexAvailability = seed.getAccessPathPreparation() == null ? "UNKNOWN"
                : "SOURCE_INDEX=" + seed.getAccessPathPreparation().isSourceIndexReady()
                        + ",PARTITION_INDEXABLE=" + seed.getAccessPathPreparation().isPartitionIndexable()
                        + ",PREDICATE_SUPPORTED=" + seed.getAccessPathPreparation().isPredicateSupported();
        String dataScale = seed.getBaseRowCount() <= 0 ? "EMPTY"
                : seed.getBaseRowCount() <= 32 ? "SMALL" : seed.getBaseRowCount() <= 256 ? "MEDIUM" : "LARGE";
        String accessPath = context == null ? "UNKNOWN"
                : "LOCATION=" + context.getLocation() + ",BOUNDARY=" + context.getBoundary() + ",ACCESS_PATH="
                        + context.getAccessPath();
        return new EcstFeature("TiDB", seed.getExpressionType(), seed.getExpressionContext().name(),
                seed.getDependencyClass(), seed.getQueryShape(), indexAvailability, dataScale, accessPath);
    }

    private String formatBug(EcstTarget target, TiDBVerticalPartitioningOracle.EcstCase testCase,
            EcstObservation observation,
            TiDBVerticalPartitioningOracle.EcstComparison comparison,
            TiDBVerticalPartitioningOracle.EcstBugValidation bugValidation) {
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
                + comparison.getReason() + System.lineSeparator() + "bugValidation=" + bugValidation
                + System.lineSeparator() + "history="
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
        recordFailure(phase, seed, target, config, testCase, failure, "FAILURE");
    }

    private void recordFailure(EcstPhase phase, TiDBVerticalPartitioningOracle.EcstSeed seed, EcstTarget target,
            EcstTransformationConfig config, TiDBVerticalPartitioningOracle.EcstCase testCase, Throwable failure,
            String outcome) {
        failureRecords++;
        String stack = stackTrace(failure);
        List<String> setup = testCase == null ? List.of() : testCase.getMaterializationSetup();
        String baseTable = testCase == null ? seed == null ? "" : seed.getBaseTable() : testCase.getBaseTable();
        String original = testCase == null ? seed == null ? "" : seed.getOriginalQuery() : testCase.getOriginalQuery();
        String transformed = testCase == null ? "" : testCase.getTransformedQuery();
        String expression = testCase == null ? seed == null ? "" : seed.getTargetExpression()
                : testCase.getTargetExpression();
        String generationInfo = testCase == null ? "" : testCase.getGenerationInfo();
        String sqlState = failure instanceof java.sql.SQLException ? ((java.sql.SQLException) failure).getSQLState() : "";
        int errorCode = failure instanceof java.sql.SQLException ? ((java.sql.SQLException) failure).getErrorCode() : 0;
        EcstFailureRecord record = new EcstFailureRecord(phase, target == null ? "" : target.getKey(),
                config == null ? "" : config.stableKey(), baseTable, original, transformed, expression,
                failure == null ? "" : failure.getMessage(), sqlState, errorCode, stack, setup, generationInfo,
                outcome);
        appendJsonLine("ecst-failures.jsonl", record.toJsonLine());
    }

    private void writeExpectedGenerationSkip(TiDBVerticalPartitioningOracle.EcstSeed seed, Throwable failure) {
        String line = "{\"type\":\"generation_skip\",\"outcome\":\"EXPECTED_GENERATION_SKIP\","
                + "\"runId\":" + runId + ",\"workerId\":\""
                + EcstFailureRecord.json(Thread.currentThread().getName()) + "\",\"message\":\""
                + EcstFailureRecord.json(failure == null ? "" : failure.getMessage()) + "\",\"baseTable\":\""
                + EcstFailureRecord.json(seed == null ? "" : seed.getBaseTable()) + "\",\"targetExpression\":\""
                + EcstFailureRecord.json(seed == null ? "" : seed.getTargetExpression()) + "\"}";
        appendJsonLine("ecst-failures.jsonl", line);
    }

    private void writeCaseRecord(EcstTarget target, EcstTransformationConfig config,
            TiDBVerticalPartitioningOracle.EcstCase testCase, EcstObservation observation,
            TiDBVerticalPartitioningOracle.EcstComparison comparison,
            TiDBVerticalPartitioningOracle.EcstSemanticPreconditionResult precondition) {
        writeCaseRecord(target, config, testCase, observation, comparison, precondition, null);
    }

    private void writeCaseRecord(EcstTarget target, EcstTransformationConfig config,
            TiDBVerticalPartitioningOracle.EcstCase testCase, EcstObservation observation,
            TiDBVerticalPartitioningOracle.EcstComparison comparison,
            TiDBVerticalPartitioningOracle.EcstSemanticPreconditionResult precondition,
            TiDBVerticalPartitioningOracle.EcstBugValidation bugValidation) {
        String status = observation == null ? "UNKNOWN" : observation.getStatus().name();
        String reason = observation == null ? "case did not reach plan analysis" : observation.getReason();
        String originalPlan = observation == null ? "" : observation.getOriginalPlan();
        String transformedPlan = observation == null ? "" : observation.getTransformedPlan();
        String shiftStatus = observation == null ? "UNKNOWN" : observation.getStatus().name();
        String comparisonStatus = comparison == null ? "NOT_RUN"
                : comparison.isComparable() ? comparison.isEqual() ? "COMPARABLE_EQUAL" : "COMPARABLE_MISMATCH"
                        : "INCOMPARABLE";
        String preconditionStatus = precondition == null ? "DATA_VALID" : precondition.getStatus().name();
        if (comparison != null && !comparison.isComparable()) {
            reason = comparison.getReason();
        }
        String observedShifts = observation == null ? "[]"
                : EcstFailureRecord.jsonArray(new ArrayList<>(observation.getObservedShifts()));
        long sourceRowCount = comparison == null ? testCase.getSourceRowCount() : comparison.getSourceRowCount();
        long originalResultCount = comparison == null ? -1 : comparison.getOriginalResultCount();
        long transformedResultCount = comparison == null ? -1 : comparison.getTransformedResultCount();
        String originalAggregation = originalPlan.matches("(?is).*\\b(hashagg|streamagg|aggregation|agg)\\b.*")
                ? "PRESENT" : "ABSENT";
        String transformedAggregation = transformedPlan.matches("(?is).*\\b(hashagg|streamagg|aggregation|agg)\\b.*")
                ? "PRESENT" : "ABSENT";
        String lineage = testCase == null || testCase.getOperation() == null ? "{}"
                : EcstFailureRecord.json(String.valueOf(testCase.getOperation().getDependencyLineage()));
        List<String> sourceDataReplay;
        try {
            sourceDataReplay = vp.getSourceDataReplay(testCase);
        } catch (Exception failure) {
            sourceDataReplay = List.of("-- source data snapshot unavailable: " + failure.getMessage());
        }
        String caseId = runId + "-" + (++caseSequence);
        String line = "{\"type\":\"case\",\"target\":\"" + EcstFailureRecord.json(target.getKey())
                + "\",\"config\":\"" + EcstFailureRecord.json(config.stableKey()) + "\",\"status\":\""
                + EcstFailureRecord.json(status) + "\",\"reason\":\"" + EcstFailureRecord.json(reason)
                + "\",\"runId\":" + runId + ",\"workerId\":\""
                + EcstFailureRecord.json(Thread.currentThread().getName()) + "\",\"caseId\":\"" + caseId
                + "\",\"sourceCommit\":\"" + EcstFailureRecord.json(sourceCommit())
                + "\",\"shiftStatus\":\"" + shiftStatus + "\",\"comparisonStatus\":\""
                + comparisonStatus + "\",\"preconditionStatus\":\"" + preconditionStatus
                + "\",\"bugValidation\":\""
                + EcstFailureRecord.json(bugValidation == null ? "" : bugValidation.toString())
                + "\",\"evidenceLevel\":\""
                + (observation == null ? "PLAN_ONLY" : observation.getEvidenceLevel().name())
                + "\",\"originalResultSample\":\""
                + EcstFailureRecord.json(comparison == null ? "" : comparison.getOriginalSample())
                + "\",\"transformedResultSample\":\""
                + EcstFailureRecord.json(comparison == null ? "" : comparison.getTransformedSample())
                + "\",\"resultDifference\":\""
                + EcstFailureRecord.json(comparison != null && !comparison.isEqual() ? comparison.getReason() : "")
                + "\",\"minimizationStatus\":\""
                + EcstFailureRecord.json(bugValidation == null ? "NOT_ATTEMPTED" : bugValidation.getMinimizationStatus())
                + "\",\"interventionStatus\":\""
                + EcstFailureRecord.json(bugValidation == null ? "INCONCLUSIVE" : bugValidation.getInterventionStatus())
                + "\",\"observedShifts\":" + observedShifts
                + ",\"sourceRowCount\":" + sourceRowCount
                + ",\"originalResultCount\":" + originalResultCount
                + ",\"transformedResultCount\":" + transformedResultCount
                + ",\"originalQuery\":\"" + EcstFailureRecord.json(testCase.getOriginalQuery())
                + "\",\"transformedQuery\":\"" + EcstFailureRecord.json(testCase.getTransformedQuery())
                + "\",\"targetExpression\":\"" + EcstFailureRecord.json(testCase.getTargetExpression())
                + "\",\"dependencies\":" + EcstFailureRecord.jsonArray(testCase.getTargetDependencies())
                + ",\"lineage\":\"" + lineage
                + "\",\"originalPlan\":\"" + EcstFailureRecord.json(originalPlan)
                + "\",\"transformedPlan\":\"" + EcstFailureRecord.json(transformedPlan)
                + "\",\"originalAggregation\":\"" + originalAggregation + "\",\"transformedAggregation\":\""
                + transformedAggregation
                + "\",\"originalMatchedNode\":\""
                + EcstFailureRecord.json(observation == null ? "" : observation.getOriginalMatchedNode())
                + "\",\"transformedMatchedNode\":\""
                + EcstFailureRecord.json(observation == null ? "" : observation.getTransformedMatchedNode())
                + "\",\"originalEvidence\":\""
                + EcstFailureRecord.json(observation == null ? "" : observation.getOriginalEvidence())
                + "\",\"transformedEvidence\":\""
                + EcstFailureRecord.json(observation == null ? "" : observation.getTransformedEvidence())
                + "\",\"generationInfo\":\"" + EcstFailureRecord.json(testCase.getGenerationInfo())
                + "\",\"setup\":" + EcstFailureRecord.jsonArray(testCase.getMaterializationSetup())
                + ",\"sourceDataReplay\":" + EcstFailureRecord.jsonArray(sourceDataReplay)
                + ",\"dbVersion\":\"" + EcstFailureRecord.json(dbVersion()) + "\"}";
        appendJsonLine("ecst-cases.jsonl", line);
        caseRecordsWritten++;
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
        seedSkipRecordsWritten++;
    }

    private void appendJsonLine(String fileName, String line) {
        File directory = new File(Main.LOG_DIRECTORY, "tidb");
        appendJsonLine(directory, fileName, line);
    }

    static void appendJsonLineForTest(File directory, String fileName, String line) {
        appendJsonLine(directory, fileName, line);
    }

    private static void appendJsonLine(File directory, String fileName, String line) {
        if (!directory.exists() && !directory.mkdirs()) {
            return;
        }
        synchronized (JSON_LOG_LOCK) {
            try (FileWriter writer = new FileWriter(new File(directory, fileName), true)) {
                writer.write(line);
                writer.write(System.lineSeparator());
            } catch (IOException ignored) {
                // Diagnostics must never alter oracle behavior.
            }
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

    private static String sourceCommit() {
        if (cachedSourceCommit != null) return cachedSourceCommit;
        String property = System.getProperty("git.commit", "");
        if (!property.isEmpty()) return cachedSourceCommit = property;
        String environment = System.getenv("GIT_COMMIT");
        if (environment != null && !environment.isEmpty()) return cachedSourceCommit = environment;
        try {
            Process process = new ProcessBuilder("git", "rev-parse", "HEAD").redirectErrorStream(true).start();
            if (process.waitFor(2, java.util.concurrent.TimeUnit.SECONDS)) {
                String resolved = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
                if (!resolved.isEmpty()) return cachedSourceCommit = resolved;
            }
        } catch (Exception ignored) {
            // Keep UNKNOWN when the runtime is not a Git checkout.
        }
        return cachedSourceCommit = "UNKNOWN";
    }

    private String dbVersion() {
        if (cachedDbVersion != null) return cachedDbVersion;
        try (java.sql.Statement statement = state.getConnection().createStatement();
                java.sql.ResultSet result = statement.executeQuery("SELECT VERSION()")) {
            cachedDbVersion = result.next() ? result.getString(1) : "UNKNOWN";
            return cachedDbVersion;
        } catch (Exception ignored) {
            return "UNKNOWN";
        }
    }

    private synchronized void writeFeedbackSummary(String snapshotType) {
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
            writer.write("snapshotType=" + snapshotType + System.lineSeparator());
            writer.write("sourceCommit=" + sourceCommit() + System.lineSeparator());
            writer.write("dbVersion=" + dbVersion() + System.lineSeparator());
            writer.write("runParameters=" + System.getProperty("ecst.run.parameters", "UNKNOWN")
                    + System.lineSeparator());
            writer.write("runId=" + runId + System.lineSeparator());
            writer.write("caseRecordsWritten=" + caseRecordsWritten + System.lineSeparator());
            writer.write("seedSkipRecordsWritten=" + seedSkipRecordsWritten + System.lineSeparator());
            writer.write("lastCommittedCaseId=" + lastCommittedCaseId + System.lineSeparator());
            writer.write("totalTrials=" + history.getTotalTrials() + System.lineSeparator());
            writer.write("completedCases=" + completedCases + System.lineSeparator());
            writer.write("attempts=" + attempts + System.lineSeparator());
            writer.write("candidateSetSkips=" + candidateSetSkips + System.lineSeparator());
            writer.write("lowReachabilitySuppressed=" + lowReachabilitySuppressed + System.lineSeparator());
            writer.write("materializationRejections=" + materializationRejections + System.lineSeparator());
            writer.write("materializationRejectsSuppressed=" + materializationRejectsSuppressed
                    + System.lineSeparator());
            writer.write("materializationRejectByConfig=" + materializationRejectCounts + System.lineSeparator());
            writer.write("seedGenerationCostNanos=" + seedGenerationCostNanos + System.lineSeparator());
            writer.write("candidateSynthesisCostNanos=" + candidateSynthesisCostNanos + System.lineSeparator());
            writer.write("materializationCostNanos=" + materializationCostNanos + System.lineSeparator());
            writer.write("planAnalysisCostNanos=" + planAnalysisCostNanos + System.lineSeparator());
            writer.write("resultComparisonCostNanos=" + resultComparisonCostNanos + System.lineSeparator());
            writer.write("creationSkips=" + creationSkips + System.lineSeparator());
            writer.write("generationExpectedSkips=" + generationExpectedSkips + System.lineSeparator());
            writer.write("seedRefreshAttempts=" + seedRefreshAttempts + System.lineSeparator());
            writer.write("seedRefreshExpectedSkips=" + seedRefreshExpectedSkips + System.lineSeparator());
            writer.write("rootCopCandidateGenerated=" + rootCopCandidateGenerated + System.lineSeparator());
            writer.write("rootCopPreconditionAccepted=" + rootCopPreconditionAccepted + System.lineSeparator());
            writer.write("rootCopExplainAnalyzed=" + rootCopExplainAnalyzed + System.lineSeparator());
            writer.write("rootCopMigrationSuccess=" + rootCopMigrationSuccess + System.lineSeparator());
            writer.write("executionFailures=" + executionFailures + System.lineSeparator());
            writer.write("comparisonSkips=" + comparisonSkips + System.lineSeparator());
            writer.write("comparableComparisons=" + comparableComparisons + System.lineSeparator());
            writer.write("equalComparisons=" + equalComparisons + System.lineSeparator());
            writer.write("incomparableComparisons=" + incomparableComparisons + System.lineSeparator());
            writer.write("failureRecords=" + failureRecords + System.lineSeparator());
            writer.write("bugCandidates=" + bugCandidates + System.lineSeparator());
            writer.write("seedMappingSkips=" + seedMappingSkips + System.lineSeparator());
            writer.write("seedExtractionAttempts=" + seedExtractionAttempts + System.lineSeparator());
            writer.write("seedExtractionMatched=" + seedExtractionMatched + System.lineSeparator());
            writer.write("seedExtractionNotFound=" + seedExtractionNotFound + System.lineSeparator());
            writer.write("seedExtractionAbsorbed=" + seedExtractionAbsorbed + System.lineSeparator());
            double seedExtractionSuccessRate = seedExtractionAttempts == 0 ? 0.0
                    : (double) seedExtractionMatched / seedExtractionAttempts;
            writer.write("seedExtractionSuccessRate=" + seedExtractionSuccessRate + System.lineSeparator());
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
