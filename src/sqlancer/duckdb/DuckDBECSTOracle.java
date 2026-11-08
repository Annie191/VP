package sqlancer.duckdb;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.oracle.ecst.EcstContext;
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstFailureRecord;
import sqlancer.common.oracle.ecst.EcstFeature;
import sqlancer.common.oracle.ecst.EcstHistory;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstPhase;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.common.oracle.ecst.EcstTransformationConfig;
import sqlancer.duckdb.DuckDBExecutionContextAnalyzer.ParsedContext;
import sqlancer.duckdb.DuckDBProvider.DuckDBGlobalState;
import sqlancer.duckdb.DuckDBVerticalPartitioningOracle.EcstCase;
import sqlancer.duckdb.DuckDBVerticalPartitioningOracle.EcstComparison;
import sqlancer.duckdb.DuckDBVerticalPartitioningOracle.EcstSeed;
import sqlancer.duckdb.DuckDBVerticalPartitioningOracle.EcstSemanticPreconditionResult;

/** Target-directed DuckDB ECST, using controlled VP construction and an independent result oracle. */
public final class DuckDBECSTOracle implements TestOracle<DuckDBGlobalState> {
    private static final Object LOG_LOCK = new Object();
    private final DuckDBGlobalState state;
    private final DuckDBVerticalPartitioningOracle vp;
    private final DuckDBExecutionContextAnalyzer analyzer;
    private final EcstHistory history = new EcstHistory();
    private final List<EcstTarget> targets = DuckDBEcstCapabilities.targets();
    private long issuedSeeds;
    private int completedCases;
    private long attemptedSeeds;
    private long materializationRejects;
    private long preconditionRejects;
    private long equalCases;
    private long incomparableCases;
    private long nonReproducedMismatches;

    public DuckDBECSTOracle(DuckDBGlobalState state) {
        this.state = state;
        this.vp = new DuckDBVerticalPartitioningOracle(state, false);
        this.analyzer = new DuckDBExecutionContextAnalyzer(state);
    }

    public int getCompletedCases() {
        return completedCases;
    }

    public EcstHistory getHistory() {
        return history;
    }

    @Override
    public void check() throws Exception {
        synchronized (DuckDBVerticalPartitioningOracle.ecstExecutionLock()) {
            checkWithRunPermit();
        }
    }

    private void checkWithRunPermit() throws Exception {
        int budget = Math.max(1, state.getDbmsSpecificOptions().ecstQueryPairs);
        completedCases = 0;
        attemptedSeeds = 0;
        materializationRejects = 0;
        preconditionRejects = 0;
        equalCases = 0;
        incomparableCases = 0;
        nonReproducedMismatches = 0;
        try {
            for (int attempt = 0; completedCases < budget && attempt < budget * 8; attempt++) {
                attemptedSeeds++;
                EcstSeed seed = null;
                boolean keepArtifacts = false;
                EcstPhase phase = EcstPhase.SEED_GENERATION;
                EcstTarget target = null;
                EcstTransformationConfig config = null;
                EcstCase testCase = null;
                long started = System.nanoTime();
                try {
                    if (issuedSeeds > 0 && issuedSeeds % 8 == 0) {
                        vp.refreshEcstBaseRelations();
                    }
                    issuedSeeds++;
                    seed = vp.createEcstSeed();
                    ParsedContext seedPlan = analyzer.extractContext(seed.getOriginalQuery(), seed.getOperation(),
                            Set.of(seed.getSourceTable()));
                    phase = EcstPhase.CONFIGURATION_SYNTHESIS;
                    Map<String, List<EcstTransformationConfig>> candidates = candidateSets(seed, seedPlan.context());
                    if (candidates.isEmpty()) {
                        writeJson("{\"type\":\"seed_skip\",\"database\":" + json(state.getDatabaseName())
                                + ",\"query\":" + json(seed.getOriginalQuery()) + ",\"reason\":"
                                + json(seedPlan.getEvidence()) + "}");
                        continue;
                    }
                    EcstFeature feature = seed.features();
                    Map<String, List<String>> candidateKeys = new LinkedHashMap<>();
                    candidates.forEach((key, values) -> candidateKeys.put(key,
                            values.stream().map(EcstTransformationConfig::stableKey).collect(Collectors.toList())));
                    phase = EcstPhase.TARGET_SELECTION;
                    String selected = history.chooseTargetDecision(candidateKeys, feature).getKey();
                    target = targets.stream().filter(t -> t.getKey().equals(selected)).findFirst()
                            .orElseThrow(IllegalStateException::new);
                    List<EcstTransformationConfig> remaining = new ArrayList<>(candidates.get(selected));
                    for (int configurationAttempt = 0; configurationAttempt < 4
                            && !remaining.isEmpty(); configurationAttempt++) {
                        String chosen = history
                                .chooseTransformDecision(selected, remaining.stream()
                                        .map(EcstTransformationConfig::stableKey).collect(Collectors.toList()), feature)
                                .getKey();
                        config = remaining.stream().filter(c -> c.stableKey().equals(chosen)).findFirst()
                                .orElseThrow(IllegalStateException::new);
                        remaining.remove(config);
                        phase = EcstPhase.MATERIALIZATION;
                        long materializationStarted = System.nanoTime();
                        try {
                            testCase = null;
                            testCase = vp.applyEcstTransformation(seed, config, target);
                            vp.prepareEcstCase(testCase);
                        } catch (SQLException failure) {
                            if (!isExpected(failure)) {
                                throw failure;
                            }
                            materializationRejects++;
                            history.recordMaterializationCost(target.getKey(), config.stableKey(),
                                    System.nanoTime() - materializationStarted, feature);
                            writeFailure(phase, seed, target, config, testCase, failure);
                            continue;
                        }
                        phase = EcstPhase.PRECONDITION_CHECK;
                        EcstSemanticPreconditionResult semantic = vp.checkSemanticPreconditions(testCase);
                        EcstSemanticPreconditionResult data = semantic.isValid()
                                ? vp.validateDataPreconditions(testCase) : semantic;
                        if (!semantic.isValid() || !data.isValid()) {
                            preconditionRejects++;
                            writeJson("{\"type\":\"precondition_reject\",\"config\":" + json(config.stableKey())
                                    + ",\"reason\":" + json(data.toString()) + "}");
                            continue;
                        }
                        phase = EcstPhase.PLAN_ANALYSIS;
                        EcstObservation observation = analyzer.analyze(testCase, target);
                        if (!sameKnownContext(seedPlan.context(), observation.getOriginal())) {
                            writeFailure(phase, seed, target, config, testCase,
                                    new IllegalStateException("source context changed after materialization"));
                            continue;
                        }
                        phase = EcstPhase.RESULT_COMPARISON;
                        EcstComparison comparison = vp.compareEcstCase(testCase);
                        boolean reproduced = false;
                        if (comparison.isComparable()) {
                            completedCases++;
                            if (comparison.isEqual()) {
                                equalCases++;
                            } else {
                                reproduced = vp.validateBugCandidate(testCase);
                                if (!reproduced) {
                                    nonReproducedMismatches++;
                                }
                            }
                        } else {
                            incomparableCases++;
                        }
                        // Unknown is an observation, never a reason to blacklist a transformation.
                        history.observe(target.getKey(), config.stableKey(), observation.getStatus(),
                                observation.getObservedShifts(), System.nanoTime() - started, feature);
                        if (comparison.isComparable() && comparison.getOriginalResultCount() > 0
                                && comparison.getTransformedResultCount() > 0) {
                            history.recordNonEmptyCoverage(observation.getObservedShifts());
                        }
                        List<String> replay = vp.getSourceDataReplay(testCase);
                        writeCase(testCase, target, observation, comparison, reproduced, replay);
                        if (reproduced) {
                            keepArtifacts = true;
                            throw new AssertionError("DuckDB ECST reproduced result mismatch\nTarget: " + target
                                    + "\nConfiguration: " + config + "\nMigration: " + observation.getStatus() + "\n"
                                    + comparison.getReason() + "\nReplay:\n" + String.join(";\n", replay) + ";\n"
                                    + testCase.getOriginalQuery() + ";\n" + testCase.getTransformedQuery()
                                    + ";\nOriginal plan:\n" + observation.getOriginalPlan() + "\nTransformed plan:\n"
                                    + observation.getTransformedPlan()
                                    + "\nMinimization: NOT_ATTEMPTED; causal intervention: INCONCLUSIVE");
                        }
                        break;
                    }
                } catch (IgnoreMeException ignored) {
                    writeJson("{\"type\":\"generation_skip\",\"phase\":" + json(phase.name()) + "}");
                } catch (Exception failure) {
                    if (!isExpected(failure)) {
                        keepArtifacts = seed != null;
                    }
                    writeFailure(phase, seed, target, config, testCase, failure);
                    if (!isExpected(failure)) {
                        throw failure;
                    }
                } finally {
                    if (!keepArtifacts) {
                        vp.cleanupEcstSeed(seed);
                    }
                }
            }
        } finally {
            writeJson("{\"type\":\"summary\",\"database\":" + json(state.getDatabaseName()) + ",\"requestedPairs\":"
                    + budget + ",\"completedPairs\":" + completedCases + ",\"attemptedSeeds\":" + attemptedSeeds
                    + ",\"equalPairs\":" + equalCases + ",\"incomparablePairs\":" + incomparableCases
                    + ",\"materializationRejects\":" + materializationRejects + ",\"preconditionRejects\":"
                    + preconditionRejects + ",\"nonReproducedMismatches\":" + nonReproducedMismatches + ",\"history\":"
                    + json(history.formatSummary()) + "}");
        }
        if (completedCases == 0) {
            throw new IgnoreMeException();
        }
    }

    static Map<String, List<EcstTransformationConfig>> candidateSets(EcstSeed seed, EcstContext context) {
        Map<String, List<EcstTransformationConfig>> result = new LinkedHashMap<>();
        for (EcstTarget target : DuckDBEcstCapabilities.targets()) {
            List<EcstTransformationConfig> configs = DuckDBEcstCapabilities.configurations(seed, context, target);
            if (!configs.isEmpty()) {
                result.put(target.getKey(), configs);
            }
        }
        return result;
    }

    private static boolean sameKnownContext(EcstContext before, EcstContext after) {
        for (EcstContextDimension dimension : EcstContextDimension.values()) {
            String original = before.value(dimension);
            if (!"UNKNOWN".equals(original) && !original.equals(after.value(dimension))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isExpected(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException) {
                SQLException sql = (SQLException) cause;
                String sqlState = sql.getSQLState();
                return "22003".equals(sqlState) || "22012".equals(sqlState) || "57014".equals(sqlState);
            }
        }
        return false;
    }

    private void writeFailure(EcstPhase phase, EcstSeed seed, EcstTarget target, EcstTransformationConfig config,
            EcstCase testCase, Throwable failure) throws IOException {
        List<String> replay = List.of();
        if (!isExpected(failure) && testCase != null && testCase.prepared) {
            try {
                replay = vp.getSourceDataReplay(testCase);
            } catch (Exception unavailable) {
                failure.addSuppressed(unavailable);
            }
        }
        SQLException sql = null;
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException) {
                sql = (SQLException) cause;
                break;
            }
        }
        java.io.StringWriter trace = new java.io.StringWriter();
        failure.printStackTrace(new java.io.PrintWriter(trace));
        writeJson(new EcstFailureRecord(phase, target == null ? "" : target.getKey(),
                config == null ? "" : config.stableKey(), seed == null ? "" : seed.getSourceTable(),
                seed == null ? "" : seed.getOriginalQuery(), testCase == null ? "" : testCase.getTransformedQuery(),
                seed == null ? "" : seed.getOperation().getNormalizedExpression(), failure.toString(),
                sql == null ? "" : sql.getSQLState(), sql == null ? 0 : sql.getErrorCode(), trace.toString(), replay,
                "DuckDB").toJsonLine());
    }

    private void writeCase(EcstCase testCase, EcstTarget target, EcstObservation observation, EcstComparison comparison,
            boolean reproduced, List<String> replay) throws Exception {
        writeJson("{\"type\":\"case\",\"database\":" + json(state.getDatabaseName()) + ",\"target\":"
                + json(target.getKey()) + ",\"config\":" + json(testCase.getAppliedConfig().stableKey())
                + ",\"originalQuery\":" + json(testCase.getOriginalQuery()) + ",\"transformedQuery\":"
                + json(testCase.getTransformedQuery()) + ",\"operation\":"
                + json(testCase.getOperation().getNormalizedExpression()) + ",\"sourceRows\":"
                + testCase.seed.getSourceRowCount() + ",\"migration\":" + json(observation.getStatus().name())
                + ",\"observedShifts\":" + EcstFailureRecord.jsonArray(new ArrayList<>(observation.getObservedShifts()))
                + ",\"originalEvidence\":" + json(observation.getOriginalEvidence()) + ",\"transformedEvidence\":"
                + json(observation.getTransformedEvidence()) + ",\"originalPlan\":"
                + json(observation.getOriginalPlan()) + ",\"transformedPlan\":" + json(observation.getTransformedPlan())
                + ",\"comparable\":" + comparison.isComparable() + ",\"equal\":" + comparison.isEqual()
                + ",\"originalRows\":" + comparison.getOriginalResultCount() + ",\"transformedRows\":"
                + comparison.getTransformedResultCount() + ",\"result\":" + json(comparison.getReason())
                + ",\"querySettings\":" + json("") + ",\"replayQueries\":"
                + EcstFailureRecord
                        .jsonArray(List.of(testCase.getOriginalQuery() + "", testCase.getTransformedQuery() + ""))
                + ",\"executionMode\":\"SINGLE_NODE\",\"reproduced\":" + reproduced
                + ",\"evidenceLevel\":\"PLAN_ONLY\",\"sourceDataReplay\":" + EcstFailureRecord.jsonArray(replay) + "}");
    }

    private void writeJson(String line) throws IOException {
        Path directory = new File(Main.LOG_DIRECTORY, "duckdb").toPath();
        synchronized (LOG_LOCK) {
            Files.createDirectories(directory);
            Files.write(
                    directory.resolve("ecst-" + state.getDatabaseName().replaceAll("[^a-zA-Z0-9_-]", "_") + ".jsonl"),
                    (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        }
    }

    private static String json(String value) {
        return "\"" + EcstFailureRecord.json(value) + "\"";
    }
}
