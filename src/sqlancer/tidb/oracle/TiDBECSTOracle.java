package sqlancer.tidb.oracle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.oracle.ecst.EcstContextDimension;
import sqlancer.common.oracle.ecst.EcstHistory;
import sqlancer.common.oracle.ecst.EcstObservation;
import sqlancer.common.oracle.ecst.EcstRuleLibrary;
import sqlancer.common.oracle.ecst.EcstTarget;
import sqlancer.common.oracle.ecst.EcstTransformationConfig;
import sqlancer.tidb.TiDBOptions;
import sqlancer.tidb.TiDBProvider;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTable;

/**
 * TiDB MVP for ECST. It deliberately uses a separate oracle entry point and the controlled VP seam, leaving the
 * existing random VP oracle untouched.
 */
public final class TiDBECSTOracle implements TestOracle<TiDBGlobalState> {
    private final TiDBGlobalState state;
    private final TiDBVerticalPartitioningOracle vp;
    private final TiDBExecutionContextAnalyzer analyzer;
    private final EcstHistory history = new EcstHistory();
    private final List<EcstTarget> targets = Arrays.asList(
            new EcstTarget("TIDB_LOCATION_SHIFT", EcstContextDimension.LOCATION, "*", "*"),
            new EcstTarget("TIDB_PUSHDOWN_SHIFT", EcstContextDimension.BOUNDARY, "*", "*"),
            new EcstTarget("TIDB_ACCESS_PATH_SHIFT", EcstContextDimension.ACCESS_PATH, "*", "*"));
    private volatile String lastSynthesisSummary = "<none>";

    public TiDBECSTOracle(TiDBGlobalState state) {
        this.state = state;
        this.vp = new TiDBVerticalPartitioningOracle(state);
        this.analyzer = new TiDBExecutionContextAnalyzer(state);
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
        int completed = 0;
        int attempts = 0;
        String lastBaseTable = null;
        while (completed < budget && attempts < budget * 4) {
            attempts++;
            Map<String, List<EcstTransformationConfig>> candidateSets = synthesizeCandidateSets();
            if (candidateSets.isEmpty()) {
                continue;
            }
            EcstTarget target = chooseTarget(candidateSets);
            List<EcstTransformationConfig> applicable = candidateSets.get(target.getKey());
            EcstTransformationConfig config = chooseConfig(target, applicable);
            long started = System.nanoTime();
            TiDBVerticalPartitioningOracle.EcstCase testCase;
            try {
                testCase = vp.createEcstCase(config);
            } catch (IgnoreMeException ignored) {
                continue;
            }
            lastBaseTable = testCase.getBaseTable();
            EcstObservation observation = analyzer.analyze(testCase.getOriginalQuery(), testCase.getTransformedQuery(),
                    testCase.getTargetExpression(), target);

            boolean keepArtifacts = false;
            try {
                TiDBVerticalPartitioningOracle.EcstComparison comparison = vp.compareEcstCase(testCase);
                if (!comparison.isEqual()) {
                    keepArtifacts = true;
                    throw new AssertionError(formatBug(target, testCase, observation, comparison));
                }
            } finally {
                history.observe(target.getKey(), config.stableKey(), observation.getStatus(),
                        observation.getObservedShifts(), System.nanoTime() - started);
                if (!keepArtifacts) {
                    vp.cleanupEcstCase(testCase);
                }
            }
            completed++;
        }
        if (completed == 0) {
            throw new IgnoreMeException();
        }
        if (lastBaseTable != null) {
            TiDBProvider.replaceVerticalPartitioningTableAfterSuccessfulCheck(state, lastBaseTable);
        }
    }

    private Map<String, List<EcstTransformationConfig>> synthesizeCandidateSets() {
        Map<String, List<EcstTransformationConfig>> result = new LinkedHashMap<>();
        List<String> synthesis = new ArrayList<>();
        QueryShape shape = inspectQueryShape();
        for (EcstTarget target : targets) {
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
                if (!isApplicableToTiDb(config, shape)) {
                    continue;
                }
                applicableCount++;
                applicable.add(config);
            }
            int maxCandidates = Math.max(1, state.getDbmsSpecificOptions().ecstMaxCandidates);
            if (applicable.size() > maxCandidates) {
                applicable = applicable.subList(0, maxCandidates);
            }
            if (!applicable.isEmpty()) {
                result.put(target.getKey(), applicable);
            }
            synthesis.add(target.getKey() + "{generated=" + generated + ",ruleAccepted=" + ruleAccepted
                    + ",legal=" + legal + ",applicable=" + applicableCount + ",selected=" + applicable.size()
                    + "}");
        }
        lastSynthesisSummary = "queryShape=" + shape + "; " + String.join("; ", synthesis);
        return result;
    }

    private QueryShape inspectQueryShape() {
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
        return new QueryShape(tables > 0, maxColumns >= 1, maxColumns >= 2, false, maxColumns);
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
                .anyMatch(rule -> rule.getTargetKey().equals(target.getKey()) && rule.supports(config));
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
        if (config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.ON) {
            return shape.hasJoin && (target.getKey().equals("TIDB_PUSHDOWN_SHIFT")
                    || target.getKey().equals("TIDB_ACCESS_PATH_SHIFT"));
        }
        if (config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.HAVING) {
            return shape.hasAggregation;
        }
        return config.getExpressionContext() != EcstTransformationConfig.ExpressionContext.SELECT || shape.hasProjection;
    }

    /** TiDB currently has a VP materializer for these contexts; other legal configs remain visible as future work. */
    private static boolean isApplicableToTiDb(EcstTransformationConfig config, QueryShape shape) {
        return config.getRepresentation() == EcstTransformationConfig.Representation.VERTICAL_PARTITIONED
                && (config.getReconstruction() == EcstTransformationConfig.Reconstruction.JOIN
                        || config.getReconstruction() == EcstTransformationConfig.Reconstruction.DERIVED_TABLE)
                && (config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.WHERE
                        || config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.SELECT
                        || config.getExpressionContext() == EcstTransformationConfig.ExpressionContext.HAVING)
                && shape.hasBaseTable;
    }

    private static final class QueryShape {
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

        private static QueryShape synthetic() {
            return new QueryShape(true, true, true, false, 2);
        }

        @Override
        public String toString() {
            return "baseTable=" + hasBaseTable + ",projection=" + hasProjection + ",aggregation="
                    + hasAggregation + ",join=" + hasJoin + ",maxDependencies=" + maxDependencies;
        }
    }

    private EcstTarget chooseTarget(Map<String, List<EcstTransformationConfig>> candidateSets) {
        Map<String, List<String>> keys = new LinkedHashMap<>();
        for (Map.Entry<String, List<EcstTransformationConfig>> entry : candidateSets.entrySet()) {
            keys.put(entry.getKey(), entry.getValue().stream().map(EcstTransformationConfig::stableKey)
                    .collect(Collectors.toList()));
        }
        String chosen = history.chooseTargetDecision(keys).getKey();
        for (EcstTarget target : targets) {
            if (target.getKey().equals(chosen)) {
                return target;
            }
        }
        return targets.get(state.getRandomly().getInteger(0, targets.size()));
    }

    private EcstTransformationConfig chooseConfig(EcstTarget target, List<EcstTransformationConfig> applicable) {
        if (applicable.isEmpty()) {
            throw new IllegalStateException("No applicable ECST configuration for target " + target.getKey());
        }
        List<String> keys = applicable.stream().map(EcstTransformationConfig::stableKey).collect(Collectors.toList());
        String chosen = history.chooseTransform(target.getKey(), keys);
        for (EcstTransformationConfig candidate : applicable) {
            if (candidate.stableKey().equals(chosen)) {
                return candidate;
            }
        }
        return applicable.get(state.getRandomly().getInteger(0, applicable.size()));
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
                + testCase.getTargetDependencies() + System.lineSeparator() + "transformedQuery="
                + testCase.getTransformedQuery() + System.lineSeparator() + "originalSample="
                + comparison.getOriginalSample() + System.lineSeparator() + "transformedSample="
                + comparison.getTransformedSample() + System.lineSeparator() + "history="
                + history.formatSummary();
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
            writer.write(history.formatSummary());
        } catch (IOException ignored) {
            // Feedback logging must never turn a completed test into a DBMS failure.
        }
    }
}
