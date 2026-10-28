package sqlancer.common.oracle.ecst;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import sqlancer.Randomly;

/** Thread-safe in-process feedback used by the ECST target and transform selectors. */
public final class EcstHistory {
    private static final double SMOOTHING_A = 1.0;
    private static final double SMOOTHING_B = 1.0;
    private static final long MIN_FEATURE_TRIALS = 3;

    private static final class Stats {
        private long success;
        private long failure;
        private long unknown;
        private long trials;
        private long coverage;
        private long costNanos;

        private synchronized void observe(EcstShiftStatus status, boolean covered, long cost) {
            switch (status) {
            case SUCCESS:
                success++;
                break;
            case FAILURE:
                failure++;
                break;
            case UNKNOWN:
                unknown++;
                break;
            default:
                throw new AssertionError(status);
            }
            trials++;
            if (covered) {
                coverage++;
            }
            costNanos += Math.max(0, cost);
        }

        private synchronized double successRate() {
            return (success + SMOOTHING_A) / (success + failure + SMOOTHING_A + SMOOTHING_B);
        }

        private synchronized long trials() {
            return trials;
        }

        private synchronized long decidableTrials() {
            return success + failure;
        }

        private synchronized long costNanos() {
            return costNanos;
        }

        private synchronized String summary() {
            return "success=" + success + ",failure=" + failure + ",unknown=" + unknown + ",trials=" + trials
                    + ",coverage=" + coverage + ",costNanos=" + costNanos;
        }
    }

    private final ConcurrentMap<String, Stats> transformStats = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Stats> targetStats = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Stats> featureTransformStats = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Stats> featureTargetStats = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Stats> coarseFeatureTransformStats = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Stats> coarseFeatureTargetStats = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Boolean> observedShifts = new ConcurrentHashMap<>();
    /** N_cov[s]: count of tests in which the concrete migration s was confirmed. */
    private final ConcurrentMap<String, Long> shiftCoverage = new ConcurrentHashMap<>();
    private volatile long totalTrials;
    private volatile String lastTargetDecision = "<none>";
    private volatile String lastTransformDecision = "<none>";

    public static final class Decision {
        private final String key;
        private final double score;
        private final double successRate;
        private final double exploration;
        private final double costSeconds;

        private Decision(String key, double score, double successRate, double exploration, double costSeconds) {
            this.key = key;
            this.score = score;
            this.successRate = successRate;
            this.exploration = exploration;
            this.costSeconds = costSeconds;
        }

        public String getKey() {
            return key;
        }

        public double getScore() {
            return score;
        }

        @Override
        public String toString() {
            return key + "{score=" + score + ",p=" + successRate + ",explore=" + exploration + ",costSeconds="
                    + costSeconds + "}";
        }
    }

    public void observe(String targetKey, String transformKey, EcstShiftStatus status, Collection<String> shifts,
            long costNanos) {
        observe(targetKey, transformKey, status, shifts, costNanos, null);
    }

    public void observe(String targetKey, String transformKey, EcstShiftStatus status, Collection<String> shifts,
            long costNanos, EcstFeature feature) {
        String target = targetKey == null ? "UNKNOWN_TARGET" : targetKey;
        String transform = transformKey == null ? "UNKNOWN_TRANSFORM" : transformKey;
        LinkedHashSet<String> distinctShifts = new LinkedHashSet<>();
        if (shifts != null) {
            for (String shift : shifts) {
                if (shift != null && !shift.trim().isEmpty()) {
                    distinctShifts.add(shift.trim());
                }
            }
        }
        boolean covered = hasTargetShift(target, distinctShifts);
        transformStats.computeIfAbsent(target + "|" + transform, ignored -> new Stats()).observe(status, covered,
                costNanos);
        targetStats.computeIfAbsent(target, ignored -> new Stats()).observe(status, covered, costNanos);
        if (feature != null) {
            featureTransformStats.computeIfAbsent(featureTransformKey(target, transform, feature), ignored -> new Stats())
                    .observe(status, covered, costNanos);
            featureTargetStats.computeIfAbsent(featureTargetKey(target, feature), ignored -> new Stats())
                    .observe(status, covered, costNanos);
            coarseFeatureTransformStats.computeIfAbsent(coarseFeatureTransformKey(target, transform, feature),
                    ignored -> new Stats()).observe(status, covered, costNanos);
            coarseFeatureTargetStats.computeIfAbsent(coarseFeatureTargetKey(target, feature), ignored -> new Stats())
                    .observe(status, covered, costNanos);
        }
        for (String shift : distinctShifts) {
            observedShifts.put(shift, Boolean.TRUE);
            shiftCoverage.merge(shift, 1L, Long::sum);
        }
        totalTrials++;
    }

    public String chooseTransform(String targetKey, Collection<String> candidates) {
        Decision decision = chooseTransformDecision(targetKey, candidates);
        return decision == null ? null : decision.getKey();
    }

    public Decision chooseTransformDecision(String targetKey, Collection<String> candidates) {
        Map<String, EcstFeature> featureMap = new LinkedHashMap<>();
        if (candidates != null) {
            for (String candidate : candidates) {
                featureMap.put(candidate, null);
            }
        }
        return chooseTransformDecision(targetKey, featureMap);
    }

    /** Feature-conditioned transform selection with full-to-coarse-to-global fallback. */
    public Decision chooseTransformDecision(String targetKey, Map<String, EcstFeature> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        Decision selected = null;
        double best = Double.NEGATIVE_INFINITY;
        Stats target = targetStats.get(targetKey);
        long targetTrials = target == null ? 0 : target.trials();
        for (Map.Entry<String, EcstFeature> candidateEntry : candidates.entrySet()) {
            String candidate = candidateEntry.getKey();
            Stats stats = selectTransformStats(targetKey, candidate, candidateEntry.getValue());
            double rate = stats == null ? 0.5 : stats.successRate();
            long decidableTrials = stats == null ? 0 : stats.decidableTrials();
            long allTrials = stats == null ? 0 : stats.trials();
            double exploration = Math.sqrt(
                    2.0 * Math.log(2.0 + Math.max(1, targetTrials)) / (1.0 + decidableTrials));
            double costPenalty = stats == null || allTrials == 0 ? 0.0
                    : ((double) stats.costNanos() / allTrials) / 1_000_000_000.0;
            double score = rate + 0.25 * exploration - 0.01 * costPenalty;
            if (score > best || score == best && Randomly.getBoolean()) {
                best = score;
                selected = new Decision(candidate, score, rate, exploration, costPenalty);
            }
        }
        lastTransformDecision = selected.toString();
        return selected;
    }

    /** First-level feedback: choose a target using novelty, reachability and exploration. */
    public String chooseTarget(Collection<String> candidates) {
        Map<String, Collection<String>> candidateMap = new LinkedHashMap<>();
        if (candidates != null) {
            for (String candidate : candidates) {
                candidateMap.put(candidate, Collections.emptyList());
            }
        }
        Decision decision = chooseTargetDecision(candidateMap);
        return decision == null ? null : decision.getKey();
    }

    /** First-level feedback with candidate-set reachability as defined by the ECST document. */
    public Decision chooseTargetDecision(Map<String, ? extends Collection<String>> candidateConfigs) {
        return chooseTargetDecision(candidateConfigs, null);
    }

    public Decision chooseTargetDecision(Map<String, ? extends Collection<String>> candidateConfigs,
            EcstFeature feature) {
        if (candidateConfigs == null || candidateConfigs.isEmpty()) {
            return null;
        }
        Decision selected = null;
        double best = Double.NEGATIVE_INFINITY;
        for (Map.Entry<String, ? extends Collection<String>> entry : candidateConfigs.entrySet()) {
            String candidate = entry.getKey();
            Stats stats = selectTargetStats(candidate, feature);
            long coverage = targetCoverage(candidate);
            long trials = stats == null ? 0 : stats.trials();
            double novelty = 1.0 / Math.sqrt(1.0 + coverage);
            double reachability = maxReachability(candidate, entry.getValue());
            double exploration = Math.sqrt(2.0 * Math.log(2.0 + Math.max(1, totalTrials)) / (1.0 + trials));
            double score = novelty + reachability + exploration;
            if (score > best || score == best && Randomly.getBoolean()) {
                best = score;
                selected = new Decision(candidate, score, reachability, exploration, 0.0);
            }
        }
        lastTargetDecision = selected.toString();
        return selected;
    }

    private double maxReachability(String targetKey, Collection<String> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            Stats target = targetStats.get(targetKey);
            return target == null ? 0.5 : target.successRate();
        }
        double best = 0.5;
        for (String candidate : candidates) {
            Stats stats = transformStats.get(targetKey + "|" + candidate);
            best = Math.max(best, stats == null ? 0.5 : stats.successRate());
        }
        return best;
    }

    public long getTotalTrials() {
        return totalTrials;
    }

    public boolean hasObservedShift(String shift) {
        return observedShifts.containsKey(shift);
    }

    public int observedShiftCount() {
        return observedShifts.size();
    }

    /** Returns N_cov for one concrete observed migration key. */
    public long getShiftCoverage(String shift) {
        return shiftCoverage.getOrDefault(shift, 0L);
    }

    /** Returns N_cov for the migration dimension represented by a target key. */
    public long getTargetCoverage(String targetKey) {
        return targetCoverage(targetKey);
    }

    public Map<String, Boolean> observedShiftsSnapshot() {
        return Collections.unmodifiableMap(observedShifts);
    }

    public Map<String, Long> shiftCoverageSnapshot() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(shiftCoverage));
    }

    public String formatSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("totalTrials=").append(totalTrials).append(System.lineSeparator());
        for (Map.Entry<String, Stats> entry : transformStats.entrySet()) {
            sb.append(entry.getKey()).append(':').append(entry.getValue().summary()).append(System.lineSeparator());
        }
        for (Map.Entry<String, Stats> entry : featureTransformStats.entrySet()) {
            sb.append("feature[").append(entry.getKey()).append("]:").append(entry.getValue().summary())
                    .append(System.lineSeparator());
        }
        sb.append("coverageByShift=").append(new LinkedHashMap<>(shiftCoverage)).append(System.lineSeparator());
        sb.append("lastTargetDecision=").append(lastTargetDecision).append(System.lineSeparator());
        sb.append("lastTransformDecision=").append(lastTransformDecision).append(System.lineSeparator());
        return sb.toString();
    }

    public LinkedHashSet<String> observedShiftKeys() {
        return new LinkedHashSet<>(observedShifts.keySet());
    }

    private long targetCoverage(String targetKey) {
        if (targetKey == null || targetKey.isEmpty()) {
            return 0;
        }
        long exact = shiftCoverage.getOrDefault(targetKey, 0L);
        if (exact > 0) {
            return exact;
        }
        String dimension = targetDimension(targetKey);
        if (dimension == null) {
            return 0;
        }
        long total = 0;
        for (Map.Entry<String, Long> entry : shiftCoverage.entrySet()) {
            if (entry.getKey().startsWith(dimension + ":")) {
                total += entry.getValue();
            }
        }
        return total;
    }

    private static boolean hasTargetShift(String targetKey, Collection<String> shifts) {
        if (shifts == null || shifts.isEmpty() || targetKey == null) {
            return false;
        }
        String dimension = targetDimension(targetKey);
        for (String shift : shifts) {
            if (targetKey.equals(shift) || dimension != null && shift.startsWith(dimension + ":")) {
                return true;
            }
        }
        return false;
    }

    private static String targetDimension(String targetKey) {
        if (targetKey.startsWith("LOCATION") || targetKey.contains("LOCATION")) {
            return "LOCATION";
        }
        if (targetKey.startsWith("BOUNDARY") || targetKey.contains("PUSHDOWN")) {
            return "BOUNDARY";
        }
        if (targetKey.startsWith("ACCESS_PATH") || targetKey.contains("ACCESS_PATH")) {
            return "ACCESS_PATH";
        }
        if (targetKey.startsWith("OPERATOR") || targetKey.contains("OPERATOR")) {
            return "OPERATOR";
        }
        if (targetKey.startsWith("EVALUATION") || targetKey.contains("EVALUATION")) {
            return "EVALUATION";
        }
        return null;
    }

    private Stats selectTransformStats(String targetKey, String transformKey, EcstFeature feature) {
        if (feature != null) {
            Stats exact = featureTransformStats.get(featureTransformKey(targetKey, transformKey, feature));
            if (hasEnoughFeatureSamples(exact)) {
                return exact;
            }
            Stats coarse = coarseFeatureTransformStats.get(coarseFeatureTransformKey(targetKey, transformKey, feature));
            if (hasEnoughFeatureSamples(coarse)) {
                return coarse;
            }
        }
        return transformStats.get(targetKey + "|" + transformKey);
    }

    private Stats selectTargetStats(String targetKey, EcstFeature feature) {
        if (feature != null) {
            Stats exact = featureTargetStats.get(featureTargetKey(targetKey, feature));
            if (hasEnoughFeatureSamples(exact)) {
                return exact;
            }
            Stats coarse = coarseFeatureTargetStats.get(coarseFeatureTargetKey(targetKey, feature));
            if (hasEnoughFeatureSamples(coarse)) {
                return coarse;
            }
        }
        return targetStats.get(targetKey);
    }

    private static boolean hasEnoughFeatureSamples(Stats stats) {
        return stats != null && stats.trials() >= MIN_FEATURE_TRIALS;
    }

    private static String featureTransformKey(String targetKey, String transformKey, EcstFeature feature) {
        return targetKey + "|" + transformKey + "|" + feature.key();
    }

    private static String featureTargetKey(String targetKey, EcstFeature feature) {
        return targetKey + "|" + feature.key();
    }

    private static String coarseFeatureTransformKey(String targetKey, String transformKey, EcstFeature feature) {
        return targetKey + "|" + transformKey + "|" + feature.coarseKey();
    }

    private static String coarseFeatureTargetKey(String targetKey, EcstFeature feature) {
        return targetKey + "|" + feature.coarseKey();
    }
}
