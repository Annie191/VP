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

    /** Typed direction key used for target trial and coverage accounting. */
    public static final class TargetKey {
        private final String dimension;
        private final String from;
        private final String to;
        private final String display;

        public TargetKey(String dimension, String from, String to) {
            this(dimension, from, to, dimension + ":" + from + "->" + to);
        }

        private TargetKey(String dimension, String from, String to, String display) {
            this.dimension = normalizeKey(dimension);
            this.from = normalizeKey(from);
            this.to = normalizeKey(to);
            this.display = display == null ? this.dimension + ":" + this.from + "->" + this.to : display;
        }

        public String getDimension() { return dimension; }
        public String getFrom() { return from; }
        public String getTo() { return to; }

        static TargetKey parse(String value) {
            if (value == null || value.trim().isEmpty()) {
                return new TargetKey("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN_TARGET");
            }
            String key = value.trim();
            String concrete = targetConcreteShift(key);
            if (concrete != null) {
                return parseShift(concrete, key);
            }
            return new TargetKey(targetDimension(key), key, key, key);
        }

        static TargetKey parseShift(String value, String display) {
            int colon = value.indexOf(':');
            int arrow = value.indexOf("->");
            if (colon > 0 && arrow > colon) {
                return new TargetKey(value.substring(0, colon), value.substring(colon + 1, arrow),
                        value.substring(arrow + 2), display);
            }
            return new TargetKey("UNKNOWN", value, value, display);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof TargetKey)) return false;
            TargetKey that = (TargetKey) other;
            return dimension.equals(that.dimension) && from.equals(that.from) && to.equals(that.to);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(dimension, from, to);
        }

        @Override
        public String toString() { return display; }
    }

    public static final class ConfigTargetKey {
        private final String config;
        private final TargetKey target;

        public ConfigTargetKey(String config, TargetKey target) {
            this.config = config == null ? "UNKNOWN_TRANSFORM" : config;
            this.target = target;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ConfigTargetKey)) return false;
            ConfigTargetKey that = (ConfigTargetKey) other;
            return config.equals(that.config) && target.equals(that.target);
        }

        @Override
        public int hashCode() { return java.util.Objects.hash(config, target); }

        @Override
        public String toString() { return config + "|" + target; }
    }

    public static final class ShiftKey {
        private final String dimension;
        private final String from;
        private final String to;

        public ShiftKey(String dimension, String from, String to) {
            this.dimension = normalizeKey(dimension);
            this.from = normalizeKey(from);
            this.to = normalizeKey(to);
        }

        static ShiftKey parse(String value) {
            TargetKey key = TargetKey.parseShift(value, value);
            return new ShiftKey(key.dimension, key.from, key.to);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ShiftKey)) return false;
            ShiftKey that = (ShiftKey) other;
            return dimension.equals(that.dimension) && from.equals(that.from) && to.equals(that.to);
        }

        @Override
        public int hashCode() { return java.util.Objects.hash(dimension, from, to); }

        @Override
        public String toString() { return dimension + ":" + from + "->" + to; }
    }

    private static String normalizeKey(String value) {
        return value == null || value.trim().isEmpty() ? "UNKNOWN" : value.trim().toUpperCase(java.util.Locale.ROOT);
    }

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

        private synchronized void recordTrial() {
            trials++;
        }

        private synchronized void recordStatus(EcstShiftStatus status, boolean covered, long cost) {
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

        private synchronized long costNanos() {
            return costNanos;
        }

        private synchronized String summary() {
            return "success=" + success + ",failure=" + failure + ",unknown=" + unknown + ",trials=" + trials
                    + ",coverage=" + coverage + ",costNanos=" + costNanos;
        }
    }

    private final ConcurrentMap<ConfigTargetKey, Stats> configStats = new ConcurrentHashMap<>();
    private final ConcurrentMap<TargetKey, Stats> targetStats = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Stats> featureTransformStats = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Stats> featureTargetStats = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Stats> coarseFeatureTransformStats = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Stats> coarseFeatureTargetStats = new ConcurrentHashMap<>();
    private final ConcurrentMap<ShiftKey, Boolean> observedShifts = new ConcurrentHashMap<>();
    /** N_cov[s]: count of tests in which the concrete migration s was confirmed. */
    private final ConcurrentMap<ShiftKey, Long> shiftCoverage = new ConcurrentHashMap<>();
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

        public double getSuccessRate() {
            return successRate;
        }

        public double getExploration() {
            return exploration;
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

    public void recordTrial(String targetKey) {
        targetStats.computeIfAbsent(TargetKey.parse(targetKey), ignored -> new Stats()).recordTrial();
        totalTrials++;
    }

    public void recordTrial(EcstTarget target) {
        recordTrial(target == null ? null : target.getKey());
    }

    public void recordSuccess(String targetKey, String transformKey) {
        recordOutcome(targetKey, transformKey, EcstShiftStatus.SUCCESS);
    }

    public void recordSuccess(EcstTarget target, String transformKey) {
        recordSuccess(target == null ? null : target.getKey(), transformKey);
    }

    public void recordFailure(String targetKey, String transformKey) {
        recordOutcome(targetKey, transformKey, EcstShiftStatus.FAILURE);
    }

    public void recordFailure(EcstTarget target, String transformKey) {
        recordFailure(target == null ? null : target.getKey(), transformKey);
    }

    public void recordUnknown(String targetKey, String transformKey) {
        recordOutcome(targetKey, transformKey, EcstShiftStatus.UNKNOWN);
    }

    public void recordUnknown(EcstTarget target, String transformKey) {
        recordUnknown(target == null ? null : target.getKey(), transformKey);
    }

    public void recordCoverage(String shift) {
        ShiftKey key = ShiftKey.parse(shift);
        observedShifts.put(key, Boolean.TRUE);
        shiftCoverage.merge(key, 1L, Long::sum);
    }

    private void recordOutcome(String targetKey, String transformKey, EcstShiftStatus status) {
        TargetKey target = TargetKey.parse(targetKey);
        ConfigTargetKey config = new ConfigTargetKey(transformKey, target);
        targetStats.computeIfAbsent(target, ignored -> new Stats()).recordStatus(status, false, 0);
        Stats configStatsForKey = configStats.computeIfAbsent(config, ignored -> new Stats());
        configStatsForKey.recordTrial();
        configStatsForKey.recordStatus(status, false, 0);
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
        TargetKey typedTarget = TargetKey.parse(target);
        boolean covered = hasTargetShift(typedTarget, distinctShifts);
        configStats.computeIfAbsent(new ConfigTargetKey(transform, typedTarget), ignored -> new Stats()).observe(status, covered,
                costNanos);
        targetStats.computeIfAbsent(typedTarget, ignored -> new Stats()).observe(status, covered, costNanos);
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
            recordCoverage(shift);
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

    public Decision chooseTransformDecision(String targetKey, Collection<String> candidates, EcstFeature feature) {
        Map<String, EcstFeature> featureMap = new LinkedHashMap<>();
        if (candidates != null) {
            for (String candidate : candidates) {
                featureMap.put(candidate, feature);
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
        Stats target = targetStats.get(TargetKey.parse(targetKey));
        long targetTrials = target == null ? 0 : target.trials();
        for (Map.Entry<String, EcstFeature> candidateEntry : candidates.entrySet()) {
            String candidate = candidateEntry.getKey();
            Stats stats = selectTransformStats(targetKey, candidate, candidateEntry.getValue());
            double rate = stats == null ? 0.5 : stats.successRate();
            long allTrials = stats == null ? 0 : stats.trials();
            double exploration = Math.sqrt(
                    2.0 * Math.log(2.0 + Math.max(1, targetTrials)) / (1.0 + allTrials));
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
            double reachability = maxReachability(candidate, entry.getValue(), feature);
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

    private double maxReachability(String targetKey, Collection<String> candidates, EcstFeature feature) {
        if (candidates == null || candidates.isEmpty()) {
            Stats target = targetStats.get(TargetKey.parse(targetKey));
            return target == null ? 0.5 : target.successRate();
        }
        double best = Double.NEGATIVE_INFINITY;
        for (String candidate : candidates) {
            Stats stats = selectTransformStats(targetKey, candidate, feature);
            best = Math.max(best, stats == null ? 0.5 : stats.successRate());
        }
        return best;
    }

    public long getTotalTrials() {
        return totalTrials;
    }

    public boolean hasObservedShift(String shift) {
        return observedShifts.containsKey(ShiftKey.parse(shift));
    }

    public int observedShiftCount() {
        return observedShifts.size();
    }

    /** Returns N_cov for one concrete observed migration key. */
    public long getShiftCoverage(String shift) {
        return shiftCoverage.getOrDefault(ShiftKey.parse(shift), 0L);
    }

    /** Returns N_cov for the migration dimension represented by a target key. */
    public long getTargetCoverage(String targetKey) {
        return targetCoverage(targetKey);
    }

    public Map<String, Boolean> observedShiftsSnapshot() {
        Map<String, Boolean> snapshot = new LinkedHashMap<>();
        observedShifts.forEach((key, value) -> snapshot.put(key.toString(), value));
        return Collections.unmodifiableMap(snapshot);
    }

    public Map<String, Long> shiftCoverageSnapshot() {
        Map<String, Long> snapshot = new LinkedHashMap<>();
        shiftCoverage.forEach((key, value) -> snapshot.put(key.toString(), value));
        return Collections.unmodifiableMap(snapshot);
    }

    public String formatSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("totalTrials=").append(totalTrials).append(System.lineSeparator());
        for (Map.Entry<ConfigTargetKey, Stats> entry : configStats.entrySet()) {
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
        LinkedHashSet<String> result = new LinkedHashSet<>();
        observedShifts.keySet().forEach(key -> result.add(key.toString()));
        return result;
    }

    private long targetCoverage(String targetKey) {
        if (targetKey == null || targetKey.isEmpty()) {
            return 0;
        }
        String direct = targetKey;
        long exact = shiftCoverage.getOrDefault(ShiftKey.parse(direct), 0L);
        if (exact > 0) {
            return exact;
        }
        String concreteShift = targetConcreteShift(targetKey);
        if (concreteShift != null) {
            return shiftCoverage.getOrDefault(ShiftKey.parse(concreteShift), 0L);
        }
        String dimension = targetDimension(targetKey);
        if (dimension == null) {
            return 0;
        }
        long total = 0;
        for (Map.Entry<ShiftKey, Long> entry : shiftCoverage.entrySet()) {
            if (entry.getKey().toString().startsWith(dimension + ":")) {
                total += entry.getValue();
            }
        }
        return total;
    }

    private static boolean hasTargetShift(TargetKey targetKey, Collection<String> shifts) {
        if (shifts == null || shifts.isEmpty() || targetKey == null) {
            return false;
        }
        ShiftKey expected = new ShiftKey(targetKey.dimension, targetKey.from, targetKey.to);
        for (String shift : shifts) {
            if (expected.equals(ShiftKey.parse(shift))) {
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

    /** Resolves the concrete migration represented by the directional TiDB target keys. */
    private static String targetConcreteShift(String targetKey) {
        if (targetKey == null) {
            return null;
        }
        if (targetKey.startsWith("TIDB_LOCATION_")) {
            String direction = targetKey.substring("TIDB_LOCATION_".length());
            int marker = direction.indexOf("_TO_");
            if (marker > 0) {
                return "LOCATION:" + direction.substring(0, marker) + "->"
                        + direction.substring(marker + "_TO_".length());
            }
        }
        if (targetKey.contains("PUSHDOWN_ROOT_TO_COP")) {
            return "BOUNDARY:NON_PUSHDOWN->PUSHDOWN";
        }
        if (targetKey.contains("PUSHDOWN_COP_TO_ROOT")) {
            return "BOUNDARY:PUSHDOWN->NON_PUSHDOWN";
        }
        if (targetKey.startsWith("TIDB_ACCESS_PATH_") || targetKey.startsWith("TIDB_ACCESS_")) {
            String prefix = targetKey.startsWith("TIDB_ACCESS_PATH_") ? "TIDB_ACCESS_PATH_" : "TIDB_ACCESS_";
            String direction = targetKey.substring(prefix.length());
            int marker = direction.indexOf("_TO_");
            if (marker > 0) {
                return "ACCESS_PATH:" + direction.substring(0, marker) + "->"
                        + direction.substring(marker + "_TO_".length());
            }
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
        return configStats.get(new ConfigTargetKey(transformKey, TargetKey.parse(targetKey)));
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
        return targetStats.get(TargetKey.parse(targetKey));
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
