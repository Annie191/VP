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

        private synchronized long coverage() {
            return coverage;
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
    private final ConcurrentMap<String, Boolean> observedShifts = new ConcurrentHashMap<>();
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
        String target = targetKey == null ? "UNKNOWN_TARGET" : targetKey;
        String transform = transformKey == null ? "UNKNOWN_TRANSFORM" : transformKey;
        boolean covered = shifts != null && !shifts.isEmpty();
        transformStats.computeIfAbsent(target + "|" + transform, ignored -> new Stats()).observe(status, covered,
                costNanos);
        targetStats.computeIfAbsent(target, ignored -> new Stats()).observe(status, covered, costNanos);
        if (shifts != null) {
            for (String shift : shifts) {
                observedShifts.put(shift, Boolean.TRUE);
            }
        }
        totalTrials++;
    }

    public String chooseTransform(String targetKey, Collection<String> candidates) {
        Decision decision = chooseTransformDecision(targetKey, candidates);
        return decision == null ? null : decision.getKey();
    }

    public Decision chooseTransformDecision(String targetKey, Collection<String> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        Decision selected = null;
        double best = Double.NEGATIVE_INFINITY;
        Stats target = targetStats.get(targetKey);
        long targetTrials = target == null ? 0 : target.trials();
        for (String candidate : candidates) {
            Stats stats = transformStats.get(targetKey + "|" + candidate);
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
        if (candidateConfigs == null || candidateConfigs.isEmpty()) {
            return null;
        }
        Decision selected = null;
        double best = Double.NEGATIVE_INFINITY;
        for (Map.Entry<String, ? extends Collection<String>> entry : candidateConfigs.entrySet()) {
            String candidate = entry.getKey();
            Stats stats = targetStats.get(candidate);
            long coverage = stats == null ? 0 : stats.coverage();
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

    public Map<String, Boolean> observedShiftsSnapshot() {
        return Collections.unmodifiableMap(observedShifts);
    }

    public String formatSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("totalTrials=").append(totalTrials).append(System.lineSeparator());
        for (Map.Entry<String, Stats> entry : transformStats.entrySet()) {
            sb.append(entry.getKey()).append(':').append(entry.getValue().summary()).append(System.lineSeparator());
        }
        sb.append("lastTargetDecision=").append(lastTargetDecision).append(System.lineSeparator());
        sb.append("lastTransformDecision=").append(lastTransformDecision).append(System.lineSeparator());
        return sb.toString();
    }

    public LinkedHashSet<String> observedShiftKeys() {
        return new LinkedHashSet<>(observedShifts.keySet());
    }
}
