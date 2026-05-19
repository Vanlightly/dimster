package dev.dimster.engines.explore;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Dimension-aware version of {@link ExploreWorkload.ExploreConfig} for use in scenarios.
 * Each field is a nullable list — null means "inherit from the workload-level
 * {@code exploreConfig}." Lists with multiple elements define co-varying
 * dimensions alongside other scenario fields (e.g. partitionsPerTopic).
 *
 * Jackson's ACCEPT_SINGLE_VALUE_AS_ARRAY coerces scalar YAML values
 * (e.g. {@code searchDurationSeconds: 120}) into single-element lists automatically.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ExploreDimensions {
    /** Latency percentile to check (e.g. 75 for p75) */
    public List<Double> e2eLatencyPercentile;

    /** Maximum acceptable latency in milliseconds at the given percentile */
    public List<Double> e2eLatencyThresholdMs;

    /** Fraction by which sustain phase relaxes the thresholds (0.10 = 10% more lenient) */
    public List<Double> sustainLeniency;

    /** Maximum acceptable publish delay in milliseconds at the given percentile */
    public List<Double> publishDelayThresholdMs;

    /** Starting rate for the ramp-up (msgs/sec) */
    public List<Integer> initialRate;

    /** Measurement interval in seconds during ramp-up (phase 1) */
    public List<Integer> rampIntervalSeconds;

    /** Duration of each binary search measurement in seconds (phase 2) */
    public List<Integer> searchDurationSeconds;

    /** How often to check for saturation during binary search (seconds) */
    public List<Integer> searchIntervalSeconds;

    /** Max retries of binary search + sustain if convergence fails */
    public List<Integer> searchMaxRetries;

    /** Seconds to sustain peak rate to confirm it's stable (phase 3) */
    public List<Integer> sustainDurationSeconds;

    /** Warmup seconds before each test (at initial rate, stats discarded) */
    public List<Integer> rampWarmupSeconds;

    /** Producer rate (msgs/s) during stabilization drain */
    public List<Integer> stabilizeRate;

    /** Timeout in seconds for consumers to drain during stabilization */
    public List<Integer> stabilizeTimeoutSeconds;

    /** Minimum rate (msgs/s) below which binary search declares the system cannot sustain load */
    public List<Double> minViableRate;

    /** When true, use worst per-partition e2e latency for sustainability checks */
    public List<Boolean> perPartitionE2eMode;

    /** When true, use delivery latency for e2e threshold checks */
    public List<Boolean> useDeliveryLatencyForThreshold;

    /**
     * Validate that all non-null, multi-element lists have the same length.
     * Call this before resolving against defaults so that mismatches between
     * user-specified fields are caught early (before resolve() promotes
     * missing fields to singletons, masking the mismatch).
     *
     * @throws IllegalArgumentException if any two multi-element lists differ in size
     */
    public void validate(String scenarioName) {
        int n = -1;
        String firstField = null;
        n = checkField(scenarioName, "e2eLatencyPercentile", e2eLatencyPercentile, n, firstField);
        if (n > 1 && firstField == null) firstField = "e2eLatencyPercentile";
        n = checkField(scenarioName, "e2eLatencyThresholdMs", e2eLatencyThresholdMs, n, firstField);
        if (n > 1 && firstField == null) firstField = "e2eLatencyThresholdMs";
        n = checkField(scenarioName, "sustainLeniency", sustainLeniency, n, firstField);
        if (n > 1 && firstField == null) firstField = "sustainLeniency";
        n = checkField(scenarioName, "publishDelayThresholdMs", publishDelayThresholdMs, n, firstField);
        if (n > 1 && firstField == null) firstField = "publishDelayThresholdMs";
        n = checkField(scenarioName, "initialRate", initialRate, n, firstField);
        if (n > 1 && firstField == null) firstField = "initialRate";
        n = checkField(scenarioName, "rampIntervalSeconds", rampIntervalSeconds, n, firstField);
        if (n > 1 && firstField == null) firstField = "rampIntervalSeconds";
        n = checkField(scenarioName, "searchDurationSeconds", searchDurationSeconds, n, firstField);
        if (n > 1 && firstField == null) firstField = "searchDurationSeconds";
        n = checkField(scenarioName, "searchIntervalSeconds", searchIntervalSeconds, n, firstField);
        if (n > 1 && firstField == null) firstField = "searchIntervalSeconds";
        n = checkField(scenarioName, "searchMaxRetries", searchMaxRetries, n, firstField);
        if (n > 1 && firstField == null) firstField = "searchMaxRetries";
        n = checkField(scenarioName, "sustainDurationSeconds", sustainDurationSeconds, n, firstField);
        if (n > 1 && firstField == null) firstField = "sustainDurationSeconds";
        n = checkField(scenarioName, "rampWarmupSeconds", rampWarmupSeconds, n, firstField);
        if (n > 1 && firstField == null) firstField = "rampWarmupSeconds";
        n = checkField(scenarioName, "stabilizeRate", stabilizeRate, n, firstField);
        if (n > 1 && firstField == null) firstField = "stabilizeRate";
        n = checkField(scenarioName, "stabilizeTimeoutSeconds", stabilizeTimeoutSeconds, n, firstField);
        if (n > 1 && firstField == null) firstField = "stabilizeTimeoutSeconds";
        n = checkField(scenarioName, "minViableRate", minViableRate, n, firstField);
        if (n > 1 && firstField == null) firstField = "minViableRate";
        n = checkField(scenarioName, "perPartitionE2eMode", perPartitionE2eMode, n, firstField);
        if (n > 1 && firstField == null) firstField = "perPartitionE2eMode";
        n = checkField(scenarioName, "useDeliveryLatencyForThreshold", useDeliveryLatencyForThreshold, n, firstField);
    }

    private static int checkField(String scenarioName, String fieldName, List<?> list,
                                   int currentMax, String firstField) {
        if (list == null || list.size() <= 1) {
            return currentMax;
        }
        if (currentMax > 1 && list.size() != currentMax) {
            throw new IllegalArgumentException(
                    "Scenario '%s': exploreConfig.%s has %d elements but %s has %d"
                            .formatted(scenarioName, fieldName, list.size(), firstField, currentMax));
        }
        return Math.max(currentMax, list.size());
    }
}
