package dev.dimster.engines.run;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Dimension-aware version of {@link RunConfig} for use in scenarios.
 * Each field is a nullable list — null means "inherit from the workload-level
 * {@code runConfig}." Lists with multiple elements define co-varying
 * dimensions alongside other scenario fields (e.g. producerRate, messageSize).
 *
 * Jackson's ACCEPT_SINGLE_VALUE_AS_ARRAY coerces scalar YAML values
 * (e.g. {@code rampIntervals: 5}) into single-element lists automatically.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RunDimensions {
    /** Duration of the recorded test phase in minutes. */
    public List<Integer> testDurationMinutes;

    /** Duration of the warmup phase in minutes (stats discarded). */
    public List<Integer> warmupDurationMinutes;

    /** Stats sampling interval in milliseconds. */
    public List<Integer> sampleRateMillis;

    /** Number of equal increments in the ramp-up phase. 0 disables ramp-up. */
    public List<Integer> rampIntervals;

    /** Duration of each ramp-up increment in seconds. 0 disables ramp-up. */
    public List<Integer> rampIntervalSeconds;

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
        n = checkField(scenarioName, "testDurationMinutes", testDurationMinutes, n, firstField);
        if (n > 1 && firstField == null) firstField = "testDurationMinutes";
        n = checkField(scenarioName, "warmupDurationMinutes", warmupDurationMinutes, n, firstField);
        if (n > 1 && firstField == null) firstField = "warmupDurationMinutes";
        n = checkField(scenarioName, "sampleRateMillis", sampleRateMillis, n, firstField);
        if (n > 1 && firstField == null) firstField = "sampleRateMillis";
        n = checkField(scenarioName, "rampIntervals", rampIntervals, n, firstField);
        if (n > 1 && firstField == null) firstField = "rampIntervals";
        n = checkField(scenarioName, "rampIntervalSeconds", rampIntervalSeconds, n, firstField);
    }

    private static int checkField(String scenarioName, String fieldName, List<?> list,
                                   int currentMax, String firstField) {
        if (list == null || list.size() <= 1) {
            return currentMax;
        }
        if (currentMax > 1 && list.size() != currentMax) {
            throw new IllegalArgumentException(
                    "Scenario '%s': runConfig.%s has %d elements but %s has %d"
                            .formatted(scenarioName, fieldName, list.size(), firstField, currentMax));
        }
        return Math.max(currentMax, list.size());
    }
}
