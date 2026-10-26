package dev.dimster.flink;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * The {@code flink} section of a test point's result: what ran and how it
 * went. The sweepable settings also appear flat in the result's
 * workloadConfig ({@code flinkParallelism}, ...) where charting tools look
 * for dimensions; this holds the rest.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FlinkResult {
    public String sql;
    public String jobId;
    public long checkpointIntervalMs;
    /** Restarts of the job during the test point; a non-zero value means the job failed and recovered. */
    public Long restarts;
    /**
     * Run mode: records consumed per record produced (per consumer group)
     * over the measured phase. 1.0 for a pass-through query, below 1 for a filter.
     */
    public Double selectivity;

    public static FlinkResult of(FlinkSpec spec, String jobId, long restarts) {
        FlinkResult r = new FlinkResult();
        r.sql = spec.sql;
        r.jobId = jobId;
        r.checkpointIntervalMs = spec.checkpointIntervalMs;
        r.restarts = restarts >= 0 ? restarts : null;
        return r;
    }
}
