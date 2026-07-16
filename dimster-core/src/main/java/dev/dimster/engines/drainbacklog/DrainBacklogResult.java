package dev.dimster.engines.drainbacklog;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import dev.dimster.ResultMetadata;
import dev.dimster.TestResult.PerformanceResults;
import dev.dimster.TestResult.WorkloadConfig;

/**
 * Result of a drain-backlog test run.
 */
@JsonPropertyOrder({"fileType", "mode", "metadata", "config", "scenarioResults"})
public class DrainBacklogResult {
    public String fileType = "dimster-result";
    public String mode = "drain-backlog";
    public ResultMetadata metadata = new ResultMetadata();
    public DrainBacklogWorkload config;
    public List<ScenarioResult> scenarioResults = new ArrayList<>();

    @JsonPropertyOrder({"name", "beginTime", "endTime", "tests"})
    public static class ScenarioResult {
        public String name;
        // Wall-clock timestamps bracketing this scenario's test points. The
        // dashboard exporter uses these to align HTML-window boundaries with
        // scenario boundaries so a scenario's test points stay co-located in
        // a single file. Null on the synthetic "(merged)" repeat-aggregate.
        public String beginTime;
        public String endTime;
        public List<TestOutcome> tests = new ArrayList<>();
    }

    @JsonPropertyOrder({"label", "workloadConfig", "drain", "performance"})
    public static class TestOutcome {
        public String label;
        public WorkloadConfig workloadConfig = new WorkloadConfig();
        public DrainData drain = new DrainData();
        public PerformanceResults performance = new PerformanceResults();
    }

    @JsonPropertyOrder({"backlogSizeGB", "backlogSizeBytes", "backlogMessages",
            "buildRate", "buildDurationMs",
            "drainDurationMs", "drainTimeoutMinutes", "backlogProducerRate", "sampleRateMillis",
            "backlogMessagesOverTime", "backlogBytesOverTime"})
    public static class DrainData {
        /** Target backlog size from config (GB). Supports fractions, e.g. 0.05 = 50MB. */
        public double backlogSizeGB;
        public long backlogSizeBytes;
        public long backlogMessages;
        /** Producer rate used during build phase (from config). */
        public int buildRate;
        public long buildDurationMs;
        public long drainDurationMs;
        /** Max drain duration from config (minutes). 0 means no limit. */
        public int drainTimeoutMinutes;
        public int backlogProducerRate;
        /** Stats sampling interval from config (milliseconds). */
        public int sampleRateMillis;
        public List<Long> backlogMessagesOverTime = new ArrayList<>();
        public List<Long> backlogBytesOverTime = new ArrayList<>();
    }
}
