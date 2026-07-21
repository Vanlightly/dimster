package dev.dimster.engines.run;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import dev.dimster.ResultMetadata;
import dev.dimster.TestResult;
import dev.dimster.availability.AvailabilityConfig;
import dev.dimster.availability.AvailabilityResult;

@JsonPropertyOrder({"fileType", "mode", "metadata", "baseKafkaConfig", "runConfig", "availabilityConfig", "scenarioResults"})
public class RunResult {
    public String fileType = "dimster-result";
    public String mode = "run";
    public ResultMetadata metadata = new ResultMetadata();
    public TestResult.KafkaConfigSummary baseKafkaConfig;
    public RunConfig runConfig;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public AvailabilityConfig availabilityConfig;

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
        public List<TestEntry> tests = new ArrayList<>();
    }

    @JsonPropertyOrder({"label", "kafkaConfig", "workloadConfig", "modifications", "results", "availability"})
    public static class TestEntry {
        public String label;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public TestResult.KafkaConfigSummary kafkaConfig;
        public TestResult.WorkloadConfig workloadConfig = new TestResult.WorkloadConfig();
        public TestResult.PerformanceResults results = new TestResult.PerformanceResults();

        // Per-test-point log of side-channel modifications applied via the
        // coordinator's /modify endpoint while this test point was running.
        // Null when no modifications were made (NON_NULL excludes it from
        // the JSON entirely so unmodified runs look exactly as before).
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public List<Modification> modifications;

        @JsonInclude(JsonInclude.Include.NON_NULL)
        public AvailabilityResult.AvailabilityData availability;
    }

    /**
     * One side-channel modification applied to a test point at runtime.
     *
     * Captures both the request (workloadOverrides / kafkaConfigOverrides /
     * action flags) and what the engine actually did about it (the
     * `actions` list — human-readable strings naming the worker operations
     * the diff applier issued). Empty/null fields are excluded from the
     * JSON so a simple producerRate change still produces a compact entry.
     *
     * The timestamp uses the same "yyyy-MM-dd-HH-mm-ss" format as the
     * surrounding scenario / metadata timestamps.
     */
    @JsonPropertyOrder({"timestamp", "workloadOverrides", "kafkaConfigOverrides",
            "terminate", "skipPhase", "discardStats", "runForMinutes",
            "restartConsumersCount", "restartedConsumerNames",
            "restartProducersCount", "restartedProducerNames", "actions"})
    public static class Modification {
        public String timestamp;
        // The override maps echo the user's request — e.g. producerRate,
        // producersPerTopic, messageDistributor — so the result file
        // captures exactly what was asked for, not just what was applied.
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public java.util.Map<String, Object> workloadOverrides;
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public java.util.Map<String, Object> kafkaConfigOverrides;
        // Engine-internal action flags (don't go through the override
        // pipeline — they're not workload fields).
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public Boolean terminate;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public Boolean skipPhase;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public Boolean discardStats;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public Integer runForMinutes;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public Integer restartConsumersCount;
        // Random restart actions record stable topology names so a disturbance
        // can be correlated with client and worker logs after the run. Empty
        // lists (including an explicit count of zero) stay out of the JSON.
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<String> restartedConsumerNames;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public Integer restartProducersCount;
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public List<String> restartedProducerNames;
        // Human-readable description of each worker action the diff
        // applier issued, in the order it issued them. E.g.
        //   "adjust-publish-rate to 5000.0"
        //   "add-producer producer-topic-i2 on worker-1"
        //   "restart-consumers (3) due to shareConsumerConfig.max.poll.records change"
        // Empty for action-flag-only modifications (terminate / etc.) since
        // those don't issue worker calls.
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        public java.util.List<String> actions;
    }
}
