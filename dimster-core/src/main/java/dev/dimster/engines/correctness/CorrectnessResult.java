package dev.dimster.engines.correctness;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import dev.dimster.ResultMetadata;
import dev.dimster.TestResult.PerformanceResults;
import dev.dimster.TestResult.WorkloadConfig;

/**
 * Result of a correctness test run.
 */
@JsonPropertyOrder({"fileType", "mode", "metadata", "config", "scenarioResults"})
public class CorrectnessResult {
    public String fileType = "dimster-result";
    public String mode = "correctness";
    public ResultMetadata metadata = new ResultMetadata();
    public CorrectnessWorkload config;
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

    @JsonPropertyOrder({"label", "workloadConfig", "performance", "correctness"})
    public static class TestOutcome {
        public String label;
        public WorkloadConfig workloadConfig = new WorkloadConfig();
        public PerformanceResults performance = new PerformanceResults();
        public CorrectnessData correctness = new CorrectnessData();
    }

    @JsonPropertyOrder({"passed", "totalSent", "totalReceived", "lostMessages",
            "duplicateCount", "outOfOrderCount", "crcFailureCount",
            "lostMessageRanges", "violations"})
    public static class CorrectnessData {
        public boolean passed;
        public long totalSent;
        public long totalReceived;
        public long lostMessages;
        public long duplicateCount;
        public long outOfOrderCount;
        public long crcFailureCount;
        public List<LostMessageRangeEntry> lostMessageRanges = new ArrayList<>();
        public List<ViolationEntry> violations = new ArrayList<>();
    }

    @JsonPropertyOrder({"producerId", "topic", "fromSequence", "toSequence", "count"})
    public static class LostMessageRangeEntry {
        public int producerId;
        public String topic;
        public long fromSequence;
        public long toSequence;
        public long count;

        public LostMessageRangeEntry() {}

        public LostMessageRangeEntry(int producerId, String topic,
                                     long fromSequence, long toSequence, long count) {
            this.producerId = producerId;
            this.topic = topic;
            this.fromSequence = fromSequence;
            this.toSequence = toSequence;
            this.count = count;
        }
    }

    @JsonPropertyOrder({"timestampMs", "type", "producerId", "topic", "fromSequence", "toSequence",
            "count", "partition", "highSequence"})
    public static class ViolationEntry {
        public long timestampMs;
        public String type;
        public int producerId;
        public String topic;
        public long fromSequence;
        public long toSequence;
        public long count;
        public int partition;
        public long highSequence;

        public ViolationEntry() {}

        public ViolationEntry(long timestampMs, String type, int producerId, String topic,
                              long sequence, int partition, long highSequence) {
            this.timestampMs = timestampMs;
            this.type = type;
            this.producerId = producerId;
            this.topic = topic;
            this.fromSequence = sequence;
            this.toSequence = sequence;
            this.count = 1;
            this.partition = partition;
            this.highSequence = highSequence;
        }

        /** Try to extend this range with the next sequence. Returns true if merged. */
        public boolean tryExtend(long sequence, long highSequence) {
            if (sequence == toSequence + 1 && highSequence == this.highSequence) {
                toSequence = sequence;
                count++;
                return true;
            }
            return false;
        }
    }
}
