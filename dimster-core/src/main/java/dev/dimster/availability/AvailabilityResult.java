package dev.dimster.availability;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Container for availability result data types.
 * Used by RunResult.TestEntry to carry optional availability data alongside performance results.
 */
public class AvailabilityResult {

    /**
     * Used by Worker.getAvailabilityResult() to return both availability data
     * and workload config from the worker layer.
     */
    public static class TestOutcome {
        public String label;
        public AvailabilityData availability = new AvailabilityData();
    }

    @JsonPropertyOrder({"partitions", "wallClockMs", "totalElapsedMs",
            "producerAvailabilityPct", "producerUnavailableMs",
            "e2eAvailabilityPct", "e2eUnavailableMs",
            "overallAvailabilityPct", "totalUnavailableMs", "partitionResults"})
    public static class AvailabilityData {
        public int partitions = 0;
        public long wallClockMs = 0;
        public long totalElapsedMs = 0;
        public double producerAvailabilityPct = 100.0;
        public long producerUnavailableMs = 0;
        public double e2eAvailabilityPct = 100.0;
        public long e2eUnavailableMs = 0;
        public double overallAvailabilityPct = 100.0;
        public long totalUnavailableMs = 0;
        public List<PartitionResult> partitionResults = new ArrayList<>();
    }

    @JsonPropertyOrder({"partition", "probesSent", "probesReceived", "probesFailed",
            "elapsedMs", "producerAvailabilityPct", "e2eAvailabilityPct",
            "producerUnavailableMs", "e2eUnavailableMs",
            "unavailabilityEvents", "timeSeries"})
    public static class PartitionResult {
        public int partition;
        public long probesSent;
        public long probesReceived;
        public long probesFailed;
        public long elapsedMs;
        public double producerAvailabilityPct = 100.0;
        public double e2eAvailabilityPct = 100.0;
        public long producerUnavailableMs;
        public long e2eUnavailableMs;
        public List<UnavailabilityEvent> unavailabilityEvents = new ArrayList<>();
        public List<ProbeDataPoint> timeSeries = new ArrayList<>();
    }

    @JsonPropertyOrder({"startTimestampMs", "endTimestampMs", "durationMs", "partition", "type"})
    public static class UnavailabilityEvent {
        public long startTimestampMs;
        public long endTimestampMs;
        public long durationMs;
        public int partition;
        public String type; // "PRODUCER" or "E2E"
    }

    @JsonPropertyOrder({"timestampMs", "partition", "produceDurationMs", "e2eLatencyMs"})
    public static class ProbeDataPoint {
        public long timestampMs;
        public int partition;
        public long produceDurationMs;
        public long e2eLatencyMs;
    }
}
