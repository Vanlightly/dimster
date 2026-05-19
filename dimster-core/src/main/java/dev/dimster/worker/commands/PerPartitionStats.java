package dev.dimster.worker.commands;

import java.util.Map;
import java.util.TreeMap;

import org.HdrHistogram.Histogram;

import com.fasterxml.jackson.annotation.JsonIgnore;

import static dev.dimster.stats.StatsCollector.E2E_LATENCY_MAX_MICROS;
import static dev.dimster.stats.StatsCollector.NON_E2E_LATENCY_MAX_MICROS;

public class PerPartitionStats {
    public Map<String, PartitionMetrics> partitions = new TreeMap<>();

    public static class PartitionMetrics {
        public String key;
        public long messagesSent;
        public long messagesReceived;
        public byte[] publishLatencyBytes;
        public byte[] endToEndLatencyBytes;

        @JsonIgnore
        public Histogram publishLatencyHistogram;
        @JsonIgnore
        public Histogram endToEndLatencyHistogram;
    }
}
