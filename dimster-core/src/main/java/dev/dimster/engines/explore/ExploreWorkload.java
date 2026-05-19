package dev.dimster.engines.explore;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import dev.dimster.BaseWorkload;
import dev.dimster.EngineConfig;
import dev.dimster.Scenario;
import dev.dimster.TestResult.KafkaConfigSummary;

import java.util.List;

/**
 * YAML definition for an explore-limits workload.
 * Also used as the "config" section in ExploreResult JSON output.
 *
 * Structure:
 *   name: ...
 *   exploreConfig:
 *     e2eLatencyPercentile: 75
 *     ...
 *   baseWorkload:
 *     topics: 1
 *     partitionsPerTopic: 6
 *     ...
 *   scenarios:
 *     - name: partition-test
 *       partitionsPerTopic: [1, 6]
 *       consumersPerGroup: [1, 6]
 */
@JsonPropertyOrder({"baseKafkaConfig", "exploreConfig", "baseWorkload", "scenarios"})
public class ExploreWorkload {
    /** Name is stored in ExploreResult.metadata, excluded from config serialization */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    public String name = "explore-limits";

    /** Base Kafka config summary (merged from -k file + baseWorkload), populated when writing results */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaConfigSummary baseKafkaConfig;

    public EngineConfig engineConfig = new EngineConfig();
    public ExploreConfig exploreConfig = new ExploreConfig();
    public BaseWorkload baseWorkload = new BaseWorkload();
    public List<Scenario> scenarios = List.of();

    /**
     * Algorithm configuration for the explore engine.
     */
    public static class ExploreConfig {
        /** Latency percentile to check (e.g. 75 for p75) */
        public double e2eLatencyPercentile = 75;

        /** Maximum acceptable latency in milliseconds at the given percentile */
        public double e2eLatencyThresholdMs = 50;

        /** Fraction by which sustain phase relaxes the thresholds (0.10 = 10% more lenient) */
        public double sustainLeniency = 0.10;

        /** Maximum acceptable publish delay in milliseconds at the given percentile */
        public double publishDelayThresholdMs = 200;

        /** Starting rate for the ramp-up (msgs/sec) */
        public int initialRate = 100;

        /** Measurement interval in seconds during ramp-up (phase 1) */
        public int rampIntervalSeconds = 10;

        /** Duration of each binary search measurement in seconds (phase 2) */
        public int searchDurationSeconds = 120;

        /** How often to check for saturation during binary search (seconds) */
        public int searchIntervalSeconds = 10;

        /** Max retries of binary search + sustain if convergence fails */
        public int searchMaxRetries = 3;

        /** Seconds to sustain peak rate to confirm it's stable (phase 3) */
        public int sustainDurationSeconds = 300;

        /** Warmup seconds before each test (at initial rate, stats discarded) */
        public int rampWarmupSeconds = 10;

        /** Producer rate (msgs/s) during stabilization drain. A low trickle keeps
         *  share-group consumers progressing on Kafka versions with the stop-drain bug. */
        public int stabilizeRate = 0;

        /** Timeout in seconds for consumers to drain during stabilization between
         *  search phases. Failed high-rate attempts can leave millions of stale messages
         *  whose e2e latency would contaminate subsequent measurements. */
        public int stabilizeTimeoutSeconds = 120;

        /** Minimum rate (msgs/s) below which binary search declares the system
         *  cannot sustain any load for this test point. */
        public double minViableRate = 1.0;

        /** When true, sustainability checks use the worst per-partition e2e
         *  latency instead of the aggregate histogram. Disable for workloads
         *  with thousands of partitions where per-partition tracking is too
         *  costly. */
        public boolean perPartitionE2eMode = true;

        /** When true, e2e threshold checks use delivery latency (publish →
         *  poll return) instead of standard e2e latency (publish → record
         *  processed). Useful when consumer processing time is configured
         *  and you want to measure broker-side latency independent of
         *  client-side processing. */
        public boolean useDeliveryLatencyForThreshold = false;
    }
}
