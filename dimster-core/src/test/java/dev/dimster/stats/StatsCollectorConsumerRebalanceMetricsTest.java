package dev.dimster.stats;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.StringWriter;

import org.junit.Test;

/**
 * Verifies the aggregate consumer-rebalance metrics owned by
 * {@link StatsCollector}. The text output is the contract read by Prometheus,
 * so these tests cover aggregation, cardinality, and reset semantics directly.
 */
public class StatsCollectorConsumerRebalanceMetricsTest {

    @Test
    public void rebalanceCallbacksAggregatePartitionCountsWithoutConsumerLabels() {
        StatsCollector stats = new StatsCollector();

        // Each call can originate from a different consumer. The worker-level
        // metric intentionally aggregates all of them into one time series.
        stats.partitionsAssigned(3);
        stats.partitionsAssigned(2);
        stats.partitionsAssigned(1);
        stats.partitionsRevoked(4);
        stats.partitionsRevoked(1);

        String metrics = writeMetrics(stats);

        assertTrue(metrics.contains(
                "client_consumer_partitions_assigned_count{host=\"test-host\"} 6\n"));
        assertTrue(metrics.contains(
                "client_consumer_partitions_revoked_count{host=\"test-host\"} 5\n"));
        assertFalse("Aggregate rebalance metrics must not add consumer cardinality",
                metrics.contains("consumer=\""));
    }

    @Test
    public void rebalanceMetricsAreAlwaysPresentAndOnlyResetWithAllStats() {
        StatsCollector stats = new StatsCollector();
        String initial = writeMetrics(stats);
        assertTrue(initial.contains("client_consumer_partitions_assigned_count{host=\"test-host\"} 0\n"));
        assertTrue(initial.contains("client_consumer_partitions_revoked_count{host=\"test-host\"} 0\n"));

        stats.partitionsAssigned(2);
        stats.partitionsRevoked(1);
        stats.resetRecorders();
        assertTrue(writeMetrics(stats).contains(
                "client_consumer_partitions_assigned_count{host=\"test-host\"} 2\n"));

        stats.resetAll();
        String reset = writeMetrics(stats);
        assertTrue(reset.contains("client_consumer_partitions_assigned_count{host=\"test-host\"} 0\n"));
        assertTrue(reset.contains("client_consumer_partitions_revoked_count{host=\"test-host\"} 0\n"));
    }

    private static String writeMetrics(StatsCollector stats) {
        StringWriter writer = new StringWriter();
        stats.writeConsumerRebalanceMetrics(writer, "test-host");
        return writer.toString();
    }
}
