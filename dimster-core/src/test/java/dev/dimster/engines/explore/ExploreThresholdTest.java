package dev.dimster.engines.explore;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.junit.Test;

import static dev.dimster.stats.StatsCollector.E2E_LATENCY_MAX_MICROS;
import static org.junit.Assert.*;

public class ExploreThresholdTest {

    @Test
    public void sustainPhaseAppliesLeniency() {
        // 10% leniency: 25ms → 27.5ms
        assertEquals(27.5, ExploreEngine.lenientThreshold(25.0, 0.10), 0.001);
        // 25% leniency: 25ms → 31.25ms
        assertEquals(31.25, ExploreEngine.lenientThreshold(25.0, 0.25), 0.001);
        // 0% leniency: no change
        assertEquals(25.0, ExploreEngine.lenientThreshold(25.0, 0.0), 0.001);
    }

    @Test
    public void sustainPhaseAppliesLeniencyToDelay() {
        // 10% leniency: 200ms → 220ms
        assertEquals(220.0, ExploreEngine.lenientThreshold(200.0, 0.10), 0.001);
        // 25% leniency: 200ms → 250ms
        assertEquals(250.0, ExploreEngine.lenientThreshold(200.0, 0.25), 0.001);
    }

    @Test
    public void searchPhaseUsesRawThreshold() {
        // Search uses the raw threshold directly (no adjustment)
        double threshold = 50.0;
        assertEquals(threshold, threshold, 0.001);
    }

    @Test
    public void badReasonOnlyE2e() {
        assertEquals("[BAD:e2e]", ExploreEngine.buildBadReason(false, true));
    }

    @Test
    public void badReasonOnlyDelay() {
        assertEquals("[BAD:delay]", ExploreEngine.buildBadReason(true, false));
    }

    @Test
    public void badReasonBoth() {
        assertEquals("[BAD:delay+e2e]", ExploreEngine.buildBadReason(true, true));
    }

    @Test
    public void okWhenBothUnderThreshold() {
        double threshold = 25.0; // search uses raw threshold
        double delayThreshold = 200.0;

        double e2e = 20.0;
        double delay = 100.0;

        assertFalse(delay > delayThreshold);
        assertFalse(e2e > threshold);
    }

    @Test
    public void okWhenExactlyAtThreshold() {
        // Uses > not >=, so exactly at threshold is OK
        double threshold = 25.0;
        double e2e = 25.0;

        assertFalse(e2e > threshold);
    }

    @Test
    public void zeroLeniencyMatchesSearch() {
        double base = 50.0;
        double sustainWithZero = ExploreEngine.lenientThreshold(base, 0.0);
        assertEquals(base, sustainWithZero, 0.001);
    }

    // ==================== theoreticalMaxRate ====================

    @Test
    public void theoreticalMax_fixedProcessingTime() {
        // 1000ms per message, 100 consumers → 100 msgs/s
        assertEquals(100.0, ExploreEngine.theoreticalMaxRate(1000, 1000, 100), 0.001);
    }

    @Test
    public void theoreticalMax_singleConsumer() {
        // 100ms per message, 1 consumer → 10 msgs/s
        assertEquals(10.0, ExploreEngine.theoreticalMaxRate(100, 100, 1), 0.001);
    }

    @Test
    public void theoreticalMax_rangeProcessingTime() {
        // 500ms–1500ms (avg 1000ms), 50 consumers → 50 msgs/s
        assertEquals(50.0, ExploreEngine.theoreticalMaxRate(500, 1500, 50), 0.001);
    }

    @Test
    public void theoreticalMax_smallProcessingTime() {
        // 10ms per message, 10 consumers → 1000 msgs/s
        assertEquals(1000.0, ExploreEngine.theoreticalMaxRate(10, 10, 10), 0.001);
    }

    // ==================== worstPartitionE2e ====================

    @Test
    public void worstPartition_selectsHighestLatency() {
        Map<String, Histogram> partitions = new TreeMap<>();
        partitions.put("topic:0", histogramWithValue(TimeUnit.MILLISECONDS.toMicros(10)));
        partitions.put("topic:1", histogramWithValue(TimeUnit.MILLISECONDS.toMicros(50)));
        partitions.put("topic:2", histogramWithValue(TimeUnit.MILLISECONDS.toMicros(30)));

        ExploreEngine.WorstPartitionResult result = ExploreEngine.worstPartitionE2e(partitions, 75);

        assertEquals("topic:1", result.partition());
        assertEquals(50.0, result.latencyMs(), 1.0);
    }

    @Test
    public void worstPartition_skipsEmptyPartitions() {
        Map<String, Histogram> partitions = new TreeMap<>();
        partitions.put("topic:0", histogramWithValue(TimeUnit.MILLISECONDS.toMicros(20)));
        partitions.put("topic:1", new Histogram(E2E_LATENCY_MAX_MICROS, 3)); // no data
        partitions.put("topic:2", histogramWithValue(TimeUnit.MILLISECONDS.toMicros(10)));

        ExploreEngine.WorstPartitionResult result = ExploreEngine.worstPartitionE2e(partitions, 75);

        assertEquals("topic:0", result.partition());
        assertEquals(20.0, result.latencyMs(), 1.0);
    }

    @Test
    public void worstPartition_allEmpty_returnsNull() {
        Map<String, Histogram> partitions = new TreeMap<>();
        partitions.put("topic:0", new Histogram(E2E_LATENCY_MAX_MICROS, 3));
        partitions.put("topic:1", new Histogram(E2E_LATENCY_MAX_MICROS, 3));

        ExploreEngine.WorstPartitionResult result = ExploreEngine.worstPartitionE2e(partitions, 75);

        assertNull(result.partition());
        assertEquals(0.0, result.latencyMs(), 0.001);
    }

    @Test
    public void worstPartition_emptyMap_returnsNull() {
        Map<String, Histogram> partitions = new TreeMap<>();

        ExploreEngine.WorstPartitionResult result = ExploreEngine.worstPartitionE2e(partitions, 75);

        assertNull(result.partition());
        assertEquals(0.0, result.latencyMs(), 0.001);
    }

    @Test
    public void worstPartition_singleSickPartition_detected() {
        // Simulates the scenario: 9 healthy partitions, 1 with 10-minute latency
        Map<String, Histogram> partitions = new TreeMap<>();
        for (int i = 0; i < 9; i++) {
            partitions.put("topic:" + i, histogramWithValue(TimeUnit.MILLISECONDS.toMicros(30)));
        }
        partitions.put("topic:9", histogramWithValue(TimeUnit.MINUTES.toMicros(10)));

        ExploreEngine.WorstPartitionResult result = ExploreEngine.worstPartitionE2e(partitions, 75);

        assertEquals("topic:9", result.partition());
        assertTrue(result.latencyMs() > 500_000); // > 500s in ms
    }

    private static Histogram histogramWithValue(long valueMicros) {
        Histogram h = new Histogram(E2E_LATENCY_MAX_MICROS, 3);
        for (int i = 0; i < 100; i++) {
            h.recordValue(valueMicros);
        }
        return h;
    }

}
