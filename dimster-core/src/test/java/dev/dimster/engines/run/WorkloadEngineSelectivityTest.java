package dev.dimster.engines.run;

import dev.dimster.TestResult;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class WorkloadEngineSelectivityTest {

    private static TestResult.PerformanceResults results(List<Long> sent, List<Long> consumed) {
        var r = new TestResult.PerformanceResults();
        r.sent.addAll(sent);
        r.consumed.addAll(consumed);
        return r;
    }

    @Test
    public void selectivityIsConsumedPerProducedRecordAndGroup() {
        assertEquals(0.25, WorkloadEngine.selectivity(results(List.of(1000L, 1000L), List.of(250L, 250L)), 1), 1e-9);
        // Two groups each read every output record.
        assertEquals(0.25, WorkloadEngine.selectivity(results(List.of(1000L), List.of(500L)), 2), 1e-9);
        assertEquals(1.0, WorkloadEngine.selectivity(results(List.of(3L), List.of(3L)), 1), 1e-9);
    }

    @Test
    public void nothingProducedHasNoSelectivity() {
        assertNull(WorkloadEngine.selectivity(results(List.of(0L), List.of(0L)), 1));
    }
}
