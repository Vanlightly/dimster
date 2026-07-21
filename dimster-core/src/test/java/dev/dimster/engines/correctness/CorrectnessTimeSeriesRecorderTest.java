package dev.dimster.engines.correctness;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Verifies the coordinator-side contract for complete correctness samples. */
public class CorrectnessTimeSeriesRecorderTest {

    @Test
    public void recordsCumulativeSnapshotsForEveryViolationType() {
        var recorder = new CorrectnessTimeSeriesRecorder();
        recorder.record(outcome(7, 1, 2, 3));
        recorder.record(outcome(4, 5, 6, 7));

        var finalOutcome = outcome(0, 5, 6, 7);
        recorder.copyTo(finalOutcome.correctness);

        assertEquals(java.util.List.of(7L, 4L),
                finalOutcome.correctness.lostMessagesOverTime);
        assertEquals(java.util.List.of(1L, 5L),
                finalOutcome.correctness.duplicateCountOverTime);
        assertEquals(java.util.List.of(2L, 6L),
                finalOutcome.correctness.outOfOrderCountOverTime);
        assertEquals(java.util.List.of(3L, 7L),
                finalOutcome.correctness.crcFailureCountOverTime);
    }

    @Test
    public void ignoresUnavailableSnapshots() {
        var recorder = new CorrectnessTimeSeriesRecorder();
        recorder.record(null);

        var finalOutcome = outcome(0, 0, 0, 0);
        recorder.copyTo(finalOutcome.correctness);

        assertEquals(java.util.List.of(), finalOutcome.correctness.lostMessagesOverTime);
        assertEquals(java.util.List.of(), finalOutcome.correctness.duplicateCountOverTime);
        assertEquals(java.util.List.of(), finalOutcome.correctness.outOfOrderCountOverTime);
        assertEquals(java.util.List.of(), finalOutcome.correctness.crcFailureCountOverTime);
    }

    @Test
    public void serializesSampleArraysInTheCorrectnessResultContract() throws Exception {
        var outcome = outcome(2, 3, 4, 5);
        outcome.correctness.lostMessagesOverTime.add(2L);
        outcome.correctness.duplicateCountOverTime.add(3L);
        outcome.correctness.outOfOrderCountOverTime.add(4L);
        outcome.correctness.crcFailureCountOverTime.add(5L);

        var json = new ObjectMapper().readTree(new ObjectMapper().writeValueAsBytes(outcome));
        var correctness = json.get("correctness");

        assertEquals(2L, correctness.get("lostMessagesOverTime").get(0).asLong());
        assertEquals(3L, correctness.get("duplicateCountOverTime").get(0).asLong());
        assertEquals(4L, correctness.get("outOfOrderCountOverTime").get(0).asLong());
        assertEquals(5L, correctness.get("crcFailureCountOverTime").get(0).asLong());
    }

    private static CorrectnessResult.TestOutcome outcome(long loss, long duplicates,
                                                          long outOfOrder, long crc) {
        var outcome = new CorrectnessResult.TestOutcome();
        outcome.correctness.lostMessages = loss;
        outcome.correctness.duplicateCount = duplicates;
        outcome.correctness.outOfOrderCount = outOfOrder;
        outcome.correctness.crcFailureCount = crc;
        return outcome;
    }
}
