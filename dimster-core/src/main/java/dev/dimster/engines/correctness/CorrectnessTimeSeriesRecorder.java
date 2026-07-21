package dev.dimster.engines.correctness;

/**
 * Accumulates coordinator-side correctness snapshots for one test point.
 *
 * <p>Workers own the live model checkers and return aggregate snapshots. This
 * recorder belongs at the coordinator because it samples all workers after
 * aggregation, keeping one time series per test rather than per-worker series
 * that would need index-wise merging. Detailed violation records remain useful
 * for diagnosis, but these arrays are complete even when that detail is capped.
 */
final class CorrectnessTimeSeriesRecorder {

    private final CorrectnessResult.CorrectnessData samples =
            new CorrectnessResult.CorrectnessData();

    /** Record one cumulative snapshot and return the outcome for caller logging. */
    CorrectnessResult.TestOutcome record(CorrectnessResult.TestOutcome snapshot) {
        if (snapshot == null) return null;

        var correctness = snapshot.correctness;
        samples.lostMessagesOverTime.add(correctness.lostMessages);
        samples.duplicateCountOverTime.add(correctness.duplicateCount);
        samples.outOfOrderCountOverTime.add(correctness.outOfOrderCount);
        samples.crcFailureCountOverTime.add(correctness.crcFailureCount);
        return snapshot;
    }

    /** Copy the sampled series into the authoritative final worker outcome. */
    void copyTo(CorrectnessResult.CorrectnessData target) {
        target.lostMessagesOverTime.addAll(samples.lostMessagesOverTime);
        target.duplicateCountOverTime.addAll(samples.duplicateCountOverTime);
        target.outOfOrderCountOverTime.addAll(samples.outOfOrderCountOverTime);
        target.crcFailureCountOverTime.addAll(samples.crcFailureCountOverTime);
    }
}
