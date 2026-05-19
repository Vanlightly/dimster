package dev.dimster.engines.run;

/**
 * Configuration for standard benchmark runs (timing, warmup, backlog).
 */
public class RunConfig {
    public int testDurationMinutes = 5;
    public int warmupDurationMinutes = 1;
    public int sampleRateMillis = 10000;

    /**
     * Progressive ramp-up before warm-up. When both rampIntervals and
     * rampIntervalSeconds are > 0, the producer rate is stepped from
     * producerRate/rampIntervals up to producerRate in equal increments,
     * each held for rampIntervalSeconds. Stats collected during the ramp
     * are discarded. Defaults to 3 intervals × 20 seconds (1-minute ramp);
     * set either field to 0 to disable.
     */
    public int rampIntervals = 3;
    public int rampIntervalSeconds = 20;
}
