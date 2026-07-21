package dev.dimster.engines.correctness;

/**
 * Configuration for correctness checking.
 */
public class CorrectnessConfig {

    public enum TestMode {
        /** Producers and consumers run concurrently (default). */
        CONCURRENT,
        /** Producers build a backlog while consumers are paused, then consumers drain it. */
        DRAIN
    }

    public TestMode testMode = TestMode.CONCURRENT;
    public int testDurationMinutes = 5;
    public int gracePeriodSeconds = 30;
    public boolean detectDuplicates = true;
    public boolean detectOutOfOrder = true;
    public boolean enableCrc = false;
    public boolean staticConsumerAssignment = true;
    public int sampleRateMillis = 10000;
    public int logDuplicateCount = 100;

    /**
     * Seed used to select consumers for live restart actions. The engine
     * resets the random sequence for every test point so equivalent protocol
     * cases receive equivalent consumer-ordinal selections.
     */
    public long consumerRestartSeed = 0;

    /** Target backlog size in GB for DRAIN mode. When > 0, overrides testDurationMinutes
     *  for the backlog build phase — producers run until this size is reached instead of
     *  for a fixed duration. Supports fractions, e.g. 0.05 = 50MB.
     *  0 means use testDurationMinutes (default). */
    public double backlogSizeGB = 0;

    /** Whether data loss causes the test to fail. */
    public boolean failOnLoss = true;
    /** Whether duplicates cause the test to fail (only relevant if detectDuplicates=true). */
    public boolean failOnDuplicates = false;
    /** Whether out-of-order delivery causes the test to fail (only relevant if detectOutOfOrder=true). */
    public boolean failOnOutOfOrder = true;
    /** Whether CRC failures cause the test to fail (only relevant if enableCrc=true). */
    public boolean failOnCrc = true;
}
