package dev.dimster.engines.drainbacklog;

/**
 * Configuration for drain-backlog tests.
 */
public class DrainBacklogConfig {
    /** Size of backlog to build before draining (in GB). Supports fractions, e.g. 0.05 = 50MB. */
    public double backlogSizeGB = 1;

    /** Producer rate during drain phase. 0 means producers are paused (pure consumer drain). */
    public int backlogProducerRate = 0;

    /** Producer rate while building backlog. -1 means use the baseWorkload producerRate. */
    public int buildRate = -1;

    /** Stats sampling interval in milliseconds. */
    public int sampleRateMillis = 10000;

    /** Maximum time to wait for drain to complete (minutes). 0 means no limit. */
    public int drainTimeoutMinutes = 0;
}
