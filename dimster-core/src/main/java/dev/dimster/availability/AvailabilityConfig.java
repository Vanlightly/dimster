package dev.dimster.availability;

/**
 * Configuration for availability checking.
 */
public class AvailabilityConfig {
    public int unavailabilityThresholdMs = 5000;
    public int probeTimeoutSeconds = 30;
    public int probePartitions = 1;
}
