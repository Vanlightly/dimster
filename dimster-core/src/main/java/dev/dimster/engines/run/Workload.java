package dev.dimster.engines.run;

import java.util.List;

import dev.dimster.BaseWorkload;
import dev.dimster.EngineConfig;
import dev.dimster.Scenario;
import dev.dimster.availability.AvailabilityConfig;

/**
 * YAML definition for a standard benchmark workload.
 *
 * Structure:
 *   name: ...
 *   runConfig:
 *     testDurationMinutes: 5
 *     ...
 *   availabilityConfig:   # optional — enables per-partition availability probes
 *     unavailabilityThresholdMs: 5000
 *     probeTimeoutSeconds: 30
 *   baseWorkload:
 *     topics: 1
 *     partitionsPerTopic: 6
 *     ...
 *   scenarios:            # optional — if absent, runs a single test from baseWorkload
 *     - name: scaling
 *       partitionsPerTopic: [1, 10, 100]
 */
public class Workload {
    public String name;
    public EngineConfig engineConfig = new EngineConfig();
    public RunConfig runConfig = new RunConfig();

    /** Optional availability config. If non-null, per-partition probes are enabled. */
    public AvailabilityConfig availabilityConfig;

    public BaseWorkload baseWorkload = new BaseWorkload();

    /** Optional scenarios. If null/empty, a single test uses baseWorkload as-is. */
    public List<Scenario> scenarios;
}
