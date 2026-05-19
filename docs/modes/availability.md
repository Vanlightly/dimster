# Availability Checking

Availability checking is an optional overlay for [run mode](run.md) that monitors partition-level availability during a benchmark. It is not a standalone mode -- it is enabled by adding an `availabilityConfig` section to a run workload.

## How It Works

A special availability probe topic is created with one dedicated producer and consumer per partition. The availability producers send an availability probe message once per second. Unavailability is recorded as producer-side and consumer-side as follows:

- If the produce latency exceeds `unavailabilityThresholdMs`, it is counted as producer-side unavailability unavailability time.
- If a partition fails to accept a probe within `probeTimeoutSeconds`, it is counted as producer-side unavailability time.
- If a consumer records e2e latency above `unavailabilityThresholdMs`, it is counted as consumer-side unavailability time.

This runs alongside the normal benchmark workload, so results include both standard throughput/latency metrics and availability data.

## Configuration

Add an `availabilityConfig` section to a run workload:

```yaml
name: Availability test during broker restart

runConfig:
  testDurationMinutes: 10
  warmupDurationMinutes: 1

availabilityConfig:
  unavailabilityThresholdMs: 5000
  probeTimeoutSeconds: 30
  probePartitions: 6

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  producersPerTopic: 2
  producerRate: 10000
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
```

## availabilityConfig Fields

| Field | Default | Description                                                   |
|-------|---------|---------------------------------------------------------------|
| `unavailabilityThresholdMs` | 5000 | Publish / e2e latency above this is counted as unavailability |
| `probeTimeoutSeconds` | 30 | Timeout for individual probe requests                         |
| `probePartitions` | 1 | Number of partitions to probe for availability                |

## Results

Availability results appear in both the JSON output and generated charts:

- **Unavailability events** -- List of events with start/end timestamps and affected partition.
- **Total unavailability time** -- Sum of all unavailability event durations.
- **Percent available** -- Overall availability percentage across the test duration.

The generated charts include an availability section.

## Use Cases

- **Failover time measurement** -- Kill a broker leader and measure how long partitions are unavailable before a new leader is elected.
- **Rolling restart testing** -- Perform a broker rolling restart during the test and verify that availability stays within acceptable bounds.
- **Chaos testing** -- Combine with failure injection (network partitions, disk pressure, process kills) to measure resilience.
- **Upgrade validation** -- Compare availability characteristics across Kafka versions or configuration changes.

## See Also

- [Run Mode](run.md) -- The base mode that availability checking extends.
- See [Workload Reference](../reference/workloads.md) for `baseWorkload` fields and scenario mechanics.
