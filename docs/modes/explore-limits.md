# Explore Limits

Automatically discovers the peak sustainable throughput for a Kafka cluster while keeping end-to-end latency under a configurable threshold. Uses a three-phase algorithm: ramp, binary search, and sustain.

## CLI Usage

```bash
dimster explore <env> -k <kafka-config> -w <workload> [-s <system>]
```

- `-k` / `--kafka-config` -- Path to Kafka config YAML. Optional if the workload embeds its own `kafkaConfig`.
- `-w` / `--workload` -- Path to explore workload YAML.
- `-s` / `--system` -- Optional label for the system under test (falls back to system field in env, or null if not defined in env).

## The Three Phases

### Phase 1 -- Ramp

Starts producing at `initialRate` msgs/sec and doubles the rate every `rampIntervalSeconds`. Each interval is short (default 5 seconds) to quickly find the approximate ceiling. The phase ends when latency at `e2eLatencyPercentile` exceeds `e2eLatencyThresholdMs` or `publishDelayThresholdMs` is exceeded. Publish delay records how on-schedule the producers are running. For example, if the target rate is 1000 msg/s, but the producers are only managing 900 msg/s, the producers will fall behind the publish schedule.

The next phase uses binary search with a lower bound of 0 and the failed rate becomes the upper bound.

> Note, if `consumerProcMsMin` and `consumerProcMsMax` are used, the ramp phase is skipped as the theoretical max consume throughput can be calculated. This theoretical max is used as the upper bound for the search phase.

### Phase 2 -- Search

Binary search between the lower bound and the upper bound. Each candidate rate runs for `searchDurationSeconds` (default 120 seconds), with latency and publish delay checked every `searchIntervalSeconds` (default 10 seconds). If three consecutive checks exceed a threshold, the rate is declared unsustainable and becomes the new upper bound. Otherwise it becomes the new lower bound. The next search iteration chooses the midpoint between lower and upper bounds.

The search converges when the gap between lower and upper bounds is within 5% of the upper bound. Higher throughputs will end with a larger final window than lower throughputs.

### Phase 3 -- Sustain

Holds the discovered peak rate for `sustainDurationSeconds` (default 300 seconds / 5 minutes). Latency is checked at the same interval as the search phase. The sustain phase is slightly more lenient -- the latency threshold is relaxed by `sustainLeniency` (default 10%). Being lenient helps as the search phase may have reached the very edge of the thresholds, so an extra buffer during the sustain phase reduces the chance of violations of the thresholds by a tiny degree.

If 80% or more of checks pass, the rate is confirmed as sustainable. If sustain fails, the upper bound is reduced to the failed sustain rate and the algorithm retries, skipping ramp and starting at the search and sustain phases (up to `searchMaxRetries` times).

## exploreConfig Fields

| Field | Default | Description                                                                                                                                                    |
|-------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `e2eLatencyPercentile` | 75     | Percentile to check (e.g., 75 = p75). Higher percentiles experience more variance causing higher chance of retries and variance in results                     |
| `e2eLatencyThresholdMs` | 50     | Max acceptable latency in ms at the given percentile                                                                                                           |
| `publishDelayThresholdMs` | 200    | Max acceptable publish delay in ms (detects when producers cannot keep up with the target rate)                                                                |
| `sustainLeniency` | 0.10   | Fraction by which sustain phase relaxes thresholds (0.10 = 10% more lenient)                                                                                   |
| `initialRate` | 100    | Starting rate for ramp-up (msgs/sec).                                                                                                                          |
| `rampWarmupSeconds` | 10     | Warmup before measurements begin (stats discarded)                                                                                                             |
| `rampIntervalSeconds` | 10     | Interval between ramp-up measurements                                                                                                                          |
| `searchDurationSeconds` | 120    | Duration of each binary search step                                                                                                                            |
| `searchIntervalSeconds` | 10     | Latency check interval during search                                                                                                                           |
| `sustainDurationSeconds` | 300    | Duration to hold peak rate for confirmation                                                                                                                    |
| `searchMaxRetries` | 3      | Max retries if sustain fails                                                                                                                                   |
| `stabilizeRate` | 0      | Between ramp and search, and between each midpoint test in search, the backlog is drained (stabilize mini-phase). This sets the producer rate during stablize. |
| `stabilizeTimeoutSeconds` | 120    | Maximum timeout for stabilize. If exceeded, the next search midpoint kicks-off anyway.                                                                         |                                                                    |
| `minViableRate` | 1 | The minimum throughput for a successful test                                                                                                                   |
| `useDeliveryLatencyForThreshold` | false | When `true`, use e2e delivery latency instead of standard e2e latency for threshold checks. See below. |

## E2E Latency vs E2E Delivery Latency

Dimster records two flavors of end-to-end latency:

- **E2E latency** -- measured when each individual record is processed inside the consumer poll loop. When `consumerProcMsMin`/`consumerProcMsMax` are configured and a single poll returns many records, the processing time of earlier records inflates the measured e2e latency of later records in the same batch.
- **E2E delivery latency** -- measured once per poll, before any per-record processing begins. This captures the time from publish to the moment the poll returned, unaffected by simulated consumer processing time.

Both metrics are always recorded and included in test results, including per-partition breakdowns when per-partition metrics are enabled. The `useDeliveryLatencyForThreshold` setting controls which one the explore algorithm uses for its threshold checks during ramp, search, and sustain phases. When consumer processing time is configured, `useDeliveryLatencyForThreshold: true` gives a more accurate picture of Kafka's delivery performance independent of client-side processing.

When both `perPartitionE2eMode` and `useDeliveryLatencyForThreshold` are true, the explore algorithm uses the worst per-partition delivery e2e latency for threshold checks -- combining per-partition sensitivity with processing-independent measurement.

## Workload Example

```yaml
name: Find peak throughput at p75 < 50ms

exploreConfig:
  e2eLatencyPercentile: 75
  e2eLatencyThresholdMs: 50
  sustainLeniency: 0.1 # sustain phase is 10% more lenient
  publishDelayThresholdMs: 200
  initialRate: 250
  rampIntervalSeconds: 10
  searchDurationSeconds: 90
  searchIntervalSeconds: 10
  sustainDurationSeconds: 300
  searchMaxRetries: 3
  rampWarmupSeconds: 10

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageSize: 1000
  producersPerTopic: 1
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
```

## Results

The output includes the peak sustainable rate (msgs/sec), throughput in MB/s, and latency percentiles at the peak rate. If the algorithm fails to find a sustainable rate after all retries, it reports that the test did not 'converge' on a sustainable rate (with some additional info).

## Scenarios

Explore workloads support scenarios, allowing you to sweep a parameter (e.g., partition count) and find the peak rate for each configuration:

```yaml
scenarios:
  - name: partition-scaling
    label: ["1-part", "6-part", "12-part", "24-part"]
    partitionsPerTopic: [1, 6, 12, 24]
    consumersPerGroup: [1, 6, 12, 24]
```

The exploreConfig can also be included:

```yaml
scenarios:
  - name: partition-scaling
    exploreConfig:
      e2eLatencyPercentile: 75
      label: ["10-ms", "50-ms", "100-ms", "200-ms", "500-ms"]
      e2eLatencyThresholdMs: [10, 50, 100, 200, 500]
```

## See Also

See [Workload Reference](../reference/workloads.md) for `baseWorkload` fields and scenario mechanics.
