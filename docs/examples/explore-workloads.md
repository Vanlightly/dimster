# Explore Workload Examples

This page contains annotated examples of explore limits workloads. Explore mode automatically finds the peak sustainable throughput for a given latency constraint using a three-phase algorithm: ramp, binary search, and sustain. See [Explore Limits Mode](../modes/explore-limits.md) for how the algorithm works, and [Workload Reference](../reference/workloads.md) for the complete field listing.

## Example 1: Keyed vs unkeyed, classic vs share group

```yaml
name: explore-limits-keyed-consumertype

exploreConfig:
  e2eLatencyPercentile: 75        # Which latency percentile to evaluate
  e2eLatencyThresholdMs: 25       # Target: p75 latency must stay below 25ms
  sustainLeniency: 0.1            # Sustain phase is 10% more lenient (threshold becomes 27.5ms)
  publishDelayThresholdMs: 200    # Also fail if producer-side delay exceeds 200ms
  initialRate: 2000               # Ramp starts at 2,000 msg/s, doubles each interval
  rampWarmupSeconds: 10           # Brief warmup before ramp begins
  rampIntervalSeconds: 10         # Throughput doubled every 10s during ramp
  searchDurationSeconds: 90       # Each binary search midpoint tested for 90s
  searchIntervalSeconds: 10       # Latency checked every 10s during search (3 bad = fail)
  searchMaxRetries: 3             # Retry search+sustain up to 3 times if sustain fails
  sustainDurationSeconds: 300     # Final rate must hold for 5 minutes

baseWorkload:
  topics: 20                      # 20 topics -- tests multi-topic overhead
  partitionsPerTopic: 6
  producersPerTopic: 1
  consumerGroupsPerTopic: 1       
  consumersPerGroup: 6
  messageSize: 1000
  
scenarios:
  # Compare classic vs share group with keyed messages
  - name: keyed
    label: ["keyed-classic", "keyed-sharegroup"]
    messageDistributor: "KEY_ROUND_ROBIN"       # Messages have keys (round-robin across key space)
    consumerType: [CONSUMER_GROUP, SHARE_GROUP]

  # Same comparison but without message keys
  - name: not-keyed
    label: [ "nokey-classic", "nokey-sharegroup" ]
    messageDistributor: "NO_KEY"
    consumerType: [CONSUMER_GROUP, SHARE_GROUP]
```

This runs 4 test points (2 scenarios x 2 test points each). The key fields to understand:

- `publishDelayThresholdMs` catches cases where latency looks fine on the consumer side but producers are backing up. If the delay between requesting a send and the send completing exceeds 200ms, the rate is considered unsustainable.
- `sustainLeniency: 0.1` relaxes the latency threshold by 10% during the sustain phase. This accounts for natural variance in longer runs -- a rate that marginally passes at 25ms during search might briefly hit 26ms during a 5-minute sustain.

## Example 2: Multi-dimensional scaling exploration

A comprehensive suite that explores how different dimensions (topics, partitions, producers, consumers, fan-out) affect peak throughput.

```yaml
name: explore-limits-standard-various

exploreConfig:
  e2eLatencyPercentile: 75
  e2eLatencyThresholdMs: 25
  sustainLeniency: 0.1
  publishDelayThresholdMs: 200
  initialRate: 2000
  rampWarmupSeconds: 10
  rampIntervalSeconds: 10
  searchDurationSeconds: 90
  searchIntervalSeconds: 10
  searchMaxRetries: 3
  sustainDurationSeconds: 300

baseWorkload:
  consumerType: CONSUMER_GROUP
  topics: 1
  partitionsPerTopic: 1
  producersPerTopic: 1
  consumerGroupsPerTopic: 1
  consumersPerGroup: 1
  messageSize: 1000
  messageDistributor: "NO_KEY"

scenarios:
  # How does topic count affect throughput?
  - name: topic-scaling
    label: ["1 topic", "100 topics"]
    partitionsPerTopic: 6
    producersPerTopic: 3           # Scalar: same for both test points
    consumersPerGroup: 3
    topics: [1, 100]               # Array: varies per test point

  # How does partition count affect throughput?
  - name: partition-scaling
    label: ["1 part", "100 parts", "1000 parts"]
    producersPerTopic: 16
    consumersPerGroup: [1, 100, 1000]   # Scale consumers with partitions
    partitionsPerTopic: [1, 100, 1000]

  # How does producer count affect throughput?
  - name: producer-scaling
    label: ["1 producer", "100 producers"]
    partitionsPerTopic: 24
    consumersPerGroup: 24
    producersPerTopic: [1, 100]

  # How does consumer count affect throughput?
  - name: consumer-scaling
    label: ["1 consumer", "100 consumers"]
    partitionsPerTopic: 100
    producersPerTopic: 16
    consumersPerGroup: [1, 100]

  # How does fan-out (consumer groups per topic) affect throughput?
  - name: fanout-scaling
    label: ["1x fanout", "4x fanout", "8x fanout"]
    partitionsPerTopic: 6
    producersPerTopic: 4
    consumersPerGroup: 6
    consumerGroupsPerTopic: [1, 4, 8]
```

This is a large workload: 5 scenarios with 13 test points total. At up to 30 minutes per test point, the full suite can take 4-6 hours.

The design pattern here is worth noting:

- The `baseWorkload` uses minimal values (1 topic, 1 partition, 1 producer, 1 consumer) as a clean baseline.
- Each scenario overrides only the dimensions it is testing, using scalars for "held constant" values and arrays for the dimension under test.
- This makes it easy to add or remove individual scenarios without affecting others.

## Key exploreConfig fields

| Field | Purpose | Typical value |
|---|---|---|
| `initialRate` | Starting rate for ramp phase | 2000-10000 |
| `e2eLatencyPercentile` | Which percentile to check (e.g., 75 = p75) | 75 or 99 |
| `e2eLatencyThresholdMs` | Latency ceiling in milliseconds | 10-50 |
| `publishDelayThresholdMs` | Producer-side delay ceiling | 100-500 |
| `rampIntervalSeconds` | Stats collection interval during ramp | 10 |
| `searchDurationSeconds` | Time to test each binary search midpoint | 90-120 |
| `searchIntervalSeconds` | How often latency is checked during search | 10 |
| `sustainDurationSeconds` | How long the final rate must hold | 300 |
| `sustainLeniency` | Threshold relaxation factor for sustain phase | 0.1-0.2 |
| `searchMaxRetries` | Retries if sustain fails | 1-3 |

**Estimating runtime per test point:**

- Ramp phase: 1-3 minutes (depends on how far `initialRate` is from saturation).
- Search phase: roughly `searchDurationSeconds * log2(upper_bound / lower_bound)` seconds, typically 10-20 minutes.
- Sustain phase: `sustainDurationSeconds` if it passes on the first attempt.

## Example 3: Embedded kafkaConfig

Explore runs are often the longest workloads dimster runs (hours per test point). Embedding the [Kafka client configuration](../reference/kafka-configs.md) in the workload keeps the producer / consumer settings under test attached to the workload itself, so the captured result files unambiguously document what was being explored.

```yaml
name: explore-embedded-config

exploreConfig:
  e2eLatencyPercentile: 75
  e2eLatencyThresholdMs: 25
  publishDelayThresholdMs: 200
  initialRate: 2000
  rampIntervalSeconds: 10
  searchDurationSeconds: 90
  sustainDurationSeconds: 300

baseWorkload:
  topics: 1
  partitionsPerTopic: 12
  producersPerTopic: 6
  consumerGroupsPerTopic: 1
  consumersPerGroup: 12
  messageSize: 1000

  kafkaConfig:
    replicationFactor: 3
    topicConfig:
      min.insync.replicas: 2
    producerConfig:
      acks: all
      enable.idempotence: true
      linger.ms: 5            # Small batch window -- trades a touch of latency for batching
      batch.size: 131072
    classicConsumerConfig:
      auto.offset.reset: earliest
      enable.auto.commit: false
      dimster.commit.mode: CommitAsync
      dimster.commit.linger.ms: 0
```

## Example 4: Sweeping producer settings via kafkaConfig dimensions

Use list-valued properties inside `scenarios[].kafkaConfig` to sweep a Kafka setting per test point. This example explores how `linger.ms` -- which trades latency for batching efficiency -- shifts the peak sustainable throughput.

```yaml
name: explore-linger-tradeoff

exploreConfig:
  e2eLatencyPercentile: 75
  e2eLatencyThresholdMs: 50           # Latency budget high enough that linger has room to vary
  publishDelayThresholdMs: 200
  initialRate: 5000
  rampIntervalSeconds: 10
  searchDurationSeconds: 90
  sustainDurationSeconds: 300

baseWorkload:
  topics: 1
  partitionsPerTopic: 12
  producersPerTopic: 6
  consumerGroupsPerTopic: 1
  consumersPerGroup: 12
  messageSize: 1000
  kafkaConfig:
    replicationFactor: 3
    topicConfig:
      min.insync.replicas: 2
    producerConfig:
      acks: all
      enable.idempotence: true
      batch.size: 131072

scenarios:
  - name: linger-sweep
    label: ["linger=0", "linger=5", "linger=25"]
    kafkaConfig:
      producerConfig:
        # Three test points. Each runs the full explore algorithm with this
        # producer configuration and reports the discovered peak rate.
        linger.ms: [0, 5, 25]
```

The base `kafkaConfig` (RF=3, idempotent producer, batch.size) is inherited; only `linger.ms` varies. See [Per-scenario Kafka config overrides](../reference/kafka-configs.md#per-scenario-kafka-config-overrides) for the merge semantics.

## Example 5: Sweeping latency targets via exploreConfig dimensions

The explore algorithm itself is parameterised, and any field of `exploreConfig` can be a per-test-point dimension via `scenarios[].exploreConfig`. The most useful sweep is `e2eLatencyThresholdMs` -- it answers "what's the peak rate at each latency budget?" in a single run.

```yaml
name: explore-latency-budget-curve

exploreConfig:
  e2eLatencyPercentile: 75
  publishDelayThresholdMs: 500    # Loose -- so latency, not publish delay, is the binding constraint
  initialRate: 2000
  rampIntervalSeconds: 10
  searchDurationSeconds: 90
  sustainDurationSeconds: 300

baseWorkload:
  topics: 1
  partitionsPerTopic: 12
  producersPerTopic: 6
  consumerGroupsPerTopic: 1
  consumersPerGroup: 12
  messageSize: 1000

scenarios:
  - name: latency-budget
    label: ["10ms", "25ms", "50ms", "100ms"]
    # Per-scenario overrides for exploreConfig fields. Every list-valued
    # property here must have the same length (and must match any other
    # dimension lists in the scenario).
    exploreConfig:
      e2eLatencyThresholdMs: [10, 25, 50, 100]
```

Four test points, each running the full explore algorithm with a different latency ceiling. The result is a latency-vs-throughput curve for the configured topology -- typically the cheapest way to characterise a system's tradeoff curve.

You can also combine engine-config dimensions with workload dimensions, as long as every list across the scenario shares one length. For example, `partitionsPerTopic: [6, 12, 24, 48]` paired with `e2eLatencyThresholdMs: [25, 25, 50, 50]` would produce four test points where partition count and latency target co-vary.

---

**See also:** [Explore Limits Mode](../modes/explore-limits.md), [Workload Reference](../reference/workloads.md), [Kafka Client Configuration](../reference/kafka-configs.md)
