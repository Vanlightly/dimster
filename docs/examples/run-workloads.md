# Run Workload Examples

This page contains annotated examples of run mode workloads. Run mode executes a fixed-rate throughput test and measures end-to-end latency. See [Run Mode](../modes/run.md) for how the mode works, and [Workload Reference](../reference/workloads.md) for the complete field listing.

## Example 1: Simple single-point test

The simplest useful workload -- one topic, one producer rate, no scenarios. Good for smoke testing a deployment.

```yaml
name: Quick run test

runConfig:
  testDurationMinutes: 2       # How long to measure after warmup
  warmupDurationMinutes: 1     

availabilityConfig:              # Optional: monitor partition availability in parallel
  unavailabilityThresholdMs: 5000  # A partition is "unavailable" if no probe response in 5s
  probeTimeoutSeconds: 30          # Total timeout for the probe setup
  probePartitions: 6               # Number of partitions on the dedicated probe topic

baseWorkload:
  topics: 1
  partitionsPerTopic: 2
  messageDistributor: "KEY_ROUND_ROBIN"  # Round-robin across a set of 10k keys
  messageSize: 1000              # 1 KB per message
  consumerType: CONSUMER_GROUP          # Standard consumer group (vs SHARE_GROUP)
  consumerGroupsPerTopic: 1
  consumersPerGroup: 2           # Match partition count for 1:1 consumer-to-partition
  producersPerTopic: 1
  producerRate: 2000             # 2,000 msg/s = ~2 MB/s with 1 KB messages
```

Because there are no `scenarios`, this runs a single test point. The result is one JSON file containing throughput and latency percentiles.

The `availabilityConfig` block is optional. When present, a separate probe topic is created and continuously monitored in parallel with the main workload. This is useful during chaos experiments (broker kills, rolling restarts) to measure failover times without interfering with the main test.

## Example 2: Scaling producer rate with scenarios

Uses a single scenario to sweep through six producer rates, measuring latency at each throughput level.

```yaml
name: run-quick-scaling-test

runConfig:
  testDurationMinutes: 2
  warmupDurationMinutes: 1      

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageDistributor: "NO_KEY"
  messageSize: 1000
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6           # One consumer per partition
  producersPerTopic: 6           # Multiple producers to avoid single-producer bottleneck
  producerRate: 50000            # Default rate (overridden by scenario)

scenarios:
  - name: producer-rate
    # Labels appear in charts and result filenames
    label: [ "100k", "150k", "200k", "250k", "300k", "350k" ]
    # Array values: the Nth test point uses the Nth value
    producerRate: [100000, 150000, 200000, 250000, 300000, 350000]
```

This creates 6 sequential test points. Between each, topics are torn down and recreated. Each test point runs for 2 minutes after a 1-minute warmup. Total wall-clock time: roughly 18 minutes.

The `producerRate` in `baseWorkload` (50000) is overridden by the scenario array, so the base value serves as a fallback if you comment out the scenario for quick debugging.

## Example 3: Multi-dimensional scaling

Two scenarios test different dimensions of the same base workload.

```yaml
name: quick-run-scale-topics-and-partitions

runConfig:
  testDurationMinutes: 2
  warmupDurationMinutes: 1

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageDistributor: "NO_KEY"
  messageSize: 1000
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
  producersPerTopic: 1
  producerRate: 2000

scenarios:
  # Scenario 1: vary topic count while keeping 6 partitions per topic
  - name: N-topics-6-parts
    label: [ "1-topic", "10-topics" ]
    partitionsPerTopic: 6         # Scalar: same value for all test points
    topics: [1, 10]               # Array: varies per test point

  # Scenario 2: vary partition count (and consumer count to match)
  - name: 1-topic-N-parts
    label: ["1-part", "10-parts"]
    topics: 1                     # Scalar: always 1 topic
    consumersPerGroup: [1, 10]    # Scale consumers with partitions
    partitionsPerTopic: [1, 10]
```

Key points about scenarios:

- **Scalar fields** in a scenario (like `partitionsPerTopic: 6`) apply the same value to every test point in that scenario.
- **Array fields** must all have the same length. The Nth test point takes the Nth value from each array.
- **Multiple scenarios** run sequentially. This workload runs 4 test points total (2 per scenario).
- **Fields not overridden** in the scenario inherit from `baseWorkload`.

## Example 4: Share group consumer scaling

Tests how share group throughput scales with consumer count at a fixed producer rate.

```yaml
name: run-quick-test-share

runConfig:
  testDurationMinutes: 25        # Longer test for stable measurements
  warmupDurationMinutes: 5

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageDistributor: "NO_KEY"
  messageSize: 1000
  consumerType: SHARE_GROUP      
  consumerGroupsPerTopic: 1
  consumersPerGroup: 1
  producersPerTopic: 1
  producerRate: 100000           # 100k msg/s -- high enough to saturate at low consumer counts

scenarios:
  - name: cons-per-group
    label: [ "1-con", "2-con", "4-con", "8-con", "16-con", "32-con", "64-con" ]
    consumersPerGroup: [1, 2, 4, 8, 16, 32, 64]
```

With `SHARE_GROUP`, consumers are not bound to specific partitions. Messages are distributed dynamically, so scaling beyond the partition count (6 in this case) can still improve throughput -- unlike `CONSUMER_GROUP` consumer groups where adding consumers beyond the partition count has no effect.

This workload runs 7 test points at 30 minutes each (25 + 5 warmup), for a total of roughly 3.5 hours.

## Example 5: Embedded kafkaConfig (no -k file needed)

Workloads can embed a [Kafka client configuration](../reference/kafka-configs.md) directly under `baseWorkload.kafkaConfig`. This makes the workload self-contained -- no `-k` flag required at the CLI -- and is the easiest way to keep client settings travelling with the test that depends on them.

```yaml
name: run-with-embedded-config

engineConfig:
  perPartitionMetrics: true       # Useful when partition skew matters
  cooldownMinutes: 1              # Let metric scrapers catch the final samples

runConfig:
  testDurationMinutes: 5
  warmupDurationMinutes: 1

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
  producersPerTopic: 6
  producerRate: 50000
  messageSize: 1000

  # Inline Kafka client + topic config. Replaces what would otherwise live in
  # a separate file passed via `-k`. If both an embedded config and a `-k`
  # file are provided, they are merged at the property level (embedded wins).
  kafkaConfig:
    replicationFactor: 3
    topicConfig:
      min.insync.replicas: 2
    producerConfig:
      acks: all
      enable.idempotence: true
    classicConsumerConfig:
      auto.offset.reset: earliest
      enable.auto.commit: false
      dimster.commit.mode: CommitAsync
      dimster.commit.linger.ms: 0
```

## Example 6: Sweeping producer settings via kafkaConfig dimensions

A scenario's `kafkaConfig` is a single object, but individual property values inside its config maps can be lists. Each list element becomes the value for the corresponding test point, exactly like any other scenario dimension.

This example fixes the producer rate and varies only the durability settings, so the result chart shows the latency cost of `acks=all` + idempotence vs `acks=1`.

```yaml
name: run-acks-tradeoff

runConfig:
  testDurationMinutes: 5
  warmupDurationMinutes: 1

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
  producersPerTopic: 6
  producerRate: 100000
  messageSize: 1000
  kafkaConfig:
    replicationFactor: 3
    topicConfig:
      min.insync.replicas: 2

scenarios:
  - name: acks-tradeoff
    label: ["acks=1", "acks=all+idempotent"]
    kafkaConfig:
      producerConfig:
        # Two list-valued properties: the lengths must match (and must match
        # any other dimension lists in the scenario).
        acks: [1, all]
        enable.idempotence: [false, true]   # idempotence requires acks=all
        # Scalar values inside the same map are reused on every test point.
        linger.ms: 1
```

Two test points are produced. The base `kafkaConfig` (RF=3, MIR=2) is inherited; only the `producerConfig` keys listed in the scenario are overridden, and they're overridden per test point. See [Per-scenario Kafka config overrides](../reference/kafka-configs.md#per-scenario-kafka-config-overrides) for the merge rules.

---

**See also:** [Run Mode](../modes/run.md), [Workload Reference](../reference/workloads.md), [Availability Mode](../modes/availability.md), [Kafka Client Configuration](../reference/kafka-configs.md)
