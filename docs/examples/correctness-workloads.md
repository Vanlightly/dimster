# Correctness Workload Examples

This page contains annotated examples of correctness mode workloads. Correctness mode verifies message delivery guarantees by tracking every message from production through consumption and checking for data loss, duplication, and ordering violations. See [Correctness Mode](../modes/correctness.md) for how the checker works, and [Workload Reference](../reference/workloads.md) for the complete field listing.

## Example 1: Quick correctness check

A fast validation of message delivery guarantees, suitable for CI or smoke testing.

```yaml
name: Quick correctness check

correctnessConfig:
  testDurationMinutes: 2          # Short duration -- just enough to verify the pipeline
  gracePeriodSeconds: 10          # Wait 10s after producers stop for in-flight messages
  detectDuplicates: true          # Flag any message consumed more than once
  detectOutOfOrder: true          # Flag any message consumed before a lower sequence number
  enableCrc: true                 # Verify payload integrity with CRC checksums
  failOnLoss: true
  failOnDuplicates: false
  failOnOutOfOrder: true
  failOnCrc: true

baseWorkload:
  topics: 1
  partitionsPerTopic: 10
  # messageDistributor is ignored in correctness mode -- each producer is pinned
  # to a specific partition (round-robin assignment per topic), bypassing the
  # partitioner entirely. This guarantees that each partition receives a
  # single ordered stream from one producer, making out-of-order detection
  # meaningful.
  messageSize: 256                # Small messages are sufficient for correctness
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 5
  producersPerTopic: 10           # 10 producers, each keyed to one partition
  producerRate: 1000              # Moderate rate -- correctness, not throughput
```

Each producer is pinned to a specific partition (assigned round-robin within each topic) and writes there directly, bypassing Kafka's partitioner. This is required for meaningful out-of-order detection because Kafka only guarantees ordering within a partition; pinning ensures each partition sees a single ordered stream from exactly one producer.

Each message carries a sequence number and (with `enableCrc: true`) a CRC checksum. The consumer-side checker uses these to detect:

- **Data loss:** A sequence number that was produced but never consumed.
- **Duplicates:** A sequence number consumed more than once. Expected with at-least-once delivery semantics.
- **Out-of-order:** A sequence number consumed before a lower sequence number from the same producer.
- **Corruption:** CRC mismatch between the produced and consumed payload.

**Grace period:** After `testDurationMinutes` expires, producers stop but consumers continue polling for `gracePeriodSeconds`. This allows in-flight messages to arrive before the final report. Without it, messages produced near the end of the test would be incorrectly flagged as lost. Set this longer (30-60s) in high-latency environments or when testing with failure injection.

## Example 2: Embedded kafkaConfig with strict durability

The Kafka client configuration matters more in correctness mode than in any other mode -- producer acks, idempotence, isolation level (transactions not supported yet), and the consumer commit mode all directly determine whether loss, duplication, and out-of-order events are even possible. Embedding the [Kafka config](../reference/kafka-configs.md) in the workload keeps these settings tied to the test that depends on them.

```yaml
name: correctness-embedded-config

correctnessConfig:
  testMode: CONCURRENT             # Producers and consumers run in parallel
  testDurationMinutes: 10
  gracePeriodSeconds: 60           # Generous grace -- avoid false-positive loss
  detectDuplicates: true
  detectOutOfOrder: true
  enableCrc: true
  staticConsumerAssignment: true   # Pin consumers to partitions (no rebalance noise)
  failOnLoss: true
  failOnOutOfOrder: true
  failOnDuplicates: false          # At-least-once delivery -- duplicates are expected
  failOnCrc: true

baseWorkload:
  topics: 1
  partitionsPerTopic: 10
  messageSize: 256
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 5
  producersPerTopic: 10   
  producerRate: 5000

  # Embedded Kafka config: strict durability so the test result reflects the
  # delivery guarantees Kafka should provide, not the cost of weak settings.
  kafkaConfig:
    replicationFactor: 3
    topicConfig:
      min.insync.replicas: 2
    producerConfig:
      acks: all                     # Wait for all in-sync replicas
      enable.idempotence: true      # Producer-side dedup -- prevents duplicates from retries
      max.in.flight.requests.per.connection: 5
    classicConsumerConfig:
      auto.offset.reset: earliest
      enable.auto.commit: false
      isolation.level: read_committed
      dimster.commit.mode: CommitSync   # Block on each commit -- strongest guarantee
      dimster.commit.linger.ms: 0
```

With this configuration, dimster expects zero loss, zero out-of-order. 

## Example 3: Comparing acks settings via kafkaConfig dimensions

A correctness test where the only thing varying between test points is the producer's durability setting. This is the easiest way to demonstrate the difference between weak and strong delivery guarantees on a particular cluster.

```yaml
name: correctness-acks-comparison

correctnessConfig:
  testMode: CONCURRENT
  testDurationMinutes: 5
  gracePeriodSeconds: 30
  detectDuplicates: true
  detectOutOfOrder: true
  staticConsumerAssignment: true

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageSize: 256
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 3
  producersPerTopic: 6
  producerRate: 5000
  kafkaConfig:
    replicationFactor: 3
    topicConfig:
      min.insync.replicas: 2

scenarios:
  - name: acks-vs-loss
    label: ["acks=0", "acks=1", "acks=all"]
    kafkaConfig:
      producerConfig:
        # Three test points. Pair with broker / network failure injection
        # (e.g. a leader kill mid-run) to see how each acks level reacts.
        acks: [0, 1, all]
        # Idempotence is incompatible with acks<all, so it has to vary too.
        enable.idempotence: [false, false, true]
```

> Adding chaos events is on the roadmap

All three test points run to completion and produce comparable loss / duplicate / out-of-order counts in the result file. Pair this with broker or network failure injection (e.g. a leader kill mid-run) to see how each acks level reacts. With the default `failOnLoss: true` / `failOnOutOfOrder: true`, the `acks=0` and `acks=1` points will likely be marked FAILED in their result `passed` field -- read the counters, not the flag, when characterising loss.

---

**See also:** [Correctness Mode](../modes/correctness.md), [Workload Reference](../reference/workloads.md), [Kafka Client Configuration](../reference/kafka-configs.md)
