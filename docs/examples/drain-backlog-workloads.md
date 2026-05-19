# Drain Backlog Workload Examples

This page contains annotated examples of drain backlog workloads. Drain backlog mode measures consumer catch-up performance by first building a backlog of messages, then measuring how quickly consumers can drain it. See [Drain Backlog Mode](../modes/drain-backlog.md) for how the mode works, and [Workload Reference](../reference/workloads.md) for the complete field listing.

## Example 1: Pure consumer drain (no producer load)

Builds a 50 GB backlog, then measures how fast consumers can drain it with producers completely paused.

```yaml
name: 50GB drain-backlog test

drainBacklogConfig:
  backlogSizeGB: 50             # Build 50 GB of data before draining
  backlogProducerRate: 0        # 0 means producers are paused during drain (pure consumer test)
  buildRate: 200000             # Produce at 200k msg/s during build phase (~200 MB/s at 1 KB)
  sampleRateMillis: 10000       # Sample consumer progress every 10 seconds
  drainTimeoutMinutes: 30       # end the test at 30 minutes if not drained yet

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageDistributor: "NO_KEY"
  messageSize: 1000
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 1           # Single consumer -- tests sequential drain throughput
  producersPerTopic: 6           # 6 producers for fast build phase
```

The test runs in two phases:

1. **Build phase:** 6 producers send at a combined 200,000 messages/second until 50 GB of data exists in the topic. At ~200 MB/s, this takes roughly 4 minutes.
2. **Drain phase:** Consumers start reading from the beginning. With `backlogProducerRate: 0`, producers are fully paused, so the backlog only shrinks. Dimster samples progress every 10 seconds and reports total drain time and throughput.

Using a single consumer (`consumersPerGroup: 1`) with 6 partitions means the consumer must drain all partitions sequentially. This measures worst-case single-consumer throughput. For maximum drain speed, set `consumersPerGroup` equal to `partitionsPerTopic`.

## Example 2: Drain with continued producer load

Same backlog size, but producers continue sending during the drain phase -- simulating a real-world catch-up scenario.

```yaml
name: 50GB drain-backlog with producer load test

drainBacklogConfig:
  backlogSizeGB: 50
  backlogProducerRate: 50000    # Producers continue at 50k msg/s during drain
  buildRate: 200000             # Build phase still at full speed
  sampleRateMillis: 10000
  drainTimeoutMinutes: 60

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageDistributor: "NO_KEY"
  messageSize: 1000
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 1
  producersPerTopic: 6
```

The only difference from Example 1 is `backlogProducerRate: 50000`. During the drain phase, producers continue sending at 50,000 messages/second. Consumers must drain faster than this rate, otherwise the backlog will never clear and the test will not complete.

This simulates the common scenario where consumers fell behind (e.g., due to a deployment or outage) and need to catch up while the system remains under production load.

## Example 3: Scaling backlog sizes with scenarios

A constructed example showing how to compare drain performance across backlog sizes.

```yaml
name: Backlog size dimension

drainBacklogConfig:
  buildRate: 200000
  backlogProducerRate: 0         # Pure consumer drain for each size
  sampleRateMillis: 5000         # Finer granularity for shorter drains
  drainTimeoutMinutes: 60

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageSize: 1000
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6           # One consumer per partition for max parallelism
  producersPerTopic: 6

scenarios:
  - name: backlog-sizes
    label: ["1GB", "10GB", "50GB"]
    drainBacklogConfig:
      backlogSizeGB: [1, 10, 50]
```

This runs 3 test points. Each builds a different backlog size, drains it, then tears down topics before the next.

The results reveal whether drain throughput degrades as backlog size grows. Small backlogs (1-5 GB) often fit entirely in the OS page cache, resulting in very fast drains. Larger backlogs force disk reads and give more realistic numbers for production-scale catch-up scenarios.

## Example 4: Embedded kafkaConfig and a skewed partition size

```yaml
name: drain-skewed-partitions

engineConfig:
  perPartitionMetrics: true       # Drain progress per partition is often what you want to inspect
  perConsumerMetrics: true
  cooldownMinutes: 1

drainBacklogConfig:
  backlogSizeGB: 5
  backlogProducerRate: 0          # Pure drain
  buildRate: 200000
  sampleRateMillis: 10000
  drainTimeoutMinutes: 20

baseWorkload:
  topics: 1
  partitionsPerTopic: 10
  messageDistributor: "PARTITION_ZIPF"   # Skew the backlog -- some partitions much hotter than others
  messageDistributorSkew: 1.5
  messageSize: 1000
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 5 
  producersPerTopic: 20

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
## Example 5: Sweeping share-consumer commit modes via kafkaConfig dimensions

For share groups, [`dimster.commit.mode`](../reference/kafka-configs.md#dimster-consumer-properties) controls whether the wrapper commits acknowledgements implicitly (`Poll`), asynchronously (`CommitAsync`), or synchronously (`CommitSync`). The choice has a noticeable effect on drain throughput; sweeping the three modes on the same backlog quantifies it.

```yaml
name: drain-share-commit-modes

drainBacklogConfig:
  backlogSizeGB: 5
  backlogProducerRate: 0
  buildRate: 200000
  sampleRateMillis: 10000
  drainTimeoutMinutes: 30

baseWorkload:
  topics: 1
  partitionsPerTopic: 12
  messageSize: 1000
  consumerType: SHARE_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
  producersPerTopic: 6
  kafkaConfig:
    replicationFactor: 3
    topicConfig:
      min.insync.replicas: 2
    producerConfig:
      acks: all
      enable.idempotence: true
    shareConsumerConfig:
      max.poll.records: 500
      share.acknowledgement.mode: implicit
      share.acquire.mode: batch_optimized
      dimster.commit.mode: Poll

scenarios:
  - name: commit-mode-dimension
    label: ["poll", "commit-async", "commit-sync"]
    kafkaConfig:
      shareConsumerConfig:
        dimster.commit.mode: [Poll, CommitAsync, CommitSync]
```

Three test points, each rebuilding the same 5 GB backlog and draining it under one commit mode. Result files will show drain time and throughput per mode, side by side.

The same pattern works for any other consumer-side dimension that affects drain performance -- e.g. `max.poll.records: [100, 500, 2000]`, `fetch.min.bytes: [1, 65536]`, etc. Lists across `kafkaConfig` and any other scenario fields must share a single length.

---

## Design considerations

**Build rate vs drain rate:** The `buildRate` only affects how long the build phase takes. It does not affect drain performance. Set it high to minimize build time, but stay within the cluster's write capacity.

**Consumer count:** For maximum drain throughput with `CONSUMER_GROUP` consumer groups, use as many consumers as partitions. Each consumer drains one partition in parallel. Adding consumers beyond the partition count provides no benefit. With `SHARE_GROUP`, consumers can scale beyond the partition count.

**Backlog size and disk:** Size the backlog relative to available broker memory for meaningful results. If the backlog fits in the page cache, you are measuring memory read speed, not disk throughput.

**Sample rate:** `sampleRateMillis` controls reporting granularity. Use 5000 (5 seconds) for small backlogs where the drain completes quickly, and 10000 (10 seconds) or higher for large backlogs where finer sampling adds noise.

**See also:** [Drain Backlog Mode](../modes/drain-backlog.md), [Workload Reference](../reference/workloads.md), [Kafka Client Configuration](../reference/kafka-configs.md)
