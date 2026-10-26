# Drain Backlog Mode

Measures consumer catch-up performance using a two-phase approach: first build a backlog of messages, then measure how quickly consumers can drain it. Optionally, set a produce rate during the backlog drain phase.

## CLI Usage

```bash
dimster drain-backlog <env> -k <kafka-config> -w <workload> [-s <system>]
```

- `-k` / `--kafka-config` -- Path to Kafka config YAML. Optional if the workload embeds its own `kafkaConfig`.
- `-w` / `--workload` -- Path to drain-backlog workload YAML.
- `-s` / `--system` -- Label for the system under test.

## Phases

### Phase 1 -- Build

Producers write messages until the accumulated backlog reaches `backlogSizeGB`. The producer rate during this phase is controlled by `buildRate` (-1 means use the workload's `producerRate`). Consumers are not started during this phase.

### Phase 2 -- Drain

Consumers start and begin draining the backlog. Producer behavior during drain is controlled by `backlogProducerRate`:

- `0` (default) -- Producers are paused, so the drain is pure consumer throughput.
- Any positive value -- Producers continue at the specified rate, simulating a system catching up while still receiving new traffic.

Stats are collected at `sampleRateMillis` intervals throughout the drain phase, tracking consumer throughput and progress.

### With a Flink SQL stage

With a `baseWorkload.flink` block, the backlog builds on the input topics with no Flink job running. At the drain phase the consumers start on the output topics, then the Flink job starts, and the drain time is measured from when the job runs: it covers Flink processing the backlog plus the consumers reading its output. The drain counts one output record per input record, so the query must not filter; a filter keeps the drain going until `drainTimeoutMinutes`. See [Flink SQL Stage](../concepts/flink.md#drain-backlog-mode).

## drainBacklogConfig Fields

| Field | Default | Description                                                               |
|-------|---------|---------------------------------------------------------------------------|
| `backlogSizeGB` | 1       | Size of backlog to build (GB)                                             |
| `buildRate` | -1      | Producer rate while building backlog (-1 = use workload's `producerRate`) |
| `backlogProducerRate` | -1      | Producer rate during drain (-1 = pause producers)                         |
| `sampleRateMillis` | 10000   | Stats sampling interval in milliseconds                                   |
| `drainTimeoutMinutes` | 0       | Bound the time the test will run (0 unlimited)                            |

## Workload Example

```yaml
name: 50GB backlog drain

drainBacklogConfig:
  backlogSizeGB: 50
  buildRate: 200000
  backlogProducerRate: 0
  drainTimeoutMinutes: 20

baseWorkload:
  topics: 1
  partitionsPerTopic: 12
  messageSize: 1000
  producersPerTopic: 1
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 12
```

To test drain performance while producers continue at a reduced rate:

```yaml
name: 50GB backlog drain with ongoing production

drainBacklogConfig:
  backlogSizeGB: 50
  buildRate: 200000
  backlogProducerRate: 10000
  drainTimeoutMinutes: 20

baseWorkload:
  topics: 1
  partitionsPerTopic: 12
  messageSize: 1000
  producersPerTopic: 1
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 12
```

You can simulate processing time on the consumers using the `consumerProcMsMin` and `consumerProcMsMax` fields in the workload. For example, the following will limit each consumer to no more than 1000 msgs per second:

```yaml
name: 50GB backlog drain with 1ms processing time

drainBacklogConfig:
  backlogSizeGB: 50
  buildRate: 200000
  backlogProducerRate: 0
  drainTimeoutMinutes: 20

baseWorkload:
  topics: 1
  partitionsPerTopic: 12
  messageSize: 1000
  producersPerTopic: 1
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 12
  consumerProcMsMin: 1
  consumerProcMsMax: 1
```

## Results

The output includes:

- **Time to drain** -- Total elapsed time to consume the backlog.
- **Consumer throughput** -- Messages/sec and MB/s during the drain phase.
- **Drain rate over time** -- Time series showing how consumer throughput varies as the backlog shrinks.

## When to Use

- **Consumer throughput testing** -- Measure raw consumer read performance without producer contention.
- **Outage recovery** -- Evaluate how fast a system recovers from an outage-induced backlog. Simulate with `backlogProducerRate` to model ongoing traffic during catch-up.
- **Consumer configuration comparison** -- Compare fetch sizes, poll intervals, and other consumer settings by draining the same backlog with different configurations.
- **Partition scaling** -- Use scenarios to test drain performance across different partition counts.

> Note, when testing this in the cloud, take into account that some clouds offer burst network throughput which can run out. Consumers will drain the backlog as fast as they can go, potentially exceeding the instance baseline network bandwidth.

## Scenarios

Drain-backlog workloads support scenarios for sweeping parameters:

```yaml
scenarios:
  - name: partition-scaling
    label: ["1-part", "6-part", "12-part"]
    partitionsPerTopic: [1, 6, 12]
    consumersPerGroup: [1, 6, 12]
```

The drainBacklogConfig also supports this:

```yaml
scenarios:
  - name: backlog-scaling
    drainBacklogConfig:
      label: ["10-GB", "50-GB", "100-GB"]
      backlogSizeGB: [10, 50, 100]
```

## See Also

See [Workload Reference](../reference/workloads.md) for `baseWorkload` fields and scenario mechanics.
