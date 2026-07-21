# Correctness Mode

Validates message delivery guarantees by checking for message loss, duplicates, and out-of-order delivery. Designed for regression testing Kafka versions, validating replication settings, and verifying behavior under failure injection.

## CLI Usage

```bash
dimster correctness <env> -k <kafka-config> -w <workload> [-s <system>]
```

- `-k` / `--kafka-config` -- Path to Kafka config YAML. Optional if the workload embeds its own `kafkaConfig`.
- `-w` / `--workload` -- Path to correctness workload YAML.
- `-s` / `--system` -- Label for the system under test.

## How It Works

For each topic, its set of producers and consumers are scheduled on the same worker, and share a message tracker. If the workload consists of multiple topics then the topics are spread across workers, but the producers/consumers of any given topic are always scheduled on the same worker. 

1. **Producers** send messages with sequentially numbered payloads. Each producer is pinned to the same partition in order to make ordering checks meaningful since Kafka only guarantees ordering within a partition. On each successful produce, the message is logged in the message tracker.

2. **Consumers** track received sequence numbers per producer. As messages arrive, the consumer checks each sequence number against its local model of what has been received. Each received message is registered with the message tracker.  

3. **Grace period.** After the configured test duration elapses and production stops, the system waits for `gracePeriodSeconds` to allow in-flight messages to be consumed before performing the final analysis.

4. **Detection checks:**
   - **Message loss** -- Gaps in the final sequence number stream (in the message tracker) indicate messages that were produced but never consumed.
   - **Duplicates** -- Repeated sequence numbers indicate the same message was delivered more than once. Up to `logDuplicateCount` duplicate events are logged in detail.
   - **Out-of-order** -- Non-monotonically increasing sequence numbers within a single producer's stream indicate ordering violations. Duplicates are not flagged as out-of-order.
   - **CRC integrity** -- When `enableCrc` is true, each message payload includes a CRC that is verified on consumption to detect corruption.

5. **Fail-on settings:**
   - Each type of check (loss, duplicate, out-of-order, crc) can be configured to be tolerated or cause the final result to be a fail. For example, we can detect duplicates but tolerate them in the final result. 

## correctnessConfig Fields

| Field                   | Default    | Description                                                                                                                                                                                                        |
|-------------------------|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `testMode`              | CONCURRENT | CONCURRENT/DRAIN. CONCURRENT has producers and consumers running at the same time. DRAIN has producers running for a fixed time period or backlog size, followed by consumers starting up and draining the backlog |
| `testDurationMinutes`   | 5          | Duration of the test. In DRAIN mode, the producers run for this time period (unless `backlogSizeGB` is set)                                                                                                        |
| `backlogSizeGB`   | 0          | In DRAIN mode, the producers build up a backlog of this size. When 0, uses `testDurationMinutes` instead.                                                                                                          |
| `warmupDurationMinutes` | 1          | Warmup (stats discarded)                                                                                                                                                                                           |
| `gracePeriodSeconds`    | 30         | Wait after production stops for in-flight consumption                                                                                                                                                              |
| `staticConsumerAssignment` | true       | Uses static consumer assignment to avoid duplicates during assignment (only enabled for classic consumers). When false (or share groups) normal assignment is used (according to the Kafka version).               |                                                |
| `consumerRestartSeed`    | 0          | Seed for deterministic `--restart-consumers` selection. The sequence resets for every test point.                                                                                                                  |
| `enableCrc`             | false      | Enable CRC payload integrity checks                                                                                                                                                                                |
| `sampleRateMillis`      | 10000      | Stats sampling interval                                                                                                                                                                                            |
| `logDuplicateCount`     | 100        | Max duplicate events to log in detail                                                                                                                                                                              |
| `detectDuplicates`      | true       | Check for duplicate messages                                                                                                                                                                                       |
| `detectOutOfOrder`      | true       | Check for ordering violations                                                                                                                                                                                      |
| `failOnLoss`      | true       | Whether data loss causes the test to fail                                                                                                                                                                          |
| `failOnDuplicates`      | false      | Whether duplicates causes the test to fail                                                                                                                                                                         |
| `failOnOutOfOrder`      | true       | Whether out-of-order message receipt causes the test to fail                                                                                                                                                       |
| `failOnCrc`      | true       | Whether CRC failures causes the test to fail                                                                                                                                                                       |

## Restarting consumers

During the measured phase of a `CONCURRENT` correctness test, a separate
terminal can restart a deterministic subset of consumers:

```bash
./dimster run <env> --modify --restart-consumers 10
```

Set `staticConsumerAssignment: false` for classic consumer groups. The default
manual assignment bypasses group coordination and is rejected by the live
restart action because it would not test a rebalance. Share groups always use
dynamic assignment and are eligible regardless of that setting.

The action is rejected during setup, stabilization, grace periods, between
test points, and in `DRAIN` mode. The selected names and action result are
stored in the test outcome's `consumerRestarts` array. A worker restart failure
also marks the correctness outcome as failed.

## Workload Example

```yaml
name: Correctness check with CRC

correctnessConfig:
  testMode: CONCURRENT
  testDurationMinutes: 10
  gracePeriodSeconds: 60
  detectDuplicates: true
  detectOutOfOrder: true
  enableCrc: true
  failOnLoss: true
  failOnDuplicates: false
  failOnOutOfOrder: true
  failOnCrc: true

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageSize: 100
  producersPerTopic: 6
  producerRate: 50000
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
```

## Results

The output includes:

- **Sent count** -- Total messages produced.
- **Received count** -- Total messages consumed.
- **Duplicates found** -- Number of duplicate deliveries detected.
- **Out-of-order count** -- Number of ordering violations detected.
- **Loss count** -- Number of produced messages that were never consumed.
- **Violation time series** -- `lostMessagesOverTime`,
  `duplicateCountOverTime`, `outOfOrderCountOverTime`, and
  `crcFailureCountOverTime` contain cumulative counter snapshots taken every
  `sampleRateMillis`. These arrays are complete even when detailed violation
  records are capped. Loss snapshots are provisional while consumers can still
  catch up; the final `lostMessages` scalar is authoritative after the
  grace/drain phase.

## Scenarios

Correctness workloads support scenarios for sweeping parameters:

```yaml
scenarios:
   - name: partition-scaling
     label: ["1-part", "6-part", "12-part"]
     partitionsPerTopic: [1, 6, 12]
```

## See Also

See [Workload Reference](../reference/workloads.md) for `baseWorkload` fields and scenario mechanics.
