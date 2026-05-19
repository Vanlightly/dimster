# Kafka Client Configuration Reference

Kafka config YAML files define **client and topic settings** used during benchmark runs. They are passed to the CLI via the `-k` flag or embedded in workloads under `baseWorkload.kafkaConfig`.

These are distinct from [broker configuration](dimster-config.md#broker-configuration), which controls Kafka server properties at deploy time.

Config files are typically stored in `run/kafka-configs/`. It is also possible to embed a kafkaConfig section in a workload file.

---

## Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | -- | Configuration name, used in logging and result metadata. |
| `replicationFactor` | short | `0` | Replication factor for topics created by the benchmark. |
| `topicConfig` | map | `null` | Topic-level configuration properties (applied at topic creation time). |
| `commonConfig` | map | `null` | Properties applied to both producers and consumers. Typically includes `bootstrap.servers`. |
| `producerConfig` | map | `null` | Producer-specific Kafka client properties. |
| `classicConsumerConfig` | map | `null` | Consumer properties for classic (group-based) consumers. |
| `shareConsumerConfig` | map | `null` | Consumer properties for share group consumers. |

---

## Map Format

Each config field is a YAML map of `key: value` pairs. Keys are Kafka client/topic property names; values are coerced to strings before being passed to the Kafka client:

```yaml
commonConfig:
  request.timeout.ms: 120000

producerConfig:
  acks: all
  enable.idempotence: false
  linger.ms: 1
  batch.size: 131072
```

Property keys that contain dots (the Kafka convention) are valid YAML keys without quoting. Values that look like numbers, booleans, or strings are all accepted -- they are converted to strings via `Object.toString()` before being handed to Kafka.

A value can also be a YAML list, which expresses a per-test-point dimension when used inside a scenario override (see [Per-scenario Kafka config overrides](#per-scenario-kafka-config-overrides) below).

---

## Property Inheritance

Properties are layered in this order:

1. `commonConfig` is applied first.
2. `producerConfig` is applied on top of `commonConfig` for producers. `classicConsumerConfig` or `shareConsumerConfig` is applied on top of `commonConfig` for consumers. Keys present in the more specific map override the common value.
3. Key and value serializers/deserializers are set automatically and cannot be overridden:
   - Producers: `StringSerializer` (key), `ByteArraySerializer` (value)
   - Consumers: `StringDeserializer` (key), `ByteArrayDeserializer` (value)

---

## Dimster Consumer Properties

These properties are read by the dimster consumer wrappers, not by the Kafka client itself. They control how and when offsets / acknowledgements are committed when the wrapper is responsible for committing, and how long each `consumer.poll(...)` call blocks waiting for records.

| Property | Applies to | Allowed values | Default | Description |
|----------|------------|----------------|---------|-------------|
| `dimster.commit.mode` | `classicConsumerConfig` | `CommitAsync`, `CommitSync` | `CommitAsync` | How the wrapper commits offsets. Ignored when `enable.auto.commit=true` (the Kafka client handles commits itself). |
| `dimster.commit.mode` | `shareConsumerConfig` | `Poll`, `CommitAsync`, `CommitSync` | `Poll` | How the wrapper acknowledges share-group records. `Poll` lets the share consumer commit implicitly on each poll -- the wrapper makes no explicit commit call. `CommitAsync` / `CommitSync` call the matching method on `KafkaShareConsumer` after each batch. |
| `dimster.commit.linger.ms` | both consumer configs | non-negative integer | `0` | Minimum milliseconds between explicit commits. With the default `0`, the wrapper commits after every non-empty poll. With a higher value, commits are skipped until at least this many ms have passed since the last commit. Has no effect on classic consumers when `enable.auto.commit=true`, and no effect on share consumers when `dimster.commit.mode=Poll`. |
| `dimster.poll.timeout.ms` | both consumer configs | non-negative integer | `100` | Timeout passed to `consumer.poll(Duration)` on every poll iteration. Controls how long each poll blocks waiting for records when none are immediately available. Lower values reduce shutdown latency; higher values reduce CPU spent on empty polls. |

### Mode semantics

- **`CommitAsync`** -- non-blocking. Commit failures are surfaced via the consumer callback but do not stall the poll loop.
- **`CommitSync`** -- blocking. The classic wrapper passes a 30s timeout; the share wrapper uses the share consumer's default. The time spent committing is recorded as a sync-commit latency sample.
- **`Poll`** (share consumers only) -- no commit call is made by the wrapper. The share consumer commits acknowledgements as part of its next poll.

### When linger applies

The wrapper records the timestamp of its last commit and only issues the next commit when `now - lastCommit >= dimster.commit.linger.ms`. Use a non-zero value to amortise commit overhead across polls when commit-per-poll is too expensive; leave it at `0` to commit as often as possible.

---

## Dimster Producer Properties

These properties are read by the dimster producer wrapper, not by the Kafka client itself. They turn on transactional-producer mode and control when each transaction is committed. Like the consumer-side `dimster.*` keys, they live inside `producerConfig` and are passed through the same property bag (the Kafka client logs an "unknown config" warning for them, which is the accepted pattern).

| Property | Allowed values | Default | Description |
|----------|----------------|---------|-------------|
| `dimster.transactions` | `true`, `false` | `false` | Enables transactional producer mode. When `true`, each producer is given a unique `transactional.id` (auto-generated as `dimster-{hostname}-{topic}-{producerIdx}`, with a `-p{partition}` suffix in correctness mode), `initTransactions()` is called once at startup, and sends are wrapped in `beginTransaction` / `commitTransaction` batches. |
| `dimster.transactions.messages.per.txn` | positive integer | -- | Commit when this many messages have been sent in the current transaction. At least one of this and `dimster.transactions.max.txn.duration.ms` must be set when transactions are enabled. Sweepable across scenarios as a list value, e.g. `[10, 100, 1000]`. |
| `dimster.transactions.max.txn.duration.ms` | positive integer | -- | Commit when this many milliseconds have elapsed since `beginTransaction()`. At least one of this and `dimster.transactions.messages.per.txn` must be set when transactions are enabled. Sweepable across scenarios. |

### Commit-trigger semantics

When both `messages.per.txn` and `max.txn.duration.ms` are set, **OR semantics** apply -- the transaction is committed whenever the first trigger fires. Setting only one disables the other. The time clock starts at `beginTransaction()` (not at producer startup), so it measures the actual transaction window, not idle wait time.

The time check is evaluated only inside the producer send loop. At very low send rates this becomes "max time between commits *while sending actively*" -- an idle producer holds an open transaction until its next send arrives. This is acceptable for the benchmarking workloads dimster targets; the same property already holds for the count-based trigger.

### Auto-applied related properties

When `dimster.transactions=true`, dimster sets two related properties on your behalf if you have not set them yourself. Explicit user values always win.

- **`enable.idempotence=true`** on the producer. Idempotence is mandatory for transactional producers; recent Kafka clients auto-enable it but dimster sets it explicitly so the produced `Properties` are self-describing in logs.
- **`isolation.level=read_committed`** on `classicConsumerConfig` and `shareConsumerConfig`. Without this, a `read_uncommitted` consumer would surface aborted/uncommitted records and the workload's end-to-end latency would no longer reflect what a real EOS reader sees. Override by setting `isolation.level` explicitly in the relevant consumer config map.

### What an aborted transaction looks like

`commitTransaction()` flushes all in-flight sends before issuing the commit and throws `KafkaException` if any send failed. The producer wrapper treats any commit exception as an abort: it calls `abortTransaction()`, increments the per-host `client_producer_transactions_aborted_count` Prometheus counter, and continues the loop with a fresh transaction. Successful commits increment `client_producer_transactions_committed_count` and contribute a sample to `client_producer_transaction_commit_latency`.

---

## Producer Compression

Compression is a standard Kafka producer property, configured via `compression.type` in `producerConfig`. The producer compresses each record batch before sending it to the broker; the broker stores the batch as-is and the consumer decompresses on read. No dimster-specific code is involved — this is pure Kafka client behaviour.

### Available algorithms

| Algorithm | `compression.type` | CPU cost | Compression ratio | Notes |
|-----------|---------------------|----------|--------------------|-------|
| None | `none` | -- | 1:1 | Default. Use to isolate raw broker and network throughput. |
| Snappy | `snappy` | Low | Moderate | Balanced speed and ratio. Good general-purpose choice. |
| LZ4 | `lz4` | Low | Moderate | Fastest compressor; slightly lower ratio than Snappy on some payloads. Best for throughput benchmarks where CPU overhead must stay minimal. |
| Zstandard | `zstd` | Medium | High | Best ratio of the fast codecs. Slightly higher CPU than LZ4/Snappy but still suitable for high-throughput workloads. Tunable via `compression.zstd.level` (Kafka >= 2.1). |
| GZIP | `gzip` | High | High | Highest CPU cost. Use when compression ratio matters more than producer throughput (e.g. measuring broker disk savings). |

### Choosing an algorithm

- **Throughput benchmarks**: prefer `lz4` or `zstd`. Both have low enough CPU overhead that the producer is unlikely to become CPU-bound, while still reducing network and broker I/O.
- **Compression-ratio experiments**: use `zstd` or `gzip` and compare against `none` to quantify the I/O savings.
- **Baseline comparison**: run with `none` first to establish raw throughput, then add compression to measure the trade-off.

### Batching affects compression efficiency

Compression operates on whole record batches, so `linger.ms` and `batch.size` directly affect the ratio. A single-record batch compresses poorly; a full 128 KB batch compresses well. When benchmarking compression, set `linger.ms` to at least `5`–`10` and use a reasonable `batch.size` (the default 16384 is fine for moderate rates; increase to 131072 or higher for high-throughput tests).

### Sweeping compression types

`compression.type` is sweepable as a list value, so you can compare algorithms in a single workload run:

```yaml
scenarios:
  - name: compression-sweep
    label: ["none", "snappy", "lz4", "zstd"]
    kafkaConfig:
      producerConfig:
        compression.type: [none, snappy, lz4, zstd]
```

### Inline example

```yaml
producerConfig:
  acks: all
  compression.type: lz4
  linger.ms: 10
  batch.size: 131072
```

---

## Bootstrap Server Injection

The `bootstrap.servers` property is automatically managed by the CLI:

- **Full deploy mode**: The CLI discovers bootstrap servers from the Kafka StatefulSet headless service (e.g. `kafka-0.kafka-headless.dimster.svc.cluster.local:9092,...`).
- **External deploy mode**: The CLI uses the `connection` section's `bootstrap.servers` value from the environment's [dimster-config.yaml](dimster-config.md).

If `commonConfig` already contains a `bootstrap.servers` entry it is replaced with the injected value. Additional connection properties (`security.protocol`, `sasl.mechanism`, etc.) from the environment's `connection` section are appended to `commonConfig` only if a key with the same name is not already present. If the Kafka config file omits `commonConfig` entirely, one is synthesised containing just the connection properties.

This means you do not need to set `bootstrap.servers` in your Kafka config files -- it is always injected at runtime.

---

## Config Merging

When a workload embeds a `kafkaConfig` in its `baseWorkload` and a standalone config is also passed via `-k`, they are merged at the property (key) level:

- `name` and `replicationFactor` from the embedded config override the file-based values when set.
- For each map field (`topicConfig`, `commonConfig`, `producerConfig`, `classicConsumerConfig`, `shareConsumerConfig`), the maps are merged: keys present in both take the embedded value; keys only in the file-based map are preserved.

This allows workloads to override individual properties while inheriting everything else from the base configuration.

---

## Examples

### Production-safe config (acks=all, RF=3)

```yaml
name: kafka-safe

replicationFactor: 3

commonConfig:
   request.timeout.ms: 120000

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

shareConsumerConfig:
   dimster.commit.mode: Poll
   dimster.commit.linger.ms: 0
```

### Production-safe with sync flush

```yaml
name: kafka-safe

replicationFactor: 3

commonConfig:
   request.timeout.ms: 120000

topicConfig:
   min.insync.replicas: 2
   flush.ms: 10000

producerConfig:
   acks: all
   enable.idempotence: true

classicConsumerConfig:
   auto.offset.reset: earliest
   enable.auto.commit: false
   dimster.commit.mode: CommitAsync
   dimster.commit.linger.ms: 0

shareConsumerConfig:
   dimster.commit.mode: Poll
   dimster.commit.linger.ms: 0

```

### Minimal local config (single broker)

```yaml
name: kafka-local

replicationFactor: 1

topicConfig:
  min.insync.replicas: 1

classicConsumerConfig:
  auto.offset.reset: earliest
  enable.auto.commit: false
  dimster.commit.mode: CommitAsync
  dimster.commit.linger.ms: 0

shareConsumerConfig:
  dimster.commit.mode: CommitAsync
  dimster.commit.linger.ms: 0
```

### Transactional producer (count and time triggers)

```yaml
name: kafka-transactions

replicationFactor: 3

topicConfig:
  min.insync.replicas: 2

producerConfig:
  acks: all
  dimster.transactions: true
  dimster.transactions.messages.per.txn: 100
  dimster.transactions.max.txn.duration.ms: 200

classicConsumerConfig:
  auto.offset.reset: earliest
  enable.auto.commit: false
  dimster.commit.mode: CommitAsync
```

With both triggers set, each transaction is committed when 100 messages have been queued *or* 200 ms have elapsed since `beginTransaction()`, whichever comes first. `enable.idempotence=true` on the producer and `isolation.level=read_committed` on the consumer are auto-applied (see [Auto-applied related properties](#auto-applied-related-properties)).

### Embedded in a workload

Kafka config can be embedded directly inside a workload YAML under `baseWorkload.kafkaConfig`, eliminating the need for a separate `-k` file:

```yaml
name: my-benchmark

runConfig:
  testDurationMinutes: 5

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  producerRate: 50000
  kafkaConfig:
   replicationFactor: 3
   topicConfig:
      min.insync.replicas: 2
   producerConfig:
      acks: all
```

If both an embedded config and a `-k` file are provided, they are merged as described above.

### Per-scenario Kafka config overrides

A scenario's `kafkaConfig` is a single object (not a list). To vary a property across test points, give that property a YAML list whose length matches the scenario's other dimensions:

```yaml
scenarios:
  - name: acks-comparison
    label: ["acks=1", "acks=all"]
    kafkaConfig:
      producerConfig:
        acks: [1, all]
```

The scenario expander resolves each test point by picking the value at the matching index from every list-valued property. Scalar values inside the same map are reused on every test point. All list-valued properties across all maps in the scenario must share the same length.

### Compression comparison

Sweep compression algorithms to compare throughput and latency trade-offs in a single run:

```yaml
name: compression-comparison

runConfig:
  testDurationMinutes: 5

baseWorkload:
  topics: 1
  partitionsPerTopic: 10
  producerRate: 50000
  messageSize: 500
  kafkaConfig:
    replicationFactor: 3
    topicConfig:
      min.insync.replicas: 2
    producerConfig:
      acks: all
      linger.ms: 10
      batch.size: 131072

scenarios:
  - name: compression-type
    label: ["none", "snappy", "lz4", "zstd"]
    kafkaConfig:
      producerConfig:
        compression.type: [none, snappy, lz4, zstd]
```

Batching properties (`linger.ms`, `batch.size`) are set on the base workload so the comparison isolates the effect of the compression algorithm rather than batch-formation differences.
