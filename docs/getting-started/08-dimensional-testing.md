# 08 — Dimensional testing

Page 06 ran a single fixed workload. Real benchmarking work is rarely a single point — you usually want to compare many configurations: different partition counts, batch sizes, consumer counts, message sizes, and so on.

Dimster's answer is **dimensional testing**: one workload file declares a base configuration plus one or more *scenarios* that sweep one or more fields across an array of values. Each array element is a **test point**, and Dimster runs them sequentially under a single campaign — so the results all live in one folder, with consistent labels, ready to chart side by side.

This page walks two of the smallest possible examples. The full mental model lives in [Dimensional Testing](../concepts/dimensional-testing.md); this is just enough to recognize the shape.

---

## Example 1 — sweep a single workload field

`run/workloads/quick-test/quick-run-consumer-scaling-test.yaml`:

```yaml
name: run-quick-consumer-scaling-test

runConfig:
  testDurationMinutes: 2
  warmupDurationMinutes: 1

baseWorkload:
  topics: 1
  partitionsPerTopic: 50
  messageSize: 1000
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  producersPerTopic: 6
  producerRate: 1000

scenarios:
  - name: scaling-consumers
    label: [ "1-con", "10-con", "20-con", "30-con", "40-con", "50-con" ]
    consumersPerGroup: [1, 10, 20, 30, 40, 50]
```

What's going on:

- `baseWorkload` is the default — every test point starts from these values.
- The single scenario `scaling-consumers` overrides `consumersPerGroup` with a 6-element array. That means **6 test points**, each running for the full 2 minutes back-to-back.
- `label` is also a 6-element array. Arrays inside one scenario co-vary by index, so test point 0 gets `consumersPerGroup=1` and label `"1-con"`, test point 5 gets `consumersPerGroup=50` and `"50-con"`. Labels are what show up on the charts.

Run it:

```bash
./dimster run local \
    -k run/kafka-configs/kafka_local.yaml \
    -w run/workloads/quick-test/quick-run-consumer-scaling-test.yaml
```

When it finishes, the result folder contains one row per test point and the charts plot all six side by side, labelled `1-con … 50-con`.

---

## Example 2 — sweep a nested Kafka client setting

You're not limited to top-level workload fields. Anything in the workload schema can be overridden by a scenario, including nested `kafkaConfig` fields.

`run/workloads/quick-test/quick-run-test-batch-size.yaml`:

```yaml
name: Quick run test - batch.size dimension

runConfig:
  testDurationMinutes: 2
  warmupDurationMinutes: 1

baseWorkload:
  topics: 1
  partitionsPerTopic: 6
  messageSize: 100
  consumerType: CONSUMER_GROUP
  consumerGroupsPerTopic: 1
  consumersPerGroup: 6
  producersPerTopic: 4
  producerRate: 10000
  kafkaConfig:
    replicationFactor: 3
    topicConfig:
      min.insync.replicas: 2
    producerConfig:
      acks: all
    classicConsumerConfig:
      auto.offset.reset: earliest

scenarios:
  - name: batch-size
    label: ["batch-8k", "batch-16k", "batch-32k", "batch-64k", "batch-128k", "batch-256k"]
    kafkaConfig:
      producerConfig:
        batch.size: [8192, 16384, 32768, 65536, 131072, 262144]
```

Same shape as example 1, but the swept field is buried two levels deep inside `kafkaConfig.classicConsumerConfig`. The scenario only mentions the path it's overriding — the rest of `kafkaConfig` (replication factor, acks, etc.) is inherited from the base workload.

> Heads up — this workload sets `replicationFactor: 3`, which won't run on the default single-broker `local` env. Either drop the `kafkaConfig` block from `baseWorkload`, or run it against a cluster with 3+ brokers (page 10's `localBeefy` env is still single-broker too — multi-broker is a real-cluster concern).

---

## Reference

Once you've got the hang of array overrides, most "I want to compare X across Y" benchmarks become a handful of YAML lines instead of a shell-script loop. Two scenarios with three array elements each gives you 6 test points; one scenario varying two fields together (e.g. `consumersPerGroup` and `partitionsPerTopic`) gives you a true multi-dimensional sweep — same array length, each index defining one point.

The full reference covers multi-scenario campaigns, scalar vs. array overrides, label combination rules, and the whole workload schema:

- [Dimensional Testing](../concepts/dimensional-testing.md) — the concept doc with a worked CONSUMER_GROUP vs SHARE_GROUP example.
- [Workload Reference](../reference/workloads.md) — every field you can put under `baseWorkload` or override in a scenario.

---

## Infrastructure as dimensions

You can also turn aspects of the infrastructure into dimensions. Such as the broker-count, TLS on/off, etc.

See the `run/series/README.md` to learn how to include infrastructure as dimensions.

---

## What's next

Page 09 tours the four benchmark modes — `run`, `explore`, `drain-backlog`, and `correctness`. Two of them (`explore` and `correctness`) lean on dimensional testing under the hood, so it's worth having the shape in your head before you read that page.

## If something looks wrong

- The campaign starts but only one test point runs — your scenario arrays are probably length 1, or you forgot the `scenarios:` block entirely. A workload with no scenarios is a single test point by
  design.
- "Array length mismatch" error at startup — every array inside one scenario must have the same length. If `label` has 6 entries and `consumersPerGroup` has 5, the campaign refuses to start.
- Charts label every test point `unlabelled` — the scenario is missing its `label` array. Add one entry per test point.
