# 09 — The four benchmark modes

Dimster bundles four benchmark modes. You met `run` on page 06. This page introduces the other three with the smallest workload from `run/workloads/quick-test/` for each, so you can try them all on your existing local cluster.

| Mode            | Use case                                               |
|-----------------|--------------------------------------------------------|
| `run`           | Fixed-rate throughput + end-to-end latency test points |
| `explore`       | Find the maximum sustainable throughput under an SLO   |
| `drain-backlog` | Measure how fast consumers catch up after a backlog    |
| `correctness`   | Detect data loss, duplicates, and ordering violations  |

> All four examples assume you've completed pages 04–05 (a local Kubernetes cluster up, `dimster deploy local` already run).

---

## Run — fixed rate, fixed duration

Set a throughput target, hold it for a duration, measure latency. The baseline benchmark you've already used.

```bash
./dimster run local \
    -k run/kafka-configs/kafka_local.yaml \
    -w run/workloads/quick-test/quick-run-test.yaml
```

The `quick-run-test.yaml` workload is detailed on [page 06](06-first-benchmark.md): 1 topic, 60 partitions, 1000 msg/s for 2 minutes.

**What the result tells you**: end-to-end latency percentiles and whether producers and consumers can hold the requested rate.

### Run with availability checking

Run mode has an optional **availability** overlay. Add an `availabilityConfig` block to a run workload and the coordinator probes individual partitions throughout the test. A partition that doesn't accept a probe message within `probeTimeoutSeconds`, or that registers publish or end-to-end latency greater than `unavailabilityThresholdMs`, gets recorded as an unavailability event with start and end timestamps. The normal throughput/latency benchmark keeps running alongside.

This is the right tool when you want to answer questions like *"how long are partitions unavailable when I kill a broker leader?"* or *"does a rolling restart breach my availability SLO?"*. You can also use the main workload as a stress test to see if it causes unavailability for other topics.

The smallest example, `quick-availability-test.yaml`:

```bash
./dimster run local \
    -k run/kafka-configs/kafka_local.yaml \
    -w run/workloads/quick-test/quick-availability-test.yaml
```

The workload is a normal 2-minute run plus:

```yaml
availabilityConfig:
  unavailabilityThresholdMs: 5000   # debounce — hiccups shorter than this don't count
  probeTimeoutSeconds: 30           # send timeout before a probe is treated as a failure
  probePartitions: 6                # how many partitions of the probe topic to monitor
```

By itself this run will report 100% availability — there's nothing failing on a quiet local cluster. The overlay earns its keep when you combine it with a disruption (kill `kafka-0`, restart a broker, inject network partition) during the test window.

> Heads up — availability is a **run-mode-only** overlay. Adding `availabilityConfig` to an `explore`, `drain-backlog`, or `correctness` workload has no effect; those engines ignore it.

**What the result tells you**: a list of unavailability events (start, end, partition), total unavailability time, and overall percent-available across the test duration. See [Availability Checking](../modes/availability.md) for the full field reference.

---

## Explore — find the ceiling under a latency SLO

Three-phase algorithm — ramp the throughput up until latency breaks, binary search for the highest sustainable rate, then sustain it to confirm.

```bash
./dimster explore local \
    -k run/kafka-configs/kafka_local.yaml \
    -w run/workloads/quick-test/quick-explore-limits.yaml
```

The `quick-explore-limits.yaml` workload is tuned for fast iteration on a
laptop:

- Latency SLO: **p75 end-to-end ≤ 50 ms**.
- Phase 1 (ramp): doubles the rate every 10 seconds starting from 500 msg/s until the SLO breaks.
- Phase 2 (search): binary search, 90s per midpoint, latency checked every 10s.
- Phase 3 (sustain): hold the discovered rate for 90s.
- Two scenarios swept: 1 partition / 1 consumer, then 100 / 100.

Total runtime: a few minutes per scenario.

**What the result tells you**: the highest message rate this cluster can sustain at the configured latency, per scenario.

---

## Drain backlog — measure consumer catch-up

Two-phase: build a backlog with producers only, then start the consumers and time the drain.

```bash
./dimster drain-backlog local \
    -k run/kafka-configs/kafka_local.yaml \
    -w run/workloads/quick-test/quick-backlog-test.yaml
```

The `quick-backlog-test.yaml` workload:

- Build phase: produce until the backlog reaches **3 GB** at 100 K msg/s.
- Drain phase: producers paused (`backlogProducerRate: 0`), 1 consumer drains as fast as it can, sample every 10s.
- Single topic, single partition, 1 KB messages.

> Heads up — 3 GB of payload data plus replication will fill the local Kafka PVC quickly. If you've sized the PVC down or you've run a couple of these back-to-back, watch for `kafka-0` going `CrashLoopBackOff` from a full disk.

**What the result tells you**: drain duration, drain throughput, and a
time-series of remaining backlog size.

---

## Correctness — find data loss, duplicates, ordering bugs

Producers send sequentially numbered payloads keyed to a single partition. Consumers track every sequence number and detect gaps (loss), repeats (duplicates), and out-of-order delivery.

```bash
./dimster correctness local \
    -k run/kafka-configs/kafka_local.yaml \
    -w run/workloads/quick-test/quick-correctness-test.yaml
```

The `quick-correctness-test.yaml` workload:

- Test mode: **CONCURRENT** (producers and consumers run together).
- Duration: 1 minute, plus a 120s grace period for in-flight messages to be consumed.
- 2 topics × 10 partitions, 1 producer per topic at 1000 msg/s, 5 consumers per group.
- Detects: duplicates, out-of-order, payload CRC mismatches.
- Fails the run on loss, out-of-order events or CRC mismatch; only logs duplicates.

**What the result tells you**: pass/fail summary plus per-detector counts. A `Quick correctness check` that exits with status 0 means no data loss, no CRC errors. Duplicate and ordering counts are reported but don't fail the run by default.

---

## Going beyond the smoke tests

Each of these workloads is a 1-line variation away from a real benchmark. The natural next step is to combine these modes with the dimensional testing pattern from page 08 — `quick-explore-limits.yaml` above already sweeps two scenarios, and the same `scenarios:` block works for `run`, `drain-backlog`, and `correctness` too.

For the full mode write-ups (algorithms, every config field, gotchas):

- [Run mode](../modes/run.md)
- [Explore Limits mode](../modes/explore-limits.md)
- [Drain Backlog mode](../modes/drain-backlog.md)
- [Correctness mode](../modes/correctness.md)

## If something looks wrong

- `explore` finishes in seconds with a tiny "max sustainable rate" — your   laptop is the bottleneck, not the cluster. Page 10 (`localBeefy`) gives you more room.
- `drain-backlog` errors out with disk-full messages from `kafka-0` — bump `kafka.pvcSize` in your env (or wipe the cluster between runs with `dimster teardown local && dimster deploy local`).
- `correctness` reports unexpected loss — first check `dimster health local` mid-run; a worker that flipped to `FAILED` will look like loss.
