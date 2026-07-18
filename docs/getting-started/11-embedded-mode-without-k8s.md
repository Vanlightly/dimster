# 11 — Running Dimster without Kubernetes

The numbered pages so far drive Dimster the way it's normally used: the `dimster` Rust CLI deploys a coordinator and one or more workers into Kubernetes, then drives them over HTTP. That's the standard path, with all the conveniences of the CLI.

An alternative is to run **embedded mode**, where a single JVM runs both the coordinator and an in-process worker. No Kubernetes or Docker images, just `java -cp ... dev.dimster.Dimster ...` against a Kafka broker you already have running.

Embedded mode loses the CLI convencience capabilities and can't distribute load across machines. You can still use it for high quality benchmarks, as long as the client-side workload can run on a single machine. Reach for the full k8s flow for larger scaling and the convenience that comes with the CLI and k8s.

## Limitations of embedded mode

- Single-JVM throughput is bounded by one machine's CPU and network.
- No Kafka deployed
- No monitoring deployed
- No automatic downloading of logs, chart generation, CSV generation, dashboard downloads.
- Live `dimster run <env> --modify` or `dimster stop <env>` mid-run (no coordinator HTTP endpoint to talk to).
- Anything that wants `dimster results` to pull a PVC, or dashboards, or `dimster logs`/`inspect`/`health`.

## What the Rust CLI still gives you

The post-processing commands of the CLI work on local JSON files and don't need a kubectl context:

```bash
# Convert each .json in the directory to a sibling .csv
./dimster csv path/to/results/<timestamped-result-dir>

# Generate HTML charts under <dir>/charts/
./dimster chart path/to/results/<timestamped-result-dir>

# Compare named result files side-by-side
./dimster compare \
    baseline=path/to/results/<timestamped-result-dir1>/quick-run-test.json \
    candidate=path/to/results/<timestamped-result-dir2>/quick-run-test.json

# Pivot a single result file (turn each scenario into its own column)
./dimster pivot path/to/results/<timestamped-result-dir>/quick-run-test.json

# Theoretical resource sizing for a workload (no run needed)
./dimster resources -b 3 -w run/workloads/quick-test/quick-run-test.yaml
```

## Prerequisites

- JDK 21+ (same as the rest of Dimster — check with `java -version`).
- The repo cloned, and `./mvnw` reachable.
- A Kafka broker reachable from your host. The easiest path is the docker-compose at `docs/examples/external-mode/docker-compose.yaml`, which exposes a single broker on `localhost:9092`:

```bash
docker compose -f docs/examples/external-mode/docker-compose.yaml up -d
```

## Build for a Kafka client version

Dimster supports Apache Kafka clients verison 3.0+. Because the API surface has changed since 4.0, there are different Kafka modules (called flavors) which support different client versions. For Kafka clients 3.0-4.0, set the env var `DIMSTER_KAFKA_FLAVOR=3.x`. For 4.1 set `DIMSTER_KAFKA_FLAVOR=4.1`. For clients 4.2+ set `DIMSTER_KAFKA_FLAVOR=4.2.0`. The default if unset: `4.2.0`.

There are two ways to get a runnable build.

### (a) Source-tree compile — fast iteration

Compile from the repo root, then call `./bin/benchmark`:

```bash
# build dimster-core and all flavors (one-time, plus on each code change)
./mvnw clean compile

# run (DIMSTER_KAFKA_FLAVOR defaults to 4.2.0, set it explicitly to switch)
./bin/benchmark ...
```

To switch flavors, just override the env var — the build above already produced every flavor's classes:

```bash
DIMSTER_KAFKA_FLAVOR=4.1 ./bin/benchmark ...
```

Avoid `./mvnw -pl dimster-kafka-<flavor> -am compile`: the kafka-flavor modules depend on `dimster-spi`, not `dimster-core`, so `-am` will not rebuild `dimster-core` even though the launcher splices `dimster-core/target/classes` onto the classpath. Edits to core code silently produce no-op runs against stale `.class` files. Stick with `./mvnw clean compile` (or explicitly include `dimster-core` in `-pl`).

If the launcher can't find the chosen flavor's `target/classpath.txt`, it tells you exactly what to compile — but always pair that with `dimster-core` for the same reason.

### (b) Packaged tarball — closer to production

Build a single-flavor tarball, extract it anywhere, and run from there. The launcher detects the bundled `lib/` and ignores `DIMSTER_KAFKA_FLAVOR` (the flavor is baked in at package time):

```bash
./mvnw -pl package-4.2.0 -am package -DskipTests
mkdir -p /tmp/dimster-embedded
tar xzf package-4.2.0/target/dimster-*-bin.tar.gz -C /tmp/dimster-embedded
cd /tmp/dimster-embedded/dimster-*

./bin/benchmark ...
```

For a non-default point release inside a flavor, pass `-Dkafka.version=...`:

```bash
./mvnw -pl package-3.x -am package -Dkafka.version=3.7.2 -DskipTests
```

## Run a benchmark

The CLI surface is six flags. From `./bin/benchmark --help`:

| Flag | Purpose |
|------|---------|
| `-k, --kafka-configs` | Kafka config yaml (e.g. `run/kafka-configs/kafka_local.yaml`) |
| `-w, --workloads` | Workload yaml file(s); repeat the flag for multiple |
| `-b, --bootstrap-servers` | Kafka bootstrap servers (e.g. `localhost:9092`). Injected into the kafka-config's `commonConfig`, overriding any value present |
| `-o, --output` | Output base directory. Results JSON *and* the log file land under `<output>/<timestamp>/`. Defaults to `./output` if unset |
| `-s, --system` | Free-text label embedded in the result JSON |
| `-m, --mode` | `run`, `explore`, `correctness`, or `drain-backlog` (default `run`) |

Full smoke command against the docker-compose broker:

```bash
./bin/benchmark \
    -k run/kafka-configs/kafka_local.yaml \
    -w run/workloads/quick-test/quick-run-test.yaml \
    -b localhost:9092 \
    -o /tmp/dimster-embedded-test \
    -s 'Local one broker Kafka 4.2'
```

The `quick-run-test.yaml` workload runs ~21 minutes by default (20-minute test + 1-minute warmup). For a faster smoke, temporarily set `testDurationMinutes: 1` and `warmupDurationMinutes: 0` in the workload, or pick a shorter one from `run/workloads/quick-test/`.

On a healthy run you should see:

```
INFO: Kafka client factory bound: ...KafkaClientFactoryV4_2_0 (kafka-clients version: 4.2.0, share groups: true)
INFO: Injected bootstrap.servers=localhost:9092 into kafka config (temp file ...)
INFO: Starting benchmark with config: { ... }
INFO: Workloads: [quick-run-test]
...
```

Results AND the log file land in `/tmp/dimster-embedded-test/<timestamp>/` — one JSON per workload, plus a `dimster.log` capturing the full run. Drop the `-o` flag and the same files appear under `./output/<timestamp>/` instead.

### `-b` vs baking `bootstrap.servers` into the kafka-config

`-b/--bootstrap-servers` is a convenience because the Rust CLI normally injects the brokers from the env's `connection:` block — in embedded mode there's no CLI to do that. If you'd rather keep the broker URL alongside the rest of the Kafka tuning, add it to the kafka-config yaml's `commonConfig` instead and drop the `-b` flag:

```yaml
commonConfig:
  bootstrap.servers: localhost:9092
```

`-b` always wins when both are present.

## Logging

Logging is wired via Log4j 2 with the config at `dimster-core/src/main/resources/log4j2.yaml`. Console output goes to stdout. The full log also lands in `<output-dir>/<timestamp>/dimster.log` alongside the results JSON, so each run is self-contained in a single directory.

If you need to override the log location explicitly (rare — usually only when the per-run dir isn't writable), set `DIMSTER_LOG_FILE` in the environment before launching:

```bash
DIMSTER_LOG_FILE=/var/log/dimster-embedded.log ./bin/benchmark ...
```

The env var takes precedence over the per-run default.

If no log lines appear, check that `dimster-core/target/classes` is on the classpath (the launcher puts it there automatically — this only goes wrong if you've moved files around manually).

## JVM heap and GC

The launcher reads a `HEAP_OPTS` env var (default `-Xms4G -Xmx4G`) and always appends `-XX:+UseG1GC`. Override when your workload needs more heap — high producer/consumer counts, large message sizes, or long runs with lots of latency-histogram retention:

```bash
HEAP_OPTS="-Xms8G -Xmx8G" ./bin/benchmark ...
```

For low-overhead smoke tests on a constrained machine, smaller works too:

```bash
HEAP_OPTS="-Xms1G -Xmx1G" ./bin/benchmark ...
```

If you need to add other JVM flags (e.g. enabling GC logging), fold them into `HEAP_OPTS` — everything in that var goes onto the `java` command line ahead of `-XX:+UseG1GC`.
