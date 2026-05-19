# Dimster CLI Reference

The `dimster` CLI manages the full lifecycle of Dimster benchmarks: building, deploying, running, monitoring, and collecting results.

## Usage

Compile the CLI and place the binary in the project root.

```bash
cd cli && cargo build --release && cp target/release/dimster .. && cd ..
```
Use `--help` to discover the list of commands


```bash
./dimster --help
```

Many commands are targeted at a named environment in the `dimster-config.yaml`. Run `dimster init` to create a `dimster-config.yaml` with example environments. See [dimster-config.yaml reference](dimster-config.md) for the full schema.

---

## Commands

### init

Initialize `dimster-config.yaml` with a starter set of environments.

```
dimster init [--local-k8s-distro <distro>]
```

Creates `dimster-config.yaml` in the project root, populated with:

- `local` -- active minimal environment (no kafka/workers/coordinator sections, so deploy uses tiny built-in defaults), pre-filled for the chosen `--local-k8s-distro` (defaults to `minikube`).
- `exampleLocalFull` -- commented example: chosen local k8s distro + Kafka deployed in-cluster.
- `exampleLocalExt` -- commented example: chosen local k8s distro + external Kafka (the host hostname in `bootstrap.servers` is set to the chosen distro's convention — `host.minikube.internal`, `host.k3d.internal`, or `host.docker.internal`).
- `exampleRemoteFull` -- commented example: remote Kubernetes (EKS/GKE/etc.) + Kafka deployed in-cluster.
- `exampleRemoteExt` -- commented example: remote Kubernetes + external Kafka (Confluent Cloud, MSK, etc.).

Uncomment an example block and edit the values to suit your cluster.

Refuses to overwrite an existing `dimster-config.yaml` -- delete or rename the file to generate a fresh one.

---

### build

Build the project and Docker image and load into the k8s cluster identified in the environment.

```
dimster build [env] [--in-docker]
```

| Argument / Flag | Required | Description                                                                                                       |
|-----------------|----------|-------------------------------------------------------------------------------------------------------------------|
| `env` | Yes      | Environment name. Builds Java, the Docker image, and pushes/loads the image for that environment.                 |
| `--in-docker` | No       | Run the Maven build inside Docker instead of on the host. Default is host-build, but requires Java 25 on the host |

---

### deploy

Deploy Kafka, workers, coordinator, and monitoring stack to Kubernetes.

```
dimster deploy <env> 
```

Generates a dynamic Kustomize overlay from the environment configuration, applies it to the Kubernetes cluster, and waits for all pods to become ready. In `external` deploy mode, Kafka pods are not deployed.

---

### run

Run a standard benchmark.

```
dimster run <env> -w <workload> [options]
```

| Argument / Flag | Required | Description                                                                                                            |
|-----------------|----------|------------------------------------------------------------------------------------------------------------------------|
| `env` | Yes | Environment name (e.g. `local`, `aws`).                                                                                |
| `-k`, `--kafka-config <path>` | No | Path to Kafka config YAML. Optional if the config is embedded in the workload.                                         |
| `-w`, `--workload <path>` | Conditional | Path to workload YAML. Can be specified multiple times for sequential runs. Required for a normal run; not required (and ignored if supplied) when `--modify` is used. |
| `--modify` | No | Modify a running benchmark instead of starting a new one. Sends a side-channel HTTP request to the coordinator to change parameters on the in-progress test point. Must be combined with at least one workload override, `--kafkaConfig.<path>` override, or action flag (see below). |
| `--<workloadField> <value>` | No | (with `--modify`) Override any workload field by its yaml name in camelCase, e.g. `--producerRate 50000`, `--producersPerTopic 8`, `--messageSize 2000`, `--messageDistributor PINNED_PARTITIONS`, `--consumerProcMsMin 50 --consumerProcMsMax 100`. Multiple overrides can be combined in one call. |
| `--kafkaConfig.<dotted.path> <value>` | No | (with `--modify`) Override any Kafka config setting via a flattened dotted path, e.g. `--kafkaConfig.producerConfig.batch.size 65536`, `--kafkaConfig.classicConsumerConfig.max.poll.records 200`, `--kafkaConfig.commonConfig.request.timeout.ms 60000`. The first segment selects the sub-section; the rest is the leaf key (Kafka keys are themselves dotted strings). |
| `--terminate` | No | (with `--modify`) End the in-progress test point as if its duration limit had been reached, advancing the run to the next test point. Distinct from `dimster stop` (which ends the whole run). Must be sent alone. |
| `--discard-stats` | No | (with `--modify`) Wipe per-period stats and cumulative histograms accumulated so far in the current test phase. Effectively converts the elapsed test phase into additional warmup. Test phase only and cannot be combined with field overrides. Often paired with `--run-for-minutes`. |
| `--run-for-minutes <N>` | No | (with `--modify`) Reset the test phase remaining duration to N minutes from the moment the engine processes the request. Stands alone (extends the current test phase) or pairs with `--discard-stats` (wipes stats AND gives a fresh N-minute window). Test phase only. Must be > 0. |

The CLI checks Kafka and coordinator readiness, discovers worker pods, injects `bootstrap.servers` into configs, sends the run request to the coordinator, and streams coordinator logs until the benchmark completes. Results are automatically copied from the coordinator PVC to the local results directory, along with component logs and dashboard snapshots.

When `--modify` is used the CLI takes a much shorter path: no readiness check, no workload staging, just an HTTP POST to the coordinator's `/modify` endpoint. The original `dimster run` window keeps running and tailing the coordinator log; modify invocations exit as soon as the coordinator responds. See [Live Workload Modification](../concepts/live-modification.md) for the full guide and workflow patterns.

---

### explore

Find maximum sustainable throughput under latency constraints using a three-phase algorithm (ramp, search, sustain).

```
dimster explore <env> -w <workload> [options]
```

| Argument / Flag | Required | Description |
|-----------------|----------|-------------|
| `env` | Yes | Environment name. |
| `-k`, `--kafka-config <path>` | No | Path to Kafka config YAML. Optional if embedded in the workload. |
| `-w`, `--workload <path>` | Yes | Path to explore workload YAML. |

---

### correctness

Run correctness checks (duplicate detection, ordering verification, optional CRC).

```
dimster correctness <env> -w <workload> [options]
```

| Argument / Flag | Required | Description |
|-----------------|----------|-------------|
| `env` | Yes | Environment name. |
| `-k`, `--kafka-config <path>` | No | Path to Kafka config YAML. Optional if embedded in the workload. |
| `-w`, `--workload <path>` | Yes | Path to correctness workload YAML. |

---

### drain-backlog

Measure the time to drain a consumer backlog.

```
dimster drain-backlog <env> -w <workload> [options]
```

| Argument / Flag | Required | Description |
|-----------------|----------|-------------|
| `env` | Yes | Environment name. |
| `-k`, `--kafka-config <path>` | No | Path to Kafka config YAML. Optional if embedded in the workload. |
| `-w`, `--workload <path>` | Yes | Path to drain-backlog workload YAML. |

---

### stop

Ctrl+C will detach you from the coordinator output (under the hood it is kubectl following the coordinator pod logs). From there you can gracefully stop a running benchmark.

```
dimster stop <env>
```

Sends a stop request to the coordinator. The running benchmark completes its current test point and shuts down cleanly. To download the results of any completed test points run `./dimster results <env>`

---

### reattach

Reconnect to a coordinator that is currently running (or stopping) a benchmark and start streaming its log output, and -- once the benchmark finishes -- download results to the local machine. Use this after detaching with Ctrl+C, or from a different shell while a long benchmark runs in the background. If the coordinator is already idle, `reattach` simply downloads the latest results.

```
dimster reattach <env>
```

Pressing Ctrl+C while reattached detaches again without affecting the benchmark; the same hint about `dimster reattach` and `dimster stop` is printed.

---

### resume

Restart a benchmark from the coordinator's last checkpoint. Used after a previous `run`/`explore`/`correctness`/`drain-backlog` failed. The coordinator persists its progress in a checkpoint file after each successful test point. If the coordinator fails, `resume` picks up at the next test point with a freshly-discovered worker pod list.

```
dimster resume <env>
```

Refuses to run if the coordinator is currently `RUNNING` or `STOPPING` (use `dimster reattach` or `dimster stop` first), or if no checkpoint is present. After resuming, behaves like `run`: streams coordinator output, downloads results on completion, and preserves the checkpoint for another `resume` if the benchmark fails again.

---

### results

Copy results from the coordinator's PVC to the local machine. Also saves coordinator and worker logs alongside the result files and generates the charts. This is usually unnecessary as everything is copied at the end of a run. However, if you want to finish early, you can detach, stop and then run this `results` command.

```
dimster results <env> [output_dir]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `env` | Yes | Environment name. |
| `output_dir` | No | Local output directory. Defaults to the `results.dir` value from config. |

---

### csv

Generate CSV files from local JSON result files (no cluster access required). Detects `run` vs. `explore` results from each file's `mode` field and writes the appropriate column set.

```
dimster csv <path> [options]
```

| Argument / Flag | Required | Description |
|-----------------|----------|-------------|
| `path` | Yes | Path to a JSON result file or a directory of JSON files. |
| `-o`, `--output <file>` | No | Output file path. Defaults to writing a `.csv` next to each input `.json`. |

---

### chart

Generate HTML charts from result JSON files.

```
dimster chart [results_dir]
```

| Argument | Required | Description |
|----------|----------|-------------|
| `results_dir` | No | Local results directory. Defaults to the `results.dir` value from config. |

Runs `charts/generate_charts.py` against the results directory. Requires Python with dependencies from `charts/requirements.txt`.

---

### compare

Generate side-by-side comparison charts from 2-4 `run` result JSON files. Each input is given a short label that appears in the chart legend.

```
dimster compare <LABEL=FILE> <LABEL=FILE> [LABEL=FILE] [LABEL=FILE] [-o <output>]
```

| Argument / Flag | Required | Description |
|-----------------|----------|-------------|
| `LABEL=FILE` | Yes (2-4) | A short label and the path to a result JSON file (e.g. `baseline=results/2026-04-20/run.json`). |
| `-o`, `--output <dir>` | No | Output directory for the generated charts. Defaults to the directory of the first input file. |

Runs `charts/generate_compare_charts.py`. Requires Python with dependencies from `charts/requirements.txt`. Each input file must exist; missing files fail before the script runs.

Example:

```
dimster compare baseline=results/2026-04-15/run.json tuned=results/2026-04-20/run.json -o results/compare/
```

---

### teardown

Remove Dimster resources from Kubernetes.

```
dimster teardown <env>
```

Deletes the pods and PVCs. The namespace itself is left in place.

---

### dashboard

Port-forward Grafana and Prometheus to localhost for live monitoring.

```
dimster dashboard <env> [grafana_port] [prometheus_port]
```

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `env` | Yes | -- | Environment name. |
| `grafana_port` | No | `3000` | Local port for Grafana. |
| `prometheus_port` | No | `9090` | Local port for Prometheus. |

Runs until interrupted with Ctrl+C.

---

### logs

View pod logs from cluster components.

```
dimster logs <env> [component] [options]
```

| Argument / Flag | Required | Default | Description |
|-----------------|----------|---------|-------------|
| `env` | Yes | -- | Environment name. |
| `component` | No | `worker` | Component to view logs for: `worker`, `coordinator`, or `kafka`. |
| `-f`, `--follow` | No | `false` | Follow (stream) log output. |
| `-p`, `--previous` | No | `false` | Show logs from the previous (crashed) container. |
| `--tail <n>` | No | -- | Show only the last N lines from the end of logs. |

When multiple pods match (multiple workers, mulitple brokers), then kubectl streams all matching pods concurrently and interleaves their output, with --prefix tagging each line with its pod name so you can tell them apart.

---

### download-kafka-logs

Copy Kafka server log files (`*.log`, `*.log.*`) off every running broker pod into a per-pod subdirectory on the local machine. Useful for post-mortem inspection when Grafana/`dimster logs` aren't enough -- the GC log, controller log, and rolled-over server logs are all included. Not useful in `external` deploy mode (no Dimster-managed brokers).

```
dimster download-kafka-logs <env> [-o <output>]
```

| Argument / Flag | Required | Default | Description |
|-----------------|----------|---------|-------------|
| `env` | Yes | -- | Environment name. |
| `-o`, `--output <dir>` | No | `.` | Local output directory. One subdirectory is created per broker pod (e.g. `<output>/kafka-0/`). |

Each broker is queried for files under `/opt/kafka/logs` matching `*.log` or `*.log.*`, then copied with `kubectl cp`. Failures on individual files are logged as warnings rather than aborting the download.

---

### health

Check health and readiness of all worker pods.

```
dimster health <env>
```
If a benchmark is in progress, it reports the status of each client (producer/consumer) on each worker pod. Statuses: `PENDING`, `RUNNING`, `FAILED`.

---

### kafka

Run Kafka CLI tools inside a broker pod. Not available in `external` deploy mode.

```
dimster kafka <env> [broker_id] [-- <command>...]
```

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `env` | Yes | -- | Environment name. |
| `broker_id` | No | `0` | Broker pod ordinal number. |
| `command` | No | -- | Command to run (passed after `--`). If omitted, opens an interactive shell. |

Example:

```
dimster kafka aws 0 -- kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

### inspect

Print a read-only snapshot of Kafka cluster state by exec'ing into the `kafka-0` broker pod. Not available in `external` deploy mode.

```
dimster inspect <env>
```

Dumps three sections:

- **Topics** -- `kafka-topics.sh --describe` for every topic (partition/replica/ISR layout).
- **Consumer Groups** -- per-group `--describe`, `--members`, and `--state`. Uses `kafka-groups.sh --consumer` when available, since `kafka-consumer-groups.sh --list` can miss classic groups in Kafka 4.x.
- **Share Groups** -- per-group `--describe`, `--members`, and `--state`, when `kafka-share-groups.sh` is present in the broker image.
