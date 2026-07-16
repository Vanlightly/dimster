# Results and Charts

Dimster produces structured results from every benchmark run and can generate interactive charts for analysis.

All results are shareable assets:
- Detailed JSON results which include:
    - Per test-point throughput and various latencies
    - The effective workload and client configs that produced each test point result
    - Information on the hardware and software version
- The workload file(s), Kafka client config file (if used), the effective broker config, and the environment definition that selected the cluster topology and resource sizes — all captured as the run started so a results archive is self-describing
- Charts as a single interactive html file. Time periods can be selected and the legend is clickable to hide/show individual series (one series per test point)
- Logs of all Kafka brokers, workers and the coordinator
- Grafana dashboards converted to HTML files

The idea is to have full traceability and the ability to easily reproduce a benchmark based on its results.

## Result Storage

After a benchmark run completes, the CLI downloads results (stored as JSON files on the coordinator's PVC), logs, dashboards and generate charts. The following assets are created in your local result dir (within a timestamped subdirectory (e.g., `results/2026-03-21-17-25-49/`):
- **JSON result file**: a single json result file with all throughput, latencies and all configuration data.
- **`configs/` sub-directory**: the inputs that produced the run, captured at run start so each result is self-describing:
    - `workload-<name>.yaml` — one file per workload submitted to the run (the original file, byte-for-byte).
    - `kafka.yaml` — the Kafka client config file passed via `-k`, if any.
    - `broker.yaml` — the **effective** broker config (the broker config file merged with any inline `kafka.extraConfig` overrides), so the captured file matches what the broker actually ran with.
    - `environment.yaml` — the environment block from `dimster-config.yaml` that selected the cluster topology, resource sizes, monitoring layout, and so on. The `image`, `context`, and `connection` fields are stripped so deployment artifacts and external-cluster credentials don't end up in shared archives.
- **Logs**: Coordinator, worker and Kafka logs are downloaded to the `logs` sub-directory.
- **Dashboards**: Grafana dashboards are translated into HTML files and written to `dashboards` sub-directory.
- **Charts**: A set of charts are generated based on the JSON result file.

## Repeated Scenario Results

When a scenario uses `repeat` greater than `1`, Dimster writes each repeated run and adds a synthetic `<scenario name> (merged)` result for convenience. That merged result averages the repeated runs' summary fields. For latency metrics, this means a merged p99 is the average of each repeated run's p99, not the p99 recalculated from all latency samples pooled together. This is useful as a compact run-level summary, but the individual repeated runs remain the source of truth for run-to-run variance and strict pooled-percentile analysis.

## Grafana Dashboards

During a benchmark run, live Grafana dashboards are available via:

```
dimster dashboard [env]
```

This port-forwards Grafana (localhost:3000) and Prometheus (localhost:9090). Three dashboards are pre-provisioned:

- **Client Metrics** -- publish latency, schedule latency, end-to-end latency percentiles, message rates, byte rates
- **Kafka Broker** -- cluster health, throughput, request latency, replication lag, JVM stats
- **System (Linux)** -- CPU, memory, disk I/O, network (via node-exporter)

## Dashboard Export

After each benchmark run, Dimster automatically exports Grafana dashboard snapshots as static HTML files into the `dashboards` sub-directory, providing a permanent record of what the live dashboards showed during the run.

Long campaigns are split across multiple HTML files so each file stays a manageable size. The split points align with **scenario boundaries**: every test point in a given scenario is guaranteed to land in the same HTML file, so a single scenario is never cut in half by a window boundary. The exporter bin-packs scenarios into windows up to 1.5x the target window length, which keeps short scenarios grouped together and long scenarios in a window of their own. Each engine stamps the begin/end times of each scenario into the JSON result file; the exporter reads those ranges to compute the splits.

See [CLI Reference](../reference/cli.md) for chart and dashboard commands.

## Other visualization options

`dimster csv <result-file.json>` converts the JSON result file into a CSV file, which can be used with other charting options / placing in a spreadsheet.

`dimster pivot <result-file.json>` creates charts which create a plot per dimension index and each scenario is a series (rather than a plot per scenario with test points as series).
