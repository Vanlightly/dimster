use std::path::Path;

use anyhow::{Result, bail};
use serde::Deserialize;

// --- Explore result types ---

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExploreResult {
    #[serde(default)]
    metadata: ExploreMetadata,
    // New format uses scenarioResults, old format uses scenarios
    #[serde(default, alias = "scenarios")]
    scenario_results: Vec<ScenarioResult>,
    // Legacy top-level fields (old format) — used as fallbacks
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    begin_time: Option<String>,
    #[serde(default)]
    end_time: Option<String>,
    #[serde(default)]
    environment: Option<String>,
    #[serde(default)]
    system: Option<String>,
}

impl ExploreResult {
    fn effective_name(&self) -> &str {
        if !self.metadata.name.is_empty() {
            &self.metadata.name
        } else {
            self.name.as_deref().unwrap_or("")
        }
    }
    fn effective_environment(&self) -> &str {
        self.metadata
            .environment
            .as_deref()
            .or(self.environment.as_deref())
            .unwrap_or("")
    }
    fn effective_system(&self) -> &str {
        self.metadata
            .system
            .as_deref()
            .or(self.system.as_deref())
            .unwrap_or("")
    }
    fn effective_begin_time(&self) -> &str {
        self.metadata
            .begin_time
            .as_deref()
            .or(self.begin_time.as_deref())
            .unwrap_or("")
    }
    fn effective_end_time(&self) -> &str {
        self.metadata
            .end_time
            .as_deref()
            .or(self.end_time.as_deref())
            .unwrap_or("")
    }
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ExploreMetadata {
    #[serde(default)]
    name: String,
    #[serde(default)]
    #[allow(dead_code)]
    kafka_config_name: Option<String>,
    #[serde(default)]
    begin_time: Option<String>,
    #[serde(default)]
    end_time: Option<String>,
    #[serde(default)]
    environment: Option<String>,
    #[serde(default)]
    system: Option<String>,
}

#[derive(Deserialize)]
struct ScenarioResult {
    name: String,
    tests: Vec<TestOutcome>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestOutcome {
    label: Option<String>,
    topics: i32,
    partitions_per_topic: i32,
    producers_per_topic: i32,
    #[serde(default, alias = "subscriptionsPerTopic")]
    consumer_groups_per_topic: i32,
    #[serde(alias = "consumerPerSubscription")]
    consumers_per_group: i32,
    message_size: i32,
    key_distributor: Option<String>,
    consumer_type: Option<String>,
    peak_sustainable_rate: f64,
    #[serde(rename = "peakSustainableThroughputMBs")]
    peak_sustainable_throughput_mbs: f64,
    #[serde(default)]
    found_rate_upper: f64,
    converged: bool,
    reason: Option<String>,
    publish_latency: LatencyBreakdown,
    end_to_end_latency: LatencyBreakdown,
}

#[derive(Deserialize)]
struct LatencyBreakdown {
    p50: f64,
    p75: f64,
    p99: f64,
    p999: f64,
    p9999: f64,
}

// --- Run result types (consolidated format) ---

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunResult {
    metadata: RunMetadata,
    scenario_results: Vec<RunScenarioResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunMetadata {
    name: Option<String>,
    kafka_config_name: Option<String>,
    environment: Option<String>,
    system: Option<String>,
    begin_time: Option<String>,
    end_time: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunScenarioResult {
    name: String,
    tests: Vec<RunTestEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunTestEntry {
    label: Option<String>,
    workload_config: RunWorkloadConfig,
    results: RunPerformanceResults,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct RunWorkloadConfig {
    #[serde(default)]
    key_distributor: Option<String>,
    #[serde(default)]
    key_distributor_skew: f64,
    #[serde(default)]
    consumer_type: Option<String>,
    #[serde(default)]
    message_size: i64,
    #[serde(default)]
    topics: i32,
    #[serde(default)]
    partitions_per_topic: i32,
    #[serde(default)]
    producers_per_topic: i32,
    #[serde(default)]
    producer_rate: f64,
    #[serde(default)]
    consumer_groups_per_topic: i32,
    #[serde(default)]
    consumers_per_group: i32,
    #[serde(default)]
    consumer_proc_ms_min: i32,
    #[serde(default)]
    consumer_proc_ms_max: i32,
    #[serde(default)]
    test_duration_minutes: i32,
    #[serde(default)]
    warmup_duration_minutes: i32,
    #[serde(default)]
    sample_rate_millis: i32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct RunPerformanceResults {
    #[serde(default)]
    publish_rate: Vec<f64>,
    #[serde(default)]
    consume_rate: Vec<f64>,

    #[serde(default)]
    aggregated_publish_latency_avg: f64,
    #[serde(default, rename = "aggregatedPublishLatency50pct")]
    aggregated_publish_latency_p50: f64,
    #[serde(default, rename = "aggregatedPublishLatency75pct")]
    aggregated_publish_latency_p75: f64,
    #[serde(default, rename = "aggregatedPublishLatency95pct")]
    aggregated_publish_latency_p95: f64,
    #[serde(default, rename = "aggregatedPublishLatency99pct")]
    aggregated_publish_latency_p99: f64,
    #[serde(default, rename = "aggregatedPublishLatency999pct")]
    aggregated_publish_latency_p999: f64,
    #[serde(default, rename = "aggregatedPublishLatency9999pct")]
    aggregated_publish_latency_p9999: f64,
    #[serde(default)]
    aggregated_publish_latency_max: f64,

    #[serde(default)]
    aggregated_publish_delay_latency_avg: f64,
    #[serde(default, rename = "aggregatedPublishDelayLatency50pct")]
    aggregated_publish_delay_latency_p50: f64,
    #[serde(default, rename = "aggregatedPublishDelayLatency75pct")]
    aggregated_publish_delay_latency_p75: f64,
    #[serde(default, rename = "aggregatedPublishDelayLatency95pct")]
    aggregated_publish_delay_latency_p95: f64,
    #[serde(default, rename = "aggregatedPublishDelayLatency99pct")]
    aggregated_publish_delay_latency_p99: f64,
    #[serde(default, rename = "aggregatedPublishDelayLatency999pct")]
    aggregated_publish_delay_latency_p999: f64,
    #[serde(default, rename = "aggregatedPublishDelayLatency9999pct")]
    aggregated_publish_delay_latency_p9999: f64,
    #[serde(default)]
    aggregated_publish_delay_latency_max: f64,

    #[serde(default)]
    aggregated_end_to_end_latency_avg: f64,
    #[serde(default, rename = "aggregatedEndToEndLatency50pct")]
    aggregated_end_to_end_latency_p50: f64,
    #[serde(default, rename = "aggregatedEndToEndLatency75pct")]
    aggregated_end_to_end_latency_p75: f64,
    #[serde(default, rename = "aggregatedEndToEndLatency95pct")]
    aggregated_end_to_end_latency_p95: f64,
    #[serde(default, rename = "aggregatedEndToEndLatency99pct")]
    aggregated_end_to_end_latency_p99: f64,
    #[serde(default, rename = "aggregatedEndToEndLatency999pct")]
    aggregated_end_to_end_latency_p999: f64,
    #[serde(default, rename = "aggregatedEndToEndLatency9999pct")]
    aggregated_end_to_end_latency_p9999: f64,
    #[serde(default)]
    aggregated_end_to_end_latency_max: f64,
}

// --- Drain-backlog result types ---

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DrainBacklogResult {
    metadata: DrainBacklogMetadata,
    scenario_results: Vec<DrainBacklogScenarioResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DrainBacklogMetadata {
    name: Option<String>,
    kafka_config_name: Option<String>,
    environment: Option<String>,
    system: Option<String>,
    // hardware string is drain-backlog-specific (records cluster shape)
    hardware: Option<String>,
    begin_time: Option<String>,
    end_time: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DrainBacklogScenarioResult {
    name: String,
    tests: Vec<DrainBacklogTestEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DrainBacklogTestEntry {
    label: Option<String>,
    workload_config: DrainBacklogWorkloadConfig,
    drain: DrainStats,
    // `performance` field is intentionally not deserialized — drain-phase
    // latency/rate stats are out of scope for the CSV (drain summary only).
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct DrainBacklogWorkloadConfig {
    #[serde(default)]
    key_distributor: Option<String>,
    #[serde(default)]
    consumer_type: Option<String>,
    #[serde(default)]
    message_size: i64,
    #[serde(default)]
    topics: i32,
    #[serde(default)]
    partitions_per_topic: i32,
    #[serde(default)]
    producers_per_topic: i32,
    #[serde(default)]
    producer_rate: f64,
    #[serde(default)]
    consumer_groups_per_topic: i32,
    #[serde(default)]
    consumers_per_group: i32,
    #[serde(default)]
    consumer_proc_ms_min: i32,
    #[serde(default)]
    consumer_proc_ms_max: i32,
    #[serde(default)]
    warmup_duration_minutes: i32,
    #[serde(default)]
    warmup_producer_rate: f64,
    #[serde(default)]
    sample_rate_millis: i32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct DrainStats {
    #[serde(default)]
    backlog_messages: i64,
    #[serde(default)]
    backlog_size_bytes: i64,
    // JSON key uses uppercase "GB" so the camelCase rule alone won't match.
    #[serde(default, rename = "backlogSizeGB")]
    backlog_size_gb: f64,
    #[serde(default)]
    build_rate: f64,
    // -1 in JSON when no producer is running during drain (pure-drain test)
    #[serde(default)]
    backlog_producer_rate: f64,
    #[serde(default)]
    build_duration_ms: i64,
    // 0 when the drain timed out before completing
    #[serde(default)]
    drain_duration_ms: i64,
    #[serde(default)]
    drain_timeout_minutes: i32,
}

// --- CSV headers ---

const EXPLORE_HEADER: &str = "name,environment,system,begin_time,end_time,\
scenario,label,topics,partitions,producers,consumer_groups,consumers_per_group,\
message_size,key_distributor,consumer_type,\
peak_rate,peak_throughput_mbs,found_rate_upper,converged,reason,\
publish_latency_p50,publish_latency_p75,publish_latency_p99,\
publish_latency_p999,publish_latency_p9999,\
e2e_latency_p50,e2e_latency_p75,e2e_latency_p99,\
e2e_latency_p999,e2e_latency_p9999";

const RUN_HEADER: &str = "workload,kafka_config,environment,system,label,scenario,\
begin_time,end_time,\
topics,partitions,producers_per_topic,consumer_groups_per_topic,consumers_per_group,\
message_size,producer_rate,key_distributor,key_distributor_skew,consumer_type,\
consumer_proc_ms_min,consumer_proc_ms_max,\
test_duration_minutes,warmup_duration_minutes,sample_rate_millis,\
prod_rate_avg,prod_rate_min,prod_rate_max,\
con_rate_avg,con_rate_min,con_rate_max,\
publish_latency_avg,publish_latency_p50,publish_latency_p75,publish_latency_p95,\
publish_latency_p99,publish_latency_p999,publish_latency_p9999,publish_latency_max,\
publish_delay_avg,publish_delay_p50,publish_delay_p75,publish_delay_p95,\
publish_delay_p99,publish_delay_p999,publish_delay_p9999,publish_delay_max,\
e2e_latency_avg,e2e_latency_p50,e2e_latency_p75,e2e_latency_p95,\
e2e_latency_p99,e2e_latency_p999,e2e_latency_p9999,e2e_latency_max";

const DRAIN_BACKLOG_HEADER: &str = "workload,kafka_config,environment,system,hardware,\
begin_time,end_time,scenario,label,\
topics,partitions,producers_per_topic,consumer_groups_per_topic,consumers_per_group,\
message_size,producer_rate,key_distributor,consumer_type,\
consumer_proc_ms_min,consumer_proc_ms_max,\
warmup_duration_minutes,warmup_producer_rate,sample_rate_millis,\
backlog_messages,backlog_size_bytes,backlog_size_gb,\
build_rate,backlog_producer_rate,\
build_duration_ms,drain_duration_ms,drain_timeout_minutes,\
drain_minutes,drain_throughput_mbs";

// --- Main entry point ---

pub fn run(path: &str, output: Option<&str>) -> Result<()> {
    let p = Path::new(path);

    let files: Vec<std::path::PathBuf> = if p.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(p)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
            .collect();
        entries.sort();
        entries
    } else if p.is_file() {
        vec![p.to_path_buf()]
    } else {
        bail!("Path not found: {}", path);
    };

    if files.is_empty() {
        bail!("No JSON files found in: {}", path);
    }

    let mut converted = 0;

    for file in &files {
        let content = std::fs::read_to_string(file)?;
        let raw: serde_json::Value = serde_json::from_str(&content)
            .map_err(|e| anyhow::anyhow!("Cannot parse {}: {}", file.display(), e))?;

        let mode = raw.get("mode").and_then(|v| v.as_str()).unwrap_or("");

        let (header, rows) = match mode {
            "explore" => {
                let result: ExploreResult = serde_json::from_value(raw)
                    .map_err(|e| anyhow::anyhow!("Cannot deserialize {}: {}", file.display(), e))?;
                let mut rows = Vec::new();
                convert_explore(&result, &mut rows);
                (EXPLORE_HEADER, rows)
            }
            "run" => {
                let result: RunResult = serde_json::from_value(raw)
                    .map_err(|e| anyhow::anyhow!("Cannot deserialize {}: {}", file.display(), e))?;
                let mut rows = Vec::new();
                convert_run(&result, &mut rows);
                (RUN_HEADER, rows)
            }
            "drain-backlog" => {
                let result: DrainBacklogResult = serde_json::from_value(raw)
                    .map_err(|e| anyhow::anyhow!("Cannot deserialize {}: {}", file.display(), e))?;
                let mut rows = Vec::new();
                convert_drain_backlog(&result, &mut rows);
                (DRAIN_BACKLOG_HEADER, rows)
            }
            _ => {
                eprintln!("Skipping {} (unknown mode={:?})", file.display(), mode);
                continue;
            }
        };

        let out_path = match output {
            Some(o) => o.to_string(),
            None => file.with_extension("csv").to_string_lossy().to_string(),
        };

        let csv = format!("{}\n{}\n", header, rows.join("\n"));
        std::fs::write(&out_path, &csv)?;
        println!("Wrote {} rows to {}", rows.len(), out_path);
        converted += 1;
    }

    if converted == 0 {
        bail!("No results found in: {}", path);
    }

    Ok(())
}

// --- Explore conversion ---

fn convert_explore(result: &ExploreResult, rows: &mut Vec<String>) {
    for scenario in &result.scenario_results {
        for test in &scenario.tests {
            let row = format!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.2},{:.6},{:.2},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
                csv_escape(result.effective_name()),
                csv_escape(result.effective_environment()),
                csv_escape(result.effective_system()),
                csv_escape(result.effective_begin_time()),
                csv_escape(result.effective_end_time()),
                csv_escape(&scenario.name),
                csv_escape(test.label.as_deref().unwrap_or("")),
                test.topics,
                test.partitions_per_topic,
                test.producers_per_topic,
                test.consumer_groups_per_topic,
                test.consumers_per_group,
                test.message_size,
                csv_escape(test.key_distributor.as_deref().unwrap_or("")),
                csv_escape(test.consumer_type.as_deref().unwrap_or("")),
                test.peak_sustainable_rate,
                test.peak_sustainable_throughput_mbs,
                test.found_rate_upper,
                test.converged,
                csv_escape(test.reason.as_deref().unwrap_or("")),
                test.publish_latency.p50,
                test.publish_latency.p75,
                test.publish_latency.p99,
                test.publish_latency.p999,
                test.publish_latency.p9999,
                test.end_to_end_latency.p50,
                test.end_to_end_latency.p75,
                test.end_to_end_latency.p99,
                test.end_to_end_latency.p999,
                test.end_to_end_latency.p9999,
            );
            rows.push(row);
        }
    }
}

// --- Run conversion ---

fn convert_run(result: &RunResult, rows: &mut Vec<String>) {
    let m = &result.metadata;

    for scenario in &result.scenario_results {
        for test in &scenario.tests {
            let wc = &test.workload_config;
            let r = &test.results;

            let (prod_min, prod_avg, prod_max) = rate_stats(&r.publish_rate);
            let (con_min, con_avg, con_max) = rate_stats(&r.consume_rate);

            let row = format!(
                "{},{},{},{},{},{},{},{},\
                 {},{},{},{},{},\
                 {},{},{},{},{},\
                 {},{},\
                 {},{},{},\
                 {:.2},{:.2},{:.2},{:.2},{:.2},{:.2},\
                 {:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},\
                 {:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},\
                 {:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
                csv_escape(m.name.as_deref().unwrap_or("")),
                csv_escape(m.kafka_config_name.as_deref().unwrap_or("")),
                csv_escape(m.environment.as_deref().unwrap_or("")),
                csv_escape(m.system.as_deref().unwrap_or("")),
                csv_escape(test.label.as_deref().unwrap_or("")),
                csv_escape(&scenario.name),
                csv_escape(m.begin_time.as_deref().unwrap_or("")),
                csv_escape(m.end_time.as_deref().unwrap_or("")),
                wc.topics,
                wc.partitions_per_topic,
                wc.producers_per_topic,
                wc.consumer_groups_per_topic,
                wc.consumers_per_group,
                wc.message_size,
                wc.producer_rate,
                csv_escape(wc.key_distributor.as_deref().unwrap_or("")),
                wc.key_distributor_skew,
                csv_escape(wc.consumer_type.as_deref().unwrap_or("")),
                wc.consumer_proc_ms_min,
                wc.consumer_proc_ms_max,
                wc.test_duration_minutes,
                wc.warmup_duration_minutes,
                wc.sample_rate_millis,
                prod_avg,
                prod_min,
                prod_max,
                con_avg,
                con_min,
                con_max,
                r.aggregated_publish_latency_avg,
                r.aggregated_publish_latency_p50,
                r.aggregated_publish_latency_p75,
                r.aggregated_publish_latency_p95,
                r.aggregated_publish_latency_p99,
                r.aggregated_publish_latency_p999,
                r.aggregated_publish_latency_p9999,
                r.aggregated_publish_latency_max,
                r.aggregated_publish_delay_latency_avg,
                r.aggregated_publish_delay_latency_p50,
                r.aggregated_publish_delay_latency_p75,
                r.aggregated_publish_delay_latency_p95,
                r.aggregated_publish_delay_latency_p99,
                r.aggregated_publish_delay_latency_p999,
                r.aggregated_publish_delay_latency_p9999,
                r.aggregated_publish_delay_latency_max,
                r.aggregated_end_to_end_latency_avg,
                r.aggregated_end_to_end_latency_p50,
                r.aggregated_end_to_end_latency_p75,
                r.aggregated_end_to_end_latency_p95,
                r.aggregated_end_to_end_latency_p99,
                r.aggregated_end_to_end_latency_p999,
                r.aggregated_end_to_end_latency_p9999,
                r.aggregated_end_to_end_latency_max,
            );
            rows.push(row);
        }
    }
}

// --- Drain-backlog conversion ---

fn convert_drain_backlog(result: &DrainBacklogResult, rows: &mut Vec<String>) {
    let m = &result.metadata;

    for scenario in &result.scenario_results {
        for test in &scenario.tests {
            let wc = &test.workload_config;
            let d = &test.drain;

            let drain_minutes = d.drain_duration_ms as f64 / 60_000.0;
            // Average MB/s consumed during drain. Guard against div-by-zero
            // for the timeout case where drain_duration_ms is 0.
            let drain_throughput_mbs = if d.drain_duration_ms > 0 {
                (d.backlog_size_bytes as f64 / 1_048_576.0) / (d.drain_duration_ms as f64 / 1000.0)
            } else {
                0.0
            };

            let row = format!(
                "{},{},{},{},{},{},{},{},{},\
                 {},{},{},{},{},\
                 {},{},{},{},\
                 {},{},\
                 {},{},{},\
                 {},{},{:.2},\
                 {},{},\
                 {},{},{},\
                 {:.3},{:.3}",
                csv_escape(m.name.as_deref().unwrap_or("")),
                csv_escape(m.kafka_config_name.as_deref().unwrap_or("")),
                csv_escape(m.environment.as_deref().unwrap_or("")),
                csv_escape(m.system.as_deref().unwrap_or("")),
                csv_escape(m.hardware.as_deref().unwrap_or("")),
                csv_escape(m.begin_time.as_deref().unwrap_or("")),
                csv_escape(m.end_time.as_deref().unwrap_or("")),
                csv_escape(&scenario.name),
                csv_escape(test.label.as_deref().unwrap_or("")),
                wc.topics,
                wc.partitions_per_topic,
                wc.producers_per_topic,
                wc.consumer_groups_per_topic,
                wc.consumers_per_group,
                wc.message_size,
                wc.producer_rate,
                csv_escape(wc.key_distributor.as_deref().unwrap_or("")),
                csv_escape(wc.consumer_type.as_deref().unwrap_or("")),
                wc.consumer_proc_ms_min,
                wc.consumer_proc_ms_max,
                wc.warmup_duration_minutes,
                wc.warmup_producer_rate,
                wc.sample_rate_millis,
                d.backlog_messages,
                d.backlog_size_bytes,
                d.backlog_size_gb,
                d.build_rate,
                d.backlog_producer_rate,
                d.build_duration_ms,
                d.drain_duration_ms,
                d.drain_timeout_minutes,
                drain_minutes,
                drain_throughput_mbs,
            );
            rows.push(row);
        }
    }
}

fn rate_stats(rates: &[f64]) -> (f64, f64, f64) {
    if rates.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let min = rates.iter().copied().reduce(f64::min).unwrap_or(0.0);
    let max = rates.iter().copied().reduce(f64::max).unwrap_or(0.0);
    let avg = rates.iter().sum::<f64>() / rates.len() as f64;
    (min, avg, max)
}

/// Escape a field for CSV: quote if it contains comma, quote, or newline.
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
