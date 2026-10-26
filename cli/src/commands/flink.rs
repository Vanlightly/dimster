//! Deploy-time rendering for the optional Flink session cluster.
//!
//! An environment with a `flink:` block gets a JobManager and N TaskManagers
//! (`run/deploy/kubernetes/base/flink/`), using the stock Flink image. The
//! coordinator runs one Flink SQL job per test point on it (see
//! `dev.dimster.flink.FlinkPipeline`). This module turns the env's flink block
//! into kustomize patches and the Prometheus scrape job; `deploy.rs` decides
//! when to include them, as for the other components.

use anyhow::Result;

use crate::config::{FlinkEnvConfig, ResourceRequests};

/// Defaults when the flink block leaves a field out. Sized for a laptop:
/// stateless SQL needs little memory, and two slots allow parallelism 2.
const DEFAULT_JM_CPU: &str = "500m";
const DEFAULT_JM_MEMORY: &str = "1Gi";
const DEFAULT_TM_REPLICAS: u32 = 1;
const DEFAULT_TM_SLOTS: u32 = 2;
const DEFAULT_TM_CPU: &str = "1";
const DEFAULT_TM_MEMORY: &str = "2Gi";

/// Smallest process sizes Flink starts with. Flink takes 256Mi of metaspace
/// and at least 192Mi of JVM overhead out of the process size first; below
/// these, the JobManager has no room for its 128Mi off-heap and heap, the
/// TaskManager none for its 256Mi of framework memory, network buffers and
/// task heap, and the process exits at startup.
const MIN_JM_MEMORY_MIB: u64 = 768;
const MIN_TM_MEMORY_MIB: u64 = 1024;

/// Base manifests, relative to the overlay root.
pub(crate) const RESOURCES: [&str; 3] = [
    "base/flink/jobmanager.yaml",
    "base/flink/taskmanager.yaml",
    "base/flink/metrics-service.yaml",
];

/// Pod label shared by the JobManager and TaskManagers.
pub(crate) const POD_LABEL: &str = "app=flink";

fn requests_or<'a>(
    requests: Option<&'a ResourceRequests>,
    cpu: &'a str,
    memory: &'a str,
) -> (&'a str, &'a str) {
    (
        requests.and_then(|r| r.cpu.as_deref()).unwrap_or(cpu),
        requests.and_then(|r| r.memory.as_deref()).unwrap_or(memory),
    )
}

/// A Kubernetes memory quantity ("2Gi", "1536Mi", "2G", "512M", bytes) in
/// MiB, for Flink's `*.memory.process.size`, which doesn't accept the
/// Kubernetes suffixes. Decimal units are rounded down.
fn memory_mib(quantity: &str) -> Result<u64> {
    let q = quantity.trim();
    let split = q.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(q.len());
    let (number, unit) = q.split_at(split);
    let n: f64 = number
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid memory quantity '{}'", quantity))?;
    let bytes = match unit {
        "" => n,
        "Ki" => n * 1024.0,
        "Mi" => n * 1024.0 * 1024.0,
        "Gi" => n * 1024.0 * 1024.0 * 1024.0,
        "Ti" => n * 1024.0 * 1024.0 * 1024.0 * 1024.0,
        "k" | "K" => n * 1e3,
        "M" => n * 1e6,
        "G" => n * 1e9,
        "T" => n * 1e12,
        other => anyhow::bail!("Unsupported memory unit '{}' in '{}'", other, quantity),
    };
    Ok((bytes / (1024.0 * 1024.0)) as u64)
}

/// Settings both Flink processes need to find each other and export metrics.
fn common_properties() -> Vec<(String, String)> {
    [
        ("jobmanager.rpc.address", "flink-jobmanager"),
        ("jobmanager.rpc.port", "6123"),
        ("blob.server.port", "6124"),
        (
            "metrics.reporter.prom.factory.class",
            "org.apache.flink.metrics.prometheus.PrometheusReporterFactory",
        ),
        ("metrics.reporter.prom.port", "9249"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

/// Rejects memory requests too small for Flink to start, so that `deploy`
/// fails with the reason instead of leaving pods in CrashLoopBackOff.
pub(crate) fn check_memory(flink: &FlinkEnvConfig, env_name: &str) -> Result<()> {
    let (_, jm_memory) = requests_or(
        flink.jobmanager.as_ref().and_then(|j| j.requests.as_ref()),
        DEFAULT_JM_CPU,
        DEFAULT_JM_MEMORY,
    );
    let (_, tm_memory) = requests_or(
        flink
            .taskmanagers
            .as_ref()
            .and_then(|t| t.requests.as_ref()),
        DEFAULT_TM_CPU,
        DEFAULT_TM_MEMORY,
    );
    for (field, memory, min_mib) in [
        (
            "flink.jobmanager.requests.memory",
            jm_memory,
            MIN_JM_MEMORY_MIB,
        ),
        (
            "flink.taskmanagers.requests.memory",
            tm_memory,
            MIN_TM_MEMORY_MIB,
        ),
    ] {
        if memory_mib(memory)? < min_mib {
            anyhow::bail!(
                "Environment '{}' sets {} to {}, but Flink needs at least {}Mi: it reserves 256Mi of \
                 metaspace and at least 192Mi of JVM overhead before the memory the process runs with.",
                env_name,
                field,
                memory,
                min_mib
            );
        }
    }
    Ok(())
}

/// `FLINK_PROPERTIES` content: Dimster's settings, then the env's `config`
/// (later keys win when the entrypoint merges them).
fn render_properties(props: Vec<(String, String)>, extra: &[(String, String)]) -> String {
    props
        .iter()
        .chain(extra.iter())
        .map(|(k, v)| format!("{k}: {v}\n"))
        .collect()
}

pub(crate) fn jobmanager_properties(flink: &FlinkEnvConfig) -> Result<String> {
    let jm = flink.jobmanager.as_ref();
    let (_, memory) = requests_or(
        jm.and_then(|j| j.requests.as_ref()),
        DEFAULT_JM_CPU,
        DEFAULT_JM_MEMORY,
    );
    let mut props = common_properties();
    props.extend(
        [
            ("jobmanager.bind-host", "0.0.0.0".to_string()),
            ("rest.address", "flink-jobmanager".to_string()),
            ("rest.bind-address", "0.0.0.0".to_string()),
            (
                "jobmanager.memory.process.size",
                format!("{}m", memory_mib(memory)?),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v)),
    );
    Ok(render_properties(props, &flink.config_strings()))
}

pub(crate) fn taskmanager_properties(flink: &FlinkEnvConfig) -> Result<String> {
    let tm = flink.taskmanagers.as_ref();
    let (_, memory) = requests_or(
        tm.and_then(|t| t.requests.as_ref()),
        DEFAULT_TM_CPU,
        DEFAULT_TM_MEMORY,
    );
    let slots = tm.and_then(|t| t.slots).unwrap_or(DEFAULT_TM_SLOTS);
    let mut props = common_properties();
    props.extend(
        [
            ("taskmanager.bind-host", "0.0.0.0".to_string()),
            ("taskmanager.rpc.port", "6122".to_string()),
            ("taskmanager.numberOfTaskSlots", slots.to_string()),
            (
                "taskmanager.memory.process.size",
                format!("{}m", memory_mib(memory)?),
            ),
            // Dimster's jobs are stateless SQL with no RocksDB state, so most
            // of Flink's default managed memory (40%) would sit unused.
            ("taskmanager.memory.managed.fraction", "0.1".to_string()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v)),
    );
    Ok(render_properties(props, &flink.config_strings()))
}

/// JSON6902 patches for one Flink Deployment: image, replicas, resources
/// (requests = limits, like the other benchmark pods), FLINK_PROPERTIES.
fn deployment_patch(
    name: &str,
    image: &str,
    replicas: u32,
    cpu: &str,
    memory: &str,
    properties: &str,
) -> String {
    // A JSON string is valid YAML and keeps the multi-line properties on
    // one line inside the `patch: |` block.
    let properties = serde_json::to_string(properties).expect("string serializes");
    format!(
        r#"  - target:
      kind: Deployment
      name: {name}
    patch: |
      - op: replace
        path: /spec/replicas
        value: {replicas}
      - op: replace
        path: /spec/template/spec/containers/0/image
        value: {image}
      - op: replace
        path: /spec/template/spec/containers/0/resources
        value:
          requests:
            cpu: "{cpu}"
            memory: "{memory}"
          limits:
            cpu: "{cpu}"
            memory: "{memory}"
      - op: replace
        path: /spec/template/spec/containers/0/env/0
        value:
          name: FLINK_PROPERTIES
          value: {properties}
"#
    )
}

fn affinity_patch(name: &str, node_label: &str) -> Result<String> {
    let (key, value) = node_label.split_once('=').ok_or_else(|| {
        anyhow::anyhow!(
            "Invalid nodeLabel '{}': expected format 'key=value'",
            node_label
        )
    })?;
    Ok(format!(
        r#"  - target:
      kind: Deployment
      name: {name}
    patch: |
      - op: add
        path: /spec/template/spec/affinity
        value:
          nodeAffinity:
            requiredDuringSchedulingIgnoredDuringExecution:
              nodeSelectorTerms:
                - matchExpressions:
                    - key: {key}
                      operator: In
                      values: ["{value}"]
"#
    ))
}

/// Mounts the Kafka client TLS Secret where workloads' SSL properties point,
/// since the Flink source and sink connect with the workload's Kafka
/// properties. The JobManager needs it too: the Kafka source enumerator runs there.
fn tls_patch(name: &str, mount_path: &str, secret: &str) -> String {
    format!(
        r#"  - target:
      kind: Deployment
      name: {name}
    patch: |
      - op: add
        path: /spec/template/spec/containers/0/volumeMounts
        value:
          - name: dimster-tls
            mountPath: {mount_path}
            readOnly: true
      - op: add
        path: /spec/template/spec/volumes
        value:
          - name: dimster-tls
            secret:
              secretName: {secret}
              defaultMode: 0444
"#
    )
}

/// All kustomize patches for the Flink cluster. `tls` is the client Secret
/// (mount path, secret name) when Kafka TLS is enabled.
pub(crate) fn patches(flink: &FlinkEnvConfig, tls: Option<(&str, &str)>) -> Result<String> {
    let image = flink.image();
    let jm = flink.jobmanager.as_ref();
    let tm = flink.taskmanagers.as_ref();
    let (jm_cpu, jm_memory) = requests_or(
        jm.and_then(|j| j.requests.as_ref()),
        DEFAULT_JM_CPU,
        DEFAULT_JM_MEMORY,
    );
    let (tm_cpu, tm_memory) = requests_or(
        tm.and_then(|t| t.requests.as_ref()),
        DEFAULT_TM_CPU,
        DEFAULT_TM_MEMORY,
    );
    let tm_replicas = tm.and_then(|t| t.replicas).unwrap_or(DEFAULT_TM_REPLICAS);

    let mut out = String::new();
    out.push_str(&deployment_patch(
        "flink-jobmanager",
        image,
        1,
        jm_cpu,
        jm_memory,
        &jobmanager_properties(flink)?,
    ));
    out.push_str(&deployment_patch(
        "flink-taskmanager",
        image,
        tm_replicas,
        tm_cpu,
        tm_memory,
        &taskmanager_properties(flink)?,
    ));
    if let Some(label) = jm.and_then(|j| j.node_label.as_deref()) {
        out.push_str(&affinity_patch("flink-jobmanager", label)?);
    }
    if let Some(label) = tm.and_then(|t| t.node_label.as_deref()) {
        out.push_str(&affinity_patch("flink-taskmanager", label)?);
    }
    if let Some((mount_path, secret)) = tls {
        out.push_str(&tls_patch("flink-jobmanager", mount_path, secret));
        out.push_str(&tls_patch("flink-taskmanager", mount_path, secret));
    }
    Ok(out)
}

/// Prometheus scrape job for the Flink reporters, appended to the
/// `scrape_configs` list (the last key of prometheus.yml) when the env has
/// Flink, so envs without Flink don't scrape a name that never resolves.
pub(crate) fn scrape_job(ns: &str) -> String {
    format!(
        r#"
      - job_name: 'flink'
        dns_sd_configs:
          - names:
              - 'flink-metrics.{ns}.svc.cluster.local'
            type: A
            port: 9249
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flink(yaml: &str) -> FlinkEnvConfig {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn memory_quantities_convert_to_mib() {
        assert_eq!(memory_mib("2Gi").unwrap(), 2048);
        assert_eq!(memory_mib("1536Mi").unwrap(), 1536);
        assert_eq!(memory_mib("1G").unwrap(), 953);
        assert_eq!(memory_mib("1048576").unwrap(), 1);
        assert!(memory_mib("2 bananas").is_err());
    }

    #[test]
    fn memory_below_what_flink_starts_with_is_rejected() {
        assert!(check_memory(&flink("{}"), "e").is_ok());
        assert!(
            check_memory(
                &flink("jobmanager: {requests: {memory: 768Mi}}\ntaskmanagers: {requests: {memory: 1Gi}}\n"),
                "e"
            )
            .is_ok()
        );
        let err = check_memory(&flink("jobmanager: {requests: {memory: 500Mi}}\n"), "e")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("flink.jobmanager.requests.memory to 500Mi"),
            "{err}"
        );
        assert!(err.contains("at least 768Mi"), "{err}");
        let err = check_memory(&flink("taskmanagers: {requests: {memory: 512Mi}}\n"), "e")
            .unwrap_err()
            .to_string();
        assert!(err.contains("flink.taskmanagers.requests.memory"), "{err}");
    }

    #[test]
    fn defaults_give_one_taskmanager_with_two_slots() {
        let f = flink("{}");
        let tm = taskmanager_properties(&f).unwrap();
        assert!(tm.contains("taskmanager.numberOfTaskSlots: 2\n"), "{tm}");
        assert!(
            tm.contains("taskmanager.memory.process.size: 2048m\n"),
            "{tm}"
        );
        assert!(tm.contains("metrics.reporter.prom.port: 9249\n"));
        let jm = jobmanager_properties(&f).unwrap();
        assert!(
            jm.contains("jobmanager.memory.process.size: 1024m\n"),
            "{jm}"
        );
        assert!(jm.contains("rest.bind-address: 0.0.0.0\n"));

        let p = patches(&f, None).unwrap();
        assert!(p.contains("name: flink-jobmanager"));
        assert!(p.contains("value: flink:2.2.1-java21"));
        assert!(!p.contains("affinity"));
        assert!(!p.contains("dimster-tls"));
    }

    #[test]
    fn env_settings_and_extra_config_are_rendered() {
        let f = flink(
            "image: flink:2.2.1-java17\n\
             taskmanagers:\n  replicas: 3\n  slots: 4\n  nodeLabel: role=flink\n  \
             requests: {cpu: '2', memory: 4Gi}\n\
             config:\n  taskmanager.memory.managed.fraction: 0.3\n",
        );
        let tm = taskmanager_properties(&f).unwrap();
        assert!(tm.contains("taskmanager.numberOfTaskSlots: 4\n"));
        assert!(tm.contains("taskmanager.memory.process.size: 4096m\n"));
        // The env's config comes last, so it overrides Dimster's default.
        assert!(
            tm.ends_with("taskmanager.memory.managed.fraction: 0.3\n"),
            "{tm}"
        );

        let p = patches(&f, Some(("/etc/dimster/tls", "dimster-kafka-client-tls"))).unwrap();
        assert!(p.contains("value: 3\n"));
        assert!(p.contains("value: flink:2.2.1-java17"));
        assert!(p.contains("cpu: \"2\""));
        assert!(p.contains("values: [\"flink\"]"));
        assert_eq!(p.matches("secretName: dimster-kafka-client-tls").count(), 2);
        // Properties are a single JSON string line inside the patch.
        assert!(p.contains("value: \"jobmanager.rpc.address: flink-jobmanager\\n"));
    }

    #[test]
    fn scrape_job_targets_the_namespace() {
        assert!(scrape_job("dimster2").contains("flink-metrics.dimster2.svc.cluster.local"));
    }
}
