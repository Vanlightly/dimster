// Captures the user-facing inputs for a run (workload YAMLs, kafka config,
// broker config) into a stable on-disk location alongside the run's results.
//
// Why this module exists
// ----------------------
// A `dimster run` invocation produces a results directory containing JSONs,
// logs, and charts — but historically the *inputs* that produced those results
// (the workload file, the optional kafka config, the broker config that was
// last deployed) lived only on the user's filesystem and could be edited or
// lost. This module makes each run self-describing: at run start, the CLI
// chooses the results subdirectory name (a yyyy-MM-dd-HH-mm-ss timestamp),
// creates `{output_dir}/{timestamp}/configs/`, and copies the inputs in. The
// timestamp is then passed to the coordinator via `RunRequest.resultsSubdir`
// so the Java side writes its own artifacts (run-request.json, result JSONs)
// into the same directory.
//
// Failure semantics: the configs are written *before* anything is sent to the
// cluster. If the run fails before the coordinator is reached, the user still
// has evidence of what was attempted in `configs/`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow, bail};

use crate::commands::broker_config;

/// Returns a local-time timestamp formatted as `yyyy-MM-dd-HH-mm-ss`.
///
/// Local time (not UTC) is used to match the Java coordinator's existing
/// `SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")` behavior, which uses the JVM's
/// default zone. Falls back to a sentinel if `date` is unavailable so callers
/// always get a non-empty string.
pub fn now_timestamp() -> String {
    let output = std::process::Command::new("date")
        .args(["+%Y-%m-%d-%H-%M-%S"])
        .output();
    match output {
        Ok(o) => {
            let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
            if s.is_empty() {
                "1970-01-01-00-00-00".to_string()
            } else {
                s
            }
        }
        Err(_) => "1970-01-01-00-00-00".to_string(),
    }
}

/// Creates `{output_dir}/{timestamp}/configs/` and copies the supplied
/// workload, kafka, and broker config files into it.
///
/// Returns the path to the run directory (`{output_dir}/{timestamp}`).
///
/// Errors loudly on:
///   - two workloads sharing the same file stem (the Java side keys workloads
///     by stem in a HashMap, so a collision would silently lose one workload —
///     better to refuse than to capture the wrong file)
///   - the configs directory already existing (vanishingly rare two-runs-in-
///     the-same-second collision; merging would corrupt the captured inputs)
///   - any source file being unreadable
pub fn prepare_run_dir(
    output_dir: &str,
    timestamp: &str,
    workloads: &[String],
    kafka_config: Option<&str>,
    broker_config: Option<&str>,
    extra_config: Option<&str>,
    config_path: &Path,
    env_name: &str,
) -> Result<PathBuf> {
    let run_dir = Path::new(output_dir).join(timestamp);
    let configs_dir = run_dir.join("configs");

    if configs_dir.exists() {
        bail!(
            "Run configs directory already exists: {}. \
             Wait one second and retry, or delete the directory.",
            configs_dir.display()
        );
    }
    std::fs::create_dir_all(&configs_dir).map_err(|e| {
        anyhow!(
            "Failed to create run configs directory '{}': {}",
            configs_dir.display(),
            e
        )
    })?;

    // Copy each workload file as configs/workload-{stem}.yaml. The stem must
    // match the key the Java coordinator uses in its workloads HashMap (see
    // run.rs read loop) so the captured filename and the `workloadName` in
    // result JSONs line up.
    let mut seen_stems: HashMap<String, String> = HashMap::new();
    for wl_path in workloads {
        let path = Path::new(wl_path);
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("workload")
            .to_string();

        if let Some(prev) = seen_stems.get(&stem) {
            bail!(
                "Two workload files share the stem '{}': '{}' and '{}'. \
                 Rename one — the run-time HashMap would silently drop one of them.",
                stem, prev, wl_path
            );
        }
        seen_stems.insert(stem.clone(), wl_path.clone());

        let dest = configs_dir.join(format!("workload-{}.yaml", stem));
        std::fs::copy(path, &dest).map_err(|e| {
            anyhow!(
                "Failed to copy workload '{}' to '{}': {}",
                wl_path,
                dest.display(),
                e
            )
        })?;
    }

    // Capture the user's original kafka config (not the connection-injected
    // version). The injected version is reconstructable from this file plus
    // the env config; the user's original is not.
    if let Some(path) = kafka_config {
        let dest = configs_dir.join("kafka.yaml");
        std::fs::copy(path, &dest).map_err(|e| {
            anyhow!(
                "Failed to copy kafka config '{}' to '{}': {}",
                path,
                dest.display(),
                e
            )
        })?;
    }

    // Broker config is only meaningful when it was actually used to deploy.
    // The CLI doesn't verify that; it captures whatever the env config points
    // at right now, on the assumption the user keeps these in sync.
    //
    // The captured file is the *effective* broker config — i.e. the broker
    // config file properties merged with any inline `kafka.extraConfig`
    // overrides — so the YAML on disk matches what the broker actually ran
    // with. Copying the source file verbatim would silently drop the
    // overrides and mislead anyone analysing the results later.
    if let Some(eff) = broker_config::resolve_effective(broker_config, extra_config)? {
        let dest = configs_dir.join("broker.yaml");
        let yaml = eff.to_yaml()?;
        std::fs::write(&dest, yaml).map_err(|e| {
            anyhow!(
                "Failed to write effective broker config to '{}': {}",
                dest.display(),
                e
            )
        })?;
    }

    // Capture the env subtree from dimster-config.yaml (with deployment- and
    // connection-specific fields stripped). See `write_env_capture` for the
    // rationale on which fields are removed.
    write_env_capture(config_path, env_name, &configs_dir.join("environment.yaml"))?;

    println!(
        "==> Captured run configs to {}/",
        configs_dir.display()
    );

    Ok(run_dir)
}

/// Reads `config_path` (the dimster-config.yaml in effect for this run),
/// pulls out the `environments.<env_name>` subtree, strips fields that aren't
/// useful in a results archive, and writes the result to `dest`.
///
/// Stripped fields:
///   - `image`      — env-level deployment artifact name; tells you nothing
///                    about the run's behavior
///   - `context`    — kubectl context; machine-specific user data
///   - `connection` — external-Kafka connection info (may include
///                    bootstrap.servers / credentials we don't want pasted
///                    into shared result tarballs)
///
/// `kafka.image` is intentionally kept — it identifies which Kafka container
/// image was used, which IS relevant to anyone analyzing results later.
///
/// We re-read the YAML file rather than serializing the in-memory
/// `Environment` struct to keep the diff small (Environment + ~10 nested
/// types would all need `Serialize` + `skip_serializing_if` annotations) and
/// to preserve the user's exact field set without inventing defaults.
fn write_env_capture(config_path: &Path, env_name: &str, dest: &Path) -> Result<()> {
    let content = std::fs::read_to_string(config_path).map_err(|e| {
        anyhow!(
            "Failed to read dimster config '{}' for environment capture: {}",
            config_path.display(),
            e
        )
    })?;

    let root: serde_yaml::Value = serde_yaml::from_str(&content).map_err(|e| {
        anyhow!(
            "Failed to parse dimster config '{}' as YAML: {}",
            config_path.display(),
            e
        )
    })?;

    // Pull out environments.<env_name> as a Mapping. Anything else (missing
    // section, env not present, env not a mapping) is a bug in the caller —
    // we shouldn't have gotten here without a validated env.
    let env_value = root
        .get("environments")
        .and_then(|envs| envs.get(env_name))
        .ok_or_else(|| {
            anyhow!(
                "Environment '{}' not found in '{}' while capturing run configs.",
                env_name,
                config_path.display()
            )
        })?;

    let mut env_map = env_value
        .as_mapping()
        .ok_or_else(|| {
            anyhow!(
                "Environment '{}' in '{}' is not a YAML mapping.",
                env_name,
                config_path.display()
            )
        })?
        .clone();

    // Use `shift_remove` (not `remove`): the latter is swap-remove on the
    // underlying IndexMap and would scramble the surviving keys' order. We
    // want the captured file to read in the same top-to-bottom order as the
    // user's source config.
    for key in ["image", "context", "connection"] {
        env_map.shift_remove(serde_yaml::Value::String(key.to_string()));
    }

    let yaml = serde_yaml::to_string(&serde_yaml::Value::Mapping(env_map))
        .map_err(|e| anyhow!("Failed to serialize environment capture: {}", e))?;

    std::fs::write(dest, yaml).map_err(|e| {
        anyhow!(
            "Failed to write environment capture to '{}': {}",
            dest.display(),
            e
        )
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write_file(dir: &Path, name: &str, contents: &str) -> String {
        let p = dir.join(name);
        fs::write(&p, contents).unwrap();
        p.to_string_lossy().into_owned()
    }

    /// Writes a minimal dimster-config.yaml containing a single env named
    /// `local` whose body is `env_yaml`. Returns the file path. Tests that
    /// don't care about the env capture content can pass any non-empty body.
    fn write_config(dir: &Path, env_yaml: &str) -> PathBuf {
        let body = format!(
            "environments:\n  local:\n{}\n",
            env_yaml
                .lines()
                .map(|l| format!("    {}", l))
                .collect::<Vec<_>>()
                .join("\n"),
        );
        let p = dir.join("dimster-config.yaml");
        fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn copies_all_three_kinds_to_expected_names() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir(&src).unwrap();
        let out = tmp.path().join("out").to_string_lossy().into_owned();

        let wl = write_file(&src, "myload.yaml", "wl: 1");
        let kc = write_file(&src, "kafka.yaml", "kc: 1");
        let bc = write_file(
            &src,
            "broker.yaml",
            "name: default\nproperties:\n  num.io.threads: 8\n",
        );
        let cfg = write_config(&src, "kafka:\n  version: \"4.2.0\"");

        let run_dir = prepare_run_dir(
            &out,
            "2026-04-22-10-00-00",
            &[wl],
            Some(&kc),
            Some(&bc),
            None,
            &cfg,
            "local",
        )
        .unwrap();

        let configs = run_dir.join("configs");
        assert_eq!(fs::read_to_string(configs.join("workload-myload.yaml")).unwrap(), "wl: 1");
        assert_eq!(fs::read_to_string(configs.join("kafka.yaml")).unwrap(), "kc: 1");

        // Broker config is captured as the effective YAML, not the source file.
        let captured = fs::read_to_string(configs.join("broker.yaml")).unwrap();
        assert!(captured.contains("name: default"));
        assert!(captured.contains("num.io.threads"));

        // Environment capture exists alongside the other configs.
        assert!(configs.join("environment.yaml").exists());
    }

    #[test]
    fn captures_effective_broker_config_with_extra_overrides() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir(&src).unwrap();
        let out = tmp.path().join("out").to_string_lossy().into_owned();

        let wl = write_file(&src, "smoke.yaml", "x");
        let bc = write_file(
            &src,
            "broker.yaml",
            "name: default\nproperties:\n  num.io.threads: 8\n  log.retention.hours: 1\n",
        );
        let cfg = write_config(&src, "kafka:\n  version: \"4.2.0\"");

        let run_dir = prepare_run_dir(
            &out,
            "2026-04-22-10-00-00",
            &[wl],
            None,
            Some(&bc),
            Some("num.io.threads=16,foo=bar"),
            &cfg,
            "local",
        )
        .unwrap();

        // Re-parse the captured YAML and check the merge took effect.
        let captured_path = run_dir.join("configs/broker.yaml");
        let eff = broker_config::resolve_effective(
            Some(&captured_path.to_string_lossy()),
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(eff.properties.get("num.io.threads").unwrap(), "16");
        assert_eq!(eff.properties.get("log.retention.hours").unwrap(), "1");
        assert_eq!(eff.properties.get("foo").unwrap(), "bar");
    }

    #[test]
    fn captures_inline_only_extra_config_when_no_broker_config_file() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir(&src).unwrap();
        let out = tmp.path().join("out").to_string_lossy().into_owned();

        let wl = write_file(&src, "smoke.yaml", "x");
        let cfg = write_config(&src, "kafka:\n  version: \"4.2.0\"");
        let run_dir = prepare_run_dir(
            &out,
            "2026-04-22-10-00-00",
            &[wl],
            None,
            None,
            Some("a=1"),
            &cfg,
            "local",
        )
        .unwrap();

        // Re-parse and verify shape, rather than asserting on YAML quoting style.
        let captured_path = run_dir.join("configs/broker.yaml");
        let eff = broker_config::resolve_effective(
            Some(&captured_path.to_string_lossy()),
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(eff.name, "(inline)");
        assert_eq!(eff.properties.get("a").unwrap(), "1");
    }

    #[test]
    fn skips_optional_files_silently() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir(&src).unwrap();
        let out = tmp.path().join("out").to_string_lossy().into_owned();

        let wl = write_file(&src, "smoke.yaml", "x");
        let cfg = write_config(&src, "kafka:\n  version: \"4.2.0\"");
        let run_dir = prepare_run_dir(
            &out,
            "2026-04-22-10-00-00",
            &[wl],
            None,
            None,
            None,
            &cfg,
            "local",
        )
        .unwrap();

        let configs = run_dir.join("configs");
        assert!(configs.join("workload-smoke.yaml").exists());
        assert!(!configs.join("kafka.yaml").exists());
        assert!(!configs.join("broker.yaml").exists());
        // Environment capture is never optional — it's always written.
        assert!(configs.join("environment.yaml").exists());
    }

    #[test]
    fn errors_on_workload_stem_collision() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("a");
        let b = tmp.path().join("b");
        fs::create_dir(&a).unwrap();
        fs::create_dir(&b).unwrap();
        let out = tmp.path().join("out").to_string_lossy().into_owned();

        let wl_a = write_file(&a, "foo.yaml", "1");
        let wl_b = write_file(&b, "foo.yaml", "2");
        let cfg = write_config(&a, "kafka:\n  version: \"4.2.0\"");

        let err = prepare_run_dir(
            &out,
            "2026-04-22-10-00-00",
            &[wl_a, wl_b],
            None,
            None,
            None,
            &cfg,
            "local",
        )
        .unwrap_err();
        assert!(err.to_string().contains("share the stem 'foo'"));
    }

    #[test]
    fn errors_when_configs_dir_already_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let out = tmp.path().join("out");
        let configs = out.join("2026-04-22-10-00-00").join("configs");
        fs::create_dir_all(&configs).unwrap();

        // The bail happens before env capture runs, so config_path can be
        // any path — it isn't read.
        let err = prepare_run_dir(
            &out.to_string_lossy(),
            "2026-04-22-10-00-00",
            &[],
            None,
            None,
            None,
            Path::new("/nonexistent/dimster-config.yaml"),
            "local",
        )
        .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    #[test]
    fn captures_environment_minus_stripped_fields() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir(&src).unwrap();
        let out = tmp.path().join("out").to_string_lossy().into_owned();

        let wl = write_file(&src, "smoke.yaml", "x");
        // Env contains all three strip targets PLUS fields that should
        // survive (kafka, hardware, namespace).
        let cfg = write_config(
            &src,
            "image: dimster\n\
             context: minikube\n\
             namespace: dimster\n\
             hardware: \"local-laptop\"\n\
             connection:\n  bootstrap.servers: external:9092\n  security.protocol: SSL\n\
             kafka:\n  version: \"4.2.0\"\n  image: apache/kafka",
        );

        let run_dir = prepare_run_dir(
            &out,
            "2026-04-22-10-00-00",
            &[wl],
            None,
            None,
            None,
            &cfg,
            "local",
        )
        .unwrap();

        let captured_path = run_dir.join("configs/environment.yaml");
        let captured: serde_yaml::Value =
            serde_yaml::from_str(&fs::read_to_string(&captured_path).unwrap()).unwrap();
        let map = captured.as_mapping().unwrap();

        // Stripped: image, context, connection.
        assert!(map.get(serde_yaml::Value::String("image".into())).is_none());
        assert!(map.get(serde_yaml::Value::String("context".into())).is_none());
        assert!(map.get(serde_yaml::Value::String("connection".into())).is_none());

        // Surviving fields are still there.
        assert_eq!(
            map.get(serde_yaml::Value::String("namespace".into())).unwrap().as_str(),
            Some("dimster")
        );
        assert_eq!(
            map.get(serde_yaml::Value::String("hardware".into())).unwrap().as_str(),
            Some("local-laptop")
        );

        // kafka section preserved, including kafka.image (intentionally kept
        // — it identifies the Kafka container image, which IS results-relevant).
        let kafka = map
            .get(serde_yaml::Value::String("kafka".into()))
            .unwrap()
            .as_mapping()
            .unwrap();
        assert_eq!(
            kafka.get(serde_yaml::Value::String("version".into())).unwrap().as_str(),
            Some("4.2.0")
        );
        assert_eq!(
            kafka.get(serde_yaml::Value::String("image".into())).unwrap().as_str(),
            Some("apache/kafka")
        );
    }

    #[test]
    fn captures_environment_preserves_source_field_order() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir(&src).unwrap();
        let out = tmp.path().join("out").to_string_lossy().into_owned();

        let wl = write_file(&src, "smoke.yaml", "x");
        // Source order: image, distro, context, hardware, deploy, kafka.
        // After stripping image+context, surviving order MUST be:
        // distro, hardware, deploy, kafka. Guards against the easy mistake
        // of using Mapping::remove (swap-remove) instead of shift_remove.
        let cfg = write_config(
            &src,
            "image: dimster\n\
             distro: minikube\n\
             context: minikube\n\
             hardware: laptop\n\
             deploy: full\n\
             kafka:\n  version: \"4.2.0\"",
        );

        let run_dir = prepare_run_dir(
            &out,
            "2026-04-22-10-00-00",
            &[wl],
            None,
            None,
            None,
            &cfg,
            "local",
        )
        .unwrap();

        let captured: serde_yaml::Value = serde_yaml::from_str(
            &fs::read_to_string(run_dir.join("configs/environment.yaml")).unwrap(),
        )
        .unwrap();
        let keys: Vec<String> = captured
            .as_mapping()
            .unwrap()
            .keys()
            .map(|k| k.as_str().unwrap().to_string())
            .collect();
        assert_eq!(keys, vec!["distro", "hardware", "deploy", "kafka"]);
    }

    #[test]
    fn captures_environment_when_optional_stripped_fields_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir(&src).unwrap();
        let out = tmp.path().join("out").to_string_lossy().into_owned();

        let wl = write_file(&src, "smoke.yaml", "x");
        // None of image/context/connection present — should not error.
        let cfg = write_config(&src, "kafka:\n  version: \"4.2.0\"");

        let run_dir = prepare_run_dir(
            &out,
            "2026-04-22-10-00-00",
            &[wl],
            None,
            None,
            None,
            &cfg,
            "local",
        )
        .unwrap();

        let captured_path = run_dir.join("configs/environment.yaml");
        let captured: serde_yaml::Value =
            serde_yaml::from_str(&fs::read_to_string(&captured_path).unwrap()).unwrap();
        let map = captured.as_mapping().unwrap();
        assert!(map.get(serde_yaml::Value::String("kafka".into())).is_some());
    }

    #[test]
    fn errors_when_env_missing_from_config() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir(&src).unwrap();
        let out = tmp.path().join("out").to_string_lossy().into_owned();

        let wl = write_file(&src, "smoke.yaml", "x");
        let cfg = write_config(&src, "kafka:\n  version: \"4.2.0\"");

        let err = prepare_run_dir(
            &out,
            "2026-04-22-10-00-00",
            &[wl],
            None,
            None,
            None,
            &cfg,
            "production",
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("Environment 'production' not found"),
            "actual error: {}",
            err
        );
    }

    #[test]
    fn errors_when_config_file_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        fs::create_dir(&src).unwrap();
        let out = tmp.path().join("out").to_string_lossy().into_owned();

        let wl = write_file(&src, "smoke.yaml", "x");

        let err = prepare_run_dir(
            &out,
            "2026-04-22-10-00-00",
            &[wl],
            None,
            None,
            None,
            Path::new("/nonexistent/dimster-config.yaml"),
            "local",
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("Failed to read dimster config"),
            "actual error: {}",
            err
        );
    }
}
