// Resolves the effective broker config used for a deploy.
//
// Why this module exists
// ----------------------
// A Dimster environment can supply broker configuration in two places:
//
//   1. `kafka.brokerConfig` — a path to a YAML file with a `name:` and a
//      `properties:` map (e.g. run/broker-configs/default.yaml).
//   2. `kafka.extraConfig` — an inline `key=value,key=value,...` string
//      whose entries override anything in the file (last-wins).
//
// `deploy.rs` merges the two and emits a comma-separated `KAFKA_EXTRA_CONFIG`
// env var that the broker entrypoint appends to server.properties. Until now
// nothing else used the merge — `run_configs.rs` just copied the brokerConfig
// file verbatim into each run's `configs/` directory, which made the captured
// file diverge from what the broker actually ran with whenever `extraConfig`
// was set.
//
// This module centralises the merge so both deploy and the per-run capture
// see the same effective config: deploy serialises it as the env-var string,
// the capture serialises it as a broker config YAML file.
//
// Properties are stored in a `BTreeMap` so output ordering is deterministic
// (alphabetical by key). The previous deploy code used `HashMap` iteration,
// so the env-var entries are now sorted; this is a strict improvement (all
// keys are unique post-merge, so order is irrelevant for correctness).

use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

/// On-disk shape of a broker config file (run/broker-configs/*.yaml).
/// Both fields default so a missing `name:` or `properties:` doesn't error;
/// we surface a clearer error from the merge if the file is meaningless.
#[derive(Deserialize)]
struct RawBrokerConfig {
    #[serde(default = "default_name")]
    name: String,
    #[serde(default)]
    properties: BTreeMap<String, serde_yaml::Value>,
}

fn default_name() -> String {
    "(inline)".to_string()
}

/// Broker config after `kafka.brokerConfig` is merged with the `kafka.extraConfig`
/// overrides — i.e. exactly what the broker boots with. Held as plain strings
/// because that's how Kafka consumes them; numeric/boolean shapes from the YAML
/// source are stringified during the merge.
#[derive(Debug)]
pub struct EffectiveBrokerConfig {
    /// `name:` from the source file, or `(inline)` when only `extraConfig` was
    /// supplied. Carried through purely so the deploy-time log line can show
    /// which named config was picked up.
    pub name: String,
    pub properties: BTreeMap<String, String>,
}

impl EffectiveBrokerConfig {
    /// Format suitable for the `KAFKA_EXTRA_CONFIG` env var consumed by the
    /// broker pod's entrypoint: comma-separated `key=value` pairs.
    pub fn to_extra_config_string(&self) -> String {
        self.properties
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Format suitable for capture into a results directory as a broker config
    /// YAML file. Same shape as the on-disk source files in run/broker-configs/.
    pub fn to_yaml(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Out<'a> {
            name: &'a str,
            properties: &'a BTreeMap<String, String>,
        }
        let out = Out {
            name: &self.name,
            properties: &self.properties,
        };
        serde_yaml::to_string(&out).context("Failed to serialize effective broker config as YAML")
    }
}

/// Build the effective broker config from a `brokerConfig` path and an
/// optional inline `extraConfig` string. Returns `Ok(None)` when neither is
/// supplied (no broker overrides at all) so callers can decide whether to
/// emit anything.
///
/// Merge order:
///   1. Properties from the brokerConfig file (loaded as YAML).
///   2. Inline `extraConfig` `key=value` pairs, split on the first `=`
///      (matches the entrypoint's expectation that values may contain `=`).
///
/// Inline entries override file entries on conflict.
pub fn resolve_effective(
    broker_config_path: Option<&str>,
    extra_config: Option<&str>,
) -> Result<Option<EffectiveBrokerConfig>> {
    if broker_config_path.is_none() && extra_config.is_none() {
        return Ok(None);
    }

    let mut name = "(inline)".to_string();
    let mut props: BTreeMap<String, String> = BTreeMap::new();

    // Layer 1: brokerConfig file properties
    if let Some(path) = broker_config_path {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow!("Cannot read broker config '{}': {}", path, e))?;
        let raw: RawBrokerConfig = serde_yaml::from_str(&content)
            .map_err(|e| anyhow!("Cannot parse broker config '{}': {}", path, e))?;
        name = raw.name;
        for (k, v) in raw.properties {
            props.insert(k, yaml_value_to_string(v));
        }
    }

    // Layer 2: inline extraConfig overrides
    if let Some(extras) = extra_config {
        for pair in extras.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            match pair.split_once('=') {
                Some((k, v)) => {
                    props.insert(k.trim().to_string(), v.trim().to_string());
                }
                None => bail!("Invalid extraConfig entry '{}': missing '='", pair),
            }
        }
    }

    Ok(Some(EffectiveBrokerConfig {
        name,
        properties: props,
    }))
}

/// Stringify a YAML value the same way the previous deploy.rs inline logic
/// did: keep strings verbatim, render anything else through serde_yaml and
/// trim the trailing newline. Keeps the merge transparent for users who
/// wrote `min.insync.replicas: 2` (an int) in their broker config file.
fn yaml_value_to_string(v: serde_yaml::Value) -> String {
    match v {
        serde_yaml::Value::String(s) => s,
        other => serde_yaml::to_string(&other)
            .unwrap_or_default()
            .trim()
            .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn write(dir: &std::path::Path, name: &str, contents: &str) -> String {
        let p = dir.join(name);
        fs::write(&p, contents).unwrap();
        p.to_string_lossy().into_owned()
    }

    #[test]
    fn returns_none_when_no_inputs() {
        assert!(resolve_effective(None, None).unwrap().is_none());
    }

    #[test]
    fn loads_broker_config_file_only() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write(
            tmp.path(),
            "bc.yaml",
            "name: default\nproperties:\n  default.replication.factor: 3\n  min.insync.replicas: 2\n",
        );

        let eff = resolve_effective(Some(&path), None).unwrap().unwrap();
        assert_eq!(eff.name, "default");
        assert_eq!(eff.properties.get("default.replication.factor").unwrap(), "3");
        assert_eq!(eff.properties.get("min.insync.replicas").unwrap(), "2");
    }

    #[test]
    fn extra_config_overrides_file() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write(
            tmp.path(),
            "bc.yaml",
            "name: default\nproperties:\n  num.io.threads: 8\n  log.retention.hours: 1\n",
        );

        let eff = resolve_effective(Some(&path), Some("num.io.threads=16,foo=bar"))
            .unwrap()
            .unwrap();
        assert_eq!(eff.properties.get("num.io.threads").unwrap(), "16"); // overridden
        assert_eq!(eff.properties.get("log.retention.hours").unwrap(), "1"); // inherited
        assert_eq!(eff.properties.get("foo").unwrap(), "bar"); // added
    }

    #[test]
    fn extra_config_only_synthesizes_inline_name() {
        let eff = resolve_effective(None, Some("a=1,b=2"))
            .unwrap()
            .unwrap();
        assert_eq!(eff.name, "(inline)");
        assert_eq!(eff.properties.len(), 2);
    }

    #[test]
    fn extra_config_value_can_contain_equals() {
        let eff = resolve_effective(None, Some("listener.security.protocol.map=PLAINTEXT:PLAINTEXT,X=Y=Z"))
            .unwrap()
            .unwrap();
        // Split on first '=', so X=Y=Z becomes X -> "Y=Z"
        assert_eq!(eff.properties.get("X").unwrap(), "Y=Z");
    }

    #[test]
    fn errors_on_malformed_extra_config_entry() {
        let err = resolve_effective(None, Some("noequalshere")).unwrap_err();
        assert!(err.to_string().contains("missing '='"));
    }

    #[test]
    fn to_extra_config_string_is_sorted_and_comma_joined() {
        let eff = resolve_effective(None, Some("zeta=last,alpha=first,beta=middle"))
            .unwrap()
            .unwrap();
        assert_eq!(
            eff.to_extra_config_string(),
            "alpha=first,beta=middle,zeta=last"
        );
    }

    #[test]
    fn to_yaml_round_trips_through_resolve() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write(
            tmp.path(),
            "bc.yaml",
            "name: default\nproperties:\n  num.io.threads: 8\n",
        );
        let eff = resolve_effective(Some(&path), Some("foo=bar"))
            .unwrap()
            .unwrap();
        let yaml = eff.to_yaml().unwrap();

        // Re-parse the captured YAML and re-resolve from it; should be identical.
        let path2 = write(tmp.path(), "captured.yaml", &yaml);
        let eff2 = resolve_effective(Some(&path2), None).unwrap().unwrap();
        assert_eq!(eff2.name, "default");
        assert_eq!(eff2.properties, eff.properties);
    }
}
