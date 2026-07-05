use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};

use crate::config::ConnectionConfig;
use crate::kubectl::Kubectl;

/// Filter flags passed through to `benchmark-inspect` (the Java InspectMain
/// CLI) in external mode. The four bools mirror the four `--*` flags on the
/// `dimster inspect` command. See the Java side's `InspectMain.Filters` for
/// the rendering semantics — the Rust side just transports the flags.
#[derive(Debug, Clone, Copy, Default)]
pub struct InspectFilters {
    pub dimster_only: bool,
    pub topics: bool,
    pub group_offsets: bool,
    pub group_members: bool,
    pub metadata_quorum: bool,
}

impl InspectFilters {
    /// True if any flag is set. Used by full-mode `run()` to decide whether to
    /// print a "filter flags are external-mode only" warning.
    pub fn any(&self) -> bool {
        self.dimster_only
            || self.topics
            || self.group_offsets
            || self.group_members
            || self.metadata_quorum
    }
}

/// Builds the trailing `--*` flag tail to append to `benchmark-inspect`'s
/// argv. Returns flags in a fixed order so output is deterministic and easy
/// to test. Empty when no filters are set (the no-flag default = show
/// everything, matching the Java side).
fn build_inspect_argv(filters: &InspectFilters) -> Vec<&'static str> {
    let mut out = Vec::new();
    if filters.dimster_only {
        out.push("--dimster-only");
    }
    if filters.topics {
        out.push("--topics");
    }
    if filters.group_offsets {
        out.push("--group-offsets");
    }
    if filters.group_members {
        out.push("--group-members");
    }
    if filters.metadata_quorum {
        out.push("--metadata-quorum");
    }
    out
}

/// Path inside the dimster image to the inspect launcher script. Set by
/// the package assembly (see package-common/src/assemble/bin.xml — every
/// file under `bin/` lands at /benchmark/bin/<name> with mode 755).
const INSPECT_TOOL_PATH: &str = "/benchmark/bin/benchmark-inspect";

/// Name of the long-lived helper pod that hosts the AdminClient inspect
/// tool in external deploy mode. One per namespace; created lazily on
/// first inspect, sleeps idle between calls, removed by `dimster teardown`
/// (label-selected via the `app=dimster-inspect` label).
const INSPECT_POD: &str = "dimster-inspect";

/// Full-mode inspect: shells into the in-cluster kafka-0 broker pod and runs
/// Kafka's own CLI scripts (kafka-topics.sh / kafka-consumer-groups.sh /
/// kafka-share-groups.sh). Output format is whatever those scripts produce
/// — preserved here verbatim for backwards compatibility with users who
/// have grep/diff workflows against this output.
///
/// `filters` is accepted for parity with `run_external` but the four flags
/// are external-mode-only — full-mode output comes from `kafka-*.sh` shell
/// scripts which don't have an equivalent filtering surface. If any filter
/// flag is set we print a single warning line so the user knows their flags
/// were ignored, then fall through to the normal script.
pub fn run(kubectl: &Kubectl, filters: &InspectFilters) -> Result<()> {
    let pod = "kafka-0";
    if filters.any() {
        eprintln!(
            "warning: inspect filter flags (--dimster-only/--topics/--group-offsets/\
             --group-members/--metadata-quorum) are external-mode only and will be ignored in full mode"
        );
    }
    println!("==> Inspecting Kafka cluster via {}", pod);

    let script = r#"
unset KAFKA_OPTS
BOOTSTRAP="localhost:9092"
BIN="/opt/kafka/bin"

echo "================================================================================"
echo "  TOPICS"
echo "================================================================================"
$BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe 2>/dev/null

echo ""
echo "================================================================================"
echo "  CONSUMER GROUPS"
echo "================================================================================"
# kafka-consumer-groups.sh --list uses a server-side protocolTypes filter
# (via ListGroupsOptions.forConsumerGroups()) that can miss classic groups in
# Kafka 4.x.  kafka-groups.sh --list --consumer retrieves all groups first and
# filters client-side, which is more reliable.
if [ -f "$BIN/kafka-groups.sh" ]; then
    GROUPS=$($BIN/kafka-groups.sh --bootstrap-server $BOOTSTRAP --list --consumer 2>/dev/null | tail -n +2 | awk '{print $1}' | grep -v '^$')
else
    GROUPS=$($BIN/kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --list 2>/dev/null)
fi
if [ -z "$GROUPS" ]; then
    echo "(none)"
else
    for g in $GROUPS; do
        echo ""
        echo "--- Group: $g ---"
        $BIN/kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group "$g" 2>/dev/null
        echo ""
        echo "--- Group Members: $g ---"
        $BIN/kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group "$g" --members 2>/dev/null
        echo ""
        echo "--- Group State: $g ---"
        $BIN/kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group "$g" --state 2>/dev/null
    done
fi

echo ""
echo "================================================================================"
echo "  SHARE GROUPS"
echo "================================================================================"
if [ -f "$BIN/kafka-share-groups.sh" ]; then
    SGROUPS=$($BIN/kafka-share-groups.sh --bootstrap-server $BOOTSTRAP --list 2>/dev/null)
    if [ -z "$SGROUPS" ]; then
        echo "(none)"
    else
        for g in $SGROUPS; do
            echo ""
            echo "--- Share Group: $g ---"
            $BIN/kafka-share-groups.sh --bootstrap-server $BOOTSTRAP --describe --group "$g" 2>/dev/null
            echo ""
            echo "--- Share Group Members: $g ---"
            $BIN/kafka-share-groups.sh --bootstrap-server $BOOTSTRAP --describe --group "$g" --members 2>/dev/null
            echo ""
            echo "--- Share Group State: $g ---"
            $BIN/kafka-share-groups.sh --bootstrap-server $BOOTSTRAP --describe --group "$g" --state 2>/dev/null
        done
    fi
else
    echo "(kafka-share-groups.sh not available)"
fi
"#;

    kubectl.run_inherit(&["exec", pod, "-c", "kafka", "--", "bash", "-c", script])?;

    Ok(())
}

/// External-mode inspect: ensures a long-lived `dimster-inspect` pod exists
/// in the env's namespace and runs the InspectMain Java tool inside it. The
/// pod is created lazily on the first call and sleeps idle between calls,
/// so subsequent inspects (and `watch -n 5 dimster inspect ...`) only pay
/// AdminClient RPC cost.
///
/// Why a dedicated pod and not the coordinator: spinning up a second JVM
/// inside the coordinator container competes for memory and CPU with the
/// running coordinator process — enough to stall the coordinator's log
/// stream to a watching `dimster reattach` session in another terminal.
/// A separate sleeping pod sidesteps that contention entirely.
///
/// TLS/SASL note: this pod uses the same image and consumes the same
/// `connection.*` properties as the existing external-mode worker and
/// coordinator pods, so it inherits identical behavior. SASL_SSL with public
/// CAs (Confluent Cloud) and PLAINTEXT both work out of the box; custom-CA
/// or mTLS scenarios work iff the truststore path resolves inside the
/// dimster image — same constraint `dimster run` already has.
pub fn run_external(
    kubectl: &Kubectl,
    conn: &ConnectionConfig,
    filters: &InspectFilters,
) -> Result<()> {
    // Validate up front so a misconfigured env fails before we start
    // touching the cluster.
    let _ = conn.bootstrap_servers()?;

    let pod = ensure_inspect_pod(kubectl)?;

    // Probe for the launcher script. A coordinator image built before this
    // change won't have it; without this probe the user gets a confusing
    // "exec: ...: not found" from kubectl. With it they get a clear hint.
    let probe = kubectl.run(&["exec", &pod, "--", "test", "-x", INSPECT_TOOL_PATH]);
    if probe.is_err() {
        bail!(
            "inspect pod image does not include {} — rebuild with 'dimster build <env>' \
             then `kubectl delete pod {}` to refresh",
            INSPECT_TOOL_PATH,
            INSPECT_POD,
        );
    }

    println!("==> Inspecting external Kafka cluster via {}", pod);

    let props = serialize_properties(&conn.properties);
    // Build the full argv: fixed prefix + filter flag tail. Done as an owned
    // Vec<&str> rather than chaining slices because run_inherit_with_stdin
    // wants a single contiguous &[&str].
    let mut argv: Vec<&str> = vec!["exec", "-i", &pod, "--", INSPECT_TOOL_PATH, "--props", "-"];
    argv.extend(build_inspect_argv(filters));
    kubectl.run_inherit_with_stdin(&props, &argv)?;

    Ok(())
}

/// Idempotently ensure the `dimster-inspect` Pod is Ready in the env's
/// namespace, returning its name. Creates the Pod on first call; on
/// subsequent calls it just verifies readiness (which is a fast no-op on a
/// healthy sleeping pod) and returns.
///
/// The pod's image and imagePullPolicy are read from the live coordinator
/// pod's spec rather than re-derived from env config. This is the only way
/// to track the actual deployed image — `dimster build` produces tagged
/// images like `dimster:latest-kafka4.2.0`, deploy patches them in via a
/// kustomize image override, and re-deriving here would silently drift
/// every time that derivation changes (or when a user supplies
/// pre-tagged images via env config). Reading from the coordinator means
/// inspect ALWAYS matches whatever's currently running.
fn ensure_inspect_pod(kubectl: &Kubectl) -> Result<String> {
    let exists = kubectl
        .run(&["get", "pod", INSPECT_POD, "-o", "name"])
        .is_ok();

    if !exists {
        let (image, pull_policy) = read_coordinator_image_spec(kubectl)?;
        let manifest = render_inspect_pod_manifest(kubectl.namespace(), &image, &pull_policy);
        let tmp = tempfile::NamedTempFile::new()
            .context("Failed to create temp file for inspect-pod manifest")?;
        std::fs::write(tmp.path(), &manifest).context("Failed to write inspect-pod manifest")?;
        println!("==> Creating {} pod (image: {})", INSPECT_POD, image);
        kubectl.run(&["apply", "-f", &tmp.path().to_string_lossy()])?;
    }

    // 120s timeout: cold image pulls from a remote registry on a slow
    // network can easily take 60+ seconds. A pod that's already Ready
    // returns immediately, so this is cheap on the hot path.
    let wait = kubectl.run(&[
        "wait",
        "--for=condition=Ready",
        &format!("pod/{}", INSPECT_POD),
        "--timeout=120s",
    ]);
    if let Err(e) = wait {
        bail!(
            "{} pod did not become Ready: {}\n    Check `kubectl describe pod {}` and \
             `kubectl logs {}` for details. Delete with `kubectl delete pod {}` to retry.",
            INSPECT_POD,
            e,
            INSPECT_POD,
            INSPECT_POD,
            INSPECT_POD,
        );
    }

    Ok(INSPECT_POD.to_string())
}

/// Read the image reference and `imagePullPolicy` from the live
/// benchmark-coordinator pod. Used to ensure the inspect pod uses the
/// EXACT image string the env was deployed with — including any kustomize
/// image overrides like `dimster:latest-kafka4.2.0` that re-derivation
/// from env config would miss.
fn read_coordinator_image_spec(kubectl: &Kubectl) -> Result<(String, String)> {
    let image = kubectl
        .run(&[
            "get",
            "pod",
            "-l",
            "app=benchmark-coordinator",
            "-o",
            "jsonpath={.items[0].spec.containers[0].image}",
        ])
        .context("Failed to read coordinator pod image")?;
    let image = image.trim().to_string();
    if image.is_empty() {
        bail!(
            "could not read coordinator image — is the env deployed? Run 'dimster deploy <env>' first."
        );
    }
    let pull_policy = kubectl
        .run(&[
            "get",
            "pod",
            "-l",
            "app=benchmark-coordinator",
            "-o",
            "jsonpath={.items[0].spec.containers[0].imagePullPolicy}",
        ])
        .unwrap_or_default()
        .trim()
        .to_string();
    // Default to IfNotPresent if the field is empty (Kubernetes' own default
    // depends on the image tag — :latest → Always, anything else → IfNotPresent
    // — which is fine for our use too).
    let pull_policy = if pull_policy.is_empty() {
        "IfNotPresent".to_string()
    } else {
        pull_policy
    };
    Ok((image, pull_policy))
}

/// Render the YAML manifest for the inspect helper Pod. Image and
/// pull-policy are passed in (resolved from the live coordinator pod by
/// the caller) rather than re-derived here, so this manifest always
/// matches the deployed env exactly.
fn render_inspect_pod_manifest(namespace: &str, image: &str, pull_policy: &str) -> String {
    // Resources: the pod sleeps idle 99% of the time and only spawns a
    // short-lived JVM (-Xmx256m) on each inspect call to issue a handful
    // of AdminClient RPCs. Requests are sized for the idle baseline; the
    // limit gives the JVM enough headroom for startup + heap + Kafka client
    // overhead. CPU is bursty during describes and zero otherwise.
    format!(
        r#"apiVersion: v1
kind: Pod
metadata:
  name: {pod}
  namespace: {ns}
  labels:
    app: {pod}
spec:
  restartPolicy: Always
  containers:
    - name: inspect
      image: {image}
      imagePullPolicy: {pull_policy}
      command: ["sleep", "infinity"]
      resources:
        requests:
          cpu: 25m
          memory: 64Mi
        limits:
          cpu: 500m
          memory: 384Mi
"#,
        pod = INSPECT_POD,
        ns = namespace,
        image = image,
        pull_policy = pull_policy,
    )
}

/// Serialize a connection-properties map as a Java `Properties`-format
/// payload (one `key=value\n` line per entry). NOT the YAML
/// `key: "value"` shape that `ConnectionConfig::to_common_config_lines`
/// produces — that one is for `commonConfig` injection in workload YAMLs
/// and is parsed by SnakeYAML, not `Properties.load`.
fn serialize_properties(props: &BTreeMap<String, String>) -> String {
    let mut out = String::new();
    for (key, value) in props {
        out.push_str(key);
        out.push('=');
        out.push_str(value);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_properties_writes_key_equals_value_per_line() {
        let mut props = BTreeMap::new();
        props.insert("bootstrap.servers".to_string(), "broker:9092".to_string());
        props.insert("security.protocol".to_string(), "SASL_SSL".to_string());
        let s = serialize_properties(&props);
        // BTreeMap iterates in sorted key order, so output is deterministic.
        assert_eq!(
            s,
            "bootstrap.servers=broker:9092\nsecurity.protocol=SASL_SSL\n"
        );
    }

    #[test]
    fn serialize_properties_empty_returns_empty_string() {
        let props = BTreeMap::new();
        assert_eq!(serialize_properties(&props), "");
    }

    #[test]
    fn serialize_properties_preserves_special_chars_in_value() {
        // Java Properties.load supports : as separator and treats # as comment
        // start, but those only matter at the START of a line / as a key
        // delimiter. Values can contain them freely. JAAS configs frequently
        // contain colons, semicolons, and equals signs — round-trip them as-is.
        let mut props = BTreeMap::new();
        props.insert(
            "sasl.jaas.config".to_string(),
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"p\";".to_string(),
        );
        let s = serialize_properties(&props);
        assert_eq!(
            s,
            "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"p\";\n"
        );
    }

    #[test]
    fn render_inspect_pod_manifest_emits_passed_image_and_pull_policy() {
        // The function is now a pure renderer — image and pull-policy come
        // from the live coordinator pod, so the test just confirms they
        // land in the right YAML fields and the static bits (name, label,
        // command, namespace) are correct.
        let yaml = render_inspect_pod_manifest("ns1", "dimster:latest-kafka4.2.0", "IfNotPresent");
        assert!(
            yaml.contains("image: dimster:latest-kafka4.2.0"),
            "manifest:\n{yaml}"
        );
        assert!(
            yaml.contains("imagePullPolicy: IfNotPresent"),
            "manifest:\n{yaml}"
        );
        assert!(yaml.contains("name: dimster-inspect"), "manifest:\n{yaml}");
        assert!(yaml.contains("namespace: ns1"), "manifest:\n{yaml}");
        assert!(yaml.contains("app: dimster-inspect"), "manifest:\n{yaml}");
        assert!(
            yaml.contains(r#"command: ["sleep", "infinity"]"#),
            "manifest:\n{yaml}"
        );
    }

    #[test]
    fn render_inspect_pod_manifest_passes_through_registry_image_with_always() {
        let yaml = render_inspect_pod_manifest(
            "dimster",
            "123.dkr.ecr.us-west-2.amazonaws.com/dimster:latest-kafka4.2.0",
            "Always",
        );
        assert!(
            yaml.contains("image: 123.dkr.ecr.us-west-2.amazonaws.com/dimster:latest-kafka4.2.0"),
            "manifest:\n{yaml}"
        );
        assert!(
            yaml.contains("imagePullPolicy: Always"),
            "manifest:\n{yaml}"
        );
    }

    // ----------------------------------------------------------------------
    // InspectFilters / build_inspect_argv
    // ----------------------------------------------------------------------

    #[test]
    fn build_inspect_argv_empty_when_no_flags_set() {
        // No flags = show everything = no flag tail. Matches the Java side's
        // no-flag default (Filters.noSectionFlagsSet() => render all).
        let argv = build_inspect_argv(&InspectFilters::default());
        assert!(argv.is_empty(), "expected no flags, got {:?}", argv);
    }

    #[test]
    fn build_inspect_argv_emits_dimster_only() {
        let argv = build_inspect_argv(&InspectFilters {
            dimster_only: true,
            ..Default::default()
        });
        assert_eq!(argv, vec!["--dimster-only"]);
    }

    #[test]
    fn build_inspect_argv_emits_topics() {
        let argv = build_inspect_argv(&InspectFilters {
            topics: true,
            ..Default::default()
        });
        assert_eq!(argv, vec!["--topics"]);
    }

    #[test]
    fn build_inspect_argv_emits_group_offsets() {
        let argv = build_inspect_argv(&InspectFilters {
            group_offsets: true,
            ..Default::default()
        });
        assert_eq!(argv, vec!["--group-offsets"]);
    }

    #[test]
    fn build_inspect_argv_emits_group_members() {
        let argv = build_inspect_argv(&InspectFilters {
            group_members: true,
            ..Default::default()
        });
        assert_eq!(argv, vec!["--group-members"]);
    }

    #[test]
    fn build_inspect_argv_emits_metadata_quorum() {
        let argv = build_inspect_argv(&InspectFilters {
            metadata_quorum: true,
            ..Default::default()
        });
        assert_eq!(argv, vec!["--metadata-quorum"]);
    }

    #[test]
    fn build_inspect_argv_emits_all_flags_in_stable_order() {
        // Stable order matters so test diffs / shell history stay readable.
        let argv = build_inspect_argv(&InspectFilters {
            dimster_only: true,
            topics: true,
            group_offsets: true,
            group_members: true,
            metadata_quorum: true,
        });
        assert_eq!(
            argv,
            vec![
                "--dimster-only",
                "--topics",
                "--group-offsets",
                "--group-members",
                "--metadata-quorum",
            ]
        );
    }

    #[test]
    fn build_inspect_argv_combines_subset_in_stable_order() {
        // Non-empty proper subset — confirms we don't accidentally key on
        // "all flags set" vs "some flags set" anywhere.
        let argv = build_inspect_argv(&InspectFilters {
            dimster_only: true,
            topics: false,
            group_offsets: true,
            group_members: false,
            metadata_quorum: true,
        });
        assert_eq!(
            argv,
            vec!["--dimster-only", "--group-offsets", "--metadata-quorum"]
        );
    }

    #[test]
    fn inspect_filters_any_false_when_default() {
        assert!(!InspectFilters::default().any());
    }

    #[test]
    fn inspect_filters_any_true_when_one_flag_set() {
        assert!(
            InspectFilters {
                topics: true,
                ..Default::default()
            }
            .any()
        );
        assert!(
            InspectFilters {
                dimster_only: true,
                ..Default::default()
            }
            .any()
        );
        assert!(
            InspectFilters {
                group_offsets: true,
                ..Default::default()
            }
            .any()
        );
        assert!(
            InspectFilters {
                group_members: true,
                ..Default::default()
            }
            .any()
        );
        assert!(
            InspectFilters {
                metadata_quorum: true,
                ..Default::default()
            }
            .any()
        );
    }
}
