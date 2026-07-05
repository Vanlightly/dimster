// Implements the `dimster build <env>` command.
//
// Two build modes are supported:
//
//   * Host-build (default): Maven runs on the host via `./mvnw`,
//     producing package/target/dimster-*-bin.tar.gz; a trivial
//     docker/Dockerfile.package then unpacks that tarball into the
//     runtime image. Faster on Apple Silicon (no Rosetta JIT crashes
//     when Colima is amd64) and gives the same incremental Maven cache
//     reuse as a normal IDE workflow.
//
//   * In-Docker build (`--in-docker`): the multi-stage docker/Dockerfile
//     runs `mvnw clean install` inside the builder container, so the
//     host needs only Docker — no JVM, no Maven. Useful on machines
//     without a JDK and for fully reproducible CI builds.
//
// Both modes share the same surrounding flow:
//   1. Ensure `dimster-base:latest` exists locally and matches the
//      Docker daemon's architecture (rebuild from docker/Dockerfile.base
//      if not). dimster-base is the runtime base of both Dockerfiles
//      and the builder base of the multi-stage one.
//   2. Build the application image (mode-specific).
//   3. For each requested target environment, either tag + push to its
//      registry (image name contains a dot), or — for a local image —
//      load it into the env's local Kubernetes distribution. The loader
//      command is dispatched per-distro so Dimster works on minikube,
//      k3d, kind, and Docker Desktop / OrbStack k8s without minikube
//      being a hard dependency.

use std::path::Path;
use std::process::Command;

use anyhow::{Result, bail};

use crate::commands::is_registry_image;
use crate::config::{DimsterConfig, Distro};

pub fn run(
    config: &DimsterConfig,
    targets: Vec<String>,
    script_dir: &Path,
    in_docker: bool,
) -> Result<()> {
    let image_tag = &config.image.tag;

    ensure_dimster_base(script_dir)?;

    // Local image name is the canonical name we build against. Per-target
    // images are derived from `<local_image>:<image_tag>-kafka<version>` via
    // `docker tag` further down.
    let local_image = config
        .environments
        .get("local")
        .and_then(|e| e.image.as_deref())
        .unwrap_or("dimster");

    // Resolve every target's (flavor, version) pair. An env without
    // `kafka_client_version` set falls back to DEFAULT_KAFKA_VERSION via
    // Environment::kafka_flavor(). Validating up-front means typos in the
    // version string surface immediately, before any Maven or Docker work.
    struct Target<'a> {
        name: &'a str,
        env: Option<&'a crate::config::Environment>,
        flavor: &'static str,
        version: String,
    }
    let mut resolved: Vec<Target> = Vec::with_capacity(targets.len());
    for t in &targets {
        let env_config = config.environments.get(t.as_str());
        let (flavor, version) = match env_config {
            Some(e) => e.kafka_flavor()?,
            None => {
                crate::config::parse_kafka_client_version(crate::config::DEFAULT_KAFKA_VERSION)?
            }
        };
        resolved.push(Target {
            name: t.as_str(),
            env: env_config,
            flavor,
            version: version.to_string(),
        });
    }

    // Dedupe (flavor, version) pairs so we don't re-build the same image
    // when multiple targets ask for the same Kafka client. Keeps the build
    // list short for the common case (one campaign at one version) without
    // forcing the caller to think about it.
    let mut needed: Vec<(&'static str, String)> = Vec::new();
    for r in &resolved {
        let pair = (r.flavor, r.version.clone());
        if !needed.contains(&pair) {
            needed.push(pair);
        }
    }

    // Build one image per unique (flavor, version) pair, then alias it
    // under every tag computed by `all_tags`. The first entry of that list
    // is always the CLI-version tag, which serves as the canonical build
    // target — subsequent entries are added via `docker tag`. The version
    // (not the flavor) goes into every tag so two envs at different 3.x
    // patches don't overwrite each other's image.
    for (flavor, version) in &needed {
        let local_tags = all_tags(local_image, image_tag, version);
        // local_tags[0] is the CLI-version tag — always present, always
        // the canonical build name.
        let base_image = &local_tags[0];
        if in_docker {
            build_image_in_docker(base_image, flavor, version, script_dir)?;
        } else {
            run_host_maven_for(flavor, version, script_dir)?;
            package_image_from_host_build(base_image, flavor, script_dir)?;
        }
        for alias in local_tags.iter().skip(1) {
            docker_tag(base_image, alias)?;
        }
    }

    // For each target, propagate every alias to the destination (push to
    // registry, or load into the local k8s distro) so deployments
    // referencing any of the tags resolve end-to-end. `all_tags` already
    // deduplicates, so pushing/loading happens at most once per tag.
    for r in &resolved {
        let repo = r.env.and_then(|e| e.image.as_deref()).unwrap_or("dimster");
        let local_tags = all_tags(local_image, image_tag, &r.version);
        let repo_tags = all_tags(repo, image_tag, &r.version);

        if is_registry_image(repo) {
            for (local, remote) in local_tags.iter().zip(repo_tags.iter()) {
                if local != remote {
                    docker_tag(local, remote)?;
                }
                println!("==> Pushing {}...", remote);
                docker_push(remote)?;
            }
            // r.flavor is intentionally not surfaced here - it's an internal
            // mapping detail. The runtime SPI binding log inside the pod
            // (KafkaSpi.factory()) reports the bound factory class for
            // diagnostics. Report the CLI-version tag as the canonical
            // identity of what was pushed; the other aliases share the
            // same image ID.
            println!("==> Pushed: {} (kafka-clients {})", repo_tags[0], r.version);
        } else {
            // Local image — load every alias into the target env's local
            // k8s distro. Defaults to Minikube for envs that don't set
            // `distro:` so pre-existing configs continue to work unchanged.
            let distro = r.env.map(|e| e.distro()).unwrap_or(Distro::Minikube);
            let context = r.env.and_then(|e| e.context.as_deref());
            for tag in &local_tags {
                load_image_into_distro(distro, tag, context)?;
            }
            println!(
                "==> Built: {} (kafka-clients {}) for env {}",
                local_tags[0], r.version, r.name
            );
        }
    }

    println!("==> Build complete.");
    Ok(())
}

/// Compute every tag a freshly-built image should carry, for a given
/// `(repo, kafka_version)` pair. Tags are returned in stable order with
/// duplicates removed, and the first element is always the CLI-version
/// tag (so callers can use `[0]` as the canonical name to build under).
///
/// The list always contains:
///   * `<repo>:<cli-version>-kafka<kver>` — a stable pin matching the
///     Dimster CLI binary that produced the image. Useful for archival
///     and for users who want to deploy a specific Dimster version.
///   * `<repo>:latest-kafka<kver>` — a moving alias to the freshest local
///     build. Lets configs pin `tag: latest` without needing edits after
///     every rebuild.
///
/// It also contains `<repo>:<image_tag>-kafka<kver>` from the user's
/// `dimster-config.yaml` when that tag differs from both of the above
/// (e.g. a custom tag used by a registry pipeline). When `image_tag`
/// equals the CLI version or `"latest"`, the user-configured value is
/// already covered by one of the always-on tags and is not duplicated.
fn all_tags(repo: &str, image_tag: &str, kafka_version: &str) -> Vec<String> {
    let cli_version = env!("CARGO_PKG_VERSION");
    let mut tags: Vec<String> = Vec::with_capacity(3);
    let mut push_unique = |prefix: &str| {
        let tag = format!("{}:{}-kafka{}", repo, prefix, kafka_version);
        if !tags.contains(&tag) {
            tags.push(tag);
        }
    };
    push_unique(cli_version);
    push_unique("latest");
    push_unique(image_tag);
    tags
}

/// Apply an additional tag to an existing local image. Wraps `docker tag`
/// with a uniform error message so the build loop and registry-push path
/// can call it without duplicating the spawn/status boilerplate.
fn docker_tag(source: &str, dest: &str) -> Result<()> {
    let status = Command::new("docker")
        .args(["tag", source, dest])
        .status()?;
    if !status.success() {
        bail!("Docker tag failed: {} -> {}", source, dest);
    }
    Ok(())
}

/// Push a single image:tag to its registry. Wraps `docker push` with a
/// uniform error message; the caller is responsible for echoing context
/// before/after.
fn docker_push(image: &str) -> Result<()> {
    let status = Command::new("docker").args(["push", image]).status()?;
    if !status.success() {
        bail!("Docker push failed: {}", image);
    }
    Ok(())
}

/// Ensure the dimster-base runtime image exists locally and matches the
/// Docker daemon's architecture. Rebuilds from docker/Dockerfile.base if
/// the image is missing or was built for a different architecture (e.g.
/// after switching Colima between arm64 and amd64).
fn ensure_dimster_base(script_dir: &Path) -> Result<()> {
    let needs_base_build = match base_image_arch("dimster-base:latest") {
        Some(image_arch) => {
            let daemon_arch = docker_daemon_arch().unwrap_or_default();
            if image_arch != daemon_arch {
                println!(
                    "==> dimster-base is {} but Docker daemon is {}, rebuilding...",
                    image_arch, daemon_arch
                );
                true
            } else {
                false
            }
        }
        // Image doesn't exist
        None => true,
    };

    if needs_base_build {
        println!("==> Building dimster-base image...");
        let docker_dir = script_dir.join("docker");
        let status = Command::new("docker")
            .args([
                "build",
                "-f",
                "Dockerfile.base",
                "-t",
                "dimster-base:latest",
                ".",
            ])
            .current_dir(&docker_dir)
            .status()?;
        if !status.success() {
            bail!("dimster-base image build failed");
        }
    } else {
        println!("==> dimster-base image already available locally, skipping build.");
    }

    Ok(())
}

/// In-Docker build path (`--in-docker`). Build context is the repo root
/// so the multi-stage docker/Dockerfile can COPY the Maven sources
/// directly. The builder stage runs `mvnw -pl package-<flavor> -am install
/// -Dkafka.version=<version>` inside the container, building ONLY the
/// requested flavor against the requested kafka-clients version; the
/// runtime stage then unpacks the produced tarball. Each (flavor, version)
/// invocation produces one image - layer caching across calls is bounded
/// by what changes between builds.
fn build_image_in_docker(
    base_image: &str,
    flavor: &str,
    version: &str,
    script_dir: &Path,
) -> Result<()> {
    println!(
        "==> Building Docker image {} (in-docker Maven build, flavor={}, kafka-clients={})...",
        base_image, flavor, version
    );
    let status = Command::new("docker")
        .args([
            "build",
            "-f",
            "docker/Dockerfile",
            "--build-arg",
            &format!("KAFKA_FLAVOR={}", flavor),
            "--build-arg",
            &format!("KAFKA_VERSION={}", version),
            "-t",
            base_image,
            ".",
        ])
        .current_dir(script_dir)
        .status()?;
    if !status.success() {
        bail!(
            "Docker build failed for flavor={} kafka-clients={}",
            flavor,
            version
        );
    }
    Ok(())
}

/// Host-build path (default), Maven step. Runs `./mvnw -pl
/// package-<flavor> -am install -Dkafka.version=<version>` on the host to
/// produce one per-flavor tarball at the requested kafka-clients version.
/// Requires a host JDK; the Maven flags mirror the in-container invocation
/// in docker/Dockerfile so behavior is identical between modes.
///
/// Why per-(flavor, version) instead of a single global `mvnw clean
/// install`: -Dkafka.version is a Maven CLI property that overrides the
/// pom-defined value for ALL modules in a single invocation. So building
/// multiple flavors in one go would force them all to the same
/// kafka-clients version, defeating the per-flavor design.
fn run_host_maven_for(flavor: &str, version: &str, script_dir: &Path) -> Result<()> {
    println!(
        "==> Running Maven build on host for flavor {} (kafka-clients {})...",
        flavor, version
    );
    let mvnw = script_dir.join("mvnw");
    let status = Command::new(&mvnw)
        .args([
            "-q",
            "-B",
            "-ntp",
            "-pl",
            &format!("package-{}", flavor),
            "-am",
            "clean",
            "install",
            "-Dlicense.skip=true",
            "-DskipTests",
            &format!("-Dkafka.version={}", version),
        ])
        .current_dir(script_dir)
        .status()?;
    if !status.success() {
        bail!(
            "Host Maven build failed for flavor={} kafka-clients={}. Either fix \
             the build, or retry with `--in-docker` to build inside a container instead.",
            flavor,
            version
        );
    }
    Ok(())
}

/// Host-build path (default), per-flavor packaging step. Builds a
/// minimal docker/Dockerfile.package image that just unpacks the matching
/// package-<flavor>/target/dimster-*-bin.tar.gz produced by
/// `run_host_maven_for`. Build context is the repo root so the COPY can
/// pick the right per-flavor tarball directly via the KAFKA_FLAVOR
/// build-arg. KAFKA_VERSION is not needed here - the host already baked
/// the right version into the tarball.
fn package_image_from_host_build(base_image: &str, flavor: &str, script_dir: &Path) -> Result<()> {
    println!(
        "==> Packaging Docker image {} (flavor {})...",
        base_image, flavor
    );
    let status = Command::new("docker")
        .args([
            "build",
            "-f",
            "docker/Dockerfile.package",
            "--build-arg",
            &format!("KAFKA_FLAVOR={}", flavor),
            "-t",
            base_image,
            ".",
        ])
        .current_dir(script_dir)
        .status()?;
    if !status.success() {
        bail!("Docker build failed for flavor={}", flavor);
    }
    Ok(())
}

/// Returns the architecture of a local Docker image (e.g. "amd64", "arm64"),
/// or None if the image doesn't exist.
fn base_image_arch(image: &str) -> Option<String> {
    let output = Command::new("docker")
        .args(["image", "inspect", "--format", "{{.Architecture}}", image])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Returns the architecture of the Docker daemon (e.g. "amd64", "arm64").
fn docker_daemon_arch() -> Option<String> {
    let output = Command::new("docker")
        .args(["info", "--format", "{{.Architecture}}"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

// ---------------------------------------------------------------------------
// Per-distro image loading
// ---------------------------------------------------------------------------
//
// Each local Kubernetes distribution exposes a different mechanism for
// making a host-built image available to the cluster:
//
//   minikube         → `minikube image load <image>`
//   k3d              → `k3d image import <image> -c <cluster>`
//   kind             → `kind load docker-image <image> --name <cluster>`
//   docker-desktop   → no-op (host docker daemon is shared with cluster)
//
// Failures are reported as warnings rather than errors. The build itself
// already succeeded; an image-load failure typically surfaces as
// ImagePullBackOff at deploy time, which the user can then diagnose with
// the warning context here as a starting point.

/// Load the freshly built image into the cluster matching `distro`.
fn load_image_into_distro(distro: Distro, image: &str, context: Option<&str>) -> Result<()> {
    match distro {
        Distro::Minikube => load_into_minikube(image),
        Distro::K3d => load_into_k3d(image, context),
        Distro::Kind => load_into_kind(image, context),
        Distro::DockerDesktop => {
            // Docker Desktop and OrbStack expose Kubernetes via the same
            // docker daemon used for `docker build`, so the freshly
            // built image is already available to the cluster.
            println!("==> Image ready (Docker Desktop / OrbStack shares the host daemon).");
        }
    }
    Ok(())
}

fn load_into_minikube(image: &str) {
    if !is_minikube_running() {
        eprintln!("    Warning: minikube isn't running — skipping image load.");
        return;
    }
    println!("==> Loading {} into minikube...", image);
    // Remove old image first (minikube caching issue)
    let _ = Command::new("minikube")
        .args(["ssh", &format!("docker rmi -f {} 2>/dev/null", image)])
        .output();
    match Command::new("minikube")
        .args(["image", "load", image])
        .status()
    {
        Ok(s) if !s.success() => eprintln!("    Warning: minikube image load failed"),
        Err(e) => eprintln!("    Warning: failed to invoke minikube ({e})"),
        _ => {}
    }
}

fn load_into_k3d(image: &str, context: Option<&str>) {
    let cluster = match derive_cluster_name(context, "k3d-") {
        Ok(c) => c,
        Err(e) => {
            eprintln!("    Warning: {e}");
            return;
        }
    };
    println!("==> Importing {image} into k3d cluster '{cluster}'...");
    match Command::new("k3d")
        .args(["image", "import", image, "-c", &cluster])
        .status()
    {
        Ok(s) if !s.success() => eprintln!("    Warning: k3d image import failed"),
        Err(e) => {
            eprintln!("    Warning: failed to invoke k3d ({e}). Is k3d installed?")
        }
        _ => {}
    }
}

fn load_into_kind(image: &str, context: Option<&str>) {
    let cluster = match derive_cluster_name(context, "kind-") {
        Ok(c) => c,
        Err(e) => {
            eprintln!("    Warning: {e}");
            return;
        }
    };
    println!("==> Loading {image} into kind cluster '{cluster}'...");
    match Command::new("kind")
        .args(["load", "docker-image", image, "--name", &cluster])
        .status()
    {
        Ok(s) if !s.success() => eprintln!("    Warning: kind load docker-image failed"),
        Err(e) => {
            eprintln!("    Warning: failed to invoke kind ({e}). Is kind installed?")
        }
        _ => {}
    }
}

/// Derive a cluster name from a kubectl context that follows the
/// `<distro>-<cluster>` convention used by k3d (`k3d-foo`) and kind
/// (`kind-foo`). Falls back to `kubectl config current-context` when the
/// env doesn't pin a context. Returns a clear error if the resolved
/// context doesn't carry the expected prefix — better to warn the user
/// up-front than to invoke the loader against the wrong cluster.
fn derive_cluster_name(context: Option<&str>, prefix: &str) -> Result<String> {
    let ctx = match context {
        Some(c) => c.to_string(),
        None => current_kubectl_context()?,
    };
    if let Some(rest) = ctx.strip_prefix(prefix) {
        if rest.is_empty() {
            bail!(
                "kubectl context '{ctx}' is just the prefix '{prefix}' with no cluster name. \
                 Set the context explicitly in your env config."
            );
        }
        Ok(rest.to_string())
    } else {
        bail!(
            "could not derive cluster name from kubectl context '{ctx}' — expected a context \
             starting with '{prefix}'. Set the context in your env config (e.g. \
             `context: {prefix}mycluster`) or run `kubectl config use-context {prefix}<name>`."
        )
    }
}

fn current_kubectl_context() -> Result<String> {
    let output = Command::new("kubectl")
        .args(["config", "current-context"])
        .output()?;
    if !output.status.success() {
        bail!("`kubectl config current-context` failed — is kubectl installed and configured?");
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn is_minikube_running() -> bool {
    Command::new("minikube")
        .args(["status"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_cluster_name_strips_k3d_prefix() {
        assert_eq!(
            derive_cluster_name(Some("k3d-mycluster"), "k3d-").unwrap(),
            "mycluster"
        );
    }

    #[test]
    fn derive_cluster_name_strips_kind_prefix() {
        assert_eq!(
            derive_cluster_name(Some("kind-test"), "kind-").unwrap(),
            "test"
        );
    }

    #[test]
    fn derive_cluster_name_rejects_missing_prefix() {
        let err = derive_cluster_name(Some("minikube"), "k3d-").unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("could not derive cluster name"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn derive_cluster_name_rejects_bare_prefix() {
        let err = derive_cluster_name(Some("k3d-"), "k3d-").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("just the prefix"), "unexpected error: {msg}");
    }

    // ---------------------------------------------------------------------
    // all_tags
    // ---------------------------------------------------------------------
    //
    // The constant in these assertions is the CLI's own version
    // (env!("CARGO_PKG_VERSION") from cli/Cargo.toml). When the CLI is
    // bumped via bin/bump-version, these tests are auto-aligned because
    // they read CARGO_PKG_VERSION the same way the production code does.
    //
    // We test the dedup logic by exercising every distinct configuration
    // of (image_tag relative to cli_version) that can occur.

    const CLI_VERSION: &str = env!("CARGO_PKG_VERSION");

    #[test]
    fn all_tags_always_emits_cli_version_and_latest() {
        // image_tag == cli_version is the default-config case (the user
        // has no `image:` block, so default_tag() returned the CLI
        // version). Expect exactly two tags: cli-version and latest.
        let tags = all_tags("dimster", CLI_VERSION, "4.2.0");
        assert_eq!(
            tags,
            vec![
                format!("dimster:{}-kafka4.2.0", CLI_VERSION),
                "dimster:latest-kafka4.2.0".to_string(),
            ]
        );
    }

    #[test]
    fn all_tags_dedupes_when_image_tag_is_latest() {
        // image_tag == "latest" (the user explicitly pinned to latest).
        // The custom-alias slot collapses into the always-on latest tag,
        // so we still get exactly two tags.
        let tags = all_tags("dimster", "latest", "4.2.0");
        assert_eq!(
            tags,
            vec![
                format!("dimster:{}-kafka4.2.0", CLI_VERSION),
                "dimster:latest-kafka4.2.0".to_string(),
            ]
        );
    }

    #[test]
    fn all_tags_adds_custom_when_image_tag_differs() {
        // image_tag is a custom string (e.g. a registry pipeline tag).
        // Expect three tags: cli-version, latest, AND the custom one.
        let tags = all_tags("dimster", "hotfix-123", "4.2.0");
        assert_eq!(
            tags,
            vec![
                format!("dimster:{}-kafka4.2.0", CLI_VERSION),
                "dimster:latest-kafka4.2.0".to_string(),
                "dimster:hotfix-123-kafka4.2.0".to_string(),
            ]
        );
    }

    #[test]
    fn all_tags_propagates_repo() {
        // Repo prefix is honored — used for registry-targeted aliases
        // (e.g. 123.dkr.ecr.../dimster) where the local image is
        // re-tagged for push.
        let tags = all_tags("ghcr.io/me/dimster", "latest", "4.1.0");
        assert_eq!(
            tags,
            vec![
                format!("ghcr.io/me/dimster:{}-kafka4.1.0", CLI_VERSION),
                "ghcr.io/me/dimster:latest-kafka4.1.0".to_string(),
            ]
        );
    }

    #[test]
    fn all_tags_first_is_canonical_build_target() {
        // Callers rely on `tags[0]` being the CLI-version tag (used as the
        // canonical `-t` argument to `docker build`). Locking that order
        // here makes accidental reordering loud.
        let tags = all_tags("dimster", "anything-at-all", "4.2.0");
        assert_eq!(tags[0], format!("dimster:{}-kafka4.2.0", CLI_VERSION));
    }
}
