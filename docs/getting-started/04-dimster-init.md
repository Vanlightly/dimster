# 04 — Run `dimster init` to initialize the config

Most Dimster commands target a specific **named environment** which includes the k8s distro, the context, sizing and other configurations. These environments are stored in the `dimster-config.yaml` in the project root. Rather than writing it by hand, let the CLI generate a starter file for you.

## Run `init`

If you have prepared a local k8s distro, then use the `init` command with the `--local-k8s-distro` argument:

```bash
./dimster init --local-k8s-distro k3d         # or: kind | docker-desktop | minikube
```
This pre-fills the active `local` env's `context:` and `distro:` fields with sensible defaults for the chosen distro. The two `exampleLocal*` commented examples are also pre-filled with the same distro values (including the `host.<distro>.internal` hostname in the external example's `bootstrap.servers`), so they're ready to uncomment and edit without first having to swap out minikube-specific values.

Else run `init` with no arguments (which still creates the default `local` environment, prepared for Minikube).

You'll see something like:

```
Template written to /.../dimster/dimster-config.yaml — added: local, exampleLocalFull, exampleLocalExt, exampleRemoteFull, exampleRemoteExt
Edit the file to set image registries, kubectl contexts, and resource sizes.
Uncomment an example block by removing the leading '#' from each of its lines.
```

## Tour the generated file

Open `dimster-config.yaml` in your editor. You'll find five sections.

### 1. The guidance header

A commented preamble that documents the most useful tips for filling out an environment — image-naming rules, where to find `kubectl` contexts, the "memory limit > heap" rule of thumb, and the conventional node labels for multi-node setups. Worth a quick read; safe to leave intact.

### 2. `kubernetes:` block
```yaml
kubernetes:
  namespace: dimster
```
Everything Dimster creates lives in this namespace. You almost never need to change it.

### 3. `image:` block
```yaml
image:
  tag: latest
```
Tag applied to the Docker image when `dimster build` produces one.

### 4. `environments:` — the interesting part

This is where you'll spend your time. The starter file contains:

| Env name             | State      | What it's for                                                                            |
|----------------------|------------|------------------------------------------------------------------------------------------|
| `local`              | **active** | Minimal env: chosen k8s distro + in-cluster Kafka, tiny defaults — works as-is.          |
| `exampleLocalFull`   | commented  | Template: chosen distro + Kafka deployed in-cluster, with every tunable spelled out.     |
| `exampleLocalExt`    | commented  | Template: chosen distro + external Kafka (Docker Compose on the host).                   |
| `exampleRemoteFull`  | commented  | Template: remote Kubernetes (EKS/GKE) + Kafka deployed in-cluster.                       |
| `exampleRemoteExt`   | commented  | Template: remote Kubernetes + external Kafka (Confluent Cloud, MSK …).                   |

The four `example*` envs are entirely commented out. To turn one into a real env, copy it, give it a unique name, and remove the leading `#` from each line — the inner `# fieldname:` lines that document optional knobs can stay commented until you need them.

### 5. `results:` block
```yaml
results:
  dir: ./results
```
Where copied result JSON files (and Grafana snapshot HTML, log archives, etc.) land on your laptop after a benchmark finishes.

## Why the active `local` env is so short

The `local` env is intentionally tiny. With `--local-k8s-distro minikube` (the default) it looks like:

```yaml
  local:
    image: dimster
    context: minikube
    distro: minikube       # Options: minikube | k3d | kind | docker-desktop
```

With a different `--local-k8s-distro` choice the `context:` and `distro:` values are pre-filled accordingly (e.g. `k3d-dimster` / `k3d`).

Three required fields, the rest filled in by built-in defaults baked into the deploy code. Quick tour:

- **`image: dimster`** — a plain image name (no dot) tells `dimster build` to load the image into your local cluster instead of pushing it to a registry.
- **`context:`** — the kubectl context to deploy into. Edit if your cluster is named differently from the default convention (`k3d-dimster`, `kind-dimster`, `docker-desktop`, `minikube`).
- **`distro:`** — selects the per-distro image-loader command (`minikube image load` / `k3d image import` / `kind load docker-image` / no-op for Docker Desktop & OrbStack). The four valid values are `minikube`, `k3d`, `kind`, `docker-desktop`. Page 03's "Configure your env" table tells you which to set.

The defaults that fill in the rest are sized for the smallest reasonable laptop:

| Component   | Replicas | CPU req | Memory req | JVM heap            | PVC  |
|-------------|----------|---------|------------|---------------------|------|
| Kafka       | 1        | 250m    | 512Mi      | -Xms128M -Xmx256M   | 1Gi  |
| Worker      | 1        | 250m    | 768Mi      | -Xms256M -Xmx512M   | n/a  |
| Coordinator | n/a      | 250m    | 768Mi      | -Xms256M -Xmx512M   | n/a  |

These defaults apply to any env that has no `kafka:`, `workers:`, or `coordinator:` sections — a "minimal" env. As soon as you add any of those sections (like the `localBeefy` env on page 10), you take over explicit sizing and the larger production defaults apply to any fields you leave unset.

## If something looks wrong

- `init` printed an error saying `dimster-config.yaml already exists` — `init` only creates new files; it never overwrites or merges. Delete or rename the existing file, then re-run.
- You want to switch the active `local` distro — delete `dimster-config.yaml` and re-run `init` with the new `--local-k8s-distro` value.

