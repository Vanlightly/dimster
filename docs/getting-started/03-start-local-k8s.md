# 03 Start a local k8s cluster (optional)

Dimster benchmarks run in k8s, either locally (for toying around on your laptop) or a remote cluster for serious benchmarking. Either way, Dimster requires Docker locally for building Dimster images to upload to the k8s cluster.

Getting a local Dimster benchmarking lab up takes **two** distinct pieces:

1. **A Docker engine** — the container runtime Dimster's `build` command uses, and (for some distros) the runtime your Kubernetes cluster sits on top of.
2. **A Kubernetes distro** — the actual cluster you'll deploy into. You can skip this step if you have no plans to run any benchmarks locally.

These are independent choices. You can pair (almost) any Docker engine with any k8s distro; the one exception is **Docker Desktop k8s**, which only works on top of Docker Desktop's own daemon.

Dimster talks to whatever cluster you start the same way it would talk to a production EKS or GKE cluster — same `kubectl` commands, same manifests.

---

## Step 1 — Start a Docker engine

Whichever k8s distro you eventually pick, you need a running Docker daemon first. Dimster's `build` command always shells out to the local `docker` CLI, and three of the five k8s distros (k3d, kind, Docker Desktop k8s) embed their cluster inside Docker containers.

### macOS

You picked one of these on page 01:

- **Docker Desktop** — open the app. There's no separate command.
- **Colima** — start it from the terminal:
  ```bash
  colima start --cpu 4 --memory 8 --disk 30
  ```
- **OrbStack** — open the app. (OrbStack also ships its own k8s, see
  Step 2.)

The above sizing of 4 CPU / 8 GiB is more than plenty for Dimster's needs (it can run on only 2 GB of RAM with defaults).

### Linux

Docker Engine runs as a system service — you don't need to start anything per session. If `docker info` works, you're done with Step 1.

### Sanity-check

```bash
docker info | head -10        # should print server version + driver, no errors
```

---

## Step 2 — Start a Kubernetes distro

For messing around with Dimster on your laptop, pick **one** of the five distros below and follow its section. The image-load behavior on each is what makes the per-distro `distro:` field meaningful — you'll set that field at the bottom of this page.

| Distro              | Quick characterization                                         | Sits on top of...                  | Image-load command Dimster uses           |
|---------------------|----------------------------------------------------------------|------------------------------------|--------------------------------------------|
| **minikube**        | Single-node cluster in a VM. Mature, well-documented default.  | Any Docker daemon (or other driver)| `minikube image load`                      |
| **k3d**             | Lightweight k3s-in-Docker. Fast to start; small footprint.     | Any Docker daemon                  | `k3d image import -c <cluster>`            |
| **kind**            | Kubernetes-in-Docker. Common in CI; works fine on a laptop.    | Any Docker daemon                  | `kind load docker-image --name <cluster>`  |
| **Docker Desktop**  | Built-in cluster shipped with Docker Desktop.                  | Docker Desktop only                | (none — daemon is shared with cluster)     |
| **OrbStack**        | macOS-only k8s shipped with OrbStack.                          | OrbStack only                      | (none — daemon is shared with cluster)     |

Each section below covers install, start, and verify for one distro. **Do
only one** — then jump to "Configure your env's `distro:` field" at the
bottom of this page before continuing to page 05.

### Sizing rationale (applies to all distros)

Most Dimster commands target a specific **named environment** which includes the k8s distro, the context, sizing and other configurations. These environments are stored in the **dimster-config.yaml** which is covered in the next page. 

The default `local` env asks for roughly **750m CPU and 1.8 GiB of memory** across Kafka + a worker + the coordinator + the Prometheus/Grafana monitoring stack. As such will fit into the default minikube sizing. 

If pods later land in `Pending` with "Insufficient cpu/memory", bump the sizing — sometimes at the Docker layer (k3d/kind/Docker Desktop/OrbStack inherit limits from there) and sometimes at the cluster layer (minikube takes its own `--cpus`/`--memory` flags).

---

## Option A — minikube

**Install (macOS)**
```bash
brew install minikube
```

**Install (Linux)**
```bash
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube && rm minikube-linux-amd64
```

**Start (macOS — Colima or Docker Desktop)**

By default, Minikube starts with 2GB of memory which is enough for the default `local` env.

```bash
minikube start
```

For more resources, specify the CPU, memory and disk:

```bash
minikube start --cpus=4 --memory=6g --disk-size=20g
```

**Start (Linux)**
```bash
minikube start --driver=docker --cpus=4 --memory=6g --disk-size=20g
```

**Verify**
```bash
kubectl config current-context     # should print: minikube
kubectl get nodes                  # NAME minikube ... Ready
```

**Image-load behavior.** Minikube has its own internal image store separate from your host Docker daemon, so `dimster build local` runs `minikube image load dimster:latest` after each rebuild. After a `minikube stop && minikube start` the image survives. After `minikube delete` it doesn't — you'll need to re-run `dimster build local`.

---

## Option B — k3d

**Install (macOS)**
```bash
brew install k3d
```

**Install (Linux)**
```bash
curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
```

**Start**
```bash
k3d cluster create dimster \
  --servers 1 --agents 0 \
  --k3s-arg "--disable=traefik@server:0"
```

`k3d` doesn't take `--cpus` / `--memory` flags directly — it uses whatever your Docker daemon allows. On macOS with Colima, sizing is governed by the `colima start --cpu/--memory` numbers above; on Linux it's governed by host resources. The k3s server pod itself is light — 4 CPUs / 6 GiB at the Docker layer is plenty.

**Verify**
```bash
kubectl config current-context     # should print: k3d-dimster
kubectl get nodes                  # NAME k3d-dimster-server-0 ... Ready
```

**Image-load behavior.** `dimster build local` runs `k3d image import dimster:latest -c <cluster>` after each rebuild. The cluster name is derived from the kubectl context (`k3d-dimster` → cluster `dimster`); if you used a different cluster name when creating the cluster, set `context:` explicitly in `dimster-config.yaml` so the loader picks the right cluster.

---

## Option C — kind

**Install (macOS)**
```bash
brew install kind
```

**Install (Linux)**
```bash
[ "$(uname -m)" = aarch64 ] && arch=arm64 || arch=amd64
curl -Lo /tmp/kind https://kind.sigs.k8s.io/dl/latest/kind-linux-${arch}
sudo install /tmp/kind /usr/local/bin/kind && rm /tmp/kind
```

**Start**
```bash
kind create cluster --name dimster
```

Like k3d, kind uses Docker daemon resources rather than its own `--cpus`/`--memory` flags. Make sure Colima (or Docker Desktop) is sized for at least 4 CPUs / 6 GiB.

**Verify**
```bash
kubectl config current-context     # should print: kind-dimster
kubectl get nodes                  # NAME dimster-control-plane ... Ready
```

**Image-load behavior.** `dimster build local` runs `kind load docker-image dimster:latest --name <cluster>`. As with k3d, the cluster name is derived from the kubectl context (`kind-dimster` → `dimster`); set
`context:` explicitly in `dimster-config.yaml` if your cluster has a
different name.

---

## Option D — Docker Desktop k8s

**Install.** Docker Desktop ships Kubernetes built-in. Open Docker Desktop → Settings → Kubernetes → tick **Enable Kubernetes** → Apply & Restart. Wait until the indicator turns green.

There is no separate `start` command — the cluster runs as long as Docker Desktop is running.

**Sizing.** Docker Desktop → Settings → Resources → set **CPUs ≥ 4**, **Memory ≥ 6 GB**.

**Verify**
```bash
kubectl config current-context     # should print: docker-desktop
kubectl get nodes                  # NAME docker-desktop ... Ready
```

**Image-load behavior.** Docker Desktop Kubernetes shares the host's Docker daemon with the cluster, so `dimster build local` does **no explicit image load** — the freshly built image is already visible. `imagePullPolicy: IfNotPresent` (which Dimster sets) is what you want here.

---

## Option E — OrbStack k8s (macOS only)

**Install.** OrbStack ships its own Kubernetes. Open OrbStack → Kubernetes → toggle on. (If you don't have OrbStack: `brew install orbstack`.)

**Sizing.** OrbStack → Settings → Resources → CPUs ≥ 4, Memory ≥ 6 GB.

**Verify**
```bash
kubectl config current-context     # should print: orbstack
kubectl get nodes                  # NAME orbstack ... Ready
```

**Image-load behavior.** Same as Docker Desktop — OrbStack shares its Docker daemon with the cluster, so no explicit load step is needed. In `dimster-config.yaml` use `distro: docker-desktop` (it accepts `orbstack` as an alias too — both map to the same no-op loader).

---

## Configure your env's `distro:` field

The default `local` env in `dimster-config.yaml` looks like this:

```yaml
local:
  image: dimster
  context: minikube
  distro: minikube
```

If you ran `dimster init --local-k8s-distro <name>` on page 04, those fields are already set correctly and you can skip this section. Otherwise update **`distro:`** (and `context:`, if your kubectl context isn't `minikube`) to match the distro you just started:

| Distro you started   | Set `distro:` to | And `context:` to |
|----------------------|------------------|-------------------|
| minikube             | `minikube`       | `minikube`        |
| k3d                  | `k3d`            | `k3d-dimster`     |
| kind                 | `kind`           | `kind-dimster`    |
| Docker Desktop k8s   | `docker-desktop` | `docker-desktop`  |
| OrbStack k8s         | `orbstack`       | `orbstack`        |

`distro:` is what `dimster build` consults to decide which image-loader command to invoke. Get it wrong and the build still produces an image, but the cluster won't see it — pods will land in `ImagePullBackOff` at deploy time.

---

## Sanity-check the cluster

Whichever distro you picked, this should work:

```bash
kubectl config current-context
kubectl get nodes
```

If `current-context` shows something else (an old EKS cluster, perhaps), switch back with:

```bash
kubectl config use-context <your-context>     # e.g. minikube, k3d-dimster, ...
```

## Stop vs delete — know the difference

Between Dimster sessions you have two ways to free up laptop resources. The exact commands differ per distro but the choice is the same.

| Choice  | What it does                                                       | When to use |
|---------|--------------------------------------------------------------------|-------------|
| Stop    | Shuts the cluster down. State + loaded images survive. Resume in seconds. | Daily — frees CPU/memory between sessions. |
| Delete  | Destroys the cluster. All loaded images are gone.                  | Rare — only when you want a completely fresh start. |

Per-distro commands:

| Distro             | Stop                                | Delete                              |
|--------------------|-------------------------------------|-------------------------------------|
| minikube           | `minikube stop`                     | `minikube delete`                   |
| k3d                | `k3d cluster stop dimster`          | `k3d cluster delete dimster`        |
| kind               | (none — `docker stop` the node)     | `kind delete cluster --name dimster`|
| Docker Desktop k8s | quit Docker Desktop                 | Settings → Kubernetes → Reset       |
| OrbStack k8s       | quit OrbStack                       | Settings → Kubernetes → Reset       |

After a **delete**, you'll need to repeat `dimster build local` (page 05) because the image you loaded into the cluster was destroyed with it. After a **stop/start** cycle the image is still there, no rebuild needed.

## If something looks wrong

- Cluster start fails with "no available driver" or similar — Docker isn't running. On macOS run `colima start` (or open Docker Desktop / OrbStack). On Linux check `systemctl status docker`.
- `kubectl get nodes` hangs or returns "connection refused" — the cluster isn't actually up. Each distro has its own status command: `minikube status`, `k3d cluster list`, `kind get clusters`, or check Docker Desktop / OrbStack's UI.
- Pods later land in `ImagePullBackOff` — your `distro:` field probably doesn't match the cluster you actually started. Re-check the Configure-your-env section above and re-run `dimster build local`.
- Pods later land in `Pending` with "Insufficient cpu/memory" — bump the cluster sizing (or, for k3d/kind/Docker Desktop/OrbStack, the Docker daemon's resource limits).
