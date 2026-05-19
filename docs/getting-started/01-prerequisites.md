# 01 — Prerequisites

Dimster benchmarks run in k8s, either locally (for toying around on your laptop) or a remote cluster for serious benchmarking. Either way, Dimster requires Docker locally for building Dimster images to upload to the k8s cluster.

Before you can run a benchmark you need a small set of tools installed:

| Tool              | Purpose                                                                      |
|-------------------|------------------------------------------------------------------------------|
| Rust              | Compiles the `dimster` CLI itself (one time)                                 |
| Docker            | Builds the Dimster image and is the runtime under your local k8s             |
| kubectl           | Talks to the Kubernetes API                                                  |
| Local k8s distro  | Optional, pick one: minikube, k3d, kind, Docker Desktop k8s, or OrbStack k8s |
| Python 3          | Generates HTML charts from result JSON                                       |

Each one has its own subsection below with install commands for **macOS** (Homebrew) and **Linux** (Debian/Ubuntu `apt`), plus a one-line check that confirms the install worked. Skip any tool you already have and just run the verify line to confirm the version is recent enough.

> **No JVM, no Maven required.** The Dimster engine is Java but it's
> compiled inside a Docker image — the host needs only Docker. If you want
> to **modify the Java engine itself** (not just run benchmarks), you do
> need a JDK on the host; see [development.md](../development.md).

> **The CLI is the only thing you build from source as an end user.** The
> Java engine is shipped as a Docker image built by `dimster build`.
> Everything else on this list is a runtime dependency.

---

## Rust

Used once to compile the `dimster` CLI binary. After that you don't need to touch it unless you upgrade the CLI.

**macOS**
```bash
brew install rustup-init
rustup-init    # accept the defaults
```

**Linux**
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# follow the prompts, then:
source "$HOME/.cargo/env"
```

**Verify**
```bash
cargo --version
# cargo 1.x.y ...
```

---

## Docker

Container runtime. Used by `dimster build` to compile and package the Java engine, and by your local k8s distro as its container backend. **macOS — Docker Desktop** (the simplest path if your machine is licensed for it — Docker Desktop is free for personal use but requires a paid subscription for commercial use at companies above the free-tier thresholds):

```bash
brew install --cask docker
open -a Docker
```

**macOS — Colima** (use this if Docker Desktop's licensing isn't an option for you, e.g. on a work laptop without a Docker subscription). Colima is a free, open-source Docker-API-compatible daemon that drops in for the Docker Desktop daemon:

```bash
brew install colima docker
colima start
```

> If you go the Colima route and also want to use the **Docker Desktop
> Kubernetes** distro on page 03, you can't — that distro is bundled with
> Docker Desktop itself. Pick a different local k8s distro (minikube,
> k3d, kind, or OrbStack) instead.

**Linux**
```bash
# Docker's official convenience script (or use your distro's docker.io package)
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker "$USER"
newgrp docker
```

**Verify**
```bash
docker info
# should print server version + driver info, no errors
```

---

## kubectl

Command-line client for Kubernetes.

**macOS**
```bash
brew install kubectl
```

**Linux**
```bash
sudo apt install -y apt-transport-https ca-certificates curl
curl -fsSLo /usr/share/keyrings/kubernetes-archive-keyring.gpg \
  https://packages.cloud.google.com/apt/doc/apt-key.gpg
echo "deb [signed-by=/usr/share/keyrings/kubernetes-archive-keyring.gpg] https://apt.kubernetes.io/ kubernetes-xenial main" \
  | sudo tee /etc/apt/sources.list.d/kubernetes.list
sudo apt update && sudo apt install -y kubectl
```

**Verify**
```bash
kubectl version --client
# Client Version: vX.Y.Z
```

---

## A local Kubernetes distribution

For running benchmarks locally (not recommended for real benchmarking), Dimster needs a Kubernetes cluster to deploy into. On a laptop that means picking one of:

- **minikube** — single-node cluster in a VM. Mature, well-documented, the default in these docs.
- **k3d** — lightweight k3s-in-Docker. Fast to start; small footprint.
- **kind** — Kubernetes-in-Docker. Common in CI; works fine on a laptop too.
- **Docker Desktop k8s** — the cluster that ships with Docker Desktop.
- **OrbStack k8s** — the cluster shipped by OrbStack (macOS only).

**You only need to install one.** Page [03 — Start a local Kubernetes (optional)](03-start-local-k8s.md) covers installing and starting whichever you pick, including the resource-sizing guidance for Dimster. Any of the five works equally well — pick the one you're most comfortable with.

> **Already running Docker Desktop or OrbStack?** Their built-in
> Kubernetes is the path of least resistance — turn it on in the app's
> settings and skip the install step entirely.

---

## Python 3

Used by the chart generator. The macOS system Python is fine; on Linux you might need to install it explicitly.

**macOS**
```bash
# Already present on recent macOS. If not:
brew install python
```

**Linux**
```bash
sudo apt install python3 python3-venv python3-pip
```

**Verify**
```bash
python3 --version
# Python 3.x.y
```

---

## Verify everything

Run all the version checks together — if every line prints something sensible, you're ready to move on:

```bash
cargo --version && \
docker info | head -5 && \
kubectl version --client && \
python3 --version
```

(You'll verify your chosen k8s distro on page 03.)

> **Apple Silicon note.** macOS will refuse to run an unsigned `dimster`
> binary the first time you build it. The fix (`codesign --force --sign -
> ./dimster`) is in [02 — Compile the CLI](02-compile-the-cli.md).

## If something looks wrong

- A `command not found` after install on macOS usually means your shell doesn't have Homebrew on `PATH`. `brew --prefix` will tell you the path to add.
- `docker info` errors on macOS almost always mean Colima (or Docker Desktop) hasn't been started. Run `colima start` and try again.
