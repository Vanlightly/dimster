# 07 — Teardown

Cleaning up has two layers: removing **Dimster** from your k8s cluster, and deciding what to do with the **k8s cluster** itself.

## Tear down Dimster

```bash
./dimster teardown local
```

This removes everything Dimster created in the `dimster` namespace — Kafka StatefulSet, worker Deployment, coordinator, Prometheus, Grafana, the PVCs. The k8s cluster itself stays up.

When the command returns, confirm:

```bash
kubectl get pods -n dimster
# No resources found in dimster namespace.
```

## Decide what to do with the cluster (local)

Two choices, depending on how soon you'll come back. The exact commands differ per distro.

For local clusters, shutting down the cluster frees CPU/memory on your laptop. The Docker image you loaded with `dimster build` is preserved.

| Distro             | Stop                          | Resume                       |
|--------------------|-------------------------------|------------------------------|
| minikube           | `minikube stop`               | `minikube start`             |
| k3d                | `k3d cluster stop dimster`    | `k3d cluster start dimster`  |
| kind               | `docker stop <node>`          | `docker start <node>`        |
| Docker Desktop k8s | quit Docker Desktop           | open Docker Desktop          |
| OrbStack k8s       | quit OrbStack                 | open OrbStack                |

For minikube/k3d, no flags are needed on resume — they remember the sizing from your last `start`.

You can also wipes the cluster entirely. Use this when:

- You want to test the install path from scratch.
- You're upgrading the distro or Kubernetes version.
- Something is wedged in a way you can't be bothered to debug.

| Distro             | Delete                                |
|--------------------|---------------------------------------|
| minikube           | `minikube delete`                     |
| k3d                | `k3d cluster delete dimster`          |
| kind               | `kind delete cluster --name dimster`  |
| Docker Desktop k8s | Settings → Kubernetes → Reset         |
| OrbStack k8s       | Settings → Kubernetes → Reset         |

After a delete, you'll need to re-run `./dimster build local` because the image you loaded was destroyed with the cluster.

## A note on results

The results are downloaded to your `./results/` directory which lives on your laptop, not in the cluster, so none of the above stop/delete touches it. Downloaded result files persist across teardowns, restarts, and full cluster deletions. 

## What's next

Pages 01–07 took you through the happy path with the `run` benchmark mode. The next three pages branch out:

- [Page 08 — Dimensional testing](08-dimensional-testing.md) introduces scenarios and test points using two minimal example workloads.
- [Page 09 — The four modes](09-the-four-modes.md) introduces `explore`, `drain-backlog`, and `correctness` alongside `run`.
- [Page 10 — A beefier local env](10-localBeefy-env.md) shows how to add a `localBeefy` env when your laptop has more headroom to give.

## If something looks wrong

- `teardown` prints errors about resources that "no longer exist" — usually harmless. Re-run it once and the namespace will be clean.
- A cluster `stop` hangs — the corresponding `delete` is always a valid escape hatch (you'll need to rebuild the image afterwards).
