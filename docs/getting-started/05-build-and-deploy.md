# 05 — Build and deploy

Two commands stand between you and a running benchmark cluster: 
- `dimster build <env>` packages the Java engine into a Docker image; `dimster deploy
<env>` puts everything on Kubernetes.
- `dimster deploy <env>` deploys the benchmark workers/coordinator, monitoring and Kafka when doing a full deployment.

## Build the engine image

`./dimster build <env>` builds the benchmark image and loads it into the k8s cluster of the named-environment. The following does so in the local k8s deployment.

```bash
./dimster build local
```

The image bundles the `kafka-clients` version named by the env's `kafka_client_version` (defaults to `4.2.0`). Re-run `dimster build <env>` whenever you change that field. See [Kafka Versioning](../concepts/kafka-versioning.md) for the supported values.

For a minikube deployment with the default `kafka_client_version`, the expected tail of the output:

```
Successfully tagged dimster:latest-kafka4.2.0
==> Loading image into minikube...
==> Built: dimster:latest-kafka4.2.0 (kafka-clients 4.2.0) for env local
==> Build complete.
```

## Deploy to Kubernetes

`./dimster deploy <env>` deploys the benchmark workers and coordinator, the monitoring and Kafka (for an env using full deploment). The following deploys all components to the local k8s deployment.

```bash
./dimster deploy local
    Using kubectl context: minikube
==> Deploying Dimster (local) to namespace dimster...
==> Waiting for Kafka to be ready...
==> Waiting for workers to be ready...
==> Waiting for coordinator to be ready...
==> Deployment complete.

Kafka brokers:
NAME      READY   STATUS    RESTARTS   AGE
kafka-0   1/1     Running   0          4h13m

Benchmark coordinator:
NAME                                     READY   STATUS    RESTARTS   AGE
benchmark-coordinator-69b5dbc948-j6dct   1/1     Running   0          12s

Benchmark workers:
NAME                                READY   STATUS    RESTARTS   AGE
benchmark-worker-79b565696f-ph4jc   1/1     Running   0          12s
```

The `local` env creates the following:

| Resource     | What it does                                                   |
|--------------|----------------------------------------------------------------|
| Kafka        | A 1-broker StatefulSet with a 1Gi PVC for the log dir.         |
| Workers      | A 1-replica Deployment that runs the producer/consumer engine. |
| Coordinator  | A Deployment that drives the benchmark and collects results.   |
| Prometheus   | Scrapes Kafka + worker metrics for live monitoring.            |
| Grafana      | Pre-provisioned dashboards over the Prometheus data.           |

## Inspect the cluster

> It's generally a good idea to specify the kubectl context to avoid impacting another k8s cluster if you have multiple. Use `kubectl config get-contexts` to list them and use the `--context` argument with all `kubectl` commands.

Confirm everything is up:

```bash
kubectl get pods -n dimster
```

You should see something like:

```
NAME                          READY   STATUS    RESTARTS   AGE
coordinator-7c8d9b66c-r2v4z   1/1     Running   0          90s
grafana-6f4d4b8c9-zsq8n       1/1     Running   0          90s
kafka-0                       1/1     Running   0          110s
prometheus-0                  1/1     Running   0          90s
worker-0                      1/1     Running   0          90s
```

Every pod should report `1/1` Ready. If anything is `Pending` or
`CrashLoopBackOff`, jump to the troubleshooting hint at the bottom of this
page.

## Sanity-check the workers

A built-in command pings every worker pod and reports back what the
producer/consumer threads are doing:

```bash
./dimster health local
    Using kubectl context: minikube
==> Discovering worker pods...
    Found 1 worker(s)

==> Worker: benchmark-worker-79b565696f-ph4jc
    producer-dimster-topic-xnNor8s-0000 (PRODUCER) — RUNNING
    kafka-consumer-dimster-topic-xnNor8s-0000-2ca-0 (CLASSIC_CONSUMER) — RUNNING
    kafka-consumer-dimster-topic-xnNor8s-0000-2ca-1 (CLASSIC_CONSUMER) — RUNNING
    kafka-consumer-dimster-topic-xnNor8s-0000-2ca-2 (CLASSIC_CONSUMER) — RUNNING
    kafka-consumer-dimster-topic-xnNor8s-0000-2ca-3 (CLASSIC_CONSUMER) — RUNNING
    kafka-consumer-dimster-topic-xnNor8s-0000-2ca-4 (CLASSIC_CONSUMER) — RUNNING
    kafka-consumer-dimster-topic-xnNor8s-0000-2ca-5 (CLASSIC_CONSUMER) — RUNNING
    kafka-consumer-dimster-topic-xnNor8s-0000-2ca-6 (CLASSIC_CONSUMER) — RUNNING
    kafka-consumer-dimster-topic-xnNor8s-0000-2ca-7 (CLASSIC_CONSUMER) — RUNNING
    kafka-consumer-dimster-topic-xnNor8s-0000-2ca-8 (CLASSIC_CONSUMER) — RUNNING
    kafka-consumer-dimster-topic-xnNor8s-0000-2ca-9 (CLASSIC_CONSUMER) — RUNNING
```

The check is most useful mid-benchmark to see whether anything has flipped to `FAILED`.

## Now what?

You now have a live Kafka cluster, a worker pod ready to drive load against
it, a coordinator ready to receive a command to orchestrate a benchmark, and a monitoring stack watching the whole thing. Next page: actually run a benchmark.

## If something looks wrong

`dimster deploy <env>` will print out any pod scheduling errors:
- A pod stuck in `Pending` with "Insufficient cpu/memory" — your local cluster is too small. Either give the docker engine more resources, or reduce the requests CPU and memory in the env (in the `dimster-config.yaml`). 
- A pod in `ImagePullBackOff` — the image didn't make it into the cluster. Most common cause: the env's `distro:` field doesn't match the cluster you actually started (e.g. `distro: minikube` set but you're running k3d). Or you have the wrong arch (local Apple Silicon but deploying to an x86_64 instance).
- Need to see what a pod is actually saying? `./dimster logs local worker -f` (or `coordinator` / `kafka`).