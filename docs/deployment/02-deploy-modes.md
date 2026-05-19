# 02 — Full vs External deploy modes

Every Dimster env declares a `deploy:` field with one of two values. This is the single biggest decision you'll make about a deployment, so it gets its own page.

```yaml
environments:
  myEnv:
    deploy: full       # or: external
```

`full` is the default — omit the field and you get full mode.

## At a glance

| | `deploy: full` | `deploy: external` |
|---|---|---|
| Kafka StatefulSet | Yes — Dimster manages it | No — you point at someone else's Kafka |
| Workers + Coordinator | Yes | Yes |
| Monitoring (Prometheus/Grafana) | Yes | Yes (Kafka Broker dashboard will be empty) |
| `kafka:` env block | Required for sizing | Ignored — leave it out |
| `connection:` env block | Ignored | **Required** — at least `bootstrap.servers` |
| `dimster kafka` CLI command | Available | Not available |
| Kafka readiness check on `dimster run` | Yes | Skipped |
| Use case | Benchmarking your own Kafka cluster shape | Benchmarking a managed service or pre-existing cluster |

## Full mode

Dimster owns the entire Kafka lifecycle. Pick this when the *thing being benchmarked* is Kafka itself — different broker counts, different configs, different storage classes, different broker versions.

Minimal `full` env (no kafka/workers/coordinator sections, so deploy fills in tiny defaults automatically):

```yaml
environments:
  local:
    image: dimster
    context: minikube
    distro: minikube
    # deploy: full is the default
```

A real `full` env, with explicit sizing for a remote cluster:

```yaml
environments:
  myEks:
    image: 123456789.dkr.ecr.us-west-2.amazonaws.com/dimster
    context: arn:aws:eks:us-west-2:123456789:cluster/my-cluster
    deploy: full
    system: "Apache Kafka 4.2"
    hardware: "kafka=3xm6i.2xlarge, workers=2xm6i.xlarge, storage=ebs/500Gi/3000iops/300MBs"
    kafka:
      version: "4.2.0"
      replicas: 3
      pvcSize: "500Gi"
      pvcStorageClass: gp3-fast
      heap: "-Xms8G -Xmx8G"
      requests:
        cpu: "6"
        memory: "24Gi"
      brokerConfig: run/broker-configs/default.yaml
      nodeLabel: role=kafka
    workers:
      replicas: 2
      heap: "-Xms2G -Xmx10G"
      requests:
        cpu: "4"
        memory: "12Gi"
      nodeLabel: role=worker
    coordinator:
      heap: "-Xms512M -Xmx1G"
      requests:
        cpu: "500m"
        memory: "2Gi"
      nodeLabel: role=infra
    monitoring:
      nodeLabel: role=infra
```

This is the `exampleRemoteFull` template from `dimster init`, lightly edited. The `kafka.*` block carries everything Dimster needs to render the Kafka StatefulSet:

| Field | What it controls |
|-------|------------------|
| `version` | Apache Kafka image tag (combined with `image:` if you want a non-`apache/kafka` registry) |
| `replicas` | Number of brokers in the StatefulSet |
| `pvcSize` | Per-broker PVC size (each broker gets its own PVC) |
| `pvcStorageClass` | StorageClass name applied to the PVC template (see page 03) |
| `heap` | Kafka JVM heap (`-Xms`/`-Xmx`) |
| `requests.cpu` / `requests.memory` | Kubernetes resource requests on the broker container |
| `brokerConfig` | Path to a YAML file of `server.properties` overrides (run/broker-configs/) |
| `extraConfig` | Inline overrides applied on top of `brokerConfig` |
| `logConfig` / `logMaxFileSize` / `logMaxBackupIndex` | log4j tuning |
| `nodeLabel` | Pin broker pods to nodes carrying this label (see page 03) |

`workers`, `coordinator`, and `monitoring` follow the same shape: `replicas` (where applicable), `heap`, `requests.{cpu,memory}`, and `nodeLabel`.

## External mode

Dimster deploys workers + coordinator + monitoring, points them at an external bootstrap server, and lets you benchmark something it doesn't manage. Pick this when the *thing being benchmarked* is a managed Kafka service (Confluent Cloud, MSK, Redpanda Cloud) or an existing cluster maintained by another team.

Minimal `external` env:

```yaml
environments:
  myCC:
    image: dimster
    context: minikube
    deploy: external
    system: "Confluent Cloud"
    connection:
      bootstrap.servers: "pkc-abc123.us-west-2.aws.confluent.cloud:9092"
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: 'org.apache.kafka.common.security.plain.PlainLoginModule required username="API_KEY" password="API_SECRET";'
    workers:
      replicas: 2
      heap: "-Xms2G -Xmx4G"
      requests:
        cpu: "2"
        memory: "6Gi"
    coordinator:
      heap: "-Xms512M -Xmx1G"
      requests:
        cpu: "500m"
        memory: "2Gi"
```

There is no `kafka:` block. The `connection:` block carries everything the workers need:

- `bootstrap.servers` — required. Comma-separated list, the standard Kafka client property.
- Every other key under `connection:` is passed straight through to the Kafka client `commonConfig`. You add `security.protocol`, `sasl.*`, TLS settings, ACL credentials — anything a Kafka client understands.

This separation keeps your **kafka-config** and **workload** YAML files environment-agnostic — they don't need to know whether they're running against a local single-broker cluster or Confluent Cloud. The connection details live in the env, not in the workload.

### Validation

`deploy: external` without a `connection:` block is a hard error at CLI startup:

```
Environment 'myCC' has deploy: external but no connection section.
External mode requires a connection with at least bootstrap.servers.
```

That check lives in `cli/src/config.rs` (the `Environment::validate` function) and runs before any `kubectl` call, so you find out immediately rather than in a half-deployed state.

## Choosing between them

Quick decision tree:

- *Am I tuning Kafka itself? (broker config, broker version, broker hardware, storage class)* → **full**.
- *Am I benchmarking a managed service or an existing Kafka I don't control?* → **external**.
- *Am I doing local smoke tests?* → **full** with a minimal env (e.g. `local`).
- *Do I want client-side numbers (producer/consumer behaviour, latency under load) without standing up Kafka pods?* → **external** is fine even against a local Docker Compose Kafka — see page 06.

## What's next

- [03 — Scheduling and storage](03-scheduling-and-storage.md): the two fields (`nodeLabel`, `pvcStorageClass`) that decide whether a `full`-mode benchmark is a real benchmark or just a load test that happens to involve Kafka.
- [06 — External Kafka](06-external-kafka.md): per-target recipes (Confluent Cloud, self-managed, Docker Compose) for `external` mode.
