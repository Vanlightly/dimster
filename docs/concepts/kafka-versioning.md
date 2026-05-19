# Kafka Versioning — Client and Broker

Two Kafka versions matter when you run a Dimster benchmark, and they're independent settings:

- **Client version** — which `kafka-clients` jar the workers and coordinator are built against. Set with `environments.<name>.kafka_client_version`.
- **Broker version** — which Apache Kafka image runs in the StatefulSet (only when `deploy: full`). Set with `environments.<name>.kafka.version`. The value becomes the tag on the `apache/kafka` Docker image, so it must match a tag that actually exists upstream — **the oldest published `apache/kafka` image is `3.7`**. To benchmark against an older broker you'd need to point `kafka.image` at a different image (e.g. a Bitnami or custom build) since no `apache/kafka:3.6` or earlier exists on Docker Hub.

Most of the time you want client and broker to match — benchmark Kafka 4.2.0 with a 4.2.0 client. They're separable so you can study cross-version behavior, run regression baselines, or stage a client upgrade independently of a broker upgrade.

```yaml
environments:
  myenv:
    deploy: full
    kafka_client_version: "4.2.0"   # <-- client (worker / coordinator) side
    kafka:
      version: "4.2.0"              # <-- broker (StatefulSet) side
      replicas: 3
      ...
```

## Supported `kafka_client_version` values

Dimster supports `kafka-clients` versions from 3.0.0 and up. The value of `kafka_client_version` set in the Dimster environment flows through to the Maven build as `-Dkafka.version=<version>`, which sets the Maven `kafka-clients` version before compiling the benchmark coordinator and workers.

`kafka_client_version` defaults to `"4.2.0"` if the field is omitted. If you write a version outside the supported set the CLI errors out at `dimster build` time with a clear message.

If a workload requests `consumerType: SHARE_GROUP` against a build whose `kafka_client_version` doesn't support share groups, the benchmark fails immediately with a clear `"Share groups are not supported"` message.

## Workflow for switching client version

Changing `kafka_client_version` means re-running `dimster build <env>` so the new image lands in the cluster, then `dimster deploy <env>` so the worker and coordinator pods pick it up:

```bash
# 1. Edit dimster-config.yaml — set environments.<env>.kafka_client_version
# 2. Re-build (produces and loads the new image)
./dimster build <env>
# 3. Re-deploy (pins the workers/coordinator at the new image)
./dimster deploy <env>
```

The broker StatefulSet is untouched by changes to `kafka_client_version` — the broker keeps running whatever `kafka.version` says. To swap broker versions too, change `kafka.version` and re-deploy the `kafka` component.

You can mix and match to a certain extent the Kafka clients version and the Kafka broker version. But you may experience errors if they combination is incompatible.

## Verifying the bound version

Each Dimster pod logs the bound `kafka-clients` version at startup:

```
INFO: Kafka client factory bound: ... (kafka-clients version: 4.2.0, share groups: true)
INFO: Starting worker on port 8080
```

Use this to sanity-check that a freshly deployed pod is running the version you set in the config:

```bash
kubectl logs -n <namespace> -l app=benchmark-worker | grep "Kafka client"
```

## Enabling share groups on a 4.0/4.1 broker

Share groups are GA in 4.2 but **early-access** in 4.0/4.1, and need two opt-ins on the broker side. The first lives in the broker config — point your env's `kafka.brokerConfig` at `run/broker-configs/default-with-share.yaml`, which adds `group.coordinator.rebalance.protocols=classic,consumer,share` and `unstable.api.versions.enable=true` on top of the defaults.

The second is a metadata-level **feature flag** (`share.version`) that defaults to `0` (disabled) on 4.0/4.1 cluster formats. Without upgrading it, the share consumer fails to join with `UnsupportedVersionException: The cluster does not support the share group protocol`. Run this once after the brokers come up:

```bash
kubectl exec -n <namespace> kafka-0 -- env -u JMX_PORT -u KAFKA_JMX_OPTS -u KAFKA_OPTS \
  /opt/kafka/bin/kafka-features.sh \
    --bootstrap-server localhost:9092 \
    upgrade --feature share.version=1
```

Notes on the command:
- The `env -u …` prefix strips the broker's JMX env vars so the admin tool doesn't try to bind the JMX port the broker is already holding (`Address in use` otherwise).
- `--bootstrap-server` is a top-level arg; it must come before the `upgrade` subcommand or argparse rejects it as unrecognised.
- The upgrade is metadata-level, so it survives broker restarts. You only need to run it again if you reformat the cluster (drop PVCs).
- 4.2 doesn't need this — `share.version` is on by default and the broker config already advertises the share protocol.

## See also

- [`dimster-config.yaml` reference](../reference/dimster-config.md) — full schema for the config fields above.
- [Build and deploy](../getting-started/05-build-and-deploy.md) — the basic build/deploy commands referenced above.
