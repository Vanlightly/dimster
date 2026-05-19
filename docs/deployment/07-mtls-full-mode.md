# 07 — In-cluster mTLS (full deploy mode)

When the *thing being benchmarked* is a Kafka cluster Dimster owns (`deploy: full`), you can flip on mTLS with a single env-config switch. Dimster generates a self-signed CA + broker cert + client cert at deploy time, mounts the keystores into broker, worker and coordinator pods, and exposes a second SSL listener alongside the existing PLAINTEXT one. Individual workloads opt in per-run, so a single campaign can run a TLS workload back-to-back with a PLAINTEXT one against the same cluster.

If you're pointing Dimster at someone *else's* Kafka and just need to plumb client certs through, that's external-mode mTLS — see [06 — External Kafka](06-external-kafka.md#recipe-3--self-managed-kafka-with-mtls).

## How it works

1. **Deploy time.** The CLI generates a CA + broker cert (with SANs covering every `kafka-N.kafka-headless.<ns>.svc.cluster.local` hostname plus `kafka-bootstrap`) + a client cert. All material is packaged as PEM (Kafka 2.7+ reads PEM keystores natively — no PKCS12 / JKS dance) and stored in two namespaced Kubernetes Secrets:
   - `kafka-tls-broker` — mounted at `/etc/kafka/tls/` inside broker pods.
   - `kafka-tls-client` — mounted at `/etc/dimster/tls/` inside worker + coordinator pods.

   The broker StatefulSet gets a second listener (`SSL://:9094` by default) added on top of the existing `PLAINTEXT://:9092` and `CONTROLLER://:9093`. Both Services (`kafka-headless` and `kafka-bootstrap`) get the SSL port added so DNS-based bootstrap works either way.

2. **Run time.** When the CLI sees a workload with `commonConfig.security.protocol: SSL` (or `SASL_SSL`), it rewrites that workload's `bootstrap.servers` to point at the SSL listener and injects `ssl.keystore.*` / `ssl.truststore.*` / `ssl.key.password` so the client picks up the mounted keystore. Workloads without that line use PLAINTEXT — exactly the same path as a non-TLS deploy.

3. **Idempotency.** A redeploy of the same env reuses the existing Secrets so client material keeps working across redeploys. `dimster teardown` deletes the namespace, which deletes the Secrets — the next `dimster deploy` re-generates a fresh CA from scratch.

## Enabling TLS in env config

Add a `tls:` block under `kafka:`:

```yaml
environments:
  myEnvWithTls:
    deploy: full
    image: dimster
    context: minikube
    distro: minikube
    kafka:
      replicas: 3
      tls:
        enabled: true            # master switch
        # interBroker: false     # default false — see "Inter-broker TLS" below
        # sslPort: 9094          # default 9094
    workers:
      replicas: 2
    coordinator: {}
```

That's the whole deploy-side change. `dimster deploy myEnvWithTls` provisions everything; the broker, workers and coordinator come up with the secrets mounted.

### Inter-broker TLS

By default `interBroker: false`, which means **only client connections** use the SSL listener. Inter-broker traffic stays on PLAINTEXT. This is the right default for performance research — it isolates the cost of TLS for producers/consumers from the cost of TLS for replication, so you can measure each separately.

Set `interBroker: true` to flip inter-broker to SSL too. The broker cert already has `ClientAuth` extended-key-usage so it works as both server and client cert.

## Opting a workload in to TLS

Add a single line to the workload's `baseWorkload.kafkaConfig.commonConfig`:

```yaml
name: tls-canary
baseWorkload:
  topics: 1
  partitionsPerTopic: 12
  consumerType: SHARE_GROUP
  consumersPerGroup: 4
  producersPerTopic: 4
  kafkaConfig:
    replicationFactor: 3
    commonConfig:
      security.protocol: SSL    # opt in
    producerConfig:
      acks: all
scenarios:
  - name: warmup
    repeat: 1
```

The CLI takes care of `bootstrap.servers`, `ssl.keystore.location`, `ssl.truststore.location`, `ssl.key.password`, `ssl.keystore.type` and `ssl.truststore.type` automatically — you don't need to type any of those into the workload. If you *do* set one of them yourself (e.g. you mounted your own Secret on the side), your value wins.

`SASL_SSL` works the same way as `SSL` for the trigger. Add the SASL config (mechanism, JAAS) yourself under `commonConfig` — Dimster only owns the SSL bits.

## Side-by-side TLS vs. PLAINTEXT in one campaign

Because the opt-in is per-workload, a single `dimster run` can compare two configurations against the same cluster:

```bash
./dimster run myEnvWithTls \
  -w run/workloads/comparison/baseline-plaintext.yaml \
  -w run/workloads/comparison/baseline-ssl.yaml
```

The two workload files are identical except one has `security.protocol: SSL` in its `commonConfig` and the other doesn't. Both runs land in the same results directory; the throughput / latency delta is the cost of client-side TLS for that workload shape.

## Verifying it works

After `dimster deploy myEnvWithTls` completes:

```bash
# Both Secrets present
kubectl -n dimster get secret kafka-tls-broker kafka-tls-client

# Broker listening on both ports
kubectl -n dimster exec kafka-0 -- ss -ltn | grep -E ':(9092|9094)'

# A PLAINTEXT workload picks port 9092 (default behaviour)
./dimster run myEnvWithTls -w run/workloads/quick-test/quick-run-test.yaml
# → broker logs show PLAINTEXT auth

# A TLS-opted-in workload picks port 9094 and does an SSL handshake
./dimster run myEnvWithTls -w run/workloads/your-tls-workload.yaml
# → broker logs show "SSL handshake successful" entries for the worker IPs
```

## Limitations

- **`ssl.client.auth=required`** is fixed for now — there's no opt-out for full mTLS. If you need server-auth-only TLS (truststore on client, no client cert), it's a one-line entrypoint change but currently not exposed.
- **Comments in TLS workloads are not preserved.** The CLI re-serialises a TLS-opted-in workload via `serde_yaml`, which drops comments and blank lines. The original source file on your disk is unchanged — only the copy sent to the coordinator is re-emitted. PLAINTEXT workloads pass through verbatim and are unaffected.
- **External-mode TLS uses a different path.** Dimster only generates / distributes certs for `deploy: full`. For `deploy: external`, supply your own keystore / truststore files via the `connection:` block — see [06 — External Kafka](06-external-kafka.md#recipe-3--self-managed-kafka-with-mtls).
- **No SAN for arbitrary in-cluster names.** The broker cert covers `kafka-N.kafka-headless.<ns>.svc.cluster.local`, the wildcard `*.kafka-headless.<ns>.svc.cluster.local`, and `kafka-bootstrap.<ns>.svc.cluster.local`. If you reach the broker via a different DNS name (e.g. an Ingress) the handshake will fail hostname verification.
