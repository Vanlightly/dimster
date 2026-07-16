#!/usr/bin/env bash
set -euo pipefail



# Throwaway TLS campaign runner.
#
# The Dimster environment controls whether Kafka exposes TLS and whether
# replication traffic uses TLS. The workload's Kafka config controls whether
# clients use the SSL listener, so the four cases below combine deploy-time
# environment overrides with run-time Kafka config selection.

DIMSTER_BIN="${DIMSTER_BIN:-./dimster}"
ENV_NAME="${ENV_NAME:-localBeefy}"
WORKLOAD="${WORKLOAD:-run/workloads/quick-test/quick-run-test.yaml}"
PLAINTEXT_KAFKA_CONFIG="${PLAINTEXT_KAFKA_CONFIG:-run/kafka-configs/kafka_safe.yaml}"
TLS_KAFKA_CONFIG="${TLS_KAFKA_CONFIG:-run/kafka-configs/kafka_safe_tls.yaml}"
SERIES="${SERIES:-tls-permutations}"

run_case() {
  local case_name="$1"
  local client_broker_tls="$2"
  local inter_broker_tls="$3"
  local kafka_config="$4"
  local instance="$5"

  # Any TLS mode needs kafka.tls.enabled=true so deploy provisions the SSL
  # listener, certificates, and secret mounts. Client TLS is then selected by
  # the workload Kafka config, not by the environment alone.
  local tls_enabled="false"
  if [[ "${client_broker_tls}" == "true" || "${inter_broker_tls}" == "true" ]]; then
    tls_enabled="true"
  fi

  printf '\n==> Deploying TLS case: %s\n' "${case_name}"
  "${DIMSTER_BIN}" deploy "${ENV_NAME}" \
    --env-override "kafka.tls.enabled=${tls_enabled}" \
    --env-override "kafka.tls.interBroker=${inter_broker_tls}"

  printf '==> Running TLS case: %s\n' "${case_name}"
  "${DIMSTER_BIN}" run "${ENV_NAME}" \
    -k "${kafka_config}" \
    -w "${WORKLOAD}" \
    -m "series=${SERIES}" \
    -m "instance=${instance}" \
    -m "label=${case_name}" \
    -d "tlsEnabled=${tls_enabled}" \
    -d "clientBrokerTls=${client_broker_tls}" \
    -d "interBrokerTls=${inter_broker_tls}"
}

instance="tls-$(date +%s)"

run_case "tls-none" "false" "false" "${PLAINTEXT_KAFKA_CONFIG}" "${instance}"
run_case "tls-inter-broker-only" "false" "true" "${PLAINTEXT_KAFKA_CONFIG}" "${instance}"
run_case "tls-client-broker-only" "true" "false" "${TLS_KAFKA_CONFIG}" "${instance}"
run_case "tls-all" "true" "true" "${TLS_KAFKA_CONFIG}" "${instance}"
