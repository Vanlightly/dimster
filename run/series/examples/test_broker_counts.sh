#!/usr/bin/env bash
set -euo pipefail

# COPY THIS EXAMPLE TO THE PROJECT ROOT TO RUN!

# Throwaway broker-count campaign runner.
#
# Kafka broker count is a deploy-time environment setting, so each case deploys
# the same Dimster environment with a different kafka.replicas override before
# running the same workload. The run metadata ties the three results into one
# series, while the extra dimension records the broker count for charting and
# downstream analysis.

DIMSTER_BIN="${DIMSTER_BIN:-./dimster}"
ENV_NAME="${ENV_NAME:-localBeefy}"
WORKLOAD="${WORKLOAD:-run/workloads/quick-test/quick-run-test.yaml}"
KAFKA_CONFIG="${KAFKA_CONFIG:-run/kafka-configs/kafka_safe.yaml}"
SERIES="${SERIES:-broker-count}"
BROKER_COUNTS="${BROKER_COUNTS:-3 5 7}"

run_case() {
  local broker_count="$1"
  local instance="$2"
  local label="${broker_count}-brokers"

  printf '\n==> Deploying broker-count case: %s brokers\n' "${broker_count}"
  "${DIMSTER_BIN}" deploy "${ENV_NAME}" \
    --env-override "kafka.replicas=${broker_count}"

  printf '==> Running broker-count case: %s brokers\n' "${broker_count}"
  local run_args=(
    "${ENV_NAME}"
    -w "${WORKLOAD}"
    -m "series=${SERIES}"
    -m "instance=${instance}"
    -m "label=${label}"
    -d "brokers=${broker_count}"
  )

  # Leave KAFKA_CONFIG empty when the workload embeds its kafkaConfig or when
  # the default Dimster Kafka config is desired.
  if [[ -n "${KAFKA_CONFIG}" ]]; then
    run_args+=(-k "${KAFKA_CONFIG}")
  fi

  "${DIMSTER_BIN}" run "${run_args[@]}"
}

read -r -a broker_counts <<< "${BROKER_COUNTS}"
instance="brokers-$(date +%s)"

for broker_count in "${broker_counts[@]}"; do
  run_case "${broker_count}" "${instance}"
done
