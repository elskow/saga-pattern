#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FROZEN_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${FROZEN_DIR}/../../.." && pwd)"

FREEZE_SCRIPT="${FROZEN_DIR}/freeze-grafana-stack.sh"
START_SCRIPT="${FROZEN_DIR}/start-frozen-grafana-stack.sh"
COMPOSE_FILE="${FROZEN_DIR}/docker-compose.frozen.yml"

fail() { printf '[verify-frozen:error] %s\n' "$*" >&2; exit 1; }
log() { printf '[verify-frozen] %s\n' "$*"; }

bash -n "$FREEZE_SCRIPT"
bash -n "$START_SCRIPT"

if rg -n 'down -v|volume rm|prune' "$FREEZE_SCRIPT" "$START_SCRIPT" >/tmp/frozen-observability-risk.txt; then
  cat /tmp/frozen-observability-risk.txt >&2
  fail 'Freeze/start scripts contain destructive Docker volume patterns.'
fi

rg -q 'saga-grafana-frozen-thesis-20260517' "$START_SCRIPT" "$COMPOSE_FILE" || fail 'Frozen compose project name is missing.'
rg -q '3300:3000' "$COMPOSE_FILE" || fail 'Frozen Grafana port mapping is missing.'
rg -q '9900:9090' "$COMPOSE_FILE" || fail 'Frozen Prometheus port mapping is missing.'
rg -q '3320:3200' "$COMPOSE_FILE" || fail 'Frozen Tempo port mapping is missing.'
rg -q '4404:4040' "$COMPOSE_FILE" || fail 'Frozen Pyroscope port mapping is missing.'
rg -q 'storage.tsdb.retention.time=100y' "$COMPOSE_FILE" || fail 'Frozen Prometheus retention is not extended.'
rg -q 'block_retention: 876000h' "${FROZEN_DIR}/tempo.frozen.yml" || fail 'Frozen Tempo retention is not extended.'
rg -q 'FROZEN_ARTIFACT_DIR' "$COMPOSE_FILE" || fail 'Frozen compose does not mount artifact snapshots.'

OLD_ARTIFACT_DIR="${REPO_ROOT}/results/frozen-observability/thesis-20260517-170434-183249"
CANDIDATE_ARTIFACT_DIR="${REPO_ROOT}/results/frozen-observability/thesis-20260518-122032-133713"
WINDOW_FILE="${FROZEN_DIR}/thesis-20260518-window.json"

[ -d "$OLD_ARTIFACT_DIR" ] || fail "Old frozen artifact is missing: $OLD_ARTIFACT_DIR"
[ -f "$WINDOW_FILE" ] || fail "New window metadata is missing: $WINDOW_FILE"

for label in \
  fresh-comparison-20260518-121930 \
  fresh-scalability-20260518-121930 \
  fresh-resilience-20260518-121930; do
  rg -q "$label" "$FREEZE_SCRIPT" "$WINDOW_FILE" || fail "New suite label is missing from freeze metadata: $label"
  [ -d "${REPO_ROOT}/results/k6-thesis/${label}" ] || fail "New k6 result directory is missing: ${label}"
done

rg -q '2026-05-18T05:20:32Z' "$FREEZE_SCRIPT" "$WINDOW_FILE" || fail 'New freeze start window is missing.'
rg -q '2026-05-18T06:37:13Z' "$FREEZE_SCRIPT" "$WINDOW_FILE" || fail 'New freeze end window is missing.'
rg -q 'thesis-20260518-122032-133713' "$FREEZE_SCRIPT" "$START_SCRIPT" || fail 'New candidate artifact name is missing.'

FROZEN_ARTIFACT_DIR="$CANDIDATE_ARTIFACT_DIR" \
  docker compose -p saga-grafana-frozen-thesis-20260517 -f "$COMPOSE_FILE" config >/tmp/frozen-observability-compose.yml

rg -q 'grafana-frozen-thesis' /tmp/frozen-observability-compose.yml || fail 'Rendered compose is missing frozen Grafana container.'
rg -q 'prometheus-frozen-thesis' /tmp/frozen-observability-compose.yml || fail 'Rendered compose is missing frozen Prometheus container.'
rg -q 'tempo-frozen-thesis' /tmp/frozen-observability-compose.yml || fail 'Rendered compose is missing frozen Tempo container.'
rg -q 'pyroscope-frozen-thesis' /tmp/frozen-observability-compose.yml || fail 'Rendered compose is missing frozen Pyroscope container.'

log 'Frozen observability tooling checks passed.'
