#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
ARTIFACT_NAME="${ARTIFACT_NAME:-thesis-20260518-122032-133713}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${REPO_ROOT}/results/frozen-observability/${ARTIFACT_NAME}}"

SOURCE_PROJECT="saga-grafana"
WINDOW_FILE="${WINDOW_FILE:-${SCRIPT_DIR}/thesis-20260518-window.json}"
DASHBOARD_DIR="${REPO_ROOT}/local-testing/observability/grafana/dashboards"

VOLUME_KEYS=(
  grafana_data
  prometheus_data
  tempo_data
  pyroscope_data
  alloy_symb_cache
)

RESULT_DIRS=(
  results/k6-thesis/fresh-comparison-20260518-121930
  results/k6-thesis/fresh-scalability-20260518-121930
  results/k6-thesis/fresh-resilience-20260518-121930
)

log() { printf '[freeze] %s\n' "$*"; }
fail() { printf '[freeze:error] %s\n' "$*" >&2; exit 1; }

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

require_volume() {
  local volume=$1
  docker volume inspect "$volume" >/dev/null 2>&1 || fail "Missing Docker volume: $volume"
}

archive_volume() {
  local source_volume=$1
  local target_file=$2

  log "Archiving ${source_volume} -> ${target_file}"
  docker run --rm \
    --volume "${source_volume}:/source:ro" \
    --workdir /source \
    alpine:3.20 \
    tar -cf - . | zstd -T0 -19 -o "${target_file}"
}

copy_if_exists() {
  local source=$1
  local destination=$2

  if [ -e "$source" ]; then
    mkdir -p "$(dirname "$destination")"
    cp -a "$source" "$destination"
  else
    log "Skipping missing optional path: ${source}"
  fi
}

main() {
  require_command docker
  require_command zstd
  require_command sha256sum

  [ -f "$WINDOW_FILE" ] || fail "Missing window metadata: $WINDOW_FILE"
  [ -d "$DASHBOARD_DIR" ] || fail "Missing dashboard directory: $DASHBOARD_DIR"

  if [ -e "$ARTIFACT_DIR" ]; then
    fail "Artifact directory already exists: $ARTIFACT_DIR"
  fi

  for key in "${VOLUME_KEYS[@]}"; do
    require_volume "${SOURCE_PROJECT}_${key}"
  done

  mkdir -p \
    "${ARTIFACT_DIR}/volumes" \
    "${ARTIFACT_DIR}/dashboards" \
    "${ARTIFACT_DIR}/config/grafana" \
    "${ARTIFACT_DIR}/config/prometheus" \
    "${ARTIFACT_DIR}/config/tempo" \
    "${ARTIFACT_DIR}/metadata" \
    "${ARTIFACT_DIR}/results/k6-thesis"

  cp "$WINDOW_FILE" "${ARTIFACT_DIR}/metadata/$(basename "$WINDOW_FILE")"
  cp "${DASHBOARD_DIR}"/*.json "${ARTIFACT_DIR}/dashboards/"

  cp -a "${REPO_ROOT}/local-testing/observability/grafana/provisioning" "${ARTIFACT_DIR}/config/grafana/provisioning"
  cp -a "${REPO_ROOT}/local-testing/observability/grafana/alloy" "${ARTIFACT_DIR}/config/grafana/alloy"
  cp "${SCRIPT_DIR}/prometheus.frozen.yml" "${ARTIFACT_DIR}/config/prometheus/prometheus.yml"
  cp "${SCRIPT_DIR}/tempo.frozen.yml" "${ARTIFACT_DIR}/config/tempo/tempo.yml"
  cp "${REPO_ROOT}/local-testing/observability/docker-compose.grafana.yml" "${ARTIFACT_DIR}/config/docker-compose.grafana.source.yml"
  cp "${SCRIPT_DIR}/docker-compose.frozen.yml" "${ARTIFACT_DIR}/config/docker-compose.frozen.yml"

  for result_dir in "${RESULT_DIRS[@]}"; do
    copy_if_exists "${REPO_ROOT}/${result_dir}" "${ARTIFACT_DIR}/results/k6-thesis/$(basename "$result_dir")"
  done

  for key in "${VOLUME_KEYS[@]}"; do
    archive_volume "${SOURCE_PROJECT}_${key}" "${ARTIFACT_DIR}/volumes/${key}.tar.zst"
  done

  {
    printf 'artifact=%s\n' "$ARTIFACT_NAME"
    printf 'created_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'source_project=%s\n' "$SOURCE_PROJECT"
    printf 'frozen_project=%s\n' "saga-grafana-frozen-thesis-20260517"
    printf 'window_utc_from=%s\n' "2026-05-18T05:20:32Z"
    printf 'window_utc_to=%s\n' "2026-05-18T06:37:13Z"
    printf 'window_jakarta_from=%s\n' "2026-05-18 12:20:32 WIB"
    printf 'window_jakarta_to=%s\n' "2026-05-18 13:37:13 WIB"
    printf 'suite_labels=%s\n' "fresh-comparison-20260518-121930,fresh-scalability-20260518-121930,fresh-resilience-20260518-121930"
  } > "${ARTIFACT_DIR}/metadata/FREEZE-MANIFEST.txt"

  (
    cd "$ARTIFACT_DIR"
    sha256sum \
      volumes/*.tar.zst \
      dashboards/*.json \
      config/docker-compose.frozen.yml \
      config/prometheus/prometheus.yml \
      config/tempo/tempo.yml \
      metadata/* > SHA256SUMS
  )

  log "Frozen observability artifact created at: ${ARTIFACT_DIR}"
  log "Start it with: ${SCRIPT_DIR}/start-frozen-grafana-stack.sh ${ARTIFACT_DIR}"
}

main "$@"
