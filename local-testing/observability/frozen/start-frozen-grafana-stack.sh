#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.frozen.yml"
PROJECT="saga-grafana-frozen-thesis-20260517"
REPLACE_FROZEN_VOLUMES=false

VOLUME_KEYS=(
  grafana_data
  prometheus_data
  tempo_data
  pyroscope_data
  alloy_symb_cache
)

log() { printf '[frozen-start] %s\n' "$*"; }
fail() { printf '[frozen-start:error] %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'USAGE'
Usage:
  local-testing/observability/frozen/start-frozen-grafana-stack.sh [--replace-frozen-volumes] <artifact-dir>

The artifact directory is usually:
  results/frozen-observability/thesis-20260518-122032-133713

This script only touches volumes under compose project:
  saga-grafana-frozen-thesis-20260517
USAGE
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

volume_name() {
  printf '%s_%s' "$PROJECT" "$1"
}

ensure_volume() {
  local key=$1
  local volume
  volume="$(volume_name "$key")"

  docker volume inspect "$volume" >/dev/null 2>&1 || docker volume create "$volume" >/dev/null
}

volume_empty() {
  local volume=$1
  docker run --rm --volume "${volume}:/target" alpine:3.20 sh -c '[ -z "$(ls -A /target 2>/dev/null)" ]'
}

archive_has_content() {
  local archive=$1
  local entry_count
  entry_count="$(zstd -dc "$archive" | tar -tf - | grep -cv '^\./$' || true)"
  [ "$entry_count" -gt 0 ]
}

clear_volume() {
  local volume=$1
  docker run --rm --volume "${volume}:/target" alpine:3.20 sh -c 'find /target -mindepth 1 -maxdepth 1 -exec rm -rf {} +'
}

restore_volume() {
  local key=$1
  local archive=$2
  local volume
  volume="$(volume_name "$key")"

  if ! volume_empty "$volume"; then
    if [ "$REPLACE_FROZEN_VOLUMES" = true ]; then
      log "Clearing existing frozen volume: ${volume}"
      clear_volume "$volume"
    else
      fail "Frozen volume is not empty: ${volume}. Re-run with --replace-frozen-volumes to overwrite frozen data."
    fi
  fi

  log "Restoring ${archive} -> ${volume}"
  zstd -dc "$archive" | docker run --rm -i --volume "${volume}:/target" --workdir /target alpine:3.20 tar -xf -
}

main() {
  require_command docker
  require_command zstd

  if [ "${1:-}" = "--replace-frozen-volumes" ]; then
    REPLACE_FROZEN_VOLUMES=true
    shift
  fi

  if [ "$#" -ne 1 ]; then
    usage >&2
    exit 1
  fi

  local artifact_dir=$1
  if [[ "$artifact_dir" != /* ]]; then
    artifact_dir="${REPO_ROOT}/${artifact_dir}"
  fi

  [ -d "$artifact_dir" ] || fail "Artifact directory does not exist: $artifact_dir"
  [ -f "${artifact_dir}/SHA256SUMS" ] || fail "Missing checksums: ${artifact_dir}/SHA256SUMS"
  [ -d "${artifact_dir}/config/grafana/provisioning" ] || fail "Missing Grafana provisioning snapshot"
  [ -f "${artifact_dir}/config/prometheus/prometheus.yml" ] || fail "Missing frozen Prometheus config"
  [ -f "${artifact_dir}/config/tempo/tempo.yml" ] || fail "Missing frozen Tempo config"

  local nonempty_count=0
  local nonempty_archive_count=0
  for key in "${VOLUME_KEYS[@]}"; do
    local archive="${artifact_dir}/volumes/${key}.tar.zst"
    [ -f "$archive" ] || fail "Missing volume archive: ${key}.tar.zst"
    ensure_volume "$key"
    if archive_has_content "$archive"; then
      nonempty_archive_count=$((nonempty_archive_count + 1))
    fi
    if volume_empty "$(volume_name "$key")"; then
      true
    else
      nonempty_count=$((nonempty_count + 1))
    fi
  done

  (
    cd "$artifact_dir"
    sha256sum -c SHA256SUMS
  )

  if [ "$REPLACE_FROZEN_VOLUMES" = true ] || [ "$nonempty_count" -eq 0 ]; then
    for key in "${VOLUME_KEYS[@]}"; do
      restore_volume "$key" "${artifact_dir}/volumes/${key}.tar.zst"
    done
  elif [ "$nonempty_count" -eq "$nonempty_archive_count" ]; then
    log "Frozen volumes already contain data; skipping restore and starting existing frozen stack."
  else
    fail "Frozen volumes are partially restored. Re-run with --replace-frozen-volumes to restore from the archive."
  fi

  export FROZEN_ARTIFACT_DIR="$artifact_dir"
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" up -d

  log "Frozen Grafana:    http://localhost:3300"
  log "Frozen Prometheus: http://localhost:9900"
  log "Frozen Tempo:      http://localhost:3320"
  log "Frozen Pyroscope:  http://localhost:4404"
  log "Use Grafana time range UTC 2026-05-18T05:20:32Z to 2026-05-18T06:37:13Z."
}

main "$@"
