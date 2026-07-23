#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_INFRA="${SCRIPT_DIR}/docker-compose.infra.yml"
COMPOSE_CHOR="${SCRIPT_DIR}/docker-compose.choreography.local.yml"
COMPOSE_ORCH="${SCRIPT_DIR}/docker-compose.orchestration.local.yml"
COMPOSE_GRAFANA="${SCRIPT_DIR}/observability/docker-compose.grafana.yml"
DUAL_CHOR_PROJECT="saga-dual-choreography"
DUAL_ORCH_PROJECT="saga-dual-orchestration"
GRAFANA_PROJECT="saga-grafana"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

dual_chor_compose() { docker compose -p "$DUAL_CHOR_PROJECT" -f "$COMPOSE_CHOR" "$@"; }
dual_orch_compose() { docker compose -p "$DUAL_ORCH_PROJECT" -f "$COMPOSE_ORCH" "$@"; }
grafana_compose()   { docker compose -p "$GRAFANA_PROJECT" -f "$COMPOSE_GRAFANA" "$@"; }

isolated_mode_active() {
    docker compose -f "$COMPOSE_CHOR" ps -q 2>/dev/null | grep -q . && return 0
    docker compose -f "$COMPOSE_ORCH" ps -q 2>/dev/null | grep -q . && return 0
    grafana_compose ps -q 2>/dev/null | grep -q . && return 0
    return 1
}

dual_mode_active() {
    dual_chor_compose ps -q 2>/dev/null | grep -q . && return 0
    dual_orch_compose ps -q 2>/dev/null | grep -q . && return 0
    grafana_compose ps -q 2>/dev/null | grep -q . && return 0
    return 1
}

fail_if_isolated_running() {
    if isolated_mode_active; then
        log_error "dual mode cannot start while isolated choreography/orchestration/observability stacks are running"
        log_info "Stop the isolated stacks first with ./docker-local/local-runner.sh stop-all or make down"
        exit 1
    fi
}

wait_for_health() {
    local url=$1 name=$2 max=${3:-60} attempt=1
    log_info "Waiting for $name..."
    while [ $attempt -le $max ]; do
        curl -sf "$url" > /dev/null 2>&1 && { log_success "$name is healthy"; return 0; }
        echo -n "."; sleep 2; attempt=$((attempt + 1))
    done
    echo ""; log_error "$name failed to start"; return 1
}

# each spec is "url|name|max"; waits run in parallel, rc is nonzero if any fails
wait_for_health_parallel() {
    local spec url name max pid pids=() rc=0
    for spec in "$@"; do
        IFS='|' read -r url name max <<< "$spec"
        wait_for_health "$url" "$name" "$max" &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do
        wait "$pid" || rc=1
    done
    return $rc
}

# APP_BUILD_POLICY=cache skips --build on repeat runs. Plain `up -d` still
# recreates containers when compose config changes (e.g. new SUITE_LABEL env),
# but the image tag is reused as-is — cache assumes it already has current code.
app_build_flag() {
    [ "${APP_BUILD_POLICY:-build}" = "cache" ] || echo "--build"
}

wait_for_topic_bootstrap() {
    local attempt=1 max=${1:-30}
    log_info "Waiting for Kafka topic bootstrap..."
    while [ $attempt -le $max ]; do
        local state
        state=$(docker inspect kafka-topic-init --format '{{.State.Status}} {{.State.ExitCode}}' 2>/dev/null || true)
        if [ "$state" = "exited 0" ]; then
            log_success "Kafka topics ready"
            return 0
        fi
        if [[ "$state" == exited* ]]; then
            log_error "Kafka topic bootstrap failed"
            docker logs kafka-topic-init 2>/dev/null || true
            return 1
        fi
        echo -n "."; sleep 2; attempt=$((attempt + 1))
    done
    echo ""; log_error "Kafka topic bootstrap timed out"; return 1
}

start_infra() {
    docker compose -f "$COMPOSE_INFRA" up -d
    local attempt=1
    while [ $attempt -le 30 ]; do
        docker compose -f "$COMPOSE_INFRA" exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:29092 > /dev/null 2>&1 && { log_success "Kafka ready"; break; }
        echo -n "."; sleep 2; attempt=$((attempt + 1))
    done
    wait_for_topic_bootstrap
    log_success "Infrastructure ready"
}

start_observability() {
    grafana_compose up -d
    local health_checks=(
        "http://localhost:9102/minio/health/live|observability:minio|30"
        "http://localhost:9009/ready|observability:mimir|60"
        "http://localhost:3100/ready|observability:loki|45"
        "http://localhost:9090/-/healthy|observability:prometheus|45"
        "http://localhost:3200/ready|observability:tempo|45"
        "http://localhost:4040/metrics|observability:pyroscope|45"
        "http://localhost:3000/api/health|observability:grafana|90"
        "http://localhost:12345/-/ready|observability:alloy|45"
    )
    wait_for_health_parallel "${health_checks[@]}" || return 1
}

MINIO_DATA_DIR="${SCRIPT_DIR}/observability/data/minio"
OBS_BACKUP_DIR="${SCRIPT_DIR}/observability/backups"

obs_s3_wipe() {
    log_warn "Wiping MinIO object data under ${MINIO_DATA_DIR} (thesis durable store)"
    grafana_compose down --remove-orphans 2>/dev/null || true
    mkdir -p "${MINIO_DATA_DIR}"
    # MinIO container writes as root; host user cannot always rm. Wipe as root via Docker.
    if command -v docker >/dev/null 2>&1; then
        # Use a cached image only (never pull): a transient registry/CloudFront timeout
        # on `docker run <uncached>` aborts the campaign prep. minio/mc is guaranteed
        # cached (preflight asserts it) and ships a POSIX sh; alpine is preferred if present.
        local wipe_image=""
        for cand in "${WIPE_IMAGE:-}" alpine:3.20 minio/mc:RELEASE.2025-04-16T18-13-26Z; do
            [ -n "$cand" ] || continue
            if docker image inspect "$cand" >/dev/null 2>&1; then
                wipe_image="$cand"
                break
            fi
        done
        if [ -z "$wipe_image" ]; then
            log_error "No cached image available for MinIO wipe (looked for WIPE_IMAGE, alpine:3.20, minio/mc). Load one offline first."
            return 1
        fi
        log_info "MinIO wipe using cached image: ${wipe_image}"
        docker run --rm --entrypoint sh \
            -v "${MINIO_DATA_DIR}:/data" \
            "$wipe_image" \
            -c 'rm -rf /data/* /data/.[!.]* /data/..?* 2>/dev/null; exit 0' \
            || {
                log_error "Docker-based MinIO wipe failed"
                return 1
            }
    else
        find "${MINIO_DATA_DIR}" -mindepth 1 -maxdepth 1 -exec rm -rf {} + \
            || {
                log_error "Host MinIO wipe failed (need docker or root for root-owned objects)"
                return 1
            }
    fi
    if [ -n "$(find "${MINIO_DATA_DIR}" -mindepth 1 -maxdepth 1 2>/dev/null | head -1)" ]; then
        log_error "MinIO data dir not empty after wipe: ${MINIO_DATA_DIR}"
        return 1
    fi
    log_success "MinIO host data wiped; next start-observability recreates buckets via minio-init"
}

# Thesis durable store only: Tempo/Loki/Mimir/Pyroscope objects under data/minio.
# Does NOT include Prometheus/Grafana Docker named volumes (short-lived / UI state).
obs_s3_backup() {
    local dest=${1:-}
    local parent data_name
    parent="$(dirname "${MINIO_DATA_DIR}")"
    data_name="$(basename "${MINIO_DATA_DIR}")"
    mkdir -p "${OBS_BACKUP_DIR}" "${MINIO_DATA_DIR}"
    if [ -z "$dest" ]; then
        dest="${OBS_BACKUP_DIR}/obs-minio-$(date +%Y%m%d-%H%M%S).tar.gz"
    fi
    case "$dest" in
        *.tar.gz|*.tgz) ;;
        *) dest="${dest}.tar.gz" ;;
    esac
    log_info "Stopping observability for consistent MinIO snapshot..."
    grafana_compose down --remove-orphans 2>/dev/null || true
    if [ -z "$(find "${MINIO_DATA_DIR}" -mindepth 1 -maxdepth 1 2>/dev/null | head -1)" ]; then
        log_warn "MinIO data dir empty — archive will still be created"
    fi
    log_info "Archiving ${MINIO_DATA_DIR} -> ${dest}"
    tar -C "$parent" -czf "$dest" "$data_name"
    log_success "Backup written: $dest ($(du -h "$dest" | awk '{print $1}'))"
    log_info "Note: newest Tempo traces need flush (~max_block_duration 5m) before backup"
    log_info "Note: Tempo block_retention is 48h (not 0); older campaign windows still GC after that"
    log_info "Start again: $0 start-observability  (or start-dual-local)"
}

obs_s3_restore() {
    local archive=${1:-}
    local parent data_name
    parent="$(dirname "${MINIO_DATA_DIR}")"
    data_name="$(basename "${MINIO_DATA_DIR}")"
    if [ -z "$archive" ] || [ ! -f "$archive" ]; then
        log_error "Usage: $0 obs-s3-restore <backup.tar.gz>"
        exit 1
    fi
    log_warn "Restoring MinIO from ${archive} (replaces current durable store under ${MINIO_DATA_DIR})"
    grafana_compose down --remove-orphans 2>/dev/null || true
    mkdir -p "$parent" "${MINIO_DATA_DIR}"
    find "${MINIO_DATA_DIR}" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    tar -C "$parent" -xzf "$archive"
    if [ ! -d "${MINIO_DATA_DIR}" ]; then
        log_error "Archive missing top-level '${data_name}/' (expected path after extract: ${MINIO_DATA_DIR})"
        exit 1
    fi
    log_success "MinIO host data restored from ${archive}"
    log_info "Start again: $0 start-observability  (or start-dual-local)"
}

start_observability_beyla() {
    if curl -sf "http://localhost:8999/metrics" > /dev/null 2>&1; then
        log_success "observability:beyla is healthy"
        return 0
    fi

    docker rm -f beyla-thesis >/dev/null 2>&1 || true
    grafana_compose up -d --force-recreate beyla || {
        log_warn "observability:beyla failed to recreate; continuing without eBPF metrics"
        return 0
    }
    if wait_for_health "http://localhost:8999/metrics" "observability:beyla" 30; then
        return 0
    fi
    log_warn "observability:beyla metrics endpoint not reachable; continuing without eBPF metrics"
    return 0
}

stop_observability() {
    grafana_compose down 2>/dev/null || true
}

start_choreography() {
    # Always use dual project name so container_names match thesis dual stack
    # and benchmark_reset can tear them down without name conflicts.
    dual_chor_compose up -d $(app_build_flag)
    local health_checks=()
    for port in 8081 8082 8083 8084; do
        health_checks+=("http://localhost:${port}/actuator/health|choreography-service:${port}|90")
    done
    wait_for_health_parallel "${health_checks[@]}" || return 1
}

start_orchestration() {
    local scale_factor=${1:-1}
    local profile_flags=()

    if [ "$scale_factor" -ge 2 ]; then
        profile_flags+=(--profile scale2)
    fi
    if [ "$scale_factor" -ge 3 ]; then
        profile_flags+=(--profile scale3)
    fi
    if [ "$scale_factor" -ge 4 ]; then
        profile_flags+=(--profile scale4)
    fi

    dual_orch_compose "${profile_flags[@]}" up -d $(app_build_flag)
    local health_checks=(
        "http://localhost:8091/actuator/health|orchestration-order-service:8091|90"
    )
    if [ "$scale_factor" -ge 2 ]; then
        health_checks+=("http://localhost:8095/actuator/health|orchestration-order-service:8095|90")
    fi
    if [ "$scale_factor" -ge 3 ]; then
        health_checks+=("http://localhost:8096/actuator/health|orchestration-order-service:8096|90")
    fi
    if [ "$scale_factor" -ge 4 ]; then
        health_checks+=("http://localhost:8097/actuator/health|orchestration-order-service:8097|90")
    fi
    for port in 8092 8093 8094; do
        health_checks+=("http://localhost:${port}/actuator/health|orchestration-participant:${port}|90")
    done
    wait_for_health_parallel "${health_checks[@]}" || return 1
}

stop_all() {
    grafana_compose down 2>/dev/null || true
    dual_orch_compose down 2>/dev/null || true
    dual_chor_compose down 2>/dev/null || true
    docker compose -f "$COMPOSE_ORCH" down 2>/dev/null || true
    docker compose -f "$COMPOSE_CHOR" down 2>/dev/null || true
    docker compose -f "$COMPOSE_INFRA" down 2>/dev/null || true
    log_success "All stopped"
}

benchmark_reset() {
    log_info "Resetting benchmark application state (preserving observability data)..."
    # Dual projects own the fixed container_names used by thesis/local dual stacks.
    # Default (directory) project downs alone leave saga-dual-* containers up → name conflicts on recreate.
    dual_orch_compose --profile scale4 down -v --remove-orphans 2>/dev/null || true
    dual_chor_compose down -v --remove-orphans 2>/dev/null || true
    docker compose -f "$COMPOSE_ORCH" --profile scale4 down -v --remove-orphans 2>/dev/null || true
    docker compose -f "$COMPOSE_CHOR" down -v --remove-orphans 2>/dev/null || true
    docker compose -f "$COMPOSE_INFRA" down -v --remove-orphans 2>/dev/null || true
    # Observability: stop containers but keep volumes (mimir/prometheus series data).
    grafana_compose down --remove-orphans 2>/dev/null || true
    log_success "Benchmark stack reset complete (observability data preserved)"
}

benchmark_start() {
    local pattern=$1
    local scale_factor=${2:-1}

    case "$pattern" in
        choreography)
            start_infra
            start_choreography
            ;;
        orchestration)
            start_infra
            start_orchestration "$scale_factor"
            ;;
        *)
            log_error "Unknown benchmark pattern: $pattern"
            exit 1
            ;;
    esac
}

start_dual_local() {
    fail_if_isolated_running
    start_infra
    start_observability
    dual_chor_compose up -d $(app_build_flag)
    dual_orch_compose up -d $(app_build_flag)
    for port in 8081 8082 8083 8084 8091 8092 8093 8094; do
        wait_for_health "http://localhost:${port}/actuator/health" "dual-service:${port}" 90
    done
}

stop_dual_local() {
    docker rm -f \
		order-service-choreography \
		payment-service-choreography \
		inventory-service-choreography \
		shipping-service-choreography \
		order-service-orchestration \
		payment-service-orchestration \
		inventory-service-orchestration \
		shipping-service-orchestration \
		grafana \
		prometheus \
		tempo \
		alloy \
		beyla \
		beyla-thesis \
		pyroscope \
		cadvisor \
		node-exporter >/dev/null 2>&1 || true
	grafana_compose down -v --remove-orphans 2>/dev/null || true
	dual_orch_compose down -v --remove-orphans 2>/dev/null || true
	dual_chor_compose down -v --remove-orphans 2>/dev/null || true
	docker compose -f "$COMPOSE_INFRA" down -v --remove-orphans 2>/dev/null || true
	log_success "Dual local stacks stopped"
}

status_dual_local() {
	dual_infra_status() {
		docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | grep -E '(^NAME|^(zookeeper|kafka|chor-order-db|chor-payment-db|chor-inventory-db|chor-shipping-db|orch-order-db|orch-payment-db|orch-inventory-db|orch-shipping-db)\b)' || echo "Not running"
	}
    for label in "Infrastructure" "Observability" "Choreography" "Orchestration"; do
        echo ""
        log_info "=== Dual ${label} ==="
        case "$label" in
            Infrastructure) dual_infra_status ;;
            Observability) grafana_compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "Not running" ;;
            Choreography) dual_chor_compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "Not running" ;;
            Orchestration) dual_orch_compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "Not running" ;;
        esac
    done
}

stop_choreography()  { docker compose -f "$COMPOSE_CHOR" down; }
stop_orchestration() { docker compose -f "$COMPOSE_ORCH" down; }

clean() {
    log_warn "Remove ALL data volumes? (y/N)"
    read -r response
    [[ "$response" =~ ^[Yy]$ ]] && { stop_all; docker compose -f "$COMPOSE_INFRA" down -v; log_success "Volumes removed"; } || log_info "Aborted"
}

status() {
    for label_file in "Infrastructure:$COMPOSE_INFRA" "Observability:$COMPOSE_GRAFANA" "Choreography:$COMPOSE_CHOR" "Orchestration:$COMPOSE_ORCH"; do
        local label="${label_file%%:*}" file="${label_file#*:}"
        echo ""; log_info "=== $label ==="
        docker compose -f "$file" ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "Not running"
    done
}

health() {
    check_ports() {
        local label=$1; shift
        log_info "=== $label ==="
        for port in "$@"; do
            local st=$(curl -sf "http://localhost:$port/actuator/health" 2>/dev/null | jq -r '.status' 2>/dev/null || echo "DOWN")
            [ "$st" == "UP" ] && log_success "Port $port: $st" || log_error "Port $port: $st"
        done
    }
    check_ports "Choreography" 8081 8082 8083 8084
    echo ""
    check_ports "Orchestration" 8091 8095 8096 8097
}

logs() {
    local lines=${2:-100}
    if [ -z "$1" ]; then
        docker compose -f "$COMPOSE_INFRA" -f "$COMPOSE_CHOR" -f "$COMPOSE_ORCH" logs --tail="$lines" -f
    else
        docker compose -f "$COMPOSE_INFRA" -f "$COMPOSE_CHOR" -f "$COMPOSE_ORCH" logs --tail="$lines" -f "$1"
    fi
}

usage() {
    cat << EOF
Usage: $0 <command>

Commands:
  start-infra, start-observability, start-observability-beyla, stop-observability
  obs-s3-wipe
  obs-s3-backup [path.tar.gz]     # default: observability/backups/obs-minio-<ts>.tar.gz
  obs-s3-restore <path.tar.gz>    # replaces data/minio; does not auto-start stack
  start-choreography, start-orchestration
  start-dual-local, stop-dual-local, status-dual-local
  benchmark-reset, benchmark-start <pattern> [scale-factor]
  stop-choreography, stop-orchestration, stop-all, clean
  status, health, logs [service]
EOF
}

case "${1:-}" in
    start-infra)         start_infra ;;
    start-observability) start_observability ;;
    start-observability-beyla) start_observability_beyla ;;
    stop-observability)  stop_observability ;;
    obs-s3-wipe)         obs_s3_wipe ;;
    obs-s3-backup)       obs_s3_backup "$2" ;;
    obs-s3-restore)      obs_s3_restore "$2" ;;
    start-choreography)  start_choreography ;;
    start-orchestration) start_orchestration "$2" ;;
    start-dual-local)    start_dual_local ;;
    stop-dual-local)     stop_dual_local ;;
    status-dual-local)   status_dual_local ;;
    benchmark-reset)     benchmark_reset ;;
    benchmark-start)     benchmark_start "$2" "$3" ;;
    stop-choreography)   stop_choreography ;;
    stop-orchestration)  stop_orchestration ;;
    stop-all)            stop_all ;;
    clean)               clean ;;
    status)              status ;;
    health)              health ;;
    logs)                logs "$2" "$3" ;;
    *)                   usage; exit 1 ;;
esac
