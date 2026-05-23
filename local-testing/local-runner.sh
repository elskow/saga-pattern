#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_INFRA="${SCRIPT_DIR}/docker-compose.infra.yml"
COMPOSE_CHOR="${SCRIPT_DIR}/docker-compose.choreography.local.yml"
COMPOSE_ORCH="${SCRIPT_DIR}/docker-compose.orchestration.local.yml"
COMPOSE_SIGNOZ="${SCRIPT_DIR}/observability/docker-compose.signoz.yml"
COMPOSE_GRAFANA="${SCRIPT_DIR}/observability/docker-compose.grafana.yml"
DUAL_CHOR_PROJECT="saga-dual-choreography"
DUAL_ORCH_PROJECT="saga-dual-orchestration"
SIGNOZ_PROJECT="saga-signoz"
GRAFANA_PROJECT="saga-grafana"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

dual_chor_compose() { docker compose -p "$DUAL_CHOR_PROJECT" -f "$COMPOSE_CHOR" "$@"; }
dual_orch_compose() { docker compose -p "$DUAL_ORCH_PROJECT" -f "$COMPOSE_ORCH" "$@"; }
signoz_compose()    { docker compose -p "$SIGNOZ_PROJECT" -f "$COMPOSE_SIGNOZ" "$@"; }
grafana_compose()   { docker compose -p "$GRAFANA_PROJECT" -f "$COMPOSE_GRAFANA" "$@"; }

isolated_mode_active() {
    docker compose -f "$COMPOSE_CHOR" ps -q 2>/dev/null | grep -q . && return 0
    docker compose -f "$COMPOSE_ORCH" ps -q 2>/dev/null | grep -q . && return 0
    signoz_compose ps -q 2>/dev/null | grep -q . && return 0
    grafana_compose ps -q 2>/dev/null | grep -q . && return 0
    return 1
}

dual_mode_active() {
    dual_chor_compose ps -q 2>/dev/null | grep -q . && return 0
    dual_orch_compose ps -q 2>/dev/null | grep -q . && return 0
    signoz_compose ps -q 2>/dev/null | grep -q . && return 0
    grafana_compose ps -q 2>/dev/null | grep -q . && return 0
    return 1
}

fail_if_isolated_running() {
    if isolated_mode_active; then
        log_error "dual mode cannot start while isolated choreography/orchestration/observability stacks are running"
        log_info "Stop the isolated stacks first with ./local-testing/local-runner.sh stop-all or make down"
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
    sleep 10
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
    wait_for_health "http://localhost:9090/-/healthy" "observability:prometheus" 45
    wait_for_health "http://localhost:3200/ready" "observability:tempo" 45
    wait_for_health "http://localhost:4040/metrics" "observability:pyroscope" 45
    wait_for_health "http://localhost:3000/api/health" "observability:grafana" 90
    wait_for_health "http://localhost:12345/-/ready" "observability:alloy" 45
}

start_observability_beyla() {
    if curl -sf "http://localhost:8999/metrics" > /dev/null 2>&1; then
        log_success "observability:beyla is healthy"
        return 0
    fi

    docker rm -f beyla-thesis >/dev/null 2>&1 || true
    grafana_compose up -d --force-recreate beyla
    wait_for_health "http://localhost:8999/metrics" "observability:beyla" 45
}

start_signoz_observability() {
    signoz_compose up -d
    wait_for_health "http://localhost:8080/api/v1/health" "observability:signoz" 90
    wait_for_health "http://localhost:13133" "observability:signoz-otel-collector" 45
}

stop_observability() {
    grafana_compose down 2>/dev/null || true
}

stop_signoz_observability() {
    signoz_compose down 2>/dev/null || true
}

start_choreography() {
    docker compose -f "$COMPOSE_CHOR" up -d --build
    for port in 8081 8082 8083 8084; do
        wait_for_health "http://localhost:${port}/actuator/health" "choreography-service:${port}" 90
    done
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

    docker compose -f "$COMPOSE_ORCH" "${profile_flags[@]}" up -d --build
    wait_for_health "http://localhost:8091/actuator/health" "orchestration-order-service:8091" 90
    if [ "$scale_factor" -ge 2 ]; then
        wait_for_health "http://localhost:8095/actuator/health" "orchestration-order-service:8095" 90
    fi
    if [ "$scale_factor" -ge 3 ]; then
        wait_for_health "http://localhost:8096/actuator/health" "orchestration-order-service:8096" 90
    fi
    if [ "$scale_factor" -ge 4 ]; then
        wait_for_health "http://localhost:8097/actuator/health" "orchestration-order-service:8097" 90
    fi
    for port in 8092 8093 8094; do
        wait_for_health "http://localhost:${port}/actuator/health" "orchestration-participant:${port}" 90
    done
}

stop_all() {
    signoz_compose down 2>/dev/null || true
    grafana_compose down 2>/dev/null || true
    dual_orch_compose down 2>/dev/null || true
    dual_chor_compose down 2>/dev/null || true
    docker compose -f "$COMPOSE_ORCH" down 2>/dev/null || true
    docker compose -f "$COMPOSE_CHOR" down 2>/dev/null || true
    docker compose -f "$COMPOSE_INFRA" down 2>/dev/null || true
    log_success "All stopped"
}

benchmark_reset() {
    log_info "Resetting benchmark stack state..."
    grafana_compose down -v --remove-orphans 2>/dev/null || true
    signoz_compose down -v --remove-orphans 2>/dev/null || true
    docker compose -f "$COMPOSE_ORCH" --profile scale4 down -v --remove-orphans 2>/dev/null || true
    docker compose -f "$COMPOSE_CHOR" down -v --remove-orphans 2>/dev/null || true
    docker compose -f "$COMPOSE_INFRA" down -v --remove-orphans 2>/dev/null || true
    log_success "Benchmark stack reset complete"
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
    dual_chor_compose up -d --build
    dual_orch_compose up -d --build
    for port in 8081 8082 8083 8084 8091 8092 8093 8094; do
        wait_for_health "http://localhost:${port}/actuator/health" "dual-service:${port}" 90
    done
    wait_for_health "http://localhost:8080/api/v1/health" "dual-observability:signoz" 30
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
		signoz \
		signoz-otel-collector \
		signoz-clickhouse \
		signoz-zookeeper-1 \
		signoz-init-clickhouse \
		signoz-telemetrystore-migrator \
		grafana \
		prometheus \
		tempo \
		alloy \
		beyla \
		beyla-thesis \
		pyroscope \
		cadvisor \
		node-exporter >/dev/null 2>&1 || true
	signoz_compose down -v --remove-orphans 2>/dev/null || true
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
    for label_file in "Infrastructure:$COMPOSE_INFRA" "Observability:$COMPOSE_GRAFANA" "SigNoz Legacy:$COMPOSE_SIGNOZ" "Choreography:$COMPOSE_CHOR" "Orchestration:$COMPOSE_ORCH"; do
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
  start-signoz-observability, stop-signoz-observability
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
    start-signoz-observability) start_signoz_observability ;;
    stop-signoz-observability) stop_signoz_observability ;;
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
