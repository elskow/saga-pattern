#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_INFRA="${SCRIPT_DIR}/docker-compose.infra.yml"
COMPOSE_CHOR="${SCRIPT_DIR}/docker-compose.choreography.local.yml"
COMPOSE_ORCH="${SCRIPT_DIR}/docker-compose.orchestration.local.yml"
COMPOSE_OBS="${SCRIPT_DIR}/observability/docker-compose.observability.yml"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

wait_for_health() {
    local url=$1 name=$2 max=${3:-60} attempt=1
    log_info "Waiting for $name..."
    while [ $attempt -le $max ]; do
        curl -sf "$url" > /dev/null 2>&1 && { log_success "$name is healthy"; return 0; }
        echo -n "."; sleep 2; attempt=$((attempt + 1))
    done
    echo ""; log_error "$name failed to start"; return 1
}

start_infra() {
    docker compose -f "$COMPOSE_INFRA" up -d
    sleep 10
    local attempt=1
    while [ $attempt -le 30 ]; do
        docker compose -f "$COMPOSE_INFRA" exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:29092 > /dev/null 2>&1 && { log_success "Kafka ready"; break; }
        echo -n "."; sleep 2; attempt=$((attempt + 1))
    done
    wait_for_health "http://localhost:16686" "Jaeger"
    log_success "Infrastructure ready"
}

start_choreography() {
    docker compose -f "$COMPOSE_CHOR" up -d --build
    wait_for_health "http://localhost:8081/actuator/health" "choreography-order-service" 90
}

start_orchestration() {
    docker compose -f "$COMPOSE_ORCH" up -d --build
    wait_for_health "http://localhost:8085/actuator/health" "orchestration-order-service" 90
}

stop_all() {
    docker compose -f "$COMPOSE_OBS" down 2>/dev/null || true
    docker compose -f "$COMPOSE_ORCH" down 2>/dev/null || true
    docker compose -f "$COMPOSE_CHOR" down 2>/dev/null || true
    docker compose -f "$COMPOSE_INFRA" down 2>/dev/null || true
    log_success "All stopped"
}

stop_choreography()  { docker compose -f "$COMPOSE_CHOR" down; }
stop_orchestration() { docker compose -f "$COMPOSE_ORCH" down; }

clean() {
    log_warn "Remove ALL data volumes? (y/N)"
    read -r response
    [[ "$response" =~ ^[Yy]$ ]] && { stop_all; docker compose -f "$COMPOSE_INFRA" down -v; log_success "Volumes removed"; } || log_info "Aborted"
}

status() {
    for label_file in "Infrastructure:$COMPOSE_INFRA" "Observability:$COMPOSE_OBS" "Choreography:$COMPOSE_CHOR" "Orchestration:$COMPOSE_ORCH"; do
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
    check_ports "Orchestration" 8085 8086 8087 8088
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
  start-infra, start-observability, stop-observability
  start-choreography, start-orchestration
  stop-choreography, stop-orchestration, stop-all, clean
  status, health, logs [service]
EOF
}

case "${1:-}" in
    start-infra)         start_infra ;;
    start-observability) docker compose -f "$COMPOSE_OBS" up -d ;;
    stop-observability)  docker compose -f "$COMPOSE_OBS" down ;;
    start-choreography)  start_choreography ;;
    start-orchestration) start_orchestration ;;
    stop-choreography)   stop_choreography ;;
    stop-orchestration)  stop_orchestration ;;
    stop-all)            stop_all ;;
    clean)               clean ;;
    status)              status ;;
    health)              health ;;
    logs)                logs "$2" "$3" ;;
    *)                   usage; exit 1 ;;
esac
