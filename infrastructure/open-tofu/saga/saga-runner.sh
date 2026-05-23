#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_INFRA="${SCRIPT_DIR}/docker-compose.infra.yml"
COMPOSE_CHOR="${SCRIPT_DIR}/docker-compose.choreography.yml"
COMPOSE_ORCH="${SCRIPT_DIR}/docker-compose.orchestration.yml"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

wait_for_health() {
    local url=$1 name=$2 max=${3:-30} attempt=1
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
    sleep 5
    local attempt=1
    while [ $attempt -le 30 ]; do
        docker compose -f "$COMPOSE_INFRA" exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:29092 > /dev/null 2>&1 && { log_success "Kafka ready"; break; }
        echo -n "."; sleep 2; attempt=$((attempt + 1))
    done
    wait_for_topic_bootstrap
    log_success "Infrastructure ready"
}

start_choreography() {
    docker compose -f "$COMPOSE_CHOR" up -d
    sleep 5
    wait_for_health "http://localhost:8081/actuator/health" "choreography-order-service"
}

start_orchestration() {
    docker compose -f "$COMPOSE_ORCH" up -d
    sleep 5
    wait_for_health "http://localhost:8091/actuator/health" "orchestration-order-service"
}

stop_all() {
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
    for label_file in "Infrastructure:$COMPOSE_INFRA" "Choreography:$COMPOSE_CHOR" "Orchestration:$COMPOSE_ORCH"; do
        local label="${label_file%%:*}" file="${label_file#*:}"
        echo ""; log_info "=== $label ==="
        docker compose -f "$file" ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "Not running"
    done
}

health() {
    local pattern=$1
    check_ports() {
        local label=$1; shift
        log_info "=== $label ==="
        for port in "$@"; do
            local st=$(curl -sf "http://localhost:$port/actuator/health" 2>/dev/null | jq -r '.status' 2>/dev/null || echo "DOWN")
            [ "$st" == "UP" ] && log_success "Port $port: $st" || log_error "Port $port: $st"
        done
    }
    [ -z "$pattern" ] || [ "$pattern" == "choreography" ] && check_ports "Choreography" 8081 8082 8083 8084
    [ -z "$pattern" ] || [ "$pattern" == "orchestration" ] && check_ports "Orchestration" 8091 8095 8096 8097
}

scale() {
    [ -z "$1" ] || [ -z "$2" ] && { echo "Usage: $0 scale <service> <count>"; exit 1; }
    docker compose -f "$COMPOSE_CHOR" ps "$1" > /dev/null 2>&1 && { docker compose -f "$COMPOSE_CHOR" up -d --scale "$1=$2" --no-recreate; return; }
    docker compose -f "$COMPOSE_ORCH" ps "$1" > /dev/null 2>&1 && { docker compose -f "$COMPOSE_ORCH" up -d --scale "$1=$2" --no-recreate; return; }
    log_error "Service $1 not found"; exit 1
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
  start-infra, start-choreography, start-orchestration
  stop-choreography, stop-orchestration, stop-all, clean
  status, health [pattern], scale <svc> <n>, logs [service]
EOF
}

case "${1:-}" in
    start-infra)         start_infra ;;
    start-choreography)  start_choreography ;;
    start-orchestration) start_orchestration ;;
    stop-choreography)   stop_choreography ;;
    stop-orchestration)  stop_orchestration ;;
    stop-all)            stop_all ;;
    clean)               clean ;;
    status)              status ;;
    health)              health "$2" ;;
    scale)               scale "$2" "$3" ;;
    logs)                logs "$2" "$3" ;;
    *)                   usage; exit 1 ;;
esac
