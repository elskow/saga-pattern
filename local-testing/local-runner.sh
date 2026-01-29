#!/bin/bash
# =============================================================================
# Local Testing Runner
# =============================================================================
# Manages saga pattern services for local development and testing
#
# Usage:
#   ./local-runner.sh start-infra              # Start infrastructure
#   ./local-runner.sh start-choreography       # Start choreography (builds from source)
#   ./local-runner.sh start-orchestration      # Start orchestration (builds from source)
#   ./local-runner.sh stop-all                 # Stop all services
#   ./local-runner.sh status                   # Show status
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_INFRA="${SCRIPT_DIR}/docker-compose.infra.yml"
COMPOSE_CHOR="${SCRIPT_DIR}/docker-compose.choreography.local.yml"
COMPOSE_ORCH="${SCRIPT_DIR}/docker-compose.orchestration.local.yml"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

wait_for_health() {
    local url=$1
    local name=$2
    local max_attempts=${3:-60}
    local attempt=1

    log_info "Waiting for $name to be healthy..."
    while [ $attempt -le $max_attempts ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            log_success "$name is healthy"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    echo ""
    log_error "$name failed to become healthy after $max_attempts attempts"
    return 1
}

start_infra() {
    log_info "Starting infrastructure services..."
    docker compose -f "$COMPOSE_INFRA" up -d
    
    log_info "Waiting for Kafka to be ready..."
    sleep 10
    
    local attempt=1
    while [ $attempt -le 30 ]; do
        if docker compose -f "$COMPOSE_INFRA" exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:29092 > /dev/null 2>&1; then
            log_success "Kafka is ready"
            break
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    wait_for_health "http://localhost:16686" "Jaeger"
    log_success "Infrastructure is ready!"
}

start_choreography() {
    log_info "Building and starting choreography services..."
    docker compose -f "$COMPOSE_CHOR" up -d --build
    
    log_info "Waiting for services (JVM startup takes ~60s)..."
    wait_for_health "http://localhost:8081/actuator/health" "choreography-order-service" 90
    
    log_success "Choreography services are ready!"
    log_info "Entry point: http://localhost:8081"
}

start_orchestration() {
    log_info "Building and starting orchestration services..."
    docker compose -f "$COMPOSE_ORCH" up -d --build
    
    log_info "Waiting for services (JVM startup takes ~60s)..."
    wait_for_health "http://localhost:8085/actuator/health" "orchestration-order-service" 90
    
    log_success "Orchestration services are ready!"
    log_info "Entry point: http://localhost:8085"
}

stop_all() {
    log_info "Stopping all services..."
    docker compose -f "$COMPOSE_ORCH" down 2>/dev/null || true
    docker compose -f "$COMPOSE_CHOR" down 2>/dev/null || true
    docker compose -f "$COMPOSE_INFRA" down 2>/dev/null || true
    log_success "All services stopped"
}

stop_choreography() {
    log_info "Stopping choreography services..."
    docker compose -f "$COMPOSE_CHOR" down
    log_success "Choreography services stopped"
}

stop_orchestration() {
    log_info "Stopping orchestration services..."
    docker compose -f "$COMPOSE_ORCH" down
    log_success "Orchestration services stopped"
}

clean() {
    log_warn "This will remove ALL data volumes. Are you sure? (y/N)"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        stop_all
        log_info "Removing volumes..."
        docker compose -f "$COMPOSE_INFRA" down -v
        log_success "All volumes removed"
    else
        log_info "Aborted"
    fi
}

status() {
    echo ""
    log_info "=== Infrastructure ===" 
    docker compose -f "$COMPOSE_INFRA" ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "Not running"
    
    echo ""
    log_info "=== Choreography ===" 
    docker compose -f "$COMPOSE_CHOR" ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "Not running"
    
    echo ""
    log_info "=== Orchestration ===" 
    docker compose -f "$COMPOSE_ORCH" ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "Not running"
}

health() {
    echo ""
    log_info "=== Choreography Health ==="
    for port in 8081 8082 8083 8084; do
        local status=$(curl -sf "http://localhost:$port/actuator/health" 2>/dev/null | jq -r '.status' 2>/dev/null || echo "DOWN")
        if [ "$status" == "UP" ]; then
            log_success "Port $port: $status"
        else
            log_error "Port $port: $status"
        fi
    done
    
    echo ""
    log_info "=== Orchestration Health ==="
    for port in 8085 8086 8087 8088; do
        local status=$(curl -sf "http://localhost:$port/actuator/health" 2>/dev/null | jq -r '.status' 2>/dev/null || echo "DOWN")
        if [ "$status" == "UP" ]; then
            log_success "Port $port: $status"
        else
            log_error "Port $port: $status"
        fi
    done
}

logs() {
    local service=$1
    local lines=${2:-100}
    
    if [ -z "$service" ]; then
        docker compose -f "$COMPOSE_INFRA" -f "$COMPOSE_CHOR" -f "$COMPOSE_ORCH" logs --tail="$lines" -f
    else
        docker compose -f "$COMPOSE_INFRA" -f "$COMPOSE_CHOR" -f "$COMPOSE_ORCH" logs --tail="$lines" -f "$service"
    fi
}

usage() {
    echo "Local Testing Runner"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  start-infra          Start infrastructure (Kafka, DBs, Jaeger)"
    echo "  start-choreography   Build and start choreography services"
    echo "  start-orchestration  Build and start orchestration services"
    echo "  stop-choreography    Stop choreography services"
    echo "  stop-orchestration   Stop orchestration services"
    echo "  stop-all             Stop all services"
    echo "  clean                Remove all volumes"
    echo "  status               Show service status"
    echo "  health               Check service health"
    echo "  logs [service]       Show logs"
    echo ""
    echo "Quick Start:"
    echo "  $0 start-infra && $0 start-choreography"
    echo ""
    echo "Run Gatling Test:"
    echo "  cd ../load-testing/gatling"
    echo "  mvn gatling:test -Pchoreography,quick -Dgatling.simulationClass=simulations.HappyPathSimulation"
}

case "${1:-}" in
    start-infra) start_infra ;;
    start-choreography) start_choreography ;;
    start-orchestration) start_orchestration ;;
    stop-choreography) stop_choreography ;;
    stop-orchestration) stop_orchestration ;;
    stop-all) stop_all ;;
    clean) clean ;;
    status) status ;;
    health) health ;;
    logs) logs "$2" "$3" ;;
    *) usage; exit 1 ;;
esac
