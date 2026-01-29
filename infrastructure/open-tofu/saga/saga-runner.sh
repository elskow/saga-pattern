#!/bin/bash
# =============================================================================
# Saga Pattern Thesis Test Runner
# =============================================================================
# This script manages the saga pattern services for thesis benchmarking
# 
# Usage:
#   ./saga-runner.sh start-infra              # Start infrastructure (Kafka, DBs, Jaeger)
#   ./saga-runner.sh start-choreography       # Start choreography pattern services
#   ./saga-runner.sh start-orchestration      # Start orchestration pattern services
#   ./saga-runner.sh stop-all                 # Stop all services
#   ./saga-runner.sh clean                    # Remove all volumes (fresh start)
#   ./saga-runner.sh status                   # Show service status
#   ./saga-runner.sh health [pattern]         # Check service health
#   ./saga-runner.sh scale [service] [count]  # Scale a service
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_INFRA="${SCRIPT_DIR}/docker-compose.infra.yml"
COMPOSE_CHOR="${SCRIPT_DIR}/docker-compose.choreography.yml"
COMPOSE_ORCH="${SCRIPT_DIR}/docker-compose.orchestration.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Wait for a service to be healthy
wait_for_health() {
    local url=$1
    local name=$2
    local max_attempts=${3:-30}
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

# Start infrastructure services
start_infra() {
    log_info "Starting infrastructure services..."
    docker compose -f "$COMPOSE_INFRA" up -d
    
    log_info "Waiting for infrastructure to be ready..."
    sleep 5
    
    # Wait for Kafka
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
    
    # Wait for Jaeger
    wait_for_health "http://localhost:16686" "Jaeger"
    
    log_success "Infrastructure is ready!"
}

# Start choreography services
start_choreography() {
    log_info "Starting choreography pattern services..."
    docker compose -f "$COMPOSE_CHOR" up -d
    
    log_info "Waiting for services to be healthy..."
    sleep 5
    
    wait_for_health "http://localhost:8081/actuator/health" "choreography-order-service"
    
    log_success "Choreography services are ready!"
    log_info "Entry point: http://localhost:8081"
}

# Start orchestration services
start_orchestration() {
    log_info "Starting orchestration pattern services..."
    docker compose -f "$COMPOSE_ORCH" up -d
    
    log_info "Waiting for services to be healthy..."
    sleep 5
    
    wait_for_health "http://localhost:8085/actuator/health" "orchestration-order-service"
    
    log_success "Orchestration services are ready!"
    log_info "Entry point: http://localhost:8085"
}

# Stop all services
stop_all() {
    log_info "Stopping all services..."
    docker compose -f "$COMPOSE_ORCH" down 2>/dev/null || true
    docker compose -f "$COMPOSE_CHOR" down 2>/dev/null || true
    docker compose -f "$COMPOSE_INFRA" down 2>/dev/null || true
    log_success "All services stopped"
}

# Stop only choreography
stop_choreography() {
    log_info "Stopping choreography services..."
    docker compose -f "$COMPOSE_CHOR" down
    log_success "Choreography services stopped"
}

# Stop only orchestration
stop_orchestration() {
    log_info "Stopping orchestration services..."
    docker compose -f "$COMPOSE_ORCH" down
    log_success "Orchestration services stopped"
}

# Clean all volumes
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

# Show service status
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

# Check health of services
health() {
    local pattern=$1
    echo ""
    
    if [ -z "$pattern" ] || [ "$pattern" == "choreography" ]; then
        log_info "=== Choreography Health ==="
        for port in 8081 8082 8083 8084; do
            local status=$(curl -sf "http://localhost:$port/actuator/health" 2>/dev/null | jq -r '.status' 2>/dev/null || echo "DOWN")
            if [ "$status" == "UP" ]; then
                log_success "Port $port: $status"
            else
                log_error "Port $port: $status"
            fi
        done
    fi
    
    if [ -z "$pattern" ] || [ "$pattern" == "orchestration" ]; then
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
    fi
}

# Scale a service
scale() {
    local service=$1
    local count=$2
    
    if [ -z "$service" ] || [ -z "$count" ]; then
        log_error "Usage: $0 scale <service> <count>"
        log_info "Example: $0 scale order-service 3"
        exit 1
    fi
    
    log_info "Scaling $service to $count replicas..."
    
    # Try choreography first, then orchestration
    if docker compose -f "$COMPOSE_CHOR" ps "$service" > /dev/null 2>&1; then
        docker compose -f "$COMPOSE_CHOR" up -d --scale "$service=$count" --no-recreate
    elif docker compose -f "$COMPOSE_ORCH" ps "$service" > /dev/null 2>&1; then
        docker compose -f "$COMPOSE_ORCH" up -d --scale "$service=$count" --no-recreate
    else
        log_error "Service $service not found in running compose files"
        exit 1
    fi
    
    log_success "$service scaled to $count replicas"
}

# Show logs
logs() {
    local service=$1
    local lines=${2:-100}
    
    if [ -z "$service" ]; then
        log_info "Showing last $lines lines from all services..."
        docker compose -f "$COMPOSE_INFRA" -f "$COMPOSE_CHOR" -f "$COMPOSE_ORCH" logs --tail="$lines" -f
    else
        docker compose -f "$COMPOSE_INFRA" -f "$COMPOSE_CHOR" -f "$COMPOSE_ORCH" logs --tail="$lines" -f "$service"
    fi
}

# Show usage
usage() {
    echo "Saga Pattern Thesis Test Runner"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  start-infra          Start infrastructure (Kafka, DBs, Jaeger)"
    echo "  start-choreography   Start choreography pattern services"
    echo "  start-orchestration  Start orchestration pattern services"
    echo "  stop-choreography    Stop choreography services only"
    echo "  stop-orchestration   Stop orchestration services only"
    echo "  stop-all             Stop all services"
    echo "  clean                Remove all volumes (fresh start)"
    echo "  status               Show service status"
    echo "  health [pattern]     Check service health (pattern: choreography|orchestration)"
    echo "  scale <svc> <n>      Scale a service to n replicas"
    echo "  logs [service]       Show logs (optionally for specific service)"
    echo ""
    echo "Thesis Testing Workflow:"
    echo "  1. $0 start-infra"
    echo "  2. $0 start-choreography"
    echo "  3. Run Gatling tests against port 8081"
    echo "  4. $0 stop-choreography"
    echo "  5. $0 start-orchestration"
    echo "  6. Run Gatling tests against port 8085"
    echo "  7. $0 stop-all"
}

# Main
case "${1:-}" in
    start-infra)
        start_infra
        ;;
    start-choreography)
        start_choreography
        ;;
    start-orchestration)
        start_orchestration
        ;;
    stop-choreography)
        stop_choreography
        ;;
    stop-orchestration)
        stop_orchestration
        ;;
    stop-all)
        stop_all
        ;;
    clean)
        clean
        ;;
    status)
        status
        ;;
    health)
        health "$2"
        ;;
    scale)
        scale "$2" "$3"
        ;;
    logs)
        logs "$2" "$3"
        ;;
    *)
        usage
        exit 1
        ;;
esac
