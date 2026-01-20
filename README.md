# Saga Pattern Comparison: Choreography vs Orchestration

A thesis project comparing two implementations of the Saga Pattern for managing distributed transactions in microservices architecture.

## Table of Contents

- [Overview](#overview)
- [Architecture Diagrams](#architecture-diagrams)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Service Ports](#service-ports)
- [API Testing](#api-testing)
- [Load Testing](#load-testing)
- [Monitoring & Observability](#monitoring--observability)
- [Thesis Comparison Metrics](#thesis-comparison-metrics)

---

## Overview

This project implements an **e-commerce order processing system** using two different Saga Pattern approaches:

1. **Choreography-based Saga** - Decentralized approach where services communicate through Kafka events
2. **Orchestration-based Saga** - Centralized approach using Eventuate Tram with a Saga orchestrator

Both implementations handle the same business flow with full compensation (rollback) on failure.

---

## Architecture Diagrams

### Business Flow (Same for Both Patterns)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           HAPPY PATH (Success)                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐             │
│   │  Create  │───▶│  Process │───▶│  Reserve │───▶│ Schedule │───▶ COMPLETED│
│   │  Order   │    │  Payment │    │ Inventory│    │ Shipping │             │
│   └──────────┘    └──────────┘    └──────────┘    └──────────┘             │
│                                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                        COMPENSATION (On Failure)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐             │
│   │  Reject  │◀───│  Refund  │◀───│  Release │◀───│  Cancel  │◀─── FAILURE │
│   │  Order   │    │  Payment │    │ Inventory│    │ Shipping │             │
│   └──────────┘    └──────────┘    └──────────┘    └──────────┘             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

### Choreography Pattern (Event-Driven)

Each service publishes events and reacts to events from other services. **No central coordinator.**

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CHOREOGRAPHY PATTERN                                 │
│                    (Decentralized Event-Driven)                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐                           ┌─────────────────┐          │
│  │  Order Service  │──── OrderCreatedEvent ───▶│ Payment Service │          │
│  │    (8081)       │◀── PaymentCompletedEvent ─│    (8082)       │          │
│  └────────┬────────┘                           └────────┬────────┘          │
│           │                                             │                    │
│           │ OrderCompletedEvent              PaymentCompletedEvent           │
│           │                                             │                    │
│           ▼                                             ▼                    │
│  ┌─────────────────┐                           ┌─────────────────┐          │
│  │Shipping Service │◀── InventoryReservedEvent─│Inventory Service│          │
│  │    (8084)       │                           │    (8083)       │          │
│  └─────────────────┘                           └─────────────────┘          │
│           │                                                                  │
│           └──────────── ShippingScheduledEvent ──────────▶ Order Service    │
│                                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                           ┌─────────────────┐                                │
│                           │   Apache Kafka  │                                │
│                           │  (Message Bus)  │                                │
│                           └─────────────────┘                                │
│                                                                              │
│  Topics: order-events, payment-events, inventory-events, shipping-events    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘

Event Flow (Happy Path):
═══════════════════════

  Order          Payment         Inventory        Shipping
    │                │                │                │
    │ OrderCreated   │                │                │
    │───────────────▶│                │                │
    │                │ PaymentCompleted                │
    │                │───────────────▶│                │
    │                │                │ InventoryReserved
    │                │                │───────────────▶│
    │                │                │                │ ShippingScheduled
    │◀───────────────┼────────────────┼────────────────│
    │ (Order Completed)               │                │
    ▼                ▼                ▼                ▼

Compensation Flow (On Failure):
═══════════════════════════════

  If Inventory fails after Payment succeeded:
    
    │ InventoryReservationFailed     │
    │◀───────────────────────────────│
    │                                │
    │ (Triggers PaymentRefund)       │
    │───────────────▶│               │
    │ PaymentRefunded│               │
    │◀───────────────│               │
    │ (Order Rejected)               │
    ▼                ▼               ▼
```

---

### Orchestration Pattern (Central Coordinator)

The Order Service acts as the **Saga Orchestrator**, controlling the entire flow.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATION PATTERN                                 │
│                    (Centralized Saga Manager)                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│                      ┌───────────────────────────┐                          │
│                      │      Order Service        │                          │
│                      │    ┌─────────────────┐    │                          │
│                      │    │  SAGA MANAGER   │    │                          │
│                      │    │  (Orchestrator) │    │                          │
│                      │    └────────┬────────┘    │                          │
│                      │             │ (8085)      │                          │
│                      └─────────────┼─────────────┘                          │
│                                    │                                         │
│              ┌─────────────────────┼─────────────────────┐                  │
│              │                     │                     │                  │
│              ▼                     ▼                     ▼                  │
│     ┌────────────────┐   ┌────────────────┐   ┌────────────────┐           │
│     │Payment Service │   │Inventory Service│  │Shipping Service│           │
│     │   (8086)       │   │   (8087)        │  │   (8088)       │           │
│     │                │   │                 │  │                │           │
│     │ ProcessPayment │   │ ReserveInventory│  │ ScheduleShip   │           │
│     │ RefundPayment  │   │ ReleaseInventory│  │ CancelShipping │           │
│     └────────────────┘   └─────────────────┘  └────────────────┘           │
│                                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│     ┌─────────────────┐          ┌─────────────────┐                        │
│     │  Eventuate Tram │          │   Eventuate     │                        │
│     │   (Commands)    │◀────────▶│   CDC Service   │                        │
│     └─────────────────┘          └─────────────────┘                        │
│              │                            │                                  │
│              └────────────┬───────────────┘                                  │
│                           ▼                                                  │
│                    ┌─────────────┐                                           │
│                    │Apache Kafka │                                           │
│                    └─────────────┘                                           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘

Command/Reply Flow (Happy Path):
════════════════════════════════

  Saga Manager      Payment         Inventory        Shipping
       │                │                │                │
       │ ProcessPayment │                │                │
       │───────────────▶│                │                │
       │ PaymentProcessed               │                │
       │◀───────────────│                │                │
       │                │                │                │
       │ ReserveInventory               │                │
       │────────────────────────────────▶│                │
       │                InventoryReserved│                │
       │◀────────────────────────────────│                │
       │                │                │                │
       │ ScheduleShipping               │                │
       │─────────────────────────────────────────────────▶│
       │                               ShippingScheduled │
       │◀─────────────────────────────────────────────────│
       │                │                │                │
       ▼ (Saga Complete)                 │                │

Compensation Flow (Automatic by Saga Manager):
══════════════════════════════════════════════

  If Shipping fails after Payment & Inventory succeeded:

       │ ShippingFailed │                │                │
       │◀────────────────────────────────────────────────│
       │                │                │                │
       │ ReleaseInventory (Compensation) │                │
       │────────────────────────────────▶│                │
       │                InventoryReleased│                │
       │◀────────────────────────────────│                │
       │                │                │                │
       │ RefundPayment (Compensation)    │                │
       │───────────────▶│                │                │
       │ PaymentRefunded│                │                │
       │◀───────────────│                │                │
       │                │                │                │
       ▼ (Saga Compensated - Order Rejected)              │
```

---

### Side-by-Side Comparison

```
┌─────────────────────────────┬─────────────────────────────┐
│       CHOREOGRAPHY          │       ORCHESTRATION         │
├─────────────────────────────┼─────────────────────────────┤
│                             │                             │
│  ┌───┐ ┌───┐ ┌───┐ ┌───┐   │         ┌───────┐          │
│  │ O │ │ P │ │ I │ │ S │   │         │SAGA   │          │
│  │ R │ │ A │ │ N │ │ H │   │         │MANAGER│          │
│  │ D │ │ Y │ │ V │ │ I │   │         └───┬───┘          │
│  │ E │ │ M │ │ E │ │ P │   │      ┌──────┼──────┐       │
│  │ R │ │ T │ │ N │ │   │   │      │      │      │       │
│  └─┬─┘ └─┬─┘ └─┬─┘ └─┬─┘   │    ┌─┴─┐  ┌─┴─┐  ┌─┴─┐    │
│    │     │     │     │     │    │PAY│  │INV│  │SHP│    │
│    └──┬──┴──┬──┴──┬──┘     │    └───┘  └───┘  └───┘    │
│       │     │     │        │                             │
│   ════╧═════╧═════╧════    │        (Commands via       │
│       EVENT BUS            │         Eventuate Tram)    │
│      (Kafka Topics)        │                             │
│                             │                             │
├─────────────────────────────┼─────────────────────────────┤
│ - Decentralized control     │ - Centralized control      │
│ - Loose coupling            │ - Clear transaction state  │
│ - Each service knows        │ - Only orchestrator knows  │
│   its next step             │   the full workflow        │
│ - Complex failure tracking  │ - Simple failure handling  │
│ - No single point of failure│ - Orchestrator is critical │
│ - Better for simple flows   │ - Better for complex flows │
└─────────────────────────────┴─────────────────────────────┘
```

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Language | Java 17 |
| Framework | Spring Boot 3.2.0 |
| Build Tool | Maven (multi-module) |
| Choreography Messaging | Apache Kafka |
| Orchestration Framework | Eventuate Tram Saga |
| Database | PostgreSQL (database per service) |
| Metrics | Micrometer + Prometheus |
| Tracing | Zipkin |
| Dashboards | Grafana |
| Load Testing | k6 |
| Containerization | Docker & Docker Compose |
| Container Images | Jib (no Dockerfile needed) |

---

## Project Structure

```
saga-pattern/
├── pom.xml                              # Root POM (multi-module)
├── common/                              # Shared classes (commands, events, DTOs)
│   └── src/main/java/com/thesis/common/
│       ├── commands/                    # Saga commands (orchestration)
│       ├── events/                      # Domain events (choreography)
│       ├── dto/                         # Shared DTOs
│       └── exception/                   # Custom exceptions
│
├── choreography-saga/                   # Kafka-based implementation
│   ├── order-service/                   # Port 8081
│   ├── payment-service/                 # Port 8082
│   ├── inventory-service/               # Port 8083
│   └── shipping-service/                # Port 8084
│
├── orchestration-saga/                  # Eventuate Tram implementation
│   ├── order-service/                   # Port 8085 (contains Saga Manager)
│   ├── payment-service/                 # Port 8086
│   ├── inventory-service/               # Port 8087
│   └── shipping-service/                # Port 8088
│
├── infrastructure/
│   ├── docker-compose.choreography.yml        # Full stack
│   ├── docker-compose.orchestration.yml       # Full stack
│   ├── docker-compose.choreography-infra.yml  # Infra only
│   ├── docker-compose.orchestration-infra.yml # Infra only
│   ├── prometheus/                            # Prometheus configs
│   └── grafana/                               # Grafana dashboards
│
└── load-testing/k6/
    ├── happy-path-test.js               # Basic success testing
    ├── comparison-test.js               # Side-by-side comparison
    └── failure-scenarios-test.js        # Compensation testing
```

---

## Quick Start

### Prerequisites

- Java 17+
- Maven 3.8+
- Docker & Docker Compose
- (Optional) k6 for load testing

### Option 1: Run with Docker Compose (Recommended)

```bash
# Build all services
mvn clean package -DskipTests

# Run Choreography Pattern
cd infrastructure
docker compose -f docker-compose.choreography.yml up -d

# OR Run Orchestration Pattern
docker compose -f docker-compose.orchestration.yml up -d

# Check services are healthy
docker compose -f docker-compose.choreography.yml ps
```

### Option 2: Run Locally (for debugging)

```bash
# Start infrastructure only
cd infrastructure
docker compose -f docker-compose.choreography-infra.yml up -d

# Run services locally (in separate terminals)
cd choreography-saga/order-service && mvn spring-boot:run
cd choreography-saga/payment-service && mvn spring-boot:run
cd choreography-saga/inventory-service && mvn spring-boot:run
cd choreography-saga/shipping-service && mvn spring-boot:run
```

---

## Service Ports

### Choreography Pattern

| Service | App Port | DB Port | Actuator |
|---------|----------|---------|----------|
| Order Service | 8081 | 5432 | /actuator/health |
| Payment Service | 8082 | 5433 | /actuator/health |
| Inventory Service | 8083 | 5434 | /actuator/health |
| Shipping Service | 8084 | 5435 | /actuator/health |
| Kafka | 9092 | - | - |
| Zipkin | 9411 | - | - |

### Orchestration Pattern

| Service | App Port | DB Port | Actuator |
|---------|----------|---------|----------|
| Order Service | 8085 | 5436 | /actuator/health |
| Payment Service | 8086 | 5437 | /actuator/health |
| Inventory Service | 8087 | 5438 | /actuator/health |
| Shipping Service | 8088 | 5439 | /actuator/health |
| Eventuate CDC | 8099 | - | - |
| Kafka | 9093 | - | - |
| Zipkin | 9412 | - | - |

### Monitoring (Shared)

| Service | Port | Credentials |
|---------|------|-------------|
| Prometheus | 9090 | - |
| Grafana | 3000 | admin/admin |

---

## API Testing

### Create Order

```bash
# Choreography (port 8081)
curl -X POST http://localhost:8081/api/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "customerId": "CUST-001",
    "shippingAddress": "123 Main Street, City, Country",
    "items": [
      {
        "productId": "PROD-001",
        "productName": "Sample Product",
        "quantity": 2,
        "price": 49.99
      }
    ]
  }'

# Orchestration (port 8085)
curl -X POST http://localhost:8085/api/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "customerId": "CUST-001",
    "shippingAddress": "123 Main Street, City, Country",
    "items": [
      {
        "productId": "PROD-001",
        "productName": "Sample Product",
        "quantity": 2,
        "price": 49.99
      }
    ]
  }'
```

### Get Order Status

```bash
# Choreography
curl http://localhost:8081/api/orders/{orderId}

# Orchestration
curl http://localhost:8085/api/orders/{orderId}
```

### Health Check

```bash
curl http://localhost:8081/actuator/health
```

---

## Load Testing

### Install k6

```bash
# macOS
brew install k6

# Ubuntu/Debian
sudo gpg -k
sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt-get update
sudo apt-get install k6
```

### Run Tests

```bash
cd load-testing/k6

# Happy path test (single pattern)
k6 run happy-path-test.js

# Comparison test (both patterns side-by-side)
k6 run comparison-test.js

# Failure scenarios (test compensation)
k6 run failure-scenarios-test.js
```

---

## Monitoring & Observability

### Grafana Dashboards

1. Open http://localhost:3000
2. Login with `admin` / `admin`
3. Navigate to Dashboards > Saga Metrics

### Prometheus Metrics

- http://localhost:9090 - Prometheus UI
- Query examples:
  - `orders_created_total` - Total orders created
  - `orders_completed_total` - Successfully completed orders
  - `orders_failed_total` - Failed orders
  - `order_processing_duration_seconds` - Processing time histogram

### Distributed Tracing (Zipkin)

- Choreography: http://localhost:9411
- Orchestration: http://localhost:9412

---

## Thesis Comparison Metrics

### Key Metrics to Compare

| Metric | Description | How to Measure |
|--------|-------------|----------------|
| **Throughput** | Orders processed per second | k6 `http_reqs` rate |
| **Latency (p50, p95, p99)** | Response time percentiles | k6 `http_req_duration` |
| **Success Rate** | % of orders completed successfully | `orders_completed / orders_created` |
| **Compensation Time** | Time to rollback failed saga | Custom metric in load test |
| **Resource Usage** | CPU, Memory per service | Docker stats / Prometheus |

### Expected Findings

| Aspect | Choreography | Orchestration |
|--------|--------------|---------------|
| **Latency** | Lower (no central coordinator) | Higher (extra hop to orchestrator) |
| **Throughput** | Higher (parallel event processing) | Lower (sequential command/reply) |
| **Debugging** | Harder (distributed logs) | Easier (centralized saga state) |
| **Coupling** | Loose (event-driven) | Tighter (orchestrator knows all) |
| **Failure Handling** | Complex (each service handles) | Simple (orchestrator manages) |
| **Scalability** | Better (independent services) | Limited by orchestrator |

### Running the Comparison

```bash
# 1. Start both systems
cd infrastructure
docker compose -f docker-compose.choreography.yml up -d
docker compose -f docker-compose.orchestration.yml up -d

# 2. Wait for services to be healthy (check Grafana)

# 3. Run comparison load test
cd ../load-testing/k6
k6 run comparison-test.js

# 4. Collect results from:
#    - k6 output (console)
#    - Grafana dashboards (screenshots)
#    - Zipkin traces (example traces)
#    - Prometheus queries (raw data)
```

---

## Stopping Services

```bash
cd infrastructure

# Stop choreography
docker compose -f docker-compose.choreography.yml down

# Stop orchestration
docker compose -f docker-compose.orchestration.yml down

# Remove all data (clean start)
docker compose -f docker-compose.choreography.yml down -v
docker compose -f docker-compose.orchestration.yml down -v
```

---

## Credentials (Development Only)

| Service | Username | Password |
|---------|----------|----------|
| PostgreSQL | postgres | postgres |
| Grafana | admin | admin |

---

## License

This project is part of a thesis research and is provided for educational purposes.
