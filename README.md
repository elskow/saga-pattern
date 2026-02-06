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
| Language | Java 25 (works on 21+) |
| Framework | Spring Boot 3.5.10 |
| Build Tool | Maven (multi-module) |
| Choreography Messaging | Apache Kafka |
| Orchestration Framework | Eventuate Tram Saga |
| Database | PostgreSQL (database per service) |
| Metrics | Micrometer + Prometheus |
| Tracing | Jaeger |
| Dashboards | Grafana |
| Load Testing | Gatling (Scala) |
| Containerization | Docker & Docker Compose |
| Container Images | Jib (no Dockerfile needed) |

### Java 21/25 Modernization Highlights

This project has been modernized to take full advantage of Java 21/25 features:

**Language Features:**
- **Records** replace Lombok DTOs/events with compact validation constructors
- **Sealed interfaces** (`ChoreographyEvent`, `SagaReply`) enable exhaustive pattern matching
- **Pattern-matching switch** for clean event dispatch in listeners
- **SequencedCollection** `getFirst()` in tests; `List.of()` factories replace `Arrays.asList()`
- **`.formatted()`** string templating throughout the codebase

**Concurrency (Preview Features - require `--enable-preview`):**
- **Virtual threads** enabled via `spring.threads.virtual.enabled=true` for all services
- **StructuredTaskScope** (`OutboxPublisherScheduler.java`) replaces `CompletableFuture.allOf()` for parallel outbox publishing with proper lifecycle management
- **ScopedValue** (`SagaContext.java`) provides virtual-thread-safe correlation ID propagation as a modern alternative to ThreadLocal/MDC

**ScopedValue Usage Example:**
```java
import com.thesis.common.context.SagaContext;
import com.thesis.common.context.SagaContext.ContextData;

// Run code within a saga context scope
SagaContext.run(
    ContextData.forChoreography("order-123", "corr-456"),
    () -> {
        // Context is available throughout the scope
        String orderId = SagaContext.orderId();
        String correlationId = SagaContext.correlationId();
        processOrder(); // MDC is also synchronized for logging
    }
);
```

### Additional Performance Levers

- **Generational ZGC** for heavy benchmarks: add `-XX:+UseZGC -XX:+ZGenerational` when load-testing to shrink tail latencies
- **Virtual-thread executors** (`Executors.newVirtualThreadPerTaskExecutor()`) for any remaining blocking adapters (JDBC is already compatible; wrap Kafka client calls if done synchronously)
- **`Stream::mapMulti`** for flatter collection transforms in event mappers to cut intermediate allocations

### Spring Boot 3.5+ Cleanup Opportunities

- Prefer `RestClient`/`HttpServiceProxyFactory` over legacy `RestTemplate` for external calls; pairs well with virtual threads and reduces boilerplate
- Use record-based `@ConfigurationProperties` to drop Lombok config classes and gain constructor binding validation
- Replace hand-rolled error payloads with `ProblemDetail` in exception handlers for shorter code and standardized responses
- Enable lightweight observability defaults (`management.otlp.metrics.export.enabled=true`/`tracing.export.enabled=true`) to keep telemetry consistent across services

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
│   └── open-tofu/                             # VM deployment (OpenTofu/Terraform)
│       ├── main.tf                            # Infrastructure provisioning
│       ├── saga/                              # Service compose files
│       │   ├── docker-compose.infra.yml       # Databases, Kafka, Zookeeper
│       │   ├── docker-compose.choreography.yml
│       │   └── docker-compose.orchestration.yml
│       └── observability/                     # Monitoring stack
│           ├── docker-compose.yml             # Prometheus, Grafana, Jaeger
│           ├── prometheus/prometheus.yml
│           └── grafana/
│
└── load-testing/
    └── gatling/                         # Gatling simulations
        └── src/test/scala/simulations/  # Scala test simulations
```

---

## Quick Start

### Prerequisites

- Java 21+ (tested on 25)
- Maven 3.8+
- Docker & Docker Compose
- (Optional) Gatling runs via Maven — no separate install needed

### JVM 21/25 Runtime Settings (recommended)

- Enable preview when locally testing Loom-friendly features (if needed): `JAVA_TOOL_OPTIONS="--enable-preview"`
- Prefer virtual threads for blocking IO (already on): `spring.threads.virtual.enabled=true`
- Use G1 (default) or ZGC for lower tail latency: `-XX:+UseZGC` for heavy load tests

### Option 1: Run on VMs (Recommended for thesis testing)

This project uses a 3-VM setup deployed via OpenTofu. See [docs/SETUP-GUIDE.md](docs/SETUP-GUIDE.md) for full details.

```bash
# 1. Provision infrastructure with OpenTofu
cd infrastructure/open-tofu
tofu init && tofu apply

# 2. SSH to saga-node and start services
ssh ubuntu@<saga-node-ip>
cd ~/saga

# Start infrastructure (databases, Kafka)
sudo docker compose -f docker-compose.infra.yml up -d

# Start Choreography services
sudo docker compose -f docker-compose.choreography.yml up -d

# OR Start Orchestration services
sudo docker compose -f docker-compose.orchestration.yml up -d

# Check services are healthy
sudo docker compose -f docker-compose.choreography.yml ps
```

### Option 2: Run Locally (for development)

```bash
# Build all services
mvn clean package -DskipTests

# Use the VM compose files locally (requires Docker)
cd infrastructure/open-tofu/saga
docker compose -f docker-compose.infra.yml up -d
docker compose -f docker-compose.choreography.yml up -d
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
| Jaeger UI | 16686 | - | - |

### Orchestration Pattern

| Service | App Port | DB Port | Actuator |
|---------|----------|---------|----------|
| Order Service | 8085 | 5436 | /actuator/health |
| Payment Service | 8086 | 5437 | /actuator/health |
| Inventory Service | 8087 | 5438 | /actuator/health |
| Shipping Service | 8088 | 5439 | /actuator/health |
| Eventuate CDC | 8099 | - | - |
| Kafka | 9093 | - | - |
| Jaeger UI | 16686 | - | - |

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

## Load Testing (Gatling)

### Prerequisites

- Java 17+ (Gatling runner)
- Maven 3.8+

### Run Tests Locally

```bash
cd load-testing/gatling

# Quick smoke test (choreography)
mvn gatling:test -Dgatling.simulationClass=simulations.HappyPathSimulation \
  -DbaseHost=localhost -Dpattern=choreography -DtestDuration=60

# Quick smoke test (orchestration)
mvn gatling:test -Dgatling.simulationClass=simulations.HappyPathSimulation \
  -DbaseHost=localhost -Dpattern=orchestration -DtestDuration=60

# Sustained mixed workload (primary thesis simulation)
mvn gatling:test -Dgatling.simulationClass=simulations.SustainedMixedSimulation \
  -DbaseHost=localhost -Dpattern=choreography -DtestDuration=300
```

### Run via Jenkins (Recommended for Thesis)

The `Jenkinsfile.benchmark` pipeline automates the full comparison workflow:
1. Starts infrastructure (Kafka, PostgreSQL, Jaeger)
2. Runs choreography benchmark with warmup/cooldown
3. Runs orchestration benchmark with warmup/cooldown
4. Collects and archives Gatling HTML reports

### Available Simulations

| Simulation | Purpose |
|------------|---------|
| `SustainedMixedSimulation` | Primary thesis test: 60% valid + 20% payment failure + 20% inventory failure |
| `HappyPathSimulation` | Baseline: valid orders only |
| `FailureScenariosSimulation` | Compensation testing |
| `BurstSpikeSimulation` | Spike resilience |
| `GradualRampupSimulation` | Scalability degradation point |
| `ContentionSimulation` | Concurrent resource access |
| `IdempotencySimulation` | Duplicate handling |

---

## Monitoring & Observability

### Grafana Dashboards

1. Open http://localhost:3000
2. Login with `admin` / `admin`
3. Navigate to Dashboards > Saga Metrics

### Prometheus Metrics

- http://localhost:9090 - Prometheus UI
- Query examples:
  - `saga_orders_created_total` - Total orders created
  - `saga_orders_completed_total` - Successfully completed orders
  - `saga_orders_failed_total` - Failed orders
  - `saga_total_duration_seconds` - End-to-end saga duration
  - `saga_step_payment_duration_seconds` - Payment step duration
  - `saga_compensations_total` - Total compensations triggered
  - `saga_messages_total` - Kafka messages sent/received
  - `saga_db_writes_total` - Database write operations

### Distributed Tracing (Jaeger)

- Jaeger UI: http://localhost:16686

---

## Thesis Comparison Metrics

### Key Metrics to Compare

| Metric | Description | How to Measure |
|--------|-------------|----------------|
| **Saga Throughput** | Terminal sagas per second | Gatling saga metrics summary |
| **Saga Duration (avg, p95, max)** | End-to-end saga completion time | `saga_total_duration_seconds` |
| **Step Duration** | Per-step latency (payment, inventory, shipping) | `saga_step_*_duration_seconds` |
| **Success Rate** | % of orders completed successfully | `saga_orders_completed / saga_orders_created` |
| **Compensation Rate** | Compensations triggered | `saga_compensations_total` |
| **Message Count** | Kafka messages per transaction | `saga_messages_per_transaction` |
| **DB Write Count** | Database writes per transaction | `saga_db_writes_per_transaction` |
| **Message Latency** | Inter-service message latency | `saga_message_latency_seconds` |

### Expected Findings

| Aspect | Choreography | Orchestration |
|--------|--------------|---------------|
| **Latency** | Lower (no central coordinator) | Higher (extra hop to orchestrator) |
| **Throughput** | Higher (parallel event processing) | Lower (sequential command/reply) |
| **Debugging** | Harder (distributed logs) | Easier (centralized saga state) |
| **Coupling** | Loose (event-driven) | Tighter (orchestrator knows all) |
| **Failure Handling** | Complex (each service handles) | Simple (orchestrator manages) |
| **Scalability** | Better (independent services) | Limited by orchestrator |

### Running the Comparison (via Jenkins)

The recommended approach uses the Jenkins benchmark pipeline for reproducible A/B testing:

```bash
# Trigger via Jenkins UI or CLI:
# Job: saga-pattern-benchmark
# Parameters:
#   PROFILE: thesis-baseline (5min) or thesis-stress (15min)
#   SIMULATION: SustainedMixedSimulation
#   RUN_CHOREOGRAPHY: true
#   RUN_ORCHESTRATION: true
#   WARMUP_REQUESTS: 100
#   COOLDOWN_SECONDS: 30

# The pipeline will:
# 1. Clean up previous state
# 2. Start infra (Kafka, PostgreSQL, Jaeger)
# 3. Start choreography -> warmup -> run Gatling -> stop
# 4. Cooldown between patterns
# 5. Start orchestration -> warmup -> run Gatling -> stop
# 6. Collect Gatling HTML reports as build artifacts
# 7. Clean up
```

### Collecting Results

After a benchmark run, results are available from:
- **Gatling HTML reports** - Archived as Jenkins build artifacts
- **Grafana dashboards** - `http://<observability-ip>:3000` (Thesis - Saga Pattern Comparison)
- **Jaeger traces** - `http://<observability-ip>:16686`
- **Prometheus queries** - `http://<observability-ip>:9090`

---

## Stopping Services

```bash
# SSH to saga-node
ssh ubuntu@<saga-node-ip>
cd ~/saga

# Stop choreography services
sudo docker compose -f docker-compose.choreography.yml down

# Stop orchestration services
sudo docker compose -f docker-compose.orchestration.yml down

# Stop infrastructure (databases, Kafka)
sudo docker compose -f docker-compose.infra.yml down

# Remove all data (clean start)
sudo docker compose -f docker-compose.infra.yml down -v
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
