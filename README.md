# Saga Pattern Comparison, Choreography vs Orchestration

This repository compares two saga styles for the same order workflow under one thesis measurement method.

The default runtime is now Go for both variants.

## What is being compared

* **Choreography**: Go services communicate through Kafka events.
* **Orchestration**: Go services use an order service orchestrator over Kafka and Postgres.
* **Methodology**: both variants are exercised with the same API shape, the same Gatling simulations, and the same observability surface.

The business flow stays the same in both variants:

1. create order
2. process payment
3. reserve inventory
4. schedule shipping
5. compensate in reverse order on failure

## Runtime truth

### Go default path

* Choreography is a Go deployment on ports `8081` to `8084`.
* Orchestration is a Go deployment with a benchmark entrypoint on `8085`.
* In scaled orchestration benchmark runs, order service replicas are scraped on `8085`, `8086`, `8087`, and `8088`. The exact replica list is `8085, 8086, 8087, 8088`.
* Internal orchestration participants stay on `8091`, `8092`, and `8093`. The exact participant list is `8091, 8092, 8093`.
* Kafka is the transport for both variants.
* Postgres backs the orchestration runtime and the service state used by the benchmark stack.

## Root Make commands

These are the root commands you should reach for first:

* `make help` prints the target summary.
* `make tidy` syncs the root Go workspace.
* `make build` builds all Go service binaries into `build/bin/`.
* `make test` runs the root Go test suite.
* `make up-infra` starts the shared local infrastructure.
* `make up-choreography` starts the local choreography stack.
* `make up-orchestration` starts the local orchestration stack.
* `make down` stops local stacks.
* `make smoke-choreography` runs the Go choreography smoke harness.
* `make smoke-orchestration` runs the Go orchestration smoke harness.
* `make gatling-choreography-quick` runs the quick Gatling choreography suite.
* `make gatling-orchestration-quick` runs the quick Gatling orchestration suite.
* `make verify-thesis-surface` checks docs and frozen benchmark references against the compatibility matrix.
* `make verify-jenkins-benchmark` checks `Jenkinsfile.benchmark` surface expectations.

## Service ports

### Local default surface

| Pattern | Order | Payment | Inventory | Shipping |
|---|---:|---:|---:|---:|
| Choreography | 8081 | 8082 | 8083 | 8084 |
| Orchestration | 8085 | 8086 | 8087 | 8088 |

Health path: `/actuator/health`  
Prometheus path: `/actuator/prometheus`

### VM benchmark surface

| Pattern | Benchmark entrypoint | Replica scrape targets | Internal participants |
|---|---:|---|---|
| Choreography | 8081 | 8081, 8082, 8083, 8084 | 8091, 8092, 8093 |
| Orchestration | 8085 | 8085, 8086, 8087, 8088 | 8091, 8092, 8093 |

For orchestration, the benchmark client sends traffic to `8085`. Prometheus and benchmark health checks may still hit the full `8085` to `8088` replica range during scaled runs.

## API surface

Common routes:

* `POST /api/orders`
* `GET /api/orders/{orderId}`

Request asymmetry preserved for parity work:

* Choreography create order omits `totalAmount`.
* Orchestration create order requires `totalAmount`.
* Choreography returns `201` with a full order response.
* Orchestration returns `202` with `status=SAGA_STARTED`.

### Example create order requests

```bash
# Choreography
curl -X POST http://localhost:8081/api/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "customerId": "CUST-001",
    "shippingAddress": "123 Main Street, City, Country",
    "items": [
      {
        "productId": "PROD-001",
        "productName": "Laptop",
        "quantity": 1,
        "price": 999.99
      }
    ]
  }'

# Orchestration
curl -X POST http://localhost:8085/api/orders \
  -H 'Content-Type: application/json' \
  -d '{
    "customerId": "CUST-001",
    "totalAmount": 999.99,
    "shippingAddress": "123 Main Street, City, Country",
    "items": [
      {
        "productId": "PROD-001",
        "productName": "Laptop",
        "quantity": 1,
        "price": 999.99
      }
    ]
  }'
```

## Testing and benchmarking

### Go test and verification

```bash
make test
make verify-thesis-surface
make verify-jenkins-benchmark
go test ./test/compatibility/... -run TestDocsMatchCompatibilityMatrix
```

### Quick Gatling runs

```bash
make gatling-choreography-quick
make gatling-orchestration-quick
```

### Jenkins benchmark contract

`Jenkinsfile.benchmark` keeps the benchmark parameter names frozen for reproducible thesis runs:

* `BUILD_LABEL`
* `PROFILE`
* `SIMULATION`
* `RUN_CHOREOGRAPHY`
* `RUN_ORCHESTRATION`
* `WARMUP_REQUESTS`
* `COOLDOWN_SECONDS`
* `SAGA_NODE_IP`
* `GATLING_RUNNER_IP`
* `PULL_FRESH_IMAGES`
* `SCALE_FACTOR`

Supported benchmark profiles stay:

* `quick`
* `thesis-baseline`
* `thesis-stress`

## Metrics used by the thesis surface

The docs, scripts, and compatibility checks treat these as required metric names:

* `saga_orders_created_total`
* `saga_orders_completed_total`
* `saga_orders_failed_total`
* `saga_order_processing_time_seconds`
* `saga_total_duration_seconds`
* `saga_framework_duration_seconds`
* `saga_framework_step_duration_seconds`
* `saga_compensations_total`
* `saga_compensations_payment_total`
* `saga_compensations_inventory_total`
* `saga_compensations_shipping_total`
* `saga_framework_compensation_started_total`
* `saga_framework_compensation_completed_total`

Example PromQL surfaces preserved by the matrix:

```promql
rate(saga_orders_completed_total{pattern="choreography",service="choreography"}[5m])
rate(saga_orders_completed_total{pattern="orchestration",service="orchestration"}[5m])
rate(saga_order_processing_time_seconds_sum{pattern="choreography",service="choreography"}[5m]) /
rate(saga_order_processing_time_seconds_count{pattern="choreography",service="choreography"}[5m])
rate(saga_total_duration_seconds_sum{pattern="orchestration",service="orchestration"}[5m]) /
rate(saga_total_duration_seconds_count{pattern="orchestration",service="orchestration"}[5m])
```

## Repository guide

* `docs/README.md` is the short project guide.
* `docs/TESTING.md` describes compatibility, metrics, Make targets, and benchmark flows.
* `docs/THESIS.md` keeps the research framing and measurement method.
* `test/compatibility/fixtures/compatibility-matrix.json` is the frozen compatibility source of truth.

## Thesis framing

The thesis question has not changed. This repository still compares choreography and orchestration under the same workload model, the same benchmark profiles, and the same observable outputs.

What changed is the implementation reality: the active runtime path is Go for both variants.
