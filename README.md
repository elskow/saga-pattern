# Saga Pattern Comparison, Choreography vs Orchestration

This repository compares two saga styles for the same order workflow under one thesis measurement method.

The default runtime is now Go for both variants.

## What is being compared

* **Choreography**: Go services communicate through Kafka events.
* **Orchestration**: Go services use an order service orchestrator over Kafka and Postgres.
* **Methodology**: both variants are exercised with the same API shape, the same k6 scenarios, and the same Grafana observability surface.

The business flow stays the same in both variants:

1. create order
2. process payment
3. reserve inventory
4. schedule shipping
5. compensate in reverse order on failure

## Runtime truth

### Go default path

* Choreography is a Go deployment on ports `8081` to `8084`.
* Orchestration is a Go deployment with a benchmark entrypoint on `8091`.
* In scaled orchestration benchmark runs, the order service replica scrape list is `8091, 8095, 8096, 8097`.
* Internal orchestration participants stay on `8092`, `8093`, and `8094`. The exact participant list is `8092, 8093, 8094`.
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
* `make up-dual-local` starts a dev-only local mode where both variants run together.
* `make status-dual-local` shows the dual-mode stack state.
* `make down-dual-local` stops the dual-mode stack.
* `make down` stops local stacks.
* `make smoke-choreography` runs the Go choreography smoke harness.
* `make smoke-orchestration` runs the Go orchestration smoke harness.
* `make k6-choreography-quick` runs the quick k6 choreography thesis smoke.
* `make k6-orchestration-quick` runs the quick k6 orchestration thesis smoke.
* `make verify-thesis-surface` checks docs and frozen benchmark references against the compatibility matrix.
* `make verify-jenkins-benchmark` checks `Jenkinsfile.benchmark` surface expectations.

## Service ports

### Local default surface

| Pattern | Order | Payment | Inventory | Shipping |
|---|---:|---:|---:|---:|
| Choreography | 8081 | 8082 | 8083 | 8084 |
| Orchestration | 8091 | 8092 | 8093 | 8094 |

Health path: `/actuator/health`  
Prometheus path: `/actuator/prometheus`

### Dev-only dual local mode

The repo also supports an additive local mode where both variants run together on the same host.

```bash
make up-dual-local
make status-dual-local
make down-dual-local
```

Rules for this mode:

* it is **dev-only**, not the benchmark/thesis execution path
* it reuses the same local compose files as isolated local runs
* it starts shared infra and observability by default
* it uses suffix-style runtime-facing names such as `payment-service-choreography` and `payment-service-orchestration`
* it fails fast if the isolated choreography/orchestration local mode is already running
* benchmark/OpenTofu contracts remain unchanged

### VM benchmark surface

| Pattern | Benchmark entrypoint | Replica scrape targets | Internal participants |
|---|---:|---|---|
| Choreography | 8081 | 8081, 8082, 8083, 8084 | 8091, 8092, 8093 |
| Orchestration | 8091 | 8091, 8095, 8096, 8097 | 8092, 8093, 8094 |

For orchestration, the benchmark client sends traffic to `8091`. Prometheus and benchmark health checks may still hit the full scaled order-replica range during scaled runs.

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
curl -X POST http://localhost:8091/api/orders \
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

### Quick k6 Runs

```bash
make k6-choreography-quick
make k6-orchestration-quick
load-testing/thesis/run-k6-thesis.sh --pattern choreography --scenario inventory-reservation-failure --profile quick
```

Use the unified suite as the default thesis evidence so Grafana shows one continuous dataset while k6 labels still separate `suite_label`, `pattern`, `scenario`, and `run_label`:

```bash
load-testing/thesis/run-k6-thesis.sh \
  --suite \
  --suite-kind comparison \
  --suite-label final-grafana-suite \
  --profile thesis-targeted \
  --k6-web-dashboard
```

Suite mode starts infra, Grafana observability, choreography services, and orchestration services once. Service images are built only at suite startup; changing scenario phases does not rebuild images or restart the runtime.
For Grafana-mode runs, k6 Prometheus remote-write is enabled and verified by default so the Grafana dashboards can show k6 TPS, latency, and correctness directly.

Use secondary suites when the thesis needs evidence beyond the core comparison:

```bash
load-testing/thesis/run-k6-thesis.sh --suite --suite-kind scalability --suite-label final-scalability-suite --profile thesis-stress --k6-web-dashboard
load-testing/thesis/run-k6-thesis.sh --suite --suite-kind resilience --suite-label final-resilience-suite --profile thesis-targeted --k6-web-dashboard
```

`scalability` covers `gradual-rampup` and `contention`; `resilience` covers injected inventory/shipping failures followed by recovery successful-order phases.

### Jenkins benchmark contract

`Jenkinsfile.benchmark` keeps the benchmark parameter names frozen for reproducible thesis runs:

* `BUILD_LABEL`
* `PROFILE`
* `SCENARIO`
* `RUN_CHOREOGRAPHY`
* `RUN_ORCHESTRATION`
* `SAGA_NODE_IP`
* `PULL_FRESH_IMAGES`

Supported benchmark profiles stay:

* `quick`
* `thesis-targeted`
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
