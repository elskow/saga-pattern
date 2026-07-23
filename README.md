# Saga Pattern Comparison: Choreography vs Orchestration

Same order workflow, two saga styles, one measurement method. Runtime: Go for both.

| Style | How |
|---|---|
| Choreography | Services talk over Kafka events |
| Orchestration | Order service drives steps over Kafka + Postgres |

Flow: create order → payment → inventory → shipping → compensate reverse on failure.

## Ports

| Pattern | Order | Payment | Inventory | Shipping |
|---|---:|---:|---:|---:|
| Choreography | 8081 | 8082 | 8083 | 8084 |
| Orchestration | 8091 | 8092 | 8093 | 8094 |

Health: `/actuator/health` · Metrics: `/actuator/prometheus`

Scaled orchestration order replicas (benchmark scrape): `8091, 8095, 8096, 8097`.

### VM surface

| Pattern | Entry | Replica scrape | Internal participants |
|---|---:|---|---|
| Choreography | 8081 | 8081–8084 | 8091–8093 |
| Orchestration | 8091 | 8091, 8095–8097 | 8092–8094 |

## Make targets

```bash
make help
make tidy build test
make up-infra up-choreography up-orchestration down
make up-dual-local status-dual-local down-dual-local   # dev-only both stacks
make smoke-choreography smoke-orchestration
make k6-choreography-quick k6-orchestration-quick
```

### Dual local (dev only)

Not the thesis/benchmark path. Shared infra + Grafana; container names like `payment-service-choreography`. Fails if isolated chor/orch stacks already run.

```bash
make up-dual-local && make status-dual-local
make down-dual-local
```

## API

* `POST /api/orders`
* `GET /api/orders/{orderId}`

| | Choreography | Orchestration |
|---|---|---|
| `totalAmount` | omit | required |
| Create response | `201` full order | `202` `status=SAGA_STARTED` |

```bash
# Choreography
curl -X POST http://localhost:8081/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"CUST-001","shippingAddress":"123 Main Street","items":[{"productId":"PROD-001","productName":"Laptop","quantity":1,"price":999.99}]}'

# Orchestration
curl -X POST http://localhost:8091/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"CUST-001","totalAmount":999.99,"shippingAddress":"123 Main Street","items":[{"productId":"PROD-001","productName":"Laptop","quantity":1,"price":999.99}]}'
```

## Tests and benchmarks

```bash
make test
make k6-choreography-quick
make k6-orchestration-quick
benchmarks/run-suite.sh --pattern choreography --scenario inventory-reservation-failure --profile quick
```

Thesis evidence: one suite run, continuous Grafana window, labels `suite_label` / `pattern` / `scenario` / `run_label`.

```bash
benchmarks/run-suite.sh --suite --suite-kind comparison --profile thesis-targeted --k6-web-dashboard
benchmarks/run-suite.sh --suite --suite-kind comparison --suite-label thesis-baseline-20260716 --profile thesis-targeted --k6-web-dashboard
```

Default output: `benchmarks/results/k6-thesis/latest/` (overwritten unless `--suite-label`).

Suite starts infra + Grafana + both patterns once; images build at suite start only. Grafana mode enables k6 Prometheus remote-write (verified soft by default).

Secondary kinds:

```bash
benchmarks/run-suite.sh --suite --suite-kind scalability --profile thesis-targeted --k6-web-dashboard
benchmarks/run-suite.sh --suite --suite-kind resilience --profile thesis-targeted --k6-web-dashboard
```

`scalability`: gradual-rampup + contention. `resilience`: inventory/shipping failure then recovery happy phases.

## Required metric names

`saga_orders_created_total`, `saga_orders_completed_total`, `saga_orders_failed_total`, `saga_order_processing_time_seconds`, `saga_total_duration_seconds`, `saga_framework_duration_seconds`, `saga_framework_step_duration_seconds`, `saga_compensations_total`, `saga_compensations_payment_total`, `saga_compensations_inventory_total`, `saga_compensations_shipping_total`, `saga_framework_compensation_started_total`, `saga_framework_compensation_completed_total`

```promql
rate(saga_orders_completed_total{pattern="choreography",service="choreography"}[5m])
rate(saga_orders_completed_total{pattern="orchestration",service="orchestration"}[5m])
```

## Docs

| Doc | Content |
|---|---|
| [docs/GETTING-STARTED.md](docs/GETTING-STARTED.md) | Prereqs, structure, local/VM |
| [docs/README.md](docs/README.md) | Short index |
| [benchmarks/README.md](benchmarks/README.md) | Suite runner |
| [docker-local/observability/grafana/README.md](docker-local/observability/grafana/README.md) | Grafana stack |
| [docs/archive/](docs/archive/) | Historical plans, digs, agent notes |
