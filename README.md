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

## Make targets

```bash
make help
make tidy build test
make up-infra up-choreography up-orchestration down
make up-dual-local status-dual-local down-dual-local   # dev-only both stacks
make smoke-choreography smoke-orchestration
make k6-choreography-quick k6-orchestration-quick
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