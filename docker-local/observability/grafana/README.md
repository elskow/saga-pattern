# Grafana observability stack

Local thesis surface. Durable blocks: MinIO under `docker-local/observability/data/minio` (Tempo, Loki, Mimir, Pyroscope). Prometheus is a 24h local scrape/receive buffer that remote-writes to Mimir.

## Components

| Component | URL | Role |
|---|---|---|
| Grafana | `http://localhost:3000` | UI |
| Prometheus | `http://localhost:9090` | Scrape + k6 RW receive → Mimir |
| Mimir | `http://localhost:9009` | Long metrics (Grafana default DS) |
| Tempo | `http://localhost:3200` | Traces |
| Loki | `http://localhost:3100` | Logs |
| Alloy | `http://localhost:12345` | OTLP pipeline |
| Pyroscope | `http://localhost:4040` | Profiles |
| Beyla | `http://localhost:8999/metrics` | eBPF metrics |
| MinIO | `http://localhost:9102` | Object store (console `:9101`) |
| cAdvisor | `http://localhost:8088` | Container metrics |

## Data flow

- Traces: app OTLP → `alloy:4318` → Tempo (S3)
- Beyla traces: host → `localhost:4318` → Alloy → Tempo
- Profiles: Alloy eBPF → Pyroscope (S3)
- App metrics: Prom scrapes `/actuator/prometheus` → remote_write Mimir
- k6: `http://localhost:9090/api/v1/write` (suite default); live UI `http://localhost:5665` with `--k6-web-dashboard`

## Commands

```bash
make up-observability

./docker-local/local-runner.sh obs-s3-backup
./docker-local/local-runner.sh obs-s3-restore path/to/backup.tar.gz

benchmarks/run-suite.sh --suite --profile quick --k6-web-dashboard
# results: benchmarks/results/k6-thesis/latest/  (or --suite-label NAME)
```

## Dashboards

Provisioned from `grafana/dashboards/`. Variables: `suite_label`, `pattern`, `scenario`, `run_label`.

| Dashboard | Role |
|---|---|
| Saga Thesis Defense Overview | Top status |
| Saga Thesis Capture Template | Bab IV capture guide |
| Saga Thesis Snapshot - All Evidence | One-page evidence |
| Saga k6 Throughput Analysis | TPS / HTTP / correctness / latency |
| Saga Comparative Analysis | Chor vs orch by scenario |
| Saga Root Cause Analysis | Spanmetrics, Beyla, CPU/mem |
| Saga Evidence Health | RW, targets, labels |
| Saga Secondary Suite Analysis | Scalability / resilience |

## Notes

- Beyla/Alloy need host PID/eBPF for Docker processes.
- Tempo `local-blocks` enables recent TraceQL metrics.
- Suite verifies k6 series in Mimir per phase `run_label` (soft unless `K6_PROMETHEUS_RW_STRICT=true`).
- Default Prom scrape: orch order `8091` only; add `8095–8097` only when scale profile runs.
- Newest Tempo traces need flush (~5m `max_block_duration`) before MinIO backup if you need those IDs.

## Retention (thesis / multi-phase suites)

| Store | Setting | Semantics | Thesis note |
|---|---|---|---|
| **Tempo** | `compactor.compaction.block_retention: 48h` | Keep completed trace blocks | Must cover longest suite wall **and** time until export/re-export. `0` dropped blocks in ~15–20 min → empty early-phase `spans.csv`. |
| **Tempo** | `compacted_block_retention: 1h` | Keep pre-merge blocks after compact | Querier blocklist lag only; not data lifetime. |
| **Tempo** | `ingester.max_block_duration: 5m` | Head-block flush cadence | Wait ≥5m after last load before MinIO backup if newest IDs required. |
| **Mimir** | `compactor_blocks_retention_period: 0` | **Disable** deletion (keep) | Durable metrics + spanmetrics. `0` ≠ Tempo’s old `0`. |
| **Prometheus** | `--storage.tsdb.retention.time=24h` | Local scrape/RW buffer only | k6/app series long-term live in **Mimir**. Do not re-query Prom for multi-day claims. |
| **Loki** | `retention_enabled: false` | Keep logs | OK for thesis. |
| **Pyroscope** | no retention set | Default keep | OK for thesis. |

**Harness:** `run-suite.sh` exports Tempo traces **per phase** (after phase manifest) and again at suite end. Metrics still suite-end batch from Mimir.

**Do not** set Tempo `block_retention: 0` on campaign hosts.
