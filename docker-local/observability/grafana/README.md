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
| **Tempo** | `block_retention: 876000h` (~100y) | Practical keep-forever | Tempo has **no** official forever. `0`/`0s` means delete finished blocks on next retention tick (empty early `spans.csv`). Use huge finite window. |
| **Tempo** | `compacted_block_retention: 24h` | Keep pre-merge inputs after compact | Querier blocklist lag; docs require ≥ 2× `blocklist_poll`. |
| **Tempo** | `ingester.max_block_duration: 5m` | Head-block flush cadence | Wait ≥5m after last load before MinIO backup if newest IDs required. |
| **Mimir** | `compactor_blocks_retention_period: 0` | Disable age deletion | True forever for metrics. Compaction still soft-deletes superseded *source* ULIDs (normal). |
| **Prometheus** | long TSDB window (compose) | Local buffer only | Durable metrics live in **Mimir**. |
| **Loki** | `retention_enabled: false` | Keep logs | OK for thesis. |
| **Pyroscope** | CLI `-compactor.blocks-retention-period=0` | Keep profiles | Default was 31d without flag. |

**Harness:** `run-suite.sh` exports Tempo traces **per phase** (after phase manifest) and again at suite end. Metrics still suite-end batch from Mimir.

**Never** set Tempo `block_retention: 0` / `0s` / `0h` — that is immediate age-delete, not unlimited.
