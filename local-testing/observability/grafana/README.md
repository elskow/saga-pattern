# Grafana Observability Stack

This local stack is the thesis-facing observability surface for new benchmark runs. It intentionally excludes Loki.

## Components

| Component | URL | Purpose |
|---|---|---|
| Grafana | `http://localhost:3000` | Dashboard and presentation UI |
| Prometheus | `http://localhost:9090` | Metrics store and k6 remote-write receiver |
| Tempo | `http://localhost:3200` | Trace store/query backend |
| Alloy | `http://localhost:12345` | OTLP collector and telemetry pipeline |
| Pyroscope | `http://localhost:4040` | Profiling backend |
| Beyla | `http://localhost:8999/metrics` | eBPF service/network telemetry metrics |
| cAdvisor | `http://localhost:8088` | Container CPU/memory metrics |

## Data Flow

- Go service OpenTelemetry traces: service container -> `alloy:4318` -> Tempo.
- Beyla eBPF traces: host-network Beyla -> `localhost:4318/v1/traces` -> Alloy -> Tempo.
- Alloy eBPF CPU profiles: Docker process discovery -> `pyroscope.ebpf` -> Pyroscope.
- Beyla eBPF metrics: Prometheus scrapes `localhost:8999/metrics` through `host.docker.internal:8999`.
- App metrics: Prometheus scrapes `/actuator/prometheus` on service containers.
- Container/node metrics: Prometheus scrapes cAdvisor and node-exporter.
- k6 metrics: Grafana-mode thesis runs enable Prometheus remote-write by default so k6 writes to `http://localhost:9090/api/v1/write`. Use `--no-k6-prometheus-rw` only for diagnostics outside Grafana evidence capture.
- k6 live presentation: use `--k6-web-dashboard`; the dashboard is available at `http://localhost:5665` during the run and exports `k6-dashboard.html` into the run directory.

## Commands

```bash
make up-observability

load-testing/thesis/run-k6-thesis.sh \
  --suite \
  --suite-label grafana-suite-quick \
  --profile quick \
  --k6-web-dashboard
```

The suite command starts the services once, builds images once, and keeps one Grafana/Prometheus/Tempo window for all scenario phases.

## Dashboard Templates

Grafana provisions these focused thesis dashboards from `grafana/dashboards/`:

| Dashboard | Purpose |
|---|---|
| `Saga Thesis Defense Overview` | top-level presentation map and quick status |
| `Saga Thesis Capture Template` | screenshot-first Bab IV capture contract with metrics, Tempo, Beyla, and Pyroscope guidance |
| `Saga Thesis Snapshot - All Evidence` | snapshot-friendly all-in-one public thesis evidence view for the clean comparison, scalability, and resilience rerun |
| `Saga k6 Throughput Analysis` | k6-first TPS, HTTP RPS, terminal correctness, outcome matching, and client latency |
| `Saga Comparative Analysis` | choreography vs orchestration throughput/latency/correctness comparison by scenario |
| `Saga Root Cause Analysis` | Tempo spanmetrics, Beyla network calls, service histograms, CPU, and memory |
| `Saga Evidence Health` | verifies k6 remote-write, labels, targets, spanmetrics, Beyla, and app metrics before screenshots |
| `Saga Secondary Suite Analysis` | supplemental scalability/resilience view with counter-derived terminal ratios so saturation phases stay visible |

Use the same variables across dashboards: `suite_label`, `pattern`, `scenario`, and `run_label`.

## Notes

- Beyla and Alloy run with host PID/eBPF privileges because this is a local benchmark stack that needs eBPF visibility into Dockerized services.
- Tempo enables the `local-blocks` metrics-generator processor so TraceQL metrics queries such as `| rate() by(resource.service.name)` work for recent traces.
- Pyroscope receives saga service CPU profiles from Alloy eBPF discovery. Grafana links Tempo traces to Pyroscope by mapping trace `service.name` to Pyroscope `service_name`.
- The k6 runner verifies after every Grafana-mode phase that Prometheus contains iteration, HTTP, order outcome, success-rate, outcome-match, and latency trend series for the phase `run_label`.
- This stack observes service/network behavior; it should not be described as complete syscall tracing.
- The default Prometheus config scrapes the scale-1 orchestration order service on `8091`. Do not add optional order replicas `8095-8097` unless the scaled orchestration profile is actually running, otherwise target health will correctly show those non-running targets as down.
