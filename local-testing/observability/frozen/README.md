# Frozen Grafana Observability Stack

This directory preserves the fresh continuous thesis observability window as a separate runnable stack. It is intentionally isolated from the live `saga-grafana` stack so future resets, benchmark reruns, and dashboard experiments do not mutate the frozen evidence backend.

## Evidence Window

| Timezone | From | To |
|---|---|---|
| UTC | `2026-05-18T05:20:32Z` | `2026-05-18T06:37:13Z` |
| Asia/Jakarta | `2026-05-18 12:20:32 WIB` | `2026-05-18 13:37:13 WIB` |

The frozen bundle includes:

- Grafana, Prometheus, Tempo, Pyroscope, and Alloy Docker volume archives.
- All Grafana dashboard JSON files from the live thesis dashboard directory.
- Grafana provisioning, Alloy config, frozen Prometheus config, and frozen Tempo config.
- Fresh k6 result directories for comparison, scalability, and resilience.
- Checksums for the archived data and config snapshots.

## Create The Freeze

Run this before the live 24h retention window expires:

```bash
local-testing/observability/frozen/freeze-grafana-stack.sh
```

It creates:

```text
results/frozen-observability/thesis-20260518-122032-133713/
```

The script only reads the live `saga-grafana_*` Docker volumes. It does not stop containers, remove volumes, prune Docker data, or call `down -v`.

## Start The Frozen Stack

```bash
local-testing/observability/frozen/start-frozen-grafana-stack.sh \
  results/frozen-observability/thesis-20260518-122032-133713
```

URLs:

| Component | URL |
|---|---|
| Grafana | `http://localhost:3300` |
| Prometheus | `http://localhost:9900` |
| Tempo | `http://localhost:3320` |
| Pyroscope | `http://localhost:4404` |

If the frozen volumes already contain data and you intentionally want to restore the archive again, use:

```bash
local-testing/observability/frozen/start-frozen-grafana-stack.sh \
  --replace-frozen-volumes \
  results/frozen-observability/thesis-20260518-122032-133713
```

That replacement mode only clears volumes under compose project `saga-grafana-frozen-thesis-20260517`.

Re-running the normal start command is safe after the first restore. When all frozen volumes already contain data, the script skips restoration and starts the existing frozen stack.

## Why This Is Separate

The live helper script `local-testing/local-runner.sh` has benchmark reset paths that call `down -v` for the live observability compose project. Those commands can delete live `saga-grafana_*` volumes.

The frozen stack avoids that risk by using:

- Compose project: `saga-grafana-frozen-thesis-20260517`
- Separate container names: `grafana-frozen-thesis`, `prometheus-frozen-thesis`, `tempo-frozen-thesis`, `pyroscope-frozen-thesis`, `alloy-frozen-thesis`
- Separate host ports: `3300`, `9900`, `3320`, `4404`, `13245`
- Long frozen retention: Prometheus `100y`, Tempo `876000h` (~100 years)

## Validate Tooling

```bash
local-testing/observability/frozen/tests/verify-frozen-observability.sh
```

After startup, set Grafana's time range to the evidence window above and open any dashboard in the `Saga Thesis` folder.
