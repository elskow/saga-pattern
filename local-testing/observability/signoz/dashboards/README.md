# Saga Comparison SigNoz Dashboard

This directory contains a SigNoz dashboard JSON for comparing choreography and orchestration saga behavior.

## Files

- `saga-comparison-dashboard.json`: importable SigNoz dashboard asset.
- `load-saga-dashboard.sh`: validates local SigNoz health and prints the exact import steps.

## Load Locally

```bash
make up-dual-local
./local-testing/observability/signoz/dashboards/load-saga-dashboard.sh
```

Then open `http://localhost:8080`, go to Dashboards, choose `+ New dashboard`, select `Import JSON`, and upload `saga-comparison-dashboard.json`. If you previously imported an older copy, delete that dashboard or import this JSON as a new dashboard; SigNoz does not automatically refresh imported dashboards from files.

## What It Shows

- order created/completed/failed rates by saga pattern
- success and compensation ratios
- choreography processing time vs orchestration total duration
- orchestration framework step duration by step/direction/result
- compensation breakdown by payment, inventory, and shipping
- recent trace breadcrumbs using safe attributes such as `saga.phase`, `saga.outcome`, `failure.type`, `request.id`, and `order.id`

The dashboard intentionally avoids raw payloads, shipping addresses, product names, tracking numbers, and raw free-text failure reasons.
