#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASHBOARD_JSON="${SCRIPT_DIR}/saga-comparison-dashboard.json"
SIGNOZ_URL="${SIGNOZ_URL:-http://localhost:8080}"

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required to check SigNoz health" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to validate ${DASHBOARD_JSON}" >&2
  exit 1
fi

if [ ! -f "${DASHBOARD_JSON}" ]; then
  echo "Dashboard JSON not found: ${DASHBOARD_JSON}" >&2
  exit 1
fi

jq empty "${DASHBOARD_JSON}"

if ! curl -fsS "${SIGNOZ_URL}/api/v1/health" >/dev/null; then
  echo "SigNoz is not healthy at ${SIGNOZ_URL}" >&2
  echo "Start it with: make up-observability" >&2
  exit 1
fi

cat <<EOF
SigNoz is healthy and the dashboard JSON is valid.

Import the dashboard manually:
1. Open ${SIGNOZ_URL}
2. Go to Dashboards
3. Select + New dashboard
4. Choose Import JSON
5. Upload or paste:
   ${DASHBOARD_JSON}

If you imported an older copy, delete it or import this file as a new dashboard.
SigNoz stores imported dashboards internally; it does not auto-refresh from this file.

After import, generate saga traffic with make up-dual-local plus smoke or k6 thesis commands.
The dashboard uses saga_* Prometheus metrics and a ClickHouse trace table over safe saga attributes.
EOF
