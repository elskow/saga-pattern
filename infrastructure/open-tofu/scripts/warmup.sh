#!/bin/bash
# Warmup Script
# Usage: ./warmup.sh <host> <port> <requests>

HOST="${1:-192.168.11.152}"
PORT="${2:-8081}"
REQUESTS="${3:-100}"

echo "Warming up http://$HOST:$PORT with $REQUESTS requests..."

for i in $(seq 1 $REQUESTS); do
    curl -sf -X POST "http://$HOST:$PORT/api/orders" \
        -H "Content-Type: application/json" \
        -d '{"productId":"PROD-001","quantity":1,"customerId":"WARMUP"}' > /dev/null 2>&1 || true
    
    if [ $((i % 20)) -eq 0 ]; then
        echo -n "."
    fi
done
echo ""
echo "Warmup complete!"
