#!/bin/bash
# Usage: ./warmup.sh <host> <port> <requests>

HOST="${1:-192.168.11.152}"
PORT="${2:-8081}"
REQUESTS="${3:-100}"

if [ "$PORT" -ge 8085 ] 2>/dev/null; then
    PATTERN="orchestration"
else
    PATTERN="choreography"
fi

echo "Warming up http://$HOST:$PORT ($PATTERN) with $REQUESTS requests..."

for i in $(seq 1 $REQUESTS); do
    if [ "$PATTERN" = "orchestration" ]; then
        BODY='{"customerId":"WARMUP-'"$i"'","shippingAddress":"123 Warmup St","totalAmount":49.99,"items":[{"productId":"PROD-001","productName":"Laptop","quantity":1,"price":49.99}]}'
    else
        BODY='{"customerId":"WARMUP-'"$i"'","shippingAddress":"123 Warmup St","items":[{"productId":"PROD-001","productName":"Laptop","quantity":1,"price":49.99}]}'
    fi

    curl -sf -X POST "http://$HOST:$PORT/api/orders" \
        -H "Content-Type: application/json" \
        -d "$BODY" > /dev/null 2>&1 || true

    if [ $((i % 20)) -eq 0 ]; then
        echo -n "."
    fi
done
echo ""
echo "Warmup complete!"
