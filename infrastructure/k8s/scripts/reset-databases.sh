#!/bin/bash
# Reset all databases to clean state for testing
#
# This script truncates all tables in all 4 databases.
# Services will re-initialize test data on next startup.
#
# Usage: ./reset-databases.sh

set -e

echo "=== Database Reset ==="
echo ""
echo "This will truncate all tables in order_db, payment_db, inventory_db, and shipping_db"
echo ""

# Check if kubectl is configured
if ! kubectl cluster-info &>/dev/null; then
    echo "Error: kubectl is not configured or cluster is not reachable"
    exit 1
fi

# Check if infrastructure is running
if ! kubectl get statefulset order-db -n saga-infra &>/dev/null; then
    echo "Error: Infrastructure not deployed. Deploy with: kubectl apply -k base/infrastructure"
    exit 1
fi

# Wait for all database pods to be ready
echo "Checking database pods..."
for db in order-db payment-db inventory-db shipping-db; do
    kubectl wait --for=condition=ready pod/${db}-0 -n saga-infra --timeout=60s
done

echo ""
echo "Resetting databases..."

# Reset order_db
echo "  - order_db..."
kubectl exec -n saga-infra order-db-0 -- psql -U postgres -d order_db -c "
DO \$\$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
    -- Also truncate eventuate schema if exists (for orchestration)
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'eventuate') LOOP
        EXECUTE 'TRUNCATE TABLE eventuate.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END \$\$;
" 2>/dev/null || true

# Reset payment_db
echo "  - payment_db..."
kubectl exec -n saga-infra payment-db-0 -- psql -U postgres -d payment_db -c "
DO \$\$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'eventuate') LOOP
        EXECUTE 'TRUNCATE TABLE eventuate.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END \$\$;
" 2>/dev/null || true

# Reset inventory_db
echo "  - inventory_db..."
kubectl exec -n saga-infra inventory-db-0 -- psql -U postgres -d inventory_db -c "
DO \$\$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'eventuate') LOOP
        EXECUTE 'TRUNCATE TABLE eventuate.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END \$\$;
" 2>/dev/null || true

# Reset shipping_db
echo "  - shipping_db..."
kubectl exec -n saga-infra shipping-db-0 -- psql -U postgres -d shipping_db -c "
DO \$\$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
        EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'eventuate') LOOP
        EXECUTE 'TRUNCATE TABLE eventuate.' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END \$\$;
" 2>/dev/null || true

echo ""
echo "=== Reset Complete ==="
echo ""
echo "All databases have been reset."
echo "Services will re-initialize test data (products) on next startup."
echo ""
echo "To restart services and reinitialize data:"
echo "  kubectl rollout restart deployment -n saga-test"
