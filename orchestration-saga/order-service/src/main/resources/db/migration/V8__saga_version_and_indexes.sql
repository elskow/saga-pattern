-- Add version column for optimistic locking on saga_instances
ALTER TABLE saga_instances ADD COLUMN IF NOT EXISTS version BIGINT DEFAULT 0;

-- Add index on shipments.order_id column for shipping-service
-- Note: This migration is for order-service; shipping-service has its own migrations
