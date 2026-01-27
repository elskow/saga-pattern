-- Shipments table for shipping management
CREATE TABLE IF NOT EXISTS shipments (
    shipment_id VARCHAR(255) PRIMARY KEY,
    version BIGINT,
    order_id VARCHAR(255) NOT NULL,
    shipping_address VARCHAR(255) NOT NULL,
    tracking_number VARCHAR(255),
    status VARCHAR(50) NOT NULL,
    failure_reason TEXT,
    cancellation_reason TEXT,
    created_at TIMESTAMP,
    scheduled_at TIMESTAMP,
    cancelled_at TIMESTAMP
);

-- Unique constraint to ensure one shipment per order
CREATE UNIQUE INDEX IF NOT EXISTS idx_shipments_order_id ON shipments(order_id);
-- Index for status queries
CREATE INDEX IF NOT EXISTS idx_shipments_status ON shipments(status);
