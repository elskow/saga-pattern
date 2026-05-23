CREATE TABLE IF NOT EXISTS shipments (
    shipping_id VARCHAR(255) PRIMARY KEY,
    order_id VARCHAR(255) NOT NULL,
    shipping_address TEXT NOT NULL,
    status VARCHAR(50) NOT NULL,
    failure_reason TEXT,
    cancellation_reason TEXT,
    created_at TIMESTAMP NOT NULL,
    scheduled_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_shipments_order_id ON shipments(order_id);
CREATE INDEX IF NOT EXISTS idx_shipments_status ON shipments(status);
