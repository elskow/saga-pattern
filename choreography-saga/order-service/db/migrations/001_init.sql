CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    shipping_address TEXT NOT NULL,
    status VARCHAR(50) NOT NULL,
    items_json TEXT NOT NULL,
    total_amount DECIMAL(19,2) NOT NULL,
    payment_id VARCHAR(255),
    reservation_id VARCHAR(255),
    shipping_id VARCHAR(255),
    tracking_number VARCHAR(255),
    failure_reason TEXT,
    correlation_id VARCHAR(255) NOT NULL,
    idempotency_key VARCHAR(255) UNIQUE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_key VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
