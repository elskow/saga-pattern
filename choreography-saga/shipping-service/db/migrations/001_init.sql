CREATE TABLE IF NOT EXISTS pending_addresses (
    order_id VARCHAR(255) PRIMARY KEY,
    shipping_address TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shipments (
    shipping_id VARCHAR(255) PRIMARY KEY,
    order_id VARCHAR(255) NOT NULL UNIQUE,
    tracking_number VARCHAR(255) NOT NULL,
    shipping_address TEXT NOT NULL,
    status VARCHAR(50) NOT NULL,
    estimated_delivery TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_key VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);
