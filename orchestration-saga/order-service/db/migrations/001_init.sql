CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    total_amount DECIMAL(19,2) NOT NULL,
    shipping_address VARCHAR(255) NOT NULL,
    items_json TEXT,
    status VARCHAR(50) NOT NULL,
    payment_id VARCHAR(255),
    reservation_id VARCHAR(255),
    shipment_id VARCHAR(255),
    failure_reason TEXT,
    failure_step VARCHAR(50) NOT NULL DEFAULT '',
    compensated_steps_json TEXT NOT NULL DEFAULT '[]',
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
