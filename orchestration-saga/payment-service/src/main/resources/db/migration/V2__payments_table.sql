-- Payments table for storing payment information
CREATE TABLE IF NOT EXISTS payments (
    payment_id VARCHAR(255) PRIMARY KEY,
    version BIGINT,
    order_id VARCHAR(255) NOT NULL,
    customer_id VARCHAR(255) NOT NULL,
    amount DECIMAL(19,2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    failure_reason TEXT,
    refund_reason TEXT,
    created_at TIMESTAMP,
    processed_at TIMESTAMP,
    refunded_at TIMESTAMP
);

-- Unique constraint to ensure one payment per order
CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id);
-- Index for status queries
CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status);
