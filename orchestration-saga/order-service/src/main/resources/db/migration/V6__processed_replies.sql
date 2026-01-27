-- Processed replies table for idempotency tracking
CREATE TABLE IF NOT EXISTS processed_replies (
    reply_id VARCHAR(255) PRIMARY KEY,
    order_id VARCHAR(255) NOT NULL,
    reply_type VARCHAR(50) NOT NULL,
    processed_at TIMESTAMP NOT NULL
);

-- Indexes for cleanup and lookup queries
CREATE INDEX IF NOT EXISTS idx_processed_replies_order_id ON processed_replies(order_id);
CREATE INDEX IF NOT EXISTS idx_processed_replies_processed_at ON processed_replies(processed_at);
