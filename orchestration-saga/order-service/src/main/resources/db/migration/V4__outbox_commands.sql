-- Outbox table for reliable command delivery
CREATE TABLE IF NOT EXISTS outbox_commands (
    outbox_id VARCHAR(255) PRIMARY KEY,
    order_id VARCHAR(255) NOT NULL,
    command_type VARCHAR(50) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    payload_json TEXT NOT NULL,
    status VARCHAR(20) NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_commands(status);
CREATE INDEX IF NOT EXISTS idx_outbox_order ON outbox_commands(order_id);
