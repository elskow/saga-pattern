-- Saga instance state persistence table
CREATE TABLE IF NOT EXISTS saga_instances (
    saga_id VARCHAR(255) PRIMARY KEY,
    order_id VARCHAR(255) NOT NULL UNIQUE,
    current_state VARCHAR(50) NOT NULL,
    saga_data_json TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for finding saga by order ID
CREATE INDEX IF NOT EXISTS idx_saga_order_id ON saga_instances(order_id);

-- Index for finding stale sagas by state and update time
CREATE INDEX IF NOT EXISTS idx_saga_state_updated ON saga_instances(current_state, updated_at);

-- Processed commands table for idempotency
CREATE TABLE IF NOT EXISTS processed_commands (
    command_id VARCHAR(255) PRIMARY KEY,
    order_id VARCHAR(255) NOT NULL,
    command_type VARCHAR(50) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for finding commands by order ID
CREATE INDEX IF NOT EXISTS idx_processed_commands_order ON processed_commands(order_id);
