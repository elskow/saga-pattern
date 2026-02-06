-- Add missing indexes to saga_instances table
CREATE INDEX IF NOT EXISTS idx_saga_order_id ON saga_instances(order_id);
CREATE INDEX IF NOT EXISTS idx_saga_state_updated ON saga_instances(current_state, updated_at);
CREATE INDEX IF NOT EXISTS idx_saga_instances_saga_id ON saga_instances(saga_id);
CREATE INDEX IF NOT EXISTS idx_saga_instances_saga_type ON saga_instances(saga_type);

-- Add composite index to outbox_commands for efficient polling
CREATE INDEX IF NOT EXISTS idx_outbox_status_last_attempt ON outbox_commands(status, last_attempt_at);

-- Add index on processed_commands for status queries
CREATE INDEX IF NOT EXISTS idx_processed_commands_status ON processed_commands(status);
