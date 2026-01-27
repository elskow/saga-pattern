-- Add version column for optimistic locking on outbox_commands
ALTER TABLE outbox_commands ADD COLUMN IF NOT EXISTS version BIGINT DEFAULT 0;
