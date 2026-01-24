-- Add status column to processed commands for better send consistency
ALTER TABLE processed_commands
    ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'SENT';

-- Backfill existing rows (if any) to SENT
UPDATE processed_commands
SET status = 'SENT'
WHERE status IS NULL;

-- Index for status lookups
CREATE INDEX IF NOT EXISTS idx_processed_commands_status
    ON processed_commands(status);
