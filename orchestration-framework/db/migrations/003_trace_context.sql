ALTER TABLE outbox_messages
    ADD COLUMN IF NOT EXISTS trace_headers_json TEXT NOT NULL DEFAULT '{}';
