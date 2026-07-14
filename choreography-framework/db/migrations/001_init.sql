CREATE TABLE IF NOT EXISTS scheduler_leases (
    name VARCHAR(255) PRIMARY KEY,
    owner VARCHAR(255) NOT NULL,
    leased_until TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_messages (
    id VARCHAR(255) PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    message_key VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload_json TEXT NOT NULL,
    status VARCHAR(32) NOT NULL,
    available_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    attempt_count INT NOT NULL,
    max_attempts INT NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    sent_at TIMESTAMP,
    claimed_by VARCHAR(255),
    claimed_until TIMESTAMP,
    trace_headers_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_pending ON outbox_messages(status, available_at, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_messages_cleanup ON outbox_messages(status, updated_at);

CREATE TABLE IF NOT EXISTS processed_events (
    event_key VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_processed_events_cleanup ON processed_events(processed_at);
