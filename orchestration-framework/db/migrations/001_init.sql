CREATE TABLE IF NOT EXISTS scheduler_leases (
    name VARCHAR(255) PRIMARY KEY,
    owner VARCHAR(255) NOT NULL,
    leased_until TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS saga_instances (
    id VARCHAR(255) PRIMARY KEY,
    saga_type VARCHAR(255) NOT NULL,
    state VARCHAR(255) NOT NULL,
    current_step VARCHAR(255) NOT NULL,
    pending_direction VARCHAR(32) NOT NULL,
    pending_command_type VARCHAR(255) NOT NULL,
    pending_reply_type VARCHAR(255) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    deadline_at TIMESTAMP NOT NULL,
    step_deadline_at TIMESTAMP NOT NULL,
    retry_count INT NOT NULL,
    max_retry_count INT NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    data_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_saga_instances_type_state ON saga_instances(saga_type, state);
CREATE INDEX IF NOT EXISTS idx_saga_instances_step_deadline ON saga_instances(step_deadline_at);

CREATE TABLE IF NOT EXISTS saga_step_history (
    id VARCHAR(255) PRIMARY KEY,
    saga_id VARCHAR(255) NOT NULL,
    step VARCHAR(255) NOT NULL,
    direction VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    command_type VARCHAR(255) NOT NULL,
    reply_type VARCHAR(255) NOT NULL,
    attempt INT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    error_message TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_saga_step_history_saga ON saga_step_history(saga_id, created_at, attempt);

CREATE TABLE IF NOT EXISTS outbox_messages (
    id VARCHAR(255) PRIMARY KEY,
    saga_id VARCHAR(255) NOT NULL,
    saga_type VARCHAR(255) NOT NULL,
    step VARCHAR(255) NOT NULL,
    direction VARCHAR(32) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    message_key VARCHAR(255) NOT NULL,
    message_type VARCHAR(255) NOT NULL,
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
    claimed_until TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_pending ON outbox_messages(status, available_at, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_messages_saga ON outbox_messages(saga_id, created_at);

CREATE TABLE IF NOT EXISTS processed_replies (
    reply_id VARCHAR(255) PRIMARY KEY,
    saga_id VARCHAR(255) NOT NULL,
    reply_type VARCHAR(255) NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_processed_replies_expiry ON processed_replies(expires_at);
