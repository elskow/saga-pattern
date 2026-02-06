-- ShedLock table for distributed scheduler locking
CREATE TABLE IF NOT EXISTS shedlock (
    name VARCHAR(64) NOT NULL,
    lock_until TIMESTAMP NOT NULL,
    locked_at TIMESTAMP NOT NULL,
    locked_by VARCHAR(255) NOT NULL,
    PRIMARY KEY (name)
);

-- Saga framework tables (if not already created by JPA)
CREATE TABLE IF NOT EXISTS saga_instance (
    id VARCHAR(255) NOT NULL,
    saga_id VARCHAR(255) NOT NULL,
    saga_type VARCHAR(255) NOT NULL,
    current_state VARCHAR(255) NOT NULL,
    context_json TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    version BIGINT DEFAULT 0,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_saga_instance_saga_id_type ON saga_instance(saga_id, saga_type);
CREATE INDEX IF NOT EXISTS idx_saga_instance_type_state ON saga_instance(saga_type, current_state);
CREATE INDEX IF NOT EXISTS idx_saga_instance_updated ON saga_instance(updated_at);

CREATE TABLE IF NOT EXISTS saga_outbox (
    id VARCHAR(255) NOT NULL,
    saga_id VARCHAR(255) NOT NULL,
    saga_type VARCHAR(255) NOT NULL,
    command_type VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    payload_json TEXT NOT NULL,
    status VARCHAR(50) NOT NULL,
    attempts INT DEFAULT 0,
    last_error VARCHAR(1000),
    last_attempt_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    version BIGINT DEFAULT 0,
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_saga_outbox_status_created ON saga_outbox(status, created_at);
CREATE INDEX IF NOT EXISTS idx_saga_outbox_saga_id ON saga_outbox(saga_id);
CREATE INDEX IF NOT EXISTS idx_saga_outbox_status_attempt ON saga_outbox(status, last_attempt_at);

CREATE TABLE IF NOT EXISTS saga_processed_messages (
    id VARCHAR(255) NOT NULL,
    saga_id VARCHAR(255) NOT NULL,
    saga_type VARCHAR(255) NOT NULL,
    message_type VARCHAR(255) NOT NULL,
    message_id VARCHAR(255) NOT NULL,
    processed_at TIMESTAMP NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT uk_processed_msg UNIQUE (saga_id, message_type, message_id)
);

CREATE INDEX IF NOT EXISTS idx_processed_msg_saga ON saga_processed_messages(saga_id, saga_type);
CREATE INDEX IF NOT EXISTS idx_processed_msg_time ON saga_processed_messages(processed_at);

CREATE SEQUENCE IF NOT EXISTS saga_processed_messages_id_seq;

ALTER TABLE saga_processed_messages
    ALTER COLUMN id SET DEFAULT nextval('saga_processed_messages_id_seq');
