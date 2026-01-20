-- Eventuate Tram Saga Schema for Order Service

-- Message table for transactional outbox pattern
CREATE TABLE IF NOT EXISTS message (
    id VARCHAR(1000) PRIMARY KEY,
    destination VARCHAR(1000) NOT NULL,
    headers VARCHAR(1000) NOT NULL,
    payload VARCHAR(10000) NOT NULL,
    published SMALLINT DEFAULT 0,
    creation_time BIGINT
);

CREATE INDEX IF NOT EXISTS message_published_idx ON message(published, id);

-- Saga instance state table (order-service only - orchestrator)
CREATE TABLE IF NOT EXISTS saga_instance (
    saga_type VARCHAR(255) NOT NULL,
    saga_id VARCHAR(255) NOT NULL,
    state_name VARCHAR(255) NOT NULL,
    last_request_id VARCHAR(255),
    saga_data_type VARCHAR(1000) NOT NULL,
    saga_data_json VARCHAR(10000) NOT NULL,
    PRIMARY KEY(saga_type, saga_id)
);

-- Saga participants table (order-service only - orchestrator)
CREATE TABLE IF NOT EXISTS saga_instance_participants (
    saga_type VARCHAR(255) NOT NULL,
    saga_id VARCHAR(255) NOT NULL,
    destination VARCHAR(255) NOT NULL,
    resource VARCHAR(255) NOT NULL,
    PRIMARY KEY(saga_type, saga_id, destination, resource)
);

-- Received messages table for idempotency
CREATE TABLE IF NOT EXISTS received_messages (
    consumer_id VARCHAR(1000) PRIMARY KEY,
    message_id VARCHAR(1000) NOT NULL,
    creation_time BIGINT NOT NULL
);
