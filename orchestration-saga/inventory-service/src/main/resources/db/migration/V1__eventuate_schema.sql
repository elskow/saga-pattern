-- Eventuate Tram Saga Schema for Inventory Service

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

-- Received messages table for idempotency
CREATE TABLE IF NOT EXISTS received_messages (
    consumer_id VARCHAR(1000) PRIMARY KEY,
    message_id VARCHAR(1000) NOT NULL,
    creation_time BIGINT NOT NULL
);
