-- Products table for inventory management
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    reserved_quantity INT,
    version BIGINT
);

-- Inventory reservations table
CREATE TABLE IF NOT EXISTS inventory_reservations (
    reservation_id VARCHAR(255) PRIMARY KEY,
    order_id VARCHAR(255) NOT NULL,
    items_json TEXT,
    status VARCHAR(50) NOT NULL,
    failure_reason TEXT,
    release_reason TEXT,
    created_at TIMESTAMP,
    reserved_at TIMESTAMP,
    released_at TIMESTAMP,
    version BIGINT
);

-- Unique constraint to ensure one reservation per order
CREATE UNIQUE INDEX IF NOT EXISTS idx_reservations_order_id ON inventory_reservations(order_id);
-- Index for status queries
CREATE INDEX IF NOT EXISTS idx_reservations_status ON inventory_reservations(status);
