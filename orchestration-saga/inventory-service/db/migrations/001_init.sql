CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    reserved_quantity INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS inventory_reservations (
    reservation_id VARCHAR(255) PRIMARY KEY,
    order_id VARCHAR(255) NOT NULL,
    items_json TEXT NOT NULL,
    status VARCHAR(50) NOT NULL,
    failure_reason TEXT,
    release_reason TEXT,
    created_at TIMESTAMP NOT NULL,
    reserved_at TIMESTAMP,
    released_at TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_reservations_order_id ON inventory_reservations(order_id);
CREATE INDEX IF NOT EXISTS idx_inventory_reservations_status ON inventory_reservations(status);
