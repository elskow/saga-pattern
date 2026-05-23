CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(255) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    quantity_available INT NOT NULL,
    quantity_reserved INT NOT NULL DEFAULT 0,
    last_reservation_at TIMESTAMP,
    last_release_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pending_items (
    order_id VARCHAR(255) PRIMARY KEY,
    items_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory_reservations (
    order_id VARCHAR(255) NOT NULL,
    reservation_id VARCHAR(255) NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    released_at TIMESTAMP,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_key VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);
