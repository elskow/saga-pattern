ALTER TABLE products
    ADD COLUMN IF NOT EXISTS visible BOOLEAN NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS last_restocked_at TIMESTAMP;

UPDATE products SET name = 'Keyboard' WHERE product_id = 'PROD-004';


UPDATE products
SET last_restocked_at = COALESCE(last_restocked_at, NOW());
