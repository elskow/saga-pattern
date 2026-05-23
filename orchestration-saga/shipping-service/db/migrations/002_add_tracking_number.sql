ALTER TABLE shipments
    ADD COLUMN IF NOT EXISTS tracking_number VARCHAR(255) NOT NULL DEFAULT '';

UPDATE shipments
SET tracking_number = CONCAT('TRK-', LEFT(REPLACE(UPPER(shipping_id), '-', ''), 10))
WHERE tracking_number = '';
