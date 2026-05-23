ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS tracking_number VARCHAR(255) NOT NULL DEFAULT '';

UPDATE orders
SET tracking_number = shipment_id
WHERE tracking_number = '' AND shipment_id IS NOT NULL;
