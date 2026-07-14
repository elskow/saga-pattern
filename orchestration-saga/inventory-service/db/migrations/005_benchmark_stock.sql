UPDATE products
SET quantity = GREATEST(quantity, 10000),
    last_restocked_at = COALESCE(last_restocked_at, NOW())
WHERE product_id IN ('PROD-001', 'PROD-002', 'PROD-003', 'PROD-004', 'PROD-005', 'PROD-006', 'PROD-007', 'PROD-008');
