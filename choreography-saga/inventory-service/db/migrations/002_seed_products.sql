INSERT INTO products (product_id, product_name, quantity_available, quantity_reserved)
VALUES
    ('PROD-001', 'Laptop', 100, 0),
    ('PROD-002', 'Smartphone', 200, 0),
    ('PROD-003', 'Headphones', 500, 0),
    ('PROD-004', 'Keyboard', 150, 0),
    ('PROD-005', 'Monitor', 120, 0)
ON CONFLICT (product_id) DO NOTHING;
