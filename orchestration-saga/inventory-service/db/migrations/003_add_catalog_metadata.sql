ALTER TABLE products
    ADD COLUMN IF NOT EXISTS description TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS price NUMERIC(10,2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS image VARCHAR(255) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS category VARCHAR(100) NOT NULL DEFAULT '';

UPDATE products SET
    description = 'Blazing-fast performance meets ultra-thin design. 15" OLED display, 32GB RAM, 1TB NVMe SSD.',
    price = 20999000,
    image = '/products/laptop.png',
    category = 'Laptops'
WHERE product_id = 'PROD-001';

UPDATE products SET
    description = '6.7" ProMotion AMOLED display, 200MP camera array, 5000mAh battery. Redefine what a phone can do.',
    price = 13999000,
    image = '/products/phone.png',
    category = 'Smartphones'
WHERE product_id = 'PROD-002';

UPDATE products SET
    description = '40-hour ANC playtime, spatial audio, ultra-soft memory foam earcups. Silence the world.',
    price = 5499000,
    image = '/products/headphones.png',
    category = 'Audio'
WHERE product_id = 'PROD-003';

UPDATE products SET
    description = '75% compact layout, tactile switches, aluminum case, per-key RGB. The keyboard enthusiasts chose.',
    price = 2999000,
    image = '/products/keyboard.png',
    category = 'Peripherals'
WHERE product_id = 'PROD-004';

UPDATE products SET
    description = 'QHD 165Hz 1ms 27" curved VA panel. HDR600, 99% sRGB, USB-C 90W charging. Work and play perfected.',
    price = 8499000,
    image = '/products/monitor.png',
    category = 'Monitors'
WHERE product_id = 'PROD-005';

UPDATE products SET
    description = 'Fitness tracking, heart rate monitoring, and seamless notifications. Your active lifestyle companion.',
    price = 3499000,
    image = '/products/smartwatch.png',
    category = 'Wearables'
WHERE product_id = 'PROD-006';

UPDATE products SET
    description = 'Ergonomic wireless mouse with precision tracking and 70-day battery life. Designed for comfort.',
    price = 1299000,
    image = '/products/mouse.png',
    category = 'Peripherals'
WHERE product_id = 'PROD-007';

UPDATE products SET
    description = '11-inch liquid retina display, powerful M-series chip, all-day battery. Creativity without limits.',
    price = 11999000,
    image = '/products/tablet.png',
    category = 'Tablets'
WHERE product_id = 'PROD-008';
