# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

DROP INDEX IF EXISTS idx_art_orders_customer_id;
DROP TABLE IF EXISTS art_orders;

CREATE TABLE art_orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    status VARCHAR
);

INSERT INTO art_orders VALUES
    (1, 10, 'new'),
    (2, 20, 'paid'),
    (3, 20, 'shipped'),
    (4, 30, 'paid');

CREATE INDEX idx_art_orders_customer_id ON art_orders (customer_id);

SELECT index_name, index_type, build_state, entry_count
FROM paro_indexes()
WHERE table_name = 'art_orders'
ORDER BY index_name;

CREATE INDEX IF NOT EXISTS idx_art_orders_customer_id ON art_orders (customer_id);

SELECT order_id, status
FROM art_orders
WHERE customer_id = 20
ORDER BY order_id;

DROP INDEX idx_art_orders_customer_id;

SELECT COUNT(*) AS remaining_indexes
FROM paro_indexes()
WHERE table_name = 'art_orders';

DROP TABLE art_orders;
