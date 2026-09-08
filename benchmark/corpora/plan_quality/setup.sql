-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Collector owns a fresh temporary database. No DROP or user schema reuse.
CREATE TABLE quality_vector (id INT PRIMARY KEY, category VARCHAR, emb VECTOR(3));
INSERT INTO quality_vector VALUES
    (1, 'x', '[1,0,0]'), (2, 'x', '[0,1,0]'), (3, 'y', '[0,0,1]'),
    (4, 'y', '[2,0,0]'), (5, 'z', '[-1,0,0]');

CREATE TABLE quality_detail (order_key BIGINT, quantity DECIMAL(15, 2));
INSERT INTO quality_detail VALUES
    (1, 40.00), (1, 70.00), (2, 60.00), (2, 30.00),
    (3, NULL), (3, NULL), (4, 150.00), (NULL, 200.00);
CREATE TABLE quality_preserved (order_key BIGINT, label VARCHAR);
INSERT INTO quality_preserved VALUES
    (1, 'duplicate'), (1, 'duplicate'), (2, 'below-threshold'),
    (3, 'all-null'), (4, 'single-row'), (NULL, 'null-key');

CREATE TABLE quality_customer (customer_key BIGINT, UNIQUE (customer_key) NOT ENFORCED);
INSERT INTO quality_customer VALUES (1), (2), (3);
CREATE TABLE quality_null_customer (customer_key BIGINT, UNIQUE (customer_key) NOT ENFORCED);
INSERT INTO quality_null_customer VALUES (1), (NULL), (NULL);
CREATE TABLE quality_orders (
    order_key BIGINT, customer_key BIGINT, comment VARCHAR,
    UNIQUE (order_key) NOT ENFORCED
);
INSERT INTO quality_orders
SELECT i, (i % 2) + 1,
       CASE WHEN i % 10 = 0 THEN 'special handling requests' ELSE 'ordinary order' END
FROM generate_series(1, 4096) AS generated(i);
