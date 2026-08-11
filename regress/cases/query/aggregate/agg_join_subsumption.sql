-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- End-to-end coverage for reusing the filtered partial SUM in a Q18-shaped
-- query. Duplicate preserved keys verify multiplicity; NULL keys and all-NULL
-- groups verify SQL join and aggregate semantics.
-- @setup
DROP TABLE IF EXISTS agg_sub_detail;
DROP TABLE IF EXISTS agg_sub_preserved;
CREATE TABLE agg_sub_detail (
    order_key BIGINT,
    quantity DECIMAL(15, 2)
);
CREATE TABLE agg_sub_preserved (
    order_key BIGINT,
    label VARCHAR
);
INSERT INTO agg_sub_detail VALUES
    (1, 40.00),
    (1, 70.00),
    (2, 60.00),
    (2, 30.00),
    (3, NULL),
    (3, NULL),
    (4, 150.00),
    (NULL, 200.00);
INSERT INTO agg_sub_preserved VALUES
    (1, 'duplicate'),
    (1, 'duplicate'),
    (2, 'below-threshold'),
    (3, 'all-null'),
    (4, 'single-row'),
    (NULL, 'null-key');

EXPLAIN SELECT
    p.order_key,
    p.label,
    sum(d.quantity) AS total_quantity
FROM agg_sub_preserved AS p
JOIN agg_sub_detail AS d
  ON d.order_key = p.order_key
WHERE p.order_key IN (
    SELECT order_key
    FROM agg_sub_detail
    GROUP BY order_key
    HAVING sum(quantity) > 100.00
)
GROUP BY p.order_key, p.label
ORDER BY p.order_key;

SELECT
    p.order_key,
    p.label,
    sum(d.quantity) AS total_quantity
FROM agg_sub_preserved AS p
JOIN agg_sub_detail AS d
  ON d.order_key = p.order_key
WHERE p.order_key IN (
    SELECT order_key
    FROM agg_sub_detail
    GROUP BY order_key
    HAVING sum(quantity) > 100.00
)
GROUP BY p.order_key, p.label
ORDER BY p.order_key;

-- @teardown
DROP TABLE IF EXISTS agg_sub_preserved;
DROP TABLE IF EXISTS agg_sub_detail;
