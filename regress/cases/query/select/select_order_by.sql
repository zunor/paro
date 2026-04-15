-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS order_test;

-- @setup
DROP TABLE IF EXISTS order_null_test;

-- @setup
DROP TABLE IF EXISTS order_payload_test;

-- @setup
CREATE TABLE order_test (id INT, name VARCHAR, score INT);

INSERT INTO order_test VALUES (3, 'C', 70), (1, 'A', 90), (2, 'B', 80);

-- Order by ID (asc)
SELECT * FROM order_test ORDER BY id;

-- Order by score (desc)
SELECT * FROM order_test ORDER BY score DESC;

-- Order by name
SELECT * FROM order_test ORDER BY name;

CREATE TABLE order_null_test (id INT, grp INT, label VARCHAR);

INSERT INTO order_null_test VALUES
    (1, 1, 'alpha'),
    (2, 1, NULL),
    (3, 2, 'beta'),
    (4, NULL, 'delta'),
    (5, 1, 'gamma'),
    (6, NULL, NULL),
    (7, 2, 'beta'),
    (8, 1, 'alpha');

SELECT id, grp, label
FROM order_null_test
ORDER BY grp ASC NULLS LAST, label DESC NULLS FIRST, id ASC;

SELECT id, grp, label
FROM order_null_test
ORDER BY grp DESC NULLS FIRST, label ASC NULLS LAST, id DESC;

CREATE TABLE order_payload_test (id INT, category INT, sort_key VARCHAR, payload VARCHAR);

INSERT INTO order_payload_test VALUES
    (1, 10, 'pear', 'payload_short'),
    (2, 10, 'banana', 'payload_medium_bbbbb'),
    (3, 20, 'banana', 'payload_long_cccccccccccccc'),
    (4, 20, 'zebra', 'payload_x'),
    (5, 10, 'apple', 'payload_very_long_dddddddddddddddddd'),
    (6, 30, 'banana', NULL);

SELECT id, category, sort_key, payload
FROM order_payload_test
ORDER BY sort_key ASC NULLS LAST, id DESC;

SELECT id, category, sort_key
FROM order_payload_test
ORDER BY category ASC, sort_key DESC NULLS LAST, id ASC;

-- @teardown
DROP TABLE IF EXISTS order_test;

-- @teardown
DROP TABLE IF EXISTS order_null_test;

-- @teardown
DROP TABLE IF EXISTS order_payload_test;
