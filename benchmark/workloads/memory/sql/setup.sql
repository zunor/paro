-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS memory_order;
DROP TABLE IF EXISTS memory_window;
DROP TABLE IF EXISTS memory_agg;
DROP TABLE IF EXISTS memory_join_l;
DROP TABLE IF EXISTS memory_join_r;

SET memory_limit = '32MB';
SET temp_directory = '/tmp/paro_benchmark_memory';
SET max_temp_directory_size = DEFAULT;
SET force_external = true;

CREATE TABLE memory_order(id INT, payload INT, payload_text VARCHAR);
INSERT INTO memory_order
SELECT
    g,
    ${order_rows} - g + 1,
    'mid_' || CAST(g % 997 AS VARCHAR) || '_x_x_x_x_x_x_x_x'
FROM generate_series(1, ${order_rows}) AS t(g);

INSERT INTO memory_order VALUES
    (100001, -1, 'zzzz_payload_12'),
    (100002, -2, 'zzzz_payload_11'),
    (100003, -3, 'zzzz_payload_10'),
    (100004, -4, 'zzzz_payload_9'),
    (100005, -5, 'zzzz_payload_8'),
    (100006, -6, 'zzzz_payload_7'),
    (100007, -7, 'zzzz_payload_6'),
    (100008, -8, 'zzzz_payload_5'),
    (100009, -9, 'zzzz_payload_4'),
    (100010, -10, 'zzzz_payload_3'),
    (100011, -11, 'zzzz_payload_2'),
    (100012, -12, 'zzzz_payload_1');

CREATE TABLE memory_window(part INT, id INT, score INT);
INSERT INTO memory_window
SELECT (g % 8) + 1, g, 100000 - (g % 251)
FROM generate_series(1, ${window_rows}) AS t(g);

CREATE TABLE memory_agg(k1 INT, k2 INT, v INT);
INSERT INTO memory_agg
SELECT g, g % 257, 1
FROM generate_series(1, ${agg_rows}) AS t(g);

CREATE TABLE memory_join_l(id INT);
CREATE TABLE memory_join_r(id INT);
INSERT INTO memory_join_l
SELECT g
FROM generate_series(1, ${join_rows}) AS t(g);
INSERT INTO memory_join_r
SELECT g
FROM generate_series(1, ${join_rows}) AS t(g);
