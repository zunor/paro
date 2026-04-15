-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS spill_order;
DROP TABLE IF EXISTS spill_window;
DROP TABLE IF EXISTS spill_agg;
DROP TABLE IF EXISTS spill_join_l;
DROP TABLE IF EXISTS spill_join_r;

SET memory_limit = '32MB';
SET temp_directory = '/tmp/paro_benchmark_spill';
SET max_temp_directory_size = DEFAULT;
SET force_external = true;

CREATE TABLE spill_order(id INT, payload INT, payload_text VARCHAR);
INSERT INTO spill_order
SELECT
    g,
    ${order_rows} - g + 1,
    'mid_' || CAST(g % 997 AS VARCHAR) || '_x_x_x_x_x_x_x_x'
FROM generate_series(1, ${order_rows}) AS t(g);

INSERT INTO spill_order VALUES
    (100001, -1, 'zzzz_payload_6'),
    (100002, -2, 'zzzz_payload_5'),
    (100003, -3, 'zzzz_payload_4'),
    (100004, -4, 'zzzz_payload_3'),
    (100005, -5, 'zzzz_payload_2'),
    (100006, -6, 'zzzz_payload_1');

CREATE TABLE spill_window(part INT, id INT, score INT);
INSERT INTO spill_window
SELECT (g % 8) + 1, g, 100000 - (g % 251)
FROM generate_series(1, ${window_rows}) AS t(g);

CREATE TABLE spill_agg(k1 INT, k2 INT, v INT);
INSERT INTO spill_agg
SELECT g, g % 257, 1
FROM generate_series(1, ${agg_rows}) AS t(g);

CREATE TABLE spill_join_l(id INT);
CREATE TABLE spill_join_r(id INT);
INSERT INTO spill_join_l
SELECT g
FROM generate_series(1, ${join_rows}) AS t(g);
INSERT INTO spill_join_r
SELECT g
FROM generate_series(1, ${join_rows}) AS t(g);
