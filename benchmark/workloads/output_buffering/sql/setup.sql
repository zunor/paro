-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Setup tables for output buffering and memory verification benchmarks.
-- Tests fetch-driven bounded output vs completed-output unbounded paths.

DROP TABLE IF EXISTS buf_scan;
DROP TABLE IF EXISTS buf_join_l;
DROP TABLE IF EXISTS buf_join_r;
DROP TABLE IF EXISTS buf_agg;

CREATE TABLE buf_scan(id INT, v1 INT, v2 INT, payload VARCHAR);
INSERT INTO buf_scan
SELECT g, g % 1000, g * 7, 'row_' || CAST(g AS VARCHAR)
FROM generate_series(1, ${scan_rows}) AS t(g);

CREATE TABLE buf_join_l(id INT, fk INT, val INT);
INSERT INTO buf_join_l
SELECT g, (g % ${join_right_rows}) + 1, g * 3
FROM generate_series(1, ${join_left_rows}) AS t(g);

CREATE TABLE buf_join_r(id INT, payload INT);
INSERT INTO buf_join_r
SELECT g, g * 11
FROM generate_series(1, ${join_right_rows}) AS t(g);

CREATE TABLE buf_agg(grp INT, v INT);
INSERT INTO buf_agg
SELECT g % ${agg_groups}, g
FROM generate_series(1, ${agg_rows}) AS t(g);
