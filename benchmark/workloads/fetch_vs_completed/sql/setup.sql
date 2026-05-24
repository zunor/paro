-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Setup for fetch-driven vs completed-output comparison benchmark.
-- Uses same data for both paths to enable direct latency/memory comparison.

DROP TABLE IF EXISTS cmp_scan;
DROP TABLE IF EXISTS cmp_join_l;
DROP TABLE IF EXISTS cmp_join_r;
DROP TABLE IF EXISTS cmp_agg;

CREATE TABLE cmp_scan(id INT, v1 INT, v2 INT, payload VARCHAR);
INSERT INTO cmp_scan
SELECT g, g % 1000, g * 7, 'payload_' || CAST(g AS VARCHAR)
FROM generate_series(1, ${rows}) AS t(g);

CREATE TABLE cmp_join_l(id INT, fk INT, val INT);
INSERT INTO cmp_join_l
SELECT g, g % ${join_right}, g * 3
FROM generate_series(1, ${join_left}) AS t(g);

CREATE TABLE cmp_join_r(id INT, payload INT);
INSERT INTO cmp_join_r
SELECT g, g * 11
FROM generate_series(1, ${join_right}) AS t(g);

CREATE TABLE cmp_agg(grp INT, v INT);
INSERT INTO cmp_agg
SELECT g % ${agg_groups}, g
FROM generate_series(1, ${rows}) AS t(g);
