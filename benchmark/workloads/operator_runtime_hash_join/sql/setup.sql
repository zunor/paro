-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS hash_join_i64_probe;
DROP TABLE IF EXISTS hash_join_i64_build;
DROP TABLE IF EXISTS hash_join_long_probe;
DROP TABLE IF EXISTS hash_join_long_build;
DROP TABLE IF EXISTS hash_join_null_probe;
DROP TABLE IF EXISTS hash_join_null_build;
DROP TABLE IF EXISTS hash_join_string_probe;
DROP TABLE IF EXISTS hash_join_string_build;
DROP TABLE IF EXISTS hash_join_right_probe;
DROP TABLE IF EXISTS hash_join_right_build;

SET force_external = DEFAULT;
SET memory_limit = '${memory_limit}';
SET temp_directory = '${temp_directory}';
SET max_temp_directory_size = DEFAULT;

CREATE TABLE hash_join_i64_probe(k BIGINT, s VARCHAR, payload BIGINT);
CREATE TABLE hash_join_i64_build(k BIGINT, s VARCHAR, payload BIGINT);
INSERT INTO hash_join_i64_probe
SELECT g, 'k_' || CAST(g % 4096 AS VARCHAR), g * 11
FROM generate_series(1, ${rows}) AS t(g);
INSERT INTO hash_join_i64_build
SELECT g, 'k_' || CAST(g % 4096 AS VARCHAR), g * 13
FROM generate_series(1, ${rows}) AS t(g);

CREATE TABLE hash_join_long_probe(k INT, payload INT);
CREATE TABLE hash_join_long_build(k INT, payload INT);
INSERT INTO hash_join_long_probe
SELECT g % ${long_key_space}, g
FROM generate_series(1, ${long_rows}) AS t(g);
INSERT INTO hash_join_long_build
SELECT g % ${long_key_space}, g * 7
FROM generate_series(1, ${long_rows}) AS t(g);

CREATE TABLE hash_join_null_probe(k INT, payload INT);
CREATE TABLE hash_join_null_build(k INT, payload INT);
INSERT INTO hash_join_null_probe
SELECT CASE WHEN g % 2 = 0 THEN NULL ELSE g END, g
FROM generate_series(1, ${null_rows}) AS t(g);
INSERT INTO hash_join_null_build
SELECT CASE WHEN g % 2 = 0 THEN NULL ELSE g END, g * 17
FROM generate_series(1, ${null_rows}) AS t(g);

CREATE TABLE hash_join_string_probe(k VARCHAR, payload INT);
CREATE TABLE hash_join_string_build(k VARCHAR, payload INT);
INSERT INTO hash_join_string_probe
SELECT 'key_' || CAST(g AS VARCHAR), g
FROM generate_series(1, ${string_rows}) AS t(g);
INSERT INTO hash_join_string_build
SELECT 'key_' || CAST(g AS VARCHAR), g * 19
FROM generate_series(1, ${string_rows}) AS t(g);

CREATE TABLE hash_join_right_probe(k INT, payload INT);
CREATE TABLE hash_join_right_build(k INT, payload INT);
INSERT INTO hash_join_right_probe
SELECT g, g
FROM generate_series(1, 999) AS t(g);
INSERT INTO hash_join_right_build
SELECT g, g * 23
FROM generate_series(1, 1000) AS t(g);
