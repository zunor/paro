-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS baseline_scan;
DROP TABLE IF EXISTS baseline_int_l;
DROP TABLE IF EXISTS baseline_int_r;
DROP TABLE IF EXISTS baseline_string_l;
DROP TABLE IF EXISTS baseline_string_r;
DROP TABLE IF EXISTS baseline_range_l;
DROP TABLE IF EXISTS baseline_range_r;

SET force_external = DEFAULT;
SET memory_limit = '${memory_limit}';
SET temp_directory = '${temp_directory}';
SET max_temp_directory_size = DEFAULT;

CREATE TABLE baseline_scan(
  id INT,
  selectivity_key INT,
  flag INT,
  agg_key INT,
  sort_score INT,
  payload VARCHAR
);

INSERT INTO baseline_scan
SELECT
  g,
  g % ${scan_key_space},
  g % 10,
  g % ${agg_groups},
  ${scan_rows} - g + 1,
  'payload_' || CAST(g AS VARCHAR)
FROM generate_series(1, ${scan_rows}) AS t(g);

CREATE TABLE baseline_int_l(id INT, payload INT);
CREATE TABLE baseline_int_r(id INT, payload INT);
INSERT INTO baseline_int_l
SELECT g, g * 3
FROM generate_series(1, ${int_join_rows}) AS t(g);
INSERT INTO baseline_int_r
SELECT g, g * 5
FROM generate_series(1, ${int_join_rows}) AS t(g);

CREATE TABLE baseline_string_l(key_text VARCHAR, payload INT);
CREATE TABLE baseline_string_r(key_text VARCHAR, payload INT);
INSERT INTO baseline_string_l
SELECT 'key_' || CAST(g AS VARCHAR), g * 7
FROM generate_series(1, ${string_join_rows}) AS t(g);
INSERT INTO baseline_string_r
SELECT 'key_' || CAST(g AS VARCHAR), g * 11
FROM generate_series(1, ${string_join_rows}) AS t(g);

CREATE TABLE baseline_range_l(lo INT, hi INT, lo_limit INT, payload INT);
CREATE TABLE baseline_range_r(lo INT, hi INT, payload INT);
INSERT INTO baseline_range_l
SELECT
  g,
  ${ie_rows} - g,
  CASE WHEN g + 16 > 128 THEN 128 ELSE g + 16 END,
  g
FROM generate_series(1, ${ie_rows}) AS t(g);
INSERT INTO baseline_range_r
SELECT g, ${ie_rows} - g, g
FROM generate_series(1, ${ie_rows}) AS t(g);
