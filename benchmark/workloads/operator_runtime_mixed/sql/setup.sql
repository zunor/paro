-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS mixed_small;
DROP TABLE IF EXISTS mixed_scan;
DROP TABLE IF EXISTS mixed_join_l;
DROP TABLE IF EXISTS mixed_join_r;
DROP TABLE IF EXISTS mixed_sort;

SET force_external = DEFAULT;
SET memory_limit = '${memory_limit}';
SET temp_directory = '${temp_directory}';
SET max_temp_directory_size = DEFAULT;

CREATE TABLE mixed_small(id INT, v INT);
INSERT INTO mixed_small
SELECT g, g
FROM generate_series(1, ${small_rows}) AS t(g);

CREATE TABLE mixed_scan(
  id INT,
  selectivity_key INT,
  flag INT,
  bucket INT,
  payload VARCHAR
);
INSERT INTO mixed_scan
SELECT
  g,
  g % ${scan_key_space},
  g % 10,
  g % 1000,
  'mixed_' || CAST((g * 1103515245 + ${random_seed}) % 1000003 AS VARCHAR)
FROM generate_series(1, ${scan_rows}) AS t(g);

CREATE TABLE mixed_join_l(id INT, fk INT, payload INT);
CREATE TABLE mixed_join_r(id INT, payload INT);
INSERT INTO mixed_join_l
SELECT g, ((g - 1) % ${join_right_rows}) + 1, g * 17
FROM generate_series(1, ${join_left_rows}) AS t(g);
INSERT INTO mixed_join_r
SELECT g, g * 19
FROM generate_series(1, ${join_right_rows}) AS t(g);

CREATE TABLE mixed_sort(id INT, sort_score INT, payload VARCHAR);
INSERT INTO mixed_sort
SELECT
  g,
  ${sort_rows} - g + 1,
  'sort_' || CAST((g * 48271 + ${random_seed}) % 1000003 AS VARCHAR)
FROM generate_series(1, ${sort_rows}) AS t(g);
