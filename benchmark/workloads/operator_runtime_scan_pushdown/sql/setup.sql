-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS scan_pushdown_rows;
DROP TABLE IF EXISTS scan_pushdown_wide;

SET memory_limit = '${memory_limit}';
SET temp_directory = '${temp_directory}';
SET max_temp_directory_size = DEFAULT;

CREATE TABLE scan_pushdown_rows(
  id INT,
  filter_key INT,
  nullable_key INT,
  score INT,
  payload VARCHAR
);

INSERT INTO scan_pushdown_rows
SELECT
  g,
  g % ${scan_key_space},
  CASE WHEN g % 10 = 0 THEN NULL ELSE g % 100 END,
  ${scan_rows} - g + 1,
  'payload_' || CAST((g * 1103515245 + ${random_seed}) % 1000003 AS VARCHAR)
FROM generate_series(1, ${scan_rows}) AS t(g);

CREATE TABLE scan_pushdown_wide(
  id INT,
  filter_key INT,
  payload_0 BIGINT,
  payload_1 BIGINT,
  payload_2 BIGINT,
  payload_3 BIGINT,
  payload_4 BIGINT,
  payload_5 BIGINT,
  payload_6 BIGINT,
  payload_7 BIGINT,
  payload_8 BIGINT,
  payload_9 BIGINT
);

INSERT INTO scan_pushdown_wide
SELECT
  g,
  g % ${scan_key_space},
  g,
  g * 3,
  g * 5,
  g * 7,
  g * 11,
  g * 13,
  g * 17,
  g * 19,
  g * 23,
  g * 29
FROM generate_series(1, ${scan_rows}) AS t(g);
