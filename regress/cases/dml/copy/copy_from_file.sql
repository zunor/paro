-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS dml_copy_from_src;

-- @setup
DROP TABLE IF EXISTS dml_copy_from_text;

-- @setup
DROP TABLE IF EXISTS dml_copy_from_csv;

-- @setup
DROP TABLE IF EXISTS dml_copy_from_map;

-- @setup
DROP TABLE IF EXISTS dml_copy_from_text_defaults;

-- @setup
DROP TABLE IF EXISTS dml_copy_from_large_src;

-- @setup
DROP TABLE IF EXISTS dml_copy_from_large_dst;

-- @setup
DROP TABLE IF EXISTS dml_copy_from_parallel_dst;

-- @setup
DROP TABLE IF EXISTS dml_copy_from_ndjson;

-- @setup
DROP TABLE IF EXISTS dml_copy_from_ndjson_alias;

-- @setup
DROP TABLE IF EXISTS dml_copy_from_ndjson_large;

-- @setup
DROP TABLE IF EXISTS dml_copy_from_where_csv;

-- @setup
DROP TABLE IF EXISTS dml_copy_from_where_ndjson;

-- @setup
CREATE TABLE dml_copy_from_src (
  id INT,
  name TEXT
);

-- @setup
CREATE TABLE dml_copy_from_text (
  id INT,
  name TEXT
);

-- @setup
CREATE TABLE dml_copy_from_csv (
  id INT,
  name TEXT
);

-- @setup
CREATE TABLE dml_copy_from_map (
  id INT,
  name TEXT
);

-- @setup
CREATE TABLE dml_copy_from_text_defaults (
  id INT,
  name TEXT
);

-- @setup
CREATE TABLE dml_copy_from_large_src (
  id INT,
  name TEXT
);

-- @setup
CREATE TABLE dml_copy_from_large_dst (
  id INT,
  name TEXT
);

-- @setup
CREATE TABLE dml_copy_from_parallel_dst (
  id INT,
  name TEXT
);

-- @setup
CREATE TABLE dml_copy_from_ndjson (
  id INT,
  name TEXT,
  note TEXT
);

-- @setup
CREATE TABLE dml_copy_from_ndjson_alias (
  id INT,
  name TEXT
);

-- @setup
CREATE TABLE dml_copy_from_ndjson_large (
  id INT,
  name TEXT
);

-- @setup
CREATE TABLE dml_copy_from_where_csv (
  id INT,
  name TEXT
);

-- @setup
CREATE TABLE dml_copy_from_where_ndjson (
  id INT,
  name TEXT
);

INSERT INTO dml_copy_from_src VALUES
  (1, 'alpha'),
  (2, 'pipe|bar'),
  (3, NULL),
  (4, 'comma,space'),
  (5, 'quote "here"');

COPY dml_copy_from_src TO '/tmp/paro_copy_from_text.txt';

COPY dml_copy_from_text FROM '/tmp/paro_copy_from_text.txt';

-- @query rowsort
SELECT id, name FROM dml_copy_from_text;

COPY (
  SELECT *
  FROM (
    VALUES
      (10, 'plain'::TEXT),
      (11, 'hex\\x41value'),
      (12, 'oct\\101value'),
      (13, 'slash\\\\path'),
      (14, '\\\\N'),
      (15, NULL::TEXT)
  ) AS v(id, name)
  ORDER BY id
) TO '/tmp/paro_copy_from_text_defaults.txt';

COPY dml_copy_from_text_defaults FROM '/tmp/paro_copy_from_text_defaults.txt';

-- @query
SELECT id, name
FROM dml_copy_from_text_defaults
ORDER BY id;

COPY dml_copy_from_src TO '/tmp/paro_copy_from_csv.csv' WITH (FORMAT csv, HEADER true);

COPY dml_copy_from_csv FROM '/tmp/paro_copy_from_csv.csv' WITH (FORMAT csv, HEADER true);

-- @query rowsort
SELECT id, name FROM dml_copy_from_csv;

COPY dml_copy_from_src TO '/tmp/paro_copy_from_where.csv' WITH (FORMAT csv, HEADER true);

COPY dml_copy_from_where_csv
FROM '/tmp/paro_copy_from_where.csv'
WITH (FORMAT csv, HEADER true)
WHERE id >= 2 AND name IS NOT NULL;

-- @query rowsort
SELECT id, name FROM dml_copy_from_where_csv;

COPY dml_copy_from_src TO '/tmp/paro_copy_from_where.ndjson' WITH (FORMAT ndjson);

COPY dml_copy_from_where_ndjson
FROM '/tmp/paro_copy_from_where.ndjson'
WITH (FORMAT ndjson)
WHERE dml_copy_from_where_ndjson.id >= 4;

-- @query rowsort
SELECT id, name FROM dml_copy_from_where_ndjson;

-- @statement error Column not found
COPY dml_copy_from_where_csv
FROM '/tmp/paro_copy_from_where.csv'
WITH (FORMAT csv, HEADER true)
WHERE not_exists > 0;

COPY (
  SELECT name, id
  FROM dml_copy_from_src
  ORDER BY id
) TO '/tmp/paro_copy_from_map.csv' WITH (FORMAT csv);

COPY dml_copy_from_map (name, id) FROM '/tmp/paro_copy_from_map.csv' WITH (FORMAT csv);

-- @query rowsort
SELECT id, name FROM dml_copy_from_map;

INSERT INTO dml_copy_from_large_src
SELECT i, 'name_' || i::VARCHAR
FROM generate_series(1, 1000) AS t(i);

COPY dml_copy_from_large_src TO '/tmp/paro_copy_from_large.csv' WITH (FORMAT csv);

SET copy_buffer_size = 257;

SET copy_flush_threads = 2;

COPY dml_copy_from_large_dst FROM '/tmp/paro_copy_from_large.csv' WITH (FORMAT csv);

-- @query
SELECT COUNT(*) AS copied_rows, MIN(id) AS min_id, MAX(id) AS max_id
FROM dml_copy_from_large_dst;

-- @query
SELECT current_setting('copy_buffer_size') AS copy_buffer_size,
       current_setting('copy_flush_threads') AS copy_flush_threads;

SET copy_buffer_size = DEFAULT;

SET copy_flush_threads = DEFAULT;

COPY (
  SELECT i, 'parallel_' || i::VARCHAR
  FROM generate_series(1, 20000) AS t(i)
) TO '/tmp/paro_copy_from_parallel.csv' WITH (FORMAT csv);

SET threads = 4;

COPY dml_copy_from_parallel_dst FROM '/tmp/paro_copy_from_parallel.csv'
  WITH (FORMAT csv, PARALLEL true, PARALLEL_WORKERS 4);

-- @query
SELECT COUNT(*) AS copied_rows,
       COUNT(DISTINCT id) AS distinct_ids,
       MIN(id) AS min_id,
       MAX(id) AS max_id
FROM dml_copy_from_parallel_dst;

SET threads = DEFAULT;

COPY (
  SELECT id,
         name,
         CASE WHEN id % 2 = 0 THEN 'even' ELSE NULL::TEXT END AS note
  FROM dml_copy_from_src
  ORDER BY id
) TO '/tmp/paro_copy_from_ndjson.json' WITH (FORMAT ndjson);

COPY dml_copy_from_ndjson
FROM '/tmp/paro_copy_from_ndjson.json'
WITH (FORMAT ndjson);

-- @query rowsort
SELECT id, name, note
FROM dml_copy_from_ndjson;

COPY dml_copy_from_src TO '/tmp/paro_copy_from_ndjson_table.json'
  WITH (FORMAT ndjson);

COPY dml_copy_from_ndjson_alias
FROM '/tmp/paro_copy_from_ndjson_table.json'
WITH (FORMAT json);

-- @query rowsort
SELECT id, name
FROM dml_copy_from_ndjson_alias;

COPY (
  SELECT i AS id, 'json_' || i::VARCHAR AS name
  FROM generate_series(1, 5000) AS t(i)
) TO '/tmp/paro_copy_from_ndjson_large.json' WITH (FORMAT ndjson);

COPY dml_copy_from_ndjson_large
FROM '/tmp/paro_copy_from_ndjson_large.json'
WITH (FORMAT ndjson);

-- @query
SELECT COUNT(*) AS copied_rows, MIN(id) AS min_id, MAX(id) AS max_id
FROM dml_copy_from_ndjson_large;

COPY (
  SELECT 1 AS id
) TO '/tmp/paro_copy_from_bad_ndjson.txt' WITH (FORMAT text);

-- @statement error Invalid NDJSON record
COPY dml_copy_from_ndjson_alias
FROM '/tmp/paro_copy_from_bad_ndjson.txt'
WITH (FORMAT ndjson);

COPY (
  SELECT 'abc' AS id, 'bad' AS name
) TO '/tmp/paro_copy_from_bad_ndjson_type.json' WITH (FORMAT ndjson);

-- @statement error invalid integer value
COPY dml_copy_from_ndjson_alias
FROM '/tmp/paro_copy_from_bad_ndjson_type.json'
WITH (FORMAT ndjson);

COPY (
  SELECT id
  FROM dml_copy_from_src
  ORDER BY id
) TO '/tmp/paro_copy_from_bad_columns.csv' WITH (FORMAT csv);

-- @statement error CSV row has incorrect column count
COPY dml_copy_from_text FROM '/tmp/paro_copy_from_bad_columns.csv' WITH (FORMAT csv);

COPY (
  SELECT 'abc' AS id, 'bad' AS name
) TO '/tmp/paro_copy_from_type_mismatch.csv' WITH (FORMAT csv);

-- @statement error invalid input syntax for type BIGINT
COPY dml_copy_from_text FROM '/tmp/paro_copy_from_type_mismatch.csv' WITH (FORMAT csv);

-- @statement error Failed to open CSV file
COPY dml_copy_from_text FROM '/tmp/paro_copy_from_missing.csv' WITH (FORMAT csv);

-- @teardown
DROP TABLE IF EXISTS dml_copy_from_src;

-- @teardown
DROP TABLE IF EXISTS dml_copy_from_text;

-- @teardown
DROP TABLE IF EXISTS dml_copy_from_csv;

-- @teardown
DROP TABLE IF EXISTS dml_copy_from_map;

-- @teardown
DROP TABLE IF EXISTS dml_copy_from_text_defaults;

-- @teardown
DROP TABLE IF EXISTS dml_copy_from_large_src;

-- @teardown
DROP TABLE IF EXISTS dml_copy_from_large_dst;

-- @teardown
DROP TABLE IF EXISTS dml_copy_from_parallel_dst;

-- @teardown
DROP TABLE IF EXISTS dml_copy_from_ndjson;

-- @teardown
DROP TABLE IF EXISTS dml_copy_from_ndjson_alias;

-- @teardown
DROP TABLE IF EXISTS dml_copy_from_ndjson_large;

-- @teardown
DROP TABLE IF EXISTS dml_copy_from_where_csv;

-- @teardown
DROP TABLE IF EXISTS dml_copy_from_where_ndjson;
