-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS dml_copy_to_file_case;

-- @setup
DROP TABLE IF EXISTS dml_copy_to_file_empty;

-- @setup
DROP TABLE IF EXISTS dml_copy_to_file_large;

-- @setup
DROP TABLE IF EXISTS dml_copy_to_per_thread_verify;

-- @setup
CREATE TABLE dml_copy_to_file_case (
  id INT,
  name TEXT,
  note TEXT
);

-- @setup
CREATE TABLE dml_copy_to_file_empty (
  id INT,
  name TEXT
);

-- @setup
CREATE TABLE dml_copy_to_file_large (
  id INT,
  name TEXT
);

-- @setup
CREATE TABLE dml_copy_to_per_thread_verify (
  id INT,
  name TEXT
);

INSERT INTO dml_copy_to_file_case VALUES
  (1, 'alpha', NULL),
  (2, 'pipe|bar', 'quote "here"'),
  (3, 'comma,space', 'simple');

INSERT INTO dml_copy_to_file_large
SELECT i, 'name_' || i::VARCHAR
FROM generate_series(1, 50) AS t(i);

COPY dml_copy_to_file_case TO '/tmp/paro_copy_t1_13_text.txt';

-- @query file
FILE '/tmp/paro_copy_t1_13_text.txt';

COPY dml_copy_to_file_case TO '/tmp/paro_copy_t1_13_csv.csv'
  WITH (FORMAT csv, HEADER true, DELIMITER '|');

-- @query file
FILE '/tmp/paro_copy_t1_13_csv.csv';

COPY dml_copy_to_file_case TO '/tmp/paro_copy_t1_13_csv_null.csv'
  WITH (FORMAT csv, HEADER true, NULL 'NULL');

-- @query file
FILE '/tmp/paro_copy_t1_13_csv_null.csv';

COPY dml_copy_to_file_case TO '/tmp/paro_copy_t1_13_force_quote.csv'
  WITH (FORMAT csv, FORCE_QUOTE *);

-- @query file
FILE '/tmp/paro_copy_t1_13_force_quote.csv';

COPY (
  SELECT id, name
  FROM dml_copy_to_file_case
  WHERE id >= 2
  ORDER BY id
) TO '/tmp/paro_copy_t1_13_query.csv'
  WITH (FORMAT csv);

-- @query file
FILE '/tmp/paro_copy_t1_13_query.csv';

COPY dml_copy_to_file_empty TO '/tmp/paro_copy_t1_13_empty.csv'
  WITH (FORMAT csv, HEADER true);

-- @query file
FILE '/tmp/paro_copy_t1_13_empty.csv';

COPY dml_copy_to_file_large TO '/tmp/paro_copy_t1_13_large.csv'
  WITH (FORMAT csv, HEADER true);

-- @query file
FILE '/tmp/paro_copy_t1_13_large.csv';

COPY dml_copy_to_file_case TO '/tmp/paro_copy_t4_4_table.ndjson'
  WITH (FORMAT ndjson);

-- @query file
FILE '/tmp/paro_copy_t4_4_table.ndjson';

COPY (
  SELECT id AS "ID", name, note
  FROM dml_copy_to_file_case
  ORDER BY id
) TO '/tmp/paro_copy_t4_4_alias.json'
  WITH (FORMAT json);

-- @query file
FILE '/tmp/paro_copy_t4_4_alias.json';

SET threads = 4;

COPY (
  SELECT i AS id, 'name_' || i::VARCHAR AS name
  FROM generate_series(1, 20000) AS t(i)
) TO '/tmp/paro_copy_t4_3_parallel.csv'
  WITH (FORMAT csv, PER_THREAD_OUTPUT true);

-- @statement ok
-- @normalize copy_rowcount
COPY dml_copy_to_per_thread_verify FROM '/tmp/paro_copy_t4_3_parallel_0.csv' WITH (FORMAT csv);
-- @statement ok
-- @normalize copy_rowcount
COPY dml_copy_to_per_thread_verify FROM '/tmp/paro_copy_t4_3_parallel_1.csv' WITH (FORMAT csv);
-- @statement ok
-- @normalize copy_rowcount
COPY dml_copy_to_per_thread_verify FROM '/tmp/paro_copy_t4_3_parallel_2.csv' WITH (FORMAT csv);
-- @statement ok
-- @normalize copy_rowcount
COPY dml_copy_to_per_thread_verify FROM '/tmp/paro_copy_t4_3_parallel_3.csv' WITH (FORMAT csv);

-- @query
SELECT COUNT(*) AS copied_rows,
       COUNT(DISTINCT id) AS distinct_ids,
       MIN(id) AS min_id,
       MAX(id) AS max_id
FROM dml_copy_to_per_thread_verify;

SET threads = DEFAULT;

-- @teardown
DROP TABLE IF EXISTS dml_copy_to_file_case;

-- @teardown
DROP TABLE IF EXISTS dml_copy_to_file_empty;

-- @teardown
DROP TABLE IF EXISTS dml_copy_to_file_large;

-- @teardown
DROP TABLE IF EXISTS dml_copy_to_per_thread_verify;
