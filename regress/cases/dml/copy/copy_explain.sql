# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS dml_copy_explain_src;

-- @setup
DROP TABLE IF EXISTS dml_copy_explain_dst;

-- @setup
CREATE TABLE dml_copy_explain_src (
  id INT,
  name TEXT
);

-- @setup
CREATE TABLE dml_copy_explain_dst (
  id INT,
  name TEXT
);

INSERT INTO dml_copy_explain_src VALUES
  (1, 'alpha'),
  (2, 'beta'),
  (3, 'gamma');

EXPLAIN COPY dml_copy_explain_src
TO '/tmp/paro_copy_t4_6_explain.csv'
WITH (FORMAT csv, HEADER true);

COPY dml_copy_explain_src
TO '/tmp/paro_copy_t4_6_explain.csv'
WITH (FORMAT csv, HEADER true);

EXPLAIN COPY dml_copy_explain_dst
FROM '/tmp/paro_copy_t4_6_explain.csv'
WITH (FORMAT csv, HEADER true);

EXPLAIN COPY dml_copy_explain_dst
FROM '/tmp/paro_copy_t4_6_explain.csv'
WITH (FORMAT csv, HEADER true)
WHERE id >= 2;

-- @query
SELECT COUNT(*) AS copied_rows
FROM dml_copy_explain_dst;

COPY dml_copy_explain_dst
FROM '/tmp/paro_copy_t4_6_explain.csv'
WITH (FORMAT csv, HEADER true)
WHERE id >= 2;

-- @query
SELECT COUNT(*) AS copied_rows,
       MIN(id) AS min_id,
       MAX(id) AS max_id
FROM dml_copy_explain_dst;

-- @teardown
DROP TABLE IF EXISTS dml_copy_explain_src;

-- @teardown
DROP TABLE IF EXISTS dml_copy_explain_dst;
