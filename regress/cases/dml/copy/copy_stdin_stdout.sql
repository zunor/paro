# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS dml_copy_protocol_src;

-- @setup
DROP TABLE IF EXISTS dml_copy_protocol_dst;

-- @setup
CREATE TABLE dml_copy_protocol_src (
  id INT,
  name TEXT,
  note TEXT
);

-- @setup
CREATE TABLE dml_copy_protocol_dst (
  id INT,
  name TEXT,
  note TEXT
);

INSERT INTO dml_copy_protocol_src VALUES
  (1, 'alpha', NULL),
  (2, 'comma,space', 'quote "here"'),
  (3, 'pipe|bar', 'plain'),
  (4, 'slash\\path', 'csv'),
  (5, 'tail', 'done');

-- @copy out
COPY dml_copy_protocol_src TO STDOUT WITH (FORMAT csv, HEADER true);

-- @copy in
COPY dml_copy_protocol_dst FROM STDIN WITH (FORMAT csv, HEADER true);
-- @copydata
id,name,note
10,stdin-a,
11,"stdin,comma","quote ""again"""
12,stdin-c,plain
13,stdin-d,"slash\\path"
-- @endcopy

-- @query
SELECT id, name, note
FROM dml_copy_protocol_dst
ORDER BY id;

-- @copy in
COPY dml_copy_protocol_dst FROM STDIN WITH (FORMAT csv);
-- @copydata
20,should-not-land,abort
21,should-not-land-2,abort
-- @copyfail client abort from regress
-- @endcopy

-- @query
SELECT COUNT(*) AS copied_rows
FROM dml_copy_protocol_dst;

-- @teardown
DROP TABLE IF EXISTS dml_copy_protocol_src;

-- @teardown
DROP TABLE IF EXISTS dml_copy_protocol_dst;
