-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS dml_insert_basic_case;

-- @setup
CREATE TABLE dml_insert_basic_case (
  id INT,
  name TEXT
);

INSERT INTO dml_insert_basic_case VALUES (1, 'one');

INSERT INTO dml_insert_basic_case VALUES (2, 'two'), (3, 'three');

-- @query rowsort
SELECT id, name FROM dml_insert_basic_case;

-- @teardown
DROP TABLE IF EXISTS dml_insert_basic_case;
