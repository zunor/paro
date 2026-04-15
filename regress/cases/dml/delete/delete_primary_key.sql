-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS dml_delete_primary_key_case;

-- @setup
CREATE TABLE dml_delete_primary_key_case (
  id INT PRIMARY KEY,
  score INT,
  note VARCHAR
);

INSERT INTO dml_delete_primary_key_case VALUES
  (1, 10, 'a'),
  (2, 20, 'b'),
  (3, 30, 'c');

DELETE FROM dml_delete_primary_key_case WHERE id = 2;

-- @query rowsort
SELECT id, score, note FROM dml_delete_primary_key_case;

INSERT INTO dml_delete_primary_key_case VALUES (2, 222, 'reinserted');

-- @query rowsort
SELECT id, score, note FROM dml_delete_primary_key_case;

DELETE FROM dml_delete_primary_key_case;

-- @query rowsort
SELECT id, score, note FROM dml_delete_primary_key_case;

-- @teardown
DROP TABLE IF EXISTS dml_delete_primary_key_case;
