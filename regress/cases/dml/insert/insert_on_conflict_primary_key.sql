# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS dml_insert_on_conflict_pk_case;

-- @setup
CREATE TABLE dml_insert_on_conflict_pk_case (
  id INT PRIMARY KEY,
  price INT,
  stock INT
);

INSERT INTO dml_insert_on_conflict_pk_case VALUES (1, 10, 100), (2, 20, 200);

INSERT INTO dml_insert_on_conflict_pk_case VALUES (2, 999, 999), (3, 30, 300)
ON CONFLICT (id) DO NOTHING;

-- @query rowsort
SELECT id, price, stock FROM dml_insert_on_conflict_pk_case;

INSERT INTO dml_insert_on_conflict_pk_case VALUES (2, 25, 250), (4, 40, 400)
ON CONFLICT (id) DO UPDATE SET stock = EXCLUDED.stock;

-- @query rowsort
SELECT id, price, stock FROM dml_insert_on_conflict_pk_case;

UPDATE dml_insert_on_conflict_pk_case SET stock = 251 WHERE id = 2;

-- @query rowsort
SELECT id, price, stock FROM dml_insert_on_conflict_pk_case;

-- @teardown
DROP TABLE IF EXISTS dml_insert_on_conflict_pk_case;
