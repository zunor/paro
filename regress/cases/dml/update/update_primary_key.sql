# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS dml_update_primary_key_case;

-- @setup
CREATE TABLE dml_update_primary_key_case (
  id INT PRIMARY KEY,
  price INT,
  stock INT
);

INSERT INTO dml_update_primary_key_case VALUES
  (1, 10, 100),
  (2, 20, 200),
  (3, 30, 300);

UPDATE dml_update_primary_key_case
SET price = 25
WHERE id = 2;

-- @query rowsort
SELECT id, price, stock FROM dml_update_primary_key_case;

UPDATE dml_update_primary_key_case
SET stock = stock + 5
WHERE id = 3;

-- @query rowsort
SELECT id, price, stock FROM dml_update_primary_key_case;

-- @teardown
DROP TABLE IF EXISTS dml_update_primary_key_case;
