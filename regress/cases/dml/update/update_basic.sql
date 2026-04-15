-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS dml_update_basic_case;

-- @setup
CREATE TABLE dml_update_basic_case (
  id INT,
  name VARCHAR,
  price INT
);

INSERT INTO dml_update_basic_case VALUES (1, 'apple', 10), (2, 'banana', 20);

UPDATE dml_update_basic_case SET price = 15 WHERE id = 1;

SELECT id, name, price FROM dml_update_basic_case WHERE id = 1;

UPDATE dml_update_basic_case SET name = 'cherry', price = 30 WHERE id = 2;

SELECT id, name, price FROM dml_update_basic_case WHERE id = 2;

UPDATE dml_update_basic_case SET price = price + 1;

SELECT id, name, price FROM dml_update_basic_case ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS dml_update_basic_case;
