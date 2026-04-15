# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS multirow_test;

CREATE TABLE multirow_test (id INT PRIMARY KEY, name VARCHAR, score INT);

-- 单条多行 INSERT
INSERT INTO multirow_test VALUES (1, 'Alice', 90), (2, 'Bob', 85), (3, 'Charlie', 95);

SELECT * FROM multirow_test ORDER BY id;

-- 追加 INSERT
INSERT INTO multirow_test VALUES (4, 'Diana', 88);

-- @query rowsort
SELECT id, name, score FROM multirow_test;

-- @teardown
DROP TABLE IF EXISTS multirow_test;
