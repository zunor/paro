# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS partial_col_test;

CREATE TABLE partial_col_test (id INT, name VARCHAR, value INT);

INSERT INTO partial_col_test (id, name) VALUES (1, 'Alice');
INSERT INTO partial_col_test (id, value) VALUES (2, 42);
INSERT INTO partial_col_test VALUES (3, 'Charlie', 99);

SELECT * FROM partial_col_test ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS partial_col_test;
