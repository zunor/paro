# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS ddl_create_table_case;

-- @setup
CREATE TABLE ddl_create_table_case (
  id INT,
  name TEXT
);

-- @statement count 2
INSERT INTO ddl_create_table_case VALUES (1, 'alice'), (2, 'bob');

SELECT id, name FROM ddl_create_table_case ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS ddl_create_table_case;
