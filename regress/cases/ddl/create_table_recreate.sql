# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS recreate_test;

CREATE TABLE recreate_test (id INT, name VARCHAR);
INSERT INTO recreate_test VALUES (1, 'old');
SELECT * FROM recreate_test;

DROP TABLE recreate_test;

CREATE TABLE recreate_test (id INT, value DOUBLE);
INSERT INTO recreate_test VALUES (1, 3.14);
SELECT * FROM recreate_test;

-- @teardown
DROP TABLE IF EXISTS recreate_test;
