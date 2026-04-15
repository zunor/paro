-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS integer_types_test;

CREATE TABLE integer_types_test (
    t TINYINT,
    s SMALLINT,
    i INT,
    b BIGINT,
    f FLOAT,
    d DOUBLE,
    PRIMARY KEY (t)
);

INSERT INTO integer_types_test VALUES (1, 10, 100, 1000, 1.1, 2.2);

-- @query rowsort
SELECT * FROM integer_types_test;

-- @teardown
DROP TABLE IF EXISTS integer_types_test;
