# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS basic_types_test;

CREATE TABLE basic_types_test (
    id INT,
    bool_col BOOLEAN,
    int_col INT,
    bigint_col BIGINT,
    float_col FLOAT,
    double_col DOUBLE PRECISION,
    varchar_col VARCHAR,
    date_col DATE,
    ts_col TIMESTAMP,
    PRIMARY KEY (id)
);

INSERT INTO basic_types_test VALUES
    (1, true,  42, 9999999999, 3.14, 2.718281828, 'hello', '2024-01-01', '2024-01-01 12:00:00');
INSERT INTO basic_types_test VALUES
    (2, false, -1, -100,       0.0,  -1.5,        'world', '2025-06-15', '2025-06-15 23:59:59');

-- @query rowsort
SELECT id, bool_col, int_col, bigint_col, varchar_col FROM basic_types_test;

-- @teardown
DROP TABLE IF EXISTS basic_types_test;
