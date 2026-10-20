-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Successful writes and failures in each DML position preserve statement atomicity, including later batches.

CREATE FUNCTION py_write(a INTEGER) RETURNS INTEGER LANGUAGE python STRICT
AS $$
if a == 3000:
    raise ValueError("rejected write")
return a + 10
$$;

CREATE TABLE py_write_data(id INTEGER, value INTEGER);

CREATE TABLE py_write_copy(id INTEGER, value INTEGER);

INSERT INTO py_write_copy SELECT 1, 2 UNION ALL SELECT 3, 4;

DELETE FROM py_write_copy;

INSERT INTO py_write_data VALUES (1, py_write(2)), (2, py_write(NULL));

UPDATE py_write_data SET value = py_write(value) WHERE id = 1;

SELECT id AS id, value AS value FROM py_write_data ORDER BY id;

DELETE FROM py_write_data WHERE py_write(value) > 30;

SELECT id AS id, value AS value FROM py_write_data ORDER BY id;

DELETE FROM py_write_data;

INSERT INTO py_write_data SELECT i::INTEGER, i::INTEGER FROM generate_series(1, 4097) t(i);

-- @statement error SQLSTATE=38001
UPDATE py_write_data SET value = py_write(value);

SELECT count(*) AS rows, sum(value) AS total FROM py_write_data;

SELECT count(*) AS rows FROM py_write_copy;

-- @statement error SQLSTATE=38001
INSERT INTO py_write_copy SELECT id, py_write(value) FROM py_write_data;

SELECT count(*) AS rows, sum(value) AS total FROM py_write_data;

SELECT count(*) AS rows FROM py_write_copy;

-- @statement error SQLSTATE=38001
DELETE FROM py_write_data WHERE py_write(value) > 0;

SELECT count(*) AS rows, sum(value) AS total FROM py_write_data;

SELECT count(*) AS rows FROM py_write_copy;

BEGIN;

UPDATE py_write_data SET value = py_write(value) WHERE id < 3;

ROLLBACK;

SELECT id, value FROM py_write_data WHERE id < 3 ORDER BY id;

DROP TABLE py_write_copy;

DROP TABLE py_write_data;

DROP FUNCTION py_write(INTEGER);

-- @teardown
DROP TABLE IF EXISTS py_write_copy;

-- @teardown
DROP TABLE IF EXISTS py_write_data;

-- @teardown
DROP FUNCTION IF EXISTS py_write(INTEGER);
