-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Invalid shapes/types are rejected and release invocation capacity before a healthy follow-up call.

CREATE FUNCTION py_healthy(a INTEGER) RETURNS INTEGER LANGUAGE python
AS $$
return a + 1
$$;

CREATE FUNCTION py_bad_short(a INTEGER) RETURNS INTEGER LANGUAGE python MODE VECTOR
AS $$
return a.slice(0, 0)
$$;

-- @statement error SQLSTATE=39001
SELECT py_bad_short(1);

SELECT py_healthy(41) AS recovered;

DROP FUNCTION py_bad_short(INTEGER);

CREATE FUNCTION py_bad_wide(a INTEGER) RETURNS INTEGER LANGUAGE python MODE VECTOR
AS $$
import pyarrow as pa
return a.cast(pa.int64())
$$;

-- @statement error SQLSTATE=39001
SELECT py_bad_wide(1);

SELECT py_healthy(41) AS recovered;

DROP FUNCTION py_bad_wide(INTEGER);

CREATE FUNCTION py_bad_list(a INTEGER) RETURNS INTEGER LANGUAGE python MODE VECTOR
AS $$
return a.to_pylist()
$$;

-- @statement error SQLSTATE=39001
SELECT py_bad_list(1);

SELECT py_healthy(41) AS recovered;

DROP FUNCTION py_bad_list(INTEGER);

CREATE FUNCTION py_bad_overflow(a INTEGER) RETURNS TINYINT LANGUAGE python
AS $$
return 128
$$;

-- @statement error SQLSTATE=39001
SELECT py_bad_overflow(1);

SELECT py_healthy(41) AS recovered;

DROP FUNCTION py_bad_overflow(INTEGER);

CREATE FUNCTION py_bad_columns(a INTEGER) RETURNS TABLE (value INTEGER) LANGUAGE python
AS $$
import pyarrow as pa
return pa.RecordBatch.from_arrays([a], names=["wrong"])
$$;

-- @statement error SQLSTATE=39001
SELECT * FROM py_bad_columns(1);

SELECT py_healthy(41) AS recovered;

DROP FUNCTION py_bad_columns(INTEGER);

CREATE FUNCTION py_bad_table(a INTEGER) RETURNS TABLE (value INTEGER) LANGUAGE python
AS $$
return a
$$;

-- @statement error SQLSTATE=39001
SELECT * FROM py_bad_table(1);

SELECT py_healthy(41) AS recovered;

DROP FUNCTION py_bad_table(INTEGER);

CREATE FUNCTION py_bad_bytes(a INTEGER) RETURNS VARCHAR LANGUAGE python
AS $$
return "x" * (16 * 1024 * 1024 + 1)
$$;

-- @statement error SQLSTATE=39001
SELECT py_bad_bytes(1);

SELECT py_healthy(41) AS recovered;

DROP FUNCTION py_bad_bytes(INTEGER);

CREATE FUNCTION py_bad_rows(a INTEGER) RETURNS TABLE (value INTEGER) LANGUAGE python
AS $$
import pyarrow as pa
return pa.RecordBatch.from_arrays([pa.array([1] * 65537, type=pa.int32())], names=["value"])
$$;

-- @statement error SQLSTATE=39001
SELECT * FROM py_bad_rows(1);

SELECT py_healthy(41) AS recovered;

DROP FUNCTION py_bad_rows(INTEGER);

DROP FUNCTION py_healthy(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_bad_rows(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_bad_bytes(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_bad_table(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_bad_columns(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_bad_overflow(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_bad_list(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_bad_wide(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_bad_short(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_healthy(INTEGER);
