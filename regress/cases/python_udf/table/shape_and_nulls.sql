-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Table output supports empty results, chunks, multiple typed columns and nullable lateral correlation.

CREATE FUNCTION py_rows(a INTEGER) RETURNS TABLE (value INTEGER, label VARCHAR) LANGUAGE python
AS $$
import pyarrow as pa
values = [v.as_py() for v in a if v.is_valid and v.as_py() > 0]
return pa.table({"value": pa.chunked_array([pa.array(values, type=pa.int32()), pa.array(values, type=pa.int32())]), "label": pa.array(["雪"] * len(values) + [None] * len(values), type=pa.string())})
$$;

SELECT value AS value, label AS label FROM py_rows(0);

SELECT value AS value, label AS label FROM py_rows(5) ORDER BY label NULLS LAST;

SELECT t.id AS id, r.value AS value, r.label AS label FROM (VALUES (1, 2), (2, 0), (3, NULL), (4, 2)) t(id, v) LEFT JOIN LATERAL py_rows(t.v) r ON true ORDER BY t.id, r.label NULLS LAST;

DROP FUNCTION py_rows(INTEGER);

CREATE FUNCTION py_table_limit() RETURNS TABLE (value INTEGER) LANGUAGE python
AS $$
import pyarrow as pa
return pa.table({"value": pa.array(range(65536), type=pa.int32())})
$$;

SELECT count(*) AS rows, min(value) AS minimum, max(value) AS maximum FROM py_table_limit();

DROP FUNCTION py_table_limit();

-- @teardown
DROP FUNCTION IF EXISTS py_table_limit();

-- @teardown
DROP FUNCTION IF EXISTS py_rows(INTEGER);
