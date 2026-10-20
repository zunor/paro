-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro
CREATE OR REPLACE FUNCTION py_checked(a INTEGER) RETURNS INTEGER LANGUAGE python STRICT
AS $$
assert a > 0, "unselected row reached Python"
return a + 1
$$;

SELECT v, CASE WHEN v > 0 THEN py_checked(v) ELSE 0 END AS result
FROM (VALUES (-1), (2), (NULL::INTEGER)) t(v) ORDER BY v NULLS LAST;

SELECT py_checked(py_checked(1)) AS nested;
SELECT py_checked(NULL::INTEGER) IS NULL AS strict_null;

CREATE OR REPLACE FUNCTION py_unloaded(a INTEGER) RETURNS INTEGER LANGUAGE python STRICT HANDLER 'main'
AS $$
raise RuntimeError("module must not load")
def main(ctx, a):
    return a
$$;
SELECT py_unloaded(NULL::INTEGER) IS NULL AS no_module_load;
SELECT CASE WHEN v < 0 THEN py_unloaded(v) ELSE v END AS result
FROM (VALUES (1), (2)) t(v) ORDER BY result;

CREATE OR REPLACE FUNCTION py_zero() RETURNS INTEGER LANGUAGE python MODE VECTOR
AS $$
import pyarrow as pa
return pa.array([42] * ctx.num_rows, type=pa.int32())
$$;
SELECT v, py_zero() AS answer FROM (VALUES (1), (2)) t(v) ORDER BY v;

CREATE OR REPLACE FUNCTION py_revision(a INTEGER) RETURNS INTEGER LANGUAGE python AS $$return a + 10$$;
PREPARE py_plan AS SELECT py_revision(2) AS answer;
-- @query nosort
EXECUTE py_plan;
-- @session revision_writer user=paro
CREATE OR REPLACE FUNCTION py_revision(a INTEGER) RETURNS INTEGER LANGUAGE python AS $$return a + 20$$;
-- @query nosort
EXECUTE py_plan;
DEALLOCATE py_plan;
PREPARE py_overload AS SELECT py_revision(2::SMALLINT) AS answer;
-- @query nosort
EXECUTE py_overload;
-- @session revision_writer user=paro
CREATE FUNCTION py_revision(a SMALLINT) RETURNS INTEGER LANGUAGE python AS $$return a + 100$$;
-- @query nosort
EXECUTE py_overload;
DEALLOCATE py_overload;
DROP FUNCTION py_revision(SMALLINT);

DROP FUNCTION py_checked(INTEGER);
DROP FUNCTION py_unloaded(INTEGER);
DROP FUNCTION py_zero();
DROP FUNCTION py_revision(INTEGER);
