-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

DROP FUNCTION IF EXISTS py_scale(INTEGER);

CREATE FUNCTION py_scale(a INTEGER) RETURNS TABLE (value INTEGER)
LANGUAGE python
AS $$return [value * 10 for value in a.materialize_py()]$$;

-- @normalize explain_routine_ids
EXPLAIN
SELECT t.x, s.value
FROM (VALUES (1), (2), (3)) AS t(x)
CROSS JOIN LATERAL py_scale(t.x) AS s
ORDER BY 1, 2;

DROP FUNCTION py_scale(INTEGER);
