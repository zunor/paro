-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

DROP FUNCTION IF EXISTS py_explain(INTEGER);

CREATE FUNCTION py_explain(a INTEGER) RETURNS INTEGER
LANGUAGE python
IMMUTABLE
AS $$return [value + 3 for value in a.materialize_py()]$$;

-- @normalize explain_routine_ids
EXPLAIN
SELECT py_explain(v)
FROM (VALUES (1), (2)) AS t(v)
ORDER BY 1;

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes,explain_routine_ids,explain_external_runtime
EXPLAIN ANALYZE
SELECT py_explain(v)
FROM (VALUES (1), (2)) AS t(v)
ORDER BY 1;

DROP FUNCTION py_explain(INTEGER);
