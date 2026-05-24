-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control restart profile=default
-- @control connect user=paro
DROP FUNCTION IF EXISTS py_degraded_probe(INTEGER);

CREATE FUNCTION py_degraded_probe(a INTEGER) RETURNS INTEGER
LANGUAGE python
AS $$return [value + 4 for value in a.materialize_py()]$$;

SELECT py_degraded_probe(1);

-- @control restart profile=python_worker_crash
SELECT py_degraded_probe(1);

-- @normalize python_runtime_retry_hint
SELECT py_degraded_probe(1);

SELECT 6 * 7;

-- @control restart profile=default
-- @control connect user=paro
SELECT py_degraded_probe(1);

DROP FUNCTION py_degraded_probe(INTEGER);
