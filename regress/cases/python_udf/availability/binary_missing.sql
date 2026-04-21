-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control restart profile=default
-- @control connect user=paro
DROP FUNCTION IF EXISTS py_missing_probe(INTEGER);

CREATE FUNCTION py_missing_probe(a INTEGER) RETURNS INTEGER
LANGUAGE python
AS $$return [value + 2 for value in a.materialize_py()]$$;

SELECT py_missing_probe(1);

-- @control restart profile=python_binary_missing
SELECT 7 * 6;

CREATE FUNCTION py_missing_new(a INTEGER) RETURNS INTEGER
LANGUAGE python
AS $$return [value for value in a.materialize_py()]$$;

SELECT py_missing_probe(1);

-- @control restart profile=default
-- @control connect user=paro
SELECT py_missing_probe(1);

DROP FUNCTION py_missing_probe(INTEGER);
