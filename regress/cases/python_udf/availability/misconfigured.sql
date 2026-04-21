-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control restart profile=default
-- @control connect user=paro
DROP FUNCTION IF EXISTS py_misconfigured_probe(INTEGER);

CREATE FUNCTION py_misconfigured_probe(a INTEGER) RETURNS INTEGER
LANGUAGE python
AS $$return [value + 3 for value in a.materialize_py()]$$;

SELECT py_misconfigured_probe(1);

-- @control restart profile=python_misconfigured
SELECT 20 + 22;

-- @normalize regress_paths
CREATE FUNCTION py_misconfigured_new(a INTEGER) RETURNS INTEGER
LANGUAGE python
AS $$return [value for value in a.materialize_py()]$$;

SELECT py_misconfigured_probe(1);

-- @control restart profile=default
-- @control connect user=paro
SELECT py_misconfigured_probe(1);

DROP FUNCTION py_misconfigured_probe(INTEGER);
