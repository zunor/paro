-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control restart profile=default
-- @control connect user=paro
DROP FUNCTION IF EXISTS py_disabled_probe(INTEGER);

CREATE FUNCTION py_disabled_probe(a INTEGER) RETURNS INTEGER
LANGUAGE python
AS $$return a + 1$$;

SELECT py_disabled_probe(1);

-- @control restart profile=python_disabled
SELECT 41 + 1;

CREATE FUNCTION py_disabled_new(a INTEGER) RETURNS INTEGER
LANGUAGE python
AS $$return a$$;

SELECT py_disabled_probe(1);

-- @control restart profile=default
-- @control connect user=paro
SELECT py_disabled_probe(1);

DROP FUNCTION py_disabled_probe(INTEGER);
