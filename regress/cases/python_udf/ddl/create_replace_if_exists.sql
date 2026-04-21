-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

DROP FUNCTION IF EXISTS py_replace(INTEGER);

CREATE FUNCTION py_replace(a INTEGER) RETURNS INTEGER
LANGUAGE python
IMMUTABLE
AS $$return [value + 1 for value in a.materialize_py()]$$;

SELECT py_replace(1);

CREATE OR REPLACE FUNCTION py_replace(a INTEGER) RETURNS INTEGER
LANGUAGE python
IMMUTABLE
AS $$return [value + 2 for value in a.materialize_py()]$$;

SELECT py_replace(1);

DROP FUNCTION IF EXISTS py_replace(INTEGER);
DROP FUNCTION IF EXISTS py_replace(INTEGER);
