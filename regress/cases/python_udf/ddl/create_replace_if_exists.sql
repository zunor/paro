-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

DROP FUNCTION IF EXISTS py_replace(INTEGER);

CREATE FUNCTION py_replace(a INTEGER) RETURNS INTEGER
LANGUAGE python
IMMUTABLE
AS $$return a + 1$$;

SELECT py_replace(1);

CREATE OR REPLACE FUNCTION py_replace(a INTEGER) RETURNS INTEGER
LANGUAGE python
IMMUTABLE
AS $$return a + 2$$;

SELECT py_replace(1);

DROP FUNCTION IF EXISTS py_replace(INTEGER);
DROP FUNCTION IF EXISTS py_replace(INTEGER);
