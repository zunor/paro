-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Qualified routines and typed overloads resolve independently across schema boundaries.

CREATE SCHEMA py_space_a;

CREATE SCHEMA py_space_b;

CREATE FUNCTION py_space_a.score(a INTEGER) RETURNS INTEGER LANGUAGE python
AS $$
return a + 10
$$;

CREATE FUNCTION py_space_b.score(a INTEGER) RETURNS INTEGER LANGUAGE python
AS $$
return a + 100
$$;

CREATE FUNCTION py_space_a.score(a VARCHAR) RETURNS VARCHAR LANGUAGE python
AS $$
return a + "!"
$$;

SELECT py_space_a.score(2) AS first, py_space_b.score(2) AS second, py_space_a.score('雪'::VARCHAR) AS text_value;

DROP FUNCTION py_space_a.score(INTEGER);

SELECT py_space_b.score(2) AS retained, py_space_a.score(''::VARCHAR) AS overload;

DROP FUNCTION py_space_b.score(INTEGER);

DROP FUNCTION py_space_a.score(VARCHAR);

-- @teardown
DROP FUNCTION IF EXISTS py_space_a.score(VARCHAR);

-- @teardown
DROP FUNCTION IF EXISTS py_space_b.score(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_space_a.score(INTEGER);

-- @teardown
DROP SCHEMA IF EXISTS py_space_b;

-- @teardown
DROP SCHEMA IF EXISTS py_space_a;
