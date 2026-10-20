-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Zero-argument scalar calls, immutable context and nested conditional selection exercise invocation semantics.

CREATE FUNCTION py_context() RETURNS INTEGER LANGUAGE python
AS $$
from dataclasses import FrozenInstanceError
assert ctx.num_rows > 0 and ctx.routine_id > 0 and ctx.generation > 0
assert ctx.invocation_id
try:
    ctx.num_rows = 0
except FrozenInstanceError:
    return 17
raise AssertionError("mutable context")
$$;

SELECT id, py_context() AS value FROM (VALUES (1), (2), (3)) t(id) ORDER BY id;

CREATE FUNCTION py_positive(a INTEGER) RETURNS INTEGER LANGUAGE python STRICT
AS $$
assert a > 0, "unselected argument executed"
return a * 2
$$;

SELECT id, CASE WHEN v IS NULL THEN 99 WHEN v > 0 THEN py_positive(py_positive(v)) ELSE -1 END AS value FROM (VALUES (1, NULL), (2, -2), (3, 3)) t(id, v) ORDER BY id;

SELECT py_positive(CASE WHEN v > 0 THEN v ELSE 1 END) AS value FROM (VALUES (-5), (2)) t(v) ORDER BY value;

SELECT CASE WHEN v > 0 THEN py_positive(v) ELSE py_positive(0 - v) END AS value FROM (VALUES (-3), (2)) t(v) ORDER BY value;

DROP FUNCTION py_positive(INTEGER);

DROP FUNCTION py_context();

-- @teardown
DROP FUNCTION IF EXISTS py_positive(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_context();
