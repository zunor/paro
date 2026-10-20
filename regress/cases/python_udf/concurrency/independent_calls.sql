-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Independent calls are submitted concurrently within the CI memory budget; outputs remain tied to their sessions.

CREATE FUNCTION py_concurrent(a INTEGER) RETURNS INTEGER LANGUAGE python
AS $$
import time
time.sleep(0.2)
return a * 10
$$;

-- @session worker0 user=paro async=call0
SELECT py_concurrent(0) AS value;

-- @session worker1 user=paro async=call1
SELECT py_concurrent(1) AS value;

-- @session worker2 user=paro async=call2
SELECT py_concurrent(2) AS value;

SELECT 42 AS ordinary_sql;

-- @await call2 timeout=20s

-- @await call1 timeout=20s

-- @await call0 timeout=20s

SELECT py_concurrent(7) AS recovered;

DROP FUNCTION py_concurrent(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_concurrent(INTEGER);
