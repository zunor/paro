-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Repeated statement deadlines must release owner slots and leave the same session usable.

CREATE FUNCTION py_timeout(a INTEGER) RETURNS INTEGER LANGUAGE python
AS $$
import time
if a < 0:
    time.sleep(10)
return a + 1
$$;

SELECT py_timeout(0) AS warm;

SET statement_timeout = 500;

-- @statement error SQLSTATE=57P06
SELECT py_timeout(-1);

-- @statement error SQLSTATE=57P06
SELECT py_timeout(-1);

-- @statement error SQLSTATE=57P06
SELECT py_timeout(-1);

-- @statement error SQLSTATE=57P06
SELECT py_timeout(-1);

-- @statement error SQLSTATE=57P06
SELECT py_timeout(-1);

SET statement_timeout = 0;

SELECT py_timeout(41) AS recovered, 6 * 7 AS native_value;

DROP FUNCTION py_timeout(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_timeout(INTEGER);
