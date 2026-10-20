-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

DROP FUNCTION IF EXISTS py_tx(INTEGER);

BEGIN;

CREATE FUNCTION py_tx(a INTEGER) RETURNS INTEGER
LANGUAGE python
AS $$return a + 1$$;

SELECT py_tx(1);

ROLLBACK;

SELECT py_tx(1);

BEGIN;

CREATE FUNCTION py_tx(a INTEGER) RETURNS INTEGER
LANGUAGE python
AS $$return a + 5$$;

COMMIT;

SELECT py_tx(1);

-- @control restart

SELECT py_tx(1);

DROP FUNCTION py_tx(INTEGER);
