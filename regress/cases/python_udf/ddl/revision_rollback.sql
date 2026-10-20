-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Drop/recreate and transactional replacement preserve the active catalog revision.

CREATE FUNCTION py_catalog(a INTEGER) RETURNS INTEGER LANGUAGE python
AS $$
return a + 1
$$;

BEGIN;

CREATE OR REPLACE FUNCTION py_catalog(a INTEGER) RETURNS INTEGER LANGUAGE python AS $$return a + 9$$;

SELECT py_catalog(1) AS value;

ROLLBACK;

SELECT py_catalog(1) AS value;

-- @statement error SQLSTATE=39001
CREATE OR REPLACE FUNCTION py_catalog(a INTEGER) RETURNS INTEGER LANGUAGE python HANDLER 'main' AS $$def main(a): return a$$;

SELECT py_catalog(1) AS value;

PREPARE py_catalog_plan AS SELECT py_catalog(1) AS value;

-- @query nosort
EXECUTE py_catalog_plan;

DROP FUNCTION py_catalog(INTEGER);

CREATE FUNCTION py_catalog(a INTEGER) RETURNS INTEGER LANGUAGE python
AS $$
return a + 20
$$;

-- @query nosort
EXECUTE py_catalog_plan;

DEALLOCATE py_catalog_plan;

DROP FUNCTION py_catalog(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_catalog(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_catalog(INTEGER);
