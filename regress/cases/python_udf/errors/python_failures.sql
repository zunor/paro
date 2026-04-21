-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

DROP FUNCTION IF EXISTS py_fail(INTEGER);
DROP FUNCTION IF EXISTS py_bad_contract(INTEGER);
DROP FUNCTION IF EXISTS py_missing_import(INTEGER);

CREATE FUNCTION py_fail(a INTEGER) RETURNS INTEGER
LANGUAGE python
AS $$raise ValueError("boom from worker")$$;

SELECT py_fail(1);

CREATE FUNCTION py_bad_contract(a INTEGER) RETURNS INTEGER
LANGUAGE python
AS $$return ["oops" for _ in a.materialize_py()]$$;

SELECT py_bad_contract(1);

CREATE FUNCTION py_missing_import(a INTEGER) RETURNS INTEGER
LANGUAGE python
IMPORTS ('/tmp/paro/python_udf/does_not_exist.py')
AS $$return [value for value in a.materialize_py()]$$;

SELECT py_missing_import(1);

DROP FUNCTION py_fail(INTEGER);
DROP FUNCTION py_bad_contract(INTEGER);
DROP FUNCTION py_missing_import(INTEGER);
