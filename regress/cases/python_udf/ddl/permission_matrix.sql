-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control restart profile=auth_matrix
-- @control connect user=paro
DROP FUNCTION IF EXISTS py_builder_ok(INTEGER);
DROP FUNCTION IF EXISTS py_builder_definer(INTEGER);
DROP FUNCTION IF EXISTS py_builder_trusted(INTEGER);
DROP FUNCTION IF EXISTS py_reader_denied(INTEGER);

SELECT current_user();

-- @control connect user=routine_builder
SELECT current_user();

CREATE FUNCTION py_builder_ok(a INTEGER) RETURNS INTEGER
LANGUAGE python
AS $$return [value + 10 for value in a.materialize_py()]$$;

SELECT py_builder_ok(1);

CREATE FUNCTION py_builder_definer(a INTEGER) RETURNS INTEGER
LANGUAGE python
SECURITY DEFINER
AS $$return [value for value in a.materialize_py()]$$;

CREATE FUNCTION py_builder_trusted(a INTEGER) RETURNS INTEGER
LANGUAGE python
CAPABILITY PROFILE trusted_subinterpreter
AS $$return [value for value in a.materialize_py()]$$;

-- @control connect user=alice
SELECT current_user();

CREATE FUNCTION py_reader_denied(a INTEGER) RETURNS INTEGER
LANGUAGE python
AS $$return [value for value in a.materialize_py()]$$;

SELECT py_builder_ok(1);

-- @control connect user=paro
SELECT current_user();

DROP FUNCTION py_builder_ok(INTEGER);
DROP FUNCTION IF EXISTS py_builder_definer(INTEGER);
DROP FUNCTION IF EXISTS py_builder_trusted(INTEGER);
DROP FUNCTION IF EXISTS py_reader_denied(INTEGER);

-- @control restart profile=default
