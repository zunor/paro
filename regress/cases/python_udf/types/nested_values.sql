-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Distinguish parent NULL, empty containers and nullable children, including nested containers.

CREATE FUNCTION py_nested_vector_scalar(a VECTOR(3)) RETURNS VECTOR(3) LANGUAGE python
AS $$
return a
$$;

SELECT py_nested_vector_scalar('[1.25,-2,0]'::VECTOR(3)) IS NOT DISTINCT FROM '[1.25,-2,0]'::VECTOR(3) AS preserved;

SELECT py_nested_vector_scalar('[0,0,0]'::VECTOR(3)) IS NOT DISTINCT FROM '[0,0,0]'::VECTOR(3) AS preserved;

SELECT py_nested_vector_scalar(NULL::VECTOR(3)) IS NOT DISTINCT FROM NULL::VECTOR(3) AS preserved;

DROP FUNCTION py_nested_vector_scalar(VECTOR(3));

CREATE FUNCTION py_nested_vector_vector(a VECTOR(3)) RETURNS VECTOR(3) LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_nested_vector_vector('[1.25,-2,0]'::VECTOR(3)) IS NOT DISTINCT FROM '[1.25,-2,0]'::VECTOR(3) AS preserved;

SELECT py_nested_vector_vector('[0,0,0]'::VECTOR(3)) IS NOT DISTINCT FROM '[0,0,0]'::VECTOR(3) AS preserved;

SELECT py_nested_vector_vector(NULL::VECTOR(3)) IS NOT DISTINCT FROM NULL::VECTOR(3) AS preserved;

DROP FUNCTION py_nested_vector_vector(VECTOR(3));

CREATE FUNCTION py_nested_list_scalar(a LIST(INTEGER)) RETURNS LIST(INTEGER) LANGUAGE python
AS $$
return a
$$;

SELECT py_nested_list_scalar([]::LIST(INTEGER)) IS NOT DISTINCT FROM []::LIST(INTEGER) AS preserved;

SELECT py_nested_list_scalar([NULL]::LIST(INTEGER)) IS NOT DISTINCT FROM [NULL]::LIST(INTEGER) AS preserved;

SELECT py_nested_list_scalar([4, NULL, -9]::LIST(INTEGER)) IS NOT DISTINCT FROM [4, NULL, -9]::LIST(INTEGER) AS preserved;

SELECT py_nested_list_scalar(NULL::LIST(INTEGER)) IS NOT DISTINCT FROM NULL::LIST(INTEGER) AS preserved;

DROP FUNCTION py_nested_list_scalar(LIST(INTEGER));

CREATE FUNCTION py_nested_list_vector(a LIST(INTEGER)) RETURNS LIST(INTEGER) LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_nested_list_vector([]::LIST(INTEGER)) IS NOT DISTINCT FROM []::LIST(INTEGER) AS preserved;

SELECT py_nested_list_vector([NULL]::LIST(INTEGER)) IS NOT DISTINCT FROM [NULL]::LIST(INTEGER) AS preserved;

SELECT py_nested_list_vector([4, NULL, -9]::LIST(INTEGER)) IS NOT DISTINCT FROM [4, NULL, -9]::LIST(INTEGER) AS preserved;

SELECT py_nested_list_vector(NULL::LIST(INTEGER)) IS NOT DISTINCT FROM NULL::LIST(INTEGER) AS preserved;

DROP FUNCTION py_nested_list_vector(LIST(INTEGER));

CREATE FUNCTION py_nested_array_scalar(a ARRAY(INTEGER)) RETURNS ARRAY(INTEGER) LANGUAGE python
AS $$
return a
$$;

SELECT py_nested_array_scalar([]::ARRAY(INTEGER)) IS NOT DISTINCT FROM []::ARRAY(INTEGER) AS preserved;

SELECT py_nested_array_scalar([2, NULL, 7]::ARRAY(INTEGER)) IS NOT DISTINCT FROM [2, NULL, 7]::ARRAY(INTEGER) AS preserved;

SELECT py_nested_array_scalar(NULL::ARRAY(INTEGER)) IS NOT DISTINCT FROM NULL::ARRAY(INTEGER) AS preserved;

DROP FUNCTION py_nested_array_scalar(ARRAY(INTEGER));

CREATE FUNCTION py_nested_array_vector(a ARRAY(INTEGER)) RETURNS ARRAY(INTEGER) LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_nested_array_vector([]::ARRAY(INTEGER)) IS NOT DISTINCT FROM []::ARRAY(INTEGER) AS preserved;

SELECT py_nested_array_vector([2, NULL, 7]::ARRAY(INTEGER)) IS NOT DISTINCT FROM [2, NULL, 7]::ARRAY(INTEGER) AS preserved;

SELECT py_nested_array_vector(NULL::ARRAY(INTEGER)) IS NOT DISTINCT FROM NULL::ARRAY(INTEGER) AS preserved;

DROP FUNCTION py_nested_array_vector(ARRAY(INTEGER));

CREATE FUNCTION py_nested_nested_scalar(a LIST(LIST(INTEGER))) RETURNS LIST(LIST(INTEGER)) LANGUAGE python
AS $$
return a
$$;

SELECT py_nested_nested_scalar([[], [1, NULL], NULL]::LIST(LIST(INTEGER))) IS NOT DISTINCT FROM [[], [1, NULL], NULL]::LIST(LIST(INTEGER)) AS preserved;

SELECT py_nested_nested_scalar(NULL::LIST(LIST(INTEGER))) IS NOT DISTINCT FROM NULL::LIST(LIST(INTEGER)) AS preserved;

DROP FUNCTION py_nested_nested_scalar(LIST(LIST(INTEGER)));

CREATE FUNCTION py_nested_nested_vector(a LIST(LIST(INTEGER))) RETURNS LIST(LIST(INTEGER)) LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_nested_nested_vector([[], [1, NULL], NULL]::LIST(LIST(INTEGER))) IS NOT DISTINCT FROM [[], [1, NULL], NULL]::LIST(LIST(INTEGER)) AS preserved;

SELECT py_nested_nested_vector(NULL::LIST(LIST(INTEGER))) IS NOT DISTINCT FROM NULL::LIST(LIST(INTEGER)) AS preserved;

DROP FUNCTION py_nested_nested_vector(LIST(LIST(INTEGER)));

CREATE FUNCTION py_nested_record_scalar(a STRUCT(label VARCHAR, value INTEGER)) RETURNS STRUCT(label VARCHAR, value INTEGER) LANGUAGE python
AS $$
return a
$$;

SELECT py_nested_record_scalar(('雪', 7)::STRUCT(label VARCHAR, value INTEGER)) IS NOT DISTINCT FROM ('雪', 7)::STRUCT(label VARCHAR, value INTEGER) AS preserved;

SELECT py_nested_record_scalar(('', NULL)::STRUCT(label VARCHAR, value INTEGER)) IS NOT DISTINCT FROM ('', NULL)::STRUCT(label VARCHAR, value INTEGER) AS preserved;

SELECT py_nested_record_scalar(NULL::STRUCT(label VARCHAR, value INTEGER)) IS NOT DISTINCT FROM NULL::STRUCT(label VARCHAR, value INTEGER) AS preserved;

DROP FUNCTION py_nested_record_scalar(STRUCT(label VARCHAR, value INTEGER));

CREATE FUNCTION py_nested_record_vector(a STRUCT(label VARCHAR, value INTEGER)) RETURNS STRUCT(label VARCHAR, value INTEGER) LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_nested_record_vector(('雪', 7)::STRUCT(label VARCHAR, value INTEGER)) IS NOT DISTINCT FROM ('雪', 7)::STRUCT(label VARCHAR, value INTEGER) AS preserved;

SELECT py_nested_record_vector(('', NULL)::STRUCT(label VARCHAR, value INTEGER)) IS NOT DISTINCT FROM ('', NULL)::STRUCT(label VARCHAR, value INTEGER) AS preserved;

SELECT py_nested_record_vector(NULL::STRUCT(label VARCHAR, value INTEGER)) IS NOT DISTINCT FROM NULL::STRUCT(label VARCHAR, value INTEGER) AS preserved;

DROP FUNCTION py_nested_record_vector(STRUCT(label VARCHAR, value INTEGER));

-- @teardown
DROP FUNCTION IF EXISTS py_nested_record_vector(STRUCT(label VARCHAR, value INTEGER));

-- @teardown
DROP FUNCTION IF EXISTS py_nested_record_scalar(STRUCT(label VARCHAR, value INTEGER));

-- @teardown
DROP FUNCTION IF EXISTS py_nested_nested_vector(LIST(LIST(INTEGER)));

-- @teardown
DROP FUNCTION IF EXISTS py_nested_nested_scalar(LIST(LIST(INTEGER)));

-- @teardown
DROP FUNCTION IF EXISTS py_nested_array_vector(ARRAY(INTEGER));

-- @teardown
DROP FUNCTION IF EXISTS py_nested_array_scalar(ARRAY(INTEGER));

-- @teardown
DROP FUNCTION IF EXISTS py_nested_list_vector(LIST(INTEGER));

-- @teardown
DROP FUNCTION IF EXISTS py_nested_list_scalar(LIST(INTEGER));

-- @teardown
DROP FUNCTION IF EXISTS py_nested_vector_vector(VECTOR(3));

-- @teardown
DROP FUNCTION IF EXISTS py_nested_vector_scalar(VECTOR(3));
