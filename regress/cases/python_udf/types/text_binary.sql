-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Empty, Unicode, JSON, UUID and binary values retain their distinct NULL semantics.

CREATE FUNCTION py_value_text_scalar(a VARCHAR) RETURNS VARCHAR LANGUAGE python
AS $$
return a
$$;

SELECT py_value_text_scalar(''::VARCHAR) IS NOT DISTINCT FROM ''::VARCHAR AS preserved;

SELECT py_value_text_scalar('雪🙂 café'::VARCHAR) IS NOT DISTINCT FROM '雪🙂 café'::VARCHAR AS preserved;

SELECT py_value_text_scalar('NULL'::VARCHAR) IS NOT DISTINCT FROM 'NULL'::VARCHAR AS preserved;

SELECT py_value_text_scalar('a  b'::VARCHAR) IS NOT DISTINCT FROM 'a  b'::VARCHAR AS preserved;

SELECT py_value_text_scalar(NULL::VARCHAR) IS NOT DISTINCT FROM NULL::VARCHAR AS preserved;

DROP FUNCTION py_value_text_scalar(VARCHAR);

CREATE FUNCTION py_value_text_vector(a VARCHAR) RETURNS VARCHAR LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_value_text_vector(''::VARCHAR) IS NOT DISTINCT FROM ''::VARCHAR AS preserved;

SELECT py_value_text_vector('雪🙂 café'::VARCHAR) IS NOT DISTINCT FROM '雪🙂 café'::VARCHAR AS preserved;

SELECT py_value_text_vector('NULL'::VARCHAR) IS NOT DISTINCT FROM 'NULL'::VARCHAR AS preserved;

SELECT py_value_text_vector('a  b'::VARCHAR) IS NOT DISTINCT FROM 'a  b'::VARCHAR AS preserved;

SELECT py_value_text_vector(NULL::VARCHAR) IS NOT DISTINCT FROM NULL::VARCHAR AS preserved;

DROP FUNCTION py_value_text_vector(VARCHAR);

CREATE FUNCTION py_value_blob_scalar(a BLOB) RETURNS BLOB LANGUAGE python
AS $$
return a
$$;

SELECT py_value_blob_scalar(''::BLOB) IS NOT DISTINCT FROM ''::BLOB AS preserved;

SELECT py_value_blob_scalar('binary_0123'::BLOB) IS NOT DISTINCT FROM 'binary_0123'::BLOB AS preserved;

SELECT py_value_blob_scalar(NULL::BLOB) IS NOT DISTINCT FROM NULL::BLOB AS preserved;

DROP FUNCTION py_value_blob_scalar(BLOB);

CREATE FUNCTION py_value_blob_vector(a BLOB) RETURNS BLOB LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_value_blob_vector(''::BLOB) IS NOT DISTINCT FROM ''::BLOB AS preserved;

SELECT py_value_blob_vector('binary_0123'::BLOB) IS NOT DISTINCT FROM 'binary_0123'::BLOB AS preserved;

SELECT py_value_blob_vector(NULL::BLOB) IS NOT DISTINCT FROM NULL::BLOB AS preserved;

DROP FUNCTION py_value_blob_vector(BLOB);

CREATE FUNCTION py_value_json_scalar(a JSON) RETURNS JSON LANGUAGE python
AS $$
return a
$$;

SELECT py_value_json_scalar('{"items":[1,true,null],"label":"雪"}'::JSON) IS NOT DISTINCT FROM '{"items":[1,true,null],"label":"雪"}'::JSON AS preserved;

SELECT py_value_json_scalar('null'::JSON) IS NOT DISTINCT FROM 'null'::JSON AS preserved;

SELECT py_value_json_scalar('[]'::JSON) IS NOT DISTINCT FROM '[]'::JSON AS preserved;

SELECT py_value_json_scalar(NULL::JSON) IS NOT DISTINCT FROM NULL::JSON AS preserved;

DROP FUNCTION py_value_json_scalar(JSON);

CREATE FUNCTION py_value_json_vector(a JSON) RETURNS JSON LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_value_json_vector('{"items":[1,true,null],"label":"雪"}'::JSON) IS NOT DISTINCT FROM '{"items":[1,true,null],"label":"雪"}'::JSON AS preserved;

SELECT py_value_json_vector('null'::JSON) IS NOT DISTINCT FROM 'null'::JSON AS preserved;

SELECT py_value_json_vector('[]'::JSON) IS NOT DISTINCT FROM '[]'::JSON AS preserved;

SELECT py_value_json_vector(NULL::JSON) IS NOT DISTINCT FROM NULL::JSON AS preserved;

DROP FUNCTION py_value_json_vector(JSON);

CREATE FUNCTION py_value_jsonb_scalar(a JSONB) RETURNS JSONB LANGUAGE python
AS $$
return a
$$;

SELECT py_value_jsonb_scalar('{"a":1}'::JSONB) IS NOT DISTINCT FROM '{"a":1}'::JSONB AS preserved;

SELECT py_value_jsonb_scalar(NULL::JSONB) IS NOT DISTINCT FROM NULL::JSONB AS preserved;

DROP FUNCTION py_value_jsonb_scalar(JSONB);

CREATE FUNCTION py_value_jsonb_vector(a JSONB) RETURNS JSONB LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_value_jsonb_vector('{"a":1}'::JSONB) IS NOT DISTINCT FROM '{"a":1}'::JSONB AS preserved;

SELECT py_value_jsonb_vector(NULL::JSONB) IS NOT DISTINCT FROM NULL::JSONB AS preserved;

DROP FUNCTION py_value_jsonb_vector(JSONB);

CREATE FUNCTION py_value_uuid_scalar(a UUID) RETURNS UUID LANGUAGE python
AS $$
return a
$$;

SELECT py_value_uuid_scalar('00112233-4455-6677-8899-aabbccddeeff'::UUID) IS NOT DISTINCT FROM '00112233-4455-6677-8899-aabbccddeeff'::UUID AS preserved;

SELECT py_value_uuid_scalar('ffffffff-ffff-ffff-ffff-ffffffffffff'::UUID) IS NOT DISTINCT FROM 'ffffffff-ffff-ffff-ffff-ffffffffffff'::UUID AS preserved;

SELECT py_value_uuid_scalar(NULL::UUID) IS NOT DISTINCT FROM NULL::UUID AS preserved;

DROP FUNCTION py_value_uuid_scalar(UUID);

CREATE FUNCTION py_value_uuid_vector(a UUID) RETURNS UUID LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_value_uuid_vector('00112233-4455-6677-8899-aabbccddeeff'::UUID) IS NOT DISTINCT FROM '00112233-4455-6677-8899-aabbccddeeff'::UUID AS preserved;

SELECT py_value_uuid_vector('ffffffff-ffff-ffff-ffff-ffffffffffff'::UUID) IS NOT DISTINCT FROM 'ffffffff-ffff-ffff-ffff-ffffffffffff'::UUID AS preserved;

SELECT py_value_uuid_vector(NULL::UUID) IS NOT DISTINCT FROM NULL::UUID AS preserved;

DROP FUNCTION py_value_uuid_vector(UUID);

-- @teardown
DROP FUNCTION IF EXISTS py_value_uuid_vector(UUID);

-- @teardown
DROP FUNCTION IF EXISTS py_value_uuid_scalar(UUID);

-- @teardown
DROP FUNCTION IF EXISTS py_value_jsonb_vector(JSONB);

-- @teardown
DROP FUNCTION IF EXISTS py_value_jsonb_scalar(JSONB);

-- @teardown
DROP FUNCTION IF EXISTS py_value_json_vector(JSON);

-- @teardown
DROP FUNCTION IF EXISTS py_value_json_scalar(JSON);

-- @teardown
DROP FUNCTION IF EXISTS py_value_blob_vector(BLOB);

-- @teardown
DROP FUNCTION IF EXISTS py_value_blob_scalar(BLOB);

-- @teardown
DROP FUNCTION IF EXISTS py_value_text_vector(VARCHAR);

-- @teardown
DROP FUNCTION IF EXISTS py_value_text_scalar(VARCHAR);
