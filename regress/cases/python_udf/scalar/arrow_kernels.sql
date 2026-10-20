-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Real Arrow kernels operate on nullable, filtered and sliced arrays, including chunked output.

CREATE FUNCTION py_kernel(a INTEGER, b INTEGER) RETURNS INTEGER LANGUAGE python MODE VECTOR
AS $$
import pyarrow as pa
import pyarrow.compute as pc
return pc.add(pc.fill_null(a, 0), pc.fill_null(b, 10)).cast(pa.int32())
$$;

SELECT id, py_kernel(a, b) AS value FROM (VALUES (1, 2, 4), (2, NULL, 5), (3, 8, NULL), (4, NULL, NULL)) t(id, a, b) WHERE id > 1 ORDER BY id;

CREATE FUNCTION py_unicode(a VARCHAR) RETURNS VARCHAR LANGUAGE python MODE VECTOR
AS $$
import pyarrow.compute as pc
return pc.utf8_upper(a)
$$;

SELECT id, py_unicode(v) AS value FROM (VALUES (1, 'café'), (2, '雪🙂'), (3, ''), (4, NULL)) t(id, v) ORDER BY id;

CREATE FUNCTION py_chunks(a INTEGER) RETURNS INTEGER LANGUAGE python MODE VECTOR
AS $$
import pyarrow as pa
return pa.chunked_array([a.slice(0, 1), a.slice(1)], type=pa.int32())
$$;

SELECT id, py_chunks(v) AS value FROM (VALUES (1, 8), (2, NULL), (3, -2)) t(id, v) ORDER BY id;

CREATE FUNCTION py_slice(a INTEGER) RETURNS INTEGER LANGUAGE python MODE VECTOR
AS $$
import pyarrow as pa
return pa.concat_arrays([pa.array([-999], type=pa.int32()), a]).slice(1)
$$;

SELECT id, py_slice(v) AS value FROM (VALUES (1, 8), (2, NULL), (3, -2)) t(id, v) ORDER BY id;

DROP FUNCTION py_kernel(INTEGER, INTEGER);

DROP FUNCTION py_unicode(VARCHAR);

DROP FUNCTION py_chunks(INTEGER);

DROP FUNCTION py_slice(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_slice(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_chunks(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_unicode(VARCHAR);

-- @teardown
DROP FUNCTION IF EXISTS py_kernel(INTEGER, INTEGER);
