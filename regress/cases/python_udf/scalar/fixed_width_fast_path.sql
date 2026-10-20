-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

DROP FUNCTION IF EXISTS py_numpy_fast(INTEGER);

CREATE FUNCTION py_numpy_fast(a INTEGER) RETURNS INTEGER
LANGUAGE python
IMMUTABLE
MODE VECTOR
AS $$
import pyarrow as pa
return pa.array(a.to_numpy() + 7, type=pa.int32())
$$;

SELECT py_numpy_fast(v)
FROM (VALUES (4), (9)) AS t(v)
ORDER BY 1;

DROP FUNCTION py_numpy_fast(INTEGER);
