-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

DROP FUNCTION IF EXISTS py_expand(INTEGER);

CREATE FUNCTION py_expand(a INTEGER) RETURNS TABLE (value INTEGER)
LANGUAGE python
AS $$
import pyarrow as pa
output = []
for value in a.to_pylist():
    output.extend((value, value + 100))
return pa.RecordBatch.from_arrays([pa.array(output, type=pa.int32())], names=["value"])
$$;

SELECT *
FROM py_expand(1)
ORDER BY 1;

DROP FUNCTION py_expand(INTEGER);
