-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @fixture python_udf/modules/fake_numpy
-- @control connect user=paro

DROP FUNCTION IF EXISTS py_numpy_fast(INTEGER);

CREATE FUNCTION py_numpy_fast(a INTEGER) RETURNS INTEGER
LANGUAGE python
IMMUTABLE
IMPORTS ('{{fixture:python_udf/modules/fake_numpy}}/numpy.py')
AS $$
import numpy
return a.to_numpy() + 7
$$;

SELECT py_numpy_fast(v)
FROM (VALUES (4), (9)) AS t(v)
ORDER BY 1;

DROP FUNCTION py_numpy_fast(INTEGER);
