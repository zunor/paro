-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @fixture python_udf/modules/basic_math
-- @control connect user=paro

DROP FUNCTION IF EXISTS py_imported(INTEGER);

-- @normalize regress_paths
CREATE FUNCTION py_imported(a INTEGER) RETURNS INTEGER
LANGUAGE python
IMMUTABLE
IMPORTS ('{{fixture:python_udf/modules/basic_math}}/basic_math.py')
AS $$
from basic_math import shift_all
return shift_all(a.materialize_py())
$$;

SELECT py_imported(v)
FROM (VALUES (1), (2), (3)) AS t(v)
ORDER BY 1;

DROP FUNCTION py_imported(INTEGER);
