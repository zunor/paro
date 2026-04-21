-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

DROP FUNCTION IF EXISTS py_arrow_capsule(INTEGER);

CREATE FUNCTION py_arrow_capsule(a INTEGER) RETURNS INTEGER
LANGUAGE python
IMMUTABLE
AS $$return a.__arrow_c_array__()$$;

SELECT py_arrow_capsule(v)
FROM (VALUES (8), (13)) AS t(v)
ORDER BY 1;

DROP FUNCTION py_arrow_capsule(INTEGER);
