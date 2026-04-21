-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

CREATE OR REPLACE FUNCTION py_add(a INTEGER, b INTEGER) RETURNS INTEGER
LANGUAGE python
IMMUTABLE
AS $$return a + b$$;

DROP FUNCTION py_add(INTEGER, INTEGER);
