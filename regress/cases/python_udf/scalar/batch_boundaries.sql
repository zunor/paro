-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Cross native batch boundaries with nullable rows and constant arguments; compare with a native oracle.

CREATE FUNCTION py_batch(a INTEGER, b INTEGER) RETURNS INTEGER LANGUAGE python STRICT MODE VECTOR
AS $$
import pyarrow.compute as pc
assert len(a) == len(b) == ctx.num_rows
assert a.null_count == b.null_count == 0
return pc.add(a, b)
$$;

SELECT count(*) AS rows, count(value) AS nonnull, sum(value) AS total
FROM (SELECT py_batch(CASE WHEN i % 7 = 0 THEN NULL ELSE i::INTEGER END, 3) AS value FROM generate_series(1, 1) t(i)) q;

SELECT count(*) AS rows, count(value) AS nonnull, sum(value) AS total
FROM (SELECT py_batch(CASE WHEN i % 7 = 0 THEN NULL ELSE i::INTEGER END, 3) AS value FROM generate_series(1, 1023) t(i)) q;

SELECT count(*) AS rows, count(value) AS nonnull, sum(value) AS total
FROM (SELECT py_batch(CASE WHEN i % 7 = 0 THEN NULL ELSE i::INTEGER END, 3) AS value FROM generate_series(1, 1024) t(i)) q;

SELECT count(*) AS rows, count(value) AS nonnull, sum(value) AS total
FROM (SELECT py_batch(CASE WHEN i % 7 = 0 THEN NULL ELSE i::INTEGER END, 3) AS value FROM generate_series(1, 1025) t(i)) q;

SELECT count(*) AS rows, count(value) AS nonnull, sum(value) AS total
FROM (SELECT py_batch(CASE WHEN i % 7 = 0 THEN NULL ELSE i::INTEGER END, 3) AS value FROM generate_series(1, 4097) t(i)) q;

DROP FUNCTION py_batch(INTEGER, INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_batch(INTEGER, INTEGER);
