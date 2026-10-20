-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Python expressions compose with filters, outer joins, aggregates, windows, CTEs and scalar subqueries.

CREATE FUNCTION py_rel(a INTEGER) RETURNS INTEGER LANGUAGE python STRICT
AS $$
return a + 2
$$;

CREATE TABLE py_rel_data(id INTEGER, value INTEGER);

INSERT INTO py_rel_data VALUES (1, 4), (2, 9), (3, NULL), (4, 4);

SELECT id, py_rel(value) AS value FROM py_rel_data WHERE py_rel(value) > 6 ORDER BY py_rel(value), id;

SELECT l.id AS id, r.tag AS tag FROM py_rel_data l LEFT JOIN (VALUES (6, 'six'), (11, 'eleven')) r(k, tag) ON py_rel(l.value) = r.k ORDER BY l.id;

SELECT py_rel(value) AS bucket, count(*) AS n, sum(py_rel(value)) AS total FROM py_rel_data GROUP BY py_rel(value) HAVING count(*) > 1 ORDER BY bucket;

SELECT id, row_number() OVER (ORDER BY py_rel(value) NULLS LAST, id) AS rank FROM py_rel_data ORDER BY id;

WITH transformed AS (SELECT id, py_rel(value) AS value FROM py_rel_data) SELECT id, py_rel(value) AS value FROM transformed ORDER BY id;

SELECT py_rel((SELECT max(value) FROM py_rel_data)) AS value;

SELECT DISTINCT py_rel(value) AS value FROM py_rel_data ORDER BY value NULLS LAST;

DROP TABLE py_rel_data;

DROP FUNCTION py_rel(INTEGER);

-- @teardown
DROP TABLE IF EXISTS py_rel_data;

-- @teardown
DROP FUNCTION IF EXISTS py_rel(INTEGER);
