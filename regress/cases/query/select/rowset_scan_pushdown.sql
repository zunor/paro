-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS rowset_pushdown_dim;
DROP TABLE IF EXISTS rowset_pushdown_case;

CREATE TABLE rowset_pushdown_case(
    id INT,
    k INT,
    nullable_k INT,
    score INT,
    payload VARCHAR
);

INSERT INTO rowset_pushdown_case
SELECT
    g,
    g % 10,
    CASE WHEN g % 8 = 0 THEN NULL ELSE g % 7 END,
    65 - g,
    'payload_' || CAST(g AS VARCHAR)
FROM generate_series(1, 64) AS t(g);

CREATE TABLE rowset_pushdown_dim(k INT);
INSERT INTO rowset_pushdown_dim VALUES (2), (5), (8);

-- Cardinality-only scans must not materialize an arbitrary payload column.
SELECT COUNT(*) FROM rowset_pushdown_case;
SELECT COUNT(*) FROM rowset_pushdown_case WHERE k = 3;
SELECT COUNT(*) FROM rowset_pushdown_case WHERE k >= 2 AND k < 5;
SELECT COUNT(*) FROM rowset_pushdown_case WHERE k IN (1, 4, 9);
SELECT COUNT(*) FROM rowset_pushdown_case WHERE nullable_k IS NULL;

EXPLAIN (VERBOSE)
SELECT COUNT(*) FROM rowset_pushdown_case WHERE k = 3;

EXPLAIN (VERBOSE)
SELECT payload FROM rowset_pushdown_case WHERE k = 3;

EXPLAIN (VERBOSE)
SELECT COUNT(*) FROM rowset_pushdown_case WHERE k >= 2 AND k < 5;

EXPLAIN (VERBOSE)
SELECT COUNT(*) FROM rowset_pushdown_case WHERE k IN (1, 4, 9);

EXPLAIN (VERBOSE)
SELECT COUNT(*) FROM rowset_pushdown_case WHERE nullable_k IS NULL;

EXPLAIN (VERBOSE)
SELECT COUNT(*)
FROM rowset_pushdown_case c
JOIN rowset_pushdown_dim d ON c.k = d.k;

EXPLAIN (VERBOSE)
SELECT id FROM rowset_pushdown_case ORDER BY score ASC LIMIT 5;

DROP TABLE rowset_pushdown_dim;
DROP TABLE rowset_pushdown_case;

-- A derived matched-prefix scan output must remain distinct from its stored
-- source across optimizer, physical predicate witnessing, and page decoding.
DROP TABLE IF EXISTS matched_prefix_projection_case;
CREATE TABLE matched_prefix_projection_case(id INT, phone VARCHAR, note VARCHAR);
INSERT INTO matched_prefix_projection_case VALUES
    (1, '13alpha', 'keep'),
    (2, '31beta', 'keep'),
    (3, '13', 'keep'),
    (4, '1', 'short'),
    (5, 'éclair', 'non-ascii'),
    (6, NULL, 'null'),
    (7, '44gamma', 'other');

SELECT id, substring(phone FROM 1 FOR 2) AS prefix
FROM matched_prefix_projection_case
WHERE substring(phone FROM 1 FOR 2) IN ('13', '31')
ORDER BY id;

SET rowset_scan_pushdown = false;
SELECT id, substring(phone FROM 1 FOR 2) AS prefix
FROM matched_prefix_projection_case
WHERE substring(phone FROM 1 FOR 2) IN ('13', '31')
ORDER BY id;
SET rowset_scan_pushdown = DEFAULT;

-- The prefix witness may coexist with a residual expression that cannot be
-- represented by the rowset predicate tree.
SELECT id, substring(phone FROM 1 FOR 2) AS prefix
FROM matched_prefix_projection_case
WHERE substring(phone FROM 1 FOR 2) IN ('13', '31')
  AND id % 2 = 1
ORDER BY id;

DROP TABLE matched_prefix_projection_case;

-- Hidden ORDER BY columns are an execution-only suffix, not malformed
-- projection metadata. A compact derived prefix must also coexist with late
-- fetching of an unrelated wide stored value.
DROP TABLE IF EXISTS matched_prefix_late_payload_case;
CREATE TABLE matched_prefix_late_payload_case(
    id INT,
    phone VARCHAR,
    wide VARCHAR
);
INSERT INTO matched_prefix_late_payload_case
SELECT
    g,
    CASE WHEN g % 3 = 0 THEN '44z' WHEN g % 2 = 0 THEN '31y' ELSE '13x' END,
    'wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww'
        || CAST(g AS VARCHAR)
FROM generate_series(1, 5000) AS t(g);

EXPLAIN (VERBOSE)
SELECT phone, wide
FROM matched_prefix_late_payload_case
WHERE phone IN ('13x', '31y')
ORDER BY id
LIMIT 3;

SELECT phone, wide
FROM matched_prefix_late_payload_case
WHERE phone IN ('13x', '31y')
ORDER BY id
LIMIT 3;

EXPLAIN (VERBOSE)
SELECT id, substring(phone FROM 1 FOR 2) AS prefix, wide
FROM matched_prefix_late_payload_case
WHERE substring(phone FROM 1 FOR 2) IN ('13', '31')
ORDER BY id
LIMIT 3;

SELECT id, substring(phone FROM 1 FOR 2) AS prefix, wide
FROM matched_prefix_late_payload_case
WHERE substring(phone FROM 1 FOR 2) IN ('13', '31')
ORDER BY id
LIMIT 3;

DROP TABLE matched_prefix_late_payload_case;
