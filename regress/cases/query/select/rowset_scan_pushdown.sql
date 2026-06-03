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
