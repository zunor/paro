-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS explain_cardinality_case;

CREATE TABLE explain_cardinality_case (
    id INT PRIMARY KEY,
    score INT,
    grp INT
);

INSERT INTO explain_cardinality_case VALUES
    (1, 10, 1),
    (2, 30, 1),
    (3, 20, 1),
    (4, 40, 2),
    (5, 15, 2),
    (6, 25, 2);

EXPLAIN (VERBOSE)
SELECT id
FROM explain_cardinality_case
WHERE score >= 20
ORDER BY score DESC, id
LIMIT 2;

-- @query json
EXPLAIN
SELECT id
FROM explain_cardinality_case
WHERE score >= 20
ORDER BY score DESC, id
LIMIT 2
FORMAT JSON;

SELECT id
FROM explain_cardinality_case
WHERE score >= 20
ORDER BY score DESC, id
LIMIT 2;

DROP TABLE explain_cardinality_case;
