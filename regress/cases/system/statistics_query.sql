# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS stats_small;
DROP TABLE IF EXISTS stats_big;

CREATE TABLE stats_small (
    id INT PRIMARY KEY,
    payload VARCHAR
);

CREATE TABLE stats_big (
    id INT PRIMARY KEY,
    payload VARCHAR
);

INSERT INTO stats_small VALUES
    (1, 'small');

INSERT INTO stats_big VALUES
    (1, 'alpha'),
    (2, 'beta'),
    (3, 'gamma'),
    (4, 'delta'),
    (5, 'epsilon');

SELECT table_name, estimated_rows
FROM paro_tables()
WHERE table_name IN ('stats_big', 'stats_small')
ORDER BY table_name;

EXPLAIN (VERBOSE)
SELECT s.id, b.payload
FROM stats_small AS s
JOIN stats_big AS b ON s.id = b.id;

SELECT s.id, b.payload
FROM stats_small AS s
JOIN stats_big AS b ON s.id = b.id
ORDER BY s.id;

DROP TABLE stats_small;
DROP TABLE stats_big;
