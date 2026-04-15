# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

CREATE TABLE ft_coverage_guard (id INT, content VARCHAR);

CREATE INDEX idx_ft_coverage_guard ON ft_coverage_guard USING GIN (to_tsvector('simple', content));

INSERT INTO ft_coverage_guard VALUES
    (1, 'vector after index'),
    (2, 'noise');

EXPLAIN
SELECT id
FROM ft_coverage_guard
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector')
ORDER BY id;

EXPLAIN
SELECT id
FROM ft_coverage_guard
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector')
ORDER BY ts_rank(
    to_tsvector('simple', content),
    plainto_tsquery('simple', 'vector')
) DESC
LIMIT 5;

SELECT id
FROM ft_coverage_guard
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector')
ORDER BY id;

DROP INDEX IF EXISTS idx_ft_coverage_guard;
DROP TABLE ft_coverage_guard;
