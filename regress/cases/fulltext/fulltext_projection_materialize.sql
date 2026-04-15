-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE ft_t012_projection (
    id INT,
    active BOOLEAN,
    title VARCHAR,
    score FLOAT,
    views BIGINT,
    content VARCHAR
);

INSERT INTO ft_t012_projection VALUES
    (1, true,  'Vector Intro', 1.5, 10, 'vector database vector'),
    (2, false, 'Graph Hybrid', 2.25, 20, 'vector graph database'),
    (3, true,  NULL,           3.75, 30, 'vector database'),
    (4, false, 'Noise',        9.0,  40, 'mountain river');

CREATE INDEX idx_ft_t012_projection_fts
ON ft_t012_projection USING GIN (to_tsvector('simple', content));

-- ScoreTopK path with mixed projection columns.
SELECT id, active, title, score, views
FROM ft_t012_projection
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY ts_rank(
    to_tsvector('simple', content),
    plainto_tsquery('simple', 'vector database')
) DESC, id
LIMIT 3;

-- Repeat TopK query to validate deterministic ordering.
SELECT id, active, title, score, views
FROM ft_t012_projection
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY ts_rank(
    to_tsvector('simple', content),
    plainto_tsquery('simple', 'vector database')
) DESC, id
LIMIT 3;

-- Filter path should preserve the same projection values and NULL title.
SELECT id, active, title, score, views
FROM ft_t012_projection
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY id;

DROP INDEX IF EXISTS idx_ft_t012_projection_fts;
DROP TABLE ft_t012_projection;
