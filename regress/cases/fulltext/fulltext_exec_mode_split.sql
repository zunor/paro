CREATE TABLE ft_exec_mode_split (
    id INT,
    content VARCHAR
);

INSERT INTO ft_exec_mode_split VALUES
    (1, 'vector database vector'),
    (2, 'vector database'),
    (3, 'database vector'),
    (4, 'vector'),
    (5, 'noise');

CREATE INDEX idx_ft_exec_mode_split ON ft_exec_mode_split USING GIN (to_tsvector('simple', content));

-- Filter mode should not expose usize::MAX as estimated rows.
EXPLAIN
SELECT id
FROM ft_exec_mode_split
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database');

-- TopK mode should retain LIMIT-based cardinality.
EXPLAIN
SELECT id
FROM ft_exec_mode_split
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY ts_rank(
    to_tsvector('simple', content),
    plainto_tsquery('simple', 'vector database')
) DESC
LIMIT 2;

SELECT id
FROM ft_exec_mode_split
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY id;

DROP INDEX IF EXISTS idx_ft_exec_mode_split;
DROP TABLE ft_exec_mode_split;
