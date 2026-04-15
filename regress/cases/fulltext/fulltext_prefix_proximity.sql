CREATE TABLE ft_t023_prefix_proximity (
    id INT,
    content VARCHAR
);

INSERT INTO ft_t023_prefix_proximity VALUES
    (1, 'alpha beta gamma vector'),
    (2, 'alpha x gamma vectors'),
    (3, 'alpha y z gamma'),
    (4, 'vectorization only'),
    (5, 'graph data');

CREATE INDEX idx_ft_t023_prefix_proximity_fts
ON ft_t023_prefix_proximity USING GIN (to_tsvector('simple', content));

SELECT id
FROM ft_t023_prefix_proximity
WHERE to_tsvector('simple', content) @@ to_tsquery('simple', 'vec:*')
ORDER BY id;

SELECT id
FROM ft_t023_prefix_proximity
WHERE to_tsvector('simple', content) @@ to_tsquery('simple', 'alpha <2> gamma')
ORDER BY id;

SELECT id
FROM ft_t023_prefix_proximity
WHERE to_tsvector('simple', content) @@ to_tsquery('simple', 'alpha <3> gamma')
ORDER BY id;

SELECT id
FROM ft_t023_prefix_proximity
WHERE to_tsvector('simple', content) @@ to_tsquery('simple', 'alpha <-> beta <-> gamma')
ORDER BY id;

DROP INDEX IF EXISTS idx_ft_t023_prefix_proximity_fts;
DROP TABLE ft_t023_prefix_proximity;
