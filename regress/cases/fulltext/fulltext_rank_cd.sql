-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE ft_t024_rank_cd (
    id INT,
    content VARCHAR
);

INSERT INTO ft_t024_rank_cd VALUES
    (1, 'alpha beta'),
    (2, 'alpha x beta'),
    (3, 'beta alpha'),
    (4, 'alpha beta beta'),
    (5, 'gamma delta');

CREATE INDEX idx_ft_t024_rank_cd_fts
ON ft_t024_rank_cd USING GIN (to_tsvector('simple', content));

SELECT id,
       ts_rank(to_tsvector('simple', content), plainto_tsquery('simple', 'alpha beta')) AS rank,
       ts_rank_cd(to_tsvector('simple', content), plainto_tsquery('simple', 'alpha beta')) AS rank_cd
FROM ft_t024_rank_cd
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'alpha beta')
ORDER BY id;

EXPLAIN
SELECT id
FROM ft_t024_rank_cd
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'alpha beta')
ORDER BY ts_rank_cd(
    to_tsvector('simple', content),
    plainto_tsquery('simple', 'alpha beta')
) DESC,
id
LIMIT 2;

DROP INDEX IF EXISTS idx_ft_t024_rank_cd_fts;
DROP TABLE ft_t024_rank_cd;
