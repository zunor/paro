CREATE TABLE ft_global_stats_docs (id INT, content VARCHAR);

-- Segment A: a tiny segment with one matching row.
INSERT INTO ft_global_stats_docs VALUES (100, 'vector');

-- Segment B: a much larger segment with one matching row and many non-matching rows.
INSERT INTO ft_global_stats_docs VALUES
    (1, 'vector'),
    (2, 'filler'),
    (3, 'filler'),
    (4, 'filler'),
    (5, 'filler'),
    (6, 'filler'),
    (7, 'filler'),
    (8, 'filler'),
    (9, 'filler'),
    (10, 'filler'),
    (11, 'filler'),
    (12, 'filler'),
    (13, 'filler'),
    (14, 'filler'),
    (15, 'filler'),
    (16, 'filler'),
    (17, 'filler'),
    (18, 'filler'),
    (19, 'filler'),
    (20, 'filler');

CREATE INDEX idx_ft_global_stats_docs ON ft_global_stats_docs USING GIN (to_tsvector('simple', content));

-- With global full-text stats enabled, the two matching rows should have equal score.
-- Tie-break on id should put id=1 before id=100.
SELECT id
FROM ft_global_stats_docs
WHERE fulltext_match(content, 'vector')
ORDER BY bm25(content, 'vector') DESC, id
LIMIT 2;

DROP INDEX IF EXISTS idx_ft_global_stats_docs;
DROP TABLE ft_global_stats_docs;
