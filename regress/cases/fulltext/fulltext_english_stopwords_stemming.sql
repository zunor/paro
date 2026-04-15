# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

CREATE TABLE ft_t022_english_docs (
    id INT,
    content VARCHAR
);

INSERT INTO ft_t022_english_docs VALUES
    (1, 'The databases are running quickly'),
    (2, 'A database runs quick'),
    (3, 'the and in of'),
    (4, 'mountain river');

CREATE INDEX idx_ft_t022_english_docs_fts
ON ft_t022_english_docs USING GIN (to_tsvector('english', content));

SELECT id
FROM ft_t022_english_docs
WHERE to_tsvector('english', content) @@ plainto_tsquery('english', 'the databases are running quickly')
ORDER BY id;

SELECT id
FROM ft_t022_english_docs
WHERE to_tsvector('english', content) @@ plainto_tsquery('english', 'database run quick')
ORDER BY id;

SELECT id
FROM ft_t022_english_docs
WHERE to_tsvector('english', content) @@ plainto_tsquery('english', 'river')
ORDER BY id;

DROP INDEX IF EXISTS idx_ft_t022_english_docs_fts;
DROP TABLE ft_t022_english_docs;
