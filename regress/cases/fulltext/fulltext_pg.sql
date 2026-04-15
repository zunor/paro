-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE ft_docs (
    id INT,
    category VARCHAR,
    title VARCHAR,
    content VARCHAR
);

INSERT INTO ft_docs VALUES
    (1, 'tech', 'Vector Intro', 'vector database vector'),
    (2, 'tech', 'Graph Hybrid', 'vector graph database'),
    (3, 'spam', NULL, 'vector database spam'),
    (4, 'life', 'Travel Note', 'mountain river');

CREATE INDEX idx_ft_docs_fts ON ft_docs USING GIN (to_tsvector('simple', content));

-- to_tsvector @@ plainto_tsquery
SELECT id
FROM ft_docs
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY id;

-- to_tsquery boolean expression (&, |, !)
SELECT id
FROM ft_docs
WHERE to_tsvector('simple', content) @@ to_tsquery('simple', '(vector & database) | graph')
ORDER BY id;

-- pure NOT query
SELECT id
FROM ft_docs
WHERE to_tsvector('simple', content) @@ to_tsquery('simple', '!spam')
ORDER BY id;

-- phraseto_tsquery
SELECT id
FROM ft_docs
WHERE to_tsvector('simple', content) @@ phraseto_tsquery('simple', 'vector database')
ORDER BY id;

-- websearch_to_tsquery
SELECT id
FROM ft_docs
WHERE to_tsvector('simple', content) @@ websearch_to_tsquery('simple', '"vector database" -spam')
ORDER BY id;

-- scalar + fulltext combination
SELECT id
FROM ft_docs
WHERE fulltext_match(content || '', 'vector database') AND category = 'tech';

-- EXPLAIN should show fulltext plan or fallback filter
EXPLAIN
SELECT id
FROM ft_docs
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY ts_rank(
    to_tsvector('simple', content),
    plainto_tsquery('simple', 'vector database')
) DESC
LIMIT 2;

-- ts_rank ordering + LIMIT
CREATE TABLE ft_rank (id INT, content VARCHAR);

INSERT INTO ft_rank VALUES
    (1, 'vector database vector'),
    (2, 'vector database'),
    (3, 'vector');

SELECT id,
       ts_rank(to_tsvector('simple', content), plainto_tsquery('simple', 'vector database')) AS rank
FROM ft_rank
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY rank DESC, id
LIMIT 2;

DROP INDEX IF EXISTS idx_ft_docs_fts;
DROP TABLE ft_rank;
DROP TABLE ft_docs;
