-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS search_opt_docs;

CREATE TABLE search_opt_docs (
    id INT PRIMARY KEY,
    category VARCHAR,
    content VARCHAR
);

INSERT INTO search_opt_docs VALUES
    (1, 'tech', 'graph database systems and query planning'),
    (2, 'tech', 'graph traversal with database statistics'),
    (3, 'life', 'mountain hiking and travel journal'),
    (4, 'tech', 'vector search and graph database hybrid');

CREATE INDEX idx_search_opt_docs_fts
ON search_opt_docs USING GIN (to_tsvector('simple', content));

-- @normalize explain_search_ids
EXPLAIN (VERBOSE)
SELECT id
FROM search_opt_docs
WHERE fulltext_match(content, 'graph database')
ORDER BY bm25(content, 'graph database') DESC, id
LIMIT 2;

SELECT id
FROM search_opt_docs
WHERE fulltext_match(content, 'graph database')
ORDER BY bm25(content, 'graph database') DESC, id
LIMIT 2;

-- @normalize explain_search_ids
EXPLAIN (VERBOSE)
SELECT id
FROM search_opt_docs
WHERE fulltext_match(content, 'graph database')
  AND category = 'tech';

DROP TABLE search_opt_docs;
