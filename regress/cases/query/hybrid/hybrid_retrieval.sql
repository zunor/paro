-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP PROPERTY GRAPH IF EXISTS hybrid_social_graph;
DROP INDEX IF EXISTS idx_hybrid_docs_embedding;
DROP INDEX IF EXISTS idx_hybrid_docs_fts;
DROP TABLE IF EXISTS hybrid_follows;
DROP TABLE IF EXISTS hybrid_people;
DROP TABLE IF EXISTS hybrid_docs;

CREATE TABLE hybrid_docs (
    id INT PRIMARY KEY,
    author_id BIGINT,
    title VARCHAR,
    body VARCHAR,
    embedding VECTOR(4)
);

INSERT INTO hybrid_docs VALUES
    (1, 1, 'Agent memory with vectors', 'A vector database for agent memory and retrieval', '[0.92, 0.11, 0.74, 0.33]'),
    (2, 2, 'Graph-aware retrieval', 'Combining graph traversal with semantic search for agents', '[0.87, 0.14, 0.79, 0.21]'),
    (3, 3, 'Keyword search still matters', 'Full-text ranking helps lexical precision', '[0.20, 0.91, 0.18, 0.77]'),
    (4, 4, 'Relational systems alone are not enough', 'Agents need vector, text, and graph together', '[0.90, 0.09, 0.82, 0.25]');

CREATE VECTOR INDEX idx_hybrid_docs_embedding ON hybrid_docs (embedding);
CREATE INDEX idx_hybrid_docs_fts ON hybrid_docs USING GIN (to_tsvector('simple', body));

CREATE TABLE hybrid_people (
    id BIGINT PRIMARY KEY,
    name VARCHAR
);

CREATE TABLE hybrid_follows (
    src_id BIGINT,
    dst_id BIGINT
);

INSERT INTO hybrid_people VALUES
    (1, 'Ada'),
    (2, 'Grace'),
    (3, 'Linus'),
    (4, 'Margaret');

INSERT INTO hybrid_follows VALUES
    (1, 2),
    (2, 4),
    (1, 3);

CREATE PROPERTY GRAPH hybrid_social_graph
VERTEX TABLES (
    hybrid_people LABEL Person
)
EDGE TABLES (
    hybrid_follows
        SOURCE KEY (src_id) REFERENCES hybrid_people (id)
        DESTINATION KEY (dst_id) REFERENCES hybrid_people (id)
        LABEL Follows
);

SELECT
    d.id,
    d.title,
    t.author_name,
    1.0 / (1.0 + (d.embedding <-> '[0.91, 0.10, 0.80, 0.22]')) +
    CASE
        WHEN to_tsvector('simple', d.body) @@ plainto_tsquery('simple', 'agent memory graph')
        THEN ts_rank(
            to_tsvector('simple', d.body),
            plainto_tsquery('simple', 'agent memory graph')
        )
        ELSE 0.0
    END AS score
FROM hybrid_docs d
JOIN (
    SELECT *
    FROM GRAPH_TABLE(hybrid_social_graph
        MATCH (p:Person WHERE p.name = 'Ada')-[e:Follows]->{1,2}(neighbor:Person)
        COLUMNS (
            neighbor.id AS author_id,
            neighbor.name AS author_name
        )
    ) gt
) t ON d.author_id = t.author_id
ORDER BY score DESC, d.id;

DROP PROPERTY GRAPH hybrid_social_graph;
DROP TABLE IF EXISTS hybrid_follows;
DROP TABLE IF EXISTS hybrid_people;
DROP TABLE hybrid_docs;
