-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Document-local rank is a function of these two values, not index contents.
CREATE TABLE score_identity(id INT, content VARCHAR);
INSERT INTO score_identity VALUES
    (1, 'vector database vector'), (2, 'vector database'),
    (3, 'database vector'), (4, 'vector'), (5, 'noise');

SELECT id, ts_rank(to_tsvector('simple', content),
                  plainto_tsquery('simple', 'vector database')) AS score
FROM score_identity
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY score DESC, id;

CREATE INDEX score_identity_index ON score_identity USING GIN (to_tsvector('simple', content));

SELECT id, ts_rank(to_tsvector('simple', content),
                  plainto_tsquery('simple', 'vector database')) AS score
FROM score_identity
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY score DESC, id;

-- No peer at the LIMIT boundary: corpus BM25 cannot replace this ordering.
SELECT id FROM score_identity
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY ts_rank(to_tsvector('simple', content),
                 plainto_tsquery('simple', 'vector database')) DESC LIMIT 1;

-- Unrelated corpus growth must not change any original document's score.
INSERT INTO score_identity VALUES (6, 'unrelated document');
SELECT id, ts_rank(to_tsvector('simple', content),
                  plainto_tsquery('simple', 'vector database')) AS score
FROM score_identity
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY score DESC, id;

-- Tail and transaction-local rows participate before truncation.
BEGIN;
INSERT INTO score_identity VALUES (7, 'vector vector database database');
SELECT id FROM score_identity
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY ts_rank(to_tsvector('simple', content),
                 plainto_tsquery('simple', 'vector database')) DESC LIMIT 1;
ROLLBACK;

SELECT id FROM score_identity
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY ts_rank(to_tsvector('simple', content),
                 plainto_tsquery('simple', 'vector database')) DESC LIMIT 1;
DROP TABLE score_identity;
