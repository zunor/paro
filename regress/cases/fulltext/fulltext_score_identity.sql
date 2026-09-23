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

SELECT id, ts_rank_cd(to_tsvector('simple', content),
                     plainto_tsquery('simple', 'vector database')) AS score
FROM score_identity
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY score DESC, id;

CREATE INDEX score_identity_index ON score_identity USING GIN (to_tsvector('simple', content));

-- Execution coverage is separate from scores and selected result membership.
-- Allocation ids are query-local; preserve their equality, not their numbers.
-- @normalize explain_operator_timing,explain_operator_counters,explain_summary_timing,explain_runtime_bytes,explain_logical_ids
EXPLAIN ANALYZE
SELECT id FROM score_identity
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY ts_rank(to_tsvector('simple', content),
                 plainto_tsquery('simple', 'vector database')) DESC LIMIT 1;

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

-- Cover density has its own document-local identity and executable provider.
-- @normalize explain_operator_timing,explain_operator_counters,explain_summary_timing,explain_runtime_bytes,explain_logical_ids
EXPLAIN ANALYZE
SELECT id FROM score_identity
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY ts_rank_cd(to_tsvector('simple', content),
                    plainto_tsquery('simple', 'vector database')) DESC LIMIT 1;

SELECT ts_rank_cd(to_tsvector('simple', content),
                  plainto_tsquery('simple', 'vector database')) AS score
FROM score_identity
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY ts_rank_cd(to_tsvector('simple', content),
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

BEGIN;
DELETE FROM score_identity WHERE id = 1;
SELECT ts_rank(to_tsvector('simple', content),
               plainto_tsquery('simple', 'vector database')) AS score
FROM score_identity
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY score DESC LIMIT 1;
ROLLBACK;

SELECT ts_rank(to_tsvector('simple', content),
               plainto_tsquery('simple', 'vector database')) AS score
FROM score_identity
WHERE id <> 1 AND to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY score DESC LIMIT 1;

SELECT id FROM score_identity
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY ts_rank(to_tsvector('simple', content),
                 plainto_tsquery('simple', 'vector database')) DESC, id LIMIT 2 OFFSET 1;

-- Without @@ the score domain includes zero-score documents. NULL ordering
-- and ascending rank are not provided by the matching-documents TopK source.
INSERT INTO score_identity VALUES (8, NULL);
SELECT id, ts_rank(to_tsvector('simple', content),
                  plainto_tsquery('simple', 'vector database')) AS score
FROM score_identity ORDER BY score ASC NULLS LAST, id LIMIT 8;

DROP TABLE score_identity;
