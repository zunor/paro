-- Run in a fresh database. No query-specific production behavior is required.
-- On the accepted tree both SELECTs use scalar-equivalent ordering.
-- With the archived native provider-window experiment, the second returns 3.
CREATE TABLE rank_contract_counterexample(id INT, content VARCHAR);
INSERT INTO rank_contract_counterexample VALUES
    (1, 'vector database vector'), (2, 'vector database'),
    (3, 'database vector'), (4, 'vector'), (5, 'noise');
CREATE INDEX rank_contract_index ON rank_contract_counterexample
USING GIN (to_tsvector('simple', content));

-- Two keys retain the scalar ordering oracle. Expected: 1/2.375, 2/2, 3/2.
SELECT id, ts_rank(to_tsvector('simple', content),
                  plainto_tsquery('simple', 'vector database')) AS score
FROM rank_contract_counterexample
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY score DESC, id;

-- The unique highest scalar score makes the selected bag unambiguous: {1}.
SELECT id FROM rank_contract_counterexample
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY ts_rank(to_tsvector('simple', content),
                 plainto_tsquery('simple', 'vector database')) DESC
LIMIT 1;
DROP TABLE rank_contract_counterexample;
