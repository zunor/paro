-- Ensure clean state for reruns
DROP TABLE IF EXISTS vector_topn_case;

CREATE TABLE vector_topn_case (
    id INT PRIMARY KEY,
    category VARCHAR,
    emb VECTOR(3)
);

INSERT INTO vector_topn_case VALUES
    (1, 'x', '[1,0,0]'),
    (2, 'x', '[0,1,0]'),
    (3, 'y', '[0,0,1]'),
    (4, 'y', '[2,0,0]'),
    (5, 'z', '[-1,0,0]');

-- Basic pgvector L2 operator in top-n query
EXPLAIN SELECT id FROM vector_topn_case ORDER BY emb <-> '[1,0,0]' LIMIT 3;
SELECT id FROM vector_topn_case ORDER BY emb <-> '[1,0,0]', id LIMIT 3;

-- WHERE filter + pgvector L1 operator
EXPLAIN SELECT id FROM vector_topn_case WHERE category = 'x' ORDER BY emb <+> '[1,0,0]' LIMIT 2;
SELECT id FROM vector_topn_case WHERE category = 'x' ORDER BY emb <+> '[1,0,0]', id LIMIT 2;

-- Cosine distance top-n query
EXPLAIN SELECT id FROM vector_topn_case ORDER BY emb <=> '[1,0,0]' LIMIT 3;
SELECT id FROM vector_topn_case ORDER BY emb <=> '[1,0,0]', id LIMIT 3;

-- Neg inner product operator in ORDER BY
EXPLAIN SELECT id FROM vector_topn_case ORDER BY emb <#> '[1,0,0]' LIMIT 3;
SELECT id FROM vector_topn_case ORDER BY emb <#> '[1,0,0]', id LIMIT 3;

DROP TABLE vector_topn_case;
