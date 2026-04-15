-- Ensure clean state for reruns
DROP TABLE IF EXISTS vector_ops_case;

CREATE TABLE vector_ops_case (
    id INT PRIMARY KEY,
    emb VECTOR(3)
);

INSERT INTO vector_ops_case VALUES
    (1, '[1,0,0]'),
    (2, '[0,1,0]'),
    (3, '[-1,0,0]'),
    (4, '[0,0,0]');

-- Operator/function parity for pgvector-compatible syntax
SELECT
    id,
    emb <-> '[1,0,0]' AS op_l2,
    l2_distance(emb, '[1,0,0]') AS fn_l2,
    emb <+> '[1,0,0]' AS op_l1,
    l1_distance(emb, '[1,0,0]') AS fn_l1,
    emb <=> '[1,0,0]' AS op_cos,
    cosine_distance(emb, '[1,0,0]') AS fn_cos,
    emb <#> '[1,0,0]' AS op_neg_ip,
    neg_inner_product(emb, '[1,0,0]') AS fn_neg_ip
FROM vector_ops_case
ORDER BY id;

-- Top-N ordering for each pgvector distance operator
SELECT id FROM vector_ops_case ORDER BY emb <-> '[1,0,0]', id LIMIT 4;
SELECT id FROM vector_ops_case ORDER BY emb <+> '[1,0,0]', id LIMIT 4;
SELECT id FROM vector_ops_case ORDER BY emb <=> '[1,0,0]', id LIMIT 4;
SELECT id FROM vector_ops_case ORDER BY emb <#> '[1,0,0]', id LIMIT 4;

DROP TABLE vector_ops_case;
