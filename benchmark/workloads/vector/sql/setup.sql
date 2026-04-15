DROP INDEX IF EXISTS idx_bench_vectors_emb;
DROP TABLE IF EXISTS bench_vectors;

CREATE TABLE bench_vectors (
    id BIGINT PRIMARY KEY,
    category VARCHAR,
    emb VECTOR(${dim})
);

INSERT INTO bench_vectors
SELECT
    i,
    'cat_' || (i % ${categories})::VARCHAR,
    '['
        || (i % 100)::VARCHAR || ','
        || ((i * 7) % 100)::VARCHAR || ','
        || ((i * 13) % 100)::VARCHAR
        || ']'
FROM generate_series(1, ${rows}) AS t(i);
