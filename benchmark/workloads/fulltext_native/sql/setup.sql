DROP INDEX IF EXISTS idx_bench_docs_native_fts;
DROP TABLE IF EXISTS bench_docs_native;

CREATE TABLE bench_docs_native (
    id BIGINT PRIMARY KEY,
    content VARCHAR
);

INSERT INTO bench_docs_native VALUES
    (1, 'vector database vector'),
    (2, 'vector database'),
    (3, 'vector search'),
    (4, 'database systems'),
    (5, 'mountain river');

INSERT INTO bench_docs_native
SELECT
    i + 1000,
    'lorem ipsum dolor sit amet'
FROM generate_series(1, ${rows}) AS t(i);
