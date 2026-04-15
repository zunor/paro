CREATE INDEX idx_bench_docs_pg_fts
ON bench_docs_pg USING GIN (to_tsvector('simple', content));
