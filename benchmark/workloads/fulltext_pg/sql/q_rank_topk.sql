SELECT id
FROM bench_docs_pg
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY ts_rank(
    to_tsvector('simple', content),
    plainto_tsquery('simple', 'vector database')
) DESC, id
LIMIT 2;
