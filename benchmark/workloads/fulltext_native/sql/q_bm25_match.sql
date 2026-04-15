SELECT id
FROM bench_docs_native
WHERE fulltext_match(content, 'vector')
ORDER BY bm25(content, 'vector') DESC, id
LIMIT 3;
