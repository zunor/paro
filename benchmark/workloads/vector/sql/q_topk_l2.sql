SELECT id
FROM bench_vectors
ORDER BY emb <-> '[1,0,0]', id
LIMIT ${k};
