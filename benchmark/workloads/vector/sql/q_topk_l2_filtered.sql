SELECT id
FROM bench_vectors
WHERE category = 'cat_1'
ORDER BY emb <-> '[1,0,0]', id
LIMIT ${k};
