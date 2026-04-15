-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT id
FROM bench_vectors
WHERE category = 'cat_1'
ORDER BY emb <-> '[1,0,0]', id
LIMIT ${k};
