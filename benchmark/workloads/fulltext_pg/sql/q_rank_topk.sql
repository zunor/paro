# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT id
FROM bench_docs_pg
WHERE to_tsvector('simple', content) @@ plainto_tsquery('simple', 'vector database')
ORDER BY ts_rank(
    to_tsvector('simple', content),
    plainto_tsquery('simple', 'vector database')
) DESC, id
LIMIT 2;
