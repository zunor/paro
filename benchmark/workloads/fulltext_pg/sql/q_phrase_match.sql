-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT id
FROM bench_docs_pg
WHERE to_tsvector('simple', content) @@ phraseto_tsquery('simple', 'vector database')
ORDER BY id
LIMIT 20;
