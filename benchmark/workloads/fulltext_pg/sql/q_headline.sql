-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT ts_headline('simple', content, plainto_tsquery('simple', 'vector database')) AS hl
FROM bench_docs_pg
WHERE id <= 3
ORDER BY id;
