# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT id
FROM bench_docs_native
WHERE fulltext_match(content, 'vector')
ORDER BY bm25(content, 'vector') DESC, id
LIMIT 3;
