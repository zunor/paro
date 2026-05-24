-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Completed-output path: EXPLAIN ANALYZE with hash join. The target uses a
-- discarding output port; only explain rows are returned to the client.
-- Same logical query as fetch_hash_join for direct comparison.
-- Measures: median latency and RSS during profiled execution.
EXPLAIN ANALYZE
SELECT l.id, l.val, r.payload
FROM cmp_join_l l JOIN cmp_join_r r ON l.fk = r.id;
