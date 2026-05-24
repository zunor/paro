-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- EXPLAIN ANALYZE with hash join: verifies profiled execution memory behavior
-- when the target produces large join output but discards target chunks.
EXPLAIN ANALYZE
SELECT l.id, r.val
FROM unbuf_join_l l JOIN unbuf_join_r r ON l.fk = r.id;
