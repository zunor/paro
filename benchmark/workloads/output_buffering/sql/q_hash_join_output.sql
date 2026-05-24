-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Breaker DAG: hash join build → probe → client result (fetch-driven bounded).
-- Verifies join output uses bounded queue, not full materialization.
SELECT l.id, l.val, r.payload
FROM buf_join_l l JOIN buf_join_r r ON l.fk = r.id;
