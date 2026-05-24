-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Large join output with limit: verifies early stop propagation through breaker DAG.
SELECT l.id, r.payload
FROM buf_join_l l LEFT JOIN buf_join_r r ON l.fk = r.id
LIMIT 500;
