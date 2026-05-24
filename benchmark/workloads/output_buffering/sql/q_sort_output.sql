-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Breaker DAG: sort build → sort emit → client result (fetch-driven bounded).
-- Verifies sort output uses bounded queue after sort completes.
SELECT id, v1 FROM buf_scan ORDER BY v2;
