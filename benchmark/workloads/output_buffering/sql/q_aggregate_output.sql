-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Breaker DAG: aggregate build → emit → client result (fetch-driven bounded).
-- Verifies aggregate output uses bounded queue.
SELECT grp, SUM(v) AS total, COUNT(*) AS cnt
FROM buf_agg
GROUP BY grp;
