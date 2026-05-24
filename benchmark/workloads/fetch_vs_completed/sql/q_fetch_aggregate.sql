-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Fetch-driven bounded path: aggregate build → emit → client result.
-- Measures: median latency, peak allocator bytes, output chunk count.
SELECT grp, SUM(v) AS total, COUNT(*) AS cnt
FROM cmp_agg
GROUP BY grp;
