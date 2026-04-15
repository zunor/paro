-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT group_low, count(*)
FROM bench_agg
GROUP BY group_low
ORDER BY group_low;
