-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

EXPLAIN
SELECT a.group_key, sum(e.metric) AS total_metric
FROM optimizer_plan_a AS a
JOIN optimizer_plan_b AS b ON b.a_id = a.id
JOIN optimizer_plan_c AS c ON c.b_id = b.id
JOIN optimizer_plan_d AS d ON d.c_id = c.id
JOIN optimizer_plan_e AS e ON e.d_id = d.id
GROUP BY a.group_key
ORDER BY total_metric DESC
LIMIT 10;
