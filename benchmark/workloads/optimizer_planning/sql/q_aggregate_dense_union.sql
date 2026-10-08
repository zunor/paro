-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

EXPLAIN
SELECT group_key, SUM(branch_metric) AS total_metric
FROM (
    SELECT a.group_key, CAST(SUM(e.metric) AS BIGINT) AS branch_metric
    FROM optimizer_plan_a AS a
    JOIN optimizer_plan_b AS b ON b.a_id = a.id
    JOIN optimizer_plan_c AS c ON c.b_id = b.id
    JOIN optimizer_plan_d AS d ON d.c_id = c.id
    JOIN optimizer_plan_e AS e ON e.d_id = d.id
    WHERE a.group_key = 0
    GROUP BY a.group_key
    UNION ALL
    SELECT a.group_key, CAST(SUM(e.metric) AS BIGINT)
    FROM optimizer_plan_a AS a
    JOIN optimizer_plan_b AS b ON b.a_id = a.id
    JOIN optimizer_plan_c AS c ON c.b_id = b.id
    JOIN optimizer_plan_d AS d ON d.c_id = c.id
    JOIN optimizer_plan_e AS e ON e.d_id = d.id
    WHERE a.group_key = 1
    GROUP BY a.group_key
    UNION ALL
    SELECT a.group_key, CAST(SUM(e.metric) AS BIGINT)
    FROM optimizer_plan_a AS a
    JOIN optimizer_plan_b AS b ON b.a_id = a.id
    JOIN optimizer_plan_c AS c ON c.b_id = b.id
    JOIN optimizer_plan_d AS d ON d.c_id = c.id
    JOIN optimizer_plan_e AS e ON e.d_id = d.id
    WHERE a.group_key = 2
    GROUP BY a.group_key
    UNION ALL
    SELECT a.group_key, CAST(SUM(e.metric) AS BIGINT)
    FROM optimizer_plan_a AS a
    JOIN optimizer_plan_b AS b ON b.a_id = a.id
    JOIN optimizer_plan_c AS c ON c.b_id = b.id
    JOIN optimizer_plan_d AS d ON d.c_id = c.id
    JOIN optimizer_plan_e AS e ON e.d_id = d.id
    WHERE a.group_key = 3
    GROUP BY a.group_key
    UNION ALL
    SELECT a.group_key, CAST(SUM(e.metric) AS BIGINT)
    FROM optimizer_plan_a AS a
    JOIN optimizer_plan_b AS b ON b.a_id = a.id
    JOIN optimizer_plan_c AS c ON c.b_id = b.id
    JOIN optimizer_plan_d AS d ON d.c_id = c.id
    JOIN optimizer_plan_e AS e ON e.d_id = d.id
    WHERE a.group_key = 4
    GROUP BY a.group_key
    UNION ALL
    SELECT a.group_key, CAST(SUM(e.metric) AS BIGINT)
    FROM optimizer_plan_a AS a
    JOIN optimizer_plan_b AS b ON b.a_id = a.id
    JOIN optimizer_plan_c AS c ON c.b_id = b.id
    JOIN optimizer_plan_d AS d ON d.c_id = c.id
    JOIN optimizer_plan_e AS e ON e.d_id = d.id
    WHERE a.group_key = 5
    GROUP BY a.group_key
    UNION ALL
    SELECT a.group_key, CAST(SUM(e.metric) AS BIGINT)
    FROM optimizer_plan_a AS a
    JOIN optimizer_plan_b AS b ON b.a_id = a.id
    JOIN optimizer_plan_c AS c ON c.b_id = b.id
    JOIN optimizer_plan_d AS d ON d.c_id = c.id
    JOIN optimizer_plan_e AS e ON e.d_id = d.id
    WHERE a.group_key = 6
    GROUP BY a.group_key
    UNION ALL
    SELECT a.group_key, CAST(SUM(e.metric) AS BIGINT)
    FROM optimizer_plan_a AS a
    JOIN optimizer_plan_b AS b ON b.a_id = a.id
    JOIN optimizer_plan_c AS c ON c.b_id = b.id
    JOIN optimizer_plan_d AS d ON d.c_id = c.id
    JOIN optimizer_plan_e AS e ON e.d_id = d.id
    WHERE a.group_key = 7
    GROUP BY a.group_key
    UNION ALL
    SELECT a.group_key, CAST(SUM(e.metric) AS BIGINT)
    FROM optimizer_plan_a AS a
    JOIN optimizer_plan_b AS b ON b.a_id = a.id
    JOIN optimizer_plan_c AS c ON c.b_id = b.id
    JOIN optimizer_plan_d AS d ON d.c_id = c.id
    JOIN optimizer_plan_e AS e ON e.d_id = d.id
    WHERE a.group_key = 8
    GROUP BY a.group_key
    UNION ALL
    SELECT a.group_key, CAST(SUM(e.metric) AS BIGINT)
    FROM optimizer_plan_a AS a
    JOIN optimizer_plan_b AS b ON b.a_id = a.id
    JOIN optimizer_plan_c AS c ON c.b_id = b.id
    JOIN optimizer_plan_d AS d ON d.c_id = c.id
    JOIN optimizer_plan_e AS e ON e.d_id = d.id
    WHERE a.group_key = 9
    GROUP BY a.group_key
    UNION ALL
    SELECT a.group_key, CAST(SUM(e.metric) AS BIGINT)
    FROM optimizer_plan_a AS a
    JOIN optimizer_plan_b AS b ON b.a_id = a.id
    JOIN optimizer_plan_c AS c ON c.b_id = b.id
    JOIN optimizer_plan_d AS d ON d.c_id = c.id
    JOIN optimizer_plan_e AS e ON e.d_id = d.id
    WHERE a.group_key = 10
    GROUP BY a.group_key
    UNION ALL
    SELECT a.group_key, CAST(SUM(e.metric) AS BIGINT)
    FROM optimizer_plan_a AS a
    JOIN optimizer_plan_b AS b ON b.a_id = a.id
    JOIN optimizer_plan_c AS c ON c.b_id = b.id
    JOIN optimizer_plan_d AS d ON d.c_id = c.id
    JOIN optimizer_plan_e AS e ON e.d_id = d.id
    WHERE a.group_key = 11
    GROUP BY a.group_key
) AS branches
GROUP BY group_key
ORDER BY total_metric DESC;
