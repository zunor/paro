-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

EXPLAIN
SELECT group_key, SUM(metric) AS total_metric
FROM (
    SELECT a.group_key, e.metric
    FROM optimizer_plan_a a
    JOIN optimizer_plan_b b ON b.a_id = a.id
    JOIN optimizer_plan_c c ON c.b_id = b.id
    JOIN optimizer_plan_d d ON d.c_id = c.id
    JOIN optimizer_plan_e e ON e.d_id = d.id
    WHERE a.group_key = 0
    UNION ALL
    SELECT a.group_key, e.metric FROM optimizer_plan_a a JOIN optimizer_plan_b b ON b.a_id = a.id JOIN optimizer_plan_c c ON c.b_id = b.id JOIN optimizer_plan_d d ON d.c_id = c.id JOIN optimizer_plan_e e ON e.d_id = d.id WHERE a.group_key = 1
    UNION ALL
    SELECT a.group_key, e.metric FROM optimizer_plan_a a JOIN optimizer_plan_b b ON b.a_id = a.id JOIN optimizer_plan_c c ON c.b_id = b.id JOIN optimizer_plan_d d ON d.c_id = c.id JOIN optimizer_plan_e e ON e.d_id = d.id WHERE a.group_key = 2
    UNION ALL
    SELECT a.group_key, e.metric FROM optimizer_plan_a a JOIN optimizer_plan_b b ON b.a_id = a.id JOIN optimizer_plan_c c ON c.b_id = b.id JOIN optimizer_plan_d d ON d.c_id = c.id JOIN optimizer_plan_e e ON e.d_id = d.id WHERE a.group_key = 3
    UNION ALL
    SELECT a.group_key, e.metric FROM optimizer_plan_a a JOIN optimizer_plan_b b ON b.a_id = a.id JOIN optimizer_plan_c c ON c.b_id = b.id JOIN optimizer_plan_d d ON d.c_id = c.id JOIN optimizer_plan_e e ON e.d_id = d.id WHERE a.group_key = 4
    UNION ALL
    SELECT a.group_key, e.metric FROM optimizer_plan_a a JOIN optimizer_plan_b b ON b.a_id = a.id JOIN optimizer_plan_c c ON c.b_id = b.id JOIN optimizer_plan_d d ON d.c_id = c.id JOIN optimizer_plan_e e ON e.d_id = d.id WHERE a.group_key = 5
    UNION ALL
    SELECT a.group_key, e.metric FROM optimizer_plan_a a JOIN optimizer_plan_b b ON b.a_id = a.id JOIN optimizer_plan_c c ON c.b_id = b.id JOIN optimizer_plan_d d ON d.c_id = c.id JOIN optimizer_plan_e e ON e.d_id = d.id WHERE a.group_key = 6
    UNION ALL
    SELECT a.group_key, e.metric FROM optimizer_plan_a a JOIN optimizer_plan_b b ON b.a_id = a.id JOIN optimizer_plan_c c ON c.b_id = b.id JOIN optimizer_plan_d d ON d.c_id = c.id JOIN optimizer_plan_e e ON e.d_id = d.id WHERE a.group_key = 7
    UNION ALL
    SELECT a.group_key, e.metric FROM optimizer_plan_a a JOIN optimizer_plan_b b ON b.a_id = a.id JOIN optimizer_plan_c c ON c.b_id = b.id JOIN optimizer_plan_d d ON d.c_id = c.id JOIN optimizer_plan_e e ON e.d_id = d.id WHERE a.group_key = 8
    UNION ALL
    SELECT a.group_key, e.metric FROM optimizer_plan_a a JOIN optimizer_plan_b b ON b.a_id = a.id JOIN optimizer_plan_c c ON c.b_id = b.id JOIN optimizer_plan_d d ON d.c_id = c.id JOIN optimizer_plan_e e ON e.d_id = d.id WHERE a.group_key = 9
    UNION ALL
    SELECT a.group_key, e.metric FROM optimizer_plan_a a JOIN optimizer_plan_b b ON b.a_id = a.id JOIN optimizer_plan_c c ON c.b_id = b.id JOIN optimizer_plan_d d ON d.c_id = c.id JOIN optimizer_plan_e e ON e.d_id = d.id WHERE a.group_key = 10
    UNION ALL
    SELECT a.group_key, e.metric FROM optimizer_plan_a a JOIN optimizer_plan_b b ON b.a_id = a.id JOIN optimizer_plan_c c ON c.b_id = b.id JOIN optimizer_plan_d d ON d.c_id = c.id JOIN optimizer_plan_e e ON e.d_id = d.id WHERE a.group_key = 11
) branches
GROUP BY group_key
ORDER BY total_metric DESC
LIMIT 10;
