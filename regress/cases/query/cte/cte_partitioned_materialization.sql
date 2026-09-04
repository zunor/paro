-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- All producer branches are live and every reference fixes the same constant
-- discriminator, so the complete partitioned-materialization recipe applies.
-- @normalize explain_cte_ids
EXPLAIN
WITH segmented(v, st) AS (
    SELECT 10, 1
    UNION ALL
    SELECT 20, 2
    UNION ALL
    SELECT 30, 3
)
SELECT SUM(first_ref.v + second_ref.v + third_ref.v)::BIGINT AS selected_sum
FROM (SELECT * FROM segmented WHERE st = 1) AS first_ref
CROSS JOIN (SELECT * FROM segmented WHERE st = 2) AS second_ref
CROSS JOIN (SELECT * FROM segmented WHERE st = 3) AS third_ref;

WITH segmented(v, st) AS (
    SELECT 10, 1
    UNION ALL
    SELECT 20, 2
    UNION ALL
    SELECT 30, 3
)
SELECT SUM(first_ref.v + second_ref.v + third_ref.v)::BIGINT AS selected_sum
FROM (SELECT * FROM segmented WHERE st = 1) AS first_ref
CROSS JOIN (SELECT * FROM segmented WHERE st = 2) AS second_ref
CROSS JOIN (SELECT * FROM segmented WHERE st = 3) AS third_ref;

-- A discriminator on the null-supplying side of an outer join must remain
-- NULL for unmatched rows. Partition constant substitution must reject this
-- shape instead of turning nullable_ref.st into the branch constant 2.
WITH segmented(v, st) AS (
    SELECT 10, 1
    UNION ALL
    SELECT 20, 2
    UNION ALL
    SELECT 30, 3
)
SELECT COUNT(nullable_ref.st)::BIGINT AS matched_discriminators
FROM (SELECT * FROM segmented WHERE st = 1) AS preserved_ref
LEFT JOIN (SELECT * FROM segmented WHERE st = 2) AS nullable_ref
    ON FALSE
CROSS JOIN (SELECT * FROM segmented WHERE st = 3) AS third_ref;

-- Partitioning is an all-branch sharing-layout choice. With fewer references
-- than producer branches it must leave the original CTE intact rather than
-- silently suppressing branches that currently have no consumer.
WITH segmented(v, st) AS (
    SELECT 10, 1
    UNION ALL
    SELECT 20, 2
    UNION ALL
    SELECT 30, 3
    UNION ALL
    SELECT 40, 4
)
SELECT SUM(first_ref.v + fourth_ref.v)::BIGINT AS selected_sum
FROM (SELECT * FROM segmented WHERE st = 1) AS first_ref
CROSS JOIN (SELECT * FROM segmented WHERE st = 4) AS fourth_ref;
