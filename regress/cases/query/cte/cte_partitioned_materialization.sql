-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Every reference fixes a discriminator. Native requirements admit both one
-- shared producer and per-domain producers; costing chooses the physical
-- sharing layout without changing the SQL materialization policy.
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
-- NULL for unmatched rows. Partitioning must retain the occurrence and its
-- null-extension boundary, never replace nullable_ref.st by constant 2.
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

-- Demands are owned by occurrences, not by the producer's UNION syntax. A
-- producer branch with no consumer may be pruned, while both requested domains
-- remain represented regardless of the chosen sharing layout.
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
