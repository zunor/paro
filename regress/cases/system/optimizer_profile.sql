-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT 1;

SELECT count(*) > 0 AS has_rows FROM paro_optimizers();

SELECT name, kind
FROM paro_optimizers()
WHERE name IN ('semantic_normalization', 'region_optimization', 'physical_selection', 'physical_extraction')
ORDER BY name;

SELECT count(*) > 0 AS has_invocations
FROM paro_optimizers()
WHERE invocation_count > 0;

SELECT count(*) > 0 AS has_nonnegative_elapsed
FROM paro_optimizers()
WHERE last_elapsed_us >= 0;
