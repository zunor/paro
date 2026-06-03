-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT count(*)
FROM (
  SELECT agg_key, count(DISTINCT flag)
  FROM baseline_scan
  GROUP BY agg_key
) grouped;
