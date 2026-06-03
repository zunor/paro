-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT count(*)
FROM (
  SELECT agg_key, flag, count(*)
  FROM baseline_scan
  GROUP BY GROUPING SETS ((agg_key), (flag), ())
) grouped;
