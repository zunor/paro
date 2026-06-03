-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT SUM(id)
FROM (
  SELECT id
  FROM baseline_scan
  ORDER BY sort_score DESC, id ASC
) sorted_rows;
