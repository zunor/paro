-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT SUM(k)
FROM (
  SELECT k
  FROM scheduler_scan
  ORDER BY payload DESC
) sorted_rows;
