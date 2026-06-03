-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT COUNT(*)
FROM (
  SELECT k % 128 AS bucket, SUM(payload) AS total_payload
  FROM scheduler_scan
  GROUP BY 1
) grouped;
