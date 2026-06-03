-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT COUNT(*)
FROM scheduler_probe p
LEFT JOIN scheduler_build b
  ON p.k = b.k
WHERE p.k BETWEEN 1000 AND 7000;
