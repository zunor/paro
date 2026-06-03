-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT COUNT(*)
FROM scheduler_scan
WHERE k % 2 = 0;
