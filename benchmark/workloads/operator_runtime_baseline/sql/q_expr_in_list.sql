-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT count(*)
FROM baseline_scan
WHERE flag IN (1, 3, 5, 7, 9, 11, 13, 15, 17);
