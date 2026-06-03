-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT count(*)
FROM mixed_scan
WHERE selectivity_key = 7;
