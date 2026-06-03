-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT sum(CAST(flag AS BIGINT))
FROM baseline_scan;
