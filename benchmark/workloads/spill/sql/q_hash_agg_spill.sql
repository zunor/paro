# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT count(*)
FROM (
    SELECT k1, k2, SUM(v) AS s
    FROM spill_agg
    GROUP BY k1, k2
) AS grouped;
