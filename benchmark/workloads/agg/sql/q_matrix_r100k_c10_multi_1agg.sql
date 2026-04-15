# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT count(*) AS group_cnt
FROM (
    SELECT
        key_a,
        key_b,
        sum(m1) AS agg_sum
    FROM bench_agg_matrix
    WHERE row_count = ${rows_small}
      AND group_card = ${card_low}
    GROUP BY key_a, key_b
) AS agg_matrix;
