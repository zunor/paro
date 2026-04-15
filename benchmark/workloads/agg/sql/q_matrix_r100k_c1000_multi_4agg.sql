# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT count(*) AS group_cnt
FROM (
    SELECT
        key_a,
        key_b,
        sum(m1) AS s1,
        sum(m2) AS s2,
        avg(m3) AS a3,
        max(m4) AS m4_max
    FROM bench_agg_matrix
    WHERE row_count = ${rows_small}
      AND group_card = ${card_high}
    GROUP BY key_a, key_b
) AS agg_matrix;
