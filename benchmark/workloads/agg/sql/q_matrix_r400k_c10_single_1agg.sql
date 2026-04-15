SELECT count(*) AS group_cnt
FROM (
    SELECT
        key_single,
        sum(m1) AS agg_sum
    FROM bench_agg_matrix
    WHERE row_count = ${rows_large}
      AND group_card = ${card_low}
    GROUP BY key_single
) AS agg_matrix;
