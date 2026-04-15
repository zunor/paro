SELECT count(*) AS group_cnt
FROM (
    SELECT
        key_single,
        sum(m1) AS agg_sum
    FROM bench_agg_matrix
    WHERE row_count = ${rows_small}
      AND group_card = ${card_high}
    GROUP BY key_single
) AS agg_matrix;
