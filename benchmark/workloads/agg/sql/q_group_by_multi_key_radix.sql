SELECT
    group_high,
    group_low,
    count(*)
FROM bench_agg
GROUP BY group_high, group_low;
