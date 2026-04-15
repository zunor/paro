SELECT group_low::TINYINT AS group_low_tiny, count(*)
FROM bench_agg
GROUP BY group_low::TINYINT;
