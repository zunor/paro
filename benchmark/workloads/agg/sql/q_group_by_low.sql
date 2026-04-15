SELECT group_low, count(*)
FROM bench_agg
GROUP BY group_low
ORDER BY group_low;
