SELECT count(*)
FROM (
    SELECT k1, k2, SUM(v) AS s
    FROM memory_agg
    GROUP BY k1, k2
) AS grouped;
