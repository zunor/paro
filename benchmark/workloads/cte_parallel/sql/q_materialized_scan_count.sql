WITH base AS MATERIALIZED (
    SELECT id, bucket, payload FROM cte_parallel_src
)
SELECT COUNT(*)
FROM base
WHERE payload >= 0;
