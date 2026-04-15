SELECT 1;

SELECT count(*) > 0 AS has_rows FROM paro_optimizers();

SELECT name, enabled
FROM paro_optimizers()
WHERE name IN ('expression_rewriter', 'filter_pushdown', 'join_order')
ORDER BY name;

SELECT count(*) > 0 AS has_invocations
FROM paro_optimizers()
WHERE invocation_count > 0;

SELECT count(*) > 0 AS has_nonnegative_elapsed
FROM paro_optimizers()
WHERE last_elapsed_us >= 0;
