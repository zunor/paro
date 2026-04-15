INSERT INTO benchmark_primary_key_dml_case
SELECT i, i * 10
FROM generate_series(1, ${rows}) AS t(i);

DELETE FROM benchmark_primary_key_dml_case
WHERE id % 10 = 0;
