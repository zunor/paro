SELECT COUNT(*)
FROM bench_art_scalar
WHERE key_col BETWEEN ${range_start} AND ${range_end};
