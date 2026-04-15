SELECT id
FROM phase0_sort_fixed
ORDER BY k1 ASC, k2 DESC, id ASC
LIMIT ${result_rows} OFFSET ${fixed_offset};
