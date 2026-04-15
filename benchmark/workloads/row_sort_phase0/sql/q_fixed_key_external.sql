SELECT id
FROM phase0_sort_external
ORDER BY k1 ASC, k2 DESC, id ASC
LIMIT ${result_rows} OFFSET ${external_offset};
