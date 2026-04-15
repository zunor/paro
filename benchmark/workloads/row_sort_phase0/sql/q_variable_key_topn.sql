SELECT id
FROM phase0_sort_variable
ORDER BY sort_key ASC NULLS LAST, tie DESC, id ASC
LIMIT ${result_rows} OFFSET ${variable_offset};
