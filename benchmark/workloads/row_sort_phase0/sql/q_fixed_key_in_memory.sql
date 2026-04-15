# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT id
FROM phase0_sort_fixed
ORDER BY k1 ASC, k2 DESC, id ASC
LIMIT ${result_rows} OFFSET ${fixed_offset};
