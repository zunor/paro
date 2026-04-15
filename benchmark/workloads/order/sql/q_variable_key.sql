# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT id
FROM bench_order_variable
ORDER BY sort_key ASC NULLS LAST, id ASC
LIMIT 7;
