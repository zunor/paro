# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT id
FROM bench_order_numeric
ORDER BY score DESC, id ASC
LIMIT 8;
