# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT id
FROM bench_order_payload
ORDER BY priority DESC, tie_break ASC, id ASC
LIMIT 12;
