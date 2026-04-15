# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT id
FROM spill_order
ORDER BY payload DESC, id ASC
LIMIT 12;
