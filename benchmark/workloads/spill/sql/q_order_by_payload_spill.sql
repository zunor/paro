-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT payload_text
FROM spill_order
ORDER BY payload ASC, id ASC
LIMIT 6;
