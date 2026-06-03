-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT SUM(payload_9) FROM scan_pushdown_wide WHERE filter_key = 42;
