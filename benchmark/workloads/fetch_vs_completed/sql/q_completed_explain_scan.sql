-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Completed-output path: EXPLAIN ANALYZE executes the target with a discarding
-- output port, then materializes only the rendered explain rows.
-- Same logical query as fetch_scan_filter for direct comparison.
-- Measures: median latency and RSS while avoiding target result materialization.
EXPLAIN ANALYZE SELECT id, v1 FROM cmp_scan WHERE v2 > 500;
