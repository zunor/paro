-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- EXPLAIN ANALYZE uses a discarding target output port. Target is a large scan;
-- verifies profiler stats collection does not require full materialization of
-- target output chunks.
EXPLAIN ANALYZE SELECT id, v1, v2 FROM unbuf_scan WHERE v2 > 100;
