-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Fetch-driven bounded path: sort build → sort emit → client result.
-- Measures: median latency, peak allocator bytes after sort completes.
SELECT id, v1 FROM cmp_scan ORDER BY v2;
