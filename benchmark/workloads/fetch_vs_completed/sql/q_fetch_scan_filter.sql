-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Fetch-driven bounded path: pure streaming scan + filter + project.
-- Measures: median latency, peak allocator bytes (bounded queue).
SELECT id, v1 FROM cmp_scan WHERE v2 > 500;
