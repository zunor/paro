-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Fetch-driven bounded path: hash join build → probe → client result.
-- Measures: median latency, peak allocator bytes, output chunk count.
SELECT l.id, l.val, r.payload
FROM cmp_join_l l JOIN cmp_join_r r ON l.fk = r.id;
