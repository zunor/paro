-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT COUNT(b.payload)
FROM hash_join_null_probe p
LEFT JOIN hash_join_null_build b
  ON p.k = b.k;
