-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT COUNT(*)
FROM hash_join_i64_probe p
WHERE p.k IN (SELECT b.k FROM hash_join_i64_build b);
