-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT COUNT(*)
FROM hash_join_right_probe p
RIGHT ANTI JOIN hash_join_right_build b
  ON p.k = b.k;
