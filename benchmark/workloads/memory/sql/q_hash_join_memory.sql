-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT count(r.id)
FROM memory_join_l l
LEFT JOIN memory_join_r r
  ON l.id = r.id;
