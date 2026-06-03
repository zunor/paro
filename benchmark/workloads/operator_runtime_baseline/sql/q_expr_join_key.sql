-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT count(*)
FROM baseline_int_l l
JOIN baseline_int_r r
  ON l.id + 1 = r.id
WHERE l.id <= 512
  AND r.id <= 512;
