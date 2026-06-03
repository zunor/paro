-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT count(r.payload)
FROM baseline_int_l l
JOIN baseline_int_r r
  ON l.id = r.id;
