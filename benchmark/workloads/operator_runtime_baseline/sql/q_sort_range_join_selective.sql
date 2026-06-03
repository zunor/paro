-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT count(*)
FROM baseline_range_l l
SEMI JOIN baseline_range_r r
  ON l.lo < r.lo
 AND l.lo_limit >= r.lo;
