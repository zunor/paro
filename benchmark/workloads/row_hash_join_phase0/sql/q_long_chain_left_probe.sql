# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT count(*)
FROM phase0_join_long_l l
LEFT JOIN phase0_join_long_r r
  ON l.k = r.k;
