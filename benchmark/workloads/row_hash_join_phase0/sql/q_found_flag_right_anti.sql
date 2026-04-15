# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT count(*)
FROM phase0_join_found_l l
RIGHT ANTI JOIN phase0_join_found_r r
  ON l.id = r.id;
