# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT count(r.payload)
FROM phase0_join_external_l l
LEFT JOIN phase0_join_external_r r
  ON l.id = r.id;
