-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT count(r.payload)
FROM mixed_join_l l
JOIN mixed_join_r r
  ON l.fk = r.id;
