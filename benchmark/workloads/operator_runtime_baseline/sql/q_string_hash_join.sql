-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT count(r.payload)
FROM baseline_string_l l
JOIN baseline_string_r r
  ON l.key_text = r.key_text;
