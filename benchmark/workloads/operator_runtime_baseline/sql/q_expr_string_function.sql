-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT count(*)
FROM baseline_string_l
WHERE contains(key_text, 'key_');
