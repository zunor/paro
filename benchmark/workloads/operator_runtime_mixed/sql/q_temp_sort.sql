-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT id
FROM mixed_sort
ORDER BY sort_score DESC, id ASC
LIMIT 10;
