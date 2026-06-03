-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT sum(v)
FROM mixed_small
WHERE id BETWEEN 10 AND 19;
