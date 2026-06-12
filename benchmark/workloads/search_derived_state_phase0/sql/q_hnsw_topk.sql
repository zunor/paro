-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT id
FROM bench_search_derived_state_phase0
ORDER BY emb <-> '[0.90,0.10,0.70]', id
LIMIT 3;
