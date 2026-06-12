-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT id
FROM bench_search_derived_state_phase0
ORDER BY sparse_distance(sparse_vec, '{1:1.0,3:0.5}') DESC, id
LIMIT 3;
