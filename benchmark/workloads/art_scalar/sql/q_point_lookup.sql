-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

SELECT payload
FROM bench_art_scalar
WHERE key_col = ${point_key};
