# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT COUNT(*)
FROM bench_art_scalar
WHERE key_col BETWEEN ${range_start} AND ${range_end};
