-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS mixed_small;
DROP TABLE IF EXISTS mixed_scan;
DROP TABLE IF EXISTS mixed_join_l;
DROP TABLE IF EXISTS mixed_join_r;
DROP TABLE IF EXISTS mixed_sort;

SET force_external = DEFAULT;
SET max_temp_directory_size = DEFAULT;
SET temp_directory = DEFAULT;
SET memory_limit = DEFAULT;
