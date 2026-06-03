-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS baseline_scan;
DROP TABLE IF EXISTS baseline_int_l;
DROP TABLE IF EXISTS baseline_int_r;
DROP TABLE IF EXISTS baseline_string_l;
DROP TABLE IF EXISTS baseline_string_r;
DROP TABLE IF EXISTS baseline_range_l;
DROP TABLE IF EXISTS baseline_range_r;

SET force_external = DEFAULT;
SET max_temp_directory_size = DEFAULT;
SET temp_directory = DEFAULT;
SET memory_limit = DEFAULT;
