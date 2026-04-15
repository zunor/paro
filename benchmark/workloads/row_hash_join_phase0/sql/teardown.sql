-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS phase0_join_short_l;
DROP TABLE IF EXISTS phase0_join_short_r;
DROP TABLE IF EXISTS phase0_join_long_l;
DROP TABLE IF EXISTS phase0_join_long_r;
DROP TABLE IF EXISTS phase0_join_found_l;
DROP TABLE IF EXISTS phase0_join_found_r;
DROP TABLE IF EXISTS phase0_join_external_l;
DROP TABLE IF EXISTS phase0_join_external_r;

SET force_external = DEFAULT;
SET max_temp_directory_size = DEFAULT;
SET temp_directory = DEFAULT;
SET memory_limit = DEFAULT;
