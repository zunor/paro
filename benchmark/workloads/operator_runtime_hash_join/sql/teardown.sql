-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS hash_join_i64_probe;
DROP TABLE IF EXISTS hash_join_i64_build;
DROP TABLE IF EXISTS hash_join_long_probe;
DROP TABLE IF EXISTS hash_join_long_build;
DROP TABLE IF EXISTS hash_join_null_probe;
DROP TABLE IF EXISTS hash_join_null_build;
DROP TABLE IF EXISTS hash_join_string_probe;
DROP TABLE IF EXISTS hash_join_string_build;
DROP TABLE IF EXISTS hash_join_right_probe;
DROP TABLE IF EXISTS hash_join_right_build;

SET force_external = DEFAULT;
SET max_temp_directory_size = DEFAULT;
SET temp_directory = DEFAULT;
SET memory_limit = DEFAULT;
