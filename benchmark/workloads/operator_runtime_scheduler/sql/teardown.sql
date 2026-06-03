-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS scheduler_scan;
DROP TABLE IF EXISTS scheduler_probe;
DROP TABLE IF EXISTS scheduler_build;

SET parallel_scheduler = DEFAULT;
SET threads = DEFAULT;
SET max_temp_directory_size = DEFAULT;
SET temp_directory = DEFAULT;
SET memory_limit = DEFAULT;
