# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS phase0_sort_fixed;
DROP TABLE IF EXISTS phase0_sort_variable;
DROP TABLE IF EXISTS phase0_sort_external;

SET force_external = DEFAULT;
SET max_temp_directory_size = DEFAULT;
SET temp_directory = DEFAULT;
SET memory_limit = DEFAULT;
