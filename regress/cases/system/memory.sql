-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS memory_dummy;
CREATE TABLE memory_dummy (x INT, startup_memory VARCHAR);
INSERT INTO memory_dummy VALUES (1, current_setting('memory_limit'));
SELECT 1;
SELECT count(*) FROM memory_dummy;

-- Test memory_limit
SET memory_limit = '1GB';
SELECT current_setting('memory_limit') FROM memory_dummy;

-- Test temp_directory
SET temp_directory = './paro_temp';
SELECT current_setting('temp_directory') FROM memory_dummy;

-- Test max_temp_directory_size
SET max_temp_directory_size = '512MB';
SELECT current_setting('max_temp_directory_size') FROM memory_dummy;

-- Test paro_memory() table function
-- We just check if it can be queried without error
SELECT count(*) >= 0 FROM paro_memory();

-- Test pragma_database_size()
SELECT count(*) >= 0 FROM pragma_database_size();

-- Test paro_temporary_files()
-- Might be empty, but should exist
SELECT count(*) >= 0 FROM paro_temporary_files();

-- RESET tests
SET memory_limit = DEFAULT;
-- DEFAULT restores the server's declared envelope, not a hard-coded 1 GB.
SELECT current_setting('memory_limit') = startup_memory AS restored_startup_envelope
FROM memory_dummy;

SET temp_directory = DEFAULT;
SELECT current_setting('temp_directory') <> '(empty)' AS has_default_temp_directory
FROM memory_dummy;

SET max_temp_directory_size = DEFAULT;
SELECT current_setting('max_temp_directory_size') FROM memory_dummy;

-- @teardown
DROP TABLE IF EXISTS memory_dummy;
