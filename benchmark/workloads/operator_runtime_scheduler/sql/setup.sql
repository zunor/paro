-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS scheduler_scan;
DROP TABLE IF EXISTS scheduler_probe;
DROP TABLE IF EXISTS scheduler_build;

SET parallel_scheduler = true;
SET threads = ${thread_count};
SET memory_limit = '${memory_limit}';
SET temp_directory = '${temp_directory}';
SET max_temp_directory_size = DEFAULT;

CREATE TABLE scheduler_scan(k INT, payload BIGINT, s VARCHAR);
INSERT INTO scheduler_scan SELECT g, g * 3, 'k_' || CAST(g % 128 AS VARCHAR) FROM generate_series(1, 1000) AS t(g);
INSERT INTO scheduler_scan SELECT g, g * 3, 'k_' || CAST(g % 128 AS VARCHAR) FROM generate_series(1001, 2000) AS t(g);
INSERT INTO scheduler_scan SELECT g, g * 3, 'k_' || CAST(g % 128 AS VARCHAR) FROM generate_series(2001, 3000) AS t(g);
INSERT INTO scheduler_scan SELECT g, g * 3, 'k_' || CAST(g % 128 AS VARCHAR) FROM generate_series(3001, 4000) AS t(g);
INSERT INTO scheduler_scan SELECT g, g * 3, 'k_' || CAST(g % 128 AS VARCHAR) FROM generate_series(4001, 5000) AS t(g);
INSERT INTO scheduler_scan SELECT g, g * 3, 'k_' || CAST(g % 128 AS VARCHAR) FROM generate_series(5001, 6000) AS t(g);
INSERT INTO scheduler_scan SELECT g, g * 3, 'k_' || CAST(g % 128 AS VARCHAR) FROM generate_series(6001, 7000) AS t(g);
INSERT INTO scheduler_scan SELECT g, g * 3, 'k_' || CAST(g % 128 AS VARCHAR) FROM generate_series(7001, 8000) AS t(g);

CREATE TABLE scheduler_probe(k INT, payload BIGINT);
CREATE TABLE scheduler_build(k INT, payload BIGINT);
INSERT INTO scheduler_probe SELECT k, payload FROM scheduler_scan;
INSERT INTO scheduler_build SELECT k, payload * 7 FROM scheduler_scan;
