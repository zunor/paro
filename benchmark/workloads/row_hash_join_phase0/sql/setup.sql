DROP TABLE IF EXISTS phase0_join_short_l;
DROP TABLE IF EXISTS phase0_join_short_r;
DROP TABLE IF EXISTS phase0_join_long_l;
DROP TABLE IF EXISTS phase0_join_long_r;
DROP TABLE IF EXISTS phase0_join_found_l;
DROP TABLE IF EXISTS phase0_join_found_r;
DROP TABLE IF EXISTS phase0_join_external_l;
DROP TABLE IF EXISTS phase0_join_external_r;

SET force_external = DEFAULT;
SET memory_limit = '256MB';
SET temp_directory = '/tmp/paro_benchmark_row_hash_join_phase0';
SET max_temp_directory_size = DEFAULT;

CREATE TABLE phase0_join_short_l(id INT, payload INT);
CREATE TABLE phase0_join_short_r(id INT, payload INT);
INSERT INTO phase0_join_short_l
SELECT g, g * 11
FROM generate_series(1, ${short_rows}) AS t(g);
INSERT INTO phase0_join_short_r
SELECT g, g * 13
FROM generate_series(1, ${short_rows}) AS t(g);

CREATE TABLE phase0_join_long_l(k INT, payload INT);
CREATE TABLE phase0_join_long_r(k INT, payload INT);
INSERT INTO phase0_join_long_l
SELECT g % ${long_key_space}, g
FROM generate_series(1, ${long_rows}) AS t(g);
INSERT INTO phase0_join_long_r
SELECT g % ${long_key_space}, g * 7
FROM generate_series(1, ${long_rows}) AS t(g);

CREATE TABLE phase0_join_found_l(id INT, payload VARCHAR);
CREATE TABLE phase0_join_found_r(id INT, payload VARCHAR);
INSERT INTO phase0_join_found_l
SELECT g, 'left_' || CAST(g AS VARCHAR)
FROM generate_series(1, ${found_left_rows}) AS t(g);
INSERT INTO phase0_join_found_r
SELECT g, 'right_' || CAST(g AS VARCHAR)
FROM generate_series(1, ${found_right_rows}) AS t(g);

CREATE TABLE phase0_join_external_l(id INT, payload VARCHAR);
CREATE TABLE phase0_join_external_r(id INT, payload VARCHAR);
INSERT INTO phase0_join_external_l
SELECT g, 'left_external_' || CAST(g % 997 AS VARCHAR) || '_xxxxxxxx'
FROM generate_series(1, ${external_rows}) AS t(g);
INSERT INTO phase0_join_external_r
SELECT g, 'right_external_' || CAST(g % 997 AS VARCHAR) || '_yyyyyyyy'
FROM generate_series(1, ${external_rows}) AS t(g);
