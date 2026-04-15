DROP TABLE IF EXISTS phase0_sort_fixed;
DROP TABLE IF EXISTS phase0_sort_variable;
DROP TABLE IF EXISTS phase0_sort_external;

SET force_external = DEFAULT;
SET memory_limit = '256MB';
SET temp_directory = '/tmp/paro_benchmark_row_sort_phase0';
SET max_temp_directory_size = DEFAULT;

CREATE TABLE phase0_sort_fixed(id INT, k1 INT, k2 INT, payload INT);
INSERT INTO phase0_sort_fixed
SELECT
    g,
    ${fixed_rows} - g + 1,
    g % 97,
    g * 3
FROM generate_series(1, ${fixed_rows}) AS t(g);

CREATE TABLE phase0_sort_variable(id INT, sort_key VARCHAR, tie INT, payload VARCHAR);
INSERT INTO phase0_sort_variable
SELECT
    g,
    CASE
        WHEN g % 257 = 0 THEN NULL
        ELSE 'key_' || CAST(((${variable_rows} - g) % 997) AS VARCHAR) || '_' || CAST(g % 17 AS VARCHAR)
    END,
    g % 31,
    'payload_' || CAST(g % 251 AS VARCHAR) || '_phase0_variable'
FROM generate_series(1, ${variable_rows}) AS t(g);

CREATE TABLE phase0_sort_external(id INT, k1 INT, k2 INT, sort_key VARCHAR, tie INT, payload VARCHAR);
INSERT INTO phase0_sort_external
SELECT
    g,
    ${external_rows} - g + 1,
    g % 211,
    CASE
        WHEN g % 509 = 0 THEN NULL
        ELSE 'external_' || CAST(((${external_rows} - g) % 4099) AS VARCHAR) || '_' || CAST(g % 43 AS VARCHAR)
    END,
    g % 127,
    'payload_' || CAST(g % 997 AS VARCHAR) || '_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
FROM generate_series(1, ${external_rows}) AS t(g);
