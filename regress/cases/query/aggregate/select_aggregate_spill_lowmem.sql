DROP TABLE IF EXISTS agg_phasef_spill_shape;

SET temp_directory = '/tmp/paro_regress_agg_phasef_spill';
SET max_temp_directory_size = '256MB';
SET memory_limit = '32MB';
SET force_external = true;

CREATE TABLE agg_phasef_spill_shape(k1 INT, k2 INT, v INT);

INSERT INTO agg_phasef_spill_shape
SELECT
    i::INT AS k1,
    (i % 257)::INT AS k2,
    1::INT AS v
FROM generate_series(1, 50000) AS gs(i);

SELECT count(*) AS group_cnt
FROM (
    SELECT k1, k2, SUM(v) AS sum_v
    FROM agg_phasef_spill_shape
    GROUP BY k1, k2
) AS agg_phasef_groups;

SELECT min(sum_v) AS min_sum, max(sum_v) AS max_sum
FROM (
    SELECT k1, k2, SUM(v) AS sum_v
    FROM agg_phasef_spill_shape
    GROUP BY k1, k2
) AS agg_phasef_values;

SET force_external = false;
SET memory_limit = DEFAULT;
SET max_temp_directory_size = DEFAULT;
RESET temp_directory;

DROP TABLE IF EXISTS agg_phasef_spill_shape;
