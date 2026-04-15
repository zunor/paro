DROP TABLE IF EXISTS agg_phasee_spill;

CREATE TABLE agg_phasee_spill(k1 INT, k2 INT, v INT);

INSERT INTO agg_phasee_spill
SELECT
    (i % 4096)::INT AS k1,
    ((i / 7) % 256)::INT AS k2,
    1::INT AS v
FROM generate_series(1, 200000) AS gs(i);

SET temp_directory = '/tmp/paro_regress_agg_phasee_spill';
SET max_temp_directory_size = '256MB';
SET force_external = true;
SET use_new_agg_spill = true;
SET threads = 1;

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT k1, k2, SUM(v) AS sum_v
FROM agg_phasee_spill
GROUP BY k1, k2;

SELECT
    count(*) AS group_cnt,
    min(sum_v) AS min_sum,
    max(sum_v) AS max_sum
FROM (
    SELECT k1, k2, SUM(v) AS sum_v
    FROM agg_phasee_spill
    GROUP BY k1, k2
    HAVING SUM(v) >= 10
) AS agg_phasee_having;

SET max_temp_directory_size = '64MB';
-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT k1, k2, SUM(v) AS sum_v
FROM agg_phasee_spill
GROUP BY k1, k2;

SET force_external = false;
RESET use_new_agg_spill;
SET max_temp_directory_size = DEFAULT;
SET threads = DEFAULT;

DROP TABLE IF EXISTS agg_phasee_spill;
