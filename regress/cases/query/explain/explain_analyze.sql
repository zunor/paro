-- @normalize explain_operator_timing,explain_summary_timing
EXPLAIN ANALYZE SELECT 1;

DROP TABLE IF EXISTS explain_analyze_rt;
CREATE TABLE explain_analyze_rt (id INT, score INT);
INSERT INTO explain_analyze_rt VALUES (1, 10), (2, 30), (3, 20);

-- @normalize explain_operator_timing,explain_summary_timing
EXPLAIN ANALYZE
SELECT id
FROM explain_analyze_rt
WHERE score >= 20
ORDER BY score DESC;

-- @query json
-- @normalize explain_operator_timing,explain_summary_timing
EXPLAIN ANALYZE
SELECT id
FROM explain_analyze_rt
WHERE score >= 20
ORDER BY score DESC
FORMAT JSON;

DROP TABLE explain_analyze_rt;

DROP TABLE IF EXISTS explain_analyze_topn_rt;
CREATE TABLE explain_analyze_topn_rt (id INT, score INT);
INSERT INTO explain_analyze_topn_rt
SELECT g, 7001 - g
FROM generate_series(1, 7000) AS t(g);

-- @normalize explain_operator_timing,explain_summary_timing
EXPLAIN ANALYZE
SELECT id
FROM explain_analyze_topn_rt
ORDER BY score DESC
LIMIT 3;

SET temp_directory = '/tmp/paro_regress_explain_topn';
SET force_external = true;

-- @normalize explain_operator_timing,explain_summary_timing,explain_runtime_bytes
EXPLAIN ANALYZE
SELECT id
FROM explain_analyze_topn_rt
ORDER BY score DESC
LIMIT 3;

SET force_external = DEFAULT;
SET temp_directory = DEFAULT;

DROP TABLE explain_analyze_topn_rt;
