-- @setup
DROP TABLE IF EXISTS window_spill_test;

-- @setup
DROP TABLE IF EXISTS window_large_partition_test;

-- @setup
CREATE TABLE window_spill_test (part INT, id INT, score INT, note VARCHAR);

INSERT INTO window_spill_test VALUES
    (1, 1, 90, 'p1_a'),
    (1, 2, 90, 'p1_b'),
    (1, 3, 70, 'p1_c'),
    (1, 4, 60, 'p1_d'),
    (2, 1, 95, 'p2_a'),
    (2, 2, 80, 'p2_b'),
    (2, 3, 80, 'p2_c'),
    (2, 4, 50, 'p2_d');

INSERT INTO window_spill_test
SELECT part + 2, id + 100, score, note || '_x1' FROM window_spill_test;

INSERT INTO window_spill_test
SELECT part + 4, id + 200, score + 1, note || '_x2' FROM window_spill_test;

SET force_external = true;

EXPLAIN
SELECT
    part,
    id,
    score,
    ROW_NUMBER() OVER (PARTITION BY part ORDER BY score DESC, id ASC) AS rn
FROM window_spill_test;

SET force_external = DEFAULT;

EXPLAIN
SELECT
    part,
    id,
    score,
    ROW_NUMBER() OVER (PARTITION BY part ORDER BY score DESC, id ASC) AS rn,
    RANK() OVER (PARTITION BY part ORDER BY score DESC, id ASC) AS rk,
    LEAD(score, 1, -1) OVER (PARTITION BY part ORDER BY score DESC, id ASC) AS lead_score,
    LAG(score, 1, -1) OVER (PARTITION BY part ORDER BY score DESC, id ASC) AS lag_score
FROM window_spill_test
WHERE part IN (1, 2);

CREATE TABLE window_large_partition_test (part INT, id INT, payload VARCHAR);

INSERT INTO window_large_partition_test VALUES
    (1, 1, 'a1'),
    (1, 2, 'a2'),
    (1, 3, 'a3'),
    (1, 4, 'a4'),
    (1, 5, 'a5'),
    (1, 6, 'a6'),
    (1, 7, 'a7'),
    (1, 8, 'a8'),
    (2, 1, 'b1'),
    (2, 2, 'b2'),
    (2, 3, 'b3'),
    (3, 1, 'c1'),
    (3, 2, 'c2');

INSERT INTO window_large_partition_test
SELECT part, id + 100, payload || '_x1'
FROM window_large_partition_test
WHERE part = 1;

INSERT INTO window_large_partition_test
SELECT part, id + 200, payload || '_x2'
FROM window_large_partition_test
WHERE part = 1;

INSERT INTO window_large_partition_test
SELECT part, id + 400, payload || '_x3'
FROM window_large_partition_test
WHERE part = 1;

EXPLAIN
SELECT
    part,
    id,
    ROW_NUMBER() OVER (PARTITION BY part ORDER BY id ASC) AS rn
FROM window_large_partition_test;

-- @teardown
DROP TABLE IF EXISTS window_spill_test;

-- @teardown
DROP TABLE IF EXISTS window_large_partition_test;
