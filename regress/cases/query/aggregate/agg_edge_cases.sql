-- @setup
DROP TABLE IF EXISTS agg_e_empty;
DROP TABLE IF EXISTS agg_e_single;
DROP TABLE IF EXISTS agg_e_nulls;

CREATE TABLE agg_e_empty (x INT);
CREATE TABLE agg_e_single (x INT, label VARCHAR);
CREATE TABLE agg_e_nulls (grp INT, x INT);

INSERT INTO agg_e_single VALUES (42, 'single');
INSERT INTO agg_e_nulls VALUES
    (1, NULL),
    (1, NULL),
    (2, NULL);

-- Empty input
SELECT
    count(*) AS cnt_star,
    count(x) AS cnt_x,
    sum(x) AS sum_x,
    min(x) AS min_x,
    max(x) AS max_x
FROM agg_e_empty;

-- Empty grouped result
SELECT
    grp,
    sum(x) AS sum_x
FROM (
    SELECT 1 AS grp, x
    FROM agg_e_empty
) AS agg_e_empty_grouped
GROUP BY grp
ORDER BY grp;

-- Single-row input
SELECT
    count(*) AS cnt_star,
    sum(x) AS sum_x,
    min(x) AS min_x,
    max(x) AS max_x
FROM agg_e_single;

-- All NULL payloads
SELECT
    count(*) AS cnt_star,
    count(x) AS cnt_x,
    sum(x) AS sum_x,
    min(x) AS min_x,
    max(x) AS max_x
FROM agg_e_nulls;

-- Grouped all NULL payloads
SELECT
    grp,
    count(*) AS cnt_star,
    count(x) AS cnt_x,
    sum(x) AS sum_x
FROM agg_e_nulls
GROUP BY grp
ORDER BY grp;

-- @teardown
DROP TABLE IF EXISTS agg_e_empty;
DROP TABLE IF EXISTS agg_e_single;
DROP TABLE IF EXISTS agg_e_nulls;
