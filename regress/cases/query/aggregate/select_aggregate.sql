-- @setup
DROP TABLE IF EXISTS agg_phase0;
CREATE TABLE agg_phase0 (
    grp INT,
    x INT,
    y INT,
    flag BOOLEAN,
    label VARCHAR
);
INSERT INTO agg_phase0 VALUES
    (1, 1, 1, true, 'a'),
    (1, 2, 2, false, 'b'),
    (1, 2, 3, true, 'c'),
    (2, 3, 1, true, 'd'),
    (2, 4, 4, false, 'e'),
    (2, NULL, 5, true, 'f');

-- Basic aggregates migrated from dml/select/select_aggregate.sql
SELECT count(*) FROM agg_phase0;
SELECT sum(x) FROM agg_phase0;
SELECT count(*) + sum(x) FROM agg_phase0;

-- Phase 0 contract coverage
SELECT count(DISTINCT x) FROM agg_phase0;
SELECT sum(x) FILTER (WHERE flag) FROM agg_phase0;

SELECT grp + 10 AS g, sum(x * 2) FILTER (WHERE y >= 2) AS filtered_sum
FROM agg_phase0
GROUP BY grp + 10
ORDER BY g;

EXPLAIN SELECT grp + 10 AS g, sum(x * 2) FILTER (WHERE y >= 2) AS filtered_sum
FROM (
    VALUES (1, 1, 1),
           (1, 2, 2),
           (1, 2, 3),
           (2, 3, 1),
           (2, 4, 4),
           (2, NULL, 5)
) AS agg_values(grp, x, y)
GROUP BY grp + 10;

EXPLAIN SELECT first(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST)
FROM (
    VALUES (1, 1),
           (2, 2),
           (3, 3)
) AS ordered_values(x, y);

-- Phase 1 acceptance coverage: grouped/ungrouped DISTINCT + FILTER
SELECT
    grp,
    sum(x) AS sum_all,
    count(x) AS cnt_x,
    count(*) AS cnt_star,
    count(x) FILTER (WHERE flag) AS count_true
FROM agg_phase0
GROUP BY grp
ORDER BY grp;

SELECT
    grp,
    min(x) AS min_x,
    max(x) AS max_x,
    sum(x) FILTER (WHERE y >= 3) AS sum_y_ge_3
FROM agg_phase0
GROUP BY grp
ORDER BY grp;

SELECT
    grp,
    sum(x) AS sum_all,
    sum(DISTINCT x) AS sum_distinct_x,
    count(DISTINCT y) AS count_distinct_y
FROM agg_phase0
GROUP BY grp
ORDER BY grp;

SELECT
    count(DISTINCT x) AS cd_x,
    sum(DISTINCT x) AS sd_x,
    sum(x) FILTER (WHERE y >= 3) AS sum_y_ge_3,
    count(x) FILTER (WHERE NOT flag) AS cnt_not_flag
FROM agg_phase0;

EXPLAIN SELECT grp,
       sum(x) AS sum_all,
       sum(DISTINCT x) AS sum_distinct_x,
       count(DISTINCT y) AS count_distinct_y
FROM (
    VALUES (1, 1, 1),
           (1, 2, 2),
           (1, 2, 3),
           (2, 3, 1),
           (2, 4, 4),
           (2, NULL, 5)
) AS agg_values_distinct(grp, x, y)
GROUP BY grp;

EXPLAIN SELECT grp + 10 AS g,
       sum(x) AS sum_x,
       count(x) FILTER (WHERE y >= 2) AS count_filtered
FROM (
    VALUES (1, 1, 1),
           (1, 2, 2),
           (1, 2, 3),
           (2, 3, 1),
           (2, 4, 4),
           (2, NULL, 5)
) AS agg_values2(grp, x, y)
GROUP BY grp + 10;

-- Phase 2 T2.2 acceptance coverage: FILTER extraction + filtered update
SELECT sum(x) FILTER (WHERE x > 2) AS sum_x_gt_2
FROM agg_phase0;

SELECT
    sum(x) FILTER (WHERE flag) AS sum_flag_true,
    sum(x) FILTER (WHERE NOT flag) AS sum_flag_false,
    sum(y) FILTER (WHERE x IS NULL) AS sum_y_x_null,
    count(x) FILTER (WHERE x >= 2) AS cnt_x_ge_2
FROM agg_phase0;

SELECT
    grp,
    sum(x) FILTER (WHERE y >= 2) AS sum_y_ge_2,
    sum(x) FILTER (WHERE y >= 4) AS sum_y_ge_4,
    sum(y) FILTER (WHERE x IS NULL) AS sum_y_x_null,
    count(x) FILTER (WHERE x >= 2) AS cnt_x_ge_2
FROM agg_phase0
GROUP BY grp
ORDER BY grp;

SELECT
    grp,
    sum(x) FILTER (WHERE flag) AS sum_flag_true,
    sum(DISTINCT x) FILTER (WHERE y >= 2) AS sum_distinct_y_ge_2,
    count(DISTINCT y) FILTER (WHERE x >= 2) AS count_distinct_y_x_ge_2
FROM agg_phase0
GROUP BY grp
ORDER BY grp;

EXPLAIN SELECT grp,
       sum(x) FILTER (WHERE y >= 2) AS sum_y_ge_2,
       sum(x) FILTER (WHERE y >= 4) AS sum_y_ge_4,
       count(x) FILTER (WHERE x >= 2) AS cnt_x_ge_2
FROM (
    VALUES (1, 1, 1),
           (1, 2, 2),
           (1, 2, 3),
           (2, 3, 1),
           (2, 4, 4),
           (2, NULL, 5)
) AS agg_values_filter(grp, x, y)
GROUP BY grp;

-- Phase 2 T2.3 acceptance coverage: ordered aggregate / WITHIN GROUP
SELECT
    first(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS first_desc,
    last(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS last_desc
FROM agg_phase0;

SELECT first(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST) FILTER (WHERE y < 4) AS first_desc_y_lt_4
FROM agg_phase0;

SELECT
    grp,
    first(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS first_desc,
    last(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS last_desc,
    sum(x) AS sum_all
FROM agg_phase0
GROUP BY grp
ORDER BY grp;

SELECT
    first(v) WITHIN GROUP (ORDER BY k DESC NULLS FIRST) AS first_nulls_first,
    first(v) WITHIN GROUP (ORDER BY k DESC NULLS LAST) AS first_nulls_last
FROM (
    VALUES (10, NULL),
           (20, 1),
           (30, 2)
) AS ordered_nulls(v, k);

EXPLAIN SELECT grp,
       first(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS first_desc,
       last(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS last_desc
FROM (
    VALUES (1, 1, 1),
           (1, 2, 2),
           (1, 2, 3),
           (2, 3, 1),
           (2, 4, 4),
           (2, NULL, 5)
) AS agg_values_ordered(grp, x, y)
GROUP BY grp;

-- Phase 2 T2.4 acceptance coverage: GROUPING SETS / GROUPING()
SELECT
    GROUPING(grp) AS grp_g,
    grp,
    sum(x) AS sum_x
FROM agg_phase0
GROUP BY GROUPING SETS ((grp), ())
ORDER BY grp_g, grp;

SELECT
    GROUPING(grp) AS g_grp,
    GROUPING(flag) AS g_flag,
    grp,
    flag,
    count(*) AS cnt
FROM agg_phase0
GROUP BY ROLLUP (grp, flag)
ORDER BY g_grp, g_flag, grp, flag;

SELECT
    GROUPING(grp) AS g_grp,
    GROUPING(flag) AS g_flag,
    grp,
    flag,
    count(*) AS cnt
FROM agg_phase0
GROUP BY CUBE (grp, flag)
ORDER BY g_grp, g_flag, grp, flag;

EXPLAIN SELECT GROUPING(grp) AS grp_g,
       grp,
       sum(x) AS sum_x
FROM (
    VALUES (1, 1),
           (1, 2),
           (1, 2),
           (2, 3),
           (2, 4),
           (2, NULL)
) AS agg_values_grouping(grp, x)
GROUP BY GROUPING SETS ((grp), ());

-- Phase 2 T2.5 acceptance coverage: more aggregate functions
-- T2.5.1 first_value / last_value
SELECT
    first_value(x) WITHIN GROUP (ORDER BY y ASC NULLS LAST) AS first_value_asc,
    last_value(x) WITHIN GROUP (ORDER BY y ASC NULLS LAST) AS last_value_asc
FROM agg_phase0;

SELECT
    grp,
    first_value(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS first_value_desc,
    last_value(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS last_value_desc
FROM agg_phase0
GROUP BY grp
ORDER BY grp;

-- T2.5.2 string_agg
SELECT string_agg(label) AS labels_default_sep
FROM agg_phase0;

SELECT string_agg(label, '|') FILTER (WHERE grp = 1) AS labels_grp1
FROM agg_phase0;

SELECT
    grp,
    string_agg(label, '|') WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS labels_desc
FROM agg_phase0
GROUP BY grp
ORDER BY grp;

-- T2.5.3 array_agg
SELECT array_agg(x) AS xs_all
FROM agg_phase0;

SELECT array_agg(x) FILTER (WHERE x >= 2) AS xs_ge_2
FROM agg_phase0;

SELECT
    grp,
    array_agg(x) WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS xs_desc
FROM agg_phase0
GROUP BY grp
ORDER BY grp;

-- T2.5.4 bool_and / bool_or
SELECT
    bool_and(flag) AS all_true,
    bool_or(flag) AS any_true
FROM agg_phase0;

SELECT
    grp,
    bool_and(flag) FILTER (WHERE y <= 3) AS all_true_y_le_3,
    bool_or(flag) FILTER (WHERE y <= 3) AS any_true_y_le_3
FROM agg_phase0
GROUP BY grp
ORDER BY grp;

SELECT
    bool_and(flag) FILTER (WHERE y > 100) AS all_true_empty,
    bool_or(flag) FILTER (WHERE y > 100) AS any_true_empty
FROM agg_phase0;

-- T2.5.5 stddev / variance family
SELECT
    round(var_pop(x), 6) AS var_pop_x,
    round(var_samp(x), 6) AS var_samp_x,
    round(variance(x), 6) AS variance_x,
    round(stddev_pop(x), 6) AS stddev_pop_x,
    round(stddev_samp(x), 6) AS stddev_samp_x,
    round(stddev(x), 6) AS stddev_x
FROM agg_phase0;

SELECT
    grp,
    round(var_pop(x), 6) AS var_pop_x,
    round(stddev_samp(x), 6) AS stddev_samp_x
FROM agg_phase0
GROUP BY grp
ORDER BY grp;

-- Phase 2 acceptance: distinct + filter grouped / ungrouped combinations
SELECT
    sum(DISTINCT x) FILTER (WHERE grp = 1) AS sum_distinct_grp1,
    count(DISTINCT y) FILTER (WHERE flag) AS count_distinct_y_flag
FROM agg_phase0;

SELECT
    grp,
    sum(DISTINCT x) FILTER (WHERE flag) AS sum_distinct_x_flag,
    count(DISTINCT y) FILTER (WHERE NOT flag) AS count_distinct_y_not_flag
FROM agg_phase0
GROUP BY grp
ORDER BY grp;

EXPLAIN SELECT grp,
       string_agg(label, '|') WITHIN GROUP (ORDER BY y DESC NULLS LAST) AS labels_desc,
       array_agg(x) FILTER (WHERE x >= 2) AS xs_ge_2,
       stddev(x) AS stddev_x
FROM (
    VALUES (1, 1, 1, 'a'),
           (1, 2, 2, 'b'),
           (1, 2, 3, 'c'),
           (2, 3, 1, 'd'),
           (2, 4, 4, 'e'),
           (2, NULL, 5, 'f')
) AS agg_values_t25(grp, x, y, label)
GROUP BY grp;

-- Phase 3 T3.1 acceptance coverage: perfect hash aggregate
DROP TABLE IF EXISTS agg_phase3;
CREATE TABLE agg_phase3 (
    g_tiny TINYINT,
    h_tiny TINYINT,
    x INT,
    flag BOOLEAN
);
INSERT INTO agg_phase3 VALUES
    (1, 1, 10, true),
    (1, 1, 20, false),
    (1, 2, 30, true),
    (2, 1, 40, false),
    (2, 2, 50, true),
    (2, 2, 60, false),
    (NULL, 1, 70, true),
    (NULL, NULL, 80, false);

SELECT
    g_tiny,
    sum(x) AS sum_x
FROM agg_phase3
GROUP BY g_tiny
ORDER BY g_tiny;

EXPLAIN SELECT
    g_tiny,
    sum(x) AS sum_x
FROM (
    VALUES (1::TINYINT, 1::TINYINT, 10, true),
           (1::TINYINT, 1::TINYINT, 20, false),
           (1::TINYINT, 2::TINYINT, 30, true),
           (2::TINYINT, 1::TINYINT, 40, false),
           (2::TINYINT, 2::TINYINT, 50, true),
           (2::TINYINT, 2::TINYINT, 60, false),
           (NULL::TINYINT, 1::TINYINT, 70, true),
           (NULL::TINYINT, NULL::TINYINT, 80, false)
) AS agg_phase3_values(g_tiny, h_tiny, x, flag)
GROUP BY g_tiny;

SELECT
    g_tiny,
    h_tiny,
    count(*) AS cnt,
    sum(x) FILTER (WHERE flag) AS sum_flag_true
FROM agg_phase3
GROUP BY g_tiny, h_tiny
ORDER BY g_tiny, h_tiny;

EXPLAIN SELECT
    g_tiny,
    h_tiny,
    count(*) AS cnt,
    sum(x) FILTER (WHERE flag) AS sum_flag_true
FROM (
    VALUES (1::TINYINT, 1::TINYINT, 10, true),
           (1::TINYINT, 1::TINYINT, 20, false),
           (1::TINYINT, 2::TINYINT, 30, true),
           (2::TINYINT, 1::TINYINT, 40, false),
           (2::TINYINT, 2::TINYINT, 50, true),
           (2::TINYINT, 2::TINYINT, 60, false),
           (NULL::TINYINT, 1::TINYINT, 70, true),
           (NULL::TINYINT, NULL::TINYINT, 80, false)
) AS agg_phase3_values(g_tiny, h_tiny, x, flag)
GROUP BY g_tiny, h_tiny;

EXPLAIN SELECT
    g_tiny,
    count(DISTINCT x) AS cd_x
FROM (
    VALUES (1::TINYINT, 1::TINYINT, 10, true),
           (1::TINYINT, 1::TINYINT, 20, false),
           (1::TINYINT, 2::TINYINT, 30, true),
           (2::TINYINT, 1::TINYINT, 40, false),
           (2::TINYINT, 2::TINYINT, 50, true),
           (2::TINYINT, 2::TINYINT, 60, false),
           (NULL::TINYINT, 1::TINYINT, 70, true),
           (NULL::TINYINT, NULL::TINYINT, 80, false)
) AS agg_phase3_values(g_tiny, h_tiny, x, flag)
GROUP BY g_tiny;

EXPLAIN SELECT
    g_tiny,
    sum(x) AS sum_x
FROM (
    VALUES (1::TINYINT, 1::TINYINT, 10, true),
           (1::TINYINT, 1::TINYINT, 20, false),
           (1::TINYINT, 2::TINYINT, 30, true),
           (2::TINYINT, 1::TINYINT, 40, false),
           (2::TINYINT, 2::TINYINT, 50, true),
           (2::TINYINT, 2::TINYINT, 60, false),
           (NULL::TINYINT, 1::TINYINT, 70, true),
           (NULL::TINYINT, NULL::TINYINT, 80, false)
) AS agg_phase3_values(g_tiny, h_tiny, x, flag)
GROUP BY GROUPING SETS ((g_tiny), ());

-- Phase 3 T3.2 acceptance coverage: fixed-width inline key fast path
SELECT
    (k_int + (tag * 3000000)) AS inline_key,
    count(*) AS cnt
FROM (
    VALUES (1, 0),
           (1, 1),
           (2, 0),
           (2, 1),
           (3, 0),
           (3, 1)
) AS agg_phase32_single(k_int, tag)
GROUP BY (k_int + (tag * 3000000))
ORDER BY inline_key;

EXPLAIN SELECT
    (k_int + (tag * 3000000)) AS inline_key,
    count(*) AS cnt
FROM (
    VALUES (1, 0),
           (1, 1),
           (2, 0),
           (2, 1),
           (3, 0),
           (3, 1)
) AS agg_phase32_single(k_int, tag)
GROUP BY (k_int + (tag * 3000000));

EXPLAIN SELECT
    (k_int + (tag * 3000000)) AS k_inline_int,
    k_small,
    k_tiny,
    sum(v) AS sum_v
FROM (
    VALUES (1, 1::SMALLINT, 1::TINYINT, 0, 10),
           (1, 1::SMALLINT, 1::TINYINT, 1, 20),
           (2, 2::SMALLINT, 2::TINYINT, 0, 30),
           (2, 2::SMALLINT, 2::TINYINT, 1, 40)
) AS agg_phase32_multi(k_int, k_small, k_tiny, tag, v)
GROUP BY (k_int + (tag * 3000000)), k_small, k_tiny;

EXPLAIN SELECT
    (k_big + (tag::BIGINT * 30000000000::BIGINT)) AS k_big_range,
    k_int,
    sum(v) AS sum_v
FROM (
    VALUES (1::BIGINT, 1, 0, 10),
           (1::BIGINT, 1, 1, 20),
           (2::BIGINT, 2, 0, 30),
           (2::BIGINT, 2, 1, 40)
) AS agg_phase32_wide(k_big, k_int, tag, v)
GROUP BY (k_big + (tag::BIGINT * 30000000000::BIGINT)), k_int;

-- Phase 3 T3.4 acceptance coverage: radix partitioned hash table
EXPLAIN SELECT
    k_big,
    k_int,
    sum(v) AS sum_v
FROM (
    VALUES (1::BIGINT, 1, 10),
           (5000000000::BIGINT, 1, 20),
           (5000000001::BIGINT, 2, 30),
           (9000000000::BIGINT, 2, 40)
) AS agg_phase34(k_big, k_int, v)
GROUP BY k_big, k_int;

EXPLAIN SELECT
    k_big,
    k_int,
    sum(v) AS sum_v
FROM (
    VALUES (1::BIGINT, 1, 10),
           (5000000000::BIGINT, 1, 20),
           (5000000001::BIGINT, 2, 30),
           (9000000000::BIGINT, 2, 40)
) AS agg_phase34(k_big, k_int, v)
GROUP BY GROUPING SETS ((k_big, k_int), ());

-- Phase 3 T3.5 acceptance coverage: memory tracking + partition spill + restore/reprobe
SET threads = 1;
SET use_new_agg_spill = true;
DROP TABLE IF EXISTS agg_phase35_spill;
CREATE TABLE agg_phase35_spill(k_unique BIGINT, k_repeat BIGINT, tag VARCHAR, v BIGINT);
INSERT INTO agg_phase35_spill
SELECT
    (rep::BIGINT * 1000000::BIGINT) + k::BIGINT AS k_unique,
    k::BIGINT AS k_repeat,
    CASE WHEN rep % 2 = 0 THEN 'even' ELSE 'odd' END AS tag,
    1::BIGINT AS v
FROM generate_series(1, 80) AS rep_series(rep)
CROSS JOIN generate_series(0, 3999) AS key_series(k);

SELECT count(*) AS g_cnt
FROM (
    SELECT k_unique, tag, sum(v) AS s
    FROM agg_phase35_spill
    GROUP BY k_unique, tag
) AS agg_phase35_unique;

SELECT
    min(cnt) AS min_cnt,
    max(cnt) AS max_cnt,
    count(*) AS group_cnt
FROM (
    SELECT k_repeat, tag, count(*) AS cnt
    FROM agg_phase35_spill
    GROUP BY k_repeat, tag
) AS agg_phase35_overlap;

SELECT
    count(*) AS hot_group_cnt,
    min(cnt) AS min_cnt,
    max(cnt) AS max_cnt
FROM (
    SELECT k_repeat, count(*) AS cnt
    FROM agg_phase35_spill
    GROUP BY k_repeat
    HAVING count(*) > 70
) AS agg_phase35_having;

DROP TABLE IF EXISTS agg_phase35_spill;
RESET use_new_agg_spill;
SET threads = DEFAULT;

-- Phase 4 T4.1 acceptance coverage: common aggregate dedup
EXPLAIN SELECT
    sum(x) AS s1,
    sum(x) + 1 AS s2,
    sum(x) + count(*) AS s3
FROM (
    VALUES (1),
           (2),
           (3)
) AS agg_phase41(x);

EXPLAIN SELECT
    grp,
    sum(x) AS sx,
    sum(x) + 1 AS sx_plus_1,
    sum(x) + count(*) AS sx_plus_cnt
FROM (
    VALUES (1, 1),
           (1, 2),
           (1, 2),
           (2, 3),
           (2, 4),
           (2, NULL)
) AS agg_phase41_grouped(grp, x)
GROUP BY grp
ORDER BY grp;

SELECT
    grp,
    sum(x) AS sx,
    sum(x) + 1 AS sx_plus_1,
    sum(x) + count(*) AS sx_plus_cnt
FROM agg_phase0
GROUP BY grp
ORDER BY grp;

-- Phase 4 T4.2 acceptance coverage: stats-based aggregate execution
EXPLAIN SELECT
    count(*) AS cnt_star,
    min(x) AS min_x,
    max(x) AS max_x
FROM agg_phase0;

SELECT
    count(*) AS cnt_star,
    min(x) AS min_x,
    max(x) AS max_x
FROM agg_phase0;

EXPLAIN SELECT
    min(px) AS min_px,
    max(px) AS max_px
FROM (
    SELECT x AS px
    FROM agg_phase0
) AS agg_phase42_proj;

SELECT
    min(px) AS min_px,
    max(px) AS max_px
FROM (
    SELECT x AS px
    FROM agg_phase0
) AS agg_phase42_proj;

EXPLAIN SELECT
    min(y) AS min_y,
    max(y) AS max_y
FROM agg_phase0;

SELECT
    min(y) AS min_y,
    max(y) AS max_y
FROM agg_phase0;

EXPLAIN SELECT min(x) FILTER (WHERE x > 2) AS min_filtered
FROM (
    VALUES (1),
           (2),
           (2),
           (3),
           (4),
           (NULL)
) AS agg_phase42_filter(x);

SELECT min(x) FILTER (WHERE x > 2) AS min_filtered
FROM (
    VALUES (1),
           (2),
           (2),
           (3),
           (4),
           (NULL)
) AS agg_phase42_filter(x);

DROP TABLE IF EXISTS agg_phase3;

-- @teardown
DROP TABLE IF EXISTS agg_phase0;
