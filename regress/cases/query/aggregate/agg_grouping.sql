-- @setup
DROP TABLE IF EXISTS agg_grp_t;
CREATE TABLE agg_grp_t (
    region VARCHAR,
    city VARCHAR,
    sales INT
);
INSERT INTO agg_grp_t VALUES
    ('east', 'shanghai', 10),
    ('east', 'shanghai', 20),
    ('east', 'hangzhou', 15),
    ('west', 'chengdu', 30),
    ('west', NULL, 5),
    (NULL, NULL, 8);

-- GROUPING SETS
SELECT
    GROUPING(region) AS g_region,
    region,
    sum(sales) AS sum_sales
FROM agg_grp_t
GROUP BY GROUPING SETS ((region), ())
ORDER BY g_region, region;

-- ROLLUP
SELECT
    GROUPING(region) AS g_region,
    GROUPING(city) AS g_city,
    region,
    city,
    count(*) AS cnt
FROM agg_grp_t
GROUP BY ROLLUP(region, city)
ORDER BY g_region, g_city, region, city;

-- CUBE
SELECT
    GROUPING(region) AS g_region,
    GROUPING(city) AS g_city,
    region,
    city,
    sum(sales) AS sum_sales
FROM agg_grp_t
GROUP BY CUBE(region, city)
ORDER BY g_region, g_city, region, city;

EXPLAIN SELECT
    GROUPING(region) AS g_region,
    region,
    sum(sales) AS sum_sales
FROM (
    VALUES ('east', 10),
           ('west', 20),
           ('west', 30)
) AS agg_grp_values(region, sales)
GROUP BY GROUPING SETS ((region), ())
ORDER BY g_region, region;

-- @teardown
DROP TABLE IF EXISTS agg_grp_t;
