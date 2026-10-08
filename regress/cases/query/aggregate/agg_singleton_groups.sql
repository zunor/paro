-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- End-to-end coverage for a Q13-shaped left join reduction. The declared
-- nullable UNIQUE key does not prove grouping uniqueness, even while its
-- observed data is NULL-free. Only a revalidated schema/predicate guarantee
-- may authorize singleton lowering in a reusable plan. Duplicate NULLs must
-- retain ordinary GROUP BY multiplicity.
-- @setup
DROP TABLE IF EXISTS singleton_orders;
DROP TABLE IF EXISTS singleton_orders_large;
DROP TABLE IF EXISTS singleton_customer;
DROP TABLE IF EXISTS nullable_singleton_customer;
DROP TABLE IF EXISTS prefix_nullable_singleton_customer;

CREATE TABLE singleton_customer (
    customer_key BIGINT,
    UNIQUE (customer_key) NOT ENFORCED
);
CREATE TABLE singleton_orders (
    order_key BIGINT,
    customer_key BIGINT,
    comment VARCHAR,
    UNIQUE (order_key) NOT ENFORCED
);
CREATE TABLE singleton_orders_large (
    order_key BIGINT,
    customer_key BIGINT,
    comment VARCHAR,
    UNIQUE (order_key) NOT ENFORCED
);
CREATE TABLE nullable_singleton_customer (
    customer_key BIGINT,
    UNIQUE (customer_key) NOT ENFORCED
);
CREATE TABLE prefix_nullable_singleton_customer (
    phone VARCHAR,
    customer_key BIGINT,
    UNIQUE (customer_key) NOT ENFORCED
);

INSERT INTO singleton_customer VALUES (1), (2), (3);
INSERT INTO singleton_orders VALUES
    (10, 1, 'ordinary order'),
    (11, 1, 'special handling requests'),
    (12, 2, NULL);
INSERT INTO singleton_orders_large
SELECT i, (i % 2) + 1,
       CASE WHEN i % 10 = 0 THEN 'special handling requests' ELSE 'ordinary order' END
FROM generate_series(1, 4096) AS generated(i);
INSERT INTO nullable_singleton_customer VALUES (1), (NULL), (NULL);
INSERT INTO prefix_nullable_singleton_customer VALUES
    ('13-a', 1), ('13-b', NULL), ('13-c', NULL);

-- The extra partial-merge AGGREGATE is required for this nullable UNIQUE
-- declaration. The plan-quality corpus tracks it as a performance opportunity,
-- not as permission to treat a NULL-free data observation as a schema proof.
-- A future NULL-aware singleton implementation needs a lossless grouping path
-- for NULL tuples; merely forwarding rows would change their multiplicity.
EXPLAIN SELECT c_count, count(*) AS customer_distribution
FROM (
    SELECT c.customer_key, count(o.order_key) AS c_count
    FROM singleton_customer AS c
    LEFT JOIN singleton_orders_large AS o
      ON c.customer_key = o.customer_key
     AND o.comment NOT LIKE '%special%requests%'
    GROUP BY c.customer_key
) AS counts
GROUP BY c_count
ORDER BY c_count;

SELECT c_count, count(*) AS customer_distribution
FROM (
    SELECT c.customer_key, count(o.order_key) AS c_count
    FROM singleton_customer AS c
    LEFT JOIN singleton_orders AS o
      ON c.customer_key = o.customer_key
     AND o.comment NOT LIKE '%special%requests%'
    GROUP BY c.customer_key
) AS counts
GROUP BY c_count
ORDER BY c_count;

-- SUM partial merge publishes the distinct Input singleton law: an unmatched
-- customer remains NULL rather than receiving COUNT's zero fallback.
SELECT order_sum, count(*) AS customer_distribution
FROM (
    SELECT c.customer_key, sum(o.order_key) AS order_sum
    FROM singleton_customer AS c
    LEFT JOIN singleton_orders AS o
      ON c.customer_key = o.customer_key
     AND o.comment NOT LIKE '%special%requests%'
    GROUP BY c.customer_key
) AS sums
GROUP BY order_sum
ORDER BY order_sum NULLS FIRST;

SELECT c_count, count(*) AS grouped_key_distribution
FROM (
    SELECT c.customer_key, count(o.order_key) AS c_count
    FROM nullable_singleton_customer AS c
    LEFT JOIN singleton_orders AS o
      ON c.customer_key = o.customer_key
    GROUP BY c.customer_key
) AS counts
GROUP BY c_count
ORDER BY c_count;

-- Matched-prefix lowering removes the stored phone output and shifts the
-- nullable key from ordinal 1 to ordinal 0. Statistics must be re-gathered in
-- that new binding domain; stale phone statistics would incorrectly prove the
-- duplicate NULL key unique under GROUP BY equality.
SELECT c_count, count(*) AS grouped_key_distribution
FROM (
    SELECT c.customer_key, count(o.order_key) AS c_count
    FROM prefix_nullable_singleton_customer AS c
    LEFT JOIN singleton_orders AS o
      ON c.customer_key = o.customer_key
    WHERE substring(c.phone FROM 1 FOR 2) IN ('13')
    GROUP BY c.customer_key
) AS counts
GROUP BY c_count
ORDER BY c_count;

-- Reusing a plan after a data-only change must not carry an observation-based
-- no-NULL proof from the first execution into the second execution.
BEGIN;
PREPARE singleton_reuse AS
SELECT c.customer_key, count(o.order_key)
FROM singleton_customer c LEFT JOIN singleton_orders o
  ON c.customer_key = o.customer_key
GROUP BY c.customer_key ORDER BY c.customer_key NULLS FIRST;
-- @query nosort
EXECUTE singleton_reuse;
INSERT INTO singleton_customer VALUES (NULL), (NULL);
-- @query nosort
EXECUTE singleton_reuse;
DEALLOCATE singleton_reuse;
ROLLBACK;

-- @teardown
DROP TABLE IF EXISTS prefix_nullable_singleton_customer;
DROP TABLE IF EXISTS nullable_singleton_customer;
DROP TABLE IF EXISTS singleton_orders_large;
DROP TABLE IF EXISTS singleton_orders;
DROP TABLE IF EXISTS singleton_customer;
