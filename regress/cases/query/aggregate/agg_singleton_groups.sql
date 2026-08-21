-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- End-to-end coverage for a Q13-shaped left join reduction. The declared key
-- and exact no-NULL data admit singleton lowering; the second query uses a
-- nullable UNIQUE key with duplicate NULLs and must retain ordinary GROUP BY
-- multiplicity.
-- @setup
DROP TABLE IF EXISTS singleton_orders;
DROP TABLE IF EXISTS singleton_customer;
DROP TABLE IF EXISTS nullable_singleton_customer;

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
CREATE TABLE nullable_singleton_customer (
    customer_key BIGINT,
    UNIQUE (customer_key) NOT ENFORCED
);

INSERT INTO singleton_customer VALUES (1), (2), (3);
INSERT INTO singleton_orders VALUES
    (10, 1, 'ordinary order'),
    (11, 1, 'special handling requests'),
    (12, 2, NULL);
INSERT INTO nullable_singleton_customer VALUES (1), (NULL), (NULL);

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

-- @teardown
DROP TABLE IF EXISTS nullable_singleton_customer;
DROP TABLE IF EXISTS singleton_orders;
DROP TABLE IF EXISTS singleton_customer;
