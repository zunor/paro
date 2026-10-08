-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Q11-shaped shared UNION producer. The subtraction must remain inside SUM:
-- the NULL inputs below make SUM(a)-SUM(b) a different query.
CREATE TABLE cte_domain_store (customer INTEGER, yr INTEGER, a DECIMAL(10,2), b DECIMAL(10,2));
CREATE TABLE cte_domain_web (customer INTEGER, yr INTEGER, a DECIMAL(10,2), b DECIMAL(10,2));
INSERT INTO cte_domain_store VALUES
    (1,2001,20,10),(1,2002,40,10),(1,2002,100,NULL),
    (2,2001,30,10),(2,2002,30,10),(3,2001,5,10),(3,2002,40,10);
INSERT INTO cte_domain_web VALUES
    (1,2001,20,10),(1,2002,30,10),(1,2002,NULL,100),
    (2,2001,20,10),(2,2002,40,10),(3,2001,20,10),(3,2002,30,10);

WITH yearly AS (
    SELECT customer, yr, 'store' AS channel, SUM(a-b) AS total
    FROM cte_domain_store GROUP BY customer, yr
    UNION ALL
    SELECT customer, yr, 'web' AS channel, SUM(a-b) AS total
    FROM cte_domain_web GROUP BY customer, yr
)
SELECT s1.customer, s1.total AS s2001, s2.total AS s2002,
       w1.total AS w2001, w2.total AS w2002
FROM yearly s1, yearly s2, yearly w1, yearly w2
WHERE s1.customer=s2.customer AND s1.customer=w1.customer AND s1.customer=w2.customer
  AND s1.channel='store' AND s2.channel='store' AND w1.channel='web' AND w2.channel='web'
  AND s1.yr=2001 AND s2.yr=2002 AND w1.yr=2001 AND w2.yr=2002
  AND s1.total>0 AND w1.total>0 AND s2.total/s1.total>w2.total/w1.total
ORDER BY s1.customer;

DROP TABLE cte_domain_web;
DROP TABLE cte_domain_store;
