-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Result-level coverage for grouping fact rows by compact unique dimension
-- keys before attaching descriptive payload. Distinct keys deliberately share
-- one payload so the final merge aggregate is correctness-bearing.
-- @setup
DROP TABLE IF EXISTS agg_defer_fact;
DROP TABLE IF EXISTS agg_defer_dimension;
CREATE TABLE agg_defer_dimension (
    key_a BIGINT,
    key_b BIGINT,
    payload VARCHAR,
    PRIMARY KEY (key_a, key_b)
);
CREATE TABLE agg_defer_fact (
    key_a BIGINT,
    key_b BIGINT,
    amount BIGINT,
    keep BOOLEAN
);
INSERT INTO agg_defer_dimension VALUES
    (1, 1, 'same'),
    (2, 2, 'same'),
    (3, 3, NULL),
    (4, 4, 'other');
INSERT INTO agg_defer_fact
SELECT 1, 1, 10, true FROM generate_series(1, 2000) AS generated(i);
INSERT INTO agg_defer_fact
SELECT 2, 2, 20, false FROM generate_series(1, 2000) AS generated(i);
INSERT INTO agg_defer_fact
SELECT 3, 3, NULL, true FROM generate_series(1, 2000) AS generated(i);
INSERT INTO agg_defer_fact
SELECT 4, 4, 5, true FROM generate_series(1, 2000) AS generated(i);
INSERT INTO agg_defer_fact
SELECT NULL, NULL, 100, true FROM generate_series(1, 2000) AS generated(i);
INSERT INTO agg_defer_fact
SELECT 9, 9, 200, true FROM generate_series(1, 2000) AS generated(i);

SELECT
    d.payload,
    count(*) AS row_count,
    sum(f.amount) AS total_amount,
    sum(f.amount) FILTER (WHERE f.keep) AS kept_amount
FROM agg_defer_fact AS f
JOIN agg_defer_dimension AS d
  ON f.key_a = d.key_a AND f.key_b = d.key_b
GROUP BY d.payload
ORDER BY d.payload NULLS LAST;

-- @teardown
DROP TABLE IF EXISTS agg_defer_fact;
DROP TABLE IF EXISTS agg_defer_dimension;
