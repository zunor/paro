-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Moving aggregate inputs below a row-removing join requires a proof that the
-- expression is total. The unmatched fact row deliberately overflows the
-- declared DECIMAL(38, 38) multiplication result; the original join domain
-- must remove it before evaluation.
-- @setup
DROP TABLE IF EXISTS agg_input_materialization_fact;
DROP TABLE IF EXISTS agg_input_materialization_keys;
CREATE TABLE agg_input_materialization_fact (
    key INTEGER,
    left_value DECIMAL(38, 20),
    right_value DECIMAL(38, 20)
);
CREATE TABLE agg_input_materialization_keys (key INTEGER);
INSERT INTO agg_input_materialization_fact VALUES
    (1,
     CAST('0.1' AS DECIMAL(38, 20)),
     CAST('0.1' AS DECIMAL(38, 20))),
    (2,
     CAST('1' AS DECIMAL(38, 20)),
     CAST('1' AS DECIMAL(38, 20)));
INSERT INTO agg_input_materialization_keys VALUES (1);

SELECT sum(f.left_value * f.right_value) AS safe_product
FROM agg_input_materialization_fact AS f
JOIN agg_input_materialization_keys AS k ON f.key = k.key;

-- @teardown
DROP TABLE IF EXISTS agg_input_materialization_fact;
DROP TABLE IF EXISTS agg_input_materialization_keys;
