# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS agg_test;
CREATE TABLE agg_test (x INT);
INSERT INTO agg_test VALUES (1), (2), (3);

SELECT count(*) FROM agg_test;
SELECT sum(x) FROM agg_test;
SELECT count(*) + sum(x) FROM agg_test;

-- @teardown
DROP TABLE IF EXISTS agg_test;
