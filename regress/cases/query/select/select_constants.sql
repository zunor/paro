# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

DROP TABLE IF EXISTS t_const;
CREATE TABLE t_const (x INT);
INSERT INTO t_const VALUES (1);

SELECT 1;
SELECT 42;
SELECT 42 AS answer;
SELECT 1 + 0;
SELECT count(*) FROM t_const;

DROP TABLE IF EXISTS t_const;
