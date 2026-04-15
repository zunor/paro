-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0
--
-- Derived from Databend (https://github.com/datafuselabs/databend),
-- Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

INSERT FIRST
  WHEN n1 > 100 THEN
    INTO t1
  WHEN n1 > 10 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT n1 from src;

INSERT OVERWRITE FIRST
  WHEN n1 > 100 THEN
    INTO t1
  WHEN n1 > 10 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT n1 from src;

INSERT OVERWRITE ALL
  WHEN n1 > 100 THEN
    INTO t1
  WHEN n1 > 10 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT n1 from src;

INSERT OVERWRITE ALL
    INTO t1
    INTO t2
SELECT n1 from src;

INSERT OVERWRITE FIRST
  WHEN n1 > 100 THEN
    INTO t1
  WHEN n1 > 10 THEN
    INTO t1
    INTO t2
SELECT n1 from src;

INSERT OVERWRITE ALL
  WHEN n1 > 100 THEN
    INTO t1
  WHEN n1 > 10 THEN
    INTO t1
    INTO t2
SELECT n1 from src;

INSERT ALL
  INTO t1
  INTO t1 (c1, c2, c3) VALUES (n2, n1, n3)
  INTO t2 (c1, c2, c3)
  INTO t2 VALUES (n3, n2, n1)
SELECT n1, n2, n3 from src;

INSERT OVERWRITE ALL
  INTO t1
  INTO t1 (c1, c2, c3) VALUES (n2, n1, n3)
  INTO t2 (c1, c2, c3)
  INTO t2 VALUES (n3, n2, n1)
SELECT n1, n2, n3 from src;