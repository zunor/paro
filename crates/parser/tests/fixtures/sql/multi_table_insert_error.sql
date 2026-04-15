# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0
#
# Derived from Databend (https://github.com/datafuselabs/databend),
# Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

INSERT FIRST
  ELSE
    INTO t1
    INTO t2
SELECT n1 from src;

INSERT FIRST
  ELSE
    INTO t1
    INTO t2
SELECT n1 from src;

INSERT ALL
  ELSE
    INTO t1
    INTO t2
SELECT n1 from src;