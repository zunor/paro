# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS type_roundtrip_integers;

CREATE TABLE type_roundtrip_integers (
  id INT,
  t TINYINT,
  s SMALLINT,
  i INT,
  b BIGINT,
  h HUGEINT,
  uh UHUGEINT,
  ut TINYINT UNSIGNED,
  us SMALLINT UNSIGNED,
  ui INT UNSIGNED,
  ub BIGINT UNSIGNED
);

INSERT INTO type_roundtrip_integers VALUES
  (
    1,
    -128,
    -32768,
    -2147483648,
    -9223372036854775807,
    -170141183460469231731687303715884105728,
    0,
    0,
    0,
    0,
    0
  ),
  (
    2,
    127,
    32767,
    2147483647,
    9223372036854775807,
    170141183460469231731687303715884105727,
    340282366920938463463374607431768211455,
    255,
    65535,
    4294967295,
    9223372036854775807
  ),
  (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- @query rowsort
SELECT id, t, s, i, b, h, uh, ut, us, ui, ub
FROM type_roundtrip_integers
ORDER BY id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_integers;
