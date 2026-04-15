# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS type_roundtrip_pk;
DROP TABLE IF EXISTS type_roundtrip_nonpk;

CREATE TABLE type_roundtrip_pk (
  id INT PRIMARY KEY,
  b BOOLEAN,
  i INT,
  u UINT,
  f FLOAT,
  d DOUBLE,
  v VARCHAR,
  dt DATE,
  ts TIMESTAMP
);

CREATE TABLE type_roundtrip_nonpk (
  id INT,
  b BOOLEAN,
  i INT,
  u UINT,
  f FLOAT,
  d DOUBLE,
  v VARCHAR,
  dt DATE,
  ts TIMESTAMP
);

INSERT INTO type_roundtrip_pk VALUES
  (1, true, 42, 4000000000, 1.5, 2.25, 'alpha', '2024-01-01', '2024-01-01 12:00:00'),
  (2, false, -7, 0, 0, -3.5, '', '1970-01-01', '1970-01-01 00:00:00'),
  (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO type_roundtrip_nonpk VALUES
  (1, true, 42, 4000000000, 1.5, 2.25, 'alpha', '2024-01-01', '2024-01-01 12:00:00'),
  (2, false, -7, 0, 0, -3.5, '', '1970-01-01', '1970-01-01 00:00:00'),
  (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- @query approx(0.0001)
SELECT 'pk' AS path, id, b, i, u, f, d, v, dt, ts
FROM type_roundtrip_pk
UNION ALL
SELECT 'nonpk' AS path, id, b, i, u, f, d, v, dt, ts
FROM type_roundtrip_nonpk
ORDER BY path, id;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_pk;
DROP TABLE IF EXISTS type_roundtrip_nonpk;
