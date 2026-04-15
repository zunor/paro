-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @setup
DROP TABLE IF EXISTS type_roundtrip_decimal;
DROP TABLE IF EXISTS type_roundtrip_decimal_meta;

CREATE TABLE type_roundtrip_decimal (
  id INT,
  d DECIMAL(10,2)
);

CREATE TABLE type_roundtrip_decimal_meta (
  id INT,
  d0 DECIMAL(4,0),
  d2 DECIMAL(12,2),
  d4 DECIMAL(18,4)
);

-- @query
SELECT
  CAST(NULL AS DECIMAL(10,2)) AS d_null,
  CAST(NULL AS DECIMAL(18,4)) AS d_null2;

INSERT INTO type_roundtrip_decimal VALUES
  (1, NULL),
  (2, CAST('0' AS DECIMAL(10,2))),
  (3, CAST('123.45' AS DECIMAL(10,2))),
  (4, CAST('-99999.99' AS DECIMAL(10,2)));

INSERT INTO type_roundtrip_decimal_meta VALUES
  (1, CAST('0' AS DECIMAL(4,0)), CAST('12.34' AS DECIMAL(12,2)), CAST('0.0001' AS DECIMAL(18,4))),
  (2, CAST('9999' AS DECIMAL(4,0)), CAST('1234567890.12' AS DECIMAL(12,2)), CAST('1234567890123.4567' AS DECIMAL(18,4))),
  (3, NULL, NULL, NULL);

-- @query rowsort
SELECT id, d
FROM type_roundtrip_decimal
ORDER BY id;

-- @query rowsort
SELECT id, d0, d2, d4
FROM type_roundtrip_decimal_meta
ORDER BY id;

-- @query rowsort
SELECT table_name, column_name, data_type
FROM paro_columns()
WHERE table_name IN ('type_roundtrip_decimal', 'type_roundtrip_decimal_meta')
ORDER BY table_name, column_name;

-- @teardown
DROP TABLE IF EXISTS type_roundtrip_decimal;
DROP TABLE IF EXISTS type_roundtrip_decimal_meta;
