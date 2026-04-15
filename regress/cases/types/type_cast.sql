# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

SELECT
  CAST(true AS INTEGER) AS bool_to_int_true,
  CAST(false AS INTEGER) AS bool_to_int_false,
  CAST(1 AS BOOLEAN) AS int_to_bool_true,
  CAST(0 AS BOOLEAN) AS int_to_bool_false;

SELECT CAST('2024-01-31' AS DATE) AS d;

SELECT CAST('2024-01-31 09:10:11.123456' AS TIMESTAMP) AS ts;

SELECT CAST('09:10:11.123456' AS TIME) AS t;

SELECT
  CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID) AS u,
  CAST(CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID) AS VARCHAR) AS u_text;

SELECT
  CAST('{"a":1,"b":[true,false,null]}' AS JSON) AS j,
  CAST('{"a":1,"b":[true,false,null]}' AS JSONB) AS jb;

SELECT
  CAST(CAST('{"a":1,"b":[true,false,null]}' AS JSON) AS VARCHAR) AS j_text,
  CAST(CAST('{"a":1,"b":[true,false,null]}' AS JSONB) AS VARCHAR) AS jb_text;

SELECT
  TRY_CAST('{"a":1}' AS JSON) IS NULL AS json_valid_is_null,
  TRY_CAST('{bad json' AS JSON) IS NULL AS json_invalid_is_null,
  TRY_CAST('{"a":1}' AS JSONB) IS NULL AS jsonb_valid_is_null,
  TRY_CAST('{bad json' AS JSONB) IS NULL AS jsonb_invalid_is_null;

SELECT
  CAST(123 AS DECIMAL(5,2)) AS int_to_dec,
  CAST(-7 AS DECIMAL(4,1)) AS int_to_dec_neg;

SELECT
  CAST('123.40' AS DECIMAL(6,2)) AS str_to_dec,
  CAST('-0.50' AS DECIMAL(4,2)) AS str_to_dec_neg;

SELECT
  CAST(1.25 AS DECIMAL(4,2)) AS float_to_dec,
  CAST(12.0 AS DECIMAL(4,0)) AS float_to_dec_scale0;

SELECT
  CAST(CAST('123.00' AS DECIMAL(6,2)) AS INTEGER) AS dec_to_int,
  CAST(CAST('456.00' AS DECIMAL(6,2)) AS BIGINT) AS dec_to_bigint;

SELECT
  CAST(CAST('12.5' AS DECIMAL(4,1)) AS DOUBLE) AS dec_to_double;

SELECT
  TRY_CAST('not-a-number' AS DECIMAL(6,2)) IS NULL AS dec_invalid_is_null;

SELECT
  CAST(CAST('1.235' AS DECIMAL(5,3)) AS DECIMAL(4,2)) AS dec_scale_down_round,
  CAST(CAST('1.2' AS DECIMAL(4,1)) AS DECIMAL(6,3)) AS dec_scale_up;

SELECT
  CAST(CAST('12.5' AS DECIMAL(4,1)) AS INTEGER) AS dec_round_to_int,
  CAST(CAST('-12.5' AS DECIMAL(4,1)) AS INTEGER) AS dec_round_to_int_neg;

SELECT
  TRY_CAST('10000' AS DECIMAL(4,0)) IS NULL AS dec_overflow_is_null;
