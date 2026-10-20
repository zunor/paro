-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Exact decimals, binary floating point, booleans and NULL use scalar and vector conversion.

CREATE FUNCTION py_number_decimal_scalar(a DECIMAL(38, 9)) RETURNS DECIMAL(38, 9) LANGUAGE python
AS $$
return a
$$;

SELECT py_number_decimal_scalar('-99999999999999999999999999999.123456789'::DECIMAL(38, 9)) IS NOT DISTINCT FROM '-99999999999999999999999999999.123456789'::DECIMAL(38, 9) AS preserved;

SELECT py_number_decimal_scalar('0.000000001'::DECIMAL(38, 9)) IS NOT DISTINCT FROM '0.000000001'::DECIMAL(38, 9) AS preserved;

SELECT py_number_decimal_scalar('99999999999999999999999999999.999999999'::DECIMAL(38, 9)) IS NOT DISTINCT FROM '99999999999999999999999999999.999999999'::DECIMAL(38, 9) AS preserved;

SELECT py_number_decimal_scalar(NULL::DECIMAL(38, 9)) IS NOT DISTINCT FROM NULL::DECIMAL(38, 9) AS preserved;

DROP FUNCTION py_number_decimal_scalar(DECIMAL(38, 9));

CREATE FUNCTION py_number_decimal_vector(a DECIMAL(38, 9)) RETURNS DECIMAL(38, 9) LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_number_decimal_vector('-99999999999999999999999999999.123456789'::DECIMAL(38, 9)) IS NOT DISTINCT FROM '-99999999999999999999999999999.123456789'::DECIMAL(38, 9) AS preserved;

SELECT py_number_decimal_vector('0.000000001'::DECIMAL(38, 9)) IS NOT DISTINCT FROM '0.000000001'::DECIMAL(38, 9) AS preserved;

SELECT py_number_decimal_vector('99999999999999999999999999999.999999999'::DECIMAL(38, 9)) IS NOT DISTINCT FROM '99999999999999999999999999999.999999999'::DECIMAL(38, 9) AS preserved;

SELECT py_number_decimal_vector(NULL::DECIMAL(38, 9)) IS NOT DISTINCT FROM NULL::DECIMAL(38, 9) AS preserved;

DROP FUNCTION py_number_decimal_vector(DECIMAL(38, 9));

CREATE FUNCTION py_number_float_scalar(a FLOAT) RETURNS FLOAT LANGUAGE python
AS $$
return a
$$;

SELECT py_number_float_scalar('-1.5'::FLOAT) IS NOT DISTINCT FROM '-1.5'::FLOAT AS preserved;

SELECT py_number_float_scalar(0::FLOAT) IS NOT DISTINCT FROM 0::FLOAT AS preserved;

SELECT py_number_float_scalar(3.25::FLOAT) IS NOT DISTINCT FROM 3.25::FLOAT AS preserved;

SELECT py_number_float_scalar(NULL::FLOAT) IS NOT DISTINCT FROM NULL::FLOAT AS preserved;

DROP FUNCTION py_number_float_scalar(FLOAT);

CREATE FUNCTION py_number_float_vector(a FLOAT) RETURNS FLOAT LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_number_float_vector('-1.5'::FLOAT) IS NOT DISTINCT FROM '-1.5'::FLOAT AS preserved;

SELECT py_number_float_vector(0::FLOAT) IS NOT DISTINCT FROM 0::FLOAT AS preserved;

SELECT py_number_float_vector(3.25::FLOAT) IS NOT DISTINCT FROM 3.25::FLOAT AS preserved;

SELECT py_number_float_vector(NULL::FLOAT) IS NOT DISTINCT FROM NULL::FLOAT AS preserved;

DROP FUNCTION py_number_float_vector(FLOAT);

CREATE FUNCTION py_number_double_scalar(a DOUBLE) RETURNS DOUBLE LANGUAGE python
AS $$
return a
$$;

SELECT py_number_double_scalar('-1.25e100'::DOUBLE) IS NOT DISTINCT FROM '-1.25e100'::DOUBLE AS preserved;

SELECT py_number_double_scalar(1.25e-100::DOUBLE) IS NOT DISTINCT FROM 1.25e-100::DOUBLE AS preserved;

SELECT py_number_double_scalar(0::DOUBLE) IS NOT DISTINCT FROM 0::DOUBLE AS preserved;

SELECT py_number_double_scalar(NULL::DOUBLE) IS NOT DISTINCT FROM NULL::DOUBLE AS preserved;

DROP FUNCTION py_number_double_scalar(DOUBLE);

CREATE FUNCTION py_number_double_vector(a DOUBLE) RETURNS DOUBLE LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_number_double_vector('-1.25e100'::DOUBLE) IS NOT DISTINCT FROM '-1.25e100'::DOUBLE AS preserved;

SELECT py_number_double_vector(1.25e-100::DOUBLE) IS NOT DISTINCT FROM 1.25e-100::DOUBLE AS preserved;

SELECT py_number_double_vector(0::DOUBLE) IS NOT DISTINCT FROM 0::DOUBLE AS preserved;

SELECT py_number_double_vector(NULL::DOUBLE) IS NOT DISTINCT FROM NULL::DOUBLE AS preserved;

DROP FUNCTION py_number_double_vector(DOUBLE);

CREATE FUNCTION py_number_boolean_scalar(a BOOLEAN) RETURNS BOOLEAN LANGUAGE python
AS $$
return a
$$;

SELECT py_number_boolean_scalar(false::BOOLEAN) IS NOT DISTINCT FROM false::BOOLEAN AS preserved;

SELECT py_number_boolean_scalar(true::BOOLEAN) IS NOT DISTINCT FROM true::BOOLEAN AS preserved;

SELECT py_number_boolean_scalar(NULL::BOOLEAN) IS NOT DISTINCT FROM NULL::BOOLEAN AS preserved;

DROP FUNCTION py_number_boolean_scalar(BOOLEAN);

CREATE FUNCTION py_number_boolean_vector(a BOOLEAN) RETURNS BOOLEAN LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_number_boolean_vector(false::BOOLEAN) IS NOT DISTINCT FROM false::BOOLEAN AS preserved;

SELECT py_number_boolean_vector(true::BOOLEAN) IS NOT DISTINCT FROM true::BOOLEAN AS preserved;

SELECT py_number_boolean_vector(NULL::BOOLEAN) IS NOT DISTINCT FROM NULL::BOOLEAN AS preserved;

DROP FUNCTION py_number_boolean_vector(BOOLEAN);

-- @teardown
DROP FUNCTION IF EXISTS py_number_boolean_vector(BOOLEAN);

-- @teardown
DROP FUNCTION IF EXISTS py_number_boolean_scalar(BOOLEAN);

-- @teardown
DROP FUNCTION IF EXISTS py_number_double_vector(DOUBLE);

-- @teardown
DROP FUNCTION IF EXISTS py_number_double_scalar(DOUBLE);

-- @teardown
DROP FUNCTION IF EXISTS py_number_float_vector(FLOAT);

-- @teardown
DROP FUNCTION IF EXISTS py_number_float_scalar(FLOAT);

-- @teardown
DROP FUNCTION IF EXISTS py_number_decimal_vector(DECIMAL(38, 9));

-- @teardown
DROP FUNCTION IF EXISTS py_number_decimal_scalar(DECIMAL(38, 9));
