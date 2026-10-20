-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Every integer width crosses SQL, Arrow and Python at both limits, in both modes.

CREATE FUNCTION py_bound_i8_scalar(a TINYINT) RETURNS TINYINT LANGUAGE python
AS $$
return a
$$;

SELECT py_bound_i8_scalar('-128'::TINYINT) = '-128'::TINYINT AS minimum,
       py_bound_i8_scalar('127'::TINYINT) = '127'::TINYINT AS maximum,
       py_bound_i8_scalar(NULL::TINYINT) IS NULL AS null_value;

DROP FUNCTION py_bound_i8_scalar(TINYINT);

CREATE FUNCTION py_bound_i8_vector(a TINYINT) RETURNS TINYINT LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_bound_i8_vector('-128'::TINYINT) = '-128'::TINYINT AS minimum,
       py_bound_i8_vector('127'::TINYINT) = '127'::TINYINT AS maximum,
       py_bound_i8_vector(NULL::TINYINT) IS NULL AS null_value;

DROP FUNCTION py_bound_i8_vector(TINYINT);

CREATE FUNCTION py_bound_i16_scalar(a SMALLINT) RETURNS SMALLINT LANGUAGE python
AS $$
return a
$$;

SELECT py_bound_i16_scalar('-32768'::SMALLINT) = '-32768'::SMALLINT AS minimum,
       py_bound_i16_scalar('32767'::SMALLINT) = '32767'::SMALLINT AS maximum,
       py_bound_i16_scalar(NULL::SMALLINT) IS NULL AS null_value;

DROP FUNCTION py_bound_i16_scalar(SMALLINT);

CREATE FUNCTION py_bound_i16_vector(a SMALLINT) RETURNS SMALLINT LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_bound_i16_vector('-32768'::SMALLINT) = '-32768'::SMALLINT AS minimum,
       py_bound_i16_vector('32767'::SMALLINT) = '32767'::SMALLINT AS maximum,
       py_bound_i16_vector(NULL::SMALLINT) IS NULL AS null_value;

DROP FUNCTION py_bound_i16_vector(SMALLINT);

CREATE FUNCTION py_bound_i32_scalar(a INTEGER) RETURNS INTEGER LANGUAGE python
AS $$
return a
$$;

SELECT py_bound_i32_scalar('-2147483648'::INTEGER) = '-2147483648'::INTEGER AS minimum,
       py_bound_i32_scalar('2147483647'::INTEGER) = '2147483647'::INTEGER AS maximum,
       py_bound_i32_scalar(NULL::INTEGER) IS NULL AS null_value;

DROP FUNCTION py_bound_i32_scalar(INTEGER);

CREATE FUNCTION py_bound_i32_vector(a INTEGER) RETURNS INTEGER LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_bound_i32_vector('-2147483648'::INTEGER) = '-2147483648'::INTEGER AS minimum,
       py_bound_i32_vector('2147483647'::INTEGER) = '2147483647'::INTEGER AS maximum,
       py_bound_i32_vector(NULL::INTEGER) IS NULL AS null_value;

DROP FUNCTION py_bound_i32_vector(INTEGER);

CREATE FUNCTION py_bound_i64_scalar(a BIGINT) RETURNS BIGINT LANGUAGE python
AS $$
return a
$$;

SELECT py_bound_i64_scalar('-9223372036854775808'::BIGINT) = '-9223372036854775808'::BIGINT AS minimum,
       py_bound_i64_scalar('9223372036854775807'::BIGINT) = '9223372036854775807'::BIGINT AS maximum,
       py_bound_i64_scalar(NULL::BIGINT) IS NULL AS null_value;

DROP FUNCTION py_bound_i64_scalar(BIGINT);

CREATE FUNCTION py_bound_i64_vector(a BIGINT) RETURNS BIGINT LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_bound_i64_vector('-9223372036854775808'::BIGINT) = '-9223372036854775808'::BIGINT AS minimum,
       py_bound_i64_vector('9223372036854775807'::BIGINT) = '9223372036854775807'::BIGINT AS maximum,
       py_bound_i64_vector(NULL::BIGINT) IS NULL AS null_value;

DROP FUNCTION py_bound_i64_vector(BIGINT);

CREATE FUNCTION py_bound_i128_scalar(a HUGEINT) RETURNS HUGEINT LANGUAGE python
AS $$
return a
$$;

SELECT py_bound_i128_scalar('-170141183460469231731687303715884105728'::HUGEINT) = '-170141183460469231731687303715884105728'::HUGEINT AS minimum,
       py_bound_i128_scalar('170141183460469231731687303715884105727'::HUGEINT) = '170141183460469231731687303715884105727'::HUGEINT AS maximum,
       py_bound_i128_scalar(NULL::HUGEINT) IS NULL AS null_value;

DROP FUNCTION py_bound_i128_scalar(HUGEINT);

CREATE FUNCTION py_bound_i128_vector(a HUGEINT) RETURNS HUGEINT LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_bound_i128_vector('-170141183460469231731687303715884105728'::HUGEINT) = '-170141183460469231731687303715884105728'::HUGEINT AS minimum,
       py_bound_i128_vector('170141183460469231731687303715884105727'::HUGEINT) = '170141183460469231731687303715884105727'::HUGEINT AS maximum,
       py_bound_i128_vector(NULL::HUGEINT) IS NULL AS null_value;

DROP FUNCTION py_bound_i128_vector(HUGEINT);

CREATE FUNCTION py_bound_u8_scalar(a TINYINT UNSIGNED) RETURNS TINYINT UNSIGNED LANGUAGE python
AS $$
return a
$$;

SELECT py_bound_u8_scalar('0'::TINYINT UNSIGNED) = '0'::TINYINT UNSIGNED AS minimum,
       py_bound_u8_scalar('255'::TINYINT UNSIGNED) = '255'::TINYINT UNSIGNED AS maximum,
       py_bound_u8_scalar(NULL::TINYINT UNSIGNED) IS NULL AS null_value;

DROP FUNCTION py_bound_u8_scalar(TINYINT UNSIGNED);

CREATE FUNCTION py_bound_u8_vector(a TINYINT UNSIGNED) RETURNS TINYINT UNSIGNED LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_bound_u8_vector('0'::TINYINT UNSIGNED) = '0'::TINYINT UNSIGNED AS minimum,
       py_bound_u8_vector('255'::TINYINT UNSIGNED) = '255'::TINYINT UNSIGNED AS maximum,
       py_bound_u8_vector(NULL::TINYINT UNSIGNED) IS NULL AS null_value;

DROP FUNCTION py_bound_u8_vector(TINYINT UNSIGNED);

CREATE FUNCTION py_bound_u16_scalar(a SMALLINT UNSIGNED) RETURNS SMALLINT UNSIGNED LANGUAGE python
AS $$
return a
$$;

SELECT py_bound_u16_scalar('0'::SMALLINT UNSIGNED) = '0'::SMALLINT UNSIGNED AS minimum,
       py_bound_u16_scalar('65535'::SMALLINT UNSIGNED) = '65535'::SMALLINT UNSIGNED AS maximum,
       py_bound_u16_scalar(NULL::SMALLINT UNSIGNED) IS NULL AS null_value;

DROP FUNCTION py_bound_u16_scalar(SMALLINT UNSIGNED);

CREATE FUNCTION py_bound_u16_vector(a SMALLINT UNSIGNED) RETURNS SMALLINT UNSIGNED LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_bound_u16_vector('0'::SMALLINT UNSIGNED) = '0'::SMALLINT UNSIGNED AS minimum,
       py_bound_u16_vector('65535'::SMALLINT UNSIGNED) = '65535'::SMALLINT UNSIGNED AS maximum,
       py_bound_u16_vector(NULL::SMALLINT UNSIGNED) IS NULL AS null_value;

DROP FUNCTION py_bound_u16_vector(SMALLINT UNSIGNED);

CREATE FUNCTION py_bound_u32_scalar(a INTEGER UNSIGNED) RETURNS INTEGER UNSIGNED LANGUAGE python
AS $$
return a
$$;

SELECT py_bound_u32_scalar('0'::INTEGER UNSIGNED) = '0'::INTEGER UNSIGNED AS minimum,
       py_bound_u32_scalar('4294967295'::INTEGER UNSIGNED) = '4294967295'::INTEGER UNSIGNED AS maximum,
       py_bound_u32_scalar(NULL::INTEGER UNSIGNED) IS NULL AS null_value;

DROP FUNCTION py_bound_u32_scalar(INTEGER UNSIGNED);

CREATE FUNCTION py_bound_u32_vector(a INTEGER UNSIGNED) RETURNS INTEGER UNSIGNED LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_bound_u32_vector('0'::INTEGER UNSIGNED) = '0'::INTEGER UNSIGNED AS minimum,
       py_bound_u32_vector('4294967295'::INTEGER UNSIGNED) = '4294967295'::INTEGER UNSIGNED AS maximum,
       py_bound_u32_vector(NULL::INTEGER UNSIGNED) IS NULL AS null_value;

DROP FUNCTION py_bound_u32_vector(INTEGER UNSIGNED);

CREATE FUNCTION py_bound_u64_scalar(a BIGINT UNSIGNED) RETURNS BIGINT UNSIGNED LANGUAGE python
AS $$
return a
$$;

SELECT py_bound_u64_scalar('0'::BIGINT UNSIGNED) = '0'::BIGINT UNSIGNED AS minimum,
       py_bound_u64_scalar('18446744073709551615'::BIGINT UNSIGNED) = '18446744073709551615'::BIGINT UNSIGNED AS maximum,
       py_bound_u64_scalar(NULL::BIGINT UNSIGNED) IS NULL AS null_value;

DROP FUNCTION py_bound_u64_scalar(BIGINT UNSIGNED);

CREATE FUNCTION py_bound_u64_vector(a BIGINT UNSIGNED) RETURNS BIGINT UNSIGNED LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_bound_u64_vector('0'::BIGINT UNSIGNED) = '0'::BIGINT UNSIGNED AS minimum,
       py_bound_u64_vector('18446744073709551615'::BIGINT UNSIGNED) = '18446744073709551615'::BIGINT UNSIGNED AS maximum,
       py_bound_u64_vector(NULL::BIGINT UNSIGNED) IS NULL AS null_value;

DROP FUNCTION py_bound_u64_vector(BIGINT UNSIGNED);

CREATE FUNCTION py_bound_u128_scalar(a UHUGEINT) RETURNS UHUGEINT LANGUAGE python
AS $$
return a
$$;

SELECT py_bound_u128_scalar('0'::UHUGEINT) = '0'::UHUGEINT AS minimum,
       py_bound_u128_scalar('340282366920938463463374607431768211455'::UHUGEINT) = '340282366920938463463374607431768211455'::UHUGEINT AS maximum,
       py_bound_u128_scalar(NULL::UHUGEINT) IS NULL AS null_value;

DROP FUNCTION py_bound_u128_scalar(UHUGEINT);

CREATE FUNCTION py_bound_u128_vector(a UHUGEINT) RETURNS UHUGEINT LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_bound_u128_vector('0'::UHUGEINT) = '0'::UHUGEINT AS minimum,
       py_bound_u128_vector('340282366920938463463374607431768211455'::UHUGEINT) = '340282366920938463463374607431768211455'::UHUGEINT AS maximum,
       py_bound_u128_vector(NULL::UHUGEINT) IS NULL AS null_value;

DROP FUNCTION py_bound_u128_vector(UHUGEINT);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_u128_vector(UHUGEINT);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_u128_scalar(UHUGEINT);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_u64_vector(BIGINT UNSIGNED);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_u64_scalar(BIGINT UNSIGNED);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_u32_vector(INTEGER UNSIGNED);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_u32_scalar(INTEGER UNSIGNED);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_u16_vector(SMALLINT UNSIGNED);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_u16_scalar(SMALLINT UNSIGNED);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_u8_vector(TINYINT UNSIGNED);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_u8_scalar(TINYINT UNSIGNED);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_i128_vector(HUGEINT);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_i128_scalar(HUGEINT);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_i64_vector(BIGINT);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_i64_scalar(BIGINT);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_i32_vector(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_i32_scalar(INTEGER);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_i16_vector(SMALLINT);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_i16_scalar(SMALLINT);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_i8_vector(TINYINT);

-- @teardown
DROP FUNCTION IF EXISTS py_bound_i8_scalar(TINYINT);
