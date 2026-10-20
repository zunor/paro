-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- @control connect user=paro

-- Leap days, pre-epoch values, microseconds and signed intervals survive the worker boundary.

CREATE FUNCTION py_clock_date_scalar(a DATE) RETURNS DATE LANGUAGE python
AS $$
return a
$$;

SELECT py_clock_date_scalar('1969-12-31'::DATE) IS NOT DISTINCT FROM '1969-12-31'::DATE AS preserved;

SELECT py_clock_date_scalar('2000-02-29'::DATE) IS NOT DISTINCT FROM '2000-02-29'::DATE AS preserved;

SELECT py_clock_date_scalar(NULL::DATE) IS NOT DISTINCT FROM NULL::DATE AS preserved;

DROP FUNCTION py_clock_date_scalar(DATE);

CREATE FUNCTION py_clock_date_vector(a DATE) RETURNS DATE LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_clock_date_vector('1969-12-31'::DATE) IS NOT DISTINCT FROM '1969-12-31'::DATE AS preserved;

SELECT py_clock_date_vector('2000-02-29'::DATE) IS NOT DISTINCT FROM '2000-02-29'::DATE AS preserved;

SELECT py_clock_date_vector(NULL::DATE) IS NOT DISTINCT FROM NULL::DATE AS preserved;

DROP FUNCTION py_clock_date_vector(DATE);

CREATE FUNCTION py_clock_time_scalar(a TIME) RETURNS TIME LANGUAGE python
AS $$
return a
$$;

SELECT py_clock_time_scalar('00:00:00'::TIME) IS NOT DISTINCT FROM '00:00:00'::TIME AS preserved;

SELECT py_clock_time_scalar('23:59:59.999999'::TIME) IS NOT DISTINCT FROM '23:59:59.999999'::TIME AS preserved;

SELECT py_clock_time_scalar(NULL::TIME) IS NOT DISTINCT FROM NULL::TIME AS preserved;

DROP FUNCTION py_clock_time_scalar(TIME);

CREATE FUNCTION py_clock_time_vector(a TIME) RETURNS TIME LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_clock_time_vector('00:00:00'::TIME) IS NOT DISTINCT FROM '00:00:00'::TIME AS preserved;

SELECT py_clock_time_vector('23:59:59.999999'::TIME) IS NOT DISTINCT FROM '23:59:59.999999'::TIME AS preserved;

SELECT py_clock_time_vector(NULL::TIME) IS NOT DISTINCT FROM NULL::TIME AS preserved;

DROP FUNCTION py_clock_time_vector(TIME);

CREATE FUNCTION py_clock_timestamp_scalar(a TIMESTAMP) RETURNS TIMESTAMP LANGUAGE python
AS $$
return a
$$;

SELECT py_clock_timestamp_scalar('1969-12-31 23:59:59.999999'::TIMESTAMP) IS NOT DISTINCT FROM '1969-12-31 23:59:59.999999'::TIMESTAMP AS preserved;

SELECT py_clock_timestamp_scalar('2024-02-29 12:34:56.123456'::TIMESTAMP) IS NOT DISTINCT FROM '2024-02-29 12:34:56.123456'::TIMESTAMP AS preserved;

SELECT py_clock_timestamp_scalar(NULL::TIMESTAMP) IS NOT DISTINCT FROM NULL::TIMESTAMP AS preserved;

DROP FUNCTION py_clock_timestamp_scalar(TIMESTAMP);

CREATE FUNCTION py_clock_timestamp_vector(a TIMESTAMP) RETURNS TIMESTAMP LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_clock_timestamp_vector('1969-12-31 23:59:59.999999'::TIMESTAMP) IS NOT DISTINCT FROM '1969-12-31 23:59:59.999999'::TIMESTAMP AS preserved;

SELECT py_clock_timestamp_vector('2024-02-29 12:34:56.123456'::TIMESTAMP) IS NOT DISTINCT FROM '2024-02-29 12:34:56.123456'::TIMESTAMP AS preserved;

SELECT py_clock_timestamp_vector(NULL::TIMESTAMP) IS NOT DISTINCT FROM NULL::TIMESTAMP AS preserved;

DROP FUNCTION py_clock_timestamp_vector(TIMESTAMP);

CREATE FUNCTION py_clock_tz_scalar(a TIMESTAMPTZ) RETURNS TIMESTAMPTZ LANGUAGE python
AS $$
return a
$$;

SELECT py_clock_tz_scalar('2024-02-29 12:34:56.123456+08:00'::TIMESTAMPTZ) IS NOT DISTINCT FROM '2024-02-29 12:34:56.123456+08:00'::TIMESTAMPTZ AS preserved;

SELECT py_clock_tz_scalar(NULL::TIMESTAMPTZ) IS NOT DISTINCT FROM NULL::TIMESTAMPTZ AS preserved;

DROP FUNCTION py_clock_tz_scalar(TIMESTAMPTZ);

CREATE FUNCTION py_clock_tz_vector(a TIMESTAMPTZ) RETURNS TIMESTAMPTZ LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_clock_tz_vector('2024-02-29 12:34:56.123456+08:00'::TIMESTAMPTZ) IS NOT DISTINCT FROM '2024-02-29 12:34:56.123456+08:00'::TIMESTAMPTZ AS preserved;

SELECT py_clock_tz_vector(NULL::TIMESTAMPTZ) IS NOT DISTINCT FROM NULL::TIMESTAMPTZ AS preserved;

DROP FUNCTION py_clock_tz_vector(TIMESTAMPTZ);

CREATE FUNCTION py_clock_interval_scalar(a INTERVAL) RETURNS INTERVAL LANGUAGE python
AS $$
return a
$$;

SELECT py_clock_interval_scalar('2 months 3 days 4 microseconds'::INTERVAL) IS NOT DISTINCT FROM '2 months 3 days 4 microseconds'::INTERVAL AS preserved;

SELECT py_clock_interval_scalar('-1 day'::INTERVAL) IS NOT DISTINCT FROM '-1 day'::INTERVAL AS preserved;

SELECT py_clock_interval_scalar(NULL::INTERVAL) IS NOT DISTINCT FROM NULL::INTERVAL AS preserved;

DROP FUNCTION py_clock_interval_scalar(INTERVAL);

CREATE FUNCTION py_clock_interval_vector(a INTERVAL) RETURNS INTERVAL LANGUAGE python MODE VECTOR
AS $$
return a
$$;

SELECT py_clock_interval_vector('2 months 3 days 4 microseconds'::INTERVAL) IS NOT DISTINCT FROM '2 months 3 days 4 microseconds'::INTERVAL AS preserved;

SELECT py_clock_interval_vector('-1 day'::INTERVAL) IS NOT DISTINCT FROM '-1 day'::INTERVAL AS preserved;

SELECT py_clock_interval_vector(NULL::INTERVAL) IS NOT DISTINCT FROM NULL::INTERVAL AS preserved;

DROP FUNCTION py_clock_interval_vector(INTERVAL);

-- @teardown
DROP FUNCTION IF EXISTS py_clock_interval_vector(INTERVAL);

-- @teardown
DROP FUNCTION IF EXISTS py_clock_interval_scalar(INTERVAL);

-- @teardown
DROP FUNCTION IF EXISTS py_clock_tz_vector(TIMESTAMPTZ);

-- @teardown
DROP FUNCTION IF EXISTS py_clock_tz_scalar(TIMESTAMPTZ);

-- @teardown
DROP FUNCTION IF EXISTS py_clock_timestamp_vector(TIMESTAMP);

-- @teardown
DROP FUNCTION IF EXISTS py_clock_timestamp_scalar(TIMESTAMP);

-- @teardown
DROP FUNCTION IF EXISTS py_clock_time_vector(TIME);

-- @teardown
DROP FUNCTION IF EXISTS py_clock_time_scalar(TIME);

-- @teardown
DROP FUNCTION IF EXISTS py_clock_date_vector(DATE);

-- @teardown
DROP FUNCTION IF EXISTS py_clock_date_scalar(DATE);
