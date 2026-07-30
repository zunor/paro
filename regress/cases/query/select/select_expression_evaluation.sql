-- Copyright 2024-2026 Zunor
-- SPDX-License-Identifier: Apache-2.0

-- Structural equality does not override three-valued comparison semantics.
SELECT
  v,
  v = v AS self_equal,
  v <> v AS self_not_equal,
  v <= v AS self_less_equal,
  v IS DISTINCT FROM v AS self_distinct,
  v IS NOT DISTINCT FROM v AS self_not_distinct
FROM (VALUES (NULL::INTEGER), (1)) AS values_with_null(v)
ORDER BY v NULLS FIRST;

-- Multiplication by zero must preserve input nullability.
SELECT v, v * 0 AS product
FROM (VALUES (NULL::INTEGER), (2)) AS nullable_integer(v)
ORDER BY v NULLS FIRST;

-- Floating-point arithmetic is not ordinary algebra: multiplying NaN or infinity by zero yields
-- NaN, and a negative finite value preserves a signed zero.
SELECT label, v, v * 0.0::DOUBLE AS product
FROM (
  VALUES
    (1, NULL::DOUBLE),
    (2, 'nan'::DOUBLE),
    (3, 'inf'::DOUBLE),
    (4, '-inf'::DOUBLE),
    (5, '-1'::DOUBLE),
    (6, 1.0::DOUBLE)
) AS floating_values(label, v)
ORDER BY label;

-- Invalid arithmetic domains produce NULL consistently for dynamic inputs instead of depending
-- on whether constant folding happened. In particular, integer division must not panic a query
-- worker and floating-point zero divisors must agree with their constant form.
SELECT label, numerator / divisor AS quotient, numerator % divisor AS remainder
FROM (
  VALUES
    (1, 10::INTEGER, 2::INTEGER),
    (2, 10::INTEGER, 0::INTEGER),
    (3, NULL::INTEGER, 0::INTEGER),
    (4, '-2147483648'::INTEGER, '-1'::INTEGER)
) AS integer_division(label, numerator, divisor)
ORDER BY label;

SELECT label, numerator / divisor AS quotient, numerator % divisor AS remainder
FROM (
  VALUES
    (1, 1.0::DOUBLE, 2.0::DOUBLE),
    (2, 1.0::DOUBLE, 0.0::DOUBLE),
    (3, 0.0::DOUBLE, 0.0::DOUBLE),
    (4, NULL::DOUBLE, 0.0::DOUBLE)
) AS floating_division(label, numerator, divisor)
ORDER BY label;

-- Identical volatile call trees still represent independent evaluations.
EXPLAIN
SELECT random() = random() AS independent_calls;

-- A predicate on a volatile projected value must remain above the projection so the value is
-- evaluated once per row rather than copied into both the filter and output expressions.
EXPLAIN
SELECT r
FROM (
  SELECT random() AS r
  FROM generate_series(1, 100)
) AS volatile_projection
WHERE r < 0.5;

-- An unused volatile projection is still an observable evaluation and must not be pruned.
EXPLAIN
SELECT count(*)
FROM (
  SELECT random() AS r
  FROM generate_series(1, 100)
) AS volatile_projection;

-- LIMIT selects its rows before an outer predicate is evaluated. Pushing the predicate below the
-- cardinality boundary would incorrectly admit replacement rows.
SELECT v
FROM (
  SELECT v
  FROM generate_series(1, 10) AS generated(v)
  LIMIT 5
) AS limited
WHERE v > 5
ORDER BY v;
