# Typed ORDER contract v4

Original exact output values retain v3's coefficient/exponent rational identity.
Only an already-bound arithmetic ORDER subtree with a DOUBLE operand receives
an explicit exact-to-DOUBLE conversion. Its result is binary64, not an exact
number. Secondary output keys remain exact, including distinct Decimal values
that become binary64 peers. Bag membership and LIMIT-selected multiplicities
are checked before and independently of ordering. No rows are re-sorted to pass.

## Verified engine chains

Paro `function/src/scalar/cast/decimal_casts.rs::decimal_to_float_cast` converts
the signed integer coefficient to f64, then divides by `10f64.powi(scale)`.
The arithmetic binder selects DOUBLE/DOUBLE -> DOUBLE for mixed decimal/double.
DuckDB **v1.5.5** `TryCastDecimalToFloatingPoint` uses coefficient/scale for
small exactly representable coefficients (or scale zero); otherwise it divides
the integer into quotient and remainder and combines their double conversions.
Its HUGEINT cast uses upper/lower 64-bit limbs, including the negative-small
special case. The comparator implements these **separately**, not Python's
Decimal-to-float conversion. Conversion of integer coefficients and subsequent
operations follows binary64 round-to-nearest, ties-to-even.

The supported decimal domain is precision <=38 and scale 0..22. Powers of ten
in this range are exactly representable in binary64, so Paro's powi and DuckDB's
table have identical divisors. Larger scales remain Uncovered; no assumption
about platform pow rounding is imported. Unknown NUMERIC precision/scale and
unproven mixed CASE/coercion paths also remain Uncovered. Overflow/non-finite
ORDER results fail certification rather than receiving a tolerance.

Sixty input/type pairs were checked against **both actual engines** through SQL,
including 2^53 neighbors, positive/negative 38-digit neighbors, zero and six
scales. The result report is `c2-order-cast-engine-v2.json` in the private archive;
it contains server-observed diagnostic settings and both engines' hex results.
An initial fixture used scientific decimal text unsupported by Paro's decimal
text parser; the corrected fixture uses exact fixed-point text, without changing
the tested coefficient or value. This is diagnostic correctness, not timing.

Tests additionally reject wrong secondary ordering after rounding creates peers,
wrong NULL placement, scale/coefficient overflow, non-finite multiplication,
and replacement of row 100 by row 101. Close subtraction is evaluated after the
bound casts, never by a blanket epsilon. The pinned DuckDB SQL oracle checks all
boundary casts. Existing Q39 independent certification is unchanged.

Offline rechecks must include `order_numeric_contract.py` in the comparator
manifest and compare each engine using its own typed ORDER program. Different
engine keys are not silently reconciled. Normal harness samples use the same
typed programs. The previous mixed-arithmetic Uncovered reports remain archived.
