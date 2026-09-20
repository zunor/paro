# Exact values (typed-result-v3)

This replaces the lossy Decimal.normalize comparison path. A finite Decimal
is decoded as its signed integer coefficient times a power of ten, using
arbitrary-precision integer arithmetic only. Reduced numerator/denominator
is the shared exact comparison and evidence representation. Each wire value
first has to satisfy its own declared SQL type, width and precision/scale.
Booleans are not integers; floats are not converted into exact SQL numbers.

Representation equality does not authorize a cross-engine SQL type mapping.
That permission belongs to the bound output contract. Hashes identify
evidence only and cannot substitute for bag or ordering equality.

Counterexamples: adjacent 38-digit integers remain distinct at Decimal
precisions 2/28/60; equivalent scales/exponents and signed zeros agree;
noninteger fractions retain every digit; signed/unsigned boundaries,
precision overflow, fractional truncation, nonfinite Decimal and bool/int
coercion are rejected. Authorized exact mappings induce identical key
ordering and evidence; reversed order is rejected.

Validation: `PYTHONPATH=benchmark <pinned-venv>/bin/python -m pytest
benchmark/tests/test_exact_result_value.py benchmark/tests/test_tpcds_contract.py -q`:
21 passed. Historical v2 captures and failure reports are unchanged. This
commit alone does not certify any formerly Uncovered schema mapping.
