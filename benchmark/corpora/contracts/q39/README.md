# Q39 bounded numerical/relational contract

These are maintained correctness inputs, relocated without semantic changes
from `769ad6104:benchmark/evidence/optimizer-convergence/20260920/c0/`.
They are not archived performance observations.

1. Use the declared TPC-DS SQL/data generator and identical input on both engines.
2. With `tools/capture_result_difference.py` (discover current `--help`), capture
   complete Q39 typed results and the exact rows selected by
   `q39-integer-input-bags.sql`. Preserve the raw strict mismatch separately.
3. Use `tools/verify_integer_aggregate_relation.py --help`; supply the captured
   `--inputs`, `--result`, this `q39-numeric-relation.json` as `--spec`, and an
   explicit owned `--output` under ignored `benchmark/runs/`.

Paths to tools are relative to `benchmark/`. The verifier requires matching
input bags before independently calculating high-precision moments, enumerated
Welford/Chan bounds, relational selection/join multiplicities and an exact order
prefix. Unsupported domains fail closed. It does not fit a ULP threshold to
observed Paro results or certify general floating-point equivalence.

Use `benchmark/tests/test_numeric_result_contract.py` for the small independent
oracle tests. Historical raw SF1 counterexamples/certificates are recoverable
from `769ad6104:benchmark/evidence/optimizer/20260925/planner-cleanup-v1/q39/`.
