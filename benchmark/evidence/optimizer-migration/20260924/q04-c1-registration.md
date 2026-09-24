# Q04 first-statement execution

EvidenceId: q04-c1-v1. Initial control: clean `8dfc2b1a` on re-op.
This registration precedes new collection. Goal: reduce complete first-statement
latency by removing demonstrated physical work, not by shortening timers,
changing results, weakening quality or reducing search budgets.

First inspect Q04's selected plan and operator profiles in at most four isolated
diagnostic processes; CPU sampling may accompany repeated executions in those
processes. These are exploratory, never normal C1. Use the existing immutable
SF1 seed and maintained server lifecycle. Do not construct a new trace exporter.
Choose the implementation from this evidence, then record its hypothesis and
source commit before the confirmatory comparison.

Reuse the exact seed, SQL, DuckDB 1.5.5 native/builtin extension declarations
from selected-local-properties-registration.md. Confirmed native SHA-256:
85fad85339c7e345eabb66a33a25d8503c3ee8881f9d298fa8499a97617dbf68.
Seed: 256bf479ca982e30c96f9109fb9920d04233c9643b7bcc6a5fd3f5a50554c927.
One existing checkout and target, sequential builds only; no new worktree.

Normal sampling uses the maintained TPC-DS collector: four threads, 2GB, binary
results, quality policy, verifier off, unchanged optional deadline/budgets,
PARO_COMPILE_WORK_EVIDENCE=1, no Detail or statement trace. Register six fresh
processes per arm, split C/P/C/P into three-process batches. Seeds 2026092403/04;
one warmup and one ABBA round per process, 10,000 bootstrap draws. Detail is one
separate process per batch. Generator-declared metadata permits within-Paro
engineering comparison, not symmetric cross-engine parity certification. If a
candidate is within 10% of DuckDB C1, separately run the metadata-none track
with six fresh processes before making a parity claim.

Keep all valid slow samples, errors, result/receipt/identity checks. C1, compiler
and warm are separate observations, not medians to subtract for attribution.
Independent sample unit is a process, not each warm invocation. Report paired
ratios and uncertainty. This six-block pilot is not a powered strict-parity
certification; "comparable" requires at least a <=1.10 paired point estimate
and no evidence of a >10% tail/warm regression, with uncertainty disclosed.
Investigate any >10% compiler/warm regression. Stop affected collection on
result errors, missing receipts or uncontrolled context/resource changes.

Bound retained campaign evidence to 20MiB. No raw event floods, binaries or
server logs in the archive. Preserve necessary counterexamples and negative
results. Validate affected Rust tests, workspace check/tests, strict Clippy,
benchmark and compare-only SQL regress; never bless failures.

## Registered intervention: exact linear DECIMAL lowering

The exploratory plan has three narrow-key partial/final aggregates feeding a
six-reader CTE; this is not a missing-date-filter/aggregation-placement case.
A 10-second warm CPU sample puts DECIMAL arithmetic, its per-value reader and
native-plan dispatch among the leading leaves (1,134 / 1,106 / 643 samples).
These are CPU samples, not exclusive C1 milliseconds. The existing direct
kernels decline nullable input batches, and the three total equal-scale
add/subtract nodes materialize separate vectors. Hypothesis: one certified
NULL-strict integer kernel removes the intermediate traversals/allocations
without changing optimizer decisions, floating SUM order, budgets or policy.
Fusion requires an independent precision/scale envelope for every removed node,
retains CSE/leaf evaluation order and declines unsupported/error-capable types.
No query-name, column-name, cardinality or SQL-text specialization is permitted.

The failed exploratory `EXPLAIN (ANALYZE, FORMAT JSON)` request is retained as
unsupported syntax; it yielded no execution measurements. Existing plain
EXPLAIN ANALYZE is a human-readable exploratory view, not a new timing source.
