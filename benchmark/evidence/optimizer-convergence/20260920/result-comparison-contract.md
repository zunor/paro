# C2 result comparison contract (typed-result-v2)

This registration precedes numerical acceptance changes. Old raw schemas,
exact bags and differences are retained. A protocol change is not an engine
speedup or a retroactive pass under the old protocol. Both arms must be
evaluated by this same version.

## Output identity

Keep wire labels verbatim. An explicit SQL alias (including case and dots),
column order, arity and logical type are strict. For an unaliased expression,
both engine display labels must parse to the source projection's identical
AST using the pinned DuckDB 1.5.5 syntax reader. Only source locations are
removed. Parentheses are interpreted by the parser, not deleted. Literals,
operators, argument order, column identities and casts are retained. A bare
projected column may display its declared source qualifier. Unsupported
projection expansion has no implicit ordinal mapping. Same-engine schema
repeats remain wire-exact. This is an oracle display contract, not a change
to Paro's production protocol or printer.

## Numerical acceptance: bounded scope

Integers, Decimal, NULL, keys, multiplicities, row membership and order remain
strict. The default floating comparator remains exact. No universal 2-ULP,
relative or absolute epsilon is admitted from the observed Q39 differences.
ULP is a diagnostic distance on finite binary64 values (signed zeros compare
equal); non-finite inputs require a separate explicit contract.

A numerical contract may be explicitly registered for a floating aggregate
output only with an independent high-precision reference and a forward-error
budget derived from its input domain and arithmetic, not from measured output
deviations. The proposed narrow domain is exactly representable integer bags
with sample variance/mean/CV and Welford/Chan accumulation. It must include
arbitrary partition/merge order, and reject unbounded or poorly conditioned
cases as Uncovered.

The bounded oracle `integer-welford-schedules-v1` now enumerates every update
order and binary partition/merge tree for at most six non-NULL integer inputs
per group, with sum(abs(inputs)) <= 2**53. That proves all AVG partial sums
exact. It covers the Paro unfused Chan formula and DuckDB 1.5.5's fused-mean
formula; fused multiply-add is independently rounded from exact Fractions.
The closed min/max range of those arithmetic schedules is the output
enclosure, not a tolerance fitted to either engine's observed results.
An independent 100-digit integer-moment reference reports mathematical error.
This is a narrow arithmetic contract, not a universal accuracy guarantee for
ill-conditioned variance or larger groups. Outside it the result is Uncovered.

Tests cover NULL/empty/singleton/zero mean, duplicate and negative values,
large-offset small variance, permutations, parallel merges, non-finite
rejection, signed-zero/subnormal ULP distance, and adversarial overlapping
numeric matches. The default harness does not automatically apply this
oracle to SQL: an explicit input/role and relational-boundary proof is still
required. The registration is [q39-numeric-relation.json](c0/q39-numeric-relation.json),
with [complete joined input SQL](c0/q39-integer-input-bags.sql).

Outputs must be matched bijectively while preserving exact non-approximate
columns; an approximate Counter comparison is invalid. Predicates and
ORDER BY/LIMIT must independently establish the exact selected bag and order.
An interval crossing a filter/rank boundary is Uncovered, not evidence that
nearby outputs may hide missing rows or changed duplicates. An exact singleton
at the threshold is decidable: `CV > 1` excludes `CV == 1`. General approximate
ORDER BY/LIMIT remains Uncovered. A registered unique exact ordering prefix
can prove the order without comparing an approximate suffix.

### Q39 evidence, not a universal tolerance

Both engines agree on all 360,000 joined integer input rows, including NULL
and duplicate multiplicity. The independent oracle evaluates all 90,000
groups, not just groups in the output. Their non-NULL cardinalities are 1–4,
inside the preregistered exhaustive domain. It derives 6,250 eligible groups
and exactly 243 self-join rows, with a unique exact ordering prefix. One group
has CV exactly one in every enumerated schedule and is correctly excluded.
All 972 approximate returned values in both engines fit their independently
derived arithmetic enclosures. The 100-digit reference and individual errors
are retained, along with the failing raw exact comparison.

These findings certify the captured Q39 relation under this narrow contract;
they do not repair F2's missing historical source attestation or certify the
remaining corpus. Input/result seed and oracle identities must match, output
schema must independently pass, and unsupported additional result sets,
floating ordering or LIMIT boundaries fail closed. No production aggregate,
planner printer, or SQL expected-result file is changed.
