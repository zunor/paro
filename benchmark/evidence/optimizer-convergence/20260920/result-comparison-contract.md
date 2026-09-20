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

## Numerical acceptance: scope and pending admission

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
cases as Uncovered. Acceptance code and bounds are not yet admitted here.

Outputs must be matched bijectively while preserving exact non-approximate
columns; an approximate Counter comparison is invalid. Predicates and
ORDER BY/LIMIT must independently establish the exact selected bag and order.
An interval touching a filter/rank boundary is Uncovered, not evidence that
nearby outputs may hide missing rows or changed duplicates. Q39 remains
uncertified until this relational check is complete for all input groups,
including groups absent from the returned rows.
