# Owned bridge and fact reuse: T0–T2

## Preregistered sequence

T0 precedes any optimizer optimization. Independently check d93f3b90,
3415b2de, 187140e2, 9182bea0 and 9dbfd547 with clean committed source,
original Q11, the archived immutable SF1 seed, generator-declared metadata,
4 threads, 2GB and the existing quality handoff policy. Each diagnostic records
the original SELECT's admitted fingerprint, synthesis count and published
winner count against the round anchor 4e29cd8f (5c29cf646706c8c8ba84000150211a6b,
1691, 891). Later EXPLAIN is a separately identified diagnostic and its choices
must be compared with the original SELECT before interpreting plan differences.
If all five already drift, extend the investigation to earlier committed
ancestors; do not attribute a cumulative change to a convenient midpoint.

`diagnose.py` reuses the existing server isolation, hash, result, metadata,
cache-miss and trace contracts, plus the existing external watchdog. It runs
one diagnostic block per invocation, not a performance campaign. The first
d93f3b90 check also uses the unchanged full comparator (two normal blocks and
one diagnostic); normal samples are retained as pilot only. All performance
experiments run serially. Data are privately copied; target SQL is unchanged.

T0 must explain and resolve plan drift while retaining 4fd53088's proved
native-completeness shortcut. New legal but misranked alternatives are a cost
model defect to document, not justification to delete a rewrite.

After T0, T1 refines the existing disjoint B3 ledger with per-rule construction,
statistics, fallback, settlement, staging, guard, rollback and validation
boundaries. Coverage must reach 90%; residual is explicit. Cache misses are
classified by semantic content before choosing T2. T2 preserves the fixed
round-anchor triple and rule publication map, and reuses existing fact
invalidation. Normal C1/W and diagnostic accounting remain separate.

No rule, cost, resource budget, frontier, stop or handoff policy changes are
authorized. The broad direct-only predicate experiment remains rejected.
Known optimizer and SQL regress failures remain separate and unblessed.
