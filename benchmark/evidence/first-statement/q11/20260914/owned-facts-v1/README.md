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

## T1 preregistration after transport correction

T0 found native CTE lexical-domain and aggregate fact-derivation differences
against the owned contract. Correcting those dependencies is not a
fingerprint-neutral optimization: b13d3872 is now the corrected control
(2ed1a3e2fe2f455d4411779edd54856b / 2500 / 1436). The original round anchor
remains mandatory in every comparison; its 1691/891 target is NOT restored.
The fresh b13 pilot recovers warm ratio to about .895, but C1 is 226.9ms.
T1/T2 must explain any further deviation from the corrected triple as well.

T1 uses the existing opt-in work partition. B3a includes inseparable native
producer guards; B3f is post-construction contract checking, not all guards.
B3b is propagation/gathering plus their immediate statistic adapters; B3d is
the rest of settlement/demand/layout/fact residency. B3h is node staging's
encoding/validation/duplicate handling. Apply residual stays explicit.
Rollback after apply is reported separately within B3g, not silently added
to the historic apply-only denominator. `ns` rows are exclusive; refresh
inclusive cross-checks are NEVER added to them. Rule 0 means unattributed.

Miss classification scans at most 4096 current entries, checks arena
ownership, and keeps exact operator/scalar/output/input facts/lexical CTE
identity. Only non-CTERef, non-JoinGraph root annotations that local gathering
overwrites may differ; root materialization risk remains exact. NewContent
means no equivalent LIVE entry under this local contract, not never seen in
history or absence of arbitrary algebraic equivalence. Overflow is Unverified.
This is diagnostic-only; production keys and invalidation are unchanged.

Predeclared same-binary OFF/ON/OFF diagnostic sequence: compare optimizer
wall time, exact choices and work counts. More than 10% ON overhead requires
reduced collection and a new sequence; do not use distorted data. Normal
C1/W remain trace-off, separate fixed two-block pilots, not formal parity or
T1 grant non-inferiority campaigns. All valid samples are retained.
