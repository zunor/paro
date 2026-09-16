# Q11 cross-group domain continuation v1

Date: 2026-09-17

This directory records the bounded continuation slice for the same-Memo
necessary-domain path.  It is an implementation/pilot record, not a formal
performance gate.

## Scope

The production quality lane now carries an exact `DomainContinuation` when a
native PredicateTransfer path reaches a Memo group hole.  The continuation
contains the selected `PatternBinding`, the next hole, routed predicates,
occurrence/context, and the `PatternRead` for the hole.  The read includes the
logical frontier and facts, so a negative immediate-shell lookup can be woken
by a later Memo publication.  The continuation is published only after the
existing transformation transaction commits, then runs through the existing
quality forced-binding queue, budget accounting, ReadSet observation,
rollback, verifier, and FrozenCandidate handoff path.

Normal transformation tasks do not enable the sidecar.  No search budget,
cost model, quality policy, stop policy, frontier policy, execution path, or
SQL was changed.  The continuation is not a completion certificate and does
not remove ordinary alternatives.

## Decision measurement

The Q11 diagnostic lane used four selected PredicateTransfer bindings.  An
exploratory shape probe (not used as performance evidence) showed that their
paths already expand all transparent intermediate operators and terminate at
leaf group holes (the observed leaf groups were 0, 1, 3, 7, 8 and 10).  None
of those holes had a supported immediate logical operator that could consume
another routed predicate.  Therefore the continuation mechanism was not
legally applicable to this Q11 selected path: it emitted and dispatched zero
continuations.  This falsifies the assumption that the current Q11 delay is
caused by a missing transparent cross-group hop; it does not falsify the
mechanism for a Memo in which such a hop exists.

The production vertical slice is covered by
`production_binding_records_a_resumable_continuation_at_a_memo_group_hole`:
it builds a real `MemoBuilder` graph, invokes
`PlannerTransformationRule::apply_binding`, leaves the inner group opaque,
and checks the exact continuation path and negative frontier read.  The
quality-engine test dispatches the same continuation twice and verifies
idempotent enqueue plus preservation of the read/context metadata.

## Pilot identity

The pilot was run serially with the existing `paro-benchmark` harness:

- source HEAD: `0949f8f764e28fce35cf5fb42f8a470cd9b964ee`
- source: dirty, because the shared worktree contains unrelated staged/unstaged
  and untracked changes; this is not a clean causal comparison
- binary SHA-256: `b73b839d3d65e9ef755c6d5bad89cdf54f700c7fc4094af75a9f0604d524650e`
- seed SHA-256: `9176b57eec70e0963228844168668eb027f6f792b12aececbe93cb6003ee28eb`
- SQL: `11.sql`, original SF1 Q11
- resources: 4 threads, 2 GiB
- normal: fresh process, private per-process seed copy, cache miss, trace off
- diagnostic: one separate trace-on block; excluded from C1/W
- normal blocks: 2, hence pilot only
- result/order digest: `9108a5b43530c24778abc0d8d3bf1ee218a9815eb0d15310bb4d7c6bcc5f2c78`

Both normal blocks passed the 90-row typed result, value, multiset, and order
checks.  The source and binary are intentionally identified as dirty/pilot
evidence; do not pool these numbers with prior clean reports.

## Results

| block | Paro C1 (ms) | DuckDB C1 (ms) | compiler (ms) | optimizer (ms) | syntheses |
|---:|---:|---:|---:|---:|---:|
| 0 | 175.480 | 111.824 | 47.986 | 47.211 | 1,757 |
| 1 | 156.022 | 107.569 | 47.696 | 46.885 | 1,757 |
| median | 165.751 | 109.697 | — | — | 1,757 |

Normal Paro warm median was `72.035 ms`; DuckDB warm median was `106.673 ms`.
The two-block C1 ratio was `1.508676`, 95% pilot interval
`[1.450441, 1.569248]`.  The warm ratio was `0.692669`, 95% pilot interval
`[0.638817, 0.769584]`.  These are direction-only estimates, not M-power or
parity certification.

The separate diagnostic block recorded:

| item | value |
|---|---:|
| direct binding dispatches | 4 |
| direct binding work units | 34 |
| first direct binding (us) | 10,853 |
| last direct binding (us) | 37,459 |
| first complete aggregate region (us) | 41,120 |
| quality policy interval (us) | 41,334 |
| candidate evaluations | 98 |
| avoided freezes | 97 |
| domain continuations enqueued | 0 |
| domain continuations dispatched | 0 |

The diagnostic trace reached the existing quality policy with
`QualityPolicySatisfied + SearchIncomplete`; it did not establish
`ProofComplete`.  The first complete aggregate timing is diagnostic elapsed
time, not a normal C1 phase subtraction.

## Conclusion

The implementation closes the required same-Memo continuation contract for a
real group-hole shape, including negative reads, exact occurrence/context,
idempotent dispatch, and existing transaction ownership.  It did not reduce
Q11 work because the selected Q11 path has no next transparent operator past
its group holes.  The Q11 pilot therefore provides no repeatable compiler
gain or evidence for changing the production stop policy.  The remaining
Q11 compiler path must be located elsewhere; this task should not be expanded
into more local cache, clone, or queue-priority work without a new causal
measurement.

Known status remains `QualityPolicySatisfied + SearchIncomplete`, not
`ProofComplete`; compiler `<=30 ms`, compiler `<10 ms`, and C1 parity were not
met.  The pre-existing optimizer and SQL-regression failures remain separate
and were not blessed by this pilot.

## Artifacts and hashes

- `q11-cross-group-continuation-v1.json.gz` — SHA-256
  `bf094e3e75650b1d5e0dd7ded7a2d1dd29c5c84e40fe58c829a15e4205e5d273`
- `q11-cross-group-continuation-v1.q11.block000.parod.log.gz` — SHA-256
  `6438564073e0c56e017e43a9124d13d0ee233aa7905dba47cee40f82217549dc`
- `q11-cross-group-continuation-v1.q11.block001.parod.log.gz` — SHA-256
  `f1f0326437816a2a50b2d80a57e26f45442477039e7322f42537f2fa6e28c4fc`
- `q11-cross-group-continuation-v1.q11.diagnostic000.parod.log.gz` — SHA-256
  `1bcdb20c7e6e1c6966e763605ade9dd4ad7148643e0458a1790f9b627496022b`
- `q11-cross-group-continuation-v1.q11.oracle.parod.log.gz` — SHA-256
  `862bbbc8578b0996dc756c139fe2d3f945de05e5909c3cc86205833522669d1d`
- `q11-cross-group-shape-probe-diagnostic.log.gz` — exploratory shape-only
  log from the temporary instrumentation used to validate the four binding
  shapes; it is not a normal/performance sample and its instrumented binary
  is not the v1 binary above — SHA-256
  `760a335dc79ba1cb082adfccd64ac1fcc1b543ac25258fb8201b7d03f13fe9c2`
- `11.sql` — SHA-256
  `1f3c2697b2a82f597f9e8a570413be65ab7a02da03c1fe4c3ea7b725735433b8`
