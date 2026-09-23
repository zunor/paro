# Physical search core: implementation and evidence

Registration: [physical-core-registration.md](../physical-core-registration.md).
The task delivers three ownership/lifetime changes, not a new search policy:

1. Physical tasks share immutable ReadSet and dependency snapshots. Unchanged
   refresh/canonicalization preserves storage; a changed fact detaches the
   snapshot and retains the exact invalidation scope.
2. Optional-domain opening preserves exact prices and immutable CandidateIds,
   but reopens response membership and completion. A recipe re-admits its
   resident allocation only in the matching facts/goal/grant/calibration
   context. Newly eligible optional children still run. The verified mandatory
   plan remains an interruption fallback, not proof of optional closure.
3. Frozen choices lower bottom-up into immutable selected occurrences with
   cached layouts, replayed keys and scalar slots. Local single-node shells
   adapt existing scalar contracts; no descendant-owned binder tree is rebuilt.
   Utility statements consume their binder tree once. Local child arity/schema
   checks precede indexed layout derivation; a 10,000-deep selected chain has
   iterative construction and destruction.

`EXPLAIN (COMPILE, DETAIL)` now includes a fixed-size typed exclusive work
ledger and an independent mandatory/optional/outside phase projection. Both
sum exactly to the same optimizer interval. They must not be added together,
subtracted from normal C1, or interpreted as CPU samples. Disabled normal
collection does not collect the Detail ledger.

## Retained arms

Each directory is the maintained sealed RunOutput, with source/build inputs,
all normal samples/receipts and one separate bounded capture. No raw server
logs or per-event duplicate export are archived. Five fresh normal blocks and
one Detail block per query/arm; 4 threads, 2GB, verifier off, quality policy,
normal compile-work receipts enabled, DuckDB 1.5.5, generator-declared metadata.
That metadata track is not a first-statement parity certification.

| Arm | Source | Meaning |
| --- | --- | --- |
| control | `2a74f285` | Typed attribution, before the three changes |
| resident | `ddcd76ab` | Shared physical task snapshots |
| coverage | `118ae28a` | First price-retention implementation; regressed |
| selected | `d45163e4` | Immutable selected lowering, still on regressed coverage |
| reopened | `89360f8b` | Price validity separated from active response membership |

Normal compiler medians (ms), not Detail times:

| Arm | Q11 | Q04 | Q74 |
| --- | ---: | ---: | ---: |
| control | 12.410 | 22.656 | 64.969 |
| resident | 14.223 | 19.864 | 65.794 |
| coverage | 18.948 | 32.599 | 61.766 |
| selected | 16.450 | 31.344 | 64.720 |
| reopened | 13.608 | 21.915 | 65.772 |

All samples, including the 56.500ms Q11 and 98.920ms Q74 values, remain in the
sealed cells. Sequential builds/collection and substantial host/DuckDB drift
confound cross-arm timing. These are **NotCertified pilots**. The registered
half-time target (Q11 median <=6.25ms / P90 <=7.6ms) is not met; no independent
normal compiler or C1 speedup is established.

## Attribution and negative results

The control Q11 Detail optimizer interval is 13.168ms: outside search 5.147ms,
mandatory 1.614ms, optional 6.408ms. Largest exclusive buckets include pre-work
2.717ms, subproblem 1.294ms, recipe 1.199ms, finish 1.051ms, lowering 0.949ms,
settlement 0.753ms, staging 0.601ms and encoding 0.539ms. Unclassified remains
explicit (0.518ms). There is no single 6ms pricing hotspot in this sample.

Retaining active mandatory frontiers was not equivalent to retaining valid
prices. Q11 synthesized 448 rather than 420 combinations and evaluated 21
quality candidates rather than one. Parent responses exposed partially
refreshed combinations, and the final selected identity changed. A first-visit
guard did not fix it. The final coverage design reopens membership, preserving
prices/archive identity and ordinary optional enumeration; the ineffective
guard was removed.

| Query | Control syntheses / archive publications | Reopened | Groups / logical / physical |
| --- | ---: | ---: | ---: |
| Q11 | 420 / 338 | 401 / 319 | 53 / 61 / 105 |
| Q04 | 810 / 537 | 782 / 509 | 69 / 77 / 121 |
| Q74 | 1,065 / 745 | 1,045 / 725 | 95 / 136 / 217 |

Archive publications count distinct immutable allocations, not re-admissions
to a frontier. Q11 and Q04 return to one quality evaluation; Q74 still needs
51. Reopened artifacts and actual admitted identities match the original
control on all three queries. Fingerprints are locators, not equivalence
proofs: full typed row/bag/order checks independently pass (6/90/92 rows).
All stop with QualityPolicySatisfied + SearchIncomplete, never ProofComplete.

Immutable lowering changes the Q11 local Detail bucket from 0.949ms (control)
to 0.639ms (selected), then 0.722ms (reopened). This is one diagnostic sample
per arm, not a causal timing certificate. Price retention removes 19/28/20
actual syntheses; it cannot by itself explain or promise a 50% compile saving.

## Validation in progress

The reopened source passed 6,922 workspace tests (85 ignored), optimizer 1,384
tests and strict optimizer Clippy. All 15 retained runs, 30 payloads and 15
captures pass the maintained campaign/receipt/document validators. The
compile-only gate reader rejects the TPC-DS payload shape; it was not used to
claim a cold-planning gate pass.

The first SQL gate found 164 pass / 21 fail. Eighteen failures are byte-identical
to the pre-task control, but three subquery cases expose a real local name
reduction bug: MARK/SEMI/ANTI consumed all structural child-name results rather
than only the visible side. Fixing this uses the layout's shared side contract,
with all join types/empty input/projection-map tests. No expected output changes.
A final registered matrix and full SQL gate on that corrected source follow;
the preceding performance evidence is not relabeled as that later binary.
