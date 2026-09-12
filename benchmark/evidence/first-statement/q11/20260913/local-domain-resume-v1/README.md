# Local-domain publication: bounded failed matching follow-up

2026-09-13 preregistration. Repair ordered child allocation identity together
with the directly exposed NonNullInput shared-failure traversal. Reuse only
complete empty results within one immutable Memo enumeration, retain reads,
and admit actual failed visits. No policy, budget, model or executor change.

First pilot: handoff then default, two fresh processes per arm, original Q11,
same immutable SF1 seed, 4 threads / 2 GB decimal, binary protocol, trace-off,
cache-miss and full typed ordered 90-row validation. One warmup, one ABBA round,
bootstrap 200 / seed 1. Diagnostics separate. Retain every valid slow sample.
Compare against the preceding archived candidate only as historical context;
this is not a randomized formal M1 or parity campaign.

Do not admit the allocation repair while an unexplained default regression
remains. The next local closure change, if justified, requires its own pilot.

Second mechanism registration, after v1 finished: coalesce a new necessary
condition at an existing legal leaf Filter in the selected native closure,
using the shared transfer contract, retaining the exact Memo input. This
targets the stacked-filter publications observed around 50 ms in the prior
diagnostic. Repeat handoff then default, two fresh blocks each with the same
settings. Do not attribute the failed default control to matching alone.

Confirmation registration after v2: repeat the unchanged v2 handoff binary for
four fresh blocks, same settings, retaining all samples. Then collect one
separate EXPLAIN ANALYZE profile for input/plan inspection (not normal C1).
Default v2 remains regressed; a sub-200 ms experimental handoff cannot admit
that global allocation change to production. Test the independently safe
matching/leaf coalescing subset without the allocation change afterwards.

Subset v3 registration: restore only the unadmitted allocation change and its
dependent regression test; retain invocation-local failed matching and legal
leaf coalescing. Run two fresh handoff blocks then two default blocks with the
same settings. The removed test/patch remains in the preceding evidence.
This isolates what can be shipped without the exposed default expansion.

Final subset confirmation registration: v3's initial handoff cohort shows
DuckDB C1 137.4/174.5 ms and broad W dispersion. Retain it unchanged; repeat
four fresh handoff blocks, otherwise identical, without selecting a faster
campaign as the final result. No formal acceptance claim from this pilot.

## Outcome: fast local closure demonstrated, global publication NOT admitted

Retained changes:

- `e45061c1`: invocation-local complete negative NonNullInput DAG results,
  charged failed visits, preserved read subscriptions and recursion semantics.
- `815f2222`: combine a necessary condition at an existing legal native leaf
  Filter before publishing, using the shared column-transfer contract. The
  original Memo input, residual location and output contract remain intact.

The global ordered-child allocation repair remains **withdrawn**. Its rebased
patch/test is `allocation-not-admitted.patch` (`git apply --check` passes on
the final mixed worktree). A 177.8 ms experimental handoff does not justify
shipping a 22.9-second default regression. No default policy, rule budget,
cost model, executor, task framework or parallel search change was made.

Variants: v1 = matching + allocation; v2 = matching + allocation + leaf
coalescing; v3 = retained matching + leaf coalescing, without allocation.
The retained code therefore does **not** deliver v2's candidate-arrival time.
The root is still a consumer of exact selected choices; this experiment did
not build a new grant-independent general producer task kernel.

## Normal SELECT results

All times below are milliseconds. All valid samples, including slow ones,
remain in the compressed raw reports and server logs beside this README.

| cohort | fresh blocks | Paro C1 median / p95 | DuckDB C1 median | paired C1 ratio [95% CI] | Paro W median / p95 |
| --- | ---: | ---: | ---: | --- | ---: |
| v1 handoff | 2 | 236.603 / 238.406 | 108.576 | 2.179104 [2.171412,2.186824] | 101.879 / 102.887 |
| v1 default | 2 | 22077.937 / 22741.918 | 187.820 | 128.159135 [86.527473,189.821375] | 144.331 / 192.661 |
| v2 handoff | 2 | 183.176 / 184.911 | 121.410 | 1.508997 [1.492174,1.526010] | 108.340 / 120.769 |
| v2 default | 2 | 22866.114 / 22976.581 | 134.776 | 169.676845 [168.014421,171.355717] | 143.203 / 164.746 |
| v2 handoff confirmation | 4 | 177.773 / 189.224 | 108.985 | 1.654483 [1.631166,1.693323] | 99.485 / 105.743 |
| v3 handoff | 2 | 365.586 / 365.600 | 155.990 | 2.360407 [2.094647,2.659886] | 126.001 / 170.749 |
| v3 default | 2 | 1427.725 / 1479.463 | 120.926 | 11.863562 [11.078495,12.704264] | 107.383 / 126.883 |
| v3 handoff confirmation | 4 | 261.148 / 263.011 | 106.461 | 2.437557 [2.396155,2.479674] | 92.200 / 94.913 |

v2 confirmation C1 samples: [178.644625,176.041666,176.901834,189.224083].
Its W ratio is 0.958155 [0.942688,0.969828].
v3 confirmation C1 samples: [259.476708,258.017000,263.011083,262.819333].
Its W ratio is 0.883594 [0.866591,0.896081].
The v3 initial slower campaign is not discarded or merged away. Machine drift
is apparent in DuckDB and W dispersion. These sequential pilots are not a
formal milestone campaign; the gate reports EvidenceValid/MilestoneNotPassed.
Retained v3 has not passed M1 or parity. v2 observes the sub-200 ms experimental
target but fails production admission. No cross-cohort median subtraction is
used as a same-statement stage breakdown.

Original archived Q11, 4 threads / 2 GB decimal, the same immutable SF1 seed,
binary protocol, fresh process/session, first target SELECT, trace-off and
post-timer cache-miss validation were used. All cohorts pass the complete
90-row typed schema/order/digest comparison with DuckDB. Resource declarations
are not measured RSS; broader query/grant/DOP/SQL matrices remain unverified.

## What actually became earlier

v1 and v2 same-SELECT diagnostic cohorts, respectively:

| diagnostic metric | v1 | v2 |
| --- | ---: | ---: |
| first long logical publication | 29.897 ms | 30.134 ms |
| first qualified root | 85.909 ms | 33.276 ms |
| quality policy satisfied | 89.388 ms | 34.594 ms |
| compiler return, statement elapsed | 106.844 ms | 46.709 ms |
| optimizer duration | 104.985 ms | 45.053 ms |
| direct dispatch / charged work | 7 / 36 | 1 / 18 |
| cost synthesis / recompute | 2966 / 356 | 1361 / 176 |
| quality evaluations | 250 | 71 |
| PredicateTransfer publications | 14 | 2 |

The first long closure was not constructed earlier. It stopped producing
intermediate stacked Filters that required later ordinary-rule publication
and physical consumption before the same quality policy could pass.

v2 exact diagnostic path: source71 publishes logical108 at 30.134 ms; the
separate ordinary binding publishes111 at30.585 ms but is not that root's
producer. Filter94/candidate731 is published at32.863 ms, Filter101/candidate751
at33.026 ms, producer765/logical108 at33.115 ms, root766 at33.124 ms; root766
qualifies at33.276 ms. Both Filter94->scan3 and Filter101->scan10 are already
in that long closure. By contrast v1 final filters132/133 require additional
publications at47.163/47.241 ms. These IDs are evidence identities, not code keys.
The policy-satisfied event later names candidate912. The timeline above is
the retained first-qualified root for its grant, not a claim that every normal
sample executes root766 or that all grants share one physical candidate.

All v2 PredicateTransfer logical publications are retained (two, dropped=0);
ParentPublished also has dropped=0. Other physical stages still lose4407
events, so this is not a complete CPU/wait attribution. The actual selected
chain above has retained publication/consumption events. Rule, freeze and
optimizer inclusive times are not added together.

v3 confirmation returns to quality123.883 ms / compiler145.195 ms, direct21/204,
synthesis4903/recompute1012, evaluations227. Initial v3 records source71's
long binding matched but not published, while a different ordinary binding
publishes logical96 and becomes the final producer. Its input joins246/247
appear at145.537/145.777 ms in that slower diagnostic. Logical publication
events are complete. Info logs do not expose the precise rejection error;
the restored allocation omission, prior direct counterexample and current
CTE fail/pass result support allocation as the cause, not a claim that every
rejection counter denotes that error.

All handoff cohorts report QualityPolicySatisfied + SearchIncomplete.
Default cohorts report BudgetLimited + SearchIncomplete, not ProofComplete.
A missing default quality evaluation is not evidence of satisfied obligations.

## Default-path blocker and matching result

With allocation enabled, v1/v2 default both reach groups/logical/physical
1005/8656/14850, synthesis541491/recompute123996, PredicateTransfer
matched128107/published7397/ineffective19431. v1 also records combination budget
rejections549657 and transformation budget exhaustion259895. These counts are
not all duplicates; full identity classification remains undone.

v1 default's AggregateNonNullInput binding/apply falls to65.396 ms from the
preceding experiment's19199.951 ms, with no constructed/published output.
The stopping state and explored work differ, so that subtraction is not a
normal C1 causal estimate. Current dominant rule accounting is PredicateTransfer
2999.353 ms; total optimizer20532.682 ms cannot be explained by that rule alone.
The remaining logical/physical publication, fact maintenance and pricing work
must be attributed before another global allocation admission attempt.

After withdrawing allocation, v3 default returns to groups/logical/physical
816/1373/2503, synthesis20340/recompute4151, optimizer1314.697 ms and compiler
1317.195 ms. NonNullInput is0.617 ms. This is restored default behavior, not
a claimed speedup against a new same-campaign unmodified default control.

Next single direction: make the corrected restricted-view publication identity
admissible by classifying and eliminating the exposed unnecessary domain
publication/physical-combination work. First distinguish legitimate alternatives
from equivalent repeated work; do not assume all7397 publications or541491
syntheses are redundant. Do not add queue priorities, early deadlines or a
new task framework: the tested local chain already reaches a qualified
root in about33 ms when publication succeeds.

## Independent execution profile (not C1)

The v2 EXPLAIN ANALYZE is independently compiled, not proof of the exact normal
image. Both date scans output730 rows. Fact/partial aggregate input is
1096053/289524; partial output76100/22806; final aggregate input/output
76098/22804. Customer scans output61631/100000. CTE materialization, consumer
filters and aggregation-output residuals remain. CLIENT_RESULT is90 rows;
the100-row wire payload is EXPLAIN text, not SQL result cardinality.

Profile execution325.449 ms/client687.375 ms is instrumentation-heavy and not
used as normal C1 attribution. Peak RSS369393664 bytes, declared working set
95561216 bytes, observed workers/max tasks4/4, RF installed3, spill bytes0.
Do not infer spill from the existence of zero-row spill-replay operators, or
quality from RF/aggregate counts. No executor tuning occurred.

## Tests, ownership and remaining acceptance

- Invocation-local failure DAG tests cover linear completion, real visit
  charging, shared reads, budget exhaustion, fresh invocation after changed
  facts/new Get, and cycle termination without hiding finite Get alternatives.
  Disabling only negative reuse while retaining charging makes depth16 return
  BudgetLimited instead of Complete; restoring it handles depth24.
- Real optimize_for_grants -> FrozenCandidate -> selected binding -> native
  staging/publication test verifies both predicates land on the original input
  group. Disabling leaf coalescing reproduces an intermediate Filter group.
  An independent evaluator consumes the actual published predicates and checks
  grouping/filtering results over duplicate and NULL input keys.
- With allocation: transformation68 tests passed, including the CTE multi-output
  and distinct-restricted-child/rollback regression tests.
- Final retained source: transformation66 passed / 1 failed (CTE multi-output,
  cte.rs:583); domain_45, engine102 and TaskRegistry22 passed. Counts overlap.
  The leaf production/bag test also passes separately. No SQL baselines blessed,
  no all-repository green claim. The allocation-dependent test remains archived.
- Full cross-family performance/SQL/grant/DOP matrices, cancellation/rollback
  matrices for every production shape, formal M1/parity and exact normal/profile
  image matching are not completed. The round's main production delivery
  remains incomplete despite the successful v2 local-chain experiment.

The pre-existing20 staged files (2134 insertions/35 deletions) were preserved;
matching uses a separate index containing only its34-line change and new tests.
Other mixed staging/executor/planner/docs changes were neither reverted nor
included in these commits.
Design/task appendices were committed separately in paro-docs-design as
`441cc29`, leaving its pre-existing uncommitted edits intact.

## Artifact identities

v2 handoff/default/confirmation/profile share binary:
`50834d70837a865466c67d5a9f37cf7cccfe94c083a4d66a8e079f702fbfc17e`.

v3 handoff/default/confirmation share binary:
`ec7473e4bd8ae3a7802d5d784db0767cbac04a90043311c1b29260d9533f0a2f`.

Source was measured before the two code commits on dirty80a14161. Each report
contains full source/harness attestations. README preregistration edits cause
worktree hashes to differ between confirmation cohorts while the executable
binary remains identical; do not describe these as clean-HEAD reproductions.

SQL SHA256:
`1f3c2697b2a82f597f9e8a570413be65ab7a02da03c1fe4c3ea7b725735433b8`.
Seed:
`72ccb3dc02ce127f61c06b85920a0e75c17a55a60ee341d267251c67611f62b1`.
Result:
`9108a5b43530c24778abc0d8d3bf1ee218a9815eb0d15310bb4d7c6bcc5f2c78`.
Original SQL is in the preceding column-transfer archive.
