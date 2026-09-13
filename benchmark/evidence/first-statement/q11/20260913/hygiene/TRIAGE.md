# Optimizer hygiene triage — T-CWC baseline failures (preliminary, read-only)

> Status: preliminary; the supplied clean run is recorded below, but the
> current dirty worktree was not reproduced.
>
> Date: 2026-09-13. This note is a read-only classification of the four
> archived optimizer failures plus the newly observed mark-join failure. This
> triage did not launch a test, build, SQL process, or benchmark; it only read
> the supplied clean-run log. No assertion was repaired, blessed, or otherwise
> changed.

## Scope and evidence boundary

- Current repository HEAD at final inspection: 92b904a57ecf7c1f34186bf3f2d93e157bb09b06.
- The worktree is dirty. In particular, the existing physical-extraction
  changes and the other user-owned changes were inspected only for status and
  were preserved; they are not part of this triage.
- No applicable AGENTS.md was found under paro or its workspace parent.
- The supplied clean-source optimizer run uses this same commit (full log:
  /private/tmp/paro-hygiene-optimizer.log) and reported 1213 passed / 5
  failed. The log was produced from a clean source checkout; the current
  worktree still has user-owned dirty files and was not rerun here.
- A separately supplied isolated mark-join run
  (/private/tmp/paro-hygiene-mark-isolated.log) reproduces the same expected-
  grant error before the mark-join assertion.
- A full SQL run completed 164 passed / 20 failed in 44.87s with FD16384,
  after an FD256 run aborted with EMFILE. The 20 SQL failures are deliberately
  not classified in this optimizer hygiene note; the main README is the
  source of record for that SQL run.
- The earlier evidence is the archived clean-source T-CWC run, available in
  [T-CWC RESULTS](../compile-work-closure-t0/RESULTS.md) and
  [T-CWC README](../compile-work-closure-t0/README.md). That run reported
  paro-optimizer as 1200 passed / 4 failed; SQL regress and current
  performance were not rerun by this triage.

The classifications below therefore describe the strongest conclusion
supported by the archived failure plus current-source inspection. They do not
claim that the current dirty worktree still has the same behavior.

## Classification summary

| Failure | Classification | Confidence | Main reason |
|---|---|---:|---|
| statistics_read_cache_revalidates_registry_rollback_reinsert_and_merge | Expired assertion; identity-contract clarification remains open | High | Merge invalidates and recomputes the cache, but an equivalent semantic group is allowed to receive the same semantic fingerprint. |
| nested_filters_share_one_ordered_source_work_lane | Actual defect (runtime-filter identity/composition coverage loss) | Medium-high | Two distinct executable RF occurrences reach one source lane, while the current identity/composition path can retain only one. |
| engine_admits_every_partition_discriminator_from_one_binding | Archived actual defect, masked by current expected-grant fixture failure | High for archived defect; current clean run unverified | The current clean run fails before the partition assertion; the earlier run showed a legal multi-output transformation rejected or rolled back before publication. |
| nary_sharing_plan_is_stable_across_default_budget_envelope | Unresolved representation/search contract; current target assertion masked | High for archived contract; current clean run unverified | Archived runs preserve model cost and scan shape but change the physical fingerprint under finite budget; the supplied clean run stops earlier on an undeclared grant. |
| mark_join_to_semi_is_an_explicit_isolatable_transformation | Stale fixture / expected-grant entry-contract mismatch | High | The test reaches neither its mark-join assertion nor search; the supplied run fails because the context-derived expected class is absent from the one-class fixture. |

## Supplied clean run and expected-grant mismatch

The supplied log records the following five failures:

- `nary_sharing_plan_is_stable_across_default_budget_envelope` at
  `dimension_sharing.rs:1198`: `unwrap()` receives `expected grant is not a
  declared class`.
- `statistics_read_cache_revalidates_registry_rollback_reinsert_and_merge` at
  `memo/tests.rs:81`: the archived equal-fingerprint assertion.
- `mark_join_to_semi_is_an_explicit_isolatable_transformation` at
  `planner/tests.rs:1160`: `unwrap()` receives the same expected-grant error.
- `nested_filters_share_one_ordered_source_work_lane` at
  `planner/tests.rs:1828`: the archived `1` versus `2` retention assertion.
- `engine_admits_every_partition_discriminator_from_one_binding` at
  `cte.rs:582`: `unwrap()` receives the same expected-grant error.

The source/fixture chain explains the three precondition failures without
claiming that the underlying tests are permanently obsolete:

1. `optimize_grant_portfolio` validates that a context-derived expected class
   is present in the supplied class map at
   [engine.rs:3599](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/engine.rs:3599).
   The error is an intentional fail-closed source check, not a downstream
   transformation result.
2. The production planner derives that expected class from frozen
   `compile_resources` and the Memo's grant-class budget before calling
   `optimize_for_expected_grant` at
   [planner/mod.rs:1631](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/planner/mod.rs:1631).
3. The shared test fixture returns only `ResourceGrantClassId(0)` at
   [planner/tests.rs:30](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/planner/tests.rs:30).
   The n-ary and CTE tests inline the same one-class shape at
   [dimension_sharing.rs:1192](/Users/linjunhong/workspace/paro/crates/optimizer/src/aggregate/dimension_sharing.rs:1192)
   and [cte.rs:576](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/planner/transformation/cte.rs:576).
4. `setup_session()` supplies default limits of 64 MiB and one thread at
   [test_support.rs:161](/Users/linjunhong/workspace/paro/crates/context/src/test_support.rs:161)
   and captures matching compile availability at
   [test_support.rs:239](/Users/linjunhong/workspace/paro/crates/context/src/test_support.rs:239).
   With the default three grant classes
   ([budget.rs:158](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/budget.rs:158)),
   `compile_grant_classes` creates one-quarter, one-half, and full-memory
   classes at [compile_resources.rs:65](/Users/linjunhong/workspace/paro/crates/context/src/compile_resources.rs:65),
   all with one task; `expected_grant` therefore selects the full-memory
   class, index 2, at [compile_resources.rs:32](/Users/linjunhong/workspace/paro/crates/context/src/compile_resources.rs:32).

Thus the one-class fixtures declare class 0 while the context asks for class 2.
This is a stale test-entry fixture relative to the expected-grant contract, or
an intentional test-contract mismatch that must be resolved explicitly; it is
not evidence that n-ary sharing, CTE partitioning, or mark-to-semi failed
semantically in this run. The minimum future choice is to make the fixture's
declared classes match its context, or to exercise an explicitly context-free
entry point. Those choices are not equivalent: the former can exercise the
real lazy-grant path, while the latter changes the entry contract. No choice
was made here.

## 1. Statistics cache revalidation after merge

**Failure:** statistics_read_cache_revalidates_registry_rollback_reinsert_and_merge

**Supplied clean-run observation.** The 92b904a5 log reproduces the archived
failure at log lines 1261–1266 and `memo/tests.rs:81`, with the same two
fingerprints `940ae85de045d99fde457bca8e053cc1`. This is an actual clean-run
reproduction of the assertion, but not a reproduction on the current dirty
worktree.

**Archived observation.** The failure is at
[memo/tests.rs:81](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/memo/tests.rs:81):
after merge_groups, the test requires the newly read fingerprint to differ
from published, but both archived values were
940ae85de045d99fde457bca8e053cc1. The same test first checks the expected
cache transitions around producer registration, rollback, and reinsertion.

**Current-source rationale.** Group stores the read cache as a
(cte_registry_revision, Fingerprint) pair and
local_statistics_fingerprint recomputes it when the registry revision changes at
[memo.rs:1503](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/memo.rs:1503).
merge_groups advances the registry revision and invalidates both groups at the
merge site, while compute_local_statistics_fingerprint deliberately uses
semantic facts/statistics for producer groups rather than Memo-local group
ordinals (see the implementation around
[memo.rs:1534](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/memo.rs:1534)).
Consequently, invalidation and recomputation can be real even when the
recomputed semantic value is unchanged. The failing assert_ne! conflates a
cache lifecycle event with a required semantic fingerprint change.

**Minimum next check (not performed).** Keep the lifecycle assertions tied to
revision/invalidation and repeated-read stability. Separately decide whether
the design promises a merge/allocation identity in this fingerprint; if it
does, specify that identity explicitly before changing either implementation or
test. No such change was made here.

**Risk.** Adding Memo-local group identity merely to make the assertion pass
would make semantically equivalent plans dependent on allocation order, cause
unnecessary invalidation, and potentially poison reuse across group merge or
replay. Removing the assertion without retaining a revalidation check would
hide a genuine stale-cache defect. This is why the assertion is classified as
expired, with the identity contract still open, rather than as a green/blessed
test.

## 2. Nested filters sharing one ordered source lane

**Failure:** nested_filters_share_one_ordered_source_work_lane

**Supplied clean-run observation.** The 92b904a5 log reproduces the archived
failure at log lines 1273–1278: `planner/tests.rs:1828` still observes one
retention where two are required. Unlike the n-ary and CTE cases, this test
gets past optimizer entry validation and reaches its target assertion.

**Archived observation.** The test builds two nested right-build runtime-filter
joins over the same fact probe source. The archived failure is at
[planner/tests.rs:1828](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/planner/tests.rs:1828):
the source lane has 1 retention instead of the required 2, although the
test has two selected RF edges and two filters. The expectation is one
physical source lane, not two scans; it still requires two distinct applicable
retention/evaluation records.

**Current-source rationale.** The source descriptors are assembled by
runtime_filter_source_retentions at
[contracts.rs:279](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/planner/contracts.rs:279).
Their domain fingerprint combines a semantic proof and source identity, while
the evaluation identity is supplied separately. That is a narrow identity
surface for nested occurrences with the same source/operator shape but
different build-side facts. More importantly, the source-lane composition in
[engine.rs:10297](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/engine.rs:10297)
uses find for the matching source, and the subsequent retention path dedups
by domain while the filter path dedups by evaluation. This can discard a
second executable occurrence before ordered source-work costing. The archived
20-row versus 500-row build setup makes the two occurrences materially
different, so this is a concrete coverage loss, not merely an assertion about
the number of lanes.

**Minimum next check/fix direction (not performed).** Trace the two exact
selected physical edges through descriptor creation and composition. The
smallest likely repair is to preserve all same-source descriptors and give an
occurrence identity enough stable build/fact context to distinguish different
work, while retaining deduplication for an exact replay of the same proof.
Confirm the resulting source-work semantics with an independent oracle before
touching the test. No repair was applied.

**Risk.** Over-distinguishing equivalent filters can double-charge or double-
apply source work; under-distinguishing them loses a real filter or retention.
Changing domain/evaluation identity also affects fingerprints, reuse, and cost
composition, so a local count-only fix could create either execution-quality
regression or incorrect costing.

## 3. Every partition discriminator from one CTE binding

**Failure:** engine_admits_every_partition_discriminator_from_one_binding

**Supplied clean-run observation.** The 92b904a5 log fails at log lines
1280–1283 while unwrapping the optimizer result at `cte.rs:582`, with
`expected grant is not a declared class`. The intended `rule_insertions`
assertion at the following lines is therefore not reached in this run.

**Archived observation.** The test at
[cte.rs:548](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/planner/transformation/cte.rs:548)
creates one materialized binding with two discriminator dimensions and four
legal branch combinations. The archived assertion at line 583 observed an
empty rule_insertions map, with the message that a two-output binding had
been rolled back by a one-output reservation. The following completeness
assertion was not a substitute for this missing publication.

**Current-source rationale.** CteRequirement::partitions and
partition_by_ordinal construct multiple restricted producer alternatives; the
relevant partition construction begins around
[cte.rs:1214](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/planner/transformation/cte.rs:1214).
The engine then derives an output bound and reserves output events before
applying the transformation in the path around
[engine.rs:4758](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/engine.rs:4758).
If an output reservation is unavailable, the current path can finish without
applying any output; if a later bound check fails, it rolls the output set
back. The budget layer treats optional reservations and their release as
event-identity operations at
[budget.rs:511](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/budget.rs:511).
Together with the archived {} result, this is consistent with a real
allocation/publication contract failure: one binding's complete multi-output
closure is not admitted atomically and therefore valid alternatives disappear
from the Memo.

**Minimum next check/fix direction (not performed).** Reproduce only after
permission with allocation identity and reservation traces sufficient to show
which output is reserved, rejected, or released. The likely minimal contract
shape is an atomic reservation keyed by binding, dependency version, and each
output discriminator, followed by all-or-none publication; an incomplete
budget must remain an explicit BudgetLimited result rather than silently
looking like no transformation. No implementation change was made.

**Risk.** Publishing a partial discriminator set can corrupt CTE holes,
consumer coverage, or fact versions. Conversely, widening the reservation
without accounting for every output can consume the budget or allow an
unbounded transformation. Rollback, cancellation, and group merge must all
release the same complete reservation identity.

## 4. New mark-join failure

**Failure:** mark_join_to_semi_is_an_explicit_isolatable_transformation

**Supplied clean-run observation.** The full optimizer log reports this new
failure at log lines 1268–1271, at `planner/tests.rs:1160`, with the same
undeclared expected-grant error. The failure is in the helper's final
`optimize(...) unwrap()` at
[planner/tests.rs:1143](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/planner/tests.rs:1143),
before `selected_join_type` or the mark-to-semi transformation assertion can
run.

**Supplied isolated reproduction.** The separate
`/private/tmp/paro-hygiene-mark-isolated.log` run reaches the same
`expected grant is not a declared class` error at `planner/tests.rs:1160`
(log lines 29–39), with zero passed and one filtered test failed. This confirms
the fixture mismatch is independently reproducible; it still does not test the
mark-join transformation itself.

**Fixture/source rationale.** This helper calls `setup_session()` and passes
the one-element `test_grant_classes()` fixture. The shared expected-grant
chain documented above consequently requests class 2 while the test declares
only class 0. The engine's validation at
[engine.rs:3599](/Users/linjunhong/workspace/paro/crates/optimizer/src/cascades/engine.rs:3599)
is the first observed failure. There is no evidence in this run for a
mark-join semantic or isolatability defect.

**Classification and next check.** Classify this as a stale fixture / entry-
contract mismatch, not as a mark-join defect and not as a blessed test. After
the fixture contract is explicitly aligned, the mark-join assertion must be
re-evaluated independently. No fixture or implementation change was made.

**Risk.** Silently changing this helper to a context-free grant entry point
would avoid the error by changing the path under test. Adding all declared
classes may alter lazy-grant search and expose a different downstream issue;
either choice must be recorded before interpreting the mark-join result.

## 5. N-ary sharing stability across the default budget envelope

**Failure:** nary_sharing_plan_is_stable_across_default_budget_envelope

**Supplied clean-run observation.** The 92b904a5 log fails earlier at log lines
1255–1259, at the `unwrap()` on
[dimension_sharing.rs:1198](/Users/linjunhong/workspace/paro/crates/optimizer/src/aggregate/dimension_sharing.rs:1198),
with `expected grant is not a declared class`. The fingerprint assertion at
line 1236 is not reached, so the n-ary representation behavior was not
reproduced by this clean run.

**Archived observation.** The test varies only the optional-group and optional-
composition limits in
[dimension_sharing.rs:1174](/Users/linjunhong/workspace/paro/crates/optimizer/src/aggregate/dimension_sharing.rs:1174)
for factors 8, 16, 32, 64, and 128, then requires one fingerprint at
[dimension_sharing.rs:1236](/Users/linjunhong/workspace/paro/crates/optimizer/src/aggregate/dimension_sharing.rs:1236).
The archived values preserve expected cost 78.25 and two dimension scans,
but factor 8 produced fingerprint dc8a7be4589ff995d3e1494b3675acda while
the other shown runs used e09658133c1077d23fd05f8b482e4498 (with repeated
factor-16/32 observations). The test therefore exposed plan-identity drift,
not an observed cost or result mismatch.

**Current-source rationale.** N-ary input is lowered to a left-associated
binary Union All by build_nary_union_all around
[dimension_sharing.rs:964](/Users/linjunhong/workspace/paro/crates/optimizer/src/aggregate/dimension_sharing.rs:964),
and partial_union_arm preserves branch identity in the partial projection.
Finite optional budgets can change admission/interleaving and which equivalent
sharing representation is selected. Nothing in the inspected code or archived
failure establishes that a physical fingerprint must be invariant across this
budget envelope. At the same time, the strict test may be protecting an
important reproducibility/cache contract. It is therefore an unresolved
representation/search contract, not safe to bless as an expired assertion and
not enough evidence for an actual semantic defect.

**Minimum next check/fix direction (not performed).** First specify whether
budget changes are permitted to change a tie-broken physical identity. If
stability is required, define a canonical semantic tie-break independent of
optional admission order and test it alongside cost/result invariants. If it is
not required, retain tests for cost, result, scan sharing, and valid execution
while explicitly relaxing only the identity assertion after design review. No
test or implementation was changed.

**Risk.** Weakening the assertion could conceal fingerprint churn that defeats
reuse or changes execution shape; forcing a canonical fingerprint could alter
search order, budget consumption, and selected physical plans. Either choice
must be separated from semantic correctness and from performance claims.

## Minimum reproducer commands (not executed by this triage)

These are the minimum serial reproductions for any remaining follow-up. They
are listed for the main agent's M/L1/U flow only; this triage did not launch
any of them, and the mark-join command was run separately as noted above.

    cargo test -p paro-optimizer statistics_read_cache_revalidates_registry_rollback_reinsert_and_merge -- --nocapture
    cargo test -p paro-optimizer nested_filters_share_one_ordered_source_work_lane -- --nocapture
    cargo test -p paro-optimizer engine_admits_every_partition_discriminator_from_one_binding -- --nocapture
    cargo test -p paro-optimizer nary_sharing_plan_is_stable_across_default_budget_envelope -- --nocapture
    cargo test -p paro-optimizer mark_join_to_semi_is_an_explicit_isolatable_transformation -- --nocapture

Run them one at a time with the current commit and dirty-file isolation
recorded. The supplied clean run already establishes that statistics and
nested-RF reach their archived assertions, while n-ary, CTE, and mark-join
currently stop at the expected-grant fixture mismatch. These commands are not
evidence that the current dirty worktree fails or passes.

## Read-only conclusion

The supplied full optimizer run, plus the isolated mark run, confirms the
statistics assertion and nested runtime-filter retention assertion and
independently reproduces the expected-grant fixture failure for mark-join. The
archived CTE multi-output allocation loss
remains an actual defect signal, but it is masked in the supplied clean run by
the undeclared-grant error and therefore is not revalidated at 92b904a5. The
archived n-ary fingerprint issue remains an unresolved representation/search
contract and was likewise not reached in that run. No current dirty-HEAD SQL,
benchmark, or performance result is claimed by this note; the separate SQL
pass/fail count is recorded above without classifying its 20 failures.
