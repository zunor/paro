# B3 native-shell compaction pilot

This is a post-partition implementation pilot for commit `1e21fd862753a5cafadd59bcaeef40d50a4bc8f1`.
The partition in `../work-partition` identified B3 rule apply/rollback as the
largest actionable bucket: 23.685 ms of a 67.066 ms instrumented optimizer
interval, or about 60% of the 39.5 ms previously unaccounted share. The
implementation removes redundant native-shell compaction/layout walks in the
existing native rule paths. It does not change the rule set, search budget,
cost model, frontier comparison, stop policy, or handoff policy.

## Status and provenance

This is an implementation pilot, not a formal M1/M2/M3 campaign. The harness
report correctly records `source.dirty=true` because unrelated user changes
were present in the shared worktree. Those changes were not committed by this
task and remain untouched. Consequently the pilot is not used as clean-source
performance evidence or as a causal comparison with the earlier clean
partition cohort.

- source commit recorded by the harness: `1e21fd862753a5cafadd59bcaeef40d50a4bc8f1`
- binary: `/Users/linjunhong/workspace/paro/target/release/parod`
- binary SHA-256: `060f98d1d5cffe9cdd9f4fbb2976474c55994d504c9a2c44cd968b33385edb8d`
- SQL corpus SHA-256: `65c8b356de484b4922445b09fad720b278db22a3678b81295018c9370be0c3d7`
- Paro seed SHA-256: `72ccb3dc02ce127f61c06b85920a0e75c17a55a60ee341d267251c67611f62b1`
- DuckDB SHA-256: `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`
- resources: 4 threads, 2 GiB memory, planning DOP 1, binary result format
- normal: 2 fresh process blocks, trace off, cache miss, one warmup, complete
  typed/order validation of 90 rows
- diagnostic: one separate traced process plus the additive B0--B12 ledger;
  excluded from normal C1
- harness hashes are retained in `pilot-v1.json.gz`

The diagnostic environment used `PARO_QUALITY_POLICY_HANDOFF=1`,
`PARO_COMPILE_WORK_EVIDENCE=1`, and `PARO_COLD_WORK_EVIDENCE=0`. The normal
sample was not run with statement trace.

## Results

| metric | Paro | DuckDB |
| --- | ---: | ---: |
| cold C1 samples (ms) | 188.627, 194.718 | 112.594, 108.798 |
| cold C1 median (ms) | 191.673 | 110.696 |
| cold C1 p95 in this 2-block pilot (ms) | 194.718 | 112.594 |
| warm samples (ms) | 95.765, 95.362, 96.470, 94.113 | 104.635, 106.324, 104.694, 104.144 |
| warm median (ms) | 95.563 | 104.665 |

The paired cold ratio is `1.731552` with the harness's small-sample 95% CI
`[1.675278, 1.789716]`; the warm ratio is `0.909263` with CI
`[0.900046, 0.920603]`. These intervals are not formal power or tail
certification. Both normal blocks passed cache-miss, trace-off, and complete
typed/order validation; the result digest matched DuckDB.

The two normal compile-work scalars were:

| block | compiler (ms) | optimizer (ms) | rule (ms) |
| --- | ---: | ---: | ---: |
| 0 | 54.741 | 53.852 | 20.752 |
| 1 | 57.791 | 56.810 | 21.792 |

The Q11 ledger rows for the diagnostic occurrence measured B3 at 20.704,
19.787, and 20.797 ms. The other representative buckets were B5
3.035--3.120 ms and B10 0.403--0.429 ms. The ledger remains additive; its
unclassified residual was 2.345--2.819 ms and was not assigned to another
bucket.

The diagnostic occurrence reported 1,689 cost syntheses, 835 published
winners, 299 apply attempts, 100 inserted expressions, 343 implementation
requests, 1,042 subproblem requests, and 1,393 subproblem reuses. These are
current-pilot counters, not a claim that they equal the clean partition
cohort: the report is dirty and its surrounding mixed changes/cohort differ.
The selected physical fingerprint was
`5c29cf646706c8c8ba84000150211a6b`; the 90-row result was valid. The search
state was `QualityPolicySatisfied + SearchIncomplete`, not `ProofComplete`.
The diagnostic actual search stop was 53.846 ms, quality policy was satisfied
at 52.746 ms, freeze was 2.068 ms, and compiler return was 70.014 ms.

## Interpretation

This pilot is consistent with a small B3 reduction, but it does not prove a
causal end-to-end improvement: the clean baseline and this run are different
cohorts, and the source was dirty. The code path is a policy-neutral structural
optimization: it reuses the existing post-order native shell when the
reachability proof already establishes that representation, and computes the
root output layout during the one required compaction pass. It preserves the
owned semantic peer where that is required for Memo lineage and proof
obligations.

The measured optimizer remains about 54--57 ms, above the intermediate
compiler target of 30 ms. The next safe target is the remaining repeated
PredicateTransfer native/owned construction and statistics refresh, but it
must be measured against a clean fixed-work replay before implementation. The
previous broad direct-only PredicateTransfer experiment is a registered
negative result and must not be repeated without a new semantic proof.

## Tests and omitted work

Passed for the implementation: `cargo check --locked -p paro-optimizer`,
native join preaggregation tests (3), native join subsumption tests (4), and
the staging tests (8). The full optimizer library run remains 1,234 passed and
5 known baseline failures; those failures were not blessed or changed. Full
SQL regress, formal 36-block W/C1 campaigns, default handoff activation, and
ProofComplete were not run in this pilot.

## Artifacts

### Subsequent bridge migration status (implementation, not a new performance sample)

The following commits extend native construction after the pilot above:
`f1ccd481` (CTE), `15d6e4b1` (non-null aggregate input), `5fcb8e57`
(join elimination), `2221a8f2` / `8f6a8438` (late-payload prefix), `27d155f2`
(post-reduction), and `7bbe67a2` (scalar aggregate window). The pilot binary
does not contain these commits; its times cannot establish their benefit.

The scalar-window slice resolves the finite direct detail/scalar source
grammar in the same Memo. Singleton lookups, including failures, subscribe to
logical frontier revisions; expansion and construction consume work units.
The expanded boundary snapshot supplies the actual cost-independent fact
identity. Native output must satisfy the target group schema: canonical
carrier templates do not authorize dropping an observable scalar output.

Validation in the mixed workspace: five scalar-native tests pass (production
binding/staging, output contract rejection, child-fact invalidation and budget
retry, and an independent nullable/duplicate/negative-value bag interpreter
over the produced native shell). Four existing scalar-reuse/Q22 tests pass.
The transformation suite before the additional bag test reports 103 passed,
one failure: `engine_admits_every_partition_discriminator_from_one_binding`
rejects `expected grant is not a declared class` at `cte.rs:582`. It was not
blessed. These are correctness checks, not clean-source performance evidence.

`3c1e04bb` removes the duplicate owned JoinElimination peer after complete
native success. The production-binding regression first failed with two
staged outputs instead of one, then passed after the dispatch change. This
is a staged-output count, not an inserted-expression or performance result.
The native implementation declines unknown descendants rather than claiming
partial traversal is complete. Six native tests (including a left/right,
unique/nonunique, observable/unobservable reference matrix), five existing
join-elimination tests, and 111 engine tests passed. The engine run includes
source-sensitive RF ordering, budget retry, invalidation, and cancellation/
rollback coverage. No new Q11, admitted-fingerprint comparison, or full SQL
regression was run for this commit; tests used the mixed working tree.

`9bf11f9f` extends scalar-window native production through selected
Projection/Filter ancestors. A real scoped matcher binding failed native
production before this change and now stages one native result, preserving
the reordered projection and residual filter. Ancestor expressions are
checked against each rewritten child layout; an ancestor reading the removed
scalar column is rejected even if its output type remains unchanged. Copied
ancestor proof lineage is cleared. The nullable/duplicate/negative-value bag
oracle now also executes the produced reordered projection. Seven native
scalar tests, four existing scalar-window tests and 111 engine tests passed.
No clean-source Q11 or fingerprint comparison was run for this slice.

`3533703a` extends the native scalar-window path through selected semi/anti
reduction carriers on either preserved side. A production matcher regression
failed native construction before this change and now stages one native
result for each of Semi/Anti/RightSemi/RightAnti. Window and scalar residual
are installed before reduction, over the original filtered source; the
carrier's non-preserved input is retained as its native edge and the outer
projection is composed at the carrier output. Copied carrier proofs are
cleared. The independent bag interpreter executes the actual native shell
against a separate gate bag with duplicate and NULL keys, testing all four
directions with and without a reordered projection. Its data distinguishes
SUM-before-reduction from the incorrect SUM-after-reduction placement.
Budget/retry and fact invalidation tests now include carrier inputs. Eight
native tests, four scalar-window tests, and 111 engine tests passed on the
mixed worktree. No performance, clean-source fingerprint, or SQL-regress
acceptance is inferred. Expanded non-preserved subtrees which could contain
other rewrites remain outside this producer's completeness claim.

`b289ef99` removes the owned fallback for AggregateNonNullInput, including
negative native results. Its selected matcher grammar is closed (Aggregate
over Filter/Order/TopN/Limit to Get), with no nested aggregate to rewrite.
A test-only per-thread bridge audit first observed one owned instantiation
for a nullable rejection, then zero after the change. The same test updates
the source to non-NULL, verifies an old fact read is stale, and successfully
stages the native rewrite without an owned instantiation. The audit has no
production code or tracing overhead. Two native tests, four non-null-input
semantic tests, and 111 engine tests passed. The broader transformation run
reported 108 passed/1 failed: the previously recorded CTE test still rejects
`expected grant is not a declared class`; it was not modified or blessed.
These mixed-tree correctness results are not clean performance evidence.

Audit clarification: legacy instantiation does not select an arbitrary
alternative behind a PatternOperand::Group either; it preserves a typed
hole. Alternative-rich groups therefore do not by themselves justify an
owned fallback. Remaining migration must compare the *selected* binding's
rewrite coverage rather than assume the owned path explores extra choices.

`7cff4944` removes the Memo owned fallbacks for TopNIntroduction and
LimitPushdown, including native rejection. Their selected scopes cannot
contain an additional optimizable LIMIT below the matched rewrite: the
pushdown projection ends at a hole and TopN follows its selected Order chain.
The production-binding audit first failed on negative LIMIT with one owned
instantiation. After migration, all ten signed LIMIT/OFFSET cases retain
their expected zero/one outputs with zero owned instantiations. Positive
staged payloads retain exact limit, offset, ascending and NULL-order flags;
pushdown retains its Projection over the newly staged Limit. The existing
limit suite (21 tests) and engine suite (111 tests) passed. No guard, budget,
cost model or stopping policy was changed. This is mixed-worktree correctness
evidence, not a new Q11 performance or fingerprint acceptance campaign.

`45b76b73` adds native JoinElimination traversal through DISTINCT with explicit
comparison-column requirements. A real Projection/Distinct/Projection binding
first reached one owned instantiation; after the change it stages one native
result with zero bridge calls. Ordinary DISTINCT requires all child columns,
not only those requested by its parent. This contract was also corrected in
the legacy reference so a native negative cannot fall back to the weaker
requirement. DISTINCT ON retains its explicit target and order expressions.
The reference/native matrix checks ordinary DISTINCT, left-only DISTINCT ON,
and right-observing DISTINCT ON. Seven native, five legacy join-elimination,
and 111 engine tests passed. This includes a conservative semantic contract
correction, so no unchanged candidate-count or performance claim is made.
Window/control/other unhandled descendants still prevent declaring the entire
JoinElimination path native-only. No Q11 or full SQL regress was run here.

`6e0d86bb` adds native Window dependency traversal for JoinElimination. The
production wrapped binding first required one owned instantiation and now
stages its native elimination without a bridge. A second failing regression
showed the legacy rule could eliminate a side still referenced by a retained
Window invocation merely because its output was unselected. Both paths now
retain dependencies of every invocation they leave in the plan, including
arguments, partitions, ordering and frame expressions traversed by the common
expression visitor. No future expression-pruning pass is assumed. Eight
native tests, five reference join-elimination tests and 111 engine tests
passed. Other unhandled/control descendants and negative-result completeness
still prevent removing the entire JoinElimination fallback. This contract
correction has no new Q11/fingerprint or clean performance acceptance yet.

`2fd6b1b9` adds native JoinElimination traversal through SetOperation using
each branch's complete output contract, rather than matching the parent's
new binding namespace against child bindings. The production UNION wrapper
first recorded one owned instantiation and now records zero with one native
staged result. A six-case matrix (UNION/INTERSECT/EXCEPT, ALL/DISTINCT) verifies
that both selected branches are rewritten, their distinct binding namespaces
remain intact, output layout matches the reference, and set flags are
unchanged. Nine native tests, five reference tests and 111 engine tests
passed. Control and other unhandled descendants still need coverage before
the rule's negative fallback can be removed. No Q11 performance or clean
fingerprint acceptance was run for this slice.

`0e74592b` extends native JoinElimination requirements through EmptyResult,
DependentJoin and the remaining full-child-contract wrappers (Explain,
CopyTo/Delete/Insert/GraphExpand), plus Update expression dependencies.
The production EmptyResult wrapper first required one owned instantiation
and now produces its native result with zero bridges. Reference comparisons
cover EmptyResult, Explain and scalar DependentJoin output contracts; this
does not claim end-to-end DML/graph execution coverage. Ten native tests,
five reference tests and 111 engine tests passed. CTE/control exclusion is
unchanged, and unknown descendants still cannot be treated as complete native
negative results. No Q11, full SQL regression or clean-source performance
campaign was run for this implementation slice.

`a6e7c721` distinguishes JoinElimination's complete selected-shell NoRewrite
from Unsupported. A complete negative now returns without owned construction;
control exclusions, unknown traversal and unrepresentable root/output changes
still retain their fallback. The absent-unique-key regression first observed
one bridge. Ten positive/negative cases across five wrappers now observe zero;
each negative case also updates the unique-key evidence, checks that a prior
read is stale, and successfully retries natively. A separate root-replacement
case verifies Unsupported cannot become a complete negative. Eleven native
tests and 111 engine tests passed. NoRewrite means only this selected rule
binding was checked, not ProofComplete or global search closure. No Q11 or
clean-source performance/fingerprint evidence was collected for this slice.

`d208b6ce` permits native JoinElimination to retain opaque control inputs when
their exact boundary identities and edge multiplicities survive unchanged.
Explicit CTE/recursive/reference nodes remain unsupported. A shell regression
checks a retained control input and rejects removal even when two occurrences
share the same transport identity; a set-only test would miss that case.
Boundary payloads are not rewritten. Twelve native tests, five reference tests
and 111 engine tests passed on the mixed worktree. This regression exercises
the native rewrite core, not an end-to-end Memo/CTE execution fixture; production
CTE and clean Q11/fingerprint acceptance remain unverified. No performance
claim follows from permitting this previously rejected native shape.

`2a0e2b3f` adds native MaterializedCTE traversal: producer requirements retain
the full definition output, while consumer requirements remain in the consumer
namespace. CTERef leaves are retained without construction. The production
Memo/binding/apply/staging regression initially observed one owned bridge;
it now observes zero on both a complete negative and a positive, including
retry after unique-key fact invalidation. Its consumer contains a real CTERef;
the test verifies retained producer column mapping, reference count and the
consumer's unchanged statistics-dependency fingerprint after native staging.
Twelve native, five reference and 111 engine tests passed on the mixed tree.
This is not a full SQL/recursive-CTE or frozen-execution acceptance campaign.
RecursiveCTE still declines this native producer, and Q11 performance and
clean-source fingerprint equivalence have not been rerun for this slice.

Native recursive-wrapper traversal now retains both arms' full positional
contracts without unfolding, demand narrowing, or producer rebinding. The
production-binding matrix first observed an owned instantiation through the
recursive wrapper; after migration its positive, negative and fact-update
retry paths use zero bridges. Staged payload checks retain the recursive
symbol, UNION ALL flag, declared type and self-reference table identity.
Twelve native tests (including fourteen wrapper/uniqueness cases), five
reference tests and 111 engine tests passed on the mixed tree. The recursive
fixture tests planning/staging, not termination or execution of recursion.
No SQL-regress, Q11 timing or clean-source fingerprint campaign was run.

RowFetch is now an explicit selected-rewrite barrier for native JoinElimination,
matching the reference rule's existing refusal to traverse late materialization.
The reference/native regression first returned Unsupported instead of a complete
negative; the production binding first performed one owned instantiation. Both
now retain the subtree without a bridge, including retry after unique-key facts
change. This does not remove a previously legal rewrite or certify global search
completion. Thirteen native tests, five reference tests and 111 engine tests
passed on the mixed tree; no performance/fingerprint or full SQL run was made.

ExternalProject and ExternalTable now share the explicit non-traversal contract
with RowFetch. The reference rule already leaves these boundaries untouched;
native rejection no longer constructs an owned tree just to rediscover that
negative. The external-table fixture includes lateral and parameterized input.
Both the reference/native barrier check and production bridge audit failed
before the change. Thirteen native tests (twenty wrapper/uniqueness cases),
five reference and 111 engine tests now pass, including fact-update retries.
These synthetic external fixtures validate planning, not external execution.
No budget/model/policy changes or new Q11 performance evidence are claimed.

Memo MarkJoinToSemi is now native-only for its closed MarkConsumer grammar
(Projection/Filter over one MARK pair, whose two children are holes). The old
owned rewrite is test-only. Conversion retains both exact input groups, so an
opaque control input no longer forces fallback. A four-case production matrix
checks local versus correlated marker depth, with/without a materialized control
input, against the owned reference. The negative initially performed one bridge;
all cases now use zero, and staged SEMI flags and exact input groups are checked.
The new test, existing frozen-selected MARK production test and 111 engine tests
pass on the mixed tree. No SQL execution, clean fixed-work fingerprint or Q11
performance campaign was run for this change. Other rule bridges remain.

Audit correction for the earlier native-only LIMIT change: the native producers
still rejected an opaque controlled input, although the reference LIMIT/TopN
rewrite operates above that input and retains it. With fallback removed this
was a missing legal output, not a valid complete negative. A production test
reproduced zero outputs where the reference produced one. Both native guards
now admit the unchanged opaque input; no LIMIT crosses the control boundary.
Twenty count/offset/rule/control cases compare reference outputs, require zero
bridges, and verify the exact retained input group. The matrix, 21 limit tests
and 111 engine tests passed on the mixed worktree. This restores missing search
coverage, so unchanged publication counts are NOT claimed against the defective
version. Q11/fingerprint and clean performance acceptance remain unrun.

KeyDomainTransfer audit found a correctness mismatch before removing fallback:
the owned rule checks every local probe expression for an evaluation fence,
while native code checked only Filter/Order. A production Projection containing
an unused-by-key random() published one native output where the reference
correctly rejected it. Native now uses the existing borrowed expression visitor
for the complete local operator, also covering aggregate and join expressions;
duplicated Filter/Order checks were removed. The deterministic/volatile
production regression, existing frozen-selected key-transfer test and 111
engine tests pass. The unsafe candidate removal is an intentional semantic
correction, not an unchanged-work performance result. The rule still retains
owned fallback pending the rest of its coverage audit. No Q11/SQL campaign ran.

Memo KeyDomainTransfer now uses its native local-shell implementation for both
positive and negative outcomes; the owned transfer module is compiled only for
tests. Opaque controlled inputs are retained, not traversed. A four-case
projection/control/volatile matrix first observed an owned negative retry and
now observes zero bridges, while matching reference output presence and exact
restricted-input/source groups. The matrix, existing frozen-selected key-domain
test, 111 engine tests and optimizer compilation passed on the mixed tree.
This does not certify every non-default join payload or all probe-family SQL
semantics; broader contract and fixed-work/Q11 acceptance remain required.
No timing or unchanged-fingerprint claim is inferred from removing this entry.

Key-domain output audit distinguished canonical operator layout from the target
group's ColumnId set. Incoming physical projection maps are normalized, so the
initial idea of copying their ordinals was rejected. Nevertheless a subset
target really lost its alternative at staging: the native probe retained full
output while target metadata requested fewer columns. The rewrite now selects
the target identities using the existing empty-Filter projection contract,
without renaming columns or imposing old physical order on a new child.
Four All/subset/reordered/zero-column cases verify canonical template maps,
native target binding sets and successful zero-bridge production staging.
The two key-domain tests, existing frozen-selected production test and 111
engine tests pass. This restores missing output coverage; it is not a claim of
unchanged counts against the prior version. No Q11/SQL performance run was made.

AggregateJoinPreaggregation now returns native positive/negative results without
the Memo owned fallback. Its local preaggregation wraps the nullable input as a
whole, preserving control nodes instead of traversing their opaque children.
The production matrix covers equality/non-equality and a materialized right
input; the negative first recorded one owned instantiation, then zero. Three
native tests, five reference preaggregation tests and 111 engine tests passed.

Full optimizer checks immediately before and after this slice both reported
1271 passed / 5 failed on the mixed worktree. Exact unresolved failures:
`nary_sharing_plan_is_stable_across_default_budget_envelope`,
`mark_join_to_semi_is_an_explicit_isolatable_transformation`, and
`engine_admits_every_partition_discriminator_from_one_binding` reject an
undeclared expected grant before transformation search;
`statistics_read_cache_revalidates_registry_rollback_reinsert_and_merge` expects
the fingerprint to differ after merging equivalent producer groups (Memo-only
fixture); `nested_filters_share_one_ordered_source_work_lane` sees one rather
than two retentions in a fixed physical witness. None was fixed or blessed.
This proves no new failure in this slice, not a clean historical-baseline
attribution or full migration acceptance. No Q11/SQL performance campaign ran.

### Native subsumption through tautological filters (f3b500be)

Production Memo binding previously fell through to owned settlement when an
otherwise supported subsumption spine began with Filter(TRUE). The native
spine now removes only a projection-free, evaluation-fence-free filter which
the existing shared predicate normalizer proves empty. FALSE, typed NULL and
output-projecting filters remain refused by this producer. The positive
production fixture checks one output, zero owned binding instantiations and
no settlement arena growth, both with and without the wrapper. A differential
fixture compares output layout and outer grouping/SUM expressions with the
owned FilterPushdown plus root subsumption reference; it is not an execution
or full search-coverage oracle.

Validation on the mixed worktree: native subsumption5, reference subsumption5,
engine111 passed. The new test initially failed compilation (typed NULL and
reference-copy/equality API usage); corrected before commit. No fresh Q11,
fingerprint, SQL regression or performance campaign ran for this slice.
The remaining fallback includes a complete FilterPushdown prelude, not merely
the subsumption rule. This change does not establish native coverage of that
prelude, and does not delete the fallback prematurely. Existing user changes
were excluded from the commit.

Follow-up: the same production fixture reproduced another owned fallback when
Filter(TRUE) wrapped the outer detail Get (one owned instantiation instead of
zero). Native subsumption now resolves identity-filter edges across the visible
post-order shell in one pass, replacing the earlier join-spine-only handling.
Operator payloads remain in place; group holes are not expanded. Changed
ancestors lose their old source proofs, and final compaction/layout validation
still runs. Nonempty predicates and projection/evaluation barriers are not
routed by this pass. Production cases cover no wrapper, a join-spine wrapper,
a detail-scan wrapper and three nested detail-scan wrappers, all with one
output and zero owned binding instantiations/settlement arena growth.

Follow-up validation: native5/reference5/engine111 pass; full optimizer1272 pass,
5 fail. The failures match the recorded grant declaration (3), statistics cache
(1) and ordered RF retention (1) cases, including the same failure locations
and observed values. They remain unresolved and unblessed. This does not prove
complete FilterPushdown coverage, runtime semantics or current Q11 speed; no
performance or SQL campaign ran. Nonempty predicate routing in the subsumption
prelude and other production bridges remain to be migrated.

### Independently materializable inputs (6ce046b0)

Audit found an additional avoidable bridge: AggregateInputMaterialization's
native path rejected the entire binding when any otherwise eligible scalar
could not be placed below the root join. The semantic producer instead retains
each successful placement and leaves rejected inputs at their original site.
The native loop now follows that behavior, removing the redundant eager
candidate collection/deduplication and all-input placement gate. A successful
binding remap still clears the rejection set and retries dependent expressions.

A new independent production Memo fixture has one narrowing total input whose
raw column is also a join key (must stay) and another on the opposite input
(may move). Both candidate orders are checked against the owned reference,
including preserved rejected expression, output types, one materialized input,
one output, zero owned instantiations and no settlement arena growth. The test
uses a test-only infallible scalar contract; it is not a runtime arithmetic or
bag-semantics oracle. The initial fixture mistakenly used scalar aggregation
and mixed in the separate plain-grouping gate; after correcting it to grouped
aggregation, the exact HEAD production function was restored temporarily and
the corrected test reproduced the owned bridge (1 vs0). Restoring the new
implementation makes both orders pass.

Validation: materialization12/engine111 pass; full optimizer1273 pass/5 fail,
with the same three grant-declaration errors, statistics-cache assertion and
RF-retention assertion recorded above. No failures were blessed. No Q11,
fixed-work/fingerprint or SQL runtime campaign was run. Richer placement paths
and unsupported aggregate roots remain outside this native subset.

Subsumption's nonempty-predicate prelude remains open: the existing shared
domain closure handles inner joins, not the required SEMI boundary routing.
It cannot be substituted for the complete FilterPushdown prelude unchanged.
No separate predicate semantics were introduced to bypass that gap.

### Deep native materialization placement

A production-selected nested-join fixture demonstrated a coverage mismatch
even on native success: the reference placed a4-column projection on a3-column
input, whereas native placed a7-column projection on the complete6-column
intermediate join. Zero owned invocations was therefore not sufficient proof
of the migrated placement contract.

Native placement now inspects the exact selected inner-join spine before
allocating its projection. It stops when the next join consumes a candidate
input, when the candidate spans both sides, or at a non-inner/opaque boundary.
It then remaps the recorded ancestor chain bottom-up, including the new scalar
at its actual ordinal in each ancestor input rather than reusing the leaf's
ordinal. Original candidate liveness and total-evaluation checks are unchanged;
there is no owned tree reconstruction or child-frontier expansion.

Eight production cases cover target left/right at inner and outer joins,
with/without an inner join condition blocking further descent. They compare
native/reference projection width (4 when movable,7 when blocked), validate
native layouts, and pass actual binding application/staging with one output
and no owned bridge. These are structural tests with the same test-only total
scalar fixture above, not SQL execution or an independent bag oracle.

Validation: materialization13 and engine111 pass; full optimizer1274 pass/5
fail, with the same recorded grant/cache/RF failures. No failures were blessed.
The change repairs placement coverage and may change selected plans; no claim
of identical Q11 fingerprint or improved timing is made. Fresh fixed-work,
runtime semantics and performance acceptance remain outstanding. Unsupported
aggregate roots/control ownership and other producers still retain fallbacks.

### Aggregate grouping and hidden reduction contracts (98688429, 07e3469b)

The native materialization gate unnecessarily required a nonempty plain
grouping domain and rejected post-reduction annotations. Production fixtures
reproduced owned fallback for scalar aggregation and then for a valid hidden
post-reduction. Materialization leaves the input bag, grouping-set ordinals,
GROUPING outputs and aggregate output namespace unchanged; only independently
proven total/narrowing input expressions are moved and rebound.

The existing post-reduction verifier is now generic over child ownership
(98688429), with its scalar checks unchanged. Native materialization validates
that same contract before and after rewriting (07e3469b), without an owned
adapter. Tests cover plain groups, scalar aggregation, grouping sets including
the empty set plus GROUPING(), and a hidden MAX reduction, under both orders
of movable/rejected inputs. They compare output types, grouping metadata and
the entire hidden reduction contract against the reference, retain the rejected
expression and require one output/no owned instantiation/no arena growth.

Validation: materialization13, planner post-reduction4 and engine111 pass.
Full optimizer1274 pass/5 fail at the same recorded assertions; none blessed.
No SQL bag execution, fixed-work fingerprint or Q11 performance experiment
ran. Control ownership and complete negative-path coverage still prevent a
claim that this rule's owned fallback can be removed wholesale.

### Native-only selected input materialization

Production control-owner fixture: an aggregate input is a materialized CTE
with a real producer and CTERef consumer. The owned reference wraps the owner
without crossing it; the previous native blanket control check caused one
owned instantiation. Native placement now does the same, retaining the exact
owner group beneath its projection. The test checks that group identity and
its local statistics fingerprint, one output and zero owned instantiations.

Four negative fixtures cover join-live inputs and outer-join barriers, with
and without that owner. The reference produces no rewrite; native rejection
now returns directly rather than rebuilding the binding as owned IR. The
selected AggregateRegion grammar exposes the root aggregate and its join/
projection spine; nested aggregate inputs terminate at holes and are separately
scheduled Memo problems. The production owned switch arm is removed, and the
old tree traversal and its ownership helpers are test-only. Shared scalar
totality/liveness predicates remain production code. This is a migration of
the selected binding contract, not a proof that all Memo search is complete.

Validation: materialization15 pass, non-test optimizer and compiler checks
pass; full optimizer1276 pass/5 fail at the same recorded grant/cache/RF
assertions. Tests and evidence are from the mixed workspace, not a clean
performance build. No runtime bag oracle, full fixed-work replay, fingerprint
or fresh Q11 campaign was added in this slice. Those acceptance gates remain
outstanding for the overall migration; no speed or parity claim is made.

### Direct CTE dimension deferral (1d33286a)

The owned DimensionDeferral recognizer permits Get or CTERef dimensions; native
preflight and apply permitted only Get and blanket-rejected control references.
A production Memo fixture with a real enclosing materialized producer and
CTERef dimension reproduced one owned binding instantiation. The direct native
path now accepts the same CTERef dimension and moves that exact input once to
the final join. It neither inlines the reference nor enters/rebuilds its owner.

The test pairs Get and CTERef dimensions with the owned reference, checks one
output, output types, zero owned instantiations/no arena growth, and verifies
that the final join consumes the original dimension group with an unchanged
local statistics fingerprint. No uniqueness assumption or fixed operator-count
quality policy was added. Projection inlining and widest-dimension join-region
isolation are still missing from the native path, so the general owned fallback
has not been deleted or declared redundant.

Validation: the new two-case production test, dimension_deferral5 and engine111
pass. Full optimizer1277 pass/5 fail at the same recorded grant/cache/RF
assertions. No SQL runtime bag oracle, clean fixed-work replay, Q11 fingerprint
or performance campaign was run; no speed or parity claim follows from these
structural tests. User mixed changes were excluded from the commit.

The bridge migration remains incomplete. `apply_binding` still reaches
`instantiate_bound_plan_with_group_holes`, `rewrite_planner_expressions`,
`NativeShell::from_owned`, and settlement when a native producer misses.
PredicateTransfer and LatePayloadFetch also retain owned peers for uncovered
semantics. Scalar-window support is currently limited to the direct finite
grammar with Projection/Filter ancestors and reduction carriers; alternative-rich
source groups and expanded non-preserved subtrees still require migration. Merely adding a native adapter for
each rule does not remove those bridges or prove search coverage equivalence.
Remaining acceptance includes deleting these production fallbacks after
coverage tests, a clean fixed-work comparison, admitted fingerprint and
child-choice equivalence, and fresh Q11/C1 evidence. None of those gates is
inferred from the tests above.

- `pilot-v1.json.gz`: complete harness report
- `pilot-v1.ledger.jsonl.gz`: additive B0--B12 ledger
- `pilot-v1.q11.block000.parod.log.gz` and `pilot-v1.q11.block001.parod.log.gz`:
  normal server logs
- `pilot-v1.q11.diagnostic000.parod.log.gz`: diagnostic server log
- `pilot-v1.q11.oracle.parod.log.gz`: oracle server log

SHA-256:

```text
40f92dc4bf203bb59fb76225666ae67d2618f51bffd3d3338737bfba144962e3  pilot-v1.json.gz
cf248fb3dd549d2a168544a3a7cee1b58489b36a8cabb080c4808b592c29531f  pilot-v1.ledger.jsonl.gz
fbd8df511533f4aa49cfb0cfa354c18f50d4bd5337847b66f1223c9976229b12  pilot-v1.q11.block000.parod.log.gz
09a65c3564d248c9743255719ea3fee4089c7e5b6cd470f834eae0a88bb56a91  pilot-v1.q11.block001.parod.log.gz
d20bd718509752da18af98b06d2018bba7e35f7d2b695cc1a26ba00d885f5dbb  pilot-v1.q11.diagnostic000.parod.log.gz
38800222cdeab7c12348c168c38414924c546b23f1c773dabdd94e9ea539b10e  pilot-v1.q11.oracle.parod.log.gz
```

## DimensionDeferral projection-spine migration (9e870df2)

The new production Memo/scoped-binding/apply/staging regression reproduces an
owned binding instantiation for an eligible Aggregate -> nonidentity Projection
-> Join before the fix (one bridge, expected zero). One- and two-projection
spines now use the same ownership-generic scalar substitution helper as the
reference rule. No tree export is needed: native discovery traverses the exact
visible projection children, then the existing direct-join rewrite consumes
expanded grouping and aggregate expressions. Existing mobility/merge checks
remain in place. Multiway dimension isolation and unsupported cases still use
the old fallback; this is not completion of all DimensionDeferral bridges.

The production tests assert one output, zero owned binding instantiations, no
staging arena growth, output types, the exact original dimension grouping
column, and the original fact SUM input and partial grouping key after staging.
The reference owned rule independently accepts both fixture shapes. This is a
structural comparison, not an independent execution/bag oracle.

`cargo test --locked -p paro-optimizer --lib --quiet`: 1278 passed, 5 failed.
The five failure names and assertions match the preceding recorded run: three
undeclared expected-grant errors, the statistics merge fingerprint assertion,
and nested source-work retention length 1 versus 2. None was fixed or blessed.
The strengthened projection tests and the existing direct CTE tests pass in
that full run. Tests use the mixed worktree, not clean performance evidence.
No fresh Q11, C1/W, SQL regress, complete child-choice/fingerprint comparison,
or performance gain is claimed by this slice. Budgets/model/policy unchanged.

## Selected native dimension-region isolation (850885b6 / a4800132)

The reference widest-payload selection (including table-identity tie-break),
plain inner-equi eligibility, boundary predicate membership, and comparison
orientation now have ownership-independent entry points. Native region isolation
uses these exact decisions over selected child edges instead of exporting a
whole owned tree. Fact relations are reconnected in the reference traversal
order; all fact predicates must be assigned. Group holes stay opaque. Constrained
or output-restricted joins are not flattened. An already isolated two-relation
region returns its original node without rebuilding it.

Production fixtures now include three relations, the dimension nested below
the other join, two projection wrappers, and exchanged root children. Tests
check zero owned binding instantiations/no staging arena growth, exact fact SUM
and grouping columns, final dimension column and output types, plus the retained
fact-side join and its operand orientation. The reference rule accepts each
fixture independently. Two targeted tests (multiple fixture variants) pass.
The full optimizer run before adding the exchanged-child variant was 1278/5;
the five failures and assertions match the preceding entry. The exchanged-child
variant subsequently passed the targeted run. No failures were blessed.

This does not yet remove the general DimensionDeferral fallback: authoritative
negative results, unsupported enforcement/output layouts, and broader selected
region coverage need their own audit. Nor is this an independent bag execution
oracle, exhaustive search-equivalence proof, or clean Q11 performance result.
No current C1/W, fingerprint, or fixed-work acceptance is claimed. All mixed
user files and previously staged evidence were excluded from these commits.

## Deferral dimension-key evaluation boundary (af32bcd9)

The fallback-removal audit found a real native/reference contract mismatch:
native deferral checked only the fact-side join key for evaluation mobility;
the reference checks both keys. A production Memo/scoped-binding test with a
test-bound volatile dimension expression reproduced a native output where the
reference returned unchanged. Both comparison orientations now reject that
rewrite after checking both keys. Existing positive native fixtures still pass.
This corrects candidate generation; it is not a claim of identical counts on
queries that previously exposed this unsupported rewrite.

The first attempted counterexample used CanError alone and did not establish
a barrier: the existing can_share_evaluation/is_reorder_fence contract does not
equate fallibility with volatility. That hypothesis was rejected, not used to
broaden production guards. The regression explicitly tests Volatile metadata;
it is not an execution oracle for arithmetic or a change to error semantics.

Targeted native_deferral_tests: 3 pass. Full optimizer: 1279 pass / 5 fail, with
the same five assertion identities/values as the preceding records. No bless,
no fresh Q11, no clean fixed-work or fingerprint evidence in this slice.
General fallback removal remains pending, including unique-key no-reduction
guard parity and authoritative negative coverage; this correction is necessary
before treating native rejection as final.

## Deferral existing-key rejection

The native producer lacked the reference no-further-key-reduction guard. A
production Memo fixture now uses an actual plain aggregate as the fact input:
one grouping key is completely covered by the proposed partial key, while a
two-column grouping key is not. With the incomplete native guard, the covered
case still emitted a rewrite although the reference with its structural facts
declined it. The completed guard rejects only full coverage and retains the
uncovered-key alternative; both production paths instantiate zero owned bindings.

The guard reads the unchanged selected edge's observed group keys (not a peer
group) and reuses the existing structural key derivation for a visible plain
Aggregate. This is necessary because the test's selected boundary transport
did not carry that structural key. New fact joins created by region isolation
do not inherit an original group's uniqueness certificate. Covered-key rejection
is authoritative and bypasses owned fallback, which otherwise loses the proof.
No persistent cache or new key semantics were introduced.

Fixture corrections are explicit: a fabricated group-hole BoundReference cannot
be imported through MemoBuilder; manually attached ExpressionGet statistics also
do not establish production boundary uniqueness. Neither was accepted as proof.
The final fixture derives uniqueness from real aggregate structure and compares
complete versus incomplete grouping-key coverage.

Full optimizer: 1280 passed / the same 5 failures and assertion values. No bless.
No fresh Q11/C1/W or clean fixed-work/fingerprint campaign was run. Candidate
counts on formerly over-rewritten inputs can change; no global count-equivalence
claim is made. General deferral fallback and the full owned-IR migration remain
unfinished; the existing mixed user changes were not included in this slice.

## Covered deferral recognition refusals

After region isolation, exact fact/dimension scopes and identity output layouts
are established, semantic recognition refusals now bypass owned rebuilding.
This includes key mobility, group-domain/payload checks and partial-merge
eligibility. Construction and final layout validation are explicitly outside
that authoritative interval; unsupported shapes still use the general fallback.

The volatile dimension-key fixture reproduced one owned binding instantiation
before this change despite returning no output. Both comparison orientations
now instantiate zero. New production fixtures independently confirm the owned
reference rejects DISTINCT aggregation and absence of dimension grouping payload;
native application likewise produces nothing with zero bridges and no staging
arena growth. Existing positive region/projection/key-coverage tests still pass.

Full optimizer: 1281 passed, the same 5 named failures/assertion values. No bless.
This is a transport-removal slice, not deletion of every DimensionDeferral
fallback. No clean Q11, fixed-work/fingerprint equivalence or C1/W measurements
were run; no performance gain or complete-search equivalence is inferred.

## Deferral ingress audit: two proposed gaps rejected

Production tests show Left Join rejection already happens without owned
instantiation via the earlier structural-impossibility guard. Adding another
native preflight refusal did not remove work and was withdrawn.

Input join output maps that retain only each side's payload column likewise
already reach native deferral without owned transport. Memo ingestion invokes
semantic_plan::canonicalize_projection_maps; tests assert both stored Inner
Join maps are All, then verify zero bridges, exact retained dimension group,
unchanged dimension statistics fingerprint and final output types for both Get
and materialized-CTE reference fixtures. A proposed map-admission relaxation was
withdrawn: it did not explain a real production fallback.

Only tests change in this slice. All 5 native_deferral_tests pass (multiple
fixture variants). Full optimizer and fresh performance were not rerun; the
previous 1281/5 result is not relabeled as a new run. No bridge-count reduction
or speedup is claimed. Remaining fallback audit must focus on genuinely reachable
enforcement/region or construction failures, not these canonicalized ingress
shapes. The overall migration is still incomplete.

## Deferral build-side constraint boundary

A reachable legacy fallback violation was reproduced: a root join constrained
to Left became Either after owned region isolation and deferral. Both the shared
inner-equi region eligibility and reference recognizer now treat non-Either
joins as opaque boundaries. Native root preflight rejects these bindings
authoritatively instead of handing them to the constraint-erasing fallback.

This does not forbid an unconstrained outer rewrite over a constrained fact
component. New fixtures with Get and materialized-CTE dimensions confirm that
component remains the identical original fact group below partial aggregation,
retains its Right build constraint, and uses zero owned binding instantiations.
Root Left and Right constraints are both checked, with no output, no bridges,
and no staging arena growth. No rule budget/model/stop-policy change was made;
formerly invalid constraint-crossing outputs are intentionally no longer emitted.

Full optimizer: 1282 passed / same five failure names and assertion values.
No bless, fresh Q11, fixed-work/fingerprint campaign or execution/bag oracle in
this slice. This closes a concrete fallback contract violation, not the entire
owned-IR migration. User mixed changes were excluded from the commit.

## DimensionDeferral production fallback removed

DimensionDeferral is now native-only in Memo search. Its production owned
dispatch is unreachable, and the owned recognizer, traversal, tree construction
and region rebuilding compile only under cfg(test). Shared scalar substitution,
dimension selection and condition contracts remain production code. The temporary
partial-authority flag is removed. A final root-layout mismatch is an explicit
internal contract error rather than a silent owned retry.

The selected-grammar audit covers aggregate eligibility, projection substitution,
Get/CTERef dimensions, connected inner-equi isolation, opaque group holes and
constrained subregions, both-key mobility, grouping domains, key coverage,
non-distinct/merge eligibility and exact final binding/type identity. Memo
canonicalizes join projection maps; missing expressions/layouts/arity are errors,
not permission to expand another group alternative. This is removal of this
rule's transport implementation, not proof of optimal model-cost search closure.

`cargo check --locked -p paro-optimizer -p paro-compiler --quiet` passes without
the owned implementation. Full optimizer: 1282 pass / same 5 assertion failures.
After cfg/import formatting, all 6 native deferral tests pass (multiple fixtures).
No baseline was blessed. Unit/check runs are from the mixed worktree and are not
clean performance evidence. No fresh Q11, fixed-work/admitted-prefix replay,
fingerprint or independent bag-execution campaign was run. Other rules still
retain owned bridges and those global acceptance gates remain open.

## LatePayload native prefix transport through unary paths

The production binding fixture reproduced a native-prefix fallback behind
Limit on the original producer. Native transport now follows Filter/Order/Limit
ancestors to the witnessed Filter/Get, propagates the derived column through
projection maps, and verifies the original root binding/type layout. The ASCII
membership scalar witness is shared with the existing rule instead of copied.

Testing the actual apply_binding entry exposed another bridge: even a successful
native prefix was followed by the owned peer. It is now skipped only when every
root output was replaced by a derived prefix (no stored payload output remains
for row-id lowering). Mixed stored-column outputs retain that peer. This is not
removal of the full LatePayload owned implementation. Join/window/TopN and row-id
paths still require further migration.

The production test covers direct, Limit, Order, Filter and mixed-output cases.
Pure-prefix fixtures now return one output with zero owned binding constructions,
instead of two outputs and one construction; mixed output still returns two and
performs one owned construction. This is an explicit local publication change,
not a claim that global search counts or admitted prefixes are unchanged.

Both targeted tests pass. Optimizer/compiler cargo check passes. Full optimizer:
1282 passed / the same five failure names and assertion values recorded above.
No bless, SQL regress, independent bag execution, fixed-work/prefix replay,
fingerprint gate or fresh Q11 campaign was run. These checks use the mixed
worktree and are not clean performance evidence. User changes are excluded from
the implementation commit. End-to-end acceptance and overall migration remain
incomplete.

## Shared unary prefix contract: Window, TopN and EmptyResult

LatePayload native binding transport now covers the remaining unary operators
supported by the existing prefix witness: Window, TopN and EmptyResult. Owned
proof/construction and native construction use ownership-generic unary child and
projection-map accessors. Projection and Aggregate are not admitted by this
contract. Join prefix transport and row-id fetching remain owned work.

The real Memo/matcher/apply_binding test now exercises eight fixtures, including
the three new unary wrappers; all pure-prefix cases produce one output without
owned instantiation. Mixed stored payload still takes the peer path. This is a
selected-grammar transport check, not independent execution or search closure.

A direct reference counterexample exposed a TopN contract defect: its explicit
projection map hid the newly appended prefix that the root output referenced.
Temporarily restoring HEAD's original append_prefix_through_operator while
keeping the counterexample reproduced the missing-output assertion failure.
The restored new implementation updates TopN's map through the shared accessor.
Normal Memo ingress may canonicalize this map, so this counterexample is not
claimed as a measured Q11 failure or performance bottleneck.

All 19 late_payload-filtered tests pass. Full optimizer: 1283 pass / the same
five failures (grant declarations, statistics merge fingerprint, nested source
lane); optimizer/compiler cargo check passes. No bless or unrelated failure fix.
No SQL regress, independent bag execution, clean fixed-work/fingerprint campaign
or fresh Q11 measurement in this slice. Mixed-worktree tests do not establish
performance. Overall bridge migration and its end-to-end gates remain open.

## Native prefix transport through selected Join paths

Prefix transport now follows the unique selected source through Comparison,
Any and Cross joins. It preserves the peer child and updates only the source
side's projection map, deriving each ancestor layout from both exact child
layouts before checking the final root layout. No owned tree is exported for
these successful pure-prefix paths. Mixed payload outputs still require the
existing subsequent row-id lowering and retain that fallback.

Source occurrence counts are computed once bottom-up over the native shell,
saturating at two. Opaque group boundaries fail closed: a layout alone is not
proof that another source occurrence is absent. A repeated edge to the same
native node counts twice; the production-fixture-derived test checks this rather
than deduplicating by node ID. No new Memo alternative is selected to prove it.

The real Memo/matching/apply fixture suite now includes both source sides for
all three join representations (Comparison and Any use Inner), alongside the
eight prior fixtures. Pure-prefix cases return one output with zero owned
constructions. Full optimizer: 1283 passed / same five failure names and values.
Optimizer/compiler check passes. No baseline bless or unrelated edits committed.

This does not establish outer-join bag semantics, all row-id lowering contracts,
fixed-work search equivalence, admitted fingerprint or fresh Q11 performance.
Those acceptance gates were not run. Unit/check results are from the mixed
worktree; no speedup or completed migration is claimed.

## Correction: mixed Projection prefix outputs do not chain row-id lowering

The earlier reason for retaining the mixed-output owned peer was too broad.
TopN row-preserving lowering can retain a derived prefix while fetching another
stored output, as its existing regression test demonstrates. Ordinary Projection
selective lowering uses a different contract: any referenced derived scan column
rejects that proof as SelectiveInvalidColumn. Prefix lowering has already placed
such a column in its root output; both other row-id proofs require a TopN root.

A new test executes the actual prefix-then-rowid sequence for the mixed fixture,
supplies cardinality so it cannot stop at MissingCardinality, and verifies the
explicit SelectiveInvalidColumn reason with no row-id rewrite. Successful native
prefix results now skip their owned peer regardless of other stored outputs.
The temporary prefix-only-complete flag is removed. The mixed production fixture
changes from two outputs/one owned construction to one output/zero constructions.
Non-prefix and TopN row-id lowering still retain their existing implementation.

All 20 late_payload tests pass, including the existing TopN derived-prefix
positive case. Full optimizer: 1284 passed / same five assertion failures.
Optimizer/compiler check passes. No bless. Outer-join independent bag execution,
fixed-work/prefix/fingerprint gates, SQL regress and fresh Q11 were not run;
mixed-tree unit checks are not performance evidence. Overall migration is open.

## Shared prefix output selection removes an overly strict native fallback

The native output loop used to reject the whole binding if one eligible-looking
substring lacked a matching predicate; the existing tree proof retained that
expression and rewrote other witnessed outputs. A production fixture with a
witnessed width-two prefix and unwitnessed width-one prefix now uses native
transport, leaving the latter function unchanged. Restoring only HEAD's old
native proof reproduced the failure to take the native path with this fixture;
the new shared proof passes through actual Memo/matching/apply with zero owned
instantiations and one output.

Both adapters now use the same output eligibility/selection contract: known
nonmatching outputs remain unchanged, missing evidence declines the candidate,
and conflicting witnessed source/width choices decline rather than silently
discarding one. Focused tests cover these three states and repeated compatible
outputs. Source/path evidence remains adapter-specific; native mismatched-source
paths still fall back, so this is not yet complete prefix-rule parity.

All 21 late_payload tests pass; full optimizer 1285 passed / same five failures.
Optimizer/compiler check passed before the final test-only addition and moving
an unused production import into the test module. No bless. No fresh Q11,
fixed-work/fingerprint, SQL regress or independent bag-execution campaign was
run. These mixed-tree checks do not establish end-to-end performance or completion.

## Source-specific native prefix witnesses

Native prefix selection no longer chooses the first substring's source before
running the shared output proof. Each referenced source now supplies its own
exact selected path, with a unique occurrence count and either a Filter/Get
witness location or a known unfiltered Get. Unfiltered means a known negative;
opaque/unsupported means missing evidence. Paths are reused only inside the
current immutable binding invocation, once per source, with no persistent cache
or new invalidation protocol. Only the accepted source is constructed/changed.

A production fixture puts an unfiltered peer's substring before the witnessed
source's output. The tree reference and native result retain the first function
and replace the second output; real Memo/matching/apply emits one result with
zero owned constructions. This fixture would be rejected by the previous
first-source algorithm by inspection; that old version was not rerun for this
slice, so no runtime baseline or measured bridge reduction is asserted.

All five native_late_payload tests pass (16 production fixtures). Full optimizer
1285 pass / same five failure names and assertion values. Optimizer/compiler
check passes. No bless, unrelated edits, SQL regress, independent bag execution,
fixed-work/fingerprint campaign or fresh Q11 evidence. Row-id lowering and other
owned rule paths remain; this is not a completion or performance claim.

## Ownership-generic row-id proof, ahead of native RowFetch construction

The existing row-id structural proof now accepts selected operators, a child
resolver and exact source occurrence counts. The owned caller delegates to this
same algorithm; Join kind/side policy and the whitelist are unchanged. Prefix
transport is deliberately not reused as row-id legality: TopN/Projection remain
barriers, and Any join policy differs between RowPreserving and NonNull.

Tests resolve actual native shell children from the production prefix fixtures
and check both policies with Inner/Left/Right/full-outer comparison joins on both
source sides. NULL-extended source sides are refused. Existing late_payload
tests still exercise the owned entry. All 21 pass; full optimizer 1285 pass /
same five failures. Optimizer/compiler check passes. No bless. This step removes
owned ownership from the proof algorithm, not yet from RowFetch construction or
its production call chain. No bridge-count or timing improvement is asserted.

Read-only independent audit confirms the next implementation slice: ordinary
Projection selective native construction before both TopN variants. Preserve
child cardinality max versus source expected, derived-column rejection and
SelectiveJoinLocality; create rowid -> internal carrier -> RowFetch -> exact
original output in native staging. Savepoints restore visible planner state,
not the shared atomic table-index allocator; rollback tests must not assert
allocator rewind. No files or benchmarks were changed by the auditing agent.

SQL regress, independent bag execution, fixed-work/fingerprint and fresh Q11
gates remain unrun for this slice. The mixed-tree checks do not constitute
end-to-end acceptance. Overall migration remains incomplete.

## Ordinary Projection native RowFetch construction

When prefix selection produces no result, the same selected native shell now
feeds ordinary selective row-fetch lowering. The shared row-id proof checks its
unary path; post-join fetch and TopN remain excluded. Guard ordering keeps all
symbol allocation after semantic/type/stored-column checks and the existing
cost model's child-max/source-expected reduction/benefit test. No new cost model
or weaker row-id policy was added.

Construction appends the virtual rowid, exposes it through exact unary maps,
creates an internal carrier and RowFetch, rebases the original output and checks
the final binding/type layout. Affected lineage is cleared; unchanged nodes keep
their proofs. Successful results enter existing native staging and avoid the
owned peer. Rejected/unsupported cases still fall back; ordinary Projection is
not yet declared entirely native-only, and both TopN constructions remain owned.

Production Memo/matching/apply fixtures cover single and repeated payload output,
compare needed catalog columns with the tree reference, and verify one result
with no owned instantiation. Raising only the selected input cardinality upper
bound to source cardinality refuses the rewrite. Explicit rollback after actual
publication restores Memo group, column, scalar, binding and logical-payload
counts. This checks visible state, not shared allocator rewind.

All 21 late_payload tests pass; full optimizer 1285 pass / same five failures.
Optimizer/compiler check passes. Independent read-only review found no definite
missing guard in this unary success path; it called out ordinary Window-output
carriers, projected wrappers and existing-rowid coverage still needed. Explicit
rollback is tested, not all cancellation/staging-rejection schedules. No bless,
SQL regress, independent bag execution, fixed-work/fingerprint or fresh Q11
campaign. Tests are mixed-worktree evidence only. No timing gain or complete
owned-IR migration is claimed.

## Native selective carrier coverage

Three further production fixtures exercise ordinary Window output, an existing
virtual rowid, and explicit Filter/Order projection maps over that rowid. The
Window output retains its original binding in internal carrier slot zero; the
root references the carrier namespace while fetched payload references the
materialized namespace. Carrier visibility and BigInt types are checked.
Existing rowid is reused (one VirtualRowId source, no output-width growth).

The projected-wrapper fixture confirms Memo ingress canonicalizes Filter/Order
maps to All; the producer correctly handles that actual selected representation.
No production correction was needed. These cases pass tree-reference needed-
column checks, actual apply with zero owned constructions, the cardinality-upper
negative check, and visible-state rollback checks. They are transport/transaction
coverage, not a bag-execution or performance oracle.

All 21 late_payload tests pass (21 production fixtures). Full optimizer remains
1285 pass / same five failures. Only tests changed. No bless, fresh Q11, SQL
regress, fixed-work/fingerprint campaign or new compiler timing in this slice.
Reject-path authority and both TopN constructions remain migration work.

## Shared ordinary selective admission

Owned and native selective fetch now call one admission algorithm over output
expressions, child cardinality, exact Get lookup, row-id path proof and source
statistics. This removes the native copy of column/type/storage checks,
reduction and benefit logic. The shared implementation also owns bounded
rejection reasons. The native adapter still supplies a concrete unary source;
it does not pretend an unsupported path is a fully handled no-rewrite result.

All 21 late_payload tests pass, including the 21 Memo/apply fixtures and upper-
bound/rollback checks. Full optimizer 1285 pass / same five failures; optimizer
and compiler check pass. No budget, cost constants, rule-set or stop-policy
change. No bless. This is contract consolidation ahead of reject-path bridge
removal, not a claim of fewer production bridges or a measured speedup.

The separately assigned detail-TopN implementation is an unintegrated draft;
it is excluded from this commit and these test claims. Aggregate-TopN remains
unmigrated. SQL regress, independent bag execution, fixed-work/fingerprint and
fresh Q11 acceptance remain open; mixed-tree unit runs are not clean performance
evidence.

## Detail TopN selected-native construction (b0e36aa6)

Detail TopN now constructs its ordering-payload fetch before TopN and output-
payload fetch after TopN directly from selected NativeChild nodes. It uses the
existing ownership-generic row-id proof and existing benefit model. Successful
production bindings do not instantiate an owned plan. Aggregate TopN and
unsupported/negative paths still use the owned fallback; this is not full
LatePayloadFetch migration.

The real apply/rollback test exposed a production-only output contract gap:
Memo templates canonicalize the TopN carrier map to All, while metadata still
declares only the projected outputs. The hidden-ordering fixture directly
rewrote successfully but staged zero alternatives because its root included
the hidden integer. Restoring the selected root map with the existing
`projection_for_bindings` contract fixes that rejection without weakening
staging's exact output check or importing an owned tree.

Three targeted tests cover five positive production fixtures (both fetch
frontiers, hidden ordering, derived carrier, comparison join and cross join),
zero owned instantiations on apply, visible sidecar/Memo rollback, rowid source
occurrence ambiguity, zero rows, invalid/deep order bindings, null-extension
and arbitrary-condition join fences. Non-prefix/reordered/repeated direct-shell
root maps fail closed: compact Projection output numbering cannot yet preserve
those original namespaces. Multi-source fetch ordering/fingerprint equivalence
is not established. Native and owned TopN admission still need consolidation;
positive transport tests do not prove complete equivalence of those algorithms.

Fresh mixed-worktree unit run: 1288 passed / five failed, with the same failure
names and assertions as the preceding run (three undeclared-grant errors,
statistics rollback identity, nested source lane). Optimizer check and diff
whitespace check pass. No failures blessed. No fresh Q11, SQL regress, independent
bag execution, clean fixed-work/fingerprint campaign or performance claim in
this slice. The goal remains open.

### Shared detail TopN admission follow-up

The owned adapter and selected-native adapter now call one
`prove_row_preserving_inputs` contract. Output and ordering binding guards,
derived-column handling, source/type validation, row-id evidence and both fetch
benefit decisions are no longer independently implemented in the native path.
Resolvers supply exact selected source/path/statistics evidence; missing source
facts are not guessed. This is consolidation of the existing algorithm, not
new costing or a new cache. Native negative paths still fall back, so this does
not claim all owned bridges have been removed.

Added a bounded-reason test covering zero limit, unsafe output, missing child
cardinality, missing source rows and unavailable row-id evidence. It verifies
early guards perform zero source reads and later guards preserve their specific
reason. Existing five production fixtures still apply without owned
instantiation and restore visible state on rollback. Targeted runs: 21
late_payload tests and 4 native_topn_payload tests pass. Full optimizer now
1289 pass / the same five failure names and assertions. Optimizer check passes.
No fresh Q11, full SQL regress, independent execution bag oracle, clean prefix
replay or new performance evidence. Multi-source deterministic ordering,
non-prefix root namespace preservation, aggregate TopN and negative-path
authority remain open.

### Shared aggregate TopN proof (3415b2de)

Aggregate payload proof now consumes a generic selected Projection/operator,
explicit cardinality inputs and source/path resolvers. The owned wrapper uses
that same proof; dependency choice, max-benefit ranking, plain grouping domain,
non-null rowid policy and rejection of ordering by delayed payload are unchanged.
No native aggregate construction is claimed by this commit alone.

New selected-evidence test exercises missing non-null path, absent source and
missing carrier cardinality with exact bounded rejection reasons, and the
existing aggregate-output-cardinality fallback to the carrier domain. It checks
the selected dependency and catalog-column mapping. 22 late_payload tests pass;
full optimizer 1290 pass / same five failures and assertions. Optimizer check
passes. Runs are mixed-worktree unit evidence only, not fresh performance or
independent SQL execution. The native aggregate producer is a separate pending
implementation and must pass real Memo/staging tests before its bridge can be
considered removed.

### Native aggregate TopN construction

The production LatePayloadFetch dispatch now selects the native aggregate
producer for TopN -> Projection -> Aggregate bindings. It consumes the shared
aggregate proof, appends the proven rowid, remaps nondependent group ordinals,
builds the narrow carrier and post-TopN RowFetch, and restores the selected
root output contract. No owned tree is instantiated for successful fixtures.
`Aggregate::recompute_returned_types` is now child-transport generic, so both
builders use exactly the same dependency/multiplicity invalidation method.

Five production tests cover the exact group/rowid mapping, names, limit/offset,
order flags and retained cardinality; absent projected payload omits RowFetch;
payload ordering, absent dependency, null-extended source and non-prefix root
map reject. All fixtures traverse real Memo/boundary/native construction and
actual apply/rollback (negative apply paths included). Success paths check zero
owned instantiations and rollback checks the visible sidecar/Memo counts. No
dependency was artificially inserted after Memo import. The five tests pass;
planner aggregate tests 3 pass; compiler check passes. Full optimizer 1295 pass
/ same five failure names and assertions. No bless.

Read-only comparison with owned apply found no new deterministic construction
mismatch in these covered shapes. Non-prefix/reordered final output namespaces
remain a safe native rejection, after symbol allocation; negative authority and
complete allocation rollback are not closed by this slice. Shared rowid proof
and producer tests are not independent SQL bag execution. No fresh Q11, full
SQL regress, complete fixed-work/prefix/fingerprint campaign or timing claim.
All reported tests used the mixed workspace, not clean performance evidence.
Other rules and late-payload negative paths still retain owned bridges, so the
overall migration goal remains open.

### Negative-authority audit and existing prefix reuse

9182bea0 moves ordinary selective admission before transport construction. The
shared proof now resolves exact selected source/path/cardinality inputs for all
shapes, rather than a separate unary shape guard declining before proof. Its
existing join-locality rejection still forbids post-join ordinary fetch; no new
cost proof or eligibility is introduced. Successful proof must identify one
unary source before native construction proceeds. Existing 22 late_payload
tests pass; full optimizer 1295 pass / same five failures before the next fix.

Audit found a concrete native prefix bug: if Get already has the matching
derived prefix output, its interning API reuses the existing ordinal. Native
code incorrectly required that ordinal to be a newly appended suffix. A real
Memo/apply fixture reproduced `native prefix Get appended a non-suffix output`.
9dbfd547 accepts reuse within the existing output frontier while retaining
carrier exposure and exact root validation. The new test verifies the staged
Projection references that exact existing binding, one output is produced,
owned instantiations remain zero, and rollback succeeds. 23 late_payload tests
pass; full optimizer 1296 pass / same five failures. Compiler check passes.

The audit does NOT certify blanket negative authority. A recursion-cut
PatternOperand::Group is imported by the owned path as a BoundReference, whose
source-Get count is zero; native source occurrence counting treats it as
unknown. A concrete source on one join side plus an opaque cyclic sibling can
therefore produce different uniqueness decisions. Final staging eligibility
of that hypothesized counterexample has not been tested; unknown cannot simply
be reclassified as absent. Inner carrier maps and non-prefix final maps also
remain audit obligations. No negative fallback was removed in this slice.
No fresh performance campaign, SQL bag execution or full prefix replay; mixed
worktree tests do not supply clean binary performance evidence.
