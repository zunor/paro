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
