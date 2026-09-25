# Staged planner cleanup

This follow-up to [single-planner convergence](single-planner-convergence.md)
starts at `85440a138`. It removes dormant alternative implementations, admits
two narrowly proven rewrites, and repairs the SQL regression suite. It does not
add CI checks, compatibility adapters, another optimizer policy or another
worktree. Historical evidence and recovery references are outside its scope.

## Disposition, not blanket activation

| Previous code | Decision and current owner |
| --- | --- |
| Unused outer join elimination | Production normalization, restricted to an unused base-table lookup, a complete declared unique key and ordinary local-column equality. NULL-safe equality, correlated references, incomplete composite keys and referenced outputs do not qualify. |
| LIMIT pushdown | Production normalization across infallible, reorder-safe projections only. Constant nonnegative LIMIT/OFFSET; no arbitrary size threshold. |
| Standalone late-payload rewrite | Delete the dormant implementation. It was not the current vector/full-text access selector. Active access selection, scan materialization, row-fetch lowering and their contract tests remain. |
| Partition aggregate, scalar aggregate/window fusion | Delete dormant transformations, not active partition-window lowering. Reusing detail rows versus aggregates is a cost-sensitive alternative, not a mandatory semantic pass. |
| Aggregate sharing/subsumption/post-reduction/input materialization/preaggregation/non-null alternatives | Delete dormant algorithms and tests of those algorithms. Keep production aggregate-grain enumeration, shared estimation, executable specs and lowering tests. |
| Predicate pullup and general expression matcher framework | Delete unused algorithms. Keep the small matchers consumed by production rewrites and the explicit test-support surface. |
| CTE inlining alternatives | One existing production policy: single-reference DEFAULT inline, multiple-reference DEFAULT stay shared; explicit MATERIALIZED/NOT MATERIALIZED retain their SQL contract. |
| Rejection diagnostics and old regional test adapters | Delete when no production consumer remains. Extract the shared catalog fixture into internal test support rather than retaining an optimizer as a fixture. |

Deleting an uncalled implementation does not remove an optimization from the
current production path. Nor does its historical existence prove that enabling
it is beneficial. Git retains these implementations if a future bounded local
choice warrants reimplementation. No independent old optimizer is kept under
`cfg(test)` and called an oracle.

Intermediate projections retain dependencies of every expression they still
evaluate. This pass does not remove unused expressions or suppress observable
failures; column pruning owns that separate proof. Declared key decoding is
shared with the existing estimator rather than duplicated.

## Ownership and layout

`BoundReference` becomes `SubplanRef`: immutable facts for a regional input or
frozen output. Only those two identities remain. There are no group-hole or
occurrence variants, and the reference cannot become an executable operator.
The published cost type was already `PhysicalCost`; no cosmetic second rename
or compatibility alias is introduced. Its documentation and physical-property
requirements now describe their current consumers.

Large modules are split along responsibilities: relation annotations into
join/output/graph kernels, selectivity domains into their own kernel, runtime
filter decisions out of implementation dispatch, and join reduction out of
lowering dispatch. Test modules live under their owner. A cohesive module is
not split solely to pass an arbitrary line quota. Existing formatter changes
are isolated from semantic edits. The empty local `optimizer/staged/` directory
is removed; no historical data or worktree is deleted.

## Regression adjudication

The user authorized repair, including individually reviewed expected changes.
No blanket `regress-update` or bless was used. The bounded evidence contains
the original failing block indices and SQL, old and new expected hashes, and
the final comparison. The table below records the reason for each affected
case; ordinary result differences are not excused as plan formatting.

| Case(s) | Reviewed reason |
| --- | --- |
| fulltext_exec_mode_split, fulltext_index_coverage_guard, fulltext_pg | Current provider estimates and execution shape; returned ranking/results remain tested. |
| graph_explain, graph_start_selection | Current graph start/direction and estimates. Snapshot adoption is not an optimality claim. |
| correlated_explain | Current correlated external-project/cross-input shape; result contract retained. |
| fixed_width_fast_path, imported_helper | Stable source SQL preserves `@REGRESS_FIXTURES@`; only the execution SQL expands its owned absolute path. |
| agg_join_subsumption, agg_singleton_groups, select_aggregate | Current aggregate/semi-join and DISTINCT shapes; do not pretend retired alternatives remain active. |
| cte_explain, cte_partitioned_materialization | Current CTE IDs, values cardinality and join-internal conditions. Materialization semantics and actual query results retained. |
| explain_basic | Existing TopN output plus a real renderer fix: nonzero OFFSET must be shown. Added real queries for an empty large-offset result and a nonempty offset result. |
| join_elimination | Remove retired rule-disable ablation/settings, name current stages, and test admitted outer-lookup elimination. Nonunique and referenced sides remain protected. |
| join_spill_external | Current spill-plan shape without the old RF placement. The execution/spill result test remains; no equal-performance claim is inferred. |
| rowset_scan_pushdown | Current predicate placement/layout and RF identities, not restoration of the dormant generic late-payload algorithm. |
| select_topn_fallback_spill | Show the previously omitted OFFSET 6000; do not drop it to make the display match. |
| vector_search | Current provider/access shape and estimates, with result validation retained. |
| optimizer_observability, optimizer_profile | Replace deleted rule/search diagnostics with the four actual stage names. |
| search_optimization | Current access output layout; user-visible query results unchanged. |
| statistics_query | Current RF identity and plan display. |
| transaction_settings_savepoint | Remove retired optimizer settings from SHOW ALL. Transaction assertions remain. |

Fixture handling has two explicit identities: `Block.sql` executes expanded
SQL, while `Block.transcript_sql` preserves source SQL. A regression test proves
that a returned string containing an actual path is not rewritten by this
mechanism. This fixes unstable transcripts without hiding returned data.

## Acceptance and limits

The [validation package](../../benchmark/evidence/optimizer/20260925/planner-cleanup-v1/README.md)
records exact build identities and source-diff attribution. SQL regression is
185/185 after block review, with no skipped or abnormal cases. TPC-H has 22
strict type/bag/order matches; TPC-DS has 98, with Q39 separately certified by
the existing independent integer/Welford relation contract. Its raw binary64
differences are preserved, not converted into a global floating tolerance.

Workspace Rust tests: 6,259 passed, zero failed, 85 existing ignored tests.
Workspace check, strict all-target Clippy, release build and formatting pass.
Benchmark unit tests: 241 passed; regression harness: 105 passed, one skipped.
The existing header checker passes all 106 affected source files; this is not
a claim that the historical full-repository header findings disappeared.

The five intended changed TPC-DS plans are Q28/Q32/Q38/Q97 (safe LIMIT movement)
and Q72 (unused unique outer lookup removal). The other 94 and all 22 TPC-H
plans retain canonical selected identity and plain rendering against the
same-fixture baseline. Identity equality is supporting evidence, not a proof
of SQL equivalence on every dataset.

An eight-query warm pilot retains 192 interleaved samples and their accepted
execution associations. It is **NotCertified**: external load was present and
there is no powered non-inferiority gate. In particular Q74's median ratio is
above one; it is not discarded as an inconvenient sample. No compile, C1,
parity, or formal performance improvement is claimed from this cleanup.

This closes the code-cleanup and SQL-regress task, not the full
JOB/CEB/TPC-DS/TPC-H/LDBC gate or the next execution-performance campaign.
Window execution, measured first-execution costs and dynamic range filtering
remain separate performance work, not reasons to keep dead optimizer code.
