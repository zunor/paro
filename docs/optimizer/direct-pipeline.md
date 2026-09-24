# Direct staged planning slice

`SET optimizer_search_policy='pipeline'` selects a query-only planning program.
The production default is unchanged. This is a migration slice, not a second
permanent optimizer or an exhaustive-optimality claim.

## Ownership and stages

1. Canonicalize the bound query and perform required DISTINCT decomposition.
2. Commit relational normalization and derive relation statistics.
3. Optimize bounded aggregate/join regions jointly. States are keyed by the
   relation subset and the subset at which partial states were formed (the
   latter fixes their grouping grain). Raw and partial rows are not competing
   implementations of the same relation. Join, partial and final transitions
   share local statistics propagation/gathering and `direct::select_local`.
   Reconstruct only the selected DAG, then route necessary predicates again.
4. Enumerate maximal legal join regions using the existing bounded join
   enumerator, including regions exposed by committed rewrites. Boundaries
   remain semantic boundaries, including outer
   joins, correlation, aggregation and shared CTE producers/consumers.
5. Choose local physical implementations bottom-up for one explicit resource
   class, then extract and verify one executable portfolio entry.

There is no Memo construction, alternative publication, subscription graph,
physical task registry or multi-grant Cartesian product on this path. Local
choices are temporary. Occurrence IDs are assigned before physical selection;
the physical canonical encoder remains the owner of cross-run identity.

`physical/implementation.rs` and `physical/local_cost.rs` own the shared
implementation eligibility and pure cost calculations. Cascades and the direct
path consume these contracts. `physical/join_work.rs` also owns hash work units
and their frozen calibration for regional DP: both build orientations include
retained payload width, and expected rows rank work independently of risk and
hard resource bounds. Joint aggregate regions use the full direct physical
response, including memory feasibility, task supply and RF source lineage;
ordinary join-only regions still use the older additive work enumerator.
That remaining domain must not be described as unified physical-response DP.
Some shared
identity/calibration types still live under the historical cascades namespace;
moving their module location is not required to avoid the Memo runtime.

## Safety and scope

The first slice supports relational SELECT planning, not write planning.
Unsupported write layers and Memo rule-ablation settings fail explicitly.
Required runtime contracts and physical validation remain enabled in release.
Unknown cardinalities are not hard row bounds. Memory floors cannot be clamped
to make a plan appear feasible. Spill and runtime-capped completion remain
distinct. The single class uses the session memory/task envelope; reduced-DOP
admission fallback and broad external/search-provider coverage are not yet
certified. Utility statements keep their existing direct path.

Finite program completion is not global optimality: the receipt reports
`Incomplete`, not `ProofComplete` or `QualityPolicySatisfied`. The current
receipt schema does not yet distinguish program completion from search-domain
exhaustion. Memo counters are zero; selected nodes, local alternatives and
aggregate decisions have separate pipeline counters.

Stage-end statistics are deliberately recomputed at rewrite boundaries in this
slice. Joint states share immutable child choices, output statistics and
fact-backed boundaries. Atomic inputs are priced once per region; transitions
never recurse through them. Selected output statistics are published together
with the selected tree. The final stage still derives executable contracts;
this is not a claim of one linear traversal or a final 2ms implementation.

Multi-relation residuals retain their complete relation support. They become
applicable only on a cut containing all referenced relations; their estimated
selectivity is charged once in the set cardinality, not again on each ancestor.
Raw pre-residual cardinality is retained for reconstruction and local work.
Equivalent integral OR/IN domains in one filter conjunction are intersected
by the shared estimator instead of multiplied as independent events. Neither
mechanism turns statistical estimates into semantic proofs or removes runtime
predicates.

The joint domain is explicit: at most eight atomic inputs connected by movable
inner equi-column predicates, one grouped aggregate with a registered partial
merge law, and at most one partial transition per plan. Every boundary key and
original grouping input needed above a partial transition is retained. Final
merge is mandatory, including when different dimension keys share a label or
a dimension duplicates rows. DISTINCT, ordered/non-mergeable aggregates,
non-equi cuts, outer joins and opaque projections are not given invented laws.
Unsupported regions retain their original aggregation and ordinary join DP.

One current-grant response per subset/grain is a bounded planning heuristic,
not a dominance proof under every parent or an exhaustive SQL search. The
existing connected-pair budget also bounds joint transitions; exhaustion
retains the original region and reports a fallback rather than infeasibility.
Counters separate regions, transitions, partial states, selected partials and
budget fallbacks. SQL result and execution-quality gates remain independent.

## Plan-quality preservation

The existing Q74 quality plan pushes the consumer year domain into both fact
branches and uses narrow-key partial aggregation followed by original-group
final aggregation. The earlier Memo-regional experiment lost those choices.
The direct slice can represent both, but costing chooses whether to use them;
final result validation and cold/warm measurements, not operator counts,
decide their value.

Tests exercise actual planning/execution for shared dimension values, LEFT
JOIN, EXISTS, NULL-sensitive NOT IN, correlated empty aggregates, DISTINCT,
multi-consumer UNION CTEs, cross products, partition/global windows and forced
external execution. Typed identity encoding shares the complete aggregate
payload between ordinary and partition-window operators. Resource tests reject
over-limit floors and preserve runtime-capped uncertainty. Broader corpus and
resource validation is required before changing the default or removing the
old engine.

Performance registration and results are owned by
`benchmark/evidence/optimizer/20260924/direct-pipeline-v1/`.
Shared regional work and finite-domain estimation follow-up evidence is in
`benchmark/evidence/optimizer/20260924/regional-cost-v1/`. Broad correctness,
resource and execution-quality gates, not a three-query compile pilot, own
default promotion.
