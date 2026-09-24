# Direct staged planning slice

`SET optimizer_search_policy='pipeline'` selects a query-only planning program.
The production default is unchanged. This is a migration slice, not a second
permanent optimizer or an exhaustive-optimality claim.

## Ownership and stages

1. Canonicalize the bound query and perform required DISTINCT decomposition.
2. Commit relational normalization and derive relation statistics.
3. Compare safe root-local aggregate deferral alternatives with the shared
   physical cost kernel; discard the losing tree. Route necessary predicates
   again after a committed rewrite.
4. Enumerate each maximal legal join region once using the existing bounded
   join enumerator. Boundaries remain semantic boundaries, including outer
   joins, correlation, aggregation and shared CTE producers/consumers.
5. Choose local physical implementations bottom-up for one explicit resource
   class, then extract and verify one executable portfolio entry.

There is no Memo construction, alternative publication, subscription graph,
physical task registry or multi-grant Cartesian product on this path. Local
choices are temporary. Occurrence IDs are assigned before physical selection;
the physical canonical encoder remains the owner of cross-run identity.

`physical/implementation.rs` and `physical/local_cost.rs` own the shared
implementation eligibility and pure cost calculations. Cascades and the direct
path consume these contracts; neither duplicates the formulas. Some shared
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
slice. Aggregate alternatives may still duplicate a subtree and compare full
local subtree costs. These are visible remaining costs, not a claim of one
linear traversal or a final 2ms implementation.

## Plan-quality preservation

The existing Q74 quality plan pushes the consumer year domain into both fact
branches and uses narrow-key partial aggregation followed by original-group
final aggregation. The earlier Memo-regional experiment lost those choices.
The direct slice retains both in real EXPLAIN output; final result validation
and cold/warm measurements, not operator counts, decide their value.

Tests exercise actual planning/execution for shared dimension values, LEFT
JOIN, EXISTS, NULL-sensitive NOT IN, correlated empty aggregates, DISTINCT,
multi-consumer UNION CTEs and forced external execution. Resource tests reject
over-limit floors and preserve runtime-capped uncertainty. Broader corpus and
resource validation is required before changing the default or removing the
old engine.

Performance registration and results are owned by
`benchmark/evidence/optimizer/20260924/direct-pipeline-v1/`.
