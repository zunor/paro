# Planner review follow-up

This follow-up records the long-lived contracts added after
`native-cte-arena-20260908.md`.  It intentionally does not reinterpret the
Q11 timing table in that document as a performance claim.

## Compact logical nodes

Large, configuration-heavy logical payloads are boxed at the
`LogicalOperator` boundary (including base-table/external scans, aggregates,
search and graph scans, graph expansion, CTE helpers, and graph matches).
Arena slots no longer reserve space for the largest inline payload.  A
compile-time size assertion keeps the enum below a 160-byte envelope with room
for small metadata additions; remaining inline payloads are intentionally
small and scan metadata is copied only at owned-IR boundaries.

`LogicalPlanArena::append` reduces layouts from borrowed child references.  It
does not clone every child layout merely to select a pass-through input.  The
owned layout API remains for binder callers; arena settlement uses the borrowed
variant.

## Anytime frontier and diagnostics

Winner frontiers have an explicit `SearchBudget::max_winner_frontier_candidates_per_goal`
bound (default 256).  Exact dominance and objective ordering happen before the
bound.  If a high-dimensional frontier exceeds it, the evicted alternatives are
represented by a `WinnerFrontier` budget obligation, so a result cannot claim a
complete search.  Child references use immutable candidate identities and are
not invalidated by frontier resorting or truncation.

Process-wide allocation instrumentation is now opt-in through the server's
`alloc-metrics` feature.  Normal servers do not install the metrics allocator,
leaving the allocator replacement seam open.  Reallocation accounting records
only the positive growth delta; a feature-enabled unit test fixes this contract.

## Identity and cache contracts

`BoundReferenceId` distinguishes input ordinals, Memo group holes, node
occurrences, and frozen outputs.  Settlement indexes input boundaries with a
checked accessor.  Synthetic `PlanNodeId`s are reminted or rejected before
entering identity-sensitive scope maps.

Settlement's pointer fast path stores `Weak<ColumnStatistics>` rather than
pinning every statistic for the session.  Local statistics participate in the
ordered cache key, removing the old linear probe.  A cheap operator-shape
pre-key avoids serializing a full recipe when no local of that shape has ever
been seen.  CTE domain proofs use a stable, flattened and idempotent
fingerprint before exact structural comparison; Memo group IDs are excluded
from this necessary-condition key.

Storage rowset/tablet aggregation carries explicit observed/total row coverage
for one-sided HLL sketches.  A surviving partial sketch remains an observed
lower-bound point and is never silently promoted to a complete-domain proof or
linearly extrapolated.  Coverage is derived planner/storage metadata and is not
added to the existing segment serialization format.

## Estimation and partition evidence

`GroupColumnDomain` retains an expected point only as a deterministic costing
rank inside its lower/upper evidence hull.  It is not presented as a refined
observation or a proof.  The existing EXPLAIN estimate differences in the
vector-filter and aggregate-subsumption baselines remain observational and
were not hidden by a selectivity constant.  Settlement and group-domain
transport also stop treating an uncertain expected row count as an NDV proof;
the cardinality upper edge is the only estimate-derived cap.  HLL provenance
is carried by `ColumnStatistics::is_storage_observation`, so join ordering no
longer guesses from whether the root happens to be a bare `Get`.

The CTE unit suite now asserts that partition strategies remain admissible and
that their domain fingerprints follow the same normalization as proof equality.
No SQL baseline is changed solely to manufacture a partitioned winner; a future
high-selectivity case must assert the selected physical strategy and result
together.
