# Native search delivery ledger

This work replaces search-time owned-IR round trips and forkable arena storage.
No search budget reduction or pre-physical top-k join-tree filter is a performance
optimization in this delivery.

## Deliverables

- [ ] Reproducers and fresh-process cold planning evidence collector/gate.
- [x] Explicit CTE definition-column correspondence for all domain facts.
- [x] Single-writer session storage; alternatives hold references, not COW arenas.
- [ ] Verified incumbent before optional search; time, work, memory, cancellation
      have explicit completion/exit contracts.
- [ ] Native scalar/group rule construction and incremental fact settlement.
- [ ] Post-change profiling and elimination of measured redundant work.
- [ ] Independent closure/cost oracle, SQL regress, original Q11 correctness and
      execution comparison, exact binary/harness/data provenance.

## Validation rules

Timing from a parent revision is not evidence for a changed binary. Normal
release builds establish latency; allocation-instrumented builds explain it.
Fresh processes distinguish cold planning from prepared-statement reuse. Missing
queries, failed queries, incomplete measurements, and missing provenance fail the
gate. Search incompleteness must be reported rather than erased by a faster plan.

Architecture changes may reduce duplicate nodes and work; semantic closure and
independent optimal-cost tests, not identical internal IDs, protect plan quality.
Original `SUM(x - y)` queries are never replaced by `SUM(x) - SUM(y)` for
correctness or performance comparison because NULL semantics differ.

## Status

Implementation started from clean `3ee6f28a`. The review's timing/RSS results are
external evidence, not measurements of the changes recorded here.

Single-writer storage: `LogicalPlanArena` no longer implements Clone and owns a
plain slot vector. `LogicalPlan` is an immutable borrow, and settlement/staging
exchange `PlanIndex` values. `absorb`/`adopt_or_absorb` have been removed. Seven
arena tests and 37 transformation tests pass; the shared-prefix test retains
4,096 alternative roots and verifies exactly one appended slot per alternative.
This is an ownership/work-complexity result, not a post-change Q11 timing claim.

CTE domains use `CteColumnId` and an explicit definition-to-output map on both
producers and references. Pruning/remapping preserves that map. Publication
checks schema types; mismatched advisory value statistics return no evidence.
Type lookup is indexed, and registry rollback uses insertion cursors rather
than copying all producers on each rule attempt. Seventeen CTE unit tests pass.
The new SQL regression retains `SUM(a-b)` with asymmetric NULL inputs and four
references to a UNION producer; its complete result agrees with DuckDB.
