# Native search delivery ledger

This work replaces search-time owned-IR round trips and forkable arena storage.
No search budget reduction or pre-physical top-k join-tree filter is a performance
optimization in this delivery.

## Deliverables

- [x] Reproducers and fresh-process cold planning evidence collector/gate.
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

## Verified progress through 2026-09-09

The native scalar/rule construction item is **not complete**. Production rules
still instantiate their bound operator shells into owned expressions before
rewriting and settlement. A cooperative deadline is not a hard allocation cap
or a preemption guarantee inside one legacy operation. Neither performance
target has been attained; do not treat this ledger as a completion claim.

- Mandatory physical incumbents are costed and verified before optional search.
  The engine retains exact candidate identities across frontier pruning and
  can extract the incumbent after optional deadline expiry. Cancellation takes
  precedence; entering another mandatory grant phase does not refresh the
  optional clock. Work budgets were not lowered.
- Scalar conjunction lowering visits maximal associative runs once. A 10,000
  term test checks linear interned-node growth. Executable expression nodes now
  share immutable payloads: an `Expression` is a 16-byte tag/handle, and mutation
  detaches one node without cloning its descendants. Last-owner destruction
  follows the exhaustive scalar child contract iteratively, including aggregate
  modifiers and window frames. Small-stack tests cover 10,000-level cloning,
  mutation and release, exponentially many paths through a linear shared DAG,
  and concurrent last-owner release. This does not by itself remove the native
  rule/owned-IR boundary or prove a cold-latency improvement.
- Arena rollback no longer scans unaffected settlement recipes. Search ledger
  checkpoints use a mutation journal, not whole-ledger copies. Ledger writes
  do not invalidate logical facts. Demand analysis reads borrowed child layouts
  and the operator's carrier-layout contract instead of cloning operators.
- NDV composition is associative, commutative and idempotent, with a retained
  ranking point and a separate semantic upper bound. `ColumnStatistics` no
  longer exposes `get_distinct_count`; consumers explicitly request evidence.
  Partial sketch coverage survives nested unions and rowset rebinding. A sum
  of marginal NDV points is labelled derived, not a complete observation.
- Fact-value identities now retain operand ownership and grouping evidence.
  Immutable evidence caches its fingerprint once; the exact encoding remains
  covered by a differential test.
- The graph SQL flake had an independent statement-lifecycle defect: graph
  compaction could publish a different generation between compilation,
  admission, and pipeline initialization. These phases now use one
  statement-owned pin. A new statement still invalidates an old cached plan.
  Maintenance continues to read the live provider.

### Measurements (not interchangeable baselines)

Reports are local artifacts under `benchmark/report/`. Every collector report
records the source, binary, harness, query and dataset evidence. Schema v2
requires deadline and rule-failure counters and separates allocation-instrumented
or debug-logging builds from latency evidence.

| Normal release report | Revision | Fresh-process median | Search observations |
| --- | --- | ---: | --- |
| `native-q11-cold-initial-20260908.json` (v1, diagnostic only) | `b1e33187` | 2342.3 ms | 3 optional rule failures; not a qualifying baseline |
| `native-q11-cold-journal-20260908.json` | `9e6e0c69` | 1490.2 ms | 1021 groups / 1841 logical / 2983 physical |
| `native-q11-cold-evidence-20260908.json` | `62392da9` | 1652.7 ms | 1023 / 1817 / 2906; more frontier exhaustion and RSS |
| `native-q11-cold-fact-cache-20260908.json` | `30240f8d` | 1490.4 ms | Exactly the preceding row's counts, hit/miss and exhaustion counters |
| `native-q11-cold-shared-scalars-20260909.json` | `8321589e` | 1411.7 ms | Exactly the preceding row's counters; median peak RSS 580,042,752 bytes |

The fact-cache comparison isolates about 9.8% less cold latency without reducing
search. All v2 reports above have zero deadline expiry and zero rule failures,
but `search_complete=0`: finite deterministic search budgets still truncate
exploration. The evidence-algebra change increased frontier pressure; the
newer reports must **not** be blessed as a no-regression replacement for the
journal report. The shared-scalar migration reduces median cold latency a
further 5.3% without changing those counters; its cold gate passes against
`30240f8d`. No latency measurement above represents revisions after `8321589e`.

The separately instrumented `native-q11-allocation-shared-scalars-20260909.json`
at `8321589e` records 3,641,969,536 bytes in Memo exploration. Its main rule
times are predicate transfer (217.8 ms), join-region enumeration (123.5 ms),
dimension sharing (96.9 ms) and aggregate join subsumption (68.9 ms). Do not
attribute its allocation difference from the older journal profile solely to
scalar ownership: the intervening evidence-algebra change increased physical
frontier work, and those reports have different search counts.

The allocation-instrumented `native-q11-allocation-journal-20260908.json` recorded
2,402,817,165 bytes in Memo exploration. This is allocation traffic, not peak
resident memory. The main rule times were predicate transfer (229.9 ms), join
region enumeration (138.9 ms), dimension sharing (117.4 ms), and aggregate join
subsumption (95.8 ms).

`native-q11-execution-incumbent-20260908.json` at `0fdc8b79` compares the original
SF1 Q11 against DuckDB with symmetric metadata, four threads, five independent
process blocks and three measured rounds per block. All 90 ordered result rows
match. Paro/DuckDB execution ratio is **1.169**, with hierarchical 95% CI
**[1.135, 1.187]** (Paro median 122.841 ms; DuckDB 104.273 ms). This fails the
performance target. Later changes need a new execution comparison.

### Validation scope

- Full workspace at `0fdc8b79`: 6,400 passed, 85 ignored; later changes require
  another full run.
- Full SQL regress at `842c9b99`: **184/184**, no expected-plan changes for the
  graph fix. This includes the new CTE definition-domain case. The typed
  optimizer diagnostic test was migrated from `invocation_count` to
  `metric_unit`/`metric_value`, not blindly blessed.
- `19d80c63`: 28 column-statistics tests and 966 optimizer library tests passed;
  workspace/all-target Clippy passed. Three deterministic graph lifetime tests
  cover publication between phases, next-statement revalidation and pin release.
- Shared scalar ownership migration (`8321589e`): full workspace **6,423 passed,
  85 ignored**; workspace/all-target Clippy passed. Rebuilt release SQL regress
  is **184/184**, with no expected-result updates, using the fresh private
  `/tmp/paro-shared-scalar-regress-w3MdTi/data` (45.31 s).
- Persistent scalar normalization preserves the exact input handle on no-op;
  child replacements detach only their ancestor path. It retains the existing
  top-down rule order and fixed-point semantics but uses an explicit stack for
  expressions and post-order operator visitation. The unused implicit-mutation
  `Rerun` result was removed. Optimizer library tests: **968 passed**; planner
  traversal tests: **8 passed**, including a 10,000-level small-stack check.

For SQL regress, set `ulimit -n 8192` in **both** the server shell and runner
shell. Restart-control cases inherit the runner's limit. Use the regression
configuration's `--max-memory 1073741824`; a benchmark-only `2GB` override
changes two settings assertions. The final successful run used the private
`/tmp/paro-native-regress-final-A8N4ll/data`, never the SF1 source database.

### Remaining implementation work

1. Shared executable scalar operands and native group/scalar-ID rule outputs;
   remove `instantiate_bound_plan_with_group_holes` and owned rewrite/settle
   round trips from optional search. Do not merely rename the adapter.
2. Incremental local settlement keyed by immutable operator identity and input
   facts, including the scalar-bearing cache-hit path.
3. Query-owned planning memory accounting, plus interruption checkpoints inside
   remaining large operations. A benchmark watchdog is not a server contract.
4. Locate the low Q11/cardinality estimates and the previously reviewed vector
   and aggregate estimate deviations; retain an independent plan-quality gate.
5. Final closure/oracle, fresh-process latency/RSS, SQL regress and original Q11
   execution evidence after the last implementation commit. Performance claims
   require those measurements, not the pre-change reports above.
