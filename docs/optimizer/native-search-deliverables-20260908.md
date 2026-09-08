# Native search delivery ledger

This work replaces search-time owned-IR round trips and forkable arena storage.
No search budget reduction or pre-physical top-k join-tree filter is a performance
optimization in this delivery.

## Deliverables

- [x] Reproducers and fresh-process cold planning evidence collector/gate.
- [x] Explicit CTE definition-column correspondence for all domain facts.
- [x] Single-writer session storage; alternatives hold references, not COW arenas.
- [x] Verified incumbent before optional search; cooperative time/work limits
      and cancellation have explicit completion/exit contracts.
- [ ] Query-owned planning-memory admission and bounded work inside every
      remaining legacy operation (a cooperative clock is not a hard cap).
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
| `native-q11-cold-persistent-scalars-20260909.json` | `4e217881` | 1556.5 ms | Exactly the preceding row's counters; a measured intermediate latency regression |

The fact-cache comparison isolates about 9.8% less cold latency without reducing
search. All v2 reports above have zero deadline expiry and zero rule failures,
but `search_complete=0`: finite deterministic search budgets still truncate
exploration. The evidence-algebra change increased frontier pressure; the
newer reports must **not** be blessed as a no-regression replacement for the
journal report. The shared-scalar migration reduces median cold latency a
further 5.3% without changing those counters; its cold gate passes against
`30240f8d`. The first persistent-normalizer implementation then regresses by
10.3%: its explicit stack still creates a temporary child vector per node.
The follow-up removes those vectors from normalization and the shared traversal
primitives, using one work stack and one completed-state buffer per fold instead.
That work needs a new cold measurement; the ownership contract alone is not
evidence of a performance improvement. No latency measurement above represents
revisions after `4e217881`.

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

## Subsequent evidence and contract repairs (2026-09-09)

This section supersedes the pending measurements and estimate investigation
above. It does **not** close the native-transformation or planning-memory items.

### Estimate investigation is resolved, not merely blessed

- `a78b6c35`: a Filter's refined output statistics were being reused to estimate
  that same Filter's input. Retain immutable input evidence before publishing
  the output. The five-row vector fixture now estimates two matching rows,
  agreeing with its independent two-row oracle (previously one).
- `8131f591`: aggregate-result distribution evidence disappeared at a Memo
  group boundary. Preserve the query-local numeric distribution in the value
  fact and replay the existing SUM estimate without requiring a Filter(Agg)
  representative tree. The inner post-HAVING boundary is now 3/3 estimated/
  actual rows, and the join is 4/3 (previously 1/3 for both). The final grouped
  result is 4/2: its q-error is still two, not an exact estimate.
- `e66acd6d`: EXPLAIN publishes an aggregate's fused HAVING predicate, using
  aggregate-result coordinates rather than group-key coordinates. The quality
  collector accepts either this boundary or the equivalent standalone Filter,
  and rejects missing or ambiguous matches. It cannot accidentally compare a
  pre-HAVING estimate with the post-HAVING oracle.
- `native-plan-quality-having-boundary-20260909.json`: all ten independently
  authored boundaries pass; max q-error 2, mean 1.30833, median 1. The collector
  contract changed, so this is not a comparison against an incompatible old
  collector baseline.

### Execution operating points and SQL NULL equality

`fa0e5f1d` split truly invariant algorithms from task-capacity-dependent ones;
shared Memo goals retain worker capacity. `f8ade134` supplies scan tasks from
physical storage row evidence, not optional ANALYZE metadata. The subsequent
workspace run exposed a separate search-provider extraction defect, fixed in
`cdcfe03d`: a group searched under a class-specific goal can select an invariant
provider. The node carries the selected implementation's proof, not its
sibling algorithms' search context. Enforcers use the actual grant, and
portfolio identity includes their executable admission constraints. Every node
is still verified; equal-cost class-specific enforcers cannot merge proofs.

`d51dbab8` makes NULL equality explicit on unique-key evidence. Outer joins
weaken keys on the NULL-extended side; grouping-safe keys and ordinary nullable
UNIQUE keys cannot prune one another without an equally strong domain proof.
An independent oracle checks 2,304 small join bags. `7e9bfb23` requires a schema
non-NULL guarantee for singleton lowering and replay; a NULL-free observation
is insufficient for a reusable plan. The annotation walk is iterative.

The nullable-UNIQUE SQL fixture's merge aggregate is semantically necessary
under its declared schema, not an optimization that a new fact tag alone may
erase. A NULL-aware singleton algorithm would need a lossless grouping path
for duplicate NULL tuples. The prepared-plan regression checks complete rows
before and after adding two NULLs within a transaction: exactly one new NULL
group is returned. The fixture was not changed to NOT NULL to manufacture a
plan-quality win.

### Measured progress and a rejected optimization

| Report | Revision | Observation |
| --- | --- | --- |
| `native-q11-cold-physical-supply-20260909.json` | `f8ade134` | Five fresh processes, median 1407.76 ms; 1012 groups / 1815 logical / 2894 physical |
| `native-q11-cold-limit-identity-20260909.json` | `14ec9409` | Median 1349.58 ms; 1022 / 1821 / 2900; 6308 settlement hits / 2866 misses |
| `native-q11-cold-immutable-occurrences-20260909.json` | `6d8c3dce` | Median 1367.25 ms, unchanged search counters, only 60 extra cache hits; reverted in `250a79e3` |
| `native-q11-execution-physical-supply-20260909.json` | `f8ade134` | Paro 101.071 ms / DuckDB 105.716 ms; ratio 0.967, hierarchical 95% CI [0.942, 1.011] |

The execution interval crosses one: a stable win has **not** been established.
Cold latency remains far from the requested DuckDB-level target. The limit
identity correction preserves the distinction between an absent LIMIT and an
absent OFFSET; it changes bounded-search scheduling, so its timing difference
is not evidence for a unit-cost improvement. All these cold reports retain
explicit search incompleteness, with no deadline expiry or rule failures.

The allocation-instrumented
`native-q11-allocation-physical-supply-20260909.json` at `14ec9409` records
2,606,593,471 bytes of Memo allocation traffic. Main rule elapsed times are
predicate transfer 218.8 ms, join enumeration 124.1 ms, dimension sharing
97.4 ms, subsumption 68.7 ms and dimension deferral 53.6 ms. These measurements
still identify the native rule/settlement boundary as unfinished work. The
immutable-occurrence cache was removed rather than reported as a speedup.

### Current native operand boundary and validation scope

`18c1c301` gives scalar constants an inspectable immutable leaf and exact
equality after a digest-bucket match. Scalar identity now retains correlated
column depth. Projection lineage and projection/grouping finite domains read
`ScalarExprId`/`ColumnId`, not extraction scalars; a poisoned-carrier test checks
that separation. This is a native fact consumer, **not** migration of all 21
transformations. Function/operator executable payloads and rule construction
still need a common native representation. A syntactic no-op shortcut is not
admitted while legacy settlement can discover new semantic alternatives from
changed facts.

- At `cdcfe03d`: full workspace 6,453 passed / 85 ignored; workspace/all-target
  Clippy passed, including previously failing vector and fulltext integration.
- `0e8b30bc`: reviewed SQL baselines plus the full-row transaction probe;
  rebuilt `cdcfe03d` release passes **184/184**, 44.86 s, in a new private
  instance. An earlier repeat on the same instance failed because the cursor
  fixture leaves its table behind; that data was not blessed as expected.
- At `18c1c301`: optimizer 993 passed; workspace/all-target Clippy passed.
  New post-change cold/execution measurements and a final full suite are still
  required. Do not reuse the historical performance reports as its evidence.
