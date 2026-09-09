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

## Executable scalar operands and refreshed evidence

`6bb666a5` shares immutable, aligned child layouts and reads only the key facet
when deriving uniqueness. It removes full boundary-statistic transport from that
path; NULL equality and column permutation/pruning are independently tested.
This structural improvement did **not** demonstrate a cold-latency speedup.

`19324b64` supplies exact, iterative identity for bound CAST kernels, including
context dependence, metadata roles and nested shared casts. `0ba087b4` replaces
digest-only scalar call operands with immutable executable descriptors. Native
construction/substitution retains bind data, aggregate modifiers, window frame
roles and intrinsic effect/error contracts; it needs no original extraction
expression. Typed substitution retains unchanged IDs, leaves correlated scopes
alone and rolls back its append delta on error. Export/substitution use cursors
rather than enqueueing a wide node's unadmitted siblings. Deep/shared-DAG tests
and deliberately colliding bind-data digests cover these contracts.

This closes a scalar prerequisite, **not** the relational-rule migration:
production transformations and physical extraction still use the operator
template adapter. Bound routine descriptors are currently copied when lowering
an owned call, so their construction is not a free operation. Search-time native
rule outputs, query-owned memory admission and the performance targets remain
unfinished.

| Report | Revision | Observation |
| --- | --- | --- |
| `native-plan-quality-scalar-operands-20260909.json` | `682613ea` | 10/10 boundaries pass; q-errors unchanged, max 2, mean 1.30833 |
| `native-q11-cold-scalar-operands-20260909.json` | `682613ea` | Five fresh processes, median 1344.03 ms; every search/settlement counter identical to `14ec9409` |
| `native-q11-execution-scalar-operands-20260909.json` | `682613ea` | 70 verified measurements per engine; Paro 100.064 ms / DuckDB 105.318 ms; paired ratio 0.971371, hierarchical 95% CI [0.943, 1.008] |
| `native-q11-cold-shared-layout-20260909.json` | `6bb666a5` | Median 1378.97 ms; all counters identical to the preceding cold report |
| `native-q11-cold-call-descriptors-20260909.json` | `0ba087b4` | Median 1475.47 ms; 1021 groups / 1833 logical / 2919 physical; 8094 settlement hits / 2880 misses; median peak RSS 564,510,720 bytes |

The execution interval still crosses one; it is not a stable DuckDB win. A
separate instrumented execution diagnostic confirms four actual workers, but its
140.36 ms duration is not a comparator sample. The new scalar identities change
bounded-search scheduling and evidence discrimination: the call-descriptor
report has 18,028 bindings and more child-frontier/composition exhaustion than
the shared-layout report. Its latency increase cannot be attributed solely to
descriptor allocation. All cold samples have zero deadline expiry and zero rule
failures, but retain `search_complete=0`; no budget constants were reduced.

`native-q11-allocation-call-descriptors-20260909.json` is a separate instrumented
build at `0ba087b4`. Memo exploration records 2,867,054,086 bytes of allocation
traffic, not peak live memory. Inclusive rule times: predicate transfer 224.85
ms / 1263 attempts, join enumeration 128.39 ms / 758, dimension sharing 89.45 ms /
67, and aggregate join subsumption 71.60 ms / 2015. These are not an isolated
before/after comparison against older profiles with different search counts.

Validation at committed `0ba087b4`: full workspace **6472 passed, 85 ignored**;
strict workspace/all-target Clippy passes. The new SQL/plan-quality/Q11 execution
checks after this commit are still pending; the earlier 184/184 SQL run and
execution comparison must not be presented as verification of the new binary.

## Native winner operands, predicate truth and candidate ownership

`1926400d` shares immutable bound routine kernels, with copy-on-write mutation
and exact semantic comparison. `d2d8c449` separates ordinary input columns from
post-aggregate reducer outputs in the typed binding catalog. Equal ordinals and
types do not alias these domains; rollback covers both namespaces and positional
references cannot escape the reducer that owns them.

`23f76ec8` moves production logical-template winner extraction onto the native
scalar DAG. A shared operand-field manifest defines import/export scopes;
comparison operands recover their left/right role from child column identity,
not fingerprint order. A poisoned legacy projection scalar no longer changes
the extracted expression. Search-provider executable payloads retain their
separate fused provider contract. **Transformation rule construction still uses
the legacy adapter**, so this is not completion of all 21 native rules.

Canonical scalar ordering exposed an EXPLAIN presentation dependency:
`native-plan-quality-native-export-20260909.json` failed one strict HAVING
selector because `SUM(...) > 100` displayed as `100 < SUM(...)`. No estimate
changed. `742ed05b` renders lone literals on the right using the exact flipped
comparison, without changing scalar identity, search or the gate. The subsequent
`native-plan-quality-native-export-display-20260909.json` passes **10/10** against
the unchanged scalar-operands baseline: maximum q-error 2, mean 1.30833. Both
reports remain available; the failed selector report was not overwritten.

The release SQL suite initially passed 183/184 with just `l.id < r.id` versus
`r.id > l.id` in the CTE EXPLAIN. `541741ab` acknowledges that single line after
independently comparing the complete SELECT result with DuckDB. During the next
predicate migration's safety probes, `WHERE x = x` was independently found to
retain NULL incorrectly. `9262cf92` preserves that truth test, unsupported AND
residuals, and typed/lexical equality domains. Its independent three-valued
integer oracle covers 27,225 bag-row evaluations. All three added SQL probes
match DuckDB; a rebuilt release on a fresh instance passes **184/184**, 40.86 s.
No failing query result was blessed.

| Report | Revision | Observation |
| --- | --- | --- |
| `native-q11-cold-shared-kernels-20260909.json` | `1926400d` | Five fresh processes, median 1442.61 ms versus 1475.47 ms; every prior search/settlement counter and plan digest matches |
| `native-q11-allocation-shared-kernels-20260909.json` | `1926400d` | Memo allocation traffic 2,865,500,578 bytes versus 2,867,054,086; this is traffic, not live peak RSS |

The small cold median difference is not a confidence-interval claim, and the
allocation reduction is only 0.054%. These measurements precede native physical
export and the predicate repair. They do not establish performance for those
new binaries. The previous execution CI still crosses one; stable Q11 victory,
the native relational-rule boundary and planning-memory admission are unfinished.

Validation: at `23f76ec8`, full workspace **6481 passed / 85 ignored**;
at `9262cf92`, optimizer **1014 passed**, strict workspace/all-target Clippy and
the fresh SQL suite above pass.

`e70279b0` admits immutable winner candidates before archiving them. Rejected
proposals never acquire a published identity; candidates evicted after
publication remain resolvable for existing parent references. The frontier and
archive share one allocation, and incremental binary insertion preserves the
same ordering/dominance contract. Two thousand duplicate proposals keep only
two previously published candidates in the reference-lifetime test. An
independent two-dimensional Pareto oracle checks all 120 insertion orders;
the existing source-sensitive child-composition and closure oracles also pass.
Proposal/publication counts are now explicit profile counters. This is an
ownership/work-complexity improvement, not a query-wide memory cap.

At `e70279b0`: full workspace **6486 passed / 85 ignored**, optimizer **1015
passed**, strict workspace/all-target Clippy passes. New cold and instrumented
allocation measurements will establish whether the storage change improves
the measured workload; no search-budget constant was changed.

Post-change evidence at `f26fa6c2` (code `e70279b0`):

- `native-q11-cold-published-winners-20260909.json`: five fresh processes,
  median **1377.81 ms**, versus 1442.61 ms in the shared-kernels report. Median
  peak RSS **348,913,664 bytes**, versus 563,888,128 bytes. The strict cold gate
  passes at `--max-ratio 1.0`; every pre-existing search, settlement and exhaustion
  counter is identical. The plan-text digest differs following native scalar
  export/presentation changes; this is not a claim of identical EXPLAIN text.
  New counters consistently record **28,440 proposals / 13,244 publications**.
- `native-q11-allocation-published-winners-20260909.json`: Memo allocation
  traffic **2,311,132,057 bytes**, versus 2,865,500,578. Rule-attempt counts are
  unchanged; this instrumented run is not the latency baseline.
- `native-q11-execution-published-winners-20260909.json`: seven fresh process
  blocks, 70 measured samples per engine, all complete **90-row** results and
  ordered keys verified. Paro median **98.9705 ms**, DuckDB **105.0055 ms**;
  paired ratio **0.943802**, hierarchical 95% CI **[0.938306, 0.949709]**. The
  report qualifies as faster than DuckDB for this four-thread, 2 GB, SF1,
  metadata-symmetric configuration. This is warmed execution, not cold end-to-end
  parity: first-statement medians are Paro **1493.322 ms** and DuckDB **107.612
  ms**. No general latency or scaling result is inferred from this comparison.

The Q11 warmed-execution target is met at that revision. Cold planning parity,
all native relational transformations and query-owned memory admission remain
open. Further implementation needs its own post-change validation.

### Input-isolation correction

`6f8695d0` caches the native input-column array on each immutable settlement
fact, lazily at the original interning point, and borrows leaf/unary reference
columns. Optimizer **1015 tests** and strict workspace/all-target Clippy pass,
including cache identity and recipe rollback assertions.
`native-q11-cold-fact-columns-20260909.json` records median 1417.53 ms, exactly
the preceding plan digest and all old search counters, plus 8856 input-column
cache hits / 2035 misses. **The comparison against the published-winners report
is not admissible**: the cold gate rejected a different dataset digest. The v5
execution comparator had started oracle and measured processes directly in the
input data directory; startup checkpoint/owner writes changed that seed between
cold experiments. These cold times therefore do not isolate the column cache's
effect, and neither a speedup nor a regression is attributed to it.

The comparator's new v6 contract and cold collector/gate v3 use a shared immutable
seed abstraction. Every process receives a separate copied directory whose
initial digest must match the declared seed. Source symlinks/special files and
logs inside the seed are refused; startup/query failure cleans only the private
copy. The seed is rechecked after each process, and execution qualification also
rechecks source, executable, harness, SQL, CSV data and the read-only DuckDB file.
114 benchmark tests cover copy isolation, source drift, cleanup, server launch
arguments and all declared evidence inputs. Old reports are retained as their
original evidence, not silently upgraded to this stronger contract. A new
same-seed/same-harness cold baseline and Q11 execution confirmation are required.

The new protocol was subsequently validated at `50a0986d`:

- `native-q11-cold-isolated-seed-20260909.json` (v3): five fresh processes,
  median **1353.844 ms**, median peak RSS **349,536,256 bytes**. The standalone
  cold gate passes. This establishes a new protocol baseline, not a measured
  improvement over a report with a different harness or input digest.
- `native-q11-execution-isolated-seed-20260909.json` (v6): complete 90-row
  results match, 70 samples per engine, seven process blocks. Paro median
  **99.3088 ms**, DuckDB **104.6223 ms**; paired ratio **0.956968**, hierarchical
  95% CI **[0.941149, 0.978082]**. Q11 warmed execution remains faster with the
  stronger isolation contract. First-statement medians remain **1507.979 ms**
  versus **108.497 ms**; cold parity is not attained.
- Both reports record the same immutable seed digest
  `b9824a5e83b0548615733a50b2ee385786bc125592be09d87fa72db0e50fd0c7`, with an
  independently verified private input copy for every server process.

### Native operator-local scalar evidence

Scalar dependencies now retain `(ColumnId, lexical depth)` rather than merging
local and correlated occurrences. Substitution rederives the scoped evidence;
physical join operand ownership consults only local columns. A correlated
invocation value is not incorrectly required to belong to either input schema.

Aggregate root dispatch reads immutable evidence published from native scalar
IDs, not executable expression payloads: plain-SUM subsumption eligibility and
total narrowing inputs with no other live uses of their raw columns. The fact
is computed once per published shell, not once per attempted rule match. One
hundred scalar/modifier/grouping combinations agree with the executable-IR
oracle; separate tests cover lexical scopes, repeated candidates, raw-input
liveness, and poisoning a legacy payload after publication. This closes two
root-dispatch consumers, **not the remaining rule output/settlement bridge**.
Derivation checks cooperative interruption before reads and collection work;
an interrupted attempt publishes no fact, never a false negative. Every possible
interruption prefix in the narrowing-evidence fixture is tested. At this change,
full workspace **6493 passed / 85 ignored**, optimizer **1022 passed**, and
strict workspace/all-target Clippy passes. New cold and SQL evidence is pending.

Post-change checks at `8e86d939` (code `d017a4b6`): the fresh release SQL suite
passes **184/184** in 44.28 s, with no expected-result changes.
`native-q11-cold-native-scalar-facts-20260909.json` records five fresh-process
samples, median **1369.585 ms** and median RSS **350,978,048 bytes**. Every search
counter and plan digest matches the isolated-seed baseline, but the strict
`--max-ratio 1.0` gate **fails**: wall/optimizer time is 1.012x and RSS is 1.004x.
The scoped-evidence repair is retained as an architectural/ownership contract;
these samples do not demonstrate a cold-planning improvement. The timing gate
was not loosened and its baseline was not replaced.
