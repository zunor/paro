# Optimizer decisions

Architecture lives in [the owning crate](../../crates/optimizer/readme.md).
This is the durable decision record, not a running experiment diary. Historical
measurements and failures are indexed in [Git history](../../benchmark/evidence/README.md).

## One staged planner

Use ordered semantic rewrites, shared estimates, bounded regional join/grain
choices, then committed physical construction. The former general search
engine's scheduling changes did not remove its fact/publication/solver costs;
changing the planning substrate did. This is a workload-driven engineering
choice, not a claim that every memo-based optimizer is slow or that distributed
planning can never need a richer property search.

There is one production path, no historical policy switch or silent fallback.
Regional limits keep a legal bounded fallback and expose it. They do not prove
global optimality. DP transitions should use compact estimates and calibrated
operator equations, not instantiate/reprice complete candidate trees.

### 2026-10-04 Memo versus staged TPC-DS comparison

The last revision with both planners, `5ba483059`, ran one binary under
`quality` (Cascades Memo) and `pipeline`, interleaved, on TPC-DS SF1 1–99 with
`tpcds_compare.py` (3 fresh blocks × 2 rounds, 4 threads, 2GB, verify off,
generator-declared keys, DuckDB 1.5.5 per cell). It was exploratory, with host load.
98/99 passed in both arms (Q39 is the known binary64 issue). Warm plan quality
was a wash: geometric mean quality/pipeline 1.044, 1.005 after trimming five
extremes per side, 31 versus 32 queries more than 10% faster. Memo compile
p50/p95/max was 19/495/28,460 ms against 3.5/13/20 ms, so C1 summed 53.5 s
against 14.5 s. HEAD `df290be7b` matched the old pipeline after DuckDB
normalization (warm 1.009, C1 1.021).

The losses have different owners. Memo losses were join orders: Q65 chose a
store×item Cartesian, Q72 and Q24 built large early intermediates. Staged
losses had identical join orders but fewer runtime filters reaching fact scans
(Q15, Q05, Q58). Improve RF placement and regional costing; do not restore
global Memo search. JOB was not covered and remains the stronger join-order
test. Reproduce through the `paro-benchmark` collector recipe at `5ba483059`.

### Runtime filters retain source lanes

A runtime filter runs at its traced rowset source, so it removes rows from
every streaming operator between that source and its owning join. A
source response therefore carries the scan decode a filter can change and
the probe, filter, projection and UNION ALL work attributed by the
source's share of each stream; build sides, cross products and breakers
end it. Join survivors and source retention derive from the same
per-source membership estimates. Filters pay to read their predicate
columns on every input row. A filter directly above a scan is its static
stage: predicates run in ascending selectivity, each reading its columns
only for earlier survivors, and deferred columns decode every vector block
that holds a survivor, not each survivor (`ScanAccessCostModel`). Scan
decode is charged at the difference between the executor's behavior with
and without the filter, never the eager scan price.

The scan owns conjunct order: it orders static and runtime conjuncts by
selectivity and storage keeps that order, so filter application is charged
per stage on the rows reaching it, not on every source row. A late scan reads
only its leading stage for every row; later filter keys are deferred like
payload. Retention and semi/anti reductions use the same containment rule
over key domains, min(ndv, rows). Execution keeps one membership set per
equality key, so a composite filter is one scan condition per key column,
each with its own domain, exactness and build/apply work; a row must pass
all of them. Treating composite keys as unestimable hid the most selective
filters (TPC-DS Q24, Q50, Q78).

### 2026-10-06 Rank by work, not by modeled overlap

Overlap between independent build sides stays out of the cost span; ranking
remains `max(work / tasks, critical path)` with additive composition. A
filter that serializes a probe source behind its build (TPC-DS Q30) can still
look cheaper than it runs; that is accepted.

Measured on TPC-DS SF1, four threads: idle that a different legal plan could
fill by overlapping independent subtrees bounds the geometric mean at about
1.008x (1.03x attributing every ambiguous interval to dependencies). Most
idle was breaker finish work (window, set-operation and aggregate
finalization) and narrow pipelines, which is executor work, not plan choice.
A span model with build/probe `max`, runtime-filter wait edges and calibrated
parallel latency was then implemented and measured: Q30 improved, but the
suite was 1.205x slower DuckDB-normalized (68 changed plans at 1.317x, Q16
6.4x, Q54 5.3x) and compile p50/p95 rose 15%/25%, because re-pricing spans
displaced filter and orientation choices whose work savings are real. Revisit
only with an executor-exported dependency graph and evidence that an
alternative legal plan, not a schedule, wins.

Cost coefficients are one calibration. Measuring only the runtime-filter
classes against the bootstrap hash-build unit (build 0.15 to 1.32, apply 0.10
to 3.69) removed almost every filter (Q17 4.0x, Q29 3.7x, Q50 3.4x slower):
the other classes, including the scan and decode work a filter saves, are
still bootstrap guesses, and the measured apply delta also contains staged
decode the source lanes already charge. Promote measured coefficients only
as a complete set validated on held-out workloads. The measurement harness
remains (`operator_runtime_dispatch`, `runtime_filter_calibration`).

Interleaved TPC-DS SF1 against `4ef08b5cc` at `2176959b5` (separate Cargo
targets, verified binaries, DuckDB per cell; exploratory, host load):
98/98 identical results, warm geometric mean 0.916 after DuckDB
normalization, 23 changed plans at 0.714 (411 versus 690 ms), unchanged
plans 0.989. Q05, Q15, Q25 and Q58 run 2–6 times faster. Q69 remains
about 1.2 times slower on a 12 ms query. Compile time is unchanged.

## Ownership and resource safety

Keep `paro-planner`: binder, expressions, logical and physical contracts remain
in one crate. `paro-optimizer` owns decisions; execution consumes shared plan
contracts without depending on optimizer algorithms. Estimates, proofs and
unknowns are different values. One estimator entry coordinates specialized
kernels; it does not require one giant estimator file.

`CompiledPhysicalPlan` is one plan plus a resource contract. Admission verifies
actual supply and dependencies rather than choosing another logical plan.
Runtime spill/adaptation remains fallible; accounting, write barriers,
cancellation, NULL/multiplicity and evaluation-error contracts stay in production.
`SubplanRef` is a planned regional input/frozen output, never an executable node.

Physical identity, graph traversal, bounded expression presentation and EXPLAIN
properties have separate owners. The verifier stays in `physical/verifier.rs`.
Mechanical moves preserve encoding domains/order and plan text; equal hashes
remain supporting evidence, not semantic equivalence proofs.

## Disposition of the twenty former registrations

Removing an optional transformation does not make it a mandatory pass.

| Registration | Current responsibility / decision |
| --- | --- |
| cte_inline | Single-reference DEFAULT normalization; explicit materialization directives preserved |
| cte_demand_pushdown | Shared CTE column demand |
| cte_filter_pushdown | CTE/domain normalization |
| cte_partitioned_materialization | Retired alternative; normal materialization remains |
| aggregate_post_reduction | Dormant transformation deleted; committed reduction lowering remains |
| mark_join_to_semi | Subquery/existence normalization |
| join_elimination | Proven unused unique base-table outer lookup only; no inferred foreign-key INNER elimination |
| aggregate_join_preaggregation | Regional aggregate-grain choices; dormant standalone pass deleted |
| aggregate_join_subsumption | Dormant alternative deleted |
| aggregate_non_null_input | Dormant transformation deleted; active scalar/aggregate laws retain their own proofs |
| aggregate_dimension_deferral | Regional aggregate-grain choices |
| aggregate_input_materialization | Dormant transformation deleted; physical materialization contracts remain |
| limit_pushdown | Constant LIMIT across infallible, reorder-safe projections |
| late_payload_fetch | Dormant generic transformation deleted; active access/scan materialization and row-fetch lowering remain |
| scalar_aggregate_window | Dormant cost-sensitive rewrite deleted; partition-window execution remains |
| join_region_enumeration | Shared bounded connected-region enumeration |
| top_n_introduction | Limit/TopN normalization |
| aggregate_dimension_sharing | Dormant alternative deleted |
| predicate_transfer | Predicate and CTE normalization |
| key_domain_transfer | Typed column/domain transfer |

Old algorithms are recoverable from Git, not shipped as uncalled test-only
optimizers. A future cost-sensitive optimization needs a bounded local decision
and counterexamples, not restoration as an unconditional historical pass.

## Validation and history policy

Regress snapshots describe reviewed current behavior. Fixture source SQL stays
stable while execution SQL expands owned paths; returned values are not scrubbed.
Missing OFFSET in TopN rendering was fixed, not erased from expected behavior.
The cleanup baseline `769ad6104` recorded 185/185 SQL regress, 22 TPC-H strict
before/after matches, 98 TPC-DS strict matches and independent bounded Q39
certification. These are dated observations, not a perpetual release certificate.

Routine diagnostics/raw samples live in ignored run directories. Git retains
short consequential decisions and maintained reproducer fixtures, not reports,
server logs or duplicate captures. Formal claims use the evidence workflow;
daily experiments do not inherit its entire certification procedure.

### 2026-09-25 maintenance acceptance

Physical-plan ownership split `1511cd903` was checked against `769ad6104` with
the same SF1 inputs, four threads, 2 GB, verification enabled, and separate
owned servers. All 121 TPC-H/TPC-DS plan texts and selected identities matched;
22 TPC-H and 98 TPC-DS complete typed results matched exactly. Q39's raw float
difference remained visible and passed the independent integer/Welford relation
contract (360,000 inputs, 90,000 groups, 243 reference rows). This is refactor
equivalence, not new DuckDB parity or closure of the oracle issues listed separately.

`cargo test --workspace --locked` passed (6,259 passed, 85 ignored); workspace
check, strict all-target Clippy, release build, fmt and header checks passed.
The actual SQL suite passed 185/185 without expected updates; regress harness
tests passed 105 with one skip. Benchmark/numeric-tool tests passed 238. The
retired first-statement F1–F7/model-registration loaders and their twelve tests
were removed with their experiment, not silently redirected to a new baseline;
the general gate/receipt/capacity validators remain. A test policy factory was
renamed so pytest no longer mistakes it for a passing test.

Historical raw evidence remains recoverable at `769ad6104`; only the index and
maintained numerical input fixtures remain in the current tree. After explicit
user approval and inventory review, seven stashes, three recovery refs, the
September 20–21 personal experiment archive, three merged optimizer branches
and the detached chain-replay worktree were discarded. Normal commit history
was not rewritten. Uncommitted experiments and disposable local run/build data
have no promised recovery; caches can be rebuilt. No new CI policy was added.
