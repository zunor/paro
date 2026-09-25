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
