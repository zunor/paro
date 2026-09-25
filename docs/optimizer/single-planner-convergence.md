# Single staged planner

Implementation commit: `9f242d4d8`.

This change removes the production Cascades engine, not the executor's resource,
write-safety or cancellation contracts. The comparison baseline is the default
pipeline at `5ba483059`; the old executable is retained outside the checkout for
same-fixture validation. No expected results or performance thresholds are updated.

## Rule disposition before engine removal

Removing an optional Memo registration does not make its rewrite mandatory.
The following is a disposition of registrations, not a claim that every old
alternative is explored by the default planner.

| Registration | Owner / disposition |
| --- | --- |
| cte_inline | rewrite/cte: single-reference normalization |
| cte_demand_pushdown | rewrite/cte normalization and column demand |
| cte_filter_pushdown | rewrite/cte normalization; retain column-transfer oracle |
| cte_partitioned_materialization | retire optional Memo alternative; ordinary materialization remains |
| aggregate_post_reduction | retire Memo registration; preserve independently used rewrite laws |
| mark_join_to_semi | rewrite/subquery existence normalization |
| join_elimination | retire optional Memo pass; retain independent elimination oracle, not a new mandatory pass |
| aggregate_join_preaggregation | region aggregate-grain alternatives |
| aggregate_join_subsumption | retire optional Memo registration; do not silently add a mandatory pass |
| aggregate_non_null_input | retire optional Memo registration; retain independently used scalar laws |
| aggregate_dimension_deferral | region aggregate-grain alternatives |
| aggregate_input_materialization | retire optional Memo registration; physical materialization contracts remain |
| limit_pushdown | retire optional Memo pass; retain semantic oracle; TopN introduction remains independent |
| late_payload_fetch | retire optional Memo candidate; retain semantic oracle; committed scan payload decisions remain |
| scalar_aggregate_window | retire optional Memo registration; preserve rewrite semantic tests |
| join_region_enumeration | bounded region join enumeration |
| top_n_introduction | rewrite/limit/topn |
| aggregate_dimension_sharing | retire optional Memo registration; preserve independent semantic tests |
| predicate_transfer | rewrite/predicate and CTE normalization |
| key_domain_transfer | shared typed column/domain transfer, not a Memo witness |

## Boundaries

`paro-planner` remains the owner of binding and shared logical/physical contracts.
Execution may depend on those contracts, never on optimization algorithms.
Rewrite owns pass order; estimation owns annotation; regions own bounded choices;
physical planning commits executable contracts. Admission and spill remain fallible.

Canonical plan structure is compared separately from compiler configuration and
artifact identities: removing a search policy legitimately changes the latter.
A matching hash is a locator, not a substitute for full result/type/order checks.
The baseline comparison uses the same schema, data and fixture lifecycle.

## Implemented ownership

- One production driver, without a policy selector or a second global engine.
  Normalization ordering belongs to `rewrite::program`, regional orchestration
  to `region::plan`, annotation to `estimate::annotate`, and pricing/ranking to
  `cost`. Column, relation, selectivity and equality kernels retain their
  distinct responsibilities; concatenating them into one large estimator is
  not a unification requirement.
- Ordinary subsets and aggregate-grain states share connected-region
  enumeration. Their legality and state domains remain distinct. Moving join
  estimates/pricing under their owners does not replace proven formulas with
  new estimates merely for directory symmetry.
- `paro-planner` retains binder, expressions and symmetric logical/physical
  modules. Execution only uses shared contracts in production; fixture-only
  optimizer APIs require `test-support`. CI checks transitive production edges,
  including optional and target-specific dependencies, plus the public facade.
- `CompiledPhysicalPlan` replaces a portfolio with one verified plan and its
  grant. Compilation/cache identity use the same frozen resource observation.
  Admission checks real availability and dependencies; zero task supply and
  insufficient memory cannot masquerade as a feasible operating point.
- Mutation isolation, concrete no-spill checks, resource accounting and
  executable verification remain production contracts. Removing search proofs
  must not remove runtime safety. Dedicated mutation input barriers replace the
  generic enforcer machinery.
- Compile schema v4 publishes four stage timings and bounded stage events,
  with `Planned` / `PlannedWithFallback` instead of Memo proof claims. Real
  receipt identity, transport ownership, cancellation, capacity and execution
  terminal contracts remain. Python consumers share one schema-version owner;
  retired ablation parameters and benchmark arms are removed, not emulated.
- Responsibility-based modules replace selected/select, generic enforcers,
  misc/helpers and Memo scalar adapters. Oversized cohesive modules warn in CI;
  seven still exceed 1,500 lines (including tests and test-only semantic oracles).
  There is no blanket word ban or a new collection of tiny forwarding modules.

## Acceptance

Run affected tests, workspace check/tests, strict Clippy, dependency guards and
fresh-data SQL regress. Preserve pre-existing failures and compare actual output;
do not bless. Corpus comparisons retain all valid samples and verify complete
typed results. Performance observations are not certification without a registered
sample-size and non-inferiority policy. Report unfinished work explicitly.

## Validation and honest limits

The [bounded validation record](../../benchmark/evidence/optimizer/20260925/single-planner-v1/README.md)
records dirty-source build identities and original failures. The baseline is the
old default staged path, not a different Cascades policy.

- Workspace: 6,368 passed, 85 ignored; workspace check, strict all-target Clippy,
  release build, memory/vector guards and calibration validation passed.
  Benchmark: 219 passed. Regression harness: 104 passed, one skipped.
- Real PgWire Q04 and SELECT 1 Detail captures expose all four typed stage
  completions in producer order and pass the v4 consumer validator. These
  diagnostic durations are not performance measurements.
- TPC-DS: 98 strict type/bag/order matches; Q39 retains its strict binary64
  difference and separately passes the pre-existing independent bounded
  numerical/relational oracle on both binaries (360,000 input rows, 90,000
  groups, 243 output rows). This is not a global floating epsilon.
- TPC-H: 22 strict type/bag/order matches. All 121 plain EXPLAIN renderings are
  byte-identical. All new Summary documents validate as v4.
- Selected fingerprints are **not** equal: their existing canonical encoder
  includes `PlanDependencies`, whose optimizer configuration changed when the
  policy disappeared. The encoder was not weakened to make the comparison
  green. Rendering equality plus complete result checks is evidence, not proof
  of canonical identity equality or equivalence for every possible dataset.
- Fresh-storage SQL regress: control 164/21, probe 162/23. The two additional
  failed files expect retired Memo diagnostics. Of the 21 common failed files,
  17 actual transcripts are byte-identical; two differ only in owned fixture
  paths, and two in deliberately removed settings and their downstream EXPLAIN.
  The block audit found no other new ordinary query result difference. Raw
  snapshots remain failed; no expected files were regenerated or blessed.
- Full-tree header checking still has 148 pre-existing findings; changed/new
  files pass. Skill quick-validation could not run because PyYAML is absent;
  this is not reported as a passed check.

This delivers the architectural cleanup, not an all-green SQL snapshot suite,
the complete JOB/CEB/TPC-DS/TPC-H/LDBC correctness gate, or a performance release
certification. No latency, parity or non-inferiority claim is inferred from these
correctness runs. Historical evidence, recovery references and other worktrees
are outside this cleanup and remain untouched.
