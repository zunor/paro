# Astra Memo composition delivery

This note records the delivered contracts and reproducible evidence for the
review based on `bed732fc`. The implementation is query-independent and adds
no Q10 operator, fingerprint, or rule-priority special case.

## Review units

| Commit | Contract |
|---|---|
| `5034319b` | Immutable source-row evidence, separate domain/evaluation identities, commutative predicate costing |
| `480845c5` | Exact `PatternBinding`, explicit read sets, stable child `CandidateId`, bounded child-product completion |
| `34ba2380` | Source/streaming/breaker/build-probe task-supply contracts and phase-folded work |
| `9fe1ce8d` | Independent row-set, Cartesian-product, and task-DAG oracles |
| `502be913` | Logical-frontier, fact, and statistics reads retained for successful and no-match bindings |
| `00b1affe` | Root dispatch depends on operator shells and capabilities, never equivalence provenance |
| `16d67d43` | Complete-frontier transformations inherit an invalidatable read cursor instead of re-enumerating their output |

The SQL-regression normalization in `d469030b` is separate from these optimizer
contracts. It makes cost-equivalent join direction and equal-distance vector
ties explicit in the regression contract instead of freezing one physical
winner.

## Delivered invariants

### Source work and domain evidence

`SourceWork` owns immutable `source_rows` and `base_cost`. Runtime-filter
publication carries both `DomainProofId` and `EvaluationOccurrenceId`.
Survivor upper bounds are absolute in the original source domain and distinct
unknown-correlated proofs combine by minimum. Predicate application is divided
between lanes by original rows, then normalized and costed once per retained
evaluation. Reusing a proof cannot shrink the survivor prefix twice; retaining
a second physical evaluation still charges its work.

The independent tests cover A/B versus B/A construction, lane split/merge,
duplicate proof with distinct evaluations, exact versus unknown hard bounds,
and source-local retention.

### Memo bindings and composition

`PatternBinding` names every selected expression and every remaining group
hole. `PatternRead` records the logical-frontier revision and fingerprints of
logical facts and statistics. Completed no-match reads remain subscribed.
Zero rule budgets stop before binding expansion. A successful whole-frontier
enumeration can seed its output with the same read cursor, but any observed
revision invalidates it; proof provenance is not consulted.

Physical recipes enumerate admitted Cartesian products of child frontiers.
Omission records `BudgetLimited`, including the position at which enumeration
stopped. Winners retain immutable `CandidateId` handles in an append-only
arena, independently of frontier ordering and dominance pruning. Logical
exploration is sealed before physical optimization, so every parent sees the
complete admitted child frontier for this search epoch.

The exhaustive oracle covers non-selected child candidates, rule order,
expression insertion order, ID/fingerprint renaming, budgets 0/1/K, and the
review's 1.3 optimum. Direct tests cover monotonic revisions across rollback,
late child alternatives, facts/statistics-only invalidation, and saturated
cursor invalidation.

### Transformation migration

Every member of `PlannerTransformation::ALL` uses the same binding entry point.
The old search APIs `semantic_plan::materialize`,
`preferred_semantic_expression`, `semantic_dependencies`,
`direct_topn_input_group`, and `canonical_expression` no longer exist. The
migration inventory is in `transformation-binding-migration.md`.

The semantic-plan adapter reconstructs only the exact expressions named by a
binding and cannot inspect a group to choose a representative. Transactional
staging decomposes rewritten output into one canonical shell per Memo node;
TopN explicitly reuses the `Order` input group. This boundary adapter is also
used by CTE, aggregate, join-region, MARK-to-SEMI, late-payload, and scalar
window rewrites, so cross-rule composition is scheduled through the same
frontier/read mechanism.

### Execution phases

`TaskSupplyContract` distinguishes serial, source, streaming, breaker, and
build/probe phases. Sources derive executable tasks from pre-predicate physical
work. Streaming parents inherit `output_pipeline_tasks`; breakers alone declare
new output supply; build and probe use their respective child supplies.
Runtime-filter replacement occurs on serial-normalized source work and the
result is folded once at the lane's phase operating point.

Independent task-DAG tests cover one-worker scan to wide projection, narrow
probe with wide build, blocking merge, 1/2/4/8 task operating points, and
source-filter work conservation. The execution test
`parallel_copy_shards_preserve_the_complete_row_domain` verifies multi-worker
COPY output by reading every shard and checking the complete row multiset.

## Validation and performance evidence

The final validation commands are:

```text
make static
cargo test --workspace --no-fail-fast --locked
make -C regress check
```

Their final counts are recorded in the delivery commit message and task
summary after the commands run. On 2026-09-06, `make static` passed all six
stages; the workspace suite passed 6,286 tests with 85 ignored and no failure;
and SQL regress passed 183/183 in 45.82 seconds against a freshly built release
server and isolated data directory. The Q10 comparator uses 4 threads, 2 GiB,
binary results, one warmup per fresh process, ABBA ordering, and result
validation outside the timed region.

The last qualified clean-host report before this architecture-only series is
`benchmark/report/tpcds-q10-final-bed732fc-20260906.json`: 10 fresh process
blocks, 20 samples per engine, Paro median 11.412 ms, DuckDB median 11.772 ms,
hierarchical ratio 0.962321 with 95% CI `[0.934938, 0.988702]`, and every sample
validated.

The post-cursor smoke report at `16d67d43`
`benchmark/report/tpcds-q10-astra-cursor-smoke-20260906.json` validates every
sample and confirms that planning terminates, but is not accepted as a
performance qualification: persistent macOS indexing/media-analysis load used
roughly 1.2 CPU cores throughout the run. Under that interference its three
blocks measured Paro 23.114 ms and DuckDB 11.938 ms, ratio 1.921. A current and
`bed732fc` binary both showed approximately 20 ms Paro execution under the same
load, and the extracted Q10 physical plan was unchanged. The report is retained
as negative environmental evidence rather than relabeled as a regression or a
passing comparison.

## Mechanical removal proof

From the repository root, this command must return no matches:

```text
rg -n 'semantic_plan::materialize|preferred_semantic_expression|semantic_dependencies|direct_topn_input_group|canonical_expression' crates/optimizer/src
```

`PlannerTransformation::ALL` and the migration table are the two review points
for adding a future rule: registration without a binding/staging contract is
not a supported compatibility path.
