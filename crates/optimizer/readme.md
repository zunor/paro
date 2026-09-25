# Paro optimizer

This crate turns bound statements into one verified physical plan. There is one
production program: ordered rewrites, shared estimates, bounded regional choices
and committed physical construction. There is no alternate Memo engine, policy
selector, rule-disable switch or implicit fallback to another planner.

## Architecture

```text
paro-planner: bind SQL -> typed logical plan
    rewrite::program::Normalization -> committed semantic rewrites
    estimate::annotate             -> column and relation evidence
    region::plan                   -> bounded join / aggregate-grain choices
    physical::choose               -> implementation and resource choices
    physical::lower                -> immutable executable contracts
paro-planner::physical::CompiledPhysicalPlan
    execution: admission -> executable image -> execution
```

Annotation occurs at explicit rewrite barriers, not just once before the plan
changes. Statistical expectations, proven bounds and unknown values remain
distinct. The annotation coordinator owns ordering; column, relation, join and
selectivity kernels own different equations, not independent query optimizers.

Connected-region enumeration is shared. Ordinary relation subsets and subsets
with aggregate grain are different state domains; they must not compete as
interchangeable rows. Specialized estimators borrow completed input evidence.
They must not rebuild entire plans just to reuse a cost formula. Regional work
limits bound enumeration and retain a legal fallback, not an optimality proof.

`cost` owns calibrated work formulas and ranking; `planner::physical` owns
published work/resource values, executable properties and admission contracts.
Physical construction consumes committed implementation choices rather than
choosing algorithms again. Local access-path and TopN decisions respect the
same expression, nullability and evaluation boundaries as logical rewrites.

`paro-planner` retains binding, shared expressions and symmetric `logical/` and
`physical/` modules. Execution has no production dependency on this crate. Its
dev dependency enables the explicit `test-support` fixture surface. Internal
optimizer modules are not downstream APIs.

## Source map

| Owner | Entry point | Responsibility |
| --- | --- | --- |
| Public driver | [optimizer.rs](src/optimizer.rs) | Statement split and stage orchestration |
| Rewrite | [program.rs](src/rewrite/program.rs) | One ordered normalization program and annotation barriers |
| Estimation | [annotate/](src/estimate/annotate/), [join.rs](src/estimate/join.rs), [selectivity.rs](src/estimate/selectivity.rs) | Column/relation evidence, join estimates, selectivity |
| Cost | [cost/](src/cost/), [ranking.rs](src/cost/ranking.rs) | Operator equations, source responses and candidate ranking |
| Regions | [plan.rs](src/region/plan.rs), [join/](src/region/join/), [aggregate.rs](src/region/aggregate.rs) | Legal boundaries, connected enumeration, distinct state domains |
| Physical choices | [choose.rs](src/physical/choose.rs), [access/](src/physical/access/) | Implementation, resource and access-path decisions |
| Construction | [lower/](src/physical/lower/), [finalize.rs](src/physical/finalize.rs) | Layouts, slots, concrete payloads and final verification |
| Mutation | [write.rs](src/optimizer/write.rs), [mutation.rs](src/physical/mutation.rs) | Input isolation and write contracts |
| Diagnostics | [diagnostics/](src/diagnostics/) | Stage timing and bounded typed reports |
| Shared artifacts | [planner physical/](../planner/src/physical/) | Identity, dependencies, single-plan admission and executable verification |

## Semantic and runtime contracts

- Preserve output bindings/types, NULL behavior, bag multiplicity, ordering,
  evaluation count, volatility and observable errors. A wrapper is not proof
  of safe movement across an outer join, aggregate, CTE or recursive boundary.
- Rewrites replace trees; optional cost decisions belong to regions or physical
  selection. Removing a historical rule registration does not make that rewrite
  mandatory. Independent semantic oracles remain test-only where appropriate.
- Statistics do not become facts because a rule ran. Unknown is not one row;
  a compound unique key does not imply uniqueness of any individual column.
  Filter monotonicity and join/aggregate bounds require matching semantics.
- Domain coverage and execution occurrence are different identities. RF bypass
  needs coverage of every relevant input path, not just an eligible join spec.
- Keep exact child choices, output layouts and dependencies until construction
  completes. Node ids are local references; typed canonical identity encodes
  executable semantics without Debug formatting or allocation addresses.
- A compiled artifact is one plan, not an executable image or a resource lease.
  Compilation and its cache key share the frozen resource observation. Admission
  verifies actual memory, task supply, external workers and live dependencies.
  Resource failure must not invent an unplanned executable fallback.
- Write barriers, resource feasibility, capability and dependency verification
  remain production checks. Optional expensive logical assertions are not a
  replacement for these runtime safety contracts.
- Spill and adaptive operators own runtime adaptation. Lowering may select a
  smaller DOP before freezing the artifact; admission does not silently change
  join order, algorithm or resource semantics afterward.
- Preserve error causes and cancellation, release leases on every terminal path,
  and never report an unsuccessful or unfinished phase as completed.

## Compile diagnostics

`EXPLAIN (COMPILE, FORMAT JSON)` observes the real compiler once; TEXT renders
the same typed record. Add `DETAIL` for bounded stage events; add `ANALYZE` to
execute that exact artifact and attach admission/execution receipts. Plain
COMPILE is ForcedCompile and does not populate the statement cache. Without
ANALYZE, admission/execution are NotExecuted, not zero-duration observations.

Schema v4 replaces Memo events and search-proof fields with stage work and
`Planned` / `PlannedWithFallback`. These mean a legal physical plan was produced,
with any bounded regional fallback exposed. Neither claims global optimality,
complete coverage of all SQL plans or a performance target. Missing observations
remain explicit. Consumers reject older wire versions instead of guessing.

Detail reports completed normalization, regional planning, physical selection
and physical construction stages, retaining completed prefixes if a later stage
fails. Its exclusive ledger also includes Unclassified, so measured time remains
accounted for. Timing is wall time, not CPU time; stage durations and ledger
buckets are overlapping views and must not be added together.

Normal performance samples stay trace-off. Bounded receipts associate artifact,
statement, admission and execution identities; they never infer identity from
latest/occurrence order. A missing association blocks joint attribution, not
preservation of valid timing samples. Equal fingerprints are locators, not SQL
equivalence proofs. Normal and diagnostic runs may choose different plans.

Capture is typed, capacity-limited and sealed. Enforce retained and encoded byte
limits before copying or allocating payloads; reserve terminal status and omitted
counts. Overflow still produces valid JSON and cannot change query results.
Release ownership after transport drains or the connection closes, not merely
when a sending future is dropped. Capture encoding, network drain and commit
are not free work outside a renamed optimizer timer.

CompiledArtifactReady, AdmissionReady, ExecutableImageReady and execution terminal
are distinct. Deferred image construction stays in first-statement accounting.
Observe existing decisions without replaying costing, retaining an entire plan
through a small diagnostic Arc, or adding temporary log parsers/exporters.

## Making a change

1. Identify the contract and its owning layer. Reuse the existing construction,
   annotation and resource APIs instead of adding a parallel cache or semantic authority.
2. Write a minimal counterexample or independent oracle before changing
   pruning, cardinality composition, domain transfer or completion reporting.
3. Test the real planner and SQL entry points, not only a helper with synthetic
   state that bypasses physical construction and execution.
4. Cover facts changing, shared occurrences, rollback and resource fallback
   where applicable.
5. Compare full typed query results before interpreting performance. Do not
   bless failed semantics or classify every existing failure as harmless.
6. Preserve unrelated staged, unstaged and untracked user changes. Make
   dependency-ordered, scoped commits rather than committing the whole tree.

Typical Rust checks, run from the repository root:

~~~sh
cargo fmt --all -- --check
cargo check -p paro-optimizer --all-targets --locked
cargo test -p paro-optimizer --lib --locked
cargo clippy -p paro-optimizer --all-targets --locked -- -D warnings
~~~

These commands are validation entry points, not a statement that the current
worktree passes them. Integration changes also need the affected workspace,
session/execution and SQL regression checks.

For performance work, first read the
[benchmark README](../../benchmark/README.md). When installed, also follow the
repository paro-benchmark skill, or paro-evidence for controlled comparisons;
that tooling is not a prerequisite for using the framework directly.
Use the established first-statement harness, fixed source/binary/data/SQL
identities, actual resource envelopes and complete result validation. Do not
mix competing runs on shared resources or compare a diagnostic cohort with a
normal one. Isolated output paths alone do not remove CPU/I/O or fixture
interference. Pin the competitor build, extensions and settings as well; a
parity claim does not transfer to a new competitor version. Apply the following
comparison rules before drawing conclusions.

### Comparison validity

- Declare the claim, intervention, fixed context, measurement boundary and
  independent sample unit. Unidentified comparisons are inconclusive.
- Use paired cohorts or another justified design. Do not treat a ratio of
  medians from unrelated reports as a causal speedup.
- Keep regional pruning, candidate ranking and admission decisions
  separate. Incomparable states have no rank; expected score is not the full
  resource contract. Uncalibrated costs from different facts or resource contexts
  cannot be pooled into a model-quality claim.
- Validate pruning laws with an independent small-region enumeration oracle. Measured
  dominance violations, retained work and selection regret assess model
  quality; a favorable success percentage is not a pruning proof.
- Match estimates and actuals by node, port, phase, occurrence and unit.
  Match replay requests to the actual admitted image and resources.
- Treat fingerprints as locators, not semantic equivalence proofs. Keep full
  typed results, errors and regression diffs; unchanged failing filenames are
  not evidence of unchanged failures.
- Report candidate coverage, ties, noise and repeat counts. Repeated blocks
  do not create new independent plans. Rank correlation is descriptive, not
  a substitute for pruning validity or selection regret.
- Commit a versioned preregistration record with EvidenceId before collecting
  confirmatory samples. Include numeric thresholds, exclusions, sampling,
  uncertainty and held-out rules; cite its commit/hash in the report. Known
  pilots are exploratory, and post-result amendments need new confirmation.

## Design and evidence maintenance

Keep this README short-lived-data free:

- Architecture and contract changes belong here or in a focused design.
- Decisions and important negative results belong in small indexed records.
- A campaign shares one manifest.json and README.md. An arm is a declared
  intervention/configuration, not a query, candidate or retry. A cell is a
  fixed query case and arm: store its samples/receipts in timings.json and
  refer to independently captured compile records by CaptureId. Store each
  capture once; do not duplicate common identities or conclusions per cell.
- Register finite volume limits before collection. For A arms, Q query cases,
  N cells and D Summary captures, manifest budget M is
  32,000 + 1,024*(A+Q+N+D) bytes. Per-cell budget T_i is
  4,096 + 16,384*Q_i + 1,024*S_i + 4,096*P_i + 512*R_i: Q_i counts query
  contracts (typed schemas and ordering metadata, normally one per cell),
  S_i counts scheduled timing/error
  rows including warm/retries, P_i bounds artifact/candidate receipts, and R_i
  bounds registered scalar calibration rows, never events. README is bounded
  at 20,000 bytes; each Summary at 200,000. Total registered budget
  M + 20,000 + sum(T_i) + 200,000*D must fit 64 MiB. Validate both individual
  and total uncompressed UTF-8 sizes; runtime capture limits also apply.
- Keep all sampled observations and explicit exclusions. Quota or diagnostic
  association failure cannot delete slow samples; stop further collection and
  report incomplete evidence rather than silently growing the budget. Ordinary
  99-query and candidate-calibration campaigns use the volume-based profile,
  not repeated manifests or automatic extensions. Diagnostic timings are not
  normal performance samples; raw events are not calibration rows.
- Truly larger campaigns, exact replay fixtures and correctness inputs need a
  registered bounded extension with an explicit total budget, not artificial
  arms or unlimited storage. Raw Detail traces are short-lived, quota-bound
  debugging data. Server .parod.log files do not belong in ordinary evidence
  packages; operational log retention is a separate responsibility.
- Compact history once, validate retained identities/samples/conclusions, and
  restore-test necessary evidence before deleting redundant raw data. Record
  what was deleted and cannot be reconstructed; a hash is not a backup. Do not
  move every obsolete trace into another permanent archive. Never delete the
  only reproduction of an unresolved correctness failure.
- Deleting a tracked log does not remove its Git history. History rewriting
  and destructive workspace cleanup require separate, explicit scope.
