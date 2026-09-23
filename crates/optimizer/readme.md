# Paro optimizer

This crate turns bound statements into validated physical planning artifacts.
It contains semantic normalization, Memo-based exploration, physical
implementation and costing, property enforcement, candidate verification, and
resource-aware plan portfolios.

This document is a source map and a contract guide, not a benchmark diary.
Diagnostic search paths and proposed convergence work are not automatically
the default production path. Consult an attested source manifest and the
corresponding evidence before making performance or correctness claims.

## Architecture

~~~text
Bound statement / owned binder IR
    -> semantic normalization and resident expression construction
    -> Memo groups, scalar identities, relational facts and statistics
    -> bounded logical / physical search for requirements and grant contexts
    -> exact candidate choices, verification and frozen artifacts
    -> physical portfolio
    -> resource admission and execution lowering outside this crate

Read-only diagnostics observe this flow; they must not select a plan.
~~~

The convergence target is one normalization/construction contract for both
initial expressions and newly produced alternatives. Binder-owned mutable IR
is legitimate; repeated whole-tree transport inside search is not the target
architecture. Remaining bridges must be assessed by their ownership and
semantic contract, not merely by whether a type is called “owned”.

Physical tasks retain immutable, shared ReadSet/dependency snapshots. Opening
optional implementations invalidates coverage/completion, not the exact prices
of unchanged child choices. Facts, requirements, calibration and grant contexts
still control reuse. A retained mandatory winner remains an incumbent; it does
not certify that a child's optional domain has been visited or completed.
The immutable archive and price cache survive that transition, while active
frontier membership reopens. Re-admission reuses the original candidate
allocation; it is not another pricing pass or proof of full search completion.

Frozen winners lower through immutable selected occurrences with cached output
layouts and assigned scalar slots ([selected.rs](src/physical/selected.rs)).
Local verification and positional-key replay happen once, bottom-up. Only
single-node shells with boundary references adapt existing scalar contracts;
query extraction must not rebuild a descendant-owned binder tree. Fusion and
dependency discovery share the planner's read-only input/structure interfaces.

## Source map

Paths below are relative to this crate.

| Area | Entry points | Responsibility |
| --- | --- | --- |
| Public orchestration | [lib.rs](src/lib.rs), [optimizer.rs](src/optimizer.rs) | Statement optimization, normalization, extraction and portfolio assembly |
| Query context | [context.rs](src/context.rs) | Planning configuration and statement-scoped context |
| Memo | [cascades/memo.rs](src/cascades/memo.rs) | Groups, alternatives, facts, frontiers, exact candidate identities and frozen candidates |
| Search | [cascades/engine.rs](src/cascades/engine.rs) | Implementation, composition, publication and candidate handoff |
| Tasks | [cascades/tasks.rs](src/cascades/tasks.rs) | Goal-scoped work, read sets, continuations, invalidation and lifecycle |
| Search bounds | [cascades/budget.rs](src/cascades/budget.rs), [cascades/control.rs](src/cascades/control.rs) | Work budgets, cancellation and incomplete-search reporting |
| Scalar IR | [cascades/scalar.rs](src/cascades/scalar.rs), [cascades/scalar_lowering.rs](src/cascades/scalar_lowering.rs) | Interned expressions and binding-aware import/export |
| Planner integration | [cascades/planner/mod.rs](src/cascades/planner/mod.rs), [state.rs](src/cascades/planner/state.rs) | Logical/physical payloads and planner contracts |
| Transformations | [transformation.rs](src/cascades/planner/transformation.rs), [staging.rs](src/cascades/planner/transformation/staging.rs), [settlement.rs](src/cascades/planner/transformation/settlement.rs) | Matching, semantic production, resident facts and transactional publication |
| Domain transfer | [domain_transfer.rs](src/cascades/planner/domain_transfer.rs), [quality_domain.rs](src/cascades/planner/quality_domain.rs) | Safe column/domain transport and selected-candidate evidence |
| Quality policy | [cascades/quality.rs](src/cascades/quality.rs), [quality_production.rs](src/cascades/engine/quality_production.rs) | Candidate coverage, missing requirements and policy-driven handoff |
| Specialized search | [join_order/](src/join_order/), [cte/](src/cte/), [graph/](src/graph/), [search/](src/search/) | Domain-specific normalization and candidate generation |
| Statistics | [statistics/](src/statistics/), [cost_model.rs](src/cost_model.rs) | Evidence propagation and estimation |
| Physical contracts | [physical/requirements.rs](src/physical/requirements.rs), [physical/cost.rs](src/physical/cost.rs), [physical/objective.rs](src/physical/objective.rs), [physical/resources.rs](src/physical/resources.rs) | Properties, work/cost composition, actual objective ordering and resource feasibility |
| Portfolio / extraction | [physical/portfolio.rs](src/physical/portfolio.rs), [physical/extraction/](src/physical/extraction/) | Grant variants and physical construction |
| Verification | [cascades/verifier.rs](src/cascades/verifier.rs), [physical/verifier.rs](src/physical/verifier.rs), [verify.rs](src/verify.rs) | Memo, physical and logical invariants |
| Observability | [profiler.rs](src/profiler.rs), [work_partition.rs](src/work_partition.rs), [b3.rs](src/work_partition/b3.rs), [diagnostic_snapshot.rs](src/cascades/memo/diagnostic_snapshot.rs), [compiler boundary](../compiler/src/compile.rs) | Profiling scopes, detailed attribution and bounded snapshots; actual emitters also live in optimizer.rs, cascades/engine.rs and cascades/planner/mod.rs |

Names and tables here are navigation, not a second implementation registry.
Update links when moving code; keep operator/rule registration authoritative
in code.

## Contracts to preserve

These are required invariants. The convergence plan tracks remaining
violations; listing an invariant here is not evidence that every existing path
already satisfies it.

### Semantics and facts

- Group equivalence is a semantic claim. Output bindings, types, NULL behavior,
  bag multiplicity, evaluation count, volatility and errors matter.
- A projection or wrapper is not proof that a rewrite is safe across an outer
  join, aggregate, CTE or recursive boundary.
- Local canonical laws share the postorder construction boundary in
  `construction.rs`; nonlocal substitutions and predicate routing keep their
  ordered barriers. Scalar-root reuse requires a live immutable allocation
  witness, not a stale address or an assumption that a pass ran previously.
- Relation facts and statistics have an explicit owner and revision.
  Inserting an equivalent alternative must not “vote” for a different
  cardinality using arrival order or a smaller fingerprint.
  Equivalent-publication merges retain the estimate for unchanged facts;
  transformed roots inherit their target relation's estimate. Explicit
  statistics refreshes and new constraints remain separate fact updates, and
  true group merges still combine peer uncertainty. A rule's identity or
  equivalence proof alone is not a new statistics observation.
- Observed evidence, estimated points, proven bounds and unknown values are
  different. Missing information must not silently become one row.
- Cardinality measurements must match the estimate's node, port, occurrence,
  phase and unit. Join output estimates are not estimates of build input rows.
  Estimator changes and cost/selection quality share a release gate; improving
  one scalar does not establish query-level improvement. Do not delay a
  correctness fix until full calibration, or restore wrong semantics for speed.
- Domain proofs and evaluation occurrences have different identities.
  Runtime-filter bypass requires actual coverage of all relevant input paths,
  not just an eligible join specification.

### Search and ownership

- A group alone is not a physical subproblem. Requirements, context, grants,
  objective and consumed fact revisions are part of its meaning.
- Track actual reads, including relevant negative matches. Notify consumers
  of the response or completion change they really depend on.
- One physical-subproblem owner retains published dependencies, the resident
  task cursor and pending cost/completion work for an exact `(group, goal)`.
  TaskRegistry owns lifecycle and proofs; reverse indexes route notifications.
  A redirect merges pending work but invalidates pre-merge completion.
  A current ReadSet observation borrows the immutable Memo through registry
  lookup. It is not a durable freshness bit; resident and published evidence
  still retain revision-sensitive dependencies.
- A winner retains exact child choices. Frontier pruning must not invalidate
  archived choices still referenced by a parent or frozen candidate.
- Selected quality DAGs share immutable edge lists and retain only the current
  root's transitive view. Facts invalidate affected ancestors through selected
  incoming edges; redirects also invalidate changed expression keys. Caching
  every candidate's expanded closure would recreate quadratic storage. None
  of this replaces current payload checks or final executable verification.
- Parent runtime filters, shared producers and phase composition can change
  child ordering. A local scalar winner is not always sufficient.
- Continuation pruning is a partial order, while ObjectiveProfile provides
  an ordering for selection. Incomparability must not become a tie or a proof
  that either candidate can be discarded. Budget truncation is a separate,
  explicitly incomplete decision.
- Expected score is not the complete objective ordering. Validate pruning,
  selection, feasibility and handoff separately against exact admitted plans;
  scalar correlation alone cannot certify their quality.
- A candidate estimate is not a lower bound for every legal completion.
  Bound proofs require a matching context and a valid composition law.
- Cancellation, rollback and budget rejection must leave no partial published
  contract, reused revision or fabricated complete-search state.

### Quality, resources and execution

- Keep semantic safety, optimization coverage and search completion separate.
  A rule having fired is provenance, not sufficient evidence of a selected
  plan property.
- Quality properties compose from exact selected children and live fact
  revisions. A CTE demand proof also owns its selected incoming consumer edges.
  Unchanged descendants can be reused; a changed dependency reopens its
  ancestors. Local readiness requests the complete root choice manifest only
  after applicable facts are present. It is not itself a certificate; policy
  validation and executable verification remain mandatory for handoff.
- Query statistics settle through propagation followed by one final fact
  gathering pass. Do not reintroduce a preliminary gather whose results are
  immediately replaced, or treat staging as a second construction boundary.
- A declared resource class is not necessarily feasible. Distinguish a
  verified plan, proven infeasibility, incomplete search, unsupported capability
  and an internal failure.
- Admission may execute only a verified variant that fits the actual grant.
  An unsearched class is not an executable fallback.
- A task's `NoCandidate { cursor }` response is not an infeasibility proof.
  Its existing goal, phase, ReadSet and resumable cursor govern reuse; an
  exhausted mandatory prefix does not cover optional implementations.
  `GrantSearchCoverage` retains unresolved classes, while the verified image
  remains the sole admission authority. There is currently no production
  certificate constructor for proven infeasibility or unsupported capability;
  absence must remain unknown rather than acquiring either label.
- Failed task metadata retains its supplied cause. The original `Result`
  carries SQLSTATE and cancellation; metadata must not invent a resource stop.
- Frozen candidates and eventual execution images must preserve exact
  dependencies, properties, choices and resource contracts.
- Deferred construction is not free if it is eventually requested.
  Distinguish unused variants never built from work moved to admission; count
  actual materializations in admission and first-statement accounting.

## Completion vocabulary

`optimizer_search_policy` is a validated session setting included in planning
cache identity. The SQL default, `quality`, permits handoff only after the
selected candidate satisfies the quality policy and executable verification.
`budgeted` continues optional exploration subject to the same search budgets.
Neither setting implies exhaustive search. Embedders without session settings
default to budgeted search and may explicitly supply `SearchBudget.search_policy`.
The former `PARO_QUALITY_POLICY_HANDOFF` environment switch is not an alias.

Materialized CTE necessary-domain normalization is shared by both policies,
before Memo construction. It accounts for nested producer references and uses
definition-column identities and the shared domain-transfer contract. Consumer
residuals remain in place. Quality evidence inspects the selected producer and
consumer choices, not the rule name that introduced a filter. Predicate-domain
coverage alone proves neither minimum stored width nor join-search completion.

~~~text
Semantic correctness / safety
    independent of
Optimization coverage / quality-policy satisfaction
    independent of
Completion of a declared search scope
    independent of
Trace completeness and performance certification
~~~

QualityPolicySatisfied with SearchIncomplete is a legitimate reported outcome,
not ProofComplete. Completion within a bounded join region or configured
search policy is not a proof over every possible SQL plan. A correct query
result does not by itself certify plan quality, and a fast pilot does not
certify production latency.

## Compile diagnostics

The current profiler, exclusive work ledger and Memo snapshots remain separate
producer details, but the public compile record is typed, bounded and sealed.
Later admission/execution receipts extend that record without reopening it.
The benchmark consumer uses the same versioned identities and keeps missing
associations explicit.

Detail's `optimizer_work` exposes exclusive work buckets and an orthogonal
mandatory/optional/outside phase projection. Each independently sums to the
same measured optimizer interval; adding both projections double-counts it.
Collection is fixed-size and observer-only. Normal compiler receipts, not
Detail's instrumented durations, determine performance.

Normal compilation publishes fixed summary counters, completion/quality
evidence and the compile receipt. Only explicit Detail constructs per-group
and per-frontier diagnostic rows or accounts archived source payloads. Missing
detail is not an empty frontier or zero bytes. Completed profiles transfer
ownership into their session snapshot; session inspection must not force a
second matrix construction or clone.

Preparation follows the same demand boundary: a DISTINCT feasibility fork
requires the decomposition owner's read-only eligibility proof. An eligible
fork remains mandatory even when optional search is exhausted. Physical recipes
own immutable enforcement geometry for their exact provided/required properties;
resuming a recipe reuses that geometry but still validates its live cost context,
resource feasibility and active child responses. Prepared is not complete.

The public entry point is `EXPLAIN (COMPILE, FORMAT JSON)`, with TEXT rendered
from the same typed record. Query/CTE targets support Summary and
`COMPILE, ANALYZE`; simple and known-typed extended executions use one compile
and one execution. Earlier OPTIMIZER aliases are not retained. Ordinary
EXPLAIN shows a plan; COMPILE observes its construction; ANALYZE measures the
actual execution of that sealed artifact.
The optimizer is one producer in a compiler-wide record, not the owner of
session or execution instrumentation.

The current Compile Evidence wire contract is schema v3. Physical identity is
intended to be split from admission and execution: `PlanStructureId` will be
derived from typed executable topology and payload, `CompiledArtifactId`
includes the immutable dependency contract, and `ExecutionReceiptId` belongs
to one actual admission. This crate does not yet certify that boundary because
the working physical encoder still has a Debug-derived payload fallback; that
fallback must be replaced with explicit typed binary encoders before the ID is
used as a cross-run proof. The benchmark consumer must not reconstruct compile
timing from `paro_optimizers()`, display text, arena ids or occurrence numbers.
Current readers reject older evidence schemas, and missing identity joins remain
`Uncovered` rather than being guessed.

Invalid physical identity graphs are rejected with a structured error before
artifact construction. This does not certify the identity: replacing the
remaining Debug-derived payload path with a complete typed canonical encoder is
still required before cross-run `PlanStructureId` reuse.

Summary is the default; bounded Detail is an opt-in T3 surface for supported
COMPILE targets. It retains fixed opaque references from the real Memo and
TaskRegistry lifecycle and reports overflow explicitly; it is not the complete
Trace Matrix. Without ANALYZE, the target is not executed: runtime/admission measurements are
NotExecuted, not zero. COMPILE invokes the real target compiler exactly once,
without an Explain wrapper in its Memo and without consuming or populating its
statement-plan cache. Record ForcedCompile rather than claiming a normal
SELECT cache miss. With ANALYZE, the executor admits and runs that same sealed
compiled artifact and attaches the actual execution receipt; it never compiles
twice. Unsupported syntax or statement/protocol combinations fail explicitly.

Keep the established compiler timer boundary. Parsing may precede that timer;
a compiled portfolio may still need execution-time admission and lowering.
CompiledArtifactReady, actual portfolio selection, ExecutableImageReady and
execution terminal are separate receipt states. Report expected and actual
resource classes separately, without constructing unused images to fill a
table. Sealed compile records can be linked to later execution records but
must not be rewritten to hide deferred work. The output cannot measure its
own future encoding, network drain or commit.

Capacity must be enforced before capture allocation, candidate copying and
serialization, not by compressing an unbounded report afterward. Bound retained
ownership, variable-length payloads, row/event counts, encoded bytes and total
process diagnostic memory. Reserve terminal status and omission counters.
Truncation must preserve valid JSON and label incomplete references/coverage;
it must never change the query result or masquerade as search completion.
Do not retain a large Memo graph indirectly through an apparently small Arc.

The production decisions have different contracts:

| Decision | Source | Meaning |
| --- | --- | --- |
| Continuation pruning | [cost.rs](src/physical/cost.rs) and [memo.rs](src/cascades/memo.rs) | Pareto comparison plus goal-dependent source-response equivalence; it can return no order |
| Frontier selection | [objective.rs](src/physical/objective.rs) and [memo.rs](src/cascades/memo.rs) | Objective ordering among retained candidates, with caller tie-breaks |
| Runtime admission | [portfolio.rs](src/physical/portfolio.rs) | Actual-resource and dependency checks, then objective selection among admissible operating points |

Different task supplies block continuation dominance; they do not prohibit
all comparisons across grant classes. Goal isolation and admission are
distinct layers. Do not infer a production failure from a diagnostic script
that mixed uncalibrated raw costs across classes.

Attribution has both scope definitions and call-site owners. When integrating
the finer F1/F3 ledger, map the compiler, optimizer, engine and planner emitters
along with their registered scopes; do not drop a breakdown because another
branch lacks its field name. Historical bucket labels are not permanent APIs.

When extending diagnostics:

- Observe existing decisions; do not re-run transformations, costing or
  quality evaluation to explain them.
- Distinguish cumulative publications from live frontier membership.
- Label comparison events by decision layer. Track incomparability separately
  from equality, rejection, budget truncation and actual selection; bounded
  logging must not freeze rejected proposals just to give them CandidateIds.
- Use scoped integer identities internally. A cross-run fingerprint is not
  a semantic equivalence proof.
- Keep exclusive wall time, CPU time, allocation traffic, live memory and RSS
  distinct. Do not add overlapping dimensions.
- Bound capture and export memory, record truncation, and release state on
  success, error and cancellation.
- Do not use raw SQL or parameter values by default.
- Keep diagnostic runs separate from trace-off performance samples. Export
  outside an optimizer timer can still be inside compiler or client latency.
- Encode a capture once and select it by invocation/occurrence identity;
  never embed both all statement traces and an overlapping target subset.
  SQL fingerprints alone do not identify repeated executions.
- Converge benchmark consumers on the EXPLAIN schema. Do not add temporary
  environment exporters, server-log parsers or per-experiment JSON extractors.
- Normal benchmark timing remains trace-off. Its bounded post-statement
  receipt channel records an association when the immutable artifact and real
  admission match; missing or mismatched receipts are `Uncovered` and never
  remove the timing sample.
- Classify controls by their effects, not their names. Retire replaced
  diagnostic outputs only; preserve behavior experiments, normal measurement
  receipts and unclassified/mixed controls until their owning workstream
  migrates them. A DIAGNOSTIC prefix does not imply an output-only setting.
- Associate normal samples with diagnostic outputs by input context and
  versioned compile-artifact receipts, then actual admission where available.
  Non-executing COMPILE has no actual admitted image. Different compatible
  fingerprints reject that association; equal hashes still need shape and
  contract checks. Missing or mismatched evidence blocks the joint explanation,
  not preservation of valid timing samples. Never select only matching runs.

## Making a change

1. Identify the contract and its owning layer. Reuse the existing construction,
   fact and task APIs instead of adding a parallel cache or semantic authority.
2. Write a minimal counterexample or independent oracle before changing
   pruning, cardinality composition, domain transfer or completion reporting.
3. Test the real planner/engine entry point, not only a helper with synthetic
   state that bypasses publication and consumption.
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
- Keep continuation dominance, objective ranking and admission decisions
  separate. Incomparable pairs have no rank; expected score is not the full
  objective. Uncalibrated costs from different facts or resource contexts
  cannot be pooled into a model-quality claim.
- Validate pruning laws with an independent continuation oracle. Measured
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
  4,096 + 1,024*S_i + 4,096*P_i + 512*R_i: S_i counts scheduled timing/error
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
