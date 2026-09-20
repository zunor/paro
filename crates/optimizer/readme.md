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
- Relation facts and statistics have an explicit owner and revision.
  Inserting an equivalent alternative must not “vote” for a different
  cardinality using arrival order or a smaller fingerprint.
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
- A winner retains exact child choices. Frontier pruning must not invalidate
  archived choices still referenced by a parent or frozen candidate.
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
- A declared resource class is not necessarily feasible. Distinguish a
  verified plan, proven infeasibility, incomplete search, unsupported capability
  and an internal failure.
- Admission may execute only a verified variant that fits the actual grant.
  An unsearched class is not an executable fallback.
- Frozen candidates and eventual execution images must preserve exact
  dependencies, properties, choices and resource contracts.
- Deferred construction is not free if it is eventually requested.
  Distinguish unused variants never built from work moved to admission; count
  actual materializations in admission and first-statement accounting.

## Completion vocabulary

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

## Diagnostics and Trace Matrix

The current profiler, exclusive work ledger and Memo snapshots are separate
sources. Trace Matrix convergence targets common identities, units, causal
links, bounds and lifecycle. This README does not claim the unified interface
is implemented. The contributor contracts below are usable in a standalone
clone; a separate design checkout is optional.

The target public entry point is `EXPLAIN (OPTIMIZER, FORMAT JSON)`, with TEXT
rendered from the same typed record. This syntax is a planned deliverable, not
a claim that the current parser accepts it. Summary is the default; explicit
Detail adds bounded records, not different search semantics. Without ANALYZE,
the target is not executed: runtime/admission measurements are NotExecuted,
not zero. Optimizer diagnostics force a target compilation without consuming
or populating its statement-plan cache; this is not proof of a normal SELECT
cache miss. The output cannot measure its own future network drain or commit.

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
local paro-benchmark skill; that tooling is not a tracked-clone prerequisite.
Use the established first-statement harness, fixed source/binary/data/SQL
identities, actual resource envelopes and complete result validation. Do not
run concurrent benchmark runners or compare a diagnostic cohort with a normal
one. Pin the competitor build, extensions and settings as well; a parity claim
does not transfer to a new competitor version. Apply the following comparison
rules before drawing conclusions.

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

Supplementary proposals live in the separate paro-docs-design repository:
optimizer/optimizer-convergence-design.md and
optimizer/optimizer-trace-matrix.md. They cover closeout sequencing and the
target diagnostic schema, and are not required to resolve any normative link
in this README. This repository neither vendors nor automatically tracks that
checkout; consult an explicit design revision when using those proposals.

Keep this README short-lived-data free:

- Architecture and contract changes belong here or in a focused design.
- Decisions and important negative results belong in small indexed records.
- Ordinary evidence is four files: manifest.json, timings.json,
  explain-optimizer.json and README.md, at most 250,000 uncompressed UTF-8
  bytes per arm. Keep every valid normal timing sample and explicit exclusions;
  derived medians alone are not sufficient evidence. Diagnostic timings are
  not normal performance samples.
- Larger legitimate campaigns, exact replay fixtures and correctness inputs
  need a registered, bounded extension, not silently discarded samples or an
  unlimited number of artificial arms. Raw Detail traces are short-lived,
  quota-bound debugging data. Server .parod.log files do not belong in ordinary
  evidence packages; operational log retention is a separate responsibility.
- Compact history once, validate retained identities/samples/conclusions, and
  restore-test necessary evidence before deleting redundant raw data. Record
  what was deleted and cannot be reconstructed; a hash is not a backup. Do not
  move every obsolete trace into another permanent archive. Never delete the
  only reproduction of an unresolved correctness failure.
- Deleting a tracked log does not remove its Git history. History rewriting
  and destructive workspace cleanup require separate, explicit scope.
