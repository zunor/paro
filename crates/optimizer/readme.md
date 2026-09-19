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
| Physical contracts | [physical/requirements.rs](src/physical/requirements.rs), [physical/cost.rs](src/physical/cost.rs), [physical/resources.rs](src/physical/resources.rs) | Properties, work/cost composition and resource feasibility |
| Portfolio / extraction | [physical/portfolio.rs](src/physical/portfolio.rs), [physical/extraction/](src/physical/extraction/) | Grant variants and physical construction |
| Verification | [cascades/verifier.rs](src/cascades/verifier.rs), [physical/verifier.rs](src/physical/verifier.rs), [verify.rs](src/verify.rs) | Memo, physical and logical invariants |
| Observability | [profiler.rs](src/profiler.rs), [work_partition.rs](src/work_partition.rs), [diagnostic_snapshot.rs](src/cascades/memo/diagnostic_snapshot.rs) | Existing profiling and bounded snapshots; inputs to the Trace Matrix convergence work |

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
- Deferring construction moves work; it does not make that work free.
  Admission and first-statement accounting must include deferred work.

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
sources. The proposed
[Trace Matrix contract](../../../paro-docs-design/optimizer/optimizer-trace-matrix.md)
unifies their identities, units, causal links, bounds and lifecycle. That link
assumes a sibling checkout of the design repository; the unified interface is
not claimed to be implemented by this README.

When extending diagnostics:

- Observe existing decisions; do not re-run transformations, costing or
  quality evaluation to explain them.
- Distinguish cumulative publications from live frontier membership.
- Use scoped integer identities internally. A cross-run fingerprint is not
  a semantic equivalence proof.
- Keep exclusive wall time, CPU time, allocation traffic, live memory and RSS
  distinct. Do not add overlapping dimensions.
- Bound capture and export memory, record truncation, and release state on
  success, error and cancellation.
- Do not use raw SQL or parameter values by default.
- Keep diagnostic runs separate from trace-off performance samples. Export
  outside an optimizer timer can still be inside compiler or client latency.

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

For performance work, first read the repository
[benchmark skill](../../.agents/skills/paro-benchmark/SKILL.md) and
[benchmark README](../../benchmark/README.md).
Use the established first-statement harness, fixed source/binary/data/SQL
identities, actual resource envelopes and complete result validation. Do not
run concurrent benchmark runners or compare a diagnostic cohort with a normal
one.

## Design and evidence maintenance

The current closeout plan is
[Optimizer Convergence](../../../paro-docs-design/optimizer/optimizer-convergence-design.md).
It separates workspace preparation, observability, contract fixes, default-path
admission and historical cleanup.

Keep this README short-lived-data free:

- Architecture and contract changes belong here or in a focused design.
- Decisions and important negative results belong in small indexed records.
- Benchmark samples and large traces belong in attested evidence packages,
  not in an ever-growing README.
- Archive and restore-test necessary evidence before deleting raw data.
  Never delete the only reproduction of an unresolved correctness failure.
- Deleting a tracked log does not remove its Git history. History rewriting
  and destructive workspace cleanup require separate, explicit scope.
