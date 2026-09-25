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
  mandatory. Tests exercise the production kernels or small independent
  semantic oracles, not an otherwise dormant alternative optimizer.
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

## Rewrite admission and ownership

The normalization program admits unused outer-lookup elimination only for a
base-table input whose declared unique key is covered by ordinary equality.
It retains NULL-safe joins, incomplete composite keys and any referenced
output. Intermediate projections keep every binding they still evaluate;
column pruning, not join elimination, owns deleting expressions.

Constant LIMIT movement crosses only infallible, side-effect-free projections
without external evaluation boundaries. There is no row-count magic threshold.
CTE normalization has one policy: single-reference DEFAULT owners inline,
multi-reference DEFAULT owners remain shared, and explicit SQL materialization
directives retain their semantics.

The old standalone late-payload, correlated-window/fusion, aggregate alternative
and predicate-pullup implementations are not shipped as test-only optimizers.
Their production-independent implementations remain recoverable from Git.
Active access-path selection, scan materialization, aggregate-grain enumeration,
partition-window lowering and their executable-contract tests remain owned by
their respective stages. Adding a new alternative requires bounded local
cost selection and semantic evidence, not restoring a mandatory historical pass.

`planner::logical::operator::SubplanRef` carries immutable facts for an already
planned regional input or a frozen output. It is not executable and carries no
global search/group identity. Required physical properties and `PhysicalCost`
are executable/estimation contracts, not evidence of global search completion.

## Diagnostics

`EXPLAIN (COMPILE)` observes the actual compiler; `ANALYZE` attaches execution
of that artifact. Summary/Detail share a bounded typed record. Stage completion,
artifact readiness, admission and execution are distinct observations, and
`PlannedWithFallback` is not an optimality proof. See the
[compile contract](../../docs/optimizer/compile-diagnostics.md) for lifecycle
and transport ownership.

Architectural decisions and remaining problems live in
[decisions](../../docs/optimizer/decisions.md) and
[open issues](../../docs/optimizer/open-issues.md). Contributor workflow belongs
in [AGENTS.md](../../AGENTS.md) and the versioned `paro-optimizer`,
`paro-benchmark` and `paro-evidence` skills, not in this architecture document.
