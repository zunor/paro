# Finite regional planning: first vertical slice

## Status

`SET optimizer_search_policy = 'regional'` selects the finite relational
program. It reaches the real compiler, grant admission and execution path; it
is not an EXPLAIN-only planner. The default remains `quality`: the first pilot
did **not** justify replacing it. This is a migration slice, not a claim that
the general Cascades engine has been removed or that planning is now optimal.
The old policy remains a comparison control, not a backwards-compatibility
requirement for the target architecture.

## Ownership and stages

1. Existing binding, semantic construction, scalar interning and relational
   facts create the shared catalog. Do not introduce a second expression IR.
2. The ordered program in
   `crates/optimizer/src/cascades/planner/regional.rs` operates on finite,
   reachable expression snapshots. A producer's outputs feed later passes,
   never recursively re-enter that pass. Local reductions precede aggregate
   region alternatives; demand transport follows them; join enumeration sees
   the resulting domains; local cleanup follows enumeration.
3. `Normalize` chooses an equivalent representation. A successfully committed
   replacement retires the input from every active logical discovery index,
   while its immutable identity remains available to proofs. Retirement is
   forbidden during a producer transaction or after physical implementation.
   A replacement that depends on its own group cannot retire its base case.
   `Explore` retains alternatives for contextual costing. It is not legal to
   treat a cost tradeoff as normalization merely to reduce the candidate count.
4. Native producers retain the existing exact bindings, output schemas,
   equivalence proofs, sidecar transactions, work admission and cancellation.
   Output bounds are reserved before mutation. Errors roll back and propagate;
   no silent replacement by a supposedly safe baseline.
5. Only after the program ends does physical implementation/costing start.
   There is no logical agenda, quality-forced work lane, or logical/physical
   interleaving in this mode. The existing goal-sensitive physical solver,
   resource feasibility, runtime-filter responses and exact child choices are
   reused. Parent-sensitive alternatives must not be collapsed to a single
   scalar winner without a continuation-order proof.
6. This slice prices the finite catalog for **all declared grants** and uses
   the existing eager portfolio contract. It does not claim the one-class
   lazy-search coverage of the default planner. Actual resource admission and
   image lowering remain unchanged.

The optional deadline can end relational production at a transaction boundary.
Closed-catalog physical construction still has to produce an executable result;
like mandatory baseline construction, it checks cancellation but is outside the
optional deadline. Work limits still apply. This is **not a hard compile-time
deadline**, and any tail remains in the compiler/C1 timers.

The program has an explicit `RegionalProgram` incomplete-search obligation.
Exhausting this finite heuristic program is not exhaustive equivalence search,
`QualityPolicySatisfied`, or `ProofComplete`. Additional actual budget exhaustion
is independently reported. SQL policy is included in planning/cache identity.

## Scope and remaining architecture work

The implemented region producers cover join ordering and aggregate placement;
access-path implementations still use their existing shared contracts. Outer,
semi/anti, volatile and projection-sensitive boundaries remain controlled by
the existing native matchers and semantic producers. No table names, SQL
fingerprints, query numbers or operator-count quality tests route the program.

This slice seals one shared catalog, but does **not** yet have explicit maximal
region ownership or a lightweight region-local physical solver. It still
creates old physical goal/recipe states and eagerly prices grant alternatives.
Changing the agenda alone must not be presented as eliminating that machinery.
The initial program deliberately does not explore every CTE partition/inlining
or scalar-window alternative available to general search. Omitted choices are
search coverage limitations, not semantic permission to alter SQL results.

Three failed ideas are deliberately not adopted:

- Retaining all pre/post normalization forms recreates the rewrite powerset.
- Pushing domains before recognizing aggregate regions can hide a legal
  decomposition behind wrappers. The present phase order preserves that
  opportunity; a future transparent-region recognizer needs its own binding
  and residual proofs, not a broad operator-name exemption.
- Labelling eagerly priced grant variants as a one-class lazy portfolio is a
  contract error; the verifier correctly rejected the first such experiment.

Next promotion prerequisites are explicit region input/output contracts,
bounded contextual physical alternatives without dynamic fact invalidation,
and cross-query execution-quality validation. A good result from one query is
not grounds for deleting the comparison planner or switching the default.
Cost-model or candidate-domain defects must be identified with exact selected
plans before attributing execution regressions to either one.

## Validation and evidence

Engine tests cover a finite self-matching producer, equivalence to a tiny
independent exhaustive control, retained proof sources, cyclic alternatives,
rollback after retirement, explicit rule exclusion, exhausted work and expired
time limits. The production TopN test exercises both policies and includes
low/expected grant classes and nonzero OFFSET. The regression runner accepts
`--optimizer-search-policy` on every connection/reconnect and an explicit,
initially absent `--report-dir`; it never replaces an existing explicit output
directory. Fixture path normalizers still expect a `regress/report` suffix.

See [the pilot and validation report](../../benchmark/evidence/optimizer/20260924/regional-pipeline-v1/README.md).
No expected SQL results were regenerated. Normal timings, diagnostic structures
and full typed-result validation remain separate evidence.
