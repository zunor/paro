# Q11 cold-search reconstruction

Target: original TPC-DS SF1 Q11 cold **planning** below 100 ms, without replacing
`SUM(x-y)`, lowering search budgets, disabling rules, or losing warmed execution
quality. This is not a claim that first-statement latency will be below 100 ms.
Normal release timing and allocation-instrumented attribution remain separate.

The starting revision is `3dbcfc95`, with 1287.412 ms cold planning, 456,605,696
bytes median peak RSS, 49,521 winner proposals and 13,212 frontier truncations.
The last complete Q11 execution comparison passed at ratio 0.942894 with 95%
CI [0.929616, 0.958304]. Both measurements describe `3d31841f`, not later code.

## Design constraints

- A scalar objective needs optimal substructure. Source-filter response,
  available retained-state memory, task supply, and enforced properties must
  be in the parent context or a proven compositional summary before discarding
  their alternatives. Current memory feasibility at a child is not a proof of
  feasibility when a parent retains more memory. Do not replace the frontier
  with arbitrary scalar top-k.
- An incumbent is an executable fallback. A pruning bound additionally needs
  compatible facts, context and a conservative lower-bound composition; the
  parent's runtime filters can invalidate an unfiltered child's cost bound.
- A planning arena only bounds allocations routed through its admission plan.
  An arena containing ordinary `Vec`/`Box`/map owners does not bound their nested
  heap allocations or temporary rewrite work.
- A fraction of an estimated execution time bounds planning overhead relative
  to an estimate, not actual execution regret. The 100 ms goal will not be met
  by silently shortening the anytime deadline.

This follows the optimal-substructure and required-property separation in
[CockroachDB's optimizer overview](https://github.com/cockroachdb/cockroach/blob/master/pkg/sql/opt/doc.go).
Its architecture is guidance, not evidence of Paro's obtainable timings or of
the review's proposed per-operation performance figures.

## Attribution before changes to search policy

Physical-search diagnostics now retain per-group proposal/archive counts,
per-goal frontier sizes/high-water/truncations/source-demand counts, an exact
frontier-size histogram and successful group merges. Archive counts span cost
epochs; frontier sizes describe the current epoch. Source-work slice payload
bytes include nested predicate/proof slices but are not total allocator bytes
or RSS. Collection walks stored candidates once after search, not for each
proposal; no new pruning, rule, budget or ordering policy is introduced.

Optimizer tests: 1033 pass, two doc tests ignored; optimizer/all-target Clippy
passes. Diagnostic timing and the explanation of frontier growth follow.
