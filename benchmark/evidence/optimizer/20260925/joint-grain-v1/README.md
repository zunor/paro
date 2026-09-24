# Joint aggregate grain / join-subset planning

## Implementation and scope

Source: clean `2d6048017717cf86c380bcfa9839285361d74688` (`78f949a33`
implements joint planning; `2d6048017` preserves RF source multiplicity at
the region boundary). Release executable SHA256:
`33a528fc3def8583e7ca40cb3cde73845a1f3b349cbe36170933ea9d2060c6ce`.

- Aggregate regions now use `(relation subset, partial-grain subset)` states.
  Raw and partial rows are distinct states. Join, partial and final transitions
  use the existing local statistics propagation/gathering and the same
  `direct::select_local` physical response used by final physical selection.
- Immutable child choices and relation facts replace full-tree candidate
  copies. Atomic inputs are priced once per region; transitions price only
  their local operator; only the winning DAG is reconstructed. The previous
  two-tree aggregate alternative pricing is removed.
- Both build orientations, RF eligibility, memory feasibility and task supply
  use the shared physical implementation/cost contracts. Source lineage at
  a boundary preserves the source's multiplicity evidence; a filtered output
  estimate is not substituted for source NDV.
- The bounded domain is grouped mergeable aggregates over 2–8 atomic inputs
  joined by movable inner equi-column predicates. At most one partial
  transition is allowed per plan; final merge preserves original grouping.
  Unsupported laws and exhausted transition budgets retain the original
  region, not a false infeasibility conclusion.

This completes the joint-region vertical slice, **not universal model unity**.
Ordinary join-only regions still use additive work DP; stage-end statistics
are still refreshed; one response per grain is heuristic under parent RF and
resource interactions. No global optimality or default promotion is claimed.
The production default remains `quality`; `pipeline` remains opt-in.

## Same-source normal measurements

See [registration](registration.md). Each cell has three independent fresh
processes, one warmup and two warm rounds per block. Full typed result,
multiset and required ORDER validation passed in all six cells. Compiler
times come from normal SELECT receipts with bounded work evidence enabled,
not Detail time. DuckDB is 1.5.5 with the preregistered native binary hash.

| Query | Policy | Compiler ms | C1 ms | DuckDB C1 ms | Warm ms | DuckDB warm ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Q04 | pipeline | 7.185 | 224.011 | 126.971 | 149.253 | 107.557 |
| Q04 | quality | 17.809 | 225.769 | 133.579 | 127.480 | 105.309 |
| Q11 | pipeline | 4.123 | 150.011 | 70.620 | 85.760 | 57.838 |
| Q11 | quality | 13.207 | 148.049 | 69.049 | 84.344 | 57.803 |
| Q74 | pipeline | 3.562 | 94.375 | 85.524 | 53.858 | 66.595 |
| Q74 | quality | 12.389 | 116.291 | 88.490 | 59.649 | 69.431 |

All cold compiler samples in microseconds (sorted only for this display;
original order and all samples remain in the packages):

| Query | pipeline | quality |
| --- | --- | --- |
| Q04 | 6915, 7185, 7398 | 17761, 17809, 19249 |
| Q11 | 4037, 4123, 4152 | 12323, 13207, 13295 |
| Q74 | 3459, 3562, 3987 | 10693, 12389, 13536 |

The pipeline compiles faster than quality in this cohort. This is not an
isolated causal speedup of this commit over the previous pipeline: those
historical measurements are not matched controls. Q04 still loses execution
quality; Q11 C1 does not improve despite cheaper compilation. Q74 favors the
pipeline in this pilot. Three blocks, sequential policy cells, generator-only
metadata and observed VM/background load do not certify parity or non-inferiority.

## Structural diagnosis, separate from timing

Bounded Detail captures report:

| Query | Joint regions | Transitions | Partial states | Selected partials | Budget fallbacks |
| --- | ---: | ---: | ---: | ---: | ---: |
| Q04 | 3 | 51 | 9 | 3 | 0 |
| Q11 | 2 | 34 | 6 | 2 | 0 |
| Q74 | 2 | 34 | 6 | 2 | 0 |

The diagnostic cells intentionally record execution association `Uncovered`:
EXPLAIN COMPILE is not an execution receipt. Normal per-sample receipts are
Verified. All campaign/cell/compile-document validators passed under those
different contracts; requiring diagnostic execution receipts correctly fails.
Do not attach a separate diagnostic duration or plan to an individual normal
execution without its required identity association.

Q04 triggered the preregistered >10% warm investigation threshold. Separate
untimed plain EXPLAIN inspection is retained in `q04-plan-inspection.txt`:

- Both policies use partial/final aggregation in all three producer branches.
  The pipeline builds the wide customer input in the store branch, while
  quality builds the partial aggregate. Catalog builds the partial input in
  both policies; the earlier blanket diagnosis of two wrong build sides no
  longer describes this plan.
- The six-consumer join trees and ratio-predicate activation points differ.
  Pipeline applies one ratio before the last join; quality applies one with
  fewer consumers joined. This consumer region is outside the new aggregate
  DP and still uses the older additive work objective.
- Estimated CTE rows differ (252,279 pipeline versus 1,126,093 quality), and
  pipeline has residual Filter estimates exceeding child estimates. These
  are remaining statistics ownership/coherence problems, not proof that one
  policy's estimates are accurate. The next extension should close estimate
  propagation and physical-response ranking in ordinary consumer regions,
  not add more global search or hard-code this query's build side.

This inspection narrows further work; it does not assign the measured warm
difference causally to one operator. There is no runtime adaptive build-side
switching, exhaustive SQL search, or ProofComplete result in this delivery.

## Validation

- Final-source workspace tests, including optimizer 1,433 tests: passed.
- Strict workspace/all-target Clippy, release build, task-file formatting and
  `git diff --check`: passed.
- Benchmark Make test target: 207 tests passed. Temporary missing-baseline
  messages belong to harness unit tests, not a real failed performance gate.
- Real session tests cover duplicate dimension rows/labels, NULL, filtered
  mergeable aggregates, unsupported aggregate fallback, empty input and
  forced-external CTE execution. Independent expected sums preserve duplicate
  multiplicity; pipeline and quality results/types/names match.
- Joint-state tests cover multiple aggregation cuts, exact reconstructed
  bindings/types, no escaping BoundReference, unsupported DISTINCT and zero
  transition budget. They do not prove global physical optimality.
- Full default-policy SQL regress with FD limit 65,536: **184 passed, 1
  existing settings-description failure**, zero new cases. `regress/error.txt`
  preserves the difference. No expected or `.actual` file was updated. This
  default-policy run is not broad pipeline certification.
- Memory-runtime and fallible-vector API guards: passed. Full repository
  header check reports 169 existing issues outside the task; newly added code
  has the required header. This is not a clean all-static-checks claim.

Packages under `runs/` retain manifests, normal samples/receipts, diagnostic
cells and one capture per diagnostic cell, within the registered 2 MiB/cell
and 12 MiB total. No server logs or database snapshots are archived. Historical
broad-corpus blockers remain open; this matrix does not close them.
