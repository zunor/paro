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

One fresh normal-release diagnostic at `ea1f1bc0` took 1348.831 ms with
452,952,064 bytes peak RSS. Search counts exactly match the starting revision;
this single process is attribution, not a latency regression conclusion.
There were **zero group merges**, 3,402 physical goals, and only 13 frontiers at
the 256-candidate cap. All 13 have **no ancestor source-filter demand**. The
problem is concentrated, not universal high-dimensional frontier saturation.
The immutable candidate archive retains 138,897,568 bytes of source-work
slice payloads, including nested filters and proofs. This is live payload,
not allocation traffic. The largest proposal groups are 30 (4,168),
1021/982/969 (4,099 each), 1009 (3,949), and 996 (3,537). The precise cost-axis
tradeoffs keeping these frontiers alive still need attribution before changing
their pruning contract.

## Frozen physical predicate schedules

Physical implementation holds a read guard on logical state. An independent
short-held payload-store mutex publishes immutable physical payloads; native
predicate analysis occurs outside that mutex. A completed schedule is keyed by
the logical expression ID and physical cost epoch, not by goal or permutation
bytes. Mandatory and post-exploration epochs cannot reuse changed statistics,
while exact old candidates retain their original physical payloads.

`Canonical` is a completed schedule, distinct from interruption. An interrupted
analysis publishes neither a payload nor a candidate; it cannot silently race
an ordered implementation with a cost-identical canonical fallback. The engine
still returns its separately verified mandatory incumbent on optional expiry.
The cache is valid for the engine's frozen-fact physical-search epochs, not for
arbitrary mutation of Memo facts inside an epoch.

Rollback unwinds only schedule insertions after its savepoint. A no-delta
rollback is constant work. Tests cover a held logical reader during actual
implementation, 100 repeated goals in one epoch, changed evidence in the next
epoch, exact old-payload replay, every interrupted analysis prefix, rollback,
and the existing exhaustive fence/permutation oracle. The three schedule
counters distinguish builds, hits and rollback removals; they do not alter
search admission.

Validation: 1,036 optimizer and 664 execution tests pass (two optimizer doc
tests ignored), and strict workspace/all-target Clippy passes. This revision
has not yet been timed; the cache is not claimed to close the cold target.
