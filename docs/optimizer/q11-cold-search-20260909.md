# Q11 cold-search reconstruction

Target: original TPC-DS SF1 Q11 cold **planning** below 100 ms, without replacing
`SUM(x-y)`, lowering search budgets, disabling rules, or losing warmed execution
quality. This is not a claim that first-statement latency will be below 100 ms.
Normal release timing and allocation-instrumented attribution remain separate.

The starting revision is `3dbcfc95`, with 1287.412 ms cold planning, 456,605,696
bytes median peak RSS, 49,521 winner proposals and 13,212 frontier truncations.
The starting revision's complete Q11 execution comparison passed at ratio 0.942894 with 95%
CI [0.929616, 0.958304]. Both measurements describe `3d31841f`, not later code.

## Current verified checkpoint — 100 ms is NOT achieved

Normal-release original Q11, five independent processes, unchanged immutable
SF1 input seed, four workers and 2 GB:

| Metric | Review baseline (`3d31841f`) | Current code (`7a3a36e2`) |
| --- | ---: | ---: |
| Cold EXPLAIN median | 1287.412 ms | 705.661 ms |
| Optimizer median | about 1280.5 ms | 698.706 ms |
| Median peak RSS | 456,605,696 bytes | 256,016,384 bytes |
| Logical groups / expressions | 1022 / 1582 | 1022 / 1582 |
| Winner proposals | 49,521 | 26,888 |
| Frontier truncation obligations | 13,212 | 0 |
| Child-product omission obligations | 144 | 86 |
| Search complete | false | false |

The cold gate passes against the exact review baseline with a maximum ratio
of 1.0, including all omission-count checks. Against the older isolated-seed
baseline it still fails on seven omission categories (dimension deferral,
optional groups, CTE demand, rule fires, child products, CTE inline and aggregate
subsumption). That older bounded-search quality gap is not silently blessed.
No budget limit, optional deadline, enabled-rule set, or original SQL was
changed in this round.

Current code also passes `make static`, 6,528 workspace tests (85 ignored),
strict server Clippy with `alloc-metrics`, and the full fresh-instance SQL
suite, **184/184**. The first SQL run's two
memory-setting display failures were fixture launch configuration (`1GB`
instead of `1073741824`), not product changes. One deliberately reviewed
baseline update (`26385443`) records `k < 5 AND k >= 2` evaluation order;
the fixture's result and cardinality estimate are unchanged. The independent
ten-case plan-quality gate passes against the review baseline: maximum
q-error 2.0, no worsened q-error or plan-quality metric.

The final execution comparator at `26385443` (only SQL test changes since
`7a3a36e2`) uses seven fresh process blocks and 70 paired samples per engine.
All 90 rows match, digest
`9108a5b43530c24778abc0d8d3bf1ee218a9815eb0d15310bb4d7c6bcc5f2c78`.
Paro median is **98.222 ms**, DuckDB **104.719 ms**, paired ratio **0.948372**,
hierarchical 95% CI **[0.932114, 0.976202]**. First-statement medians remain
**831.288 vs 109.412 ms**. Execution is faster; cold statement latency is not.

Evidence files in `benchmark/report/`:

- `q11-cold-immutable-boundary-views-20260909.json`
- `q11-allocation-search-contracts-20260909.json` (instrumented, attribution only)
- `q11-search-contracts-plan-quality-20260909.json`
- `q11-execution-immutable-boundary-views-20260909.json`

All collectors finished with valid build/harness/seed attestations. Test and
benchmark servers were owned, isolated, and stopped; the input seed was not
used as a live database or modified. Detailed source changes and intermediate
measurements follow; their historical test counts/timings are not final claims.

### Remaining architectural work, not completed deliverables

The new allocation profile attributes **1,395,132,794 bytes** to Memo search:
**921,859,225 bytes** to rule discovery/application and **473,273,569 bytes**
outside those scopes. The largest rule allocations are join-region enumeration
(288,665,074), predicate transfer (244,898,099), and dimension deferral
(81,229,266). Published shared source-response payload is just 1,481,560 bytes;
it is no longer the main live-memory amplifier. Instrumented EXPLAIN time
(715.479 ms, one process) is not mixed into the normal-release latency result.

Earlier 661.3/461.0 ms rule subtotals in this log sum diagnostics for rules
which published outputs. They exclude discovery/attempt work of zero-output
rules and are **not complete rule-time totals**. The allocation totals above
cover all rule scopes; elapsed rows for insertions and attempts overlap and
must not be added together.

The next high-leverage change is native rule input **and output**, not another
cache layered around the same tree round trip:

1. A binding should expose native operator/scalar IDs, opaque child GroupRefs,
   and specifically requested fact handles. A cheap applicable proof must run
   before any legacy transport/settlement work. No-match evidence must remain
   subscribed and cancellation/work admission must precede construction.
2. Predicate transfer and join-region enumeration should produce only changed
   native terms, retaining unchanged GroupRefs. Publication must derive local
   facts from those inputs and never instantiate, settle and restage unchanged
   consumed shells. Output demand and execution carriers remain separate.
3. Child continuation requirements and conservative phase/source lower bounds
   must precede scalar winner collapse or incumbent-based branch-and-bound.
   The current phase-memory correction does not itself provide those proofs.
4. Predicate scheduling still lacks calibrated per-kernel evaluation work and
   a common residual-container contract beyond Filter (review F6/F7). Planning
   memory admission also remains incomplete: shared ownership is not an
   allocation budget. These are open, not claimed resolved by this checkpoint.

Acceptance remains the independent closure/cost oracles, cold gate without
shrinking search budgets, complete SQL/q-error gates, and qualified original
Q11 execution below DuckDB. The measured improvements do not establish that
100 ms is reachable merely by multiplying speculative per-operation targets.

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

## A concrete frontier-amplification mechanism

The opt-in vector trace (`q11-cold-frontier-vectors-20260909.json`, not a normal
timing run) contains 3,328 candidates from the 13 capped frontiers. In group
30, the first three candidates have identical expected work, upper work,
latency, resource vectors, memory proof and task supply; only their work/span
**lower estimates** differ. All are retained by the old code: strict dominance
does not compare lower estimates, while its separate equality check compares
the entire `SearchCost`. The gap admits duplicate continuation operating points
and lets their child combinations multiply. Many other candidates have genuine
resource tradeoffs; this finding does not justify scalar top-k or explain every
retained candidate.

Dominance and equality now use one partial continuation comparison. All prior
ranking/resource coordinates remain; task capacity, output supply, and external
worker identity are explicit equality domains. Candidate lower evidence is
retained for proofs but does not create a new ranking axis. Incomparable source
responses are still preserved. No floating tolerance or search-budget change is
involved. A 120-permutation independent frontier oracle now includes duplicate
operating points with differing lower evidence and requires one stable winner.

Five normal processes at `7eb4463f` give a median 1363.160 ms (optimizer
1358.144 ms), RSS 444,547,072 bytes. Proposals fall only from 49,521 to 48,599,
and frontier obligations from 13,212 to 13,144. Logical groups/expressions,
bindings and settlement hit/miss counts are unchanged. Thus lower-evidence
duplication is real but does **not** explain most of the regression. The filter
schedule cache records 306 builds, 630 hits and no rollback removals.

## Goal-visible cost coordinates and phase memory

Cost pruning now takes the goal's objective, also used by portfolio pruning.
Latency preserves expected work, span, scalar work and risk tie-break axes;
throughput additionally observes CPU work, and robustness observes upper work.
Reporting-only per-resource vectors and unused upper spans are not additional
objectives. Every goal still preserves memory/forward-progress/admission
contracts, output task supply, capacity and demanded source response. This is
not a scalar winner approximation: work/span and memory tradeoffs still need a
frontier until continuation requirements can represent them directly.

Memory comparisons use the **absolute preferred operating point**, not an
elastic delta relative to each candidate's different floor. This requires the
composition law to be monotone in those coordinates. The old overlap law added
the retained-phase floor to the *whole-plan* elastic delta; an unrelated
sequential child's larger floor could therefore make an overlap phase cheaper
in memory. The new law composes real phase footprints: floors add within an
overlap, elastic working sets share the pool, and sequential phases take maxima.
An independent oracle enumerates all phases for 12³ input/local operating-point
triples and four overlap masks. A separate oracle checks objective pruning
under independently priced work/span/memory continuations. 1,039 optimizer
tests and strict workspace/all-target Clippy pass; Q11 timing is pending.

Five normal processes at `5221c858`: median EXPLAIN 1185.359 ms, optimizer
1180.565 ms, RSS 350,994,432 bytes. Groups (1,022), logical expressions (1,582),
physical expressions (2,785), binding count (7,871), and settlement hit/miss
counts are unchanged. Proposals fall to 26,888; frontier truncations fall to
**zero**, and child-product obligations from 144 to 86. This attributes most
frontier pressure to irrelevant objective axes and relative-memory accounting,
not to an irreducible 256-way source-response partition. The median sample still
spends 233.8 ms in predicate transfer and 134.8 ms in join-region enumeration;
the sub-100 ms target is not closed.

Five normal processes at `07b6a0b2` (immutable source payload sharing) give
1186.363 ms median EXPLAIN. This removes payload duplication but does not show
an additional latency improvement over the preceding change.

## Region ownership normalization

A separate macOS sampling run on the original Q11 (owned private dataset
copy, normal release) observed 163 region-normalization stack samples out of
919 optimizer samples. This is diagnostic attribution, not a timed benchmark.
The work is charged to rules because staging republishes their region facets;
it is not owned-expression materialization. Singleton scopes cannot partially
overlap any set, so their equality classes are now coalesced independently of
composite overlap closure. Laminar validation and parent construction use one
reverse-size membership pass, replacing two all-pairs scans. Final node scope
and facet payloads are moved rather than cloned.

No admission ceiling, facet priority, or normalization schedule changes. An
independent ordered-set model checks 512 mixed scope families under six optional
ceilings in both insertion orders; exhaustive parent selection is its oracle.
Another test covers 8,192 singleton anchors inside a required region. All 1,042
optimizer tests pass; normal timing follows this commit.

Five normal processes at `76e475ff`: median EXPLAIN **966.227 ms**, optimizer
961.552 ms, RSS 283,312,128 bytes. Every search counter matches `07b6a0b2`,
including groups, logical/physical expressions, bindings, settlements,
proposals, frontier distribution, and all omission counts. Rule-attributed
time falls from 661.3 to 461.0 ms in the respective median samples. The source
payload-sharing revision itself had reduced RSS to 276,086,784 bytes.

## Occurrence work is not a semantic reservation

Fact reads previously hashed `(group, consumed credit)` and kept a distinct
event-map entry at every admission, despite each read being new actual work.
The ledger now distinguishes refundable, idempotent candidate reservations
from nonrefundable executed-work meters. Each originating ledger contributes
one monotone prefix per dimension. Merging prefixes takes their maximum;
different origins add. Repeated and transitive group merges therefore cannot
duplicate credit or double-charge the same executed prefix. Rollback preserves
executed work and omission evidence while restoring candidate reservations.
Successful fact-work admission hashes/stores no event; only failure constructs
the same stable diagnostic witness as before. Budget limits and charged units
are unchanged. Independent scalar accounting covers mixed reservations,
execution, refunds, zero/max limits, merge cycles and rollback.

At `5b7ed48e`, 1,045 optimizer tests and strict workspace/all-target Clippy
pass. Five normal processes give median EXPLAIN **806.858 ms**, optimizer
802.242 ms and RSS 262,750,208 bytes. Every search counter remains identical
to `76e475ff`. This is a reduction in bookkeeping, not a smaller search.

## Incremental subscription membership

Refreshing a task's read cursors previously removed every reverse-index
membership and reinserted all of them, even if only the revision changed.
The engine now computes the sorted group-set delta and mutates only added or
removed memberships. Facts-only and frontier cursors for the same group still
produce one subscription. The wake-up path borrows its existing subscriber set
instead of copying it. Application fact observations update a sorted unique
cursor vector in place, preserving an earlier frontier read on later
facts-only accesses. A 16,384-case set-difference oracle includes duplicate
group cursors with changed revisions; real engine tests retain negative-match
and application-only wake-ups. No dependency is removed merely because the
latest binding did not read it.

At `6e070f6c`, 1,047 optimizer tests and strict workspace/all-target Clippy
pass. Five fresh processes give median EXPLAIN **745.784 ms**, optimizer
739.025 ms and RSS 260,849,664 bytes. Every search counter is unchanged from
the work-meter revision. The complete original Q11 execution comparison
(`q11-execution-search-contracts-20260909.json`, seven process blocks, five
paired measurement rounds per block) agrees on all 90 rows: Paro median
99.504 ms, DuckDB 105.283 ms, paired ratio 0.948355 with hierarchical 95% CI
[0.93526, 0.96228]. First statement is still 867.569 vs 108.162 ms; this is
not a claim that cold performance is competitive or that 100 ms is reached.

## Immutable bound-root imports

The remaining local recipe/staging paths repeatedly import the same immutable
bound scalar roots into their native scalar arenas. Imports now cache canonical
`ScalarExprId`s by a weak source-allocation witness and the exact positional
reference domain. Binding and column catalogs have process-local validity
tokens: appends preserve them, forks and rollback invalidate them. Scalar
rollback clears cached target IDs before ordinal reuse. These tokens are never
part of plan identity or deterministic search ordering.

A weak witness retains neither the source payload nor its descendants. An
outstanding weak reference makes `Arc::make_mut` detach even a unique payload,
so mutation cannot silently preserve a cached identity. Dead weak control blocks
are collected geometrically. Only successful root imports are cached; errors,
pending conjunction prefixes, and runtime evaluation occurrences are not.
Native scalar interning and exact semantic identities remain unchanged.

Tests cover independent positional domains, each namespace's rollback, catalog
forks, mutation, failed imports, and reclamation of 5,000 temporary source roots.
All 1,051 optimizer and 286 planner tests pass. Four diagnostic counters expose
hits/misses separately for settlement and planner imports; timing follows after
the source snapshot is committed.

Five normal processes at `774b087c` give median EXPLAIN **736.034 ms**.
All pre-existing search counters and the selected plan are identical to
`6e070f6c`; only the four new import counters differ. Planner imports have
3,718 hits / 8,759 misses, while settlement has just 106 / 19,378. This is
a small measured reduction, not the remaining order-of-magnitude improvement.
Inspection explains one reason for poor reuse: demand's identity binding
substitution entered the mutable scalar visitor and detached every node even
when every mapping was `(column, column)`.

Settlement now separates retention maps from actual scalar edits. Identity
substitutions do not visit expression payloads. Real substitutions use a
context-free persistent DAG rewrite: one visit per shared node, only changed
ancestor paths copied, correlated references untouched. The common traversal
contract covers aggregate/window modifier edges and short-circuits errors.
Tests cover a 2^50-occurrence shared DAG, 10,000-deep scalar, repeated local
bindings, correlated bindings, unchanged siblings and idempotent substitution.

At `34bcea6a`, five normal processes give median EXPLAIN **706.872 ms**,
optimizer 701.766 ms and RSS 263,995,392 bytes. Plan and all old search
counters are unchanged; settlement imports improve to 13,699 hits / 5,785
misses, planner imports to 6,999 / 5,478. There is no search-budget change.

## Typed immutable boundary views

Bound relation facts now have a mutable construction value and a separate
immutable publication object. The publication owns the aligned type domain
and lazily derived column-statistics handles. A bound reference reads its
types from that object, not a second independently mutable vector. Attaching
facts with a different type domain is a diagnosed contract error. Replacing
evidence creates a new publication; there is no cache requiring callers to
remember to invalidate public mutable fields.

Each immutable Memo fact value also owns its layout-specific transport views.
An internal hash indexes collision buckets; complete column mappings and
layout values establish equality. Fresh layout allocations with the same
content reuse the view, while changed evidence cannot reuse old statistics.
Views are neither keyed by unguarded addresses nor retained by a global cache.
Tests check shared handles, evidence replacement, typed attachment rejection,
layout-value reuse and real Memo evidence mutation. All 1,053 optimizer and
289 planner tests and strict workspace/all-target Clippy pass.

## Immutable source-response payloads

Candidates now share immutable source-work snapshots. Ordinary parent
composition copies handles, not four `SearchCost` values plus nested proof and
evaluation slices for every lane. A filter forks only affected lanes; a repeated
proof/evaluation occurrence keeps the original snapshot. There is no mutable
access to a published snapshot. Existing source algebra/oracle tests additionally
check parent-child sharing, unchanged sibling sharing, child isolation and
idempotent-publication identity.

Payload-byte attribution counts shared backing storage once (against the first
archive owner to reference it), plus each candidate's handle slice. It includes
all nested proof/evaluation payloads but excludes allocator/control-block
overhead; it is still not an RSS or allocation-traffic metric. Search budgets,
source response equality and costing algebra are unchanged by this storage
refactor.
