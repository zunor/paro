# P1/P2 compiler cost investigation — no P3 admission

No compiler<30ms, W noninferiority, default rollout or parity claim. Quality
handoff remains opt-in; quality satisfaction is not ProofComplete.

## Evidence identity

P1 snapshot/width code 2c587748; accepted P1 source a476731b, clean detached
worktree. P2 guard 2deb6450; its clean seven deferral tests passed. P1 phase
timing f346e310 initially failed compile (missing Debug), corrected90cbbb70;
no measurements from the uncompilable version. User staged/unstaged files were
not included. Original SQL and data are unchanged in production.

Initial commands accidentally used upstream3008-byte Q11 instead of historical
2418-byte SQL. Those reports are retained as **exploratory, excluded from
acceptance**, even though results and full-width plans match. Only `*-exact`
reports use corpus SHA256
`a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503`.
Each report binds source/binary/harness/SQL/data hashes. Inherited Paro-declared
keys versus DuckDB-empty-keys metadata asymmetry remains disclosed.

Two fresh blocks/report,4workers/2GB, exact typed90/order/cache-miss, same model,
budgets and handoff policy. Normal trace-off and statement-trace diagnostic are
separate. Snapshot-enabled reports are **instrumented diagnostics**, regardless
of the old harness's generic `normal_trace_off`/primary-eligible labels: file
serialization remains in compiler/C1. Do not use those C1 values as performance.
Raw manifests bind all compressed and original bytes, including slow samples.

## P1-a: the denominator and the counterfactual

Same exact-Q11 snapshot:891 cumulative archived publications, **473 current live
candidates /228 goals**, maximum width12. Histogram(width:number of goals):
1:131,2:40,3:23,4:17,5:3,6:6,7:3,9:2,11:1,12:2. Summed current high-water491
is not a global peak or a count of simultaneous candidates.

Offline exact-coordinate comparison retains473. Optimistically equating locally
feasible memory coordinates below2GiB retains375 (98 fewer,20.72%), preserving
source response equivalence, work/span, risk, task-supply and completion gates.
This is **NOT a proven envelope quotient**. Admission still checks actual
availability against minimum/preferred; parent overlap can distinguish child
memory floors even when both children fit independently. T1's cache key does
not prove every legal parent has enough residual memory. Proven removals remain
unknown; no production dominance relation is changed.

Source classes are determined by exact production source-response equality,
not digest equality. Retention reasons overlap and are not added. Group-level
counts are reproducible with `analyze_snapshot.py`. Replacement/invalidation
counts were not captured; explicitly null, not fabricated from891−473. This
part of requested P1-a remains incomplete.

Capture14–17us, no omitted candidates. Exact-SQL off/on/off optimizer medians
66.242/65.982/65.891ms: no observed>10% perturbation in this pilot. Serialization
is outside optimizer timer but inside diagnostic compiler/C1; its cost is not
an optimization. No normal lifecycle boundary is moved.

## P1-b: exact SQL width scan

| Width | Syntheses | Live | Optimizer ms | Rules ms | Final admitted fingerprint |
|---|---:|---:|---:|---:|---|
|1|1265|288|97.885|51.214|84bbad8e42e5846efa8a46b1932e566d|
|2|1796|435|104.765|53.207|8e26b511b584655e90f1e1e68d244055|
|4|1491|422|58.409|24.948|5c29cf646706c8c8ba84000150211a6b|
|8|1631|460|63.474|24.423|5c29cf646706c8c8ba84000150211a6b|
|unbounded|1691|473|65.961|24.461|5c29cf646706c8c8ba84000150211a6b|

Times are medians of trace-off diagnostic-instrumented blocks, not nested spans
added together. All cases validate results. Width1/2 grows logical exploration
(238/244 groups versus180) and changes plan. Width4 also changes logical counts
(280 versus278) despite the same winner. Therefore this is sensitivity, not
same-work acceleration; no causal kernel/per-entry coefficients can be uniquely
fit from these changing-work points. Unlimited frontier is not unlimited search:
other safety budgets and incomplete obligations remain.

## P2: shared immutable root guard

Shared necessary eligibility (no post-reduction, nonempty aggregates, plain
grouping domain) now precedes scoped matching and is reused by both rewrite
paths. A non-join child remains eligible and subscribed to future frontier
updates. No new negative-cache system or child-representative shortcut.

Exact Q11 probe still has317 bindings,1691 syntheses,891 publications; deferral
still82matched/22published/60rejected. No demonstrated Q11 work reduction.
The guard is a contract cleanup, **not performance delivery**. Existing guards
report only `no_output` for remaining deferral/subsumption failures; their
individual late semantic proof failures remain unresolved, not all safely
cacheable at root scope.

CTE filter pushdown is1matched/1applicable/1published/0rejected. Its~1.27ms spans
matching plus apply (instantiation/restriction/closure/settlement/validation),
not an identified pure settlement bucket or zero-output waste. No CTE rewrite
or cache was introduced based on that unsupported attribution.

## Disjoint timing and decision (completed 2026-09-14)

Clean90cbbb70, same binary, exact SQL, snapshot OFF, timing OFF/ON/OFF:
normal optimizer medians67.643/66.993/66.692ms; N1691, published891, fingerprint
unchanged. No observed>10% timing perturbation. OFF compiler68.371/67.389ms,
C1 198.727/199.231ms, W90.052/89.971ms: two-block pilots, **not formal M1/W NI**.
No compiler<30ms result. The timing-ON C1 is diagnostic, not a new performance
achievement.

Same diagnostic occurrence with timing ON: optimizer79.447ms, rules25.526ms,
optional composition kernel1.247380ms, candidate admission0.773020ms. The two
disjoint buckets total2.020400ms. Optimizer−rules−these buckets=51.900600ms
remains unattributed and includes diagnostic reporting; do not assign it all to
scheduling or dependencies. Do not subtract these diagnostic medians from a
normal cohort. Kernel includes summary lookup, task supply, source composition
and both grant constraints; admission includes preview/proof/Memo insertion,
but not subsequent wakeup/ancestor work. These are wall intervals, not measured
CPU cycles or a new marginal-U estimate.

**Decision:** neither expensive resource-vector arithmetic nor direct frontier
comparison/admission is established as the dominant cost. Do not implement P3-B
or deploy the unproved P3-A quotient. The next single direction is bounded
attribution of work outside these two functions (recipe preparation, tuple
identity/iteration, dependency checks and parent wakeup); no claim yet about
which of them dominates. Width4's improvement changes work and cannot supply
the missing compositional pruning proof.

P2 exact probe/control: C1 213.740/199.261ms, W92.298/90.449ms (see raw data for
all samples). Probe slow229.303ms retained. Small sequential pilot does not
establish causality or W inferiority, and no count reduction was achieved.
All20 per-rule publication counts and292 final fingerprint fields match.
All1147 captured ChildReady/TuplePriced/ParentPublished payloads match, including
exact child IDs and recorded costs. **Only128/1691 priced tuples are captured;
1563 dropped. Full admitted-prefix replay is not certified.**

## Remaining validation

Uncompleted: full replacement/invalidation history, proved envelope equivalence,
complete1691-tuple admission replay, individual late semantic rejection guards,
and exclusive CTE apply/settlement attribution. P2 root guard did not fix those
gaps. Independent
source-sensitive RF oracle, work/span/frontier, budget retry/cancel and facts
tests are reused:110 engine,10 winner/frontier,7 deferral,1 diagnostic-width
test passed on clean committed sources (different commits noted in logs).
Additional independent work/span-continuation and exhaustive Pareto tests each
passed. See `raw/*tests*.log.gz`; compile failure and earlier mixed-tree tests
are retained separately, not substituted for clean runs.
Full SQL regress and whole optimizer suite not rerun here.
Prior SQL164pass/20fail and optimizer1213pass/5fail remain unresolved/unblessed.
T1 W NI remains uncertified; no new W claim from two-block pilots.
