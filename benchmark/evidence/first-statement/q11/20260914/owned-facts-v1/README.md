# Owned bridge and fact reuse: T0–T2

## Preregistered sequence

T0 precedes any optimizer optimization. Independently check d93f3b90,
3415b2de, 187140e2, 9182bea0 and 9dbfd547 with clean committed source,
original Q11, the archived immutable SF1 seed, generator-declared metadata,
4 threads, 2GB and the existing quality handoff policy. Each diagnostic records
the original SELECT's admitted fingerprint, synthesis count and published
winner count against the round anchor 4e29cd8f (5c29cf646706c8c8ba84000150211a6b,
1691, 891). Later EXPLAIN is a separately identified diagnostic and its choices
must be compared with the original SELECT before interpreting plan differences.
If all five already drift, extend the investigation to earlier committed
ancestors; do not attribute a cumulative change to a convenient midpoint.

`diagnose.py` reuses the existing server isolation, hash, result, metadata,
cache-miss and trace contracts, plus the existing external watchdog. It runs
one diagnostic block per invocation, not a performance campaign. The first
d93f3b90 check also uses the unchanged full comparator (two normal blocks and
one diagnostic); normal samples are retained as pilot only. All performance
experiments run serially. Data are privately copied; target SQL is unchanged.

T0 must explain and resolve plan drift while retaining 4fd53088's proved
native-completeness shortcut. New legal but misranked alternatives are a cost
model defect to document, not justification to delete a rewrite.

After T0, T1 refines the existing disjoint B3 ledger with per-rule construction,
statistics, fallback, settlement, staging, guard, rollback and validation
boundaries. Coverage must reach 90%; residual is explicit. Cache misses are
classified by semantic content before choosing T2. T2 preserves the fixed
round-anchor triple and rule publication map, and reuses existing fact
invalidation. Normal C1/W and diagnostic accounting remain separate.

No rule, cost, resource budget, frontier, stop or handoff policy changes are
authorized. The broad direct-only predicate experiment remains rejected.
Known optimizer and SQL regress failures remain separate and unblessed.

## T1 preregistration after transport correction

T0 found native CTE lexical-domain and aggregate fact-derivation differences
against the owned contract. Correcting those dependencies is not a
fingerprint-neutral optimization: b13d3872 is now the corrected control
(2ed1a3e2fe2f455d4411779edd54856b / 2500 / 1436). The original round anchor
remains mandatory in every comparison; its 1691/891 target is NOT restored.
The fresh b13 pilot recovers warm ratio to about .895, but C1 is 226.9ms.
T1/T2 must explain any further deviation from the corrected triple as well.

T1 uses the existing opt-in work partition. B3a includes inseparable native
producer guards; B3f is post-construction contract checking, not all guards.
B3b is propagation/gathering plus their immediate statistic adapters; B3d is
the rest of settlement/demand/layout/fact residency. B3h is node staging's
encoding/validation/duplicate handling. Apply residual stays explicit.
Rollback after apply is reported separately within B3g, not silently added
to the historic apply-only denominator. `ns` rows are exclusive; refresh
inclusive cross-checks are NEVER added to them. Rule 0 means unattributed.

Miss classification scans at most 4096 current entries, checks arena
ownership, and keeps exact operator/scalar/output/input facts/lexical CTE
identity. Only non-CTERef, non-JoinGraph root annotations that local gathering
overwrites may differ; root materialization risk remains exact. NewContent
means no equivalent LIVE entry under this local contract, not never seen in
history or absence of arbitrary algebraic equivalence. Overflow is Unverified.
This is diagnostic-only; production keys and invalidation are unchanged.

Predeclared same-binary OFF/ON/OFF diagnostic sequence: compare optimizer
wall time, exact choices and work counts. More than 10% ON overhead requires
reduced collection and a new sequence; do not use distorted data. Normal
C1/W remain trace-off, separate fixed two-block pilots, not formal parity or
T1 grant non-inferiority campaigns. All valid samples are retained.

## T2 preregistration: new-content result, defer unconsumed payload work

8bb6b8fa OFF/ON/OFF keeps corrected triple 2ed1a3e/2500/1436.
Optimizer 92.718 / 100.204 / 93.059ms: ON overhead 7.88%, below 10%.
B3 residual 0.805ms of 30.964ms (97.4% classified). Statistics is 1.431ms,
owned fallback 0.770ms; native refresh 38 calls/105 nodes, inclusive 0.913ms.
The actual cache has 65 hits/263 misses: 250 live-new, 13 proved annotation
differences, no unverified or unresident matches. Do not add a fact cache to
address a hypothetical dominant refresh cost: that hypothesis is rejected.

Staging preparation/encoding is 8.243+8.586ms. The bounded intervention defers
canonical extraction-template cloning and native cost vectors until BOTH
structural reuse paths have completed. Existing identities still merge facts,
validate context and consume identical scalar/column interning and budgets.
Owned assemble validation is retained before lookup. No output/plan choice
changes are authorized. Compare full deterministic counts and frozen choice
transcripts with corrected control, and show original 4e anchor separately.

Post-change diagnostic records staging nodes reaching payload preparation
versus actually constructing a new payload. A fixed four-block normal pilot
(same resources and handoff) will report C1/W, all samples and confidence
intervals. It is not the 36-block formal W non-inferiority or parity campaign.

## Outcome: facts repaired; original search-work target NOT restored

The late-payload/RowFetch hypothesis is rejected. All five proposed suspect
commits independently produce `c24392ee / 2053 / 1158`, so drift predates them.
The adjacent clean pair **f5adaed4 → f1ccd481** identifies an earlier actual
plan change: `5c29cf64 / 1689 / 835` → `3bf228a3 / 2256 / 1077`. The first
native CTE domain transport copied consumer statistics without re-establishing
the filtered lexical producer domain. Four CTE consumers change from an
estimated 122 rows to 3,599,788, and consumer join choices change. Later work
also changes the plan to c24392ee; this is a cumulative migration, NOT proof
that one commit explains every deviation from 4e29cd8f. The shell-compaction
artifact used a dirty source tree and is not a clean commit boundary.

There is no selected RowFetch, and late_payload_fetch publishes zero. The
later c24392ee producer adds a Projection from aggregate-input materialization;
this is not a row-id fetch. These observations do not authorize suppressing a
legal rewrite or changing its cost constants.

Two production counterexamples establish transport contract defects:

- **b3aa3365:** Native CTEFilterPushdown now enters the SAME node-local
  settlement scheduler as owned transport: producer first, then consumer
  FactId/column mapping. The red fixture used 40 versus the owned estimate 3
  and a different NDV; after repair both transports agree. These are model
  estimates, not claimed actual row counts. Cancellation, rollback, opaque
  input facts, exact outputs and adopted proofs remain checked.
- **b13d3872:** Native AggregateDimensionDeferral cleared partial `group_stats`
  and never rederived them in its fresh aggregate namespace. The production
  fixture showed 40 versus owned 4 rows and missing group NDV. Shared
  settlement repairs this; unchanged fact/dimension edges remain exact Memo
  operands, rather than being regathered. The first naive adapter failed two
  existing boundary assertions; those failures were fixed, not blessed.

The restricted native adapter fails closed if settlement erases an adopted
proof into an opaque operand. It is not a universal native rewrite transport.
4fd53088's explicit completeness shortcut remains present, as does the owned
semantic peer for unproved cases. Neither the direct-only experiment nor
rule/cost/budget/frontier/stop/handoff policy changes were reintroduced.

| Checkpoint | admitted prefix | syntheses | published winners |
|---|---|---:|---:|
| Original round anchor 4e29cd8f | 5c29cf64 | 1691 | 891 |
| Five suspected commits, each independently | c24392ee | 2053 | 1158 |
| First CTE transport correction b3aa3365 | 3b553c24 | 474 | 343 |
| CTE + aggregate facts corrected b13d3872 | 2ed1a3e | 2500 | 1436 |
| T1 8bb6b8fa, all OFF/ON/OFF arms | 2ed1a3e | 2500 | 1436 |
| T2 d6fe8172, all OFF/ON/OFF arms | 2ed1a3e | 2500 | 1436 |

Full fingerprints, exact integer choices, costs, per-rule counts, source,
binary, harness, SQL, seed and resource identities are in raw reports and
[recomputed summary](summary.json). T1/T2 fixed-work counts and all final
winner fields match the corrected control. Wall-clock 20ms checkpoint
contents differ: separately timed progress is not fixed-prefix equivalence.
The original 1691/891 target is **not** met: +809 syntheses and +545 winners
remain. Returning to old estimates merely to restore an old hash is not a fix.

The b13 first SELECT and later EXPLAIN have exactly equal **1371 final_winner_2
values / 37 choices**. This licenses that particular DAG comparison, not an
assumption that EXPLAIN always executes the same image. Both branches have
partial/final aggregation; selected hash prefixes are the fact customer key
on partials and customer_id on finals; CTE consumers estimate 122 rows. The
new and 4e winner transcripts have the same operator/implementation sequence,
but ten physical fingerprint positions differ. Complete RF/prefix/statistics
differences against the original 4e image have not been independently isolated
as operator-level runtime costs. No operator-time causal attribution is made.

## B3 complete accounting and cache result

ON interval: 100.202ms, sum exactly equal to total; overall classified 91.77%,
unclassified 8.250ms remains explicit. B3 is 30.964ms, with 0.805ms residual:
**97.40% subpartition coverage**. These are diagnostic wall times, not CPU
samples or normal-path phase estimates. Refresh inclusive time is a cross-check.

| B3 interval | exclusive ms | entries | µs / interval |
|---|---:|---:|---:|
| a native producer, including inseparable producer guards | 3.744 | 210 | 17.83 |
| b statistics and immediate adapters | 1.431 | 368 | 3.89 |
| c owned bridge/rewrite | 0.770 | 110 | 7.00 |
| d settlement/demand/layout/fact residency | 7.055 | 139 | 50.76 |
| e preparation/transaction/transport traversal | 8.243 | 705 | 11.69 |
| f post-construction semantic contract | 0.257 | 169 | 1.52 |
| g sidecar rollback | 0.073 | 15 | 4.87 |
| h node encoding/validation/duplicate/publication preparation | 8.586 | 749 | 11.46 |
| explicit apply/rollback residual | 0.805 | 271 | 2.97 |

Intervals have different granularity; none is called a per-combination kernel
cost. 439 rule attempts are counted BEFORE structural preflight; 210 enter
the native-dispatch portion of apply. Apply interval entries also include
TransformContext rollback, so 271 is not a count of expensive rewrite calls.
The units are consistent with bounded local traversals; they do not justify
assigning remaining scheduling/finish time to facts or constructing a cache.

| Rule | matched / applicable / published / ineffective / rejected | B3 ms | cache hit / miss (site) |
|---|---|---:|---|
| AggregateDimensionDeferral | 165 / 45 / 45 / 0 / 101 | 11.921 | 46 / 155 native |
| AggregateInputMaterialization | 38 / 23 / 23 / 0 / 10 | 3.385 | no settlement lookup |
| PredicateTransfer | 92 / 88 / 76 / 12 / 3 | 13.396 | 19 / 85 owned |
| JoinRegionEnumeration | 4 / 4 / 4 / 0 / 0 | 0.805 | 0 / 6 owned |
| CTEFilterPushdown | 1 / 1 / 1 / 0 / 0 | 1.381 | 0 / 17 native |
| AggregateNonNullInput | 8 / 0 / 0 / 0 / 8 | 0.077 | no lookup |

Matched includes earlier cached/preflight outcomes; these categories must not
be summed into a fictitious count of actual apply calls. Full per-rule eight
subbuckets, interval counts and refresh shell-size histogram are in summary.
AggregateJoinSubsumption's 176 matches / 156 rejections never enter B3 in this
cohort; matching/preflight belongs outside this apply subpartition.

Settlement: **65 hits / 263 misses**. Native: 46/172; owned: 19/91. Misses:
250 live-new and 13 proved old-annotation differences (11 owned predicate,
2 native deferral); zero unresident or unverified cases. This is a precise
local-contract equivalence test, not arbitrary alpha/algebraic equivalence.
Native-domain refresh: **38 calls, 105 visited nodes, 0.913ms inclusive**;
it still has no fact-cache lookup. Neither those calls nor every live-new
miss is assumed redundant. The proposed dominant uncached-statistics
explanation is not supported on the corrected tree. No second cache added.

## T2 result and normal C1/W

Deferred preparation is correct but **has no demonstrated timing benefit**.
584 nodes reach preparation, 405 construct the payload template/cost vectors;
179 avoid that work. That difference includes all earlier return paths, not
179 proven duplicates. T2 retains fact merging, assemble validation and
occurrence checks. Its encoding bucket is 8.631ms versus control 8.586ms;
total ON optimizer 100.460 versus 100.202ms. OFF samples 102.198/90.915ms
also overlap the control range; do not claim a speedup. T2 ON overhead versus
its bracketing OFF mean is 4.04%. The small deferral removes unused work but
does not resolve the dominant staging/transport cost.

Final **four-block pilot**, original SQL, fresh process/session, first target
SELECT, verified cache miss, normal trace-off, 4 threads/2GB, all 90 rows and
typed schema/peer order checked:

| | Paro | DuckDB | paired ratio [95% bootstrap CI] |
|---|---:|---:|---|
| C1 median | 242.841ms | 122.845ms | 1.9840 [1.7714, 2.2547] |
| C1 p95 (four samples) | 308.403ms | 149.896ms | — |
| W median | 103.880ms | 114.796ms | 0.9026 [0.8558, 0.9503] |
| W p95 (eight samples) | 121.399ms | 132.829ms | — |

Every slow sample is retained, notably Paro C1 308.403ms and W 121.399ms.
Normal compiler per block: 78.430, 76.149, 67.731, 74.243ms; optimizer:
77.293, 75.312, 66.951, 73.505ms; syntheses always2500. No median subtraction
across cohorts is used to attribute execution. The W pilot supports regained
heat advantage over DuckDB, not restoration of the historical .872 interval
or formal 36-block non-inferiority. The intervening b13 two-block pilot has
W CI crossing 1.0 and is retained. A new clean original-4e pilot is also
retained (C1 210.092ms, W ratio .9245), not replaced by older faster numbers.

QualityPolicySatisfied + SearchIncomplete; **not ProofComplete**. QPS in the
two final OFF diagnostics is 77.452/70.316ms (diagnostic only). **M1 and parity
not achieved**, production default stop/handoff policy unchanged. Default-path
new C1 was not measured this round. T1 grant W non-inferiority remains unclosed.

The admission fingerprint and exact choices are directly captured in the
separate diagnostic SELECTs. Normal side-channel records cache identity and
2500 syntheses, but does not emit an independent per-occurrence image hash;
normal/diagnostic image equality is not claimed as separately observed.
Normal peak RSS was not sampled. Diagnostic watchdog peaks are 423.5–442.9MB
(decimal bytes), below its 2GiB stop limit; these are not normal memory peaks.
All process argv, private-copy seed identities and the 4-thread/2GB declared
resource envelope are retained in reports.

## Tests, limitations and next single direction

- Clean incoming 5ffc5a68 optimizer: 1299 passed / same five failed.
- Corrected b13: 1303 passed / same five; T1 and T2: **1306 passed / same five**.
- Partition tests4 and settlement tests29 pass; full run covers source-sensitive
  non-selected RF choice, resource/work-span, invalidation, cancellation,
  rollback and budget-retry tests. New duplicate transport assertions verify
  same payload with no deferred template construction.
- Known failures: nary dimension-sharing default-envelope stability;
  statistics-read-cache rollback/reinsert/merge; explicit mark-to-semi
  isolation; nested RF source lane; multi-partition CTE publication. Unfixed,
  unblessed. SQL regress **not run**; historical164/20 is not a current rerun.
- Materialization-fact exploratory fixture in the separate oracle worktree
  was not run or integrated. Failed intermediate fixture/adapter logs remain
  in raw, clearly not counted as passing results.
- Diagnostic cache classification scans at most4096 entries; exact same-key
  nonresidency is separate; no cross-statement/epoch reuse was introduced.
- Main user staged/unstaged work is outside these clean experiment trees.

Next single direction: **eliminate repeated settled-occurrence → staging
transport/layout/fact assembly while preserving fresh fact merges**, using
the now-correct node fact identities. Do not enlarge the fact cache or delete
owned peers. B3e/B3h remain about16.8ms; the two-object deferral proved that
canonical-template preparation alone does not account for them. The extra
809 syntheses versus the original anchor remain an explicit unresolved
search-work difference, not silently renamed a new performance baseline.
