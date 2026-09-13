# E1-COLD / T1-PROD — 2026-09-13

## Model correction

Recomputed from original per-occurrence T1 records, rather than accepting the
proposed interpretation: handoff U=20.41391656µs, F=6.90456709ms; default
U=19.56262992µs, F=441.37410748ms. The old residual/N comparison omitted an
intercept and does not reject fixed marginal synthesis cost. The earlier §6.3
stop is lifted by the user's revised task. Two-point fits do not independently
prove constant U or identify F causally. Default witness counters are identical
across arms; handoff counters and logical expression counts differ, so its F
must not be described as experimentally fixed. Dependency maintenance remains a
scalability risk, not this round's implementation direction.

## E1: cold excess reproduced, causal attribution incomplete

Clean b594e968 baseline, four fresh processes: C1 median266.318ms; same-block
C1−W−compiler residuals48.734/41.761/23.933/27.104ms (median34.433ms).
The <15ms stop condition did not trigger. These differences are not exclusive
phase timers.

Instrumentation is opt-in `PARO_COLD_WORK_EVIDENCE=1`, with existing compile
scalars enabled and statement trace disabled in all normal samples. It observes
the actual binary SELECT execution and is read after timing through the existing
evidence query. Warm executions have distinct execution IDs and the same process-
local image identity as cold. Image pointer identity is not a cross-process
fingerprint. All normal blocks validate the complete ordered, typed90-row result.

| Clean source / arm | Paro C1 median ms | DuckDB C1 median ms | W ratio |
|---|---:|---:|---:|
| 142917df / scalar off | 263.424 | 106.330 | .875 |
| 142917df / scalar on | 259.523 | 105.382 | .883 |
| 6fce0fc0 / occupancy on | 255.181 | 104.889 | .888 |
| 6fce0fc0 / occupancy off | 259.359 | 109.821 | .880 |

Each arm has four fresh blocks, a separate mandatory diagnostic block, and an
independent oracle run; every valid slow sample is retained. v1 ran off→on;
v2 ran on→off. These are pilots, not parity or formal noninferiority campaigns.
On/off median C1 shifts are −3.90/−4.18ms, exceeding the approximately2ms
absolute screen. Compiler and DuckDB timings also drift. This does **not** prove
negative collection overhead or certify ≤2ms overhead. Instrument overhead
acceptance remains inconclusive. No collection was moved outside C1 to improve
the result. The fixed-mask extension is opt-in only and is not production policy.

### Observations and limits

For v2, same-image execution cold-minus-warm is38.889/37.437/40.451/40.000ms.
Every cold execution fills987 frames, reading/copying161,929,711 input bytes;
warm fills0–1. Cold decoder construction is613–615 versus about150 warm,
with roughly26MB more decoder input. Dictionary builds7 and zone-map builds26
vanish in warm. Bytes are input volume, **not allocated capacity or unique disk
bytes**. Buffer fills include decoded pages; do not add their bytes to decoder
input as total unique data.

Minor faults are15,105–15,529 per cold execution; major faults0. The getrusage
window is execution, not the entire parse/compile/SELECT lifecycle; whole-statement
fault attribution remains unmeasured. RSS records are
process-lifetime getrusage high-water marks at execution boundaries, not a reset
statement-only peak or a working-set contract. Cold boundary maxRSS is
130,990,080–138,936,320 bytes before and377,470,976–383,942,656 bytes after;
warm boundaries retain the higher process high-water mark. Minor-fault warm
medians are324–601 per block. Fault counts are not fault time;
they cannot establish that first-touch causes most latency. Dictionary and zone
metadata construction are tiny measured worker costs. Global/local initializer
counts are73/137 in cold and warm; local initialization has a small worker-time
delta. Lazy first-batch allocations, resize work, scheduler scratch, and decoded
page allocation before its fill scope are not comprehensively covered.

Worker elapsed timers subtract nested instrumented scopes, but still include
preemption/wait and overlap across workers. v1's approximately114ms positive
worker delta must not be summed against a40ms wall difference.

v2 adds64 fixed occupancy masks, not event tracing. Bit0=buffer fill, bit1=decoder,
bit2=dictionary, bit3=zone map, bit4=global init, bit5=local init. Simultaneous
work remains in a mixed mask; nested same-worker work is exclusive. Cold union
is59.75–61.71ms versus4.72–4.85ms warm. Per-block union increases54.90–56.93ms,
while uncovered wall **decreases**14.90–18.03ms. Their signed sum matches the
direct execution delta, but this is a temporal partition, not causal critical-
path attribution. For example, roughly18.6ms cold is simultaneous buffer and
decoder activity and cannot be assigned wholly to either.

**The ≥80% mutually attributable cold-extra-wall gate is not established.**
No missing time is assigned to faults, memory first touch, decoding, or scheduler
wait by assumption. Per the task stop rule, do not implement an executor fix
from these counts. The strongest measured candidate for subsequent *isolation*
is buffer fill/decoder cold work; a causal separation is still required before
choosing between page materialization, decode reuse, or allocation changes.

### Implementation / tests

- 142917df: bounded occurrence scalars and actual warm/image identity capture.
- 6fce0fc0: fixed overlapping activity-mask wall accounting.
- Clean common tests: v1 2passed; v2 3passed, including cancellation epoch,
  concurrent-window invalidation, nested accounting and independent mask clock.
- Harness occurrence-selection test: 1passed.
- Both clean release builds passed. The intermediate dirty-tree check failed on
  a T1 missing import; it is not presented as a successful clean build.
- Baseline four optimizer failures are not fixed or blessed. Full SQL regress,
  formal power/tail/W gates, and cross-family performance are not yet run here.

## T1-PROD: production resource contract and separate pilot

Code `c23ae52ad6975b409b651687559121a3a9bfab81`, release binary SHA256
`2bcde8dc5ca306756da8e0c67d38c91766268e516427fc0b9a89893341808ea4`.
Both arms use clean committed source and E1 collection **off**. Normal is
trace-off/cache-miss with full ordered typed90-row checks; diagnostic is separate.
Each arm has4 fresh blocks; full binary/SQL/seed/harness identities are in raw
reports and archive digests are in `raw/manifest.json`.

### Contract

- Freeze available query-pool envelope and worker capacity with the statement;
  derive the expected declared class from that snapshot and configured limits.
  It is not free RSS, a concurrency-adjusted fair share, or a future reservation.
  The compile cache key uses the same snapshot and expected class.
- Preserve exact immutable all-class mandatory FrozenCandidates. Search optional
  work only for the expected class, without narrowing the declared grant registry
  or bypassing RequiredEnforcement/invariant-goal derivation. Retain mandatory
  plans in the extracted portfolio, including the expected class's safe plan.
- Portfolio explicitly lists optional-attempted and mandatory-only classes;
  deferred classes become `OptionalGrantDeferred` obligations. An active goal
  finishing is not full-portfolio closure. Cancellation and partial mandatory
  construction cannot publish a partial portfolio.
- Admission comparator, budget constants, rules, model and quality policy are
  unchanged. Unavailable/rejected classes do not get retagged plans: use an
  existing verified safe class or fail closed. Production uses the mechanism
  without a grant-probe environment variable. Quality handoff itself remains the
  existing independent opt-in policy; it was **not** made default this round.

### Fresh results

| Arm | Paro C1 median / p95 ms | DuckDB C1 median ms | paired C1 ratio [95% CI] | Paro W ms | synthesis |
|---|---:|---:|---|---:|---:|
| clean E1-off handoff control | 259.359 /273.045 | 109.821 | 2.397 [2.341,2.454] | 90.363 | 4903 |
| T1 production + existing handoff | 201.339 /203.086 | 109.116 | 1.839 [1.801,1.877] | 91.800 | 1691 |
| T1 production + default stop | 1177.129 /1225.845 | 106.828 | 10.885 [10.621,11.030] | 96.236 | 7351 |

Handoff C1 samples:201.701375/203.086458/200.977375/197.103084ms. Compiler
same-occurrence samples68.790/70.162/69.340/68.358ms, versus control median
127.012ms. Synthesis4903→1691 (−65.51%); C1 observed reduction58.020ms (−22.37%).
These are separate adjacent clean builds, not a same-binary randomized causal
estimate, and no execution phase is inferred by subtracting campaign medians.
No executor algorithm was changed.

Diagnostic handoff admission is class2,
`5c29cf646706c8c8ba84000150211a6b`, exactly the clean control/prior probe target.
Default is class2, `33eee07b2590b88a377c533ff2267634`, exactly the earlier default
control/probe/final identity; it is not the handoff identity. Normal measured
samples have no heavy admission trace; do not describe diagnostic identity as a
per-normal-sample fingerprint capture.

Diagnostic handoff quality is satisfied at63.875ms on the search profile clock;
normal compiler times above are not derived from that diagnostic cohort.
Handoff is QualityPolicySatisfied + SearchIncomplete; default is BudgetLimited
+ SearchIncomplete. Neither is ProofComplete. Default C1 remains far from parity.

Pilot gates: synthesis≤2000 and handoff C1≤210ms pass; diagnostic admitted
fingerprint is unchanged. Warm91.800ms remains near the intended execution
quality, but is1.44ms higher than the adjacent control median90.363ms. Paired
W/DuckDB ratio is.871576 [95% CI .856393,.888424], versus control.880; this is
**not** a formal zero-margin T1/control noninferiority proof. Formal W/tail,
cross-family and independently repeated campaigns remain unrun. M1≤200ms is
not met; neither M2 nor strict parity is claimed. Do not mark all production
performance acceptance complete from these pilots.

### Tests / retained limits

Clean c23ae52a: full engine109passed, context `compile_`5passed, session
`lazy_grants`2passed; clean release passed. Shared-tree preliminary grant23passed
overlaps engine coverage and is not added to a fake total. Tests exercise actual
cache lookup misses across resource snapshots; immutable-image execution under
smaller admission with duplicates/NULL/order; memory-only class fallback;
grant-invariant sharing; RequiredEnforcement; deferred obligations; cancellation,
rollback and incomplete mandatory portfolio refusal. Full SQL regress and the
entire optimizer suite were not rerun; the baseline4 failures remain unmodified.

E1 is stopped at incomplete attribution, with its limitations above. T1's
production engineering contract is implemented and targeted tests pass, while
formal noninferiority remains outstanding. Next recommendation is **causal
isolation of buffer-fill/decoder first-execution work**, not F maintenance or
an allocation fix justified solely by minor-fault counts. Do not extend this
round into executor changes, parallel search or global pruning.
