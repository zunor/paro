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
statement-only peak or a working-set contract. Fault counts are not fault time;
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

T1 production contract and its separate performance evidence are recorded below
once validated; E1 observations are not attributed to T1.
