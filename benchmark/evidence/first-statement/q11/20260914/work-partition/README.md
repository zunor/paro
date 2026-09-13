# Optimizer exhaustive partition and early quality-request rejection

## Decision

The requested partition gate passes. The B5+B10=25–35ms hypothesis is rejected:
these scopes total3.889ms, not the main missing cost. Quality work totals18.276ms,
including8.243ms producing requests that can subsequently be rejected. Matching
is2.048ms versus23.685ms applying/rolling back rules; dispatch-first matching
optimization is not the main rule-side opportunity on this workload.

Measurements below are wall time, not CPU time. No residual is assigned by
assumption. The old20.4us marginal coefficient is not kernel time and does not
identify maintenance causally. In particular, quality/root production work
correlates with search progress too.

## Exact accounting

Clean source85bdd979, original archived2418-byte Q11, two fresh statement-trace-OFF
diagnostic blocks; identical compiler start/end Instants join each ledger to its
same-occurrence compile-work scalar. Arithmetic means below are across those
same two occurrences, so the columns remain additive (not medians from separate
cohorts). Total67.065542ms; classified64.180335ms, residual2.885207ms (~4.30%).
Per-block coverage95.7213%/95.6747%. All deltas including unknown sum exactly to
the instrumented optimizer interval. JSON writing is after optimizer but remains
INSIDE diagnostic compiler/C1; no normal work is moved out of timing.

| Bucket | Exclusive ms | Scope entries | us/entry |
|---|---:|---:|---:|
| B0 preparation | 1.6165 | 2 | 808.250 |
| B1 logical agenda/orchestration | 2.4739 | 1 | 2473.944 |
| B2 dispatch/matching | 2.0476 | 7830 | .262 |
| B3 apply/rollback | 23.6847 | 492 | 48.140 |
| B4 insertion/commit | .3207 | 219 | 1.465 |
| B5 transformation/ancestor scheduling | 3.4239 | 2530 | 1.353 |
| B6 recipe/implementation | 2.0472 | 1033 | 1.982 |
| B7 physical subproblem/tuple bookkeeping | 5.9812 | 1884 | 3.175 |
| B8 combination kernel | 1.0397 | 1691 | .615 |
| B9 candidate admission | .7520 | 1691 | .445 |
| B10 publication/dependency registration | .4647 | 5332 | .087 |
| B11 orchestration | .5143 | 57 | 9.023 |
| B11 evidence excluding domain check | 3.8687 | 73 | 52.996 |
| B11 selected domain consumption check | 2.9324 | 73 | 40.170 |
| B11 production request | 8.2429 | 72 | 114.485 |
| B11 verification/freezing | 2.0487 | 228 | 8.986 |
| B11 exact fact reads | .6691 | 396 | 1.690 |
| B12 verification/extraction/checkpoints/reporting | 2.0520 | 3981 | .515 |
| Explicit unclassified | 2.8852 | — | — |

Entries are instrumented calls, including nested calls in the same category,
early returns and cache hits; they are NOT unique evaluations. B1 is one complete
logical loop exclusive of nested work, not one pop taking2.47ms. B7 includes
tuple identity/cache traversal outside kernel/admission, not just child lookup.
B10 includes many cheap registration calls; not5332 new candidates. B11 freeze
includes wrapper/verifier/DAG scopes; not228 independent frozen roots. Tiny
B12 average reflects thousands of empty checkpoint checks, not a full verifier
running in.5us. This is why counting only317bindings or891publications would
misidentify scope costs.

Same-binary OFF/ON/OFF normal optimizer medians66.324/67.065/65.103ms:
ON versus pooled OFF+1.861%, below10%. Initial13-bucket version459243d1 also
passed (+1.715%, coverage95.70–96.08%); retained, not cherry-picked away.
Snapshot and previous two-phase timers OFF, frontier default256 unchanged.

Cross-check against business counters in the SAME heavy diagnostic occurrence
(never divide normal time by a diagnostic count and claim exact accounting):
B1/1370 task requests2.218us; B2/317bindings6.681us; B3/295apply84.637us;
B4/98inserted3.383us; B5/98inserted37.669us; B6/364implementation requests6.414us;
B7/1075subproblem requests6.110us; B8/1691syntheses.662us;
B9/1691admissions.464us; B10/891publications.623us;
B11production/72requests120.602us. Registry619reuse/741unique evaluations,
299necessary combination recomputations remain unchanged. These are workload
normalizations, NOT costs of individual operations: B2 includes root dispatch
outside317bindings; B5 handles more than98 logical insertions; B7 includes
reuse/continuation visits; B1 also performs orchestration unrelated to registry
requests. No implausible per-pop or per-publication interpretation is required.
The scope-entry table supplies true same-occurrence invocation denominators.

## Conditional implementation

4e29cd8f moves the existing current-read-set and request preference check BEFORE
selected binding construction. A request's deficit/model cost/CandidateId rank
does not depend on binding payloads. Equal or worse current requests retain the
same existing request; stale child facts still require replacement even if its
rank is worse. No evidence acceptance, policy, budget, model, frontier, output,
candidate ownership or default handoff policy is changed. No new cache.

Independent counted-constructor test covers equal/worse/better requests and
child-only fact invalidation under reversed group order. Clean engine111 tests,
quality-domain17 and accounting3 pass. RF nonselected-child reversal, work/span,
phase footprint, budget retry, grant cancellation and closure oracles are in
the engine run. Additional work/span tradeoff1 and independent Pareto1 pass.
Logs retained. Compiler check passed before measurement.

## Measured implementation outcome

Same trace-off instrumented cohort accounting: production request8.242898→
.743646ms (−7.499253ms,−91.0%), total quality18.276125→10.648248ms;
optimizer67.065542→58.975896ms. Residual remains2.884145ms; it was not relabeled
to create improvement. Producer calls remain72; the change avoids constructing
bindings for requests that the unchanged comparison rejects, rather than fewer
candidate evaluations or fewer searches. No new caching layer was added.

| Normal trace-off arm (2 fresh blocks each) | compiler ms | optimizer ms | C1 ms | W ms | C1 p95 ms | Paro/DuckDB ratio [two-sided95% CI] |
|---|---:|---:|---:|---:|---:|---|
| control first |67.053|66.324|198.757|91.484|198.989|1.8724 [1.8695,1.8753]|
| control second |65.755|65.103|195.894|91.264|196.573|1.8533 [1.8458,1.8608]|
| probe first |59.574|58.865|191.197|92.329|191.299|1.7944 [1.7858,1.8031]|
| probe second |56.591|55.929|186.387|91.264|186.453|1.7782 [1.7738,1.7827]|

Small fixed pilot, sequential versions: not a formal causal C1 estimate, power
campaign, tail certification or W noninferiority claim. ON diagnostic compiler/
C1 are archived separately (probe59.816/189.764ms); do not pool them into normal.
The raw harness's `normal_trace_off`/eligibility flags only know about statement
trace, not this opt-in partition switch. `summary.json` explicitly relabels ON
arms `diagnostic_partition_trace_off`, primary eligibility false; raw reports
are preserved unchanged.
The two instrumented blocks each retain exact typed/order90 and cache-miss.
Heavy diagnostic first qualified candidate control62.005/61.330ms, probe54.511/
53.717ms; these are not normal C1 phase times. All unfinished obligations and
QualityPolicySatisfied/SearchIncomplete semantics remain unchanged.

Hard checks: all six refined-control/probe arms preserve1691syntheses,
180groups/278logical expressions/495physical expressions,
891published winners,98logical outputs,317bindings,364implementation requests,
1075subproblem requests,299recomputations,1370registry requests/619reuse/
741unique evaluations,73quality evaluations. Per-rule publication map is exact.
Admitted class2 fingerprint remains `5c29cf646706c8c8ba84000150211a6b`.
All292final fingerprint fields and1147captured ChildReady/TuplePriced/
ParentPublished payloads (excluding timestamps) match. Only128/1691TuplePriced
events were stored;1563 dropped. This is NOT a complete admitted-prefix replay.
There is no evidence of faster wall-stop producing additional work here: the
declared work counters and selected plan stay identical.

Next implementation target is rule construction, not further dispatch tuning:
apply/rollback still~23ms, with PredicateTransfer the largest existing per-rule
match+apply total (~8.8ms in control diagnostic). Reduce redundant construction
within the existing transfer contract; do not reopen envelope pruning, frontier
width, domain identity F1–F6 or redefine search completeness. This round does
not claim compiler<30ms or that all remaining rule work is removable.

## Boundaries

This remains QualityPolicySatisfied with SearchIncomplete, not ProofComplete.
Handoff is experimental; default production parity is not achieved. No36-block
formal C1/W campaign, no new W noninferiority or M1/M3 admission claim. Prior
T1 W ratio1.00224 with upper1.00824/1.01131 remains uncertified.
SQL164/20 and optimizer1213/5 are separate historical runs, not rerun or blessed.
No executor/domain-identity/allocation/parallel/B&B changes. Width4 is explicitly
registered as an overfit observation, not adopted.

Raw reports carry source/binary/harness/SQL/seed/resource hashes, typed/order90
and miss verification. Inherited metadata asymmetry (Paro declared keys versus
DuckDB empty keys) is disclosed and unchanged. Every valid slow sample is kept.
`analyze.py` joins ledger totals to individual compile-work occurrences;
`archive.py` creates lossless gzip files and raw/gzip SHA256 manifests.
