# Cross-group domain continuation: root-contract diagnosis

Date: 2026-09-17

This is a diagnostic follow-up to `cross-group-domain-continuation-v1`. It is not a
normal C1 campaign and must not be used as a parity result. The source change committed
for this investigation is `4a75752762b99c8c23585ea7f704e18b34040789`.

## Question and fixed inputs

The question was whether the early PredicateTransfer/AggregateDimensionDeferral
products were late because they crossed a missing Memo/physical continuation boundary.
The control was the clean `99ef4f319d35457053051807065107073639c67a`; the probe used the
same source plus the resident-contract repair committed above. Both used the original
SF1 Q11 SQL (`11.sql` SHA-256
`1f3c2697b2a82f597f9e8a570413be65ab7a02da03c1fe4c3ea7b725735433b8`), the captured
2 GiB/4-worker seed, the same handoff policy and the same diagnostic harness. The
release probe binary SHA-256 is
`8033a8bee6f824ab501adc0ceed554b72cee61de4a1c11194c6ff3748dc05b73`.

The raw diagnostic JSON and logs are stored in this directory as
`control-diagnostic.*` and `rebind-diagnostic.*`. The one-off EXPLAIN harness reports
`status=error` because its diagnostic request did not contain exactly one target
operation trace; therefore its optimizer counters and plan are diagnostic evidence
only, not C1 measurements. No normal performance sample is claimed from this run.

## First proven break

The 18 `PredicateTransfer` `ApplicationError`s all had the same concrete reason:
`resident contract disagrees with arena output layout`. They occur during apply and
are rolled back, not converted into an empty result. The cause is structural:

1. settlement records a `ResidentNodeContract` for the pre-freeze root layout;
2. `freeze_arena_output_layout` appends the same occurrence identity with the target
   group's final projection/layout;
3. staging consumes the stale contract and rejects the otherwise valid result.

The errors were on alternative bindings (including later alternatives for groups 157
and 159), not on the successful selected 103/106 chain. The selected path published
logical alternatives 103 and 106 at roughly 12 ms, then child groups 87/88 received
their first physical runs around 19.8/20.2 ms and published their next products around
22 ms. The first complete quality candidate was much later. This disproves the claim
that the 18 errors alone explain the selected candidate's late arrival.

The exact causal classification is therefore:

| stage | observed result |
| --- | --- |
| binding | 103/106 were produced; 18 other bindings failed during apply |
| logical publication | successful products were published |
| physical readiness | child physical work was available and ran; no missing continuation was proven |
| ancestor combination | broad promotion of the direct successors caused an unbounded search expansion and timed out |
| quality | the root eventually became `QualityPolicySatisfied`; it remained `SearchIncomplete` |

## Implemented repair

The committed repair re-derives only the root resident identity after semantic output
freezing, reusing the already-settled input facts and the existing session identity
catalogs. It does not re-settle the arena or create a second catalog. Native contracts
remain on the native path and are not passed through the settlement rebind helper.

This closes the real contract error and keeps `ApplicationError` distinct from a legal
no-output/rejection. It does not change budgets, cost constants, stopping policy,
quality verification, child choice preservation or the physical search domain.

## Diagnostic comparison

The diagnostic runs used the same one-off EXPLAIN harness. Instrumentation was removed
before the release binary was rebuilt; the archived JSON/logs are the bounded diagnostic
cohort, not normal C1 evidence.

| metric | control | contract-rebind probe |
| --- | ---: | ---: |
| optimizer wall (ms) | 69.347 | 81.111 |
| cost syntheses | 1,757 | 2,246 |
| published winners | 1,110 | 1,312 |
| Memo groups | 186 | 227 |
| physical subproblem evaluations | 558 | 679 |
| ReadSet rebuilds | 879 | 1,065 |
| recipe reprocesses | 879 | 1,159 |
| quality evaluations | 98 | 96 |
| first complete aggregate-region time (us) | 51,397 | 61,298 |
| PredicateTransfer apply errors | 18 | 0 |
| quality policy | satisfied | satisfied |

The canonicalized selected plan shape is equal after removing transport IDs; the probe
still generated more legal alternatives and did more search. Thus the repair is retained
as a correctness/contract fix, not claimed as a performance win. The attempted broad
queue promotion was reverted: it increased Memo/search work and did not establish a
safe causal improvement.

## Fresh normal control/probe pilot

After the diagnostic investigation, a serial five-block normal comparison was run with
the same original Q11, seed, 4 execution threads, 2 GB limit, handoff policy,
`generator-declared` metadata track, one cold target per fresh process, one warm
measurement, trace off, verified cache miss, and full typed/order result validation.
The harness was `tpcds_compare.py` SHA-256
`828c13abd199154985d82923d562dd9020d571acd00cfe7605963bd6a0bcb244`; the Paro seed
SHA-256 was `9176b57eec70e0963228844168668eb027f6f792b12aececbe93cb6003ee28eb`.

| arm | source | binary SHA-256 | C1 Paro median (ms) | C1 DuckDB median (ms) | C1 ratio, 95% bootstrap CI | W Paro median (ms) | W ratio |
| --- | --- | --- | ---: | ---: | --- | ---: | ---: |
| control | `99ef4f31` | `b09a8698525580f49b89ae81d2937679de74daed2f7aba1fb571ed0af0c4bc6c` | 174.712 | 122.387 | 1.448 [1.386, 1.547] | 72.668 | 0.664 |
| rebind probe | `4a757527` | `8c0764a2868cc6694156bbf0d4ecb2b7bbb9a23f7f95e314031ae34f48026a8f` | 233.408 | 140.841 | 1.623 [1.537, 1.713] | 106.475 | 0.741 |

The per-block cold ratios were control `1.637, 1.448, 1.409, 1.395, 1.366` and
probe `1.528, 1.650, 1.668, 1.792, 1.494`; all ten blocks were retained. DuckDB
also drifted between the two serial arms (`122.4 ms` versus `140.8 ms` median), so
the cross-arm C1 difference is not a clean causal estimate. It nevertheless provides
no repeatable probe improvement. Normal C1 did not expose a compiler scalar; the
separate same-image diagnostic compiler values were `69.347 ms` control and `81.111 ms`
probe, with the counter expansion shown above. This distinction is intentional: no
cross-cohort subtraction or diagnostic time was used as a normal C1 claim.

## Validation and remaining status

`cargo check --locked -p paro-optimizer --lib` passes on the shared tree after the
selective commit. The optimizer test target still cannot compile because the mixed tree
contains pre-existing stale fixture/API mismatches (`settle_arena_in` arity and the
`StagingInput::Native`/`StagingRequest` fields); these were not silently blessed or
changed in this task. A release `parod` build passes.

No five-pair fresh C1 campaign is reported: the only probe run in this investigation was
the diagnostic EXPLAIN cohort and failed the harness's exact-target-trace C1 precondition.
The subsequent five-block normal control/probe pilot is recorded above; it is a
directional pilot, not a formal parity/power gate. Current Q11 status remains
`QualityPolicySatisfied + SearchIncomplete`, not
`ProofComplete`; compiler `<=30 ms`, `<10 ms`, M1/M2 and DuckDB parity remain unmet.

The first selected-path work still lacking proof is the concrete physical/quality
dependency that turns the already-published 103/106 products into the complete root
candidate. The evidence does not justify another continuation, cache, queue-priority or
parallel-search mechanism until that dependency is isolated.
