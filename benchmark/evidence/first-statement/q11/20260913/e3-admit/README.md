# E3-ADMIT — sequential materialization is not pure overhead

## Decision

**The proposed direction gate fails.** With the existing codec gather primitive
used for unmaterialized Sequential batches, Q11 C1 worsens193.674→202.664ms,
warm91.221→112.150ms. Separate scalars show fill bytes161.930→108.724MB but
execution wall130.451→135.541ms. Neither C1≤175ms nor execution≤105ms passes.
No first-touch/second-touch production policy, default handoff, budget, allocation,
executor algorithm, domain/F, parallel or B&B change is admitted by these results.

E2 established transferability by warming resident state; it did **not** establish
that avoiding materialization preserves decode/consume throughput. This experiment
distinguishes those propositions. It does not prove every future scan-through
implementation is unviable. The retained opt-in probe is off by default.

## Implementation and validity

- `b0d86760`: page admission can decline Sequential materialization. Initial
  production column-iterator counterexample **failed**: the codec `next_batch()`
  unconditionally called `ensure_materialized()` even after PageReader declined.
  The probe now bypasses cache-fill and uncached fallback, and reuses existing
  `gather_values_at_validated` to produce only the current batch. No new decode
  algorithm. Nullable data/null bundles are tested, including cache absent,
  cache enabled, batch boundaries and two independent iterators.
- `PARO_DIAGNOSTIC_STREAM_SEQUENTIAL=1` controls the opt-in intervention, read
  once per process and frozen into page policy. Default0 preserves materialization.
  SparseGather promotion and existing decoded-cache hits remain unchanged. Thus
  this is **not** an intervention eliminating all decoded materialization.
- `1e5ff2a7`: second pre-touch execution separately timed/drained/typed-validated.
  Same original SQL, no Q11 rewrite; custom pre-touch remains diagnostic only.
- Clean normal repeat source `fbdc2221957132f72c8250b0f93dd8403bf91932`, binary
  SHA256 `b7a2c621f2b8871f8f1eb783fb5b4170421bfe8bbe80cfc11cae9ad983d423f0`.
  SQL corpus SHA256 `a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503`;
  seed `72ccb3dc02ce127f61c06b85920a0e75c17a55a60ee341d267251c67611f62b1`.
  Full source/binary/harness/SQL/data/resource identities reside in raw reports.
- Same handoff policy, model and1691 syntheses;4 workers/2GB; independent fresh
  processes/private seed copies; target occurrence0 verified cache miss; binary
  typed90/order validation; normal trace-off with existing compile scalar only.
  Diagnostic target admission class2/fingerprint
  `5c29cf646706c8c8ba84000150211a6b` is identical in all four normal-report sidecars.
  QualityPolicySatisfied + SearchIncomplete; **not ProofComplete**. Normal plan
  identity is not inferred from pointer values across processes.
- Followed repository benchmark/start-local skills for isolated owned servers
  and serial campaigns. A test-compiler overlap violated the initial launch
  window. [RUN-NOTE](RUN-NOTE.md) records it and a fixed full repeat registered
  before replacement results. All original samples/logs are retained, not deleted
  for being slow. Main results below use the isolated repeat, not selected minima.

## Isolated normal A/B: all eight target samples

Control2→stream2→stream2→control2, warmup1/ABBA round1, bootstrap1000. W below is
the per-block median of its two measured warm executions, not a cold phase.

| Arm/block | Paro C1 ms | Compiler ms | W ms | DuckDB C1 ms |
|---|---:|---:|---:|---:|
| control-a0 |191.486|62.216|88.047|104.029|
| control-a1 |193.947|62.723|90.562|104.458|
| stream-a0 |203.026|64.500|110.531|104.873|
| stream-a1 |202.409|64.694|113.014|104.840|
| stream-b0 |202.233|64.753|112.419|105.685|
| stream-b1 |202.920|65.774|111.880|105.016|
| control-b0 |194.218|63.916|91.881|104.598|
| control-b1 |193.402|63.631|91.929|104.609|

Adjacent-pair stream/control geometric C1 ratio1.048571, bootstrap95%
[1.042450,1.056085]. Medians193.674/202.664ms; observed p95 (four samples, effectively
max)194.218/203.026ms. Paired Paro/DuckDB ratios1.850739/1.928080, one-sided95%
upper1.856724/1.934100. Not close to parity. Four pairs cannot certify population
tails or formal noninferiority. Compiler medians63.177/64.724ms; no changed search
budget/model/work count. We do not subtract separate cohort medians into phases.

The launch-window-compromised first campaign is separately retained: control-a
234.824ms with warm samples116–127ms; stream-a200.925, stream-b203.279,
control-b194.734ms. Unknown exact overlap prevents attributing its large difference
to materialization or assigning a numeric compiler-interference penalty.

## Separate E1 scalars: work avoided, throughput lost

Existing instrumentation only, two fresh blocks per arm. Its ≤2ms overhead gate
remains unproved; these are not pooled with normal times above.

| Q11 metric | Control samples | Stream samples |
|---|---|---|
| execution wall ms |129.852 /131.049|134.702 /136.379|
| filled frames |987 /987|803 /803|
| fill input bytes |161,929,711 each|108,723,767 each|
| decoder constructions |611 /611|706 /707|
| decoder input bytes |41,010,294 /41,010,373|44,251,179 /44,365,434|
| minor faults |15,162 /15,596|11,874 /12,154|
| process maxRSS endpoint bytes |358,498,304 /364,019,712|305,053,696 /298,696,704|

Avoided53,205,944 fill bytes (32.86%) and184 frame fills, not162MB. Remaining
materialization/admission paths were deliberately held fixed. More decoder
constructions/input work and slower warm execution show that resident decoded
work was being reused. This is not evidence that all fault/fill time is additive,
nor a uniquely isolated inner-loop CPU attribution. RSS is a process-lifetime
high-water endpoint, not statement-reset working set or the model's memory claim.
Cold and measured warm image IDs match within every scalar block; warm fills
262,144 then0 bytes in both arms. No allocation change is justified by faults.

## Independent pre-touch second-execution axis

Original E2 checksum SELECT, no target joins/filter, same process twice. Complete
four-row typed results validated on every run and against DuckDB. Preparation
costs remain excluded only from **diagnostic target** timing, never normal C1.

| Mode/engine | First pre-touch ms | Second pre-touch ms |
|---|---|---|
| control Paro |130.030 /131.586|89.018 /86.219|
| control DuckDB |19.882 /20.278|20.339 /22.540|
| stream Paro |143.355 /141.573|135.653 /135.639|
| stream DuckDB |20.190 /19.887|23.164 /22.761|

Control medians130.808→87.618ms, DuckDB20.080→21.440ms: repeated Paro checksum
still about4.09× DuckDB. Stream repeated median135.646ms. This includes query
evaluation/aggregation, protocol, and cached plan/page effects; it is **not** an
isolated decode benchmark or a4× claim about Q11, whose control warm is faster
than DuckDB. Do not identify first-minus-second with a sum of exclusive phases.
The second cache lookup initially reported Uncovered (two occurrence rows),
not a target cache collision. `583b245b` fixes explicit occurrence1 lookup;
the unchanged first-target gate still rejects ambiguity and hit/nonzero occurrence.
Separate scalar coverage follow-up is recorded in RUN-NOTE; results below.

The follow-up uses clean source `583b245bdec6c2c9237c8cd118592e1061851514`, binary
`42a1231fb2b5a8379a9a9ad08953b352313d9a5a60f592b926ccb0cf54d71dcd` (counter code
included, not pooled with the earlier binary). All second pre-touch occurrences
are now verified1/cache-hit and use the same image as their first execution.
Second fills are **zero in all four blocks**. Control first fills992 frames /
162,516,038B; stream537 /30,744,902B. Control second execute/fetch87.588/87.014ms
(execution86.965/86.347ms), decoder73 vs first531–533. Stream second133.071/135.829ms
(execution132.378/135.118ms), decoder672 both first and second. DuckDB second
22.731/22.399ms control,22.755/20.743ms stream. Same-frame residency alone does not
preserve throughput without decoded reuse. These scalar observations corroborate
the original timing result, not a new normal C1 improvement.

## Bounded rejection evidence

`1b2e600d` adds48 fixed guard slots and a per-binding bitset. Repeated identical
guard witnesses count once per rejected binding; distinct failed proof paths
can overlap, so counts are not disjoint. Success paths do not contribute rejected
guard witnesses. Enabled only with existing diagnostic rule profile; no enlarged
lifecycle buffer or per-binding logging. General application/output/publication
failures and late-payload eligibility guards feed the same bounded summary.
Normal does not accumulate guards; added conditional code is not claimed to
have mathematically zero overhead. This binary is measured separately from E3.

Fresh default-path report `paro-e3-guards-default.json` on the follow-up binary
reproduces discovered213/matched38/applicable0/published0/rejected38:

| Guard witness | Rejected bindings |
|---|---:|
| selective_no_reduction |36|
| selective_join_locality |2|
| aggregate_shape /top_n_shape /prefix_no_witness |38 each|
| no_output |38|

`selective_no_reduction` compares the fetched-cardinality **upper estimate** to
estimated source/carrier rows; this does not prove actual RF survivors lack
reduction. `selective_join_locality` rejects a rowid crossing a join because the
current direct-scan fetch cost proof does not cover its locality/fanout. TopN
branches do not match these projection bindings, and prefix rewrite has no witness.
Counts can overlap across proof paths; they must not be added to claim152 failures.
The rule's diagnostic apply elapsed is357µs. Lifecycle still drops25,627 records,
but all these aggregate counters survive. No guard or cost constant was relaxed.
Default report C1 1138.344ms is supplemental fresh evidence, not an E3 A/B score;
it remains BudgetLimited/SearchIncomplete. Do not attribute its difference from
historical1177ms to adding counters.

## Open gates and next decision

- Byte-axis implementation closed negative for this round: scan-local payload
  gathering already exists; DECIMAL narrowing is not approved on a raw-slot2×
  argument. No such changes were made.
- T1 W91.800 vs adjacent90.363 noninferiority remains unproved. E2's113.683 block
  median/135.035 individual warm outlier remains valid. No formal power/tail
  campaign ran this round. M3 requires a preregistered independent campaign with
  fixed sample size from **process-block variance**, engine-order balance, all
  slow samples retained, tail/resource/cross-family gates and ratio upper≤1.00.
  These pilots cannot determine required power or certify the tail; no sample
  size is invented from four unusually tight replacement pairs.
- Four known baseline failures remain unmodified/unblessed; full SQL regress
  not run. Storage271 tests passed; harness19 tests passed after occurrence fix;
  optimizer targeted128 unique tests passed in main mixed tree and repeated from
  clean committed source (counter2, late-payload16, engine110). This is
  not a claim that the optimizer/full repository is green.
- E3 gate failed, no formal M1/M3 or default production success. Keep current
  materialization default. Next bounded question should be **whether contiguous
  batch decode+consume can retain current full-page throughput without full-page
  persistence**, using this probe as a negative control. Do not implement LRU-2
  merely because fill bytes fell. The checksum4× gap warrants a scoped CPU
  profile separating decode from expression/aggregate work before an algorithm
  proposal; this experiment alone does not choose that inner-loop fix.

Recompute E3 with [analyze.py](analyze.py), using the eight r-/scalar/touch reports
in the registered order. Raw artifacts and SHA256 manifest accompany this file.
