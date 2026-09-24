# Aggregate contracts: fresh control/probe comparison

Conclusion: **no established broad C1 improvement**. Q11 observations are
favorable, but concurrent host activity and DuckDB drift prevent attributing
the size of that improvement to the code. Q04 and Q74 batch pairs disagree.
There is no parity, tail-latency or non-inferiority certification.

## Eligible cohort

Use only [replacement-cohort](replacement-cohort/), registered in
[replacement-registration.md](replacement-registration.md) (`ae512a6d`) before
collection. The original [registration](registration.md) was committed in
`5b83c533`; its failed build-identity comparison is retained separately below.

- Control: clean `e37ae93f`; binary SHA256
  `4bb7edfca32192625ab61e5dd967a1be2b239d57e46e33c63020672c22d5079b`.
- Probe: clean `5b83c533`, runtime implementation `0c58eb3d`, `57a98fe2`,
  `09322560`; binary SHA256
  `fc712ef2612675767c29b29a0a88d632d2b698097ca0e2316638c04257ef197f`.
- Each arm/query has six fresh processes in P1/C1/C2/P2 order, three ABBA
  warm rounds per process (36 timed warm observations), and separate Summary
  captures. Both batches of each arm used the exact same verified binary.
- Four threads, 2GB, binary results, quality policy, verifier off,
  `PARO_COMPILE_WORK_EVIDENCE=1`, optional deadline 30000ms. Normal trace off;
  no pre-touch or allocator instrumentation. DuckDB 1.5.5 and its native module,
  SQL, harness, seed and CSV identities matched across all twelve RunOutputs.
- Generator-declared metadata is identical between Paro versions but asymmetric
  to DuckDB. The latter is a contemporaneous reference, not a parity claim.

## Observations

Milliseconds; cold values are medians over six fresh processes. Warm values
are medians of all 36 observations; process-level medians are reported below
because warm observations within a process are not independent samples.

| Query | Compiler control → probe | C1 control → probe | Warm control → probe | DuckDB C1 during control → probe |
| --- | ---: | ---: | ---: | ---: |
| Q04 | 20.036 → 19.792 | 240.532 → 249.950 | 139.585 → 137.850 | 140.847 → 159.077 |
| Q11 | 13.515 → 12.712 | 173.548 → 148.258 | 94.922 → 83.482 | 92.795 → 65.536 |
| Q74 | 34.971 → 36.061 | 139.330 → 142.064 | 61.919 → 65.634 | 86.082 → 95.145 |

Medians of per-process warm medians, control → probe:
Q04 144.295 → 138.448; Q11 93.303 → 83.082; Q74 61.404 → 65.182ms.
These are not extra samples or independent campaigns.

The registered two batch-pair ratios (probe/control, lower is faster) expose
changes hidden by combining medians:

| Query | Compiler pair 1 / pair 2 | C1 pair 1 / pair 2 | Warm pair 1 / pair 2 |
| --- | ---: | ---: | ---: |
| Q04 | 1.043 / 0.971 | 1.099 / 0.982 | 1.202 / 0.974 |
| Q11 | 0.986 / 0.841 | 0.849 / 0.779 | 0.895 / 0.856 |
| Q74 | 0.998 / 1.106 | 0.981 / 1.079 | 0.973 / 1.328 |

- Q04 does not show consistent improvement in any of the three measures.
- Q11's observed compiler/C1/warm medians improve by about 5.9%/14.6%/12.1%.
  However, its corresponding DuckDB C1 improves by about 29.4% and warm by
  about 12.0%. Both Q11 batch pairs also show faster DuckDB observations during
  probe collection. This is substantial environmental confounding, not an
  opportunity to divide engine ratios and claim a corrected causal speedup.
- Q74's second pair exceeds the registered investigation thresholds: compiler
  +10.6%, warm +32.8%. Actual plan identity, grant and search counts stay stable
  within that arm; DuckDB warm also changes 68.127 → 87.811ms in this pair.
  Preserve and report the regression signal, but do not attribute it to the
  implementation from these samples. The slow 65.910ms compile remains in the
  probe distribution.

VM/background application activity was present throughout. Bounded host
snapshots are retained. No unrelated process was terminated; no sample was
discarded for being slow. This is an exploratory controlled-source comparison
on a non-isolated host, not an isolated-host certification.

## Work, plans and correctness

| Query | Cost compositions control → probe | Selected plan identity |
| --- | ---: | --- |
| Q04 | 782 → 543 | Changes between arms; stable within each arm |
| Q11 | 401 → 380 | Same locator in both arms |
| Q74 | 1045 → 1052 | Same locator in both arms |

Both artifact structure and actual selected-image fingerprints were checked.
Fingerprints locate plans, not semantic equivalence proofs. All normal samples
independently passed full result types, row multiplicities and required ORDER:
Q04 six rows, Q11 90, Q74 92. Every cold target is a cache miss with non-null
compile work. Actual admission class is 2, maximum tasks four, memory ceiling
2,000,000,000 bytes, no fallback, image/lowering Ready and execution Completed.

All cold receipts report QualityPolicySatisfied, search_complete=false,
budget_limited=false. This is not ProofComplete. The costing change can alter
work and choices, so this combined-arm experiment cannot isolate compact-key
or owned-merge performance. The aggregate-placement quality policy is still
unchanged; this task only measures the already committed implementation.

All twelve eligible campaign summaries, cells, receipt contracts and compile
documents passed the maintained validators. No source, runtime, budget,
expected SQL output or harness measurement code was changed for this comparison.
The prior 185/185 SQL regress result is not claimed as a new run here.

## Rejected original cohort and build lesson

[rejected-cohort](rejected-cohort/) retains the entire first schedule, including
all slow samples. P1 ran the new binary, C1/C2 ran the old binary, but P2's source
label named the new revision while its binary and search counters were still
the old revision. A successful fast Cargo invocation and a hash of shared
`target/release/parod` did not establish source-to-binary provenance when roots
were alternated. Do not use its P2 as probe or pool its timings into the eligible
cohort. This is a build-identity failure, not a query-result failure.

The replacement uses one comparison checkout path for both source revisions,
invalidates the three changed/entry packages' release artifacts on every version
switch, prebuilds through the maintained API, saves the binary, and verifies
that digest before/after every cell. Dependent crates rebuild normally; one
shared target retains third-party dependencies. The selected package scope is
specific to this code diff, not a universal build recipe. Prebuild attestations
and the actual orchestration script are retained in replacement-cohort.

There was also one rejected setup invocation with a wrong CSV directory. It
failed on missing schema.sql before any server or measurement was created;
no timing was replaced or discarded. See the replacement registration.

The archive is bounded below the registered combined 16MiB limit, with no
binary, free-form server log or duplicated raw event stream. Only the newly
owned temporary comparison worktree is disposable; the existing historical
worktree, immutable seeds, recovery materials and shared target remain owned
by their original workflows.

Final archive verification: all 24 RunOutputs match their collected directories
byte-for-byte and pass the shared schema validators; total archive 7,277,941
bytes before this final note, below 16MiB. The new comparison worktree and its
two temporary saved binaries were removed after verification; they can be
rebuilt from the recorded commits. The shared release binary remains the
verified probe image. The temporary local Git ignore exception was removed.
