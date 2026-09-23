# Change-driven construction, compact quality evidence and demand-driven bounds

EvidenceId: `construction-bounds-v1`. Registration:
[construction-bounds-registration.md](../construction-bounds-registration.md).
Collected September 23–24, 2026. This is a completed engineering pilot, not a
powered performance or parity certification.

## Result

The changes preserve the admitted artifact, physical selection, grant and cost
synthesis counts in all three queries. Q04 has a directional compiler reduction;
Q11 is flat and Q74 is slightly slower. This does **not** establish a general
compiler speedup, the Q11 <10ms target, warm non-inferiority or engine parity.

Compiler milliseconds, from normal cache-miss compile receipts (six independent
fresh processes per arm/query):

| Query | Control median | Probe median | Change | Control P90 | Probe P90 | Control max | Probe max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Q04 | 20.848 | 18.866 | -9.5% | 23.011 | 20.404 | 28.344 | 21.218 |
| Q11 | 12.895 | 12.914 | +0.2% | 13.709 | 13.341 | 23.210 | 13.629 |
| Q74 | 38.550 | 39.740 | +3.1% | 39.140 | 39.957 | 52.927 | 40.531 |

P90 uses the collector's existing nearest-index `percentile` definition. At this
sample size it excludes the maximum, so maxima and every original sample are
also retained. Compiler samples, in batch/process order:

| Query/arm | All compiler samples (ms) |
| --- | --- |
| Q04 control | 23.011, 19.135, 22.355, 19.340, 28.344, 18.544 |
| Q04 probe | 19.109, 21.218, 18.061, 20.404, 18.423, 18.622 |
| Q11 control | 13.091, 23.210, 12.698, 12.432, 11.840, 13.709 |
| Q11 probe | 13.341, 12.961, 12.867, 12.299, 12.255, 13.629 |
| Q74 control | 38.945, 38.154, 52.927, 38.129, 39.140, 37.716 |
| Q74 probe | 39.957, 39.576, 40.531, 38.620, 38.581, 39.903 |

Client elapsed milliseconds (normal cohort):

| Query | Paro C1 control → probe | DuckDB C1 control → probe | Paro warm control → probe |
| --- | ---: | ---: | ---: |
| Q04 | 294.741 → 270.009 | 145.804 → 131.922 | 170.887 → 188.631 |
| Q11 | 173.193 → 153.269 | 73.139 → 68.126 | 91.270 → 85.464 |
| Q74 | 137.070 → 141.569 | 81.604 → 84.323 | 56.933 → 63.161 |

These are descriptive medians, not ratios of paired ratios or causal speedup
estimates. Warm has 12 calls per arm/query nested in six processes, not 12
independent process samples. DuckDB also moves between batches. In particular,
the Q11 C1 reduction cannot be attributed to faster compilation: its compiler
median is unchanged.

Q04 and Q74 warm medians exceed the registered 10% investigation line (about
10.4% and 10.9%). All warm receipts are cache hits; artifact/selection identities,
resource contracts and full results match, and no execution code, cost model or
budget was changed. These checks did not determine the cause. The warning stays
open; this run does not certify warm non-inferiority. No additional samples were
collected to replace the negative result.

## Actual implementation scope

1. `bd1f9aae`: canonical scalar rewriting reports actual changes, not cache
   misses. Predicate construction repeats routing only after a scalar change.
   A change conservatively reopens routing, including join/CTE barriers. This
   removes a redundant pass on no-op normalization; it is not a new global
   dirty-region normalization engine. The old separate-pass pipeline remains
   an independent test oracle.
2. `3b195cee`: aggregate-region evidence references the exact selected anchor,
   whose immutable child CandidateIds identify its subtree, instead of expanding
   each region's choice subtree again. Root choice membership and fact revisions
   remain checked. Proof iterators are borrowed rather than allocated for each
   node. The frozen-DAG oracle independently traverses the subtree. Full selected
   payload validation is still performed: the broader per-property incremental
   evidence goal is **not** complete.
3. `3b195cee`, `81c4ab21`: immutable local bound evidence is constructed only
   after an applicable live incumbent exists. Recipe-local enforcer feasibility
   is checked before recursive child preparation. Scalar cutoffs are restricted
   to terminal, cost-only objectives; a child local winner or an executable but
   quality-ineligible incumbent is not an admissible universal upper bound.
   Changing the terminal root reopens previously pruned recipes before that root
   can act as a child. RF/source-filter response, uncertain intervals and overlap
   without a composition proof remain unpruned.

Certified-pruning enablement was not changed. It is off in this registered
quality-policy campaign, and every diagnostic has zero certified recipe prunes.
The synthetic terminal-bound tests demonstrate safety/operation in the supported
domain, not useful Q11 pruning or global optimality. Do not describe this result
as full branch-and-bound completion.

The retained changes have narrower structural benefits (no-op pass elimination,
less duplicated evidence, demand-driven proof ownership and safer cutoffs).
The performance evidence does not justify claiming all three are individually
faster: this registration measured the combined implementation.

## What remains expensive

Separate Detail captures provide attribution, never normal timing substitutes:

| Query | Pre control → probe (two captures, ms) | QualityEvidence control → probe (ms) | QualityEvidence evaluations |
| --- | --- | --- | ---: |
| Q04 | 4.059/4.171 → 3.593/3.535 | 0.409/0.412 → 0.370/0.421 | 1 → 1 |
| Q11 | 2.934/2.551 → 2.500/2.568 | 0.262/0.257 → 0.237/0.246 | 1 → 1 |
| Q74 | 2.388/2.649 → 2.515/2.320 | 10.558/10.466 → 10.330/9.926 | 140 → 140 |

The main Q74 quality-evidence repetition remains. Compact region anchors alone
do not remove those 140 full candidate evaluations. Recipe/subproblem/finish,
settlement and staging entry counts also remain unchanged. A further change
must identify which exact dependencies force these validations before adding
another cache or changing scheduling; this pilot is not evidence that such a
cache already exists or will help Q11.

## Source, inputs and collection

- Clean control `45819b96b4b30f0c941dda96ff55b9eac3f4667c`; implementation is
  unchanged from registered `b367b3eae043df54f5f8d640ec9bb7013a3c0e6c`.
- Clean probe `81c4ab2196dea966faea723d94167668a888cb33`.
- Control parod SHA-256:
  `84afb4fbbc4c84c53331b84fe9cc1696214468e0c226ef467a443483bf09667b`.
- Probe parod SHA-256:
  `cd19576f3992e072f295441d3072dcf36f1fe1b835064d8fd733ada06ad73b04`.
- DuckDB 1.5.5, native extension
  `85fad85339c7e345eabb66a33a25d8503c3ee8881f9d298fa8499a97617dbf68`.
  The declared/runtime version was checked; no environment upgrade was used.
- Same SF1 relocatable seed
  `256bf479ca982e30c96f9109fb9920d04233c9643b7bcc6a5fd3f5a50554c927`.
  SQL, dataset, harness, metadata and build hashes are in each `inputs.json`.
- One checkout/target, sequential clean builds and C/P/C/P batches. No competing
  tests/builds during measurement and no additional worktree. Each batch visits
  Q04/Q11/Q74 with three fresh normal blocks and one separate Detail block.
- Four threads, 2GB, binary results, quality search policy, verifier off,
  unchanged 30-second optional deadline. Generator-declared metadata is not a
  qualifying symmetric metadata track for engine parity.
- Normal uses `PARO_COMPILE_WORK_EVIDENCE=1`, bounded typed receipts and no
  statement trace/Detail. Diagnostic captures use the maintained EXPLAIN
  collector. No temporary timer, event stream or free-text log parser.
- One warmup and one seeded ABBA round per normal block; seeds 2026092307 and
  2026092308, collector bootstrap setting 10,000. No exploratory samples pooled.

Every `{control,probe}-{1,2}-q{04,11,74}-run/` is an unchanged maintained RunOutput
with its own manifest, inputs, accepted attempts and single capture reference.
The copied collection is approximately 3.1MB, below the registered 20MiB cap;
it contains no routine server logs, binaries or duplicate trace streams.

All 12 campaigns, 108 normal receipts and 12 capture hashes validate through the
existing readers. Diagnostic artifact identity and expected grant match every
normal sample; the actual normal physical fingerprint is present in the
diagnostic portfolio for that grant. Compile-only `selected_fingerprint` remains
Uncovered/NotInstrumented and is not falsely treated as actual admission.
Fingerprint matching is an association check, not an independent equivalence
proof; complete typed results/order and the independent oracles are also tested.

RunOutput capacity refusals, omitted search counters and omitted variants are
zero. The bounded **event lists are not exhaustive**: source/capture/encoding
omission counters are respectively 1511/756/1401 for Q04, 664/119/1399 for Q11
and 2153/1887/1400 for Q74, identical in both arms/batches. The tables use complete
summary counters and work buckets, not counts reconstructed from retained
events. Do not use these Detail captures to claim a complete causal event trace.

## Search and validation

Q04/Q11/Q74 cost synthesis counts are respectively 782/401/1045, unchanged in
every normal cache-miss receipt. All admit grant class 2, four tasks, 2GB ceiling,
guaranteed memory completion and identical per-query minimum/preferred memory.
All stop `QualityPolicySatisfied` with `search_complete=false`; none is
`ProofComplete`.

- Final probe workspace Rust tests and check: passed.
- Optimizer: 1,407 passed, no failures.
- Strict workspace/all-targets Clippy: passed.
- Benchmark harness: 207 passed. Missing-baseline messages in intentional gate
  unit fixtures are not real performance results.
- Regression harness: 103 passed, one optional test skipped. An initial command
  from the repository root lacked the harness import path; running the normal
  `regress/` entry point passed without code changes.
- Final release binary, fresh owned data/port, verifier on, FD limit 65,536:
  full compare-only SQL regress **185 passed, zero failures, zero skipped**.
  See [sql-regress.txt](sql-regress.txt). No expected files changed.
- Memory runtime/vector-copy API guards and diff whitespace check: passed.
  No repository-wide formatting cleanup or blanket `make static` pass claimed.

The test server was stopped. The prior ignored regression report was restored;
the new full report remains in the evidence. Existing worktrees, seeds and
recovery material were not removed. Validation details are in
[validation.json](validation.json).
