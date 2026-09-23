# Physical search core: implementation and evidence

Registration: [physical-core-registration.md](../physical-core-registration.md).
The task delivers three ownership/lifetime changes, not a new search policy:

1. Physical tasks share immutable ReadSet and dependency snapshots. Unchanged
   refresh/canonicalization preserves storage; a changed fact detaches the
   snapshot and retains the exact invalidation scope.
2. Optional-domain opening preserves exact prices and immutable CandidateIds,
   but reopens response membership and completion. A recipe re-admits its
   resident allocation only in the matching facts/goal/grant/calibration
   context. Newly eligible optional children still run. The verified mandatory
   plan remains an interruption fallback, not proof of optional closure.
3. Frozen choices lower bottom-up into immutable selected occurrences with
   cached layouts, replayed keys and scalar slots. Local single-node shells
   adapt existing scalar contracts; no descendant-owned binder tree is rebuilt.
   Utility statements consume their binder tree once. Local child arity/schema
   checks precede indexed layout derivation; a 10,000-deep selected chain has
   iterative construction and destruction.

`EXPLAIN (COMPILE, DETAIL)` now includes a fixed-size typed exclusive work
ledger and an independent mandatory/optional/outside phase projection. Both
sum exactly to the same optimizer interval. They must not be added together,
subtracted from normal C1, or interpreted as CPU samples. Disabled normal
collection does not collect the Detail ledger.

## Retained arms

Each directory is the maintained sealed RunOutput, with source/build inputs,
all normal samples/receipts and one separate bounded capture. No raw server
logs or per-event duplicate export are archived. Five fresh normal blocks and
one Detail block per query/arm; 4 threads, 2GB, verifier off, quality policy,
normal compile-work receipts enabled, DuckDB 1.5.5, generator-declared metadata.
That metadata track is not a first-statement parity certification.

| Arm | Source | Meaning |
| --- | --- | --- |
| control | `2a74f285` | Typed attribution, before the three changes |
| resident | `ddcd76ab` | Shared physical task snapshots |
| coverage | `118ae28a` | First price-retention implementation; regressed |
| selected | `d45163e4` | Immutable selected lowering, still on regressed coverage |
| reopened | `89360f8b` | Price validity separated from active response membership |
| validated | `7c8fb9d7` | Same core with visible-side output-name contract fixed |

Normal compiler medians (ms), not Detail times:

| Arm | Q11 | Q04 | Q74 |
| --- | ---: | ---: | ---: |
| control | 12.410 | 22.656 | 64.969 |
| resident | 14.223 | 19.864 | 65.794 |
| coverage | 18.948 | 32.599 | 61.766 |
| selected | 16.450 | 31.344 | 64.720 |
| reopened | 13.608 | 21.915 | 65.772 |
| validated | 13.500 | 20.661 | 67.628 |

All samples, including the 56.500ms Q11 and 98.920ms Q74 values, remain in the
sealed cells. Sequential builds/collection and substantial host/DuckDB drift
confound cross-arm timing. These are **NotCertified pilots**. The registered
half-time target (Q11 median <=6.25ms / P90 <=7.6ms) is not met; no independent
normal compiler or C1 speedup is established.

Final compiler samples (ms), in collection order:

- Q11: 13.329, 15.087, 13.500, 17.190, 13.401 (P90 17.190).
- Q04: 20.661, 19.693, 19.701, 26.818, 21.480 (P90 26.818).
- Q74: 67.628, 76.895, 68.072, 64.055, 63.131 (P90 76.895).

P90 uses the collector's nearest-index convention (here the maximum).
Final C1 Paro/DuckDB medians are Q11 199.344/95.174ms, Q04 297.715/142.197ms,
Q74 179.262/82.861ms. Paro warm is 89.973/167.128/61.555ms respectively.
Q04 warm exceeds the original control by more than the registered 10% review
threshold. Inspection finds identical admitted artifacts, selected plans and
resources, with no production execution-layer intervention. Both engines and
other samples drift substantially, so this does **not** establish the cause or
certify warm non-regression. A controlled quiet-host/interleaved confirmation
is still required; no failure or slow sample was removed to make it pass.

## Attribution and negative results

The control Q11 Detail optimizer interval is 13.168ms: outside search 5.147ms,
mandatory 1.614ms, optional 6.408ms. Largest exclusive buckets include pre-work
2.717ms, subproblem 1.294ms, recipe 1.199ms, finish 1.051ms, lowering 0.949ms,
settlement 0.753ms, staging 0.601ms and encoding 0.539ms. Unclassified remains
explicit (0.518ms). There is no single 6ms pricing hotspot in this sample.

Retaining active mandatory frontiers was not equivalent to retaining valid
prices. Q11 synthesized 448 rather than 420 combinations and evaluated 21
quality candidates rather than one. Parent responses exposed partially
refreshed combinations, and the final selected identity changed. A first-visit
guard did not fix it. The final coverage design reopens membership, preserving
prices/archive identity and ordinary optional enumeration; the ineffective
guard was removed.

| Query | Control syntheses / archive publications | Reopened | Groups / logical / physical |
| --- | ---: | ---: | ---: |
| Q11 | 420 / 338 | 401 / 319 | 53 / 61 / 105 |
| Q04 | 810 / 537 | 782 / 509 | 69 / 77 / 121 |
| Q74 | 1,065 / 745 | 1,045 / 725 | 95 / 136 / 217 |

Archive publications count distinct immutable allocations, not re-admissions
to a frontier. Q11 and Q04 return to one quality evaluation; Q74 still needs
51. Reopened artifacts and actual admitted identities match the original
control on all three queries. Fingerprints are locators, not equivalence
proofs: full typed row/bag/order checks independently pass (6/90/92 rows).
All stop with QualityPolicySatisfied + SearchIncomplete, never ProofComplete.

Immutable lowering changes the Q11 local Detail bucket from 0.949ms (control)
to 0.639ms (selected), then 0.722ms (reopened). This is one diagnostic sample
per arm, not a causal timing certificate. Price retention removes 19/28/20
actual syntheses; it cannot by itself explain or promise a 50% compile saving.

The corrected final source has the same work counts and identities as reopened.
All 15 normal receipts per final query match its diagnostic artifact and the
original control; actual admission/resources also match. Its Q11 Detail is
13.477ms: outside 5.260ms, mandatory 1.697ms, optional 6.521ms. Lowering is
0.599ms, but finish is 1.422ms versus the control's 1.051ms; moving/removing
local work must not be reported as a whole-compiler gain. The next leverage
requires reducing real preparation/finalization and recipe/subproblem work,
not expanding the already unsuccessful frontier-retention mechanism or
assuming all mandatory work was duplicated. Those changes are outside this
three-contract delivery.

## Validation

The reopened source passed 6,922 workspace tests (85 ignored), optimizer 1,384
tests and strict optimizer Clippy. All 15 retained runs, 30 payloads and 15
captures pass the maintained campaign/receipt/document validators. The
compile-only gate reader rejects the TPC-DS payload shape; it was not used to
claim a cold-planning gate pass.

The first SQL gate found 164 pass / 21 fail. Eighteen failures are byte-identical
to the pre-task control, but three subquery cases expose a real local name
reduction bug: MARK/SEMI/ANTI consumed all structural child-name results rather
than only the visible side. Fixing this uses the layout's shared side contract,
with all join types/empty input/projection-map tests. No expected output changes.
A final registered matrix on the corrected source independently passes all
three full typed results. Its release SHA-256 is
`8a4b84fa00b2fe00e9088338caa00e8535d409c0dd1aac1542f115cd688c75aa`.
The preceding performance evidence is not relabeled as this later binary.

The final high-FD verifier-on SQL gate is **167 pass / 18 fail**. Failure-file
sets match the pre-task control, and all 18 actual outputs are byte-identical.
Their aggregate SHA-256 (sorted filename, NUL, raw SHA-256 digest per file) is
`80f68377a90ccb9473f7c6142c2c1576cc63418775d73194da49136997549915`.
Expected files are unchanged; this is not an all-green SQL gate. The first
failing run, final outputs and build/test logs remain in the owned temporary
directory `/private/tmp/paro-physical-core.t93SzO`.

All 18 sealed campaigns, 36 typed payloads and 18 captures validate through the
shared validators after archival. Archive size is about 5.1MiB, below the 32MiB
registration limit. Benchmark harness tests: 207 pass. Memory runtime/vector
API guards and the generated optimizer-calibration artifact check pass.

Final corrected-source validation:

- `cargo test --workspace --locked`: 6,923 pass, zero fail, 85 ignored;
  optimizer 1,384 pass, including exact optional-domain replay and exhaustive
  frontier/continuation oracles; the new planner side-contract test also passes.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: pass.
- `cargo check --workspace --all-targets --locked`: pass.
- `make -C benchmark test`: 207 pass. Its missing temporary-baseline messages
  are unit fixtures, not measured performance-gate results.
- Release build and task diff checks pass. Repository-wide formatting/header
  debt is not changed or represented as an all-static pass.
- Owned servers stopped; no other worktree, dataset, recovery reference or
  baseline deleted. Code integrates by fast-forward from clean `re-op` base
  `b743ca55`; the historical chain worktree remains untouched.

Implementation is complete for the three contracts and bounded attribution.
CompilerTargetMet / FirstStatementParity / warm non-regression are not
certified; this task does not convert QualityPolicySatisfied into ProofComplete.
