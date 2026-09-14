# Q11 execution-layer architecture pilot v1

Date: 2026-09-15

This archive covers three sequential execution-layer changes, each measured in an
isolated clean release worktree and then measured once as a combined build. The
optimizer, selected plan, search budget, stop policy, SQL, data layout, resource
envelope, and handoff policy were kept fixed. The changes are not a Q11 special
case and do not claim `ProofComplete`.

## Scope and commits

| arm | source commit | change |
| --- | --- | --- |
| baseline | `8fbf5f0fc2d30a1c56cccaab101fcb8cf8583fa1` | clean same-batch control |
| dictionary | `3bb98ce990992dd59622339b9c9904f8d7d0f94b` | borrowed dictionary execution views |
| batch-view | `4e8497feae8c4b7cc178cfefc461c8fbd5fca964` | read-only batch views separated from reset workspaces |
| temp-admission | `bf26f5160fbc3a855e1a0e4a9a898e7e51dfed69` | bounded local temporary-buffer admission |
| combined | `bf26f5160fbc3a855e1a0e4a9a898e7e51dfed69` | all three changes together |

The benchmark source worktrees were clean. The repository worktree SHA recorded by
the harness was `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
The original Q11 SQL is [11.sql](11.sql), SHA-256
`1f3c2697b2a82f597e9f8a570413be65ab7a02da03c1fe4c3ea7b725735433b8`.

Common identities:

- data seed: `d086f41f77cf32e4201f8d0bbadc3514b64a57577964a13ae40db2f2bd577ef5`;
- DuckDB database: `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`;
- harness files: `tpcds_compare.py`
  `828c13abd199154985d82923d562dd9020d571acd00cfe7605963bd6a0bcb244`,
  `benchmark_evidence.py`
  `58bd9d665d55e2eccf50c6be2a29c12e166b674589ca0aefbdb7616d2e31ec70`,
  `tpcds_result_contract.py`
  `6b327a3f495f06e4363382014466c56b102b08af45c7ce4a25a7ed4f033e8c00`, and
  `tpcds_setup.py`
  `9db7956fc506d82008902f962013691b510691ba03ca23c92e89fd24abf2d171`;
- all runs used generator-declared metadata, 4 execution threads, 2 GiB, private
  per-process data copies, cache miss for the target statement, and normal
  trace-off measurement.

## Normal Q11 results

There were two fresh process blocks per arm, so this is a directional pilot, not a
formal power, warm non-inferiority, M1, or parity campaign. Every valid block passed
the 90-row typed schema, value, multiset, and order checks. The raw reports preserve
the individual samples and the harness-generated confidence intervals.

| arm | Paro C1 median / p95 (ms) | DuckDB C1 median / p95 (ms) | C1 ratio (95% CI) | Paro warm median / p95 (ms) | DuckDB warm median / p95 (ms) | warm ratio (95% CI) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline | 199.242 / 199.595 | 106.824 / 106.957 | 1.865136 [1.864152, 1.866121] | 92.691 / 93.626 | 103.865 / 105.325 | 0.885690 [0.871178, 0.899587] |
| dictionary | 174.607 / 174.829 | 106.295 / 106.379 | 1.642659 [1.641854, 1.643465] | 71.429 / 73.254 | 104.703 / 104.942 | 0.686770 [0.676935, 0.702317] |
| batch-view | 176.917 / 177.705 | 107.350 / 107.799 | 1.648030 [1.647584, 1.648475] | 70.626 / 71.597 | 104.315 / 104.978 | 0.676082 [0.666101, 0.686212] |
| temp-admission | 172.071 / 173.415 | 107.864 / 107.889 | 1.595214 [1.582434, 1.608097] | 67.072 / 69.345 | 104.927 / 105.940 | 0.642243 [0.634784, 0.654602] |
| combined | 171.778 / 173.182 | 106.826 / 107.443 | 1.607990 [1.585700, 1.630594] | 67.411 / 69.065 | 104.481 / 105.584 | 0.646846 [0.638280, 0.656893] |

Against this same-batch pilot control, the directional C1 differences were -24.6 ms
(dictionary), -22.3 ms (batch-view), -27.2 ms (temp-admission), and -27.5 ms
(combined). The run order was not a randomized formal campaign and the sample size
is two blocks; these numbers are not additive attribution. The combined result is
evidence of a useful pilot direction, not a parity claim: its C1 ratio is still
about 1.61 and the 95% upper bound is far above 1.00.

The warm values did not regress in this pilot. The combined warm result is about
25.3 ms below the same-batch control, but formal warm non-inferiority remains
unproven because this is only two blocks and the existing registered power target
was not run.

## What changed and what was measured

### 1. Dictionary execution representation

`vector_decoder.rs` now uses one borrowed child construction path for complete and
sparse variable-length dictionaries. It acquires one page owner, validates codes and
UTF-8, keeps long values as page-backed `StringView`s, and uses inline storage for
short values. Fixed-width fallback reads `value_ref_at()` directly. NULL slots,
dictionary provenance, local sparse-code remapping, cache replacement and owner
lifetime remain explicit; an integer code from a different dictionary is not reused.
The focused storage suite passed 11 tests. The pilot has the same result digest and
order digest as the control, and suggests a C1 improvement, but does not isolate the
dictionary path from machine/order variation.

### 2. Batch views and workspaces

`Chunk::clone()` is now an explicitly read-only batch view: column `Arc`s are shared
and reset workspace state is not cloned or re-created. `ChunkView` exposes borrowed
columns, length, and capacity. Grouping-set construction uses explicit
`clone_referencing_vectors()` when it actually needs another vector reference. This
removes the old clone path that called `VectorResetState::try_new()` and allocated a
second vector set, while retaining independent reset/reuse state for mutable work.
The common chunk/view test and aggregate helper tests passed. The Q11 pilot did not
expose per-batch allocation counters, so the reduction in allocation work is proven
by the focused tests and code path, not assigned a separate wall-time amount.

### 3. Temporary-buffer admission

`BufferAllocator` now obtains a bounded local quota in refills (256 KiB minimum),
while `BufferPool` accounts reserved, used, and live allocation state. Reservation
is made under the admission lock; block allocation and zero initialization happen
outside that lock. `allocate_zeroed` has one owner for initialization, avoiding a
second clear without permitting uninitialized reads. Failed reserved allocation
keeps the reservation for the caller to return, and allocator drop returns unused
quota. Cancellation/error, memory-limit, pin/eviction, and concurrent pressure
paths retain their existing accounting contracts.

The Q11 report does not expose enough lock-wait/allocation counters to claim a
specific normal-wall reduction from this item. The focused suites cover the safety
contract: common allocator `25 passed`, storage BufferPool `50 passed`; the release
workspace check passed. The combined pilot is therefore reported as an end-to-end
directional result, not as proof that every C1 delta came from admission batching.

## Diagnostic and completion state

The combined diagnostic cohort is separate from normal C1. It recorded approximately
`76.352 ms` for the optimizer event, compiler return at about `77.979 ms` from the
statement trace origin, and `103.262 ms` portal execution. The declared working-set
event was `36,639,232` bytes; this is a contract value, not a peak-RSS measurement.
The diagnostic trace contains `search_incomplete`; the report status is
`QualityPolicySatisfied + SearchIncomplete`, not `ProofComplete`. No benchmark
sample proves complete optimal search, formal M1, M2, or parity.

The combined binary SHA-256 is
`7d3168ba5bebbb70b3e7528b815d28e55c98ed21557ee38da3deba6bc821d74c`. The other
binary identities are in the per-arm reports and `SHA256SUMS`.

## Tests and regression status

Passed validation:

- `cargo test --locked -p paro-common --lib`: 546 passed, 0 failed, 1 ignored;
- `cargo test --locked -p paro-storage --lib`: 2091 passed, 0 failed, 1 ignored;
- `cargo test --locked -p paro-execution --lib`: 664 passed, 0 failed, 0 ignored;
- `make -C benchmark test`: 134 passed (one optional test skipped);
- `cargo build --locked -p paro-server --bin parod` and the clean release builds.

The full SQL regression was run separately with a fresh server and an increased
`ulimit -n 65536`. It ended with `151 passed / 33 failed / 0 skipped`; the same
`Too many open files` resource-lifecycle failure eventually recurred, so no results
were blessed. The failures include existing EXPLAIN/PROFILE, full-text/vector-text,
setup-cascade, and 2 GiB memory-setting differences. The full error and log are
archived as `sql-regress-error.txt.gz`, `sql-regress-report.txt.gz`, and
`sql-regress.log.gz`; they are not part of the Q11 performance gate.

Invalid preliminary benchmark attempts (metadata track `none`, missing handoff
environment, and a non-original Q11 query directory) were discarded and are not
part of the result table.

## Remaining work

The page-read copy path was improved, but the previous page-read evidence showed no
reliable C1 gain from copy elimination alone. This round's pilot improves C1 while
preserving warm quality, yet it remains far from DuckDB and from the formal M1/M2/
parity gates. Normal peak RSS, per-occurrence image identity, formal 36-block warm
power, full SQL regress recovery, and the existing optimizer fixture failures remain
open. The next performance decision must be based on a larger clean campaign and
the release profile; it must not treat fewer allocations, bytes, or diagnostic
events as the final Q11 result.

All compressed reports and logs in this directory are checksummed by
[SHA256SUMS](SHA256SUMS).
