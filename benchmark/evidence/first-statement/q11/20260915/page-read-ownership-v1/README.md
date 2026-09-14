# Page-read ownership and decode profile (2026-09-15)

## Decision

This round targets the execution-side path only:

`page read -> compressed/decompressed page -> vector execution`.

The page-buffer ownership change is retained because it removes a real
compressed-page `Vec -> BufferPool` copy and a cached raw-page `to_vec()`.
The clean end-to-end pilot is neutral, however; it is not evidence of a
repeatable C1 improvement. The release CPU profile does not justify adding a
new decode kernel in this round. The next measured hotspot is the combined
scan/vector-expression/hash-aggregate path, not another page-cache policy
change. The E3 no-materialization policy remains off and unchanged.

No optimizer, plan choice, search budget, stop policy, resource envelope, or
execution algorithm was changed.

## Implementation

Delivery commit in the main repository: `acc22ef0`
(`perf(storage): fill page cache buffers in place`). The performance artifacts
were built in the clean isolated worktree at
`1fff3b8e54f7bb2cb52f8d87d45edf69783cd103`; its storage tree is the same as
the delivered commit. The clean source working-tree digest is
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

The implementation:

* adds `PageCache::get_or_load_into`, which allocates the cache-owned backing
  buffer once and lets the loader fill that exact buffer;
* reads a compressed page directly into the cache allocation, and uses the
  same path for prefetch;
* lets LZ4, Zstd, and uncompressed codecs decode directly into the destination;
* returns a pinned `Bytes` view for cached raw pages instead of copying it;
* keeps single-flight loading, pin/eviction, failed-load cleanup and retry,
  memory tags/limits, rowset generation isolation, and page validation intact.

The existing `get_or_load_decoded_into` path was reused. The parallel page
reader still has its existing `body.to_vec()` path; Q11's normal path does not
use that decompressor, so it was not changed speculatively.

## Reproducibility identities

| item | identity |
|---|---|
| release `parod` | `778fd401d79f4876a805a390884348466aa526c30983c31cdef7dca96e9d8f0e` |
| Q11 SQL | `1f3c2697b2a82f597f9e8a570413be65ab7a02da03c1fe4c3ea7b725735433b8` |
| Paro data seed | `d086f41f77cf32e4201f8d0bbadc3514b64a57577964a13ae40db2f2bd577ef5` |
| DuckDB database | `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7` |
| query fingerprint | `6078428509570048703` |
| `tpcds_compare.py` | `828c13abd199154985d82923d562dd9020d571acd00cfe7605963bd6a0bcb244` |
| `benchmark_evidence.py` | `58bd9d665d55e2eccf50c6be2a29c12e166b674589ca0aefbdb7616d2e31ec70` |
| `tpcds_result_contract.py` | `6b327a3f495f06e4363382014466c56b102b08af45c7ce4a25a7ed4f033e8c00` |
| `cold_planning.py` | `cab9656dfe4f13e79412c82c78060293823eed90cf5f303fe37846b956f71d4d` |

The normal and cold-work campaigns used four execution threads, a 2 GiB
server limit, private per-process data snapshots, the original Q11, binary
result transport, and `PARO_QUALITY_POLICY_HANDOFF=1`. Normal runs were
trace-off and cache-miss verified. The diagnostic cohort enabled the existing
compile/cold-work counters and statement trace separately.

## Clean normal pilot

The primary normal artifact is
`clean-page-read-final-q11.json.gz`. It used two fresh process blocks, one
warmup, one ABBA measurement round, and retained every sample. The target
occurrence was verified as a plan-cache miss (`occurrence=0`), statement trace
was verified empty, and all 90 rows passed typed schema, value, multiset, and
order validation against DuckDB.

| metric | clean base (`938b2989`) | page ownership (`1fff3b8e`) |
|---|---:|---:|
| Paro C1 median / p95 | 194.802 / 195.317 ms | 194.549 / 196.391 ms |
| DuckDB C1 median / p95 | 107.014 / 108.846 ms | 105.163 / 105.695 ms |
| paired C1 ratio, 95% CI | 1.820612 [1.784976, 1.856960] | 1.849916 [1.823244, 1.876977] |
| Paro warm median / p95 | 89.510 / 91.772 ms | 90.095 / 90.925 ms |
| DuckDB warm median / p95 | 103.086 / 103.975 ms | 103.858 / 104.177 ms |
| warm ratio, 95% CI | 0.868534 [0.855300, 0.879389] | 0.867617 [0.861979, 0.872593] |

This is a two-block pilot, not a formal power, non-inferiority, M1, or parity
campaign. C1 is statistically neutral at this sample size; warm quality is
also effectively unchanged. The small ratio movement is not attributed to the
ownership change because the DuckDB samples and host state moved as well.

The clean normal report and block logs are archived in this directory.

## Cold-work diagnostic

`cold-work-clean-final-q11.json.gz` is a separate diagnostic cohort and is not
pooled with the normal C1 table. It has trace-off target timing but enables the
existing cold-work and compile counters. Target cache-miss side-channel records
showed:

| cold target block | page fills / input | decoder constructions / input | fill exclusive worker | decoder exclusive worker | execution elapsed |
|---|---:|---:|---:|---:|---:|
| 0 | 987 / 161,929,711 B | 615 / 41,328,315 B | 56.638 ms | 61.102 ms | 128.605 ms |
| 1 | 987 / 161,929,711 B | 612 / 40,984,632 B | 56.113 ms | 61.229 ms | 130.636 ms |

The first measured warm execution in those processes had one 262,144-byte fill
and 152 decoders; the following warm execution had zero fills and 150 decoders
(about 15.07 MB decoder input). This confirms that warm decoded-page reuse is
still present. Worker-exclusive values overlap and are not added as wall-time
phases. RSS and minor-fault values remain process-level diagnostics, not a
statement-reset working-set claim.

The same diagnostic report observed compile values of 63.873/63.463 ms,
optimizer values of 63.168/62.777 ms, and 2,500 cost syntheses. These counters
are reported for identity and phase context only; they do not change the
normal C1 result.

## Release CPU profile

`profile-clean.sample.gz` was collected with macOS `sample` against the clean
release binary. It is diagnostic only: sampler-inclusive client time was
373.932 ms, and no percentage is used as a production attribution. The
profile contains the expected storage stacks through
`PageReader::read_page`, `PageCache::get_or_load_into`, page I/O, LZ4/page
decoder and `materialize_into`, but also substantial vector decode, scan,
hash-aggregate, join and task-supply stacks. There is no evidence in this
sample for a single page-copy function large enough to justify another
unbounded storage rewrite.

The profile therefore closes this round's “copy versus codec kernel” decision
as follows: retain the ownership contract, do not disable decoded caching, and
do not claim a B-side algorithmic speedup. A future change must isolate and
reduce the measured scan/vector/aggregate work while preserving the current
warm image behavior.

## Verification

* `cargo test -p paro-storage --lib`: **2086 passed, 0 failed, 1 ignored**.
* `cargo check -p paro-storage --release`: passed.
* `make -C benchmark test` with the repository Python 3.14 environment:
  **134 passed**; missing local performance baselines were informational.
* Full SQL regress with `ulimit -n 16384`: **151 passed, 33 failed, 0 skipped,
  0 new**. No expected output was blessed. The retained failures are the known
  EXPLAIN/PROFILE/full-text/vector rendering differences, setup cascades, and
  the 2 GiB memory-setting mismatch; they are not silently treated as green.

The diagnostic trace reports the quality state as
`QualityPolicySatisfied + SearchIncomplete`; it is not `ProofComplete`.

## Artifact manifest

| artifact | SHA256 |
|---|---|
| `11.sql` | `1f3c2697b2a82f597f9e8a570413be65ab7a02da03c1fe4c3ea7b725735433b8` |
| `clean-page-read-final-q11.json.gz` | `8d3aa6c65e0a288000306a96ffc8f034a2bef65078d3c1e18e5c34f98cf60624` |
| `cold-work-clean-final-q11.json.gz` | `cbc76722e90b730c17eed86d62ab8ba5bb4d0792c5120efbcb0f11a158b8a1ce` |
| `profile-clean.py` | `731e6f364465bc6b9700915e8431e2a47cecc7d58fa277312fa34346d37bc775` |
| `profile-clean.sample.gz` | `6a6dff9e5b6aadb4ba796da38b3fc89f2c1dc7e50a49a7ae8da5b89989273ab1` |

The compressed normal/diagnostic server logs and their hashes are kept beside
these artifacts in [`SHA256SUMS`](SHA256SUMS). The diagnostic statement log is intentionally retained even
though it is large; it is excluded from normal timing and is not used to
justify an endpoint performance claim.
