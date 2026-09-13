# E2-DECOUPLE — 2026-09-13

## Decision

**Pre-touch transfers the observed first-execution excess out of Q11.** In four
adjacent fresh-process pairs, target latency falls33.29–49.28ms, with unchanged
1691 synthesis and nearly unchanged compiler time. Existing E1 scalars in a
separate cohort confirm target buffer fills987→0 and execution128–131→90ms.
This supports pursuing first-touch decoded-page materialization/admission as the
next bounded design experiment, not an allocation fix inferred from page faults.

**It is not production C1 savings.** Pre-touch itself takes about132ms for Paro;
pre-touch+target is about291ms, worse than the approximately198ms control.
Neither pre-touch nor cache warming was added to production. No optimizer,
executor, allocation, budget, default handoff or model code changed this round.
Scan-through's ability to save the same cost, rather than merely defer/duplicate
decode work, remains unproved. Existing sparse decode/probation mechanisms must
be reused, not replaced by a second scan/cache subsystem without measurement.

## Contract and identities

- Clean committed source `6400ece14f60a6ed0856e0e3209102760696b6a2`; binary SHA256
  `2bcde8dc5ca306756da8e0c67d38c91766268e516427fc0b9a89893341808ea4`, identical to T1.
- Original Q11, unchanged SQL/data seed/model/budgets, 4workers/2GB, binary
  results, generator-declared metadata, existing handoff enabled. The full input,
  harness and binary hashes are in every raw report; archive hashes in manifest.
- [Pre-registration](PREREGISTRATION.md): control2→touch2→touch2→control2; all
  valid samples kept. Each report also contains a separate trace diagnostic.
  Normal timing uses compile scalars, no statement trace and no E1 collection.
- [One pre-touch SELECT](pre-touch.sql) checksums every Q11-referenced column in
  store_sales/web_sales/customer/date_dim, without the target joins/filters.
  Both engines drain its four rows and compare exact typed results. Its SQL
  fingerprint differs from Q11; every target has occurrence0 and verified miss.
  Prepared/semantic cache collisions would fail the target miss check, not be
  waived. The pre-touch SQL content hash is checked again after sampling.
- Preparation execute/fetch and both-engine preparation wall are explicit.
  The latter includes evidence read and validation; the former includes the
  pre-touch query's compilation and execution. Preparation is excluded only
  from the *diagnostic target* timer, not represented as free work.
- The harness labels pre-touch as diagnostic, sets primary-gate eligibility
  false and cannot qualify it for C1/parity. The historical `cold_statement`
  report container is retained with an explicit diagnostic cohort label.

## Trace-off experiment

Each row is an independent fresh process block. P and D are Paro and DuckDB.
Compiler is the same target occurrence's scalar, W is that block's warm median.

| Arm | Target P ms | Compiler ms | W P ms | P target−compiler−W ms | Pre-touch P ms |
|---|---:|---:|---:|---:|---:|
| control-a0 | 205.877 | 63.779 | 93.049 | 49.050 | — |
| control-a1 | 199.684 | 65.831 | 89.932 | 43.921 | — |
| touch-a0 | 156.598 | 64.583 | 89.706 | 2.309 | 135.003 |
| touch-a1 | 158.643 | 64.758 | 93.566 | .319 | 132.673 |
| touch-b0 | 161.862 | 66.892 | 113.683 | −18.714 | 130.787 |
| touch-b1 | 158.725 | 66.455 | 91.180 | 1.090 | 132.105 |
| control-b0 | 195.155 | 64.717 | 89.660 | 40.779 | — |
| control-b1 | 195.763 | 66.322 | 90.672 | 38.769 | — |

The slow warm sample135.035ms in touch-b0 is retained. Negative residual is a
signed difference of observations, **not negative execution duration**; it is not
clipped or assigned to a phase. The matched target deltas do not depend on W.

- Control target median197.723ms, observed p95/max205.877ms; pre-touch target
  median158.684ms, observed p95/max161.862ms. Four adjacent-pair ratios yield
  geometric mean.798423, bootstrap95% [.772879,.820524]. These are small pilots,
  not formal power/tail/parity campaigns.
- Compiler medians65.274/65.607ms; all normal samples1691 syntheses. Pooled W
  medians90.531/91.958ms, without a formal noninferiority claim.
- DuckDB target medians107.315/104.148ms; its pre-touch costs about20.192ms.
  Pre-touch+Paro target median291.458ms. Both-engine preparation wall median
  155.015ms; it is not a per-engine query phase.
- Median of per-block residuals42.350/.704ms. No cross-cohort median subtraction
  is used to attribute execution phases. The user's prior40.474ms is a difference
  of T1 medians; the corresponding median of per-block residuals is39.699ms.
  The 'compiler zero' arithmetic is a budget extrapolation, not a physical limit.
- Diagnostic Q11 admission remains class2/fingerprint
  `5c29cf646706c8c8ba84000150211a6b`. The different pre-touch/evidence-query
  fingerprints in those logs are not target winners. Normal fingerprints are not
  captured by a heavy trace. The target is QualityPolicySatisfied +
  SearchIncomplete, not ProofComplete; default stopping was not modified.

Recompute with [analyze.py](analyze.py), passing control-a, touch-a, touch-b,
control-b raw JSON or gzip files in that order. It checks clean source, result90,
trace-off, first miss, and shared binary/SQL identity before computing ratios.

## Separate scalar coverage check (not pooled with above)

Existing E1 instrumentation, two fresh blocks per arm, no new counters:

| Q11 execution metric | Control | After pre-touch |
|---|---:|---:|
| filled frames | 987 /987 | 0 /0 |
| fill input bytes | 161,929,711 each | 0 each |
| decoder constructions | 614 /614 | 150 /150 |
| decoder input bytes | about41.24MB | about15.07MB |
| dictionary /zone-map builds | 7 /26 | 0 /0 |
| minor faults | 15,386 /15,492 | 2,711 /2,768 |
| activity wall union ms | 60.425 /61.101 | 5.199 /4.820 |
| execution wall ms | 127.935 /130.990 | 90.130 /89.972 |
| same-image warm execution ms | 89.745 /90.644 | 90.590 /89.731 |

Pre-touch fills992 frames/162,516,038 bytes and incurs12,883–13,089 faults.
It slightly over-covers Q11, as intended; it is not a free replay of its plan.
Target and warm image identities match within each process. Pre-touch target
still incurs about2700 faults without a40ms penalty: fault counts alone cannot
justify allocation changes. Process maxRSS is not a statement-reset live-set
measurement. The E1 instrumentation overhead acceptance remains inconclusive,
so only the separate trace-off cohort supplies target-time conclusions.

This intervention warms page content, dictionary/zone metadata, OS caches and
some reusable runtime state together. The disappearance of all target fills and
restoration of warm-like execution strongly support transferable resident state;
they do not isolate fill versus decode, prove the entire Paro/DuckDB W gap is
caused by decoded caching, or guarantee production scan-through's net benefit.

## Independent byte-axis investigation: read only

### The 38 late-payload rejections

The archived [T1 default diagnostic](../e1-cold-t1-prod/raw/paro-tcwc-t1-prod-default.json.gz)
has discovered213/matched38/applicable0/constructed0/published0/rejected38,
target statement5, fingerprint6078428509570048703, admitted plan
`33eee07b2590b88a377c533ff2267634`. However, lifecycle retains1024 records and
drops25627; no retained record is rule10018. **All38 per-binding guard reasons
cannot be reconstructed from this archive.** Rejected count is not a reason code.

The selected DAG does establish concrete limitations:

- producer Projections g47→g49→g164 and g48→g50→g337 are
  Projection→Filter→Aggregate; current row-id proof cannot cross Aggregate.
- consumer g44→g43 leads through three Joins to four CTERefs, not a unique
  row-id-carrying base Get. Root g46→g45→g44 is Limit→Order→Projection, not TopN.
- [selective projection proof](../../../../../../crates/optimizer/src/aggregate/late_payload.rs)
  (518ff) needs a unique stored source, a safe row-id path, reduced cardinality
  and positive benefit;605ff explicitly rejects crossing a join due to missing
  lookup locality/fanout cost proof. `prove_unique_rowid_path`1040ff has no
  Aggregate or CTERef case. TopN rewrites709/860ff have their own shape guards.
- These are selected-path constraints, not proof that all38 attempts failed the
  same guard. The rule also includes matched-prefix lowering. No safety guard
  was weakened and no late-fetch implementation was added.

### Scan-local late materialization already exists

`late_payload_fetch` is **not** the only way to read keys before payload.
[rowset.rs](../../../../../../crates/execution/src/operators/scan/rowset.rs)270ff
adds dynamic RF predicates;616ff binds materialization from their actual columns;
238ff passes Late predicate columns to storage. [segment_iterator.rs](../../../../../../crates/storage/src/rowset/segment/segment_iterator.rs)1356ff
gathers surviving rowids and fetches remaining columns via `read_by_ordered_rowids`.
It adapts dense/sparse access in both directions. The existing runtime-predicate
rebind test was inspected, not run this round.

For the four-column store scan, a date-only RF gives predicate/deferred/eager
widths8/24/32. Under the default unknown-selectivity.25 and gather penalty2,
initial late cost20<32 when enabled. This is conditional source-level reasoning,
not an observation of each Q11 batch's actual mode; the archived plan does not
expose that full runtime decision. 38.05% overall survival does not determine
per-batch or per-page density.

[page_reader.rs](../../../../../../crates/storage/src/rowset/page_reader.rs)333ff
already has decoded-page admission: sequential access materializes; sparse
gather uses selected rows, decoded groups/runs and page-local probation.
[column_iterator.rs](../../../../../../crates/storage/src/rowset/column/column_iterator.rs)1092ff
can materialize/cache the whole data/null page bundle. Scattered survivors can
touch almost all pages even when fewer rows are output. The Sequential fallback
still calls `materialize_all()` if decoded-cache admission returns None: merely
turning off the cache is **not** a scan-through implementation. Thus zero logical
late-fetch publications does not prove absent RF→payload execution, and survivor
ratio cannot be converted directly to skipped bytes.

### Row counts and DECIMAL width

Earlier independent [default profile](../../20260912/native-necessary-condition-v1/default-profile-v1.json.gz)
has store scan output1,096,053, with date membership RF/date build730rows;
plan base cardinality2,880,404 gives38.05%. This is not a current same-image
RF-off experiment, nor a measurement of decoded rows or distinct physical I/O.
Read-only inspection of the current DuckDB seed independently confirms
store2,880,404/web719,384/customer100,000/date73,049 rows (raw inspection archived).
The162MB metric is cumulative **buffer-fill input**, including encoded and
decoded representations, not162MB of unique disk reads.

Paro decimal(7,2) uses8-byte Int64 value slots and decimal storage pre-encoding
width (`common/types/logical_type.rs`319 and `storage/codec/physical_layout.rs`7).
Default Decimal encoding uses BitShuffle/LZ4, so encoded bytes are not8/row.
DuckDB1.4.4 uses Int32 for precision7 ([versioned width boundaries](https://raw.githubusercontent.com/duckdb/duckdb/v1.4.4/src/include/duckdb/common/types/decimal.hpp)).
The two decimal slots could be narrowed in principle with a complete arithmetic,
vector/codec/storage contract change; **not a one-line type switch** and not
implemented here. Two BIGINT keys plus two decimals are32 versus24 raw bytes/row,
not an entire-row2× difference. Neither this ratio nor survivor38% proves that
Q11 encoded reads could be halved.

DuckDB 'never caches decoded data' is too broad: its [numeric bitpacking](https://raw.githubusercontent.com/duckdb/duckdb/v1.4.4/src/storage/compression/bitpacking.cpp)
has scan-local unpack buffers, and [dictionary decompression](https://raw.githubusercontent.com/duckdb/duckdb/v1.4.4/src/storage/compression/dictionary/decompression.cpp)
reuses dictionary/selection vectors. No Paro-like cross-query full decoded-page
cache was identified in the inspected numeric path. Read-only `PRAGMA storage_info`
on the actual1.4.4 seed confirms store amount columns are RLE/BitPacking, keys
RLE, customer strings Dictionary/FSST, with separate validity segments. This
supports distinguishing encoded size from value-slot width; it is not an
exhaustive audit of cross-query decoded-data reuse.

## Delivery / remaining work

Harness3 new tests and14 existing TPC-DS contract tests pass; clean harness test
and release attestation archived. No Rust algorithm changes, full SQL regress or
formal noninferiority campaign. T1's W91.800 vs90.363 legacy gap remains open;
the four baseline failures remain unmodified/unblessed. User mixed changes and
staged historical evidence are untouched.

Next single direction: evaluate **first-touch decoded-page promotion on the
existing scan/page-reader path**, preserving warm reuse and bounded resources.
E2 justifies that experiment, not a guaranteed40ms production saving. The byte
axis remains an investigation with unresolved per-binding guard distribution
and actual late/gather/page-promotion coverage, not an implementation mandate.
