# L1-LADDER: L0 stop, unequal physical work

2026-09-13. Clean runtime92b904a5, binary
`42a1231fb2b5a8379a9a9ad08953b352313d9a5a60f592b926ccb0cf54d71dcd`.
No engine algorithm or policy change. Fixed36 fresh process pairs, balanced
18ABBA/18BAAB, original L0 `count(*) from store_sales`, four workers/2GB.
Second occurrence cache hit, same image, zero fills and zero decoder constructions
in all36 blocks; all four typed results per block agree. All samples retained.

| Rung / second execution | Paro median | DuckDB median | Paired geometric ratio /95% bootstrap CI |
|---|---:|---:|---|
| L0 count(*) |1.440916ms|.100666ms|13.769768 [12.349506,15.298442]|
| L1–L7 |not run|not run|L0 preregistered stop triggered|

Observed second-execution p95: Paro1.572625ms, DuckDB.177167ms. This is
E1-on/statement-trace-off, not normal E1-off C1 certification. Per-rung E1 overhead
has not been measured. The Paro wire/client path and DuckDB worker timer also
differ; especially at sub-millisecond scales their ratio is not inner-loop speed.

## Decisive plan check (separate one-block diagnostic)

After both timed executions, plain EXPLAIN showed:

- Paro: `PROJECTION → AGGREGATE count_star → ROWSET_SCAN store_sales (2880404)`.
- DuckDB: `COLUMN_DATA_SCAN (~1 row)`; no store_sales scan/aggregate in this plan.

The raw plan is archived. L0 does not compare equal scan work: DuckDB has a
shortcut while Paro enters its rowset/count pipeline. The diagnostic Paro warm
execution scalar was1186µs with zero fill/decoder/dictionary/zone-map work.
This exposes a pre-expression path difference, **not** a universal13.77× scan
deficit. The median workload gap is only1.340250ms; it cannot by itself explain
the historical L7 ~66ms gap. No cross-cohort subtraction is used as attribution.

The L0 operational stop is obeyed. We do not force scans, change SQL, remove
metadata, or silently continue the ladder to escape it. No dominant expression
layer was identified, so **L2 is not eligible and was not implemented**. The
requested additive reconstruction of L7 remains unverified; branched workloads
and the catch-all L7−L3 contrast would not constitute a causal decomposition even
if they telescoped numerically. Next decision: redesign/authorize a matched-work
baseline before inferring scanning or per-value throughput from these contrasts.

## Preparation failures and evidence boundaries

First launch rejected the output path before processes; corrected parent-root
index in5b655181. Second launch started processes but rejected asymmetric metadata
before any target query (samples empty). Existing data has declared Paro keys
and no DuckDB keys, also true in M/E2/E3. Pre-observation amendment92b904a5 retained
the same track and recorded this asymmetry instead of modifying data. No target
timing existed when amended. These failed attempts/logs are archived, not erased.

`raw/manifest.json` binds every raw report, SQL, log and build attestation.
The completed36-block report and independent diagnostic have separate paths;
source/binary/SQL/harness/data/seed/environment identities are in each report.
The `normal_e1_off_timing=false` flag is intentional. No parity, L7 reconstruction,
plan-quality improvement, or new-default claim follows from this experiment.
