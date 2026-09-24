# Q04 linear DECIMAL execution: fresh comparison

EvidenceId: q04-c1-round2. Registered before collection at the user's request.
This is a small engineering pilot, not powered parity certification. No data
from the interrupted/interfered q04-c1-v1 collection are reused.

## Intervention and identities

- Control: clean `328dd1b4`, runtime code identical to `8dfc2b1a`.
- Probe: clean `a79caf5b` plus this registration only; implementation is
  `397fa88f` / `c7e02c67`. Only certified linear DECIMAL execution changes.
- Current probe binary SHA-256:
  `a13d084667d1971b837547fded00c0c5e83f8196a6fa8d7f20abc40fe2b5b29a`.
  Build each selected source through the maintained collector and record its
  exact binary/source attestation; never substitute a saved binary silently.
- Immutable Paro SF1 seed SHA-256:
  `256bf479ca982e30c96f9109fb9920d04233c9643b7bcc6a5fd3f5a50554c927`.
- DuckDB database SHA-256:
  `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`.
- Unmodified Q04 SQL SHA-256:
  `97c5894b2651a60ffe18177af6072bc8afd0e50a8f0000699974da771ceeffd3`.
- DuckDB declaration: `benchmark/requirements.txt`, version 1.5.5; actual
  native module SHA-256:
  `85fad85339c7e345eabb66a33a25d8503c3ee8881f9d298fa8499a97617dbf68`.
  Loaded core_functions/icu/json/parquet are built-in v1.5.5.
- Same selected Python 3.14.3, macOS 15.5 arm64, Rust 1.92.0 and locked
  dependencies. Harness/SQL/CSV/schema/build hashes are retained by the collector.

## Collection

One existing checkout and target, sequential builds; keep temporary binaries
with their hashes, restore re-op afterward. No new worktree or dataset mutation.
Use the current `tpcds_compare.py` with three fresh normal processes per batch,
one warmup and one seeded ABBA warm round per process, and one separate Detail
process. Run C/P/C/P, seeds 2026092405 and 2026092406 for the two batch pairs:
six independent normal blocks per Paro arm. Retain all samples. Use 10,000
bootstrap draws at process level, not warm invocations as independent samples.

Fixed settings: four execution threads, 2GB, binary results, quality policy,
optimizer_verify off, normal statement trace off, no pre-touch, no allocator
instrumentation or seed experiments. Set PARO_COMPILE_WORK_EVIDENCE=1 and
PARO_DIAGNOSTIC_SEARCH_STOP_MS=30000 explicitly; no search-budget reduction.
Separate bounded Detail is structural evidence only, never normal timing.
Generator-declared metadata is identical between Paro arms but is asymmetric
to DuckDB's metadata; this run cannot certify cross-engine parity.

The host has ambient VM/background-service activity. Record bounded process
snapshots before/after each batch; do not kill unrelated processes. Do not start
normal sampling during an unrelated compiler or competing test/benchmark run.
If such work appears during collection, retain the whole affected batch and
stop further confirmatory collection; no selective removal of slow rows.
Ambient interference or drift can make the pilot inconclusive even if all
SQL checks pass. No claim of an isolated machine or zero observer overhead.

## Interpretation and limits

Validate complete typed results, multiplicities, ordering, cache miss, receipts,
actual grant and plan structure. Missing normal compile_work stops compiler
comparison without deleting C1 data. Check the first completed batch before
continuing. All valid slow observations, errors and missing evidence survive.

Report all samples and arm medians for normal compiler, C1 and warm. For each
arm, use the maintained block bootstrap for its paired Paro/DuckDB C1 ratio.
Assess the code intervention from the Paro observations themselves (not a ratio
of ratios); report the two batch-pair median ratios separately and describe
any disagreement. A >10% compiler/warm regression needs investigation. Call an
improvement directional only when both pairs agree and identity/work checks
pass; these small samples do not certify causality or tail non-inferiority.
Even a C1 point ratio <=1.10 is only "close in this pilot", not parity.

Bound retained compact evidence to 5MiB; no routine .parod.log, raw CPU samples
or binaries in Git. Do not bless results or alter policy/thresholds. No extra
sampling until green; uncertainty or interference is a result, not permission
to rerun selectively.
