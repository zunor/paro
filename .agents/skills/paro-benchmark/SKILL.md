---
name: paro-benchmark
description: Run Paro engineering performance gates and exploratory cold/warm, cross-engine or operator comparisons. Default to lightweight diagnosis; use paro-evidence only for formal parity, release or non-inferiority claims.
---

# Paro benchmark

Default to exploration: answer the specific question with the smallest useful
experiment. A pilot needs source/binary, SQL/data, settings, timer scope and raw
samples, not a preregistered certification campaign. Label conclusions accordingly.

## Use the selected checkout and maintained runner

Inspect HEAD/status, preserve user work, and read [benchmark README](../../../benchmark/README.md).
Discover commands from `make -C benchmark help`, the intended CLI's `--help`
and its implementation; don't copy stale flags. Inspect recursive Make recipes
before a dry run. Reuse the declared Python environment and toolchain.

- Engineering workloads/gates: `benchmark/runner.py`, `harness/`, `policies/`.
- Corpus A/B and cold/warm: `corpora/tpcds_compare.py`; read
  [CORPORA](../../../benchmark/CORPORA.md) for data and result contracts.
- Compile diagnosis: `corpora/cold_planning.py` and EXPLAIN COMPILE.
- Actual operators: EXPLAIN ANALYZE and `corpora/d6_execution_profile.py`.
- Known-cardinality checks: `corpora/plan_quality.py`.

Use supported collectors/validators; extend their missing capability instead
of building another timer or per-report parser. Full-corpus certification is
not required for a targeted diagnostic run. Help must not start a server.

## Running the TPC-DS collector

The `corpora/` scripts are not importable as a package or runnable by path
alone: run them from `benchmark/` with `PYTHONPATH=.:corpora
.venv/bin/python corpora/<script>.py --help`. Facts that are easy to miss:

- `tpcds_compare.py` builds `cargo build --release --locked --bin parod` in
  **its own checkout** and runs `<checkout>/target/release/parod`; it refuses
  a source tree that changes during the build. The reported `source` is that
  checkout, so the collector revision is the tested revision.
- Normal compile receipts carry `compiler_elapsed_us` only when the caller
  exports `PARO_COMPILE_WORK_EVIDENCE=1`; otherwise compile time is uncovered.
  Check one cell's `compile_receipts[].compile.raw` before a long campaign.
- `--report runs/<id>/<name>.json` creates the RunOutput directory
  `runs/<id>/<name>-run/`; each invocation is one campaign over `--start..--end`.
- `--server-data-dir` is an immutable seed, cloned per process. It must have a
  root-relative catalog (`storage_dir: ./databases/db-N`); older absolute-path
  seeds are rejected. Rebuild one under the data root: start the tested
  `parod` with the empty seed directory as cwd and `--data-dir .` on an owned
  port, run `corpora/tpcds_setup.py --dsn ... --csv-dir <csv>` (declared keys,
  matching `--metadata-track generator-declared`), then stop it with SIGTERM.
  The CSV directory needs all 24 tables plus `schema.sql`/`load.sql`, whose
  `COPY` paths are absolute. Use the same storage format for every arm.
- Query SQL lives in the DuckDB checkout under
  `extension/tpcds/dsdgen/queries`; see [CORPORA](../../../benchmark/CORPORA.md).

Comparing another revision: add a detached **source-only** worktree and give
it its **own** Cargo target (`CARGO_TARGET_DIR=<worktree>/target`); the
collector reads `<checkout>/target/release/parod`. Never let two checkouts
share a target. Path-dependency artifact hashes are workspace-relative, so the
checkouts overwrite each other's crates, and freshness is judged by source
mtimes: a "fresh" build can silently link the other revision's code, and two
arms then measure one program. Before collecting, build every arm, confirm
distinct binary hashes and one plan- or behavior-level difference per arm,
and check each cell's recorded `binary_sha256` against its arm. If a shared
target was already used, `cargo clean --release -p <changed crates>` before
trusting it again. Use that revision's own collector flags; the
single-planner tree has no policy switch, and quality-versus-pipeline Memo
comparisons need a revision at or before `5ba483059`
(`--optimizer-search-policy`). Remove the worktree and its target after review.

## Practical comparison

1. Name the question and intervention. Check the actual binary, data seed,
   DOP/memory, cache regime, verification and observers. DuckDB's declared
   version is in `benchmark/requirements.txt`; verify the imported runtime
   and extensions, not a different environment's package listing.
2. Use owned ports/data and explicit output paths. Separate directories do
   not isolate CPU, I/O or servers. Serialize competing performance runs.
   Share one build target sequentially rather than cloning build products.
3. C1 is the target's cache-miss occurrence zero in a fresh process; keep warm
   and diagnostic cohorts separate. For warm A/B, alternate arms in balanced
   order in one process only when that binary exposes the real intervention,
   settings are restored and cache identity separates it. Different binaries
   require separate owned processes with interleaved blocks.
4. Verify full types, bag multiplicity and required order outside timing.
   Preserve errors/timeouts/slow samples. An unavailable oracle is a limitation,
   not permission to invent an epsilon or bless the result.
5. Rank queries by excess time versus the comparator, with coverage/timeouts
   visible; distinguish absolute burden from geometric mean and per-query ratios.
   SQL-feature categories are hypotheses, not causal attribution.
6. Diagnose relevant outliers with one separate EXPLAIN ANALYZE per engine
   (only when execution/side effects are in scope). Match operator metrics by
   actual plan/port/phase. Directly time first-execution work; do not manufacture
   its components by subtracting medians from different cohorts.

Keep normal timings trace-off. Record necessary bounded compile/admission
receipts and observer overhead; diagnostic durations never certify speed.
For planner changes also use [paro-optimizer](../paro-optimizer/SKILL.md).

## Gates and retention

Existing engineering gates retain their policy, calibration, sample minima and
failure semantics. A shadow/soft zero exit or Unmeasurable result is not a pass.
Running checks does not authorize bless, policy changes or expected updates.

Put raw reports under an ignored, uniquely owned `benchmark/runs/<run-id>/`
(or an explicit external run root). Inspect actual output flags; a top-level
path alone does not prove all legacy writers/retries are isolated. Keep all
attempts within a run. Delete disposable runs after review, normally within
14 days; preserve unresolved unique reproducers. Do not install an automatic
purge or delete another task's data.

Commit only a short decision when it changes design, with exact source,
commands/settings, limitations and how to reproduce. Do not commit routine
logs/traces/data or generate an evidence package for every edit.
For a formal release, parity or non-inferiority claim use
[paro-evidence](../paro-evidence/SKILL.md) before confirmatory sampling.
