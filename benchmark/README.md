# Paro Benchmark Framework

This directory contains the SQL end-to-end benchmark framework for Paro.

It is designed to benchmark real workloads (graph, vector, fulltext, aggregation) with:
- correctness validation,
- first-class performance gate checks,
- machine-readable JSON reports,
- human-readable Markdown summaries.

## Directory Layout

```text
benchmark/
├── config.toml              # Connection + default runtime settings
├── Makefile                 # setup / run / check / bless / clean helpers
├── requirements.txt         # Python dependencies
├── runner.py                # CLI entrypoint
├── harness/                 # loader / executor / validator / reporter
├── workloads/               # Benchmark workloads (SQL + workload.toml)
├── policies/                # Gate policy TOML files
├── baselines/               # Gate measurement references
├── suites/                  # Checked-in workload/query selections for CI
└── report/                  # Runtime outputs (result.json, summary.md)
```

## Prerequisites

- Python 3.11+ (3.10+ usually works as well)
- A running `parod` server reachable from `benchmark/config.toml`
- Rust toolchain (only needed if you build `parod` locally)

Install Python dependencies:

```bash
make -C benchmark setup
```

Verify server connectivity:

```bash
make -C benchmark ping
```

## Quick Start

Run all workloads:

```bash
make -C benchmark run
```

Run a single workload:

```bash
make -C benchmark run WORKLOAD=graph
```

Collect `EXPLAIN ANALYZE ... FORMAT JSON` sidecars when workload queries allow re-execution:

```bash
cd benchmark
./.venv/bin/python runner.py run --workload spill --collect-explain-profile
```

Filter queries by id substring:

```bash
make -C benchmark run WORKLOAD=vector FILTER=topk
```

Run the checked-in PR smoke suite:

```bash
make -C benchmark ci
```

Run a named suite directly:

```bash
make -C benchmark run SUITE=pr-smoke
```

Run the SQL group commit gate:

```bash
make bench SUITE=group_commit
```

Override workload parameters:

```bash
make -C benchmark run WORKLOAD=graph PARAMS="edges=500000 vertices=50000"
```

You can also run via root-level proxy targets:

```bash
make bench WORKLOAD=graph
```

## Workload Order

Full runs sort workloads by `[meta].run_order` and then by directory name.
The default order is `100`. Workloads that intentionally shrink global
`memory_limit` for spill coverage should use a lower value so they run before
memory-heavy cache-warming workloads.

Suite manifests keep their explicit `[[include]]` order. Keep tight-memory
workloads early there as well.

## Baseline Workflow

Current project policy:
- Generic median-latency baseline comparison has been removed.
- Use `make -C benchmark run` for ad-hoc measurement reports.
- Use `runner.py gate check` or the `make performance-gate-*` aliases for performance
  gates backed by `benchmark/policies/<gate>.toml` and
  `benchmark/baselines/<gate>/<platform>.json`.

Create/update the operator runtime gate baseline:

```bash
PARO_BENCH_CARGO_PROFILE=release PARO_BENCH_DATA_DIR_MODE=fresh \
  make -C benchmark bless GATE=operator-runtime BLESS_RUNS=3
```

Run the operator runtime SQL gate:

```bash
make -C benchmark check GATE=operator-runtime INCLUDE_SOURCE=operator-runtime-sql
```

Run the operator runtime Divan dispatch gate:

```bash
make -C benchmark check GATE=divan-dispatch
```

Compare the current checkout against an archived gate result during a git
bisect:

```bash
make -C benchmark bisect \
  GATE=operator-runtime \
  ARCHIVE=/path/to/paro-perf-archive \
  PID=auto \
  AGAINST=<good-main-commit-sha>
```

Append a clean main/nightly observation to the external archive clone:

```bash
make -C benchmark calibrate \
  GATE=operator-runtime \
  ARCHIVE=/path/to/paro-perf-archive \
  RUN_ID="$GITHUB_RUN_ID"
```

Rebuild the archive manifest from append-only files after one or more
calibration runs:

```bash
make -C benchmark archive-finalize-calibration \
  ARCHIVE=/path/to/paro-perf-archive \
  GATE=operator-runtime \
  PLATFORM=auto \
  POLICY_VERSION=auto
```

Notes:
- Performance comparison is available through `runner.py gate check`; the old
  median-only `--check` path has been removed.
- Benchmark identity includes resolved workload params.
- `SUITE` is mutually exclusive with `WORKLOAD` / `FILTER`.
- Suite manifests use exact query ids via `queries = ["..."]`; they do not reuse the fuzzy `FILTER=` substring semantics.
- Gate policy lives in TOML; baselines are measurement references only. Policy
  controls source selection, metrics, thresholds, static fallback noise floors,
  statistical model, calibration window, coverage/staging, and rollout
  enforcement. A baseline records schema version, gate, platform, policy
  version/hash, source payload, and system/build/runtime fingerprints. Platform
  and policy mismatches fail before the benchmark result is interpreted.
- Gates compare every required query in both directions and fail on missing
  baseline queries, missing current queries, query errors, and missing
  required metrics.
- When archive calibration is ready, calibration-driven noise is authoritative:
  the evaluator derives `directional_rolling_p95_delta` from clean main
  observations and then applies Mann-Whitney + Holm before confirming a
  regression. The static `noise_floors.*` policy values are a local-dev /
  bootstrap fallback for runs without usable calibration.
- In fallback mode, `noise_floors.latency_ms` covers default p50 / throughput
  jitter, `noise_floors.short_query_latency_ms` covers queries whose current
  and baseline p50 are below `noise_floors.short_query_max_p50_ms`,
  `noise_floors.p99_latency_ms` covers tail samples, and `noise_floors.rss_kb`
  covers allocator / OS sampling movement.
- SQL performance baselines refresh only from a clean, isolated run after
  correctness and plan guards pass. `gate bless` requires a release build and
  fresh data-dir marker, runs the selected source three times by default, and
  writes a per-query/per-metric median aggregate. Baselines store only the
  compact evaluator projection, not raw samples or explain/profile sidecars.
- `runner.py gate` resolves the parod process through `--pid`, `PARO_PID`,
  `.ci/parod.pid`, then listener probing on the configured host/port. Every
  resolved PID must have a command line whose executable is `parod`; gates that
  require RSS metrics fail fast when no valid PID is found. Rust microbench
  gates do not require a parod process.
- Divan dispatch gates use the `divan_bench` source and the bench wrapper JSON
  path, not terminal table parsing. The checked-in `divan-dispatch` policy uses
  `measurement_class = "rust_micro"` so calibration windows and minimum
  observations follow the rust microbench policy. The source runs
  `cargo bench --locked`, enforces `PARO_DIVAN_SAMPLE_COUNT` against the policy
  per-run sample minimum (`calibration.per_run_sample_count_rust_micro`, default
  50), and has a bounded subprocess timeout (`PARO_DIVAN_BENCH_TIMEOUT_S`,
  default 600s). Its bless path requires a
  release build but does not require a parod PID or fresh SQL data directory.
  Divan static noise floors are zeroed; local runs without archive calibration
  still report threshold regressions instead of hiding microsecond-scale moves
  behind guessed constants.
- Policy changes require `POLICY_EVOLUTION=1`; the policy version must increase
  when the policy hash changes.
- New suite queries can use `staging_until = "YYYY-MM-DD"` next to the suite
  `[[include]]` entry. Staging entries appear in the gate summary but do not
  hard-fail for missing baseline until they expire.
- Archive calibration is append-only. `gate calibrate` writes unique result and
  calibration files but never writes the manifest; it first rejects execution
  failures and regressions against the current baseline so noisy or regressed
  main runs do not poison calibration. `archive-finalize-calibration` rebuilds
  the manifest from the listing, recomputes rolling calibration, and rebuilds
  the manifest again so callers do not need to know the internal ordering.
- `gate check` reads the calibration manifest, verifies checksums, and uses a
  local 72-hour cache only when the archive is unavailable. Corrupt archive
  objects are reported instead of hidden by cache fallback. Archive outages or
  missing calibration are surfaced in `benchmark/report/gate.json` and the gate
  summary; hard gates degrade to soft for archive/calibration availability
  problems. Policy or fingerprint mismatches still fail before gate evaluation.
- When calibration is ready, the evaluator consumes archive calibration content
  directly: per-run aggregates feed `directional_rolling_p95_delta`,
  Mann-Whitney one-sided tests, and Holm family-wise correction. In quorum mode
  a first-run regression is a threshold + rolling-P95 screen; it retries only
  the failed query set three times by default (`QUORUM_RETRIES=`), and the
  confirmed decision uses first + retry aggregates without skipping RSS or
  other required metrics. Gate JSON includes p-values, Holm alpha, calibration
  sample count, derived noise floor, and any `PowerDeficit` diagnostics.
- GitHub CI uses the `start-parod` action PID output as `PARO_PID` and falls
  back to `.ci/parod.pid` only for local/legacy callers. PR benchmark CI runs
  both `operator-runtime` and `divan-dispatch` in quorum mode under the policy
  enforcement setting (`shadow` during rollout). When
  `PARO_PERF_ARCHIVE_REPOSITORY` is configured, PR jobs check out the archive
  for calibration reads; main and nightly jobs append clean calibration
  observations for both gates, finalize manifests/calibration from the
  append-only listing, and commit archive updates with the archive token.
- Policy changes should use the `policy-evolution` label. The dedicated
  workflow refreshes configured platform baselines from release builds and
  refuses non-shadow promotion when required platform runners are not declared
  or when existing baselines would fail the hard fingerprint/policy checks.
- Rollout is policy-driven: `shadow` reports but never blocks, `soft` reports
  highlighted failures without blocking, and `hard` blocks only confirmed
  regressions. Archive/calibration outages downgrade hard gates to soft;
  policy, platform, and fingerprint mismatches remain fail-fast.
- `gate bisect` reads historical `gate_result` objects from the archive and
  compares the current checkout against the archived source payload without
  writing result or calibration files. It returns non-zero for any gate
  regression, which makes it suitable for `git bisect run` after choosing a
  known-good archived commit.

### Operator Runtime Baseline Contract

- Naming: `operator_runtime_baseline` and `operator-runtime-baseline` are the
  canonical baseline workload and suite names. Numbered perf-stage names are not
  used for operator-runtime benchmark entry points.
- Owner: execution runtime maintainers own `operator-runtime`,
  `operator-runtime-mixed`, and `divan-dispatch`; policy files live in
  `benchmark/policies/`, and non-shadow promotion is checked by
  `tools/ci/check_policy_evolution.py`. A benchmark without an owner stays
  shadow-only and cannot become a release gate.
- Result storage: local raw reports live under `benchmark/report/`; release
  baselines live in `benchmark/baselines/<gate>/<platform>.json`; rolling
  calibration lives in the append-only performance archive configured by
  `PARO_PERF_ARCHIVE_REPOSITORY` in `.github/workflows/ci.yml` and
  `.github/workflows/benchmark-nightly.yml`.
- Bless permission: only policy-evolution runs from release builds, fresh data
  dirs, and explicitly declared platforms may refresh checked-in baselines.
  Local blesses are for investigation unless the policy-evolution flow guarded
  by `tools/ci/check_policy_evolution.py` produced the same source set.
- Runtime contract: gate payloads and fingerprints record thread count, query
  memory cap, temp directory, data scale, random seed, and single-task fast-path
  status from `PARO_BENCH_*` / `PARO_PERF_*` env vars via
  `benchmark/harness/runtime_contract.py`. `benchmark/Makefile` exports local
  defaults; CI exports the same values before starting `parod`.
- Re-run rule: noisy PR failures use quorum retries for the failed query set.
  `benchmark/harness/cli/gate_check.py` implements `QUORUM_RETRIES`; if archive
  calibration is unavailable, use the static RSS/P99/noise floors and do not
  bless a new baseline from that run.
- Baseline coverage: `operator-runtime-baseline` covers selective scan,
  count/filter, integer and string hash join, high-cardinality aggregate,
  large sort, forced external hash join, and IE-join fallback. The main
  `operator-runtime` gate includes those queries under staging until all
  platforms are blessed.
- Mixed concurrency: `operator-runtime-mixed` measures N small queries plus one
  large OLAP query, scan/join mixed execution, and temp-directory contention
  with P99/RSS through `benchmark/harness/sources/mixed_sql_suite.py`. Create
  its first baseline with
  `make -C benchmark bless GATE=operator-runtime-mixed PID=<parod-pid>` after
  the normal operator-runtime baseline is green.
- Hot-path audit: benchmark jobs start `parod` with `PARO_ALLOC_AUDIT=1`; the
  operator profiler emits `allocator_tracking_event_count`, and the Divan
  dispatch source stores per-chunk allocator tracking events in the projected
  baseline.

Run the operator runtime gate from the repository root:

```bash
make performance-gate-sql
make performance-gate-divan
```

Refresh its SQL baseline:

```bash
make performance-gate-bless
```

The operator runtime SQL gate requires RSS sampling for memory coverage. The
CLI accepts `PID=<pid>` through Make, `--pid <pid>` through `runner.py gate`,
or `PARO_PID` / `.ci/parod.pid` when `--pid auto` is used.

## Output Files

After each run:
- `benchmark/report/result.json`: full structured report
- `benchmark/report/summary.md`: compact human-readable report
- `benchmark/report/gate.json`: performance gate outcome and archive health

If a query opts into explain sidecars, the JSON report also includes `explain_profile` with flattened operator rows.
If memory collection is enabled, the JSON report additionally includes `memory_tags` and `spill_metrics`, and the Markdown summary adds explain / tag-delta / spill-delta sections.

## Add a New Workload

1. Create `workloads/<name>/workload.toml`.
2. Add `sql/setup.sql` and `sql/teardown.sql`.
3. Add one or more `sql/q_*.sql` query files.
4. (Optional) Add `sql/build.sql` for index build timing.
5. (Optional) Add per-query setup / teardown SQL by setting `setup = "sql/q_setup.sql"` and `teardown = "sql/q_teardown.sql"` on a `[[query]]`; use this for session-local settings such as `force_external`.
6. (Optional) For read-only, side-effect-free queries that are safe to execute twice, set `collect_explain_profile = true` and `allow_reexecute = true` in `workload.toml`.
7. Avoid enabling explain sidecars for queries that already start with `EXPLAIN` or otherwise cannot be safely re-executed.
8. Run:

```bash
make -C benchmark run WORKLOAD=<name>
```
