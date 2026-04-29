# Paro Benchmark Framework

This directory contains the SQL end-to-end benchmark framework for Paro.

It is designed to benchmark real workloads (graph, vector, fulltext, aggregation) with:
- correctness validation,
- optional baseline regression comparison,
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
├── baselines/               # Checked-in nightly baseline snapshots (optional)
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
./.venv/bin/python runner.py --workload spill --collect-explain-profile
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
- Checked-in benchmark baselines are intentionally deferred for now.
- Before the repository is open-sourced and CI noise is characterized, prefer `make -C benchmark run` to collect reports without `check` / `bless`.
- Enable baseline compare / bless only after CI and machine-level variance are stable enough to keep median-latency regressions actionable.

Create/update a baseline snapshot:

```bash
make -C benchmark bless WORKLOAD=graph BASELINE=baselines/macos-arm64.json
```

Compare current run against a baseline:

```bash
make -C benchmark check SUITE=nightly-full BASELINE=baselines/nightly-linux-amd64.json
```

Notes:
- Regression compare is based on median latency.
- Only strong validation modes participate (`scalar_equals`, `ordered_rows`).
- Benchmark identity includes resolved workload params.
- `SUITE` is mutually exclusive with `WORKLOAD` / `FILTER`.
- Suite manifests use exact query ids via `queries = ["..."]`; they do not reuse the fuzzy `FILTER=` substring semantics.

## Output Files

After each run:
- `benchmark/report/result.json`: full structured report
- `benchmark/report/summary.md`: compact human-readable report

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
