---
name: paro-benchmark
description: Run Paro engineering performance gates, workload checks, policy calibration and archive workflows in benchmark/. Use for benchmark implementation and performance-regression triage; route controlled cross-engine or cold/warm comparison campaigns to the repository's paro-evidence skill.
---

# Paro benchmark

## Worktree and authority first

- Resolve the user's selected checkout with `git rev-parse --show-toplevel`;
  inspect status and HEAD before builds or writes. Never change to a hard-coded
  main checkout. Preserve staged, unstaged and untracked user work.
- For a controlled code comparison, reuse an agreed clean isolated checkout
  with recorded source identity. Do not reset a dirty tree, copy all its
  changes, or create another large worktree/build/data copy automatically.
  An explicitly requested dirty-tree investigation is exploratory, not clean
  control/probe evidence.
- Running checks does not authorize baseline/policy updates, archive publication
  or cleanup. Missing baselines and failing gates are findings, not permission
  to bless them.

## Choose the existing workflow

Use Paro's framework; do not replace it with a custom timer or another product.

- Workload/PR checks: `benchmark/runner.py`, `harness/`, `workloads/`, `suites/`.
- Performance gates: `policies/`, `baselines/`, `harness/performance_gate/`.
- Calibration/archive/bisect: `harness/cli/` and `harness/archive/`.
- Cold-planning, first-statement or cross-engine claims: read the complete
  sibling skill [paro-evidence](../paro-evidence/SKILL.md), then use `corpora/`.
  This is a different workflow within the same benchmark framework.

Read `benchmark/README.md` and the selected policy/suite before running a gate.
For calibration or publication, also read the relevant CLI and archive contract;
`calibrate`, manifest rebuild and finalization are writes, not read-only checks.
For workload authoring, follow README's workload layout and actual validator
modes, with scoped setup/teardown, stable query IDs and explicit parameters.
Add plan/metric guards only where the workload requires them; EXPLAIN sidecars
may re-execute only a declared read-only, side-effect-free query. Run the
smallest relevant workload before proposing a separately authorized baseline.

## Discover commands from this checkout

The Makefile and actual CLI are authoritative, not a copied option catalog.

1. Run `make -C benchmark help` to obtain current targets and variables.
2. Use the selected environment's Python (normally `benchmark/.venv/bin/python`)
   to run `benchmark/runner.py --help` and the intended subcommand's `--help`.
   Archive modules are inspected from `benchmark/` with `python -m ... --help`.
   Help/discovery must not build a server, execute SQL or update a baseline.
3. Use `make -n -C benchmark <target> ...` to verify Make-to-CLI expansion.
   Do not invent missing flags or install a newer CLI to make an old command
   work. Install missing declared dependencies only when needed for the
   authorized run; do not silently upgrade the toolchain.

Representative entry points (verify against live help before use):

```sh
make -C benchmark run SUITE=pr-smoke
make -C benchmark run WORKLOAD=graph FILTER=1_hop
make -C benchmark check GATE=operator-runtime INCLUDE_SOURCE=operator-runtime-sql
make -C benchmark quality QUALITY_REPORT=/absolute/path/to/report.json
```

`SUITE` excludes `WORKLOAD`/`FILTER`; suite query IDs are exact, filters are
substrings. `check` is gate/policy-based, not workload-median comparison.
`BASELINE` is still an optional gate reference. Discover `calibrate`, `bisect`,
`archive-*`, `ci` and `quality-collect` through live help; the latter can build
and start its own server, whereas `quality` reads an existing report.

## Run and interpret a gate

- Check the selected source's server/process, dataset and setup/teardown
  requirements. SQL fixtures can mutate data and settings. Use an owned test
  instance, explicit data/temp paths and ports; never assume a listener or
  `./data` belongs to this task. Reuse a suitable available server-lifecycle
  skill when needed. Rust microbench sources need not start Paro.
- Record source/dirty state, binary and harness identity, resolved workload
  parameters, policy/platform, actual resource envelope and diagnostic settings.
  Read effective settings, including `optimizer_verify` and allocation
  instrumentation; do not infer them from a label such as "normal".
- Follow the registered policy's sample minima, calibration, quorum retries,
  coverage and enforcement. Preserve initial failures and all retry samples.
  A zero exit in shadow/soft mode, missing calibration or an Unmeasurable entry
  does not certify a performance pass.
- Validate required results and metrics before interpreting speed. Median
  latency is a summary, not sufficient evidence by itself. Do not drop valid
  slow samples, increase retries until green, or lower thresholds after results.
- Read the comparison-validity contract in `crates/optimizer/readme.md` before
  causal/model/parity claims: no unidentified cross-report ratio-of-ratios,
  uncalibrated cross-grant cost pooling, fingerprint-as-equivalence, or
  underpowered rank-correlation certification. Preregister confirmatory gates;
  label an unregistered run exploratory rather than inventing a registration.

## Outputs and updates

The current runner still has shared defaults under `benchmark/report/`, and
gate retries/source runs can overwrite earlier files. Do not claim run-scoped
isolation already exists. For ad-hoc `run`, `--output` must use a fresh parent
directory (the sibling `summary.md` is also written). Gate invocations sharing
that output tree must be serialized until the run/source/attempt migration is
implemented; disclose any unavailable retry artifacts instead of certifying
complete sample retention. Do not create a new copy of the repository merely
to isolate a report.

Separate output paths never prove performance isolation: competing CPU, memory,
I/O, servers and shared setup state can still invalidate measurements. Mixed
concurrency is a declared workload, not incidental concurrent experiments.

Only explicitly authorized baseline changes may use `bless`. Require the
selected policy's correctness/plan guards, source set, release/fresh-data
requirements and calibration; inspect the resulting diff. Policy evolution
needs separate authorization and versioned review, not a flag to waive a
failure. Never bless missing/error results or use `regress-update` to make a
performance run green. Archive publication and history/data deletion are also
separate actions requiring scope.

Report commands, exact evidence paths, source/build identity, effective gate
status and unresolved failures. Do not claim improvement from unit tests or a
diagnostic run.
