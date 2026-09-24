# Q04: certified linear DECIMAL execution

Registration: [q04-c1-v1](../q04-c1-registration.md).
Control source: `328dd1b4` (runtime code identical to `8dfc2b1a`).
Implementation: `397fa88f`, with contract hardening in `c7e02c67`.

## Change and scope

Q04's selected plan already contains date-domain filtering and narrow-key
partial aggregation. Exploratory CPU sampling identified the nullable DECIMAL
expression path: three equal-scale add/subtract nodes each perform generic
per-value dispatch and materialize an intermediate vector.

The expression program now lowers eligible linear DECIMAL trees to one
NULL-strict i64 kernel. Every removed node must independently prove totality
from its declared input/output precision and scale. An absolute leaf envelope
also bounds reassociated partial sums. Casts, scale changes, overflow-capable
arithmetic, wide decimals and shared-expression boundaries remain separate.
Leaf evaluation order is unchanged; subtraction signs and NULL demand are
preserved. Collection is iterative and bounded to 32 input occurrences.
Output copy-on-write and validity allocation remain fallible.

This is a general execution lowering, not a Q04-specific rule or a change to
Memo search, cost calibration, resource grants, stopping policy, SQL, storage
data or build profile. It neither reassociates floating-point aggregates nor
removes customer grouping columns without a uniqueness proof.

## Performance is not yet certified

The first control batch is retained byte-for-byte in
[control-interfered-run](control-interfered-run/manifest.json), including all
slow samples, normal receipts and the single diagnostic capture. Concurrent
Go compilation and VM CPU activity were observed after collection; these
samples cannot establish a causal speedup or engine parity. The collector's
`Completed` status means collection finished, not that environmental validity
passed.

The probe collector was interrupted after building and before SQL sampling.
The user then requested implementation and correctness testing first, with
performance deferred. There is no accepted probe timing and no improvement,
warm non-regression or parity claim. Do not compare this control with a later
quiet-host probe; collect a new registered interleaved comparison.

Raw sampled stacks and free-text server logs are not ordinary archived
evidence. No samples were discarded to improve a reported ratio. This compact
archive remains well below the registered 20 MiB limit.

## Correctness validation

- Final `RUST_MIN_STACK=33554432 cargo test --workspace --locked --quiet`:
  6,952 passed, zero failed, 85 ignored (including existing ignored doctests).
- `cargo check --workspace --locked` and strict workspace/all-target Clippy:
  passed.
- `make -C benchmark test PYTHON=.venv/bin/python`: 207 passed.
- Four function proof/kernel tests and two expression-program/executor tests
  cover independent arithmetic, nested subtraction, dictionary/constant/NULL
  inputs, empty batches, precision limits, bounded lowering, CSE and reused
  output validity.
- Runtime-memory and fallible vector-copy guards passed. Changed source headers
  passed. The full header audit still reports 169 issues in 57 files; all those
  files are unchanged from `8dfc2b1a`, so no full-header pass is claimed.

An initial workspace run reached passing library/integration tests but failed
at doctests because a superseding build invalidated an old dependency artifact.
The final verification above was rerun serially to completion; this was not a
product-test failure or an expected-output change.

- Final `cargo build --release --locked --bin parod -j 4`: passed.
- Compare-only SQL regress, fresh owned instance, FD limit 65,536 and
  `optimizer_verify=on`: **185 passed, zero failed/skipped/new**. No expected
  changes. The existing ignored `regress/report` directory was restored after
  retaining this run's output separately.
- SF1 Q04/Q11/Q74, final release binary, binary protocol, quality policy,
  verifier on, four threads and 2GB: exact typed identity/result multiset/ORDER
  contracts all passed against declared DuckDB 1.5.5 (6 / 90 / 92 rows).
  This was an untimed correctness-only execution, not a performance cohort.
  It reused `ImmutableDataSeed`, `isolated_paro_server`, `run_paro_raw` and the
  same `BoundResult`/typed-result validators used by `tpcds_compare.py`.

Exact build identity and result digests are in [validation.json](validation.json).
Expected SQL results, performance baselines and policies were not regenerated
or blessed. All owned test servers are stopped; unrelated processes, data and
worktrees were not changed.
