# Paro contributor guide

This is repository knowledge for humans and coding agents, not configuration
for a particular assistant. Shared skills live in `.agents/skills/`; private
notes belong in the ignored `.agents/local/`. Keep this guide and shared skills
versioned. Classify a file's content and owner before deleting it: a tool-named
directory can also contain the only copy of architecture or workflow knowledge.

## Start with the selected checkout

- Inspect the requested worktree's HEAD, status and relevant diffs. Preserve
  unrelated staged, unstaged and untracked changes; do not switch to a fixed
  main checkout, reset user work or create extra large worktrees automatically.
- Read the affected crate's `lib.rs`, `Cargo.toml` and tests before changing its
  public boundary. Prefer the existing owner/contract over another parallel API.
- Use the toolchain in [rust-toolchain.toml](rust-toolchain.toml) and dependency
  versions in [Cargo.lock](Cargo.lock). Do not silently upgrade either to make a
  command pass. Actual flags and side effects come from the selected Makefile
  and CLI, not historical command lists.

## Architecture and ownership

Paro is a Rust columnar database combining relational, vector, full-text and
graph execution. `parod` is the PostgreSQL-wire front end. The query lifecycle
is approximately:

```text
server / session -> parser: SQL to AST
                 -> compiler: planner / binder -> optimizer -> compiled program
                 -> execution: admission -> selected image -> pipelines -> results
instance: database registry, shared resources, recovery and lifecycle
context: statement environment, resource accounting and cancellation
```

This is runtime flow, not a Cargo dependency graph. The compiler entry points
`compile_statement` and `compile_statement_with_parameter_types` consume an
already parsed AST; session owns parsing, prepared statements and cache policy.
A ready physical plan need not have materialized its execution image.
Admission and deferred lowering remain real work, not free work outside the
compiler timer. See [compiler/compile.rs](crates/compiler/src/compile.rs) and
the [optimizer contracts](crates/optimizer/readme.md).

The workspace members and feature-dependent edges are defined by the
[workspace manifest](Cargo.toml) and each crate's manifest. Update this map
when a crate is added, merged or moved; do not infer new dependencies from the
order of these rows.

| Crate | Responsibility / source entry |
| --- | --- |
| `paro-common` | [Shared types, errors, vectors/chunks, memory and configuration](crates/common/src/lib.rs) |
| `paro-parser` | [Tokenizer, SQL AST with source spans, parser and visitors](crates/parser/src/lib.rs) |
| `paro-planner` | [Binding, expressions, logical plans and shared immutable physical-plan contracts](crates/planner/src/lib.rs) |
| `paro-optimizer` | [Ordered rewrites, estimation, bounded regions, costing and physical construction](crates/optimizer/readme.md) |
| `paro-compiler` | [Planner/optimizer/execution orchestration](crates/compiler/src/lib.rs) |
| `paro-execution` | [Physical operators, expression evaluation, pipelines, spill and admission](crates/execution/src/lib.rs) |
| `paro-context` | [Statement/session environment, resources, write guards and cancellation](crates/context/src/lib.rs) |
| `paro-catalog` | [Schemas, catalog entries, MVCC and dependency tracking](crates/catalog/src/lib.rs) |
| `paro-function` | [Built-in function infrastructure](crates/function/src/lib.rs) |
| `paro-external` | [External ABI, routines, sources and worker runtime](crates/external/src/lib.rs) |
| `paro-storage` | [Buffer pool, rowsets/tablets, codecs, indexes, statistics and row storage](crates/storage/src/lib.rs) |
| `paro-journal` | [Ordered durable logging and publication](crates/journal/src/lib.rs) |
| `paro-transaction` | [Transaction types, snapshots, validation, locks and commit coordination](crates/transaction/src/lib.rs) |
| `paro-scheduler` | [Execution tasks, events, worker scheduling and coordination](crates/scheduler/src/lib.rs) |
| `paro-instance` | [Database registry, storage ownership, startup/recovery and shutdown](crates/instance/src/lib.rs) |
| `paro-session` | [SQL dispatch, prepared/portal state, transactions, COPY and results](crates/session/src/lib.rs) |
| `paro-server` | [pgwire connections/cancellation and the parod entry point](crates/server/src/bin/parod.rs) |

Dependency and implementation constraints:

- `paro-execution` consumes `paro-planner::physical`, not the optimizer.
  Planner must not depend on optimizer or execution. Test-only construction
  fixtures may depend on optimizer; production and target-specific dependency
  edges are checked by `tools/ci/check_plan_boundaries.py`.
- Lower-level storage/transaction facilities must not gain dependencies on
  planner, optimizer, execution or session merely to reuse a convenience type.
  Check normal versus dev dependencies and active features, not just a path.
- Preserve `paro-transaction`'s scalar-only boundary: its
  [manifest](crates/transaction/Cargo.toml) has a `types-only` configuration
  selected without default features. The default `runtime` feature currently
  enables `parking_lot`, `paro-common` and `paro-journal`; do not describe the
  entire default crate as dependency-free or confuse its comment with that
  feature contract. Keep engine/storage/catalog dependencies out of the scalar
  boundary and test the affected feature configurations.
- Crate-root re-exports are not uniform. In particular common, planner, storage
  and execution expose types through submodules. Read `lib.rs` rather than
  guessing an import path. Journal APIs live in `paro-journal`, not an obsolete
  storage WAL namespace.
- Honor the fallible memory/accounting and vector-copy contracts. Changes to
  ownership, admission, cancellation or spill require tests at the real entry
  point, including error and rollback paths.
- Check [header rules](tools/ci/check_headers.py) for new code, including
  derived-source attribution. Memory API guards live in
  [check_memory_runtime_api.py](tools/ci/check_memory_runtime_api.py) and
  [check_vector_copy_fallible_api.py](tools/ci/check_vector_copy_fallible_api.py).
- Build-profile overrides live in `Cargo.toml`; parser/server dev/test overrides
  are deliberate. Do not change instrumentation, stack sizes or profiles as an
  unreported performance intervention. Boot configuration, worker stacks and
  shutdown handling are owned by the server entry point and instance lifecycle;
  avoid duplicating their numeric defaults here.

## Development and validation entry points

Inspect the root [Makefile](Makefile) for the current recipes. Not every target
has the same Cargo flags. The following is a routing map, not a complete flag
catalog or a claim that the current tree passes these checks:

| Need | Entry point |
| --- | --- |
| Build / local server | `make build`, `make release`, `make run` |
| Workspace tests | `make test` (includes the configured Rust test stack) |
| Static checks | `make static`: headers, fmt, memory guards, optimizer calibration, strict Clippy, actionlint when available |
| Affected crate/test | `cargo test -p <crate> <filter> --locked` |
| SQL compare-only | `make -C regress ci FILE=<pattern>` or `make -C regress ci` |
| Regression harness tests | `make regress-unit` |
| Benchmark workflows | [benchmark README](benchmark/README.md), live `make -C benchmark help` and CLI `--help` |
| Full local integration | `make ci-local`, after inspecting its process/data cleanup and resource scope |

Start a server only when the task needs one. Use explicit owned ports, data,
temp paths and PID/lifecycle handling. Existing listeners and the default data
directory are not disposable. `ci-local` and Python smoke/CI recipes create or
reset test data and manage processes; they are not read-only diagnostics. Read
compound/recursive recipes before using even a Make dry-run; `-n` is not a
sandbox for recipes containing recursive Make commands.

For SQL tests, read [regress/README.md](regress/README.md) and its Makefile for
connection settings, directives, fixtures and `.actual` output. `FILE` is a
substring filter: inspect its match set. Missing `.result` or failing output
does not authorize `regress-update`; only an explicitly scoped, independently
validated expected-result change may be regenerated and reviewed. Preserve
result/type/order failures and distinguish environment failures from semantics.

Use the versioned [paro-benchmark](.agents/skills/paro-benchmark/SKILL.md) workflow
for engineering gates and [paro-evidence](.agents/skills/paro-evidence/SKILL.md)
for controlled comparisons. Neither a skill nor a successful check authorizes
bless, policy evolution, archive publication or history cleanup. Do not copy
performance numbers or mutable experiment status into this guide.

## Optional Python execution

The current Rust boundary is `paro-external` (`abi`, `routine`, `runtime`,
`source`), not separate external-ABI/runtime/routine crates. The worker is in
[runtimes/python-worker](runtimes/python-worker/README.md) and the SDK in
[python/paro_udf](python/paro_udf/README.md). Ordinary SQL must remain usable
without Python; `python-udf-startup-smoke` exercises the disabled-runtime path.
Use `python-udf-unit`, `python-udf-regress` and `python-udf-ci` only for the
relevant validation scope and inspect their current Make recipes first.

## References and handoff

For local implementation references, inspect the actual available checkouts
(for example DuckDB, CockroachDB, StarRocks, PostgreSQL or relevant search/graph
projects). They are optional, not fixed absolute-path dependencies. Record the
revision relevant to a comparison; copy neither behavior nor licensing
assumptions from a different implementation.

Before proposing integration, run affected tests and the required static/full
checks for the change's scope. A skipped tool or unresolved fixture is not a
pass. Report exact source/build identity, commands and remaining failures;
do not bless failure output or commit mixed user work. Keep durable architecture
knowledge here or in its owning crate, shared workflows in `.agents/skills/`,
and temporary local notes in `.agents/local/`. Review additions through Git
rather than hiding future shared skills behind a per-name ignore allowlist.
