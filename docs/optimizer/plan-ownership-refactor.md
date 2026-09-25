# Plan ownership and optimizer stage layout

## Decision and scope

Keep `paro-planner`: it owns binding, bound logical IR and shared physical plan
contracts. Do not create parallel `paro-binder`/`paro-plan` crates. Execution's
normal dependency on optimizer is removed; its dev-dependency builds fixtures.
The default remains `pipeline`. The explicitly retained production Cascades
policies, resource behavior and diagnostic wire contract are not deleted.

The working baseline was clean `re-op` at `783201754`. The control executable
was built from `0852571cf`; the intervening changes only contain evidence/docs.
The implementation is committed as `6a498b7ab`.
No new worktree or dependency/toolchain upgrade was needed. Cargo.lock adds
planner's use of the already locked workspace `blake3` dependency.

## Resulting ownership

- `planner/physical`: plan topology, specs, properties, dependencies, resource
  contracts, verification, canonical encoding and shared cost values/algebra.
  Scalar/window/access identities no longer need Memo objects to encode them.
- `optimizer/rewrite`: expression, predicate, subquery, CTE, join, aggregate,
  column, limit, graph and external logical transformations.
- `optimizer/estimate`: relational statistics, cardinality bounds, keys and
  selectivity. The former root `CostModel` is named `SelectivityModel`.
- `optimizer/cost`: calibration, operator/access/join work equations, enforcer
  costs and source-response contracts shared by the strategies.
- `optimizer/region`: connected join enumeration and aggregate-grain decisions.
  The existing connected traversal is shared; different DP state domains were
  not incorrectly collapsed merely because their filenames looked similar.
- `optimizer/physical`: implementation/access choices and one committed plan
  builder (`PhysicalPlanBuilder`) used by both strategies. Selection is not a
  second plan generator, and construction does not rerank selected algorithms.
- `optimizer/diagnostics`: profile, work accounting and rejection reporting.
  Typed compile receipts, bounded Detail and lifecycle checks are retained.
- `optimizer/optimizer`: explicit staged driver and isolated Cascades adapter.
  Memo ids/property interning remain in `cascades`, not in shared plan contracts.

The old public import paths are not compatibility aliases. Private optimizer
imports borrow planner types, while downstream consumers import their owner.
Production admission/capability/write checks were not moved behind debug flags.

`make plan-boundaries` rejects direct or transitive planner→optimizer,
planner→execution and execution→optimizer normal/build dependencies. The check
includes optional, target-specific and package-alias edges; five tests exercise
those rules. CI/static checks also follow the relocated physical source files
and generated calibration artifact.

## Validation

This is a structural refactor, not a performance intervention. No new latency
or corpus-wide performance claim is made.

| Check | Result |
| --- | --- |
| `cargo check --workspace --locked` | Passed |
| `RUST_MIN_STACK=16777216 cargo test --workspace --locked` | 7,022 passed, 0 failed, 85 ignored |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | Passed |
| Release `parod` build | Passed |
| Affected-crate rustfmt, changed-file headers, diff whitespace | Passed |
| Plan dependency guard and five unit tests | Passed |
| Memory, fallible vector-copy and operator-runtime guards | Passed |
| Generated optimizer calibration check | Passed |
| Real SQL, pipeline and quality | 12 cases per policy: exact rows/types/order, structure/selection/output identities and stop state agree |
| Full SQL regress, control / fresh probe | Each 164 passed, 21 failed, 0 skipped; no expected files changed |

The SQL probe includes constants/NULL, grouped aggregates, composite left join,
correlated anti join, shared CTE, cumulative/native windows, DISTINCT/TopN,
aliases, UNION ALL and IN. Identity comparison is supplementary to actual
typed-result comparison, not a substitute for it.

All 21 regression failures occur in both arms. Nineteen `.actual` files are
byte-identical. The other two differ only in their arm-owned Python IMPORTS
fixture path. Block comparison retains the existing 26 EXPLAIN differences and
four non-EXPLAIN differences: those two fixture paths, the Memo-only
`paro_optimizers()` metric expectation and the default-policy setting description.
This does not bless the snapshots or declare the full regression suite green.

Two invalid comparisons were investigated rather than counted as passes:

- A capture immediately after table creation differed from one after database
  recovery. The recovered old executable and new executable then matched.
  Quality captures also require matching fixture lifecycle; after the same
  regression setup, all 12 identities matched in both versions.
- Reusing the control regression database produced `EXECUTE stmt1: SELECT 6`
  instead of `SELECT 3` in `prepared_cursor`. A fresh owned probe database
  passed that case and restored the exact 21-case failure set. The reused-data
  run is retained as inconclusive; its underlying reuse issue was not repaired
  or asserted to be a refactor regression.

Binary SHA-256:

- Control: `3cba155047e713e8ec30887f7d02f27d447a5af83a1c868390b2400c56fcf37d`
- Probe: `61b881f078b1f4a52ad6169d809efb24253237bae0afe5ab605e2e9eebf076b0`

Equal bounded capture SHA-256 (SQL and expected results included):

- Pipeline: `ac63ddd8ee506f6cb7e29072e21bf1b5689cc8c390788a8e7b26445437541aaf`
- Quality: `9d70498a36bf9e789c40a26ea21889664174d5025703cd93e0ff4f75c54477af`

Local raw validation, including the non-accepted attempts, remains under
`/private/tmp/paro-plan-ownership.jN17DT/`. These temporary paths are not a
permanent evidence availability promise. All task-owned servers were stopped.
Whole-repository header/format debt outside the affected files is not certified
by the scoped checks; this delivery does not claim `make static` passed.

## Explicit remaining boundaries

This establishes stage layout and shared plan ownership, not deletion readiness
for Cascades. Some shared normalization laws still live beside Memo adapters
(`restriction`, domain transfer); selectivity has a Memo scalar view, and the
driver/region limits still consume the existing search-budget settings.
Extracting those adapters and minimizing the public optimizer API are distinct
follow-ups. They must preserve one semantic implementation, not copy it into
another directory or hide a dependency behind a facade.

No rule retirement, cost-model coefficient change, multi-grant removal, blanket
identifier ban, history cleanup or new performance gate was bundled into the
move. Algorithm changes still require independent correctness and corpus
quality validation. Production Cascades removal needs its own explicit scope.
