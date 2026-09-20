# C2 correctness integration (2026-09-20)

This is a correctness baseline, not a latency campaign. Chain, handoff,
budgets, cost coefficients and default stopping policy are unchanged. F2's
historical dirty-build attribution remains open and is **not admitted**.
Quality-policy satisfaction is not proof of complete search.

## Sources and protocol

[Source manifest](c2-source-manifest.json) identifies the clean control
`f450d93c` and probe `8d33a84a`, their independently built release binaries,
the 99 SQL files, immutable seed, pinned DuckDB 1.5.5 extension and lifecycle
harness. Later Rust changes (`8976a670`, `9f16d5e8`) add tests only. No original mixed-worktree
source is silently included. The main, documentation and old experiment
worktrees' 133/17/57 preserved files still have their recorded contents.

Both arms use `typed-result-v2`, verifier on, four threads, 2 GB, handoff off,
chain off, FD limit 65536 and fresh private data copies. Server-observed
environment validation is mandatory. Correctness captures are serial and
are not normal compiler/C1/warm samples. One-second sampling of a long-running
Q74 is separately archived; it must not be treated as a timing sample.

A shared-target control build initially reused the probe executable. It was
detected before any experiment, archived as INVALID, and replaced by an
independent build after invalidating only the copied workspace build cache.
The valid control SHA is `cb439767d3a2c49f154be107308c7789e951a2cfd27f8b53a4f552d8517063aa`;
probe SHA is `06973e0431936b3533f6a79d4e30d11a579e9685f51ac7c8d9c52e942066b9bb`.

## Production state authority

| Observation | Authority and propagation | Not permitted |
| --- | --- | --- |
| Verified executable | Exact FrozenCandidate and its resource/dependency contract; retained through GrantOptimization and portfolio admission | Inventing an image for a declared but unsearched class |
| No current candidate | TaskRegistry `NoCandidate { cursor }`, exact goal/phase/ReadSet; unresolved grant coverage | Calling a yielded, budget-limited or mandatory-only prefix infeasible |
| Completed subproblem | Existing scoped completion/bound proof, including phase, facts and grant | Reusing mandatory completion for unseen optional implementations |
| Proven infeasible / unsupported domain | No production certificate producer currently exists; absence stays unknown | Manufacturing either certificate from an empty winner |
| Explicit capability, resource or internal error | Original Result and SQLSTATE; failed-task metadata preserves the supplied cause | Swallowing the error into an empty frontier or safe fallback |
| Cancellation | Original cancellation error; existing rollback/temporary-goal cleanup | Reclassifying cancellation as ResourceStop or Complete |

Tests cover mandatory→optional recovery without logical publication, repeated
reuse, empty prefix, partial and missing expected grants, resource shrink,
fact invalidation, group merge, rollback, exact nonselected RF choices, and
error/cancel preservation. A real planner cross-product test uses the
production implementation registry, costing, freeze and extraction with a
statement snapshot; it retains class 0 when expected class 2 is unresolved.
SQL coverage includes the complete corpus and regression suite; custom Leaf
tests alone are not claimed as the SQL gate.

## Result contract and Q02/Q39

[Comparison contract](result-comparison-contract.md) preserves explicit
aliases, order, types and wire labels. Generated labels are checked against
the source projection AST with the pinned parser, not regex parenthesis
removal or blanket name erasure. Q02 returns 2,513 exactly matching rows with
passing schema and order checks.

Q39 keeps its raw exact-float failure. The separately registered
`integer-welford-schedules-v1` oracle proves the captured input arithmetic,
all CV-filter decisions, duplicate-preserving self-join and exact ordering
prefix: 360,000 inputs, 90,000 groups, 6,250 eligible groups, 243 output rows,
972 approximate values per engine. The one exact CV=1 group is excluded.
This is not a tolerance inferred from observed ULPs. Unsupported domains,
crossing predicates and approximate ranking/LIMIT remain Uncovered.

## Integration gate ledger

Final paired capture and regress inventories are recorded alongside this
file. Raw evidence is retained under
`/Users/linjunhong/paro-convergence-archive/20260920/c0/`.
No `.result` has been blessed or overwritten.

| Gate | Result | Evidence |
| --- | --- | --- |
| Workspace check, all targets, locked | PASS | `c2-workspace-check-final.log` |
| Workspace tests | 6,846 passed / 0 failed / 85 ignored | `c2-workspace-tests-attested.log` |
| Optimizer full suite (included above) | 1,348 passed / 0 failed | Same workspace log |
| Strict all-target Clippy, `-D warnings` | PASS, no blanket allows | `c2-strict-clippy-final.log` |
| Benchmark tests | 177 passed; one existing return-value warning | `c2-benchmark-tests-attested.log` |
| SQL regress harness tests | 100 passed / 1 skipped | `c2-regress-harness-tests-r2.log` |
| Clean control/probe 99-query execution | 99 / 99 execute in each arm | [Per-query ledger](c2-corpus-summary.md) |
| Cross-engine correctness certificate | **OPEN**: 68 exact + 1 bounded numerical pass + 30 Uncovered in each arm | [Full verdicts and raw errors](c2-corpus-gate.json) |
| Full SQL regress, verifier on | 157 pass / 27 raw snapshot failures in each arm; no new observed result differences | [Every-block paired audit](c2-regress-gate.json) |
| Normal performance baseline | **NOT RUN**: correctness gate open | No latency/parity certification |

Rust validation ran on clean `9f16d5e8`; the final benchmark tests include
the regression-audit adversarial tests at clean `f2ba8603`. The existing
Python warning is `test_performance_gate.py::test_policy` returning a value;
it is not suppressed. An initial planner fixture lacked statement context;
the corrected fixture keeps its original assertions. Its failed log is kept.
Benchmark and regress unit suites run separately because both use a top-level
Python package named `harness`; the earlier collection failure is retained.

Both complete screens use the same protocol. All 99 native schemas agree
between control and probe. For 98 queries the raw exact values, multiplicities
and row sequences also agree; Q39 alone differs in floating bits and is
independently certified on **both** arms, retaining 57 and 48 raw exact-row
differences respectively. No cross-arm equality is used to waive a DuckDB
comparison failure. Separate schema, exact bag and order verdicts prevent
schema failure from hiding row errors.

All **27 `.actual` files are byte-identical** across the two full regress
runs. Auditing every block in the 27 transcripts (468 blocks per arm), rather
than only names or first differences, finds 68 EXPLAIN snapshot differences
and two configuration-display differences per arm. The latter are
`memory_limit=2GB` and pg_settings' verifier/T0-b read-only diagnostic entries.
There are no missing blocks, orphan actuals or embedded errors. This supports
no new **observed** SQL result regression against the clean control, not a
claim that the 27 raw snapshot failures passed. The original expected files
and actuals are unchanged. Initial startup-connection failure artifacts also
remain archived separately, not counted as a completed suite.

## Reproduction and review

All commands run in the integration checkout, with the pinned benchmark
venv and `ulimit -n 65536` for server experiments. The source manifest supplies
exact binaries, seed, SQL, oracle and harness identities. Use
`benchmark/tools/capture_result_difference.py --harness <manifest-harness>
--binary <control-or-probe> --seed <manifest-seed> --duckdb <manifest-database>
--sql <corpus-file> --output <new-capture> --verify on --handoff off`
serially, without overwriting archived captures.

Recompute each Q39 certificate using
`benchmark/tools/verify_integer_aggregate_relation.py --inputs <archive>/c2-q39-input-bags.json
--result <archive>/c2-screen-<arm>/39.json --spec benchmark/evidence/optimizer-convergence/20260920/c0/q39-numeric-relation.json
--output <new-certificate>`. The certificate binds seed/oracle/input/result
hashes; the 403 MiB input capture is kept outside Git.

Recompute the full regress audit with
`benchmark/tools/audit_regress_reports.py --repository .
--control <archive>/c2-regress-control-report --probe <archive>/c2-regress-probe-report`.
The audit uses the existing result parser and checks later blocks even after
an earlier EXPLAIN mismatch. Counterexample tests reject orphan actuals and
detect a later result mismatch. It does not bless anything.

[Validation manifest](c2-validation-manifest.json) pins the complete logs,
certificates, audit and preservation rechecks. Raw data lives at the archive
path above, not in an assumed-to-be-permanent temporary experiment directory.

## Remaining work / performance withholding

Thirty queries keep the corpus gate open: 12 primary exact type-domain
mismatches, 11 unsupported output-identity mappings and 7 primary ordering
comparison gaps. Their complete errors and secondary checks are listed in
the corpus ledger; none is silently reclassified as a display-only pass.
They require explicit type/identity/order contracts, not a global name, type
or float exception. General floating threshold/order/LIMIT cases outside
the registered finite oracle also remain Uncovered. A complete infeasibility
or unsupported-domain certificate producer is not implied by truthful
unresolved task state.

No Q04/Q11/Q74 normal performance baseline is run while correctness remains
uncertified. Compiler/admission/image/C1/warm and actual-grant performance
statistics are **not measured in this stage**. Restored legal optional work
is not rolled back to recover old counts. No ProofComplete, production-ready,
sub-10ms or parity claim is made.
