# EXPLAIN (COMPILE) supported trace matrix

Status: current re-op delivery. This is a support boundary and correctness
record, not a claim that the complete optimizer Trace Matrix is ready. Search
policy, budget, grant selection defaults, F2 and performance targets are
unchanged.

## Supported end-to-end cells

| Cell | Producer boundary | Consumer / test evidence | Status |
| --- | --- | --- | --- |
| simple `SELECT`/CTE `EXPLAIN (COMPILE)` TEXT | real session compiler, sealed `CompileCapture` | `crates/session/tests/explain_compile_test.rs` | Supported |
| same document as JSON | `CompileDocument` renderer/validator | `crates/execution/src/explain/compile_render.rs` tests | Supported |
| bounded `DETAIL` for supported SELECT/CTE | actual Memo/TaskRegistry search milestones; fixed opaque refs | session Detail test plus optimizer lifecycle-retention tests | Supported, bounded |
| `COMPILE, ANALYZE` | one sealed compile, real admission, one execution handle and terminal receipt | session PgWire integration and executor receipt tests | Supported |
| extended Parse/Bind/Describe/Execute | known parameter types; Describe does not execute; incomplete Bind rejects | `prepared::extended_query` tests | Supported |
| forced compile and cache hit | immutable compile receipt; current compile is `NotExecuted` on hit | receipt collector and contract tests | Supported |
| selection/reservation/lowering/image/terminal | facts recorded at their real executor boundaries | execution receipt tests and typed `paro_optimizers()` rows | Supported |
| normal benchmark sample association | exact statement/execution identity snapshot, no latest/occurrence guessing | `test_compile_work_evidence.py`, receipt contract tests | Supported; `Uncovered` is fail-closed |
| campaign ownership | CampaignId/ArmId/QueryCase/RunId/SourceId/AttemptId; cell is QueryCase×ArmId | `test_run_output.py`, runner/gate source tests | Supported |
| bounded writer | payload, terminal-control and manifest UTF-8 limits before atomic publish | RunOutput quota and manifest-limit tests | Supported |

## Explicitly outside this boundary

- Full Trace Matrix coverage across every protocol, source, workload and Detail
  signal is not claimed.
- DML/DDL/utility and unsupported parameter/protocol shapes remain explicit
  unsupported or uncovered results.
- Historical reports without receipts are not backfilled.
- C2 correctness closure, F2 admission, optimizer policy changes and latency
  targets are not certified here.
- Existing EXPLAIN-only regress differences and the `topn_large_fallback`
  guard remain separately classified; no expected files are blessed by this
  document.

## Invariants

1. A Summary capture allocates no Detail event buffer. Detail is opt-in and
   bounded at `MAX_DETAIL_EVENTS`; overflow is counted in `omitted_detail`.
2. Detail events are copied from real same-Memo task, proposal, candidate,
   quality, grant and search lifecycle records. The renderer does not rerun
   rules, reprice candidates, or dump the Memo.
3. Compile, admission and execution records are immutable at their own
   boundaries. A cache hit references the original compile receipt; every
   execution has its own admission receipt.
4. Missing, truncated, incompatible or ambiguous cross-record association
   blocks only the joint interpretation. Timings, failures and slow samples
   remain retained.
5. The run manifest and every owned result/failure/summary write are bounded
   by encoded UTF-8 bytes and published atomically. Terminal failure is never
   overwritten by a later retry.

The external design source is
`/Users/linjunhong/workspace/paro-docs-design/optimizer/optimizer-trace-matrix.md`.
This repository document records the subset implemented and tested in the
current re-op; it must not be read as `TraceMatrixReady`.

## Historical validation snapshot

The implementation gate was validated from clean-source commit
`4f6786a65abcd9c4da8fd5de71aa57abb13d6d68` with the pinned DuckDB 1.5.5
benchmark environment. The following are gate results for this supported
boundary, not a performance campaign:

| Gate | Result |
| --- | --- |
| `cargo check --workspace --locked` | pass |
| `cargo test --workspace --locked` | pass |
| strict all-target Clippy (`-D warnings`) | pass |
| benchmark unit suite | 174 passed |
| fresh-directory SQL regress | 177 passed, 8 known EXPLAIN-only failures, 0 result mismatches |

The eight regress failures remain unblessed and are retained as
`agg_join_subsumption`, `agg_singleton_groups`, `explain_analyze`,
`explain_basic`, `join_explain_advanced`, `rowset_scan_pushdown`,
`statistics_query`, and `pgvector_topn_filter_flow`. The reused-directory
`prepared_cursor_t` duplicate-fixture failure is invalid-run evidence and is
not folded into the fresh-directory result. No expected or `.actual` file was
changed by this delivery.

The validation manifest is
[`compile-trace-matrix-validation.json`](compile-trace-matrix-validation.json).
It records the exact support boundary and deliberately leaves full Matrix,
C2, F2, and performance claims uncertified.

The later re-op commits are intentionally not folded into this table without
a new clean validation manifest. Typed quality-detail export, canonical
physical identity, successful extended-Sync draining, and durable RunOutput
publication have targeted checks only; they do not retroactively certify this
historical workspace/benchmark/regress snapshot.

## Compile Evidence v2 convergence work

The current re-op follow-up keeps the same ownership boundary for normal and
diagnostic output. Corpus cells are emitted through the shared typed receipt
contract and are owned by an explicit `QueryCase x ArmId` registration. Raw
typed `EXPLAIN (COMPILE)` captures are written once under the owning run and
the cell payload keeps only a bounded path, schema version, and content hash.
Readers resolve and verify that reference before validating the Rust-owned
document; a missing or mismatched capture is `Uncovered`, not a successful
sample.

Candidate and transformation-task Detail records now retain producer sequence
numbers. The exporter no longer assigns source order with `enumerate()`, so
omitted records remain represented by the producer-side omission accounting.

The targeted contract tests and workspace check cover this follow-up. A fresh
clean-source campaign, full SQL regression, strict Clippy run, and complete
Trace Matrix gate remain required before this document can claim
`TraceMatrixReady`, C2, F2, or a performance result.

The current verification run completed the workspace tests, strict Clippy,
benchmark unit tests, and the high-file-descriptor SQL regress harness without
changing expected files. The regress result was 177 passed and 8 existing
failures (`agg_join_subsumption`, `agg_singleton_groups`, `explain_analyze`,
`explain_basic`, `join_explain_advanced`, `rowset_scan_pushdown`,
`statistics_query`, and `pgvector_topn_filter_flow`). Those failures remain
unresolved evidence; they are not converted into a pass by the typed output
work.
