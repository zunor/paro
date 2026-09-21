# EXPLAIN (COMPILE) supported trace matrix — Compile Evidence v3

Status: current re-op delivery. This is a support boundary and correctness
record, not a claim that the complete optimizer Trace Matrix is ready. The
current producer/consumer envelope is v3; v1/v2/v5 readers reject input. Search
policy, budget, grant selection defaults, F2 and performance targets are
unchanged.

## Supported end-to-end cells

| Cell | Producer boundary | Consumer / test evidence | Status |
| --- | --- | --- | --- |
| simple `SELECT`/CTE `EXPLAIN (COMPILE)` TEXT | real session compiler, sealed `CompileCapture` | `crates/session/tests/explain_compile_test.rs` | Supported |
| same document as JSON | `CompileDocument` renderer/validator | `crates/execution/src/explain/compile_render.rs` tests | Supported |
| bounded `DETAIL` for supported SELECT/CTE | actual Memo/TaskRegistry search milestones; typed stream-local sequence and parent/ordinal refs | producer/renderer unit coverage; six-shape real PgWire smoke passed, full campaign pending | Targeted only; not fully certified |
| `COMPILE, ANALYZE` | one sealed compile, real admission, one execution handle and terminal receipt | session PgWire integration and executor receipt tests | Supported |
| extended Parse/Bind/Describe/Execute | known parameter types; Describe does not execute; incomplete Bind rejects | `prepared::extended_query` tests | Supported |
| forced compile and cache hit | immutable compile receipt; current compile is `NotExecuted` on hit | receipt collector and contract tests | Supported |
| selection/reservation/lowering/image/terminal | facts recorded at their real executor boundaries | execution receipt tests and typed `paro_optimizers()` rows | Supported |
| normal benchmark sample association | exact statement/execution identity snapshot, no latest/occurrence guessing | `test_compile_work_evidence.py`, receipt contract tests | Supported; `Uncovered` is fail-closed |
| campaign ownership | CampaignId/ArmId/QueryCase/RunId/SourceId/AttemptId; cell is QueryCase×ArmId; summaries contain references only | `test_run_output.py`, runner/gate source tests | Supported, bounded |
| bounded writer | payload, terminal-control and manifest UTF-8 limits before atomic publish | RunOutput quota and manifest-limit tests | Supported |
| stable physical identity | intended typed structural identity boundary | current plan encoder still has a Debug-derived payload fallback | Blocked; not cross-run certified |

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
6. Current compile, cell and receipt documents share schema version 3. The
   producer's Detail sequence is authoritative; Python validation is a
   fail-closed consumer, not a second semantic producer. The physical identity
   encoder is not certified until every identity-bearing payload uses an
   explicit typed encoding; the current Debug fallback is an open boundary.
7. A cell is complete only after its declared owned output/attempt state is
   present. A result file alone is not a completion certificate; missing or
   incompatible receipts remain `Uncovered`.

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

The later re-op commits are intentionally not folded into this historical
table without a new clean validation manifest. This v3 work records the
producer/consumer contract and targeted checks, but does not retroactively
certify a complete campaign or claim `TraceMatrixReady`. In particular, the
current working changes to the physical identity encoder remain unaccepted
until the Debug-derived fields are replaced by typed binary encoders and the
real producer-to-gate campaign is rerun.

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
clean-source campaign, full SQL regression, strict Clippy run, complete Detail
producer coverage, and the typed physical-identity boundary remain required
before this document can claim `TraceMatrixReady`, C2, F2, or a performance
result.

The 2026-09-22 targeted real producer smoke additionally ran
`EXPLAIN (COMPILE, DETAIL, FORMAT JSON)` through PgWire and the shared reader/
validator for `SELECT 1`, a two-table join, CTE/UNION, aggregate, runtime-filter
join, and a multi-child join. The resulting streams contained 34, 222, 230,
101, 282, and 434 events respectively and were accepted without hand-written
Detail fixtures. Their SHA-256 values and the bounded evidence directory are
recorded in `compile-trace-matrix-validation.json`; this remains a targeted
smoke, not a complete campaign.

The historical verification run completed the workspace tests, strict Clippy,
benchmark unit tests, and the high-file-descriptor SQL regress harness without
changing expected files. A new full validation is not claimed by this working
tree snapshot. The retained regress result was 177 passed and 8 existing
failures (`agg_join_subsumption`, `agg_singleton_groups`, `explain_analyze`,
`explain_basic`, `join_explain_advanced`, `rowset_scan_pushdown`,
`statistics_query`, and `pgvector_topn_filter_flow`). Those failures remain
unresolved evidence; they are not converted into a pass by the typed output
work.

## Current first blocking boundary

The current Rust identity implementation still streams `Debug` formatting for
several physical operator/specification and property payloads. The stream is
not EXPLAIN text, but it is still presentation formatting rather than the
required typed binary contract. Therefore the identity changes are not
accepted as a cross-run `PlanStructureId`, and the full producer-to-reader/
gate campaign has not been certified. This is the first blocking boundary for
Trace Matrix closure; no performance or C2/F2 claim is inferred from the
targeted passes.
