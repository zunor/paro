# T1 lifecycle and protocol review closure

This addresses the independent review of `e1a04282`. It does not admit C2 or
F2, change optimizer policy, or certify timing neutrality. The original review
and probes remain at `/private/tmp/paro-compile-review.mHJ3gB/`.

## Review disposition

All five reported mechanisms are valid defects or interface holes, not grounds
for replacing the existing observation architecture.

| Finding | Repair | Counterexample/production coverage |
| --- | --- | --- |
| Blocked send ignores cancellation; dropped request retains auto transaction | Scoped ownership of only the request-created transaction; cancellation-aware delivery; original error retained | Real Session: backpressure cancellation, external future drop, writer error, next SELECT, caller explicit transaction |
| Diagnostic writes bypass terminal transport state | Reuse ProtocolSink availability and first transport failure registration | Real TCP/Framed writer shutdown + diagnostic flush + terminal error/finish attempts |
| Unavailable cannot be read; contradictory completion accepted | Context-owned Summary/Unavailable document, current schema 2; one reader; terminal and successful phase closure checks | Both unavailable reasons, Complete/false/5/budget contradiction, successful artifact with uncovered phases |
| Binding-only rules absent | Union of binding/total elapsed, apply and publication keys; separate binding calls/time | Real engine no-match and output-budget refusal after binding, zero apply; finite closure oracle unchanged |
| Public mutable Vec bypasses cap | Fixed-size producer fields, bounded collection methods, immutable profile, sealed-only render view | 809,720 rule updates, 70,000 variants, retained lease after producer drop |

No matching, costing or proof work is rerun to construct Summary. Binding
counters are enabled only when a request capture exists. Binding/apply timings
are a projection of optimizer time, not additive compiler phases. Capture
admission still precedes allocation; no Memo, plan or global event history is
retained. Schema 1 is preserved in Git/history and prior evidence; no compatibility
reader or second handwritten JSON envelope is retained.

## Validation and evidence identity

Implementation: `2c646def`; combined source with reviewed workspace formatting:
`f0a0ff0b9520947c276a223885693f0c22e40c5a`.
Raw new logs and main-worktree preservation receipts:
`/private/tmp/paro-t1-review-DcAkmW/`.

Strict workspace all-target Clippy passed. Benchmark unit tests: 187 passed,
one pre-existing warning. Regress harness tests: 101 passed, one skipped.
Workspace tests: **6863 passed, 0 failed, 85 pre-existing ignored**. Workspace
check passed. Real pgwire query/CTE TEXT/JSON golden checks and the Rust schema
reader passed; the actual SQL output includes binding-only rows with zero apply
attempts. Full SQL regress (verify on, fresh data, FD 65536, one worker):
**177 passed, 8 failed**, with the same eight `.actual` files byte-identical to
the preserved pre-review run. Those failures remain unresolved and unblessed.
The test server and harness-managed replacements have stopped; test data is
retained at `/private/tmp/paro-t1-review-data-5loqcG`.

Clean server build source: `aad7bb79` (documentation only beyond `f0a0ff0b`);
crates tree `56ada930fbe40f1b29a578e50cd12a94d7816bb2`.
Dev `parod` SHA-256:
`260affbb5da337e2e49bf44e409bfddab869defea5c22a4b44ae801e45b7506d`.
Cargo.lock SHA-256:
`26d5f1f12ea0a418d78523cb1645f6cb8501109f9bc8a0c07a073e150a81c23e`.
SQL probe SHA-256:
`f33f0a8354baf0239082a155790acdbb4f186704aa67d3c827db5b130b57ab86`.

| Raw log | SHA-256 |
| --- | --- |
| workspace-test.log | `58f4b57b1ce7ab39267f1b9813e5611eee974b2512eda404b9c1cbc0ada39a29` |
| workspace-check.log | `c538f19da529d1729b32a7c7f4d65fdac683fb620d14711a33379be6955a22e7` |
| clippy.log | `754c33b21d95e9d3d6f5965b31ef45047ea3ec80c9ba43ae9ae6de8f85904d72` |
| benchmark.log | `63b395265a1c6cc0451f199a69e5178be531814b1c68e0a7f62e6d64c91b08c6` |
| regress-unit.log | `50690607e7cbe9c284ebde3405215b70c191fdf4ffc2f4036ac4d8c0eea97d2f` |
| regress.log | `125d65036216e99b78a0c467746d6882c99d9da2b4d912f60378f42ec4bc6bc6` |
| pgwire-summary.log | `9cecf92eca8a2240831316ac609ebce4c35ad6169391124fe95742a886933d5a` |

Both pre/post-review regress report directories are retained alongside the logs.
The checks validate the committed source, **not** the uncommitted behavior
experiments remaining in the main worktree. Validation followed the selected
checkout's `paro-benchmark` workflow and the `paro-start-regress` /
`paro-start-local` lifecycle skills; no baseline update was authorized or run.

Initial development attempts are not passing evidence: the first added
transaction assertion reused a Session after unrelated error cases; lifecycle
counterexamples now start from fresh Sessions. An initial limited-rule fixture
expected binding work even when the fire budget refused the task before matching;
the added output-budget fixture exercises rejection *after* binding instead.
Existing closure/optimum assertions remain intact.

## re-op mixed-worktree disposition

`988519c7` commits exactly 32 formatting-only Rust files. For each, formatting
HEAD and worktree content with the same rustfmt settings produced identical
bytes. This is not blanket admission of the remaining behavior changes.

Fifteen tracked mixed files and 83 historical untracked files were left outside
that commit. Categories requiring separate review/admission:

- Pre-Memo N2 plus normalization-proof wiring and harness propagation: a behavior
  experiment, not T1 observation. No default/policy admission is inferred.
- Quality-preflight traversal/index changes: separate semantic and work-equivalence
  validation required; do not treat them as formatting.
- Removal of both post-freeze resident-contract rebind calls and their helpers:
  contradicts the retained correctness repair; **not safe to commit as cleanup**.
- Temporary projection/aggregate/join debug logging: exposes sample values or
  shape-specific probes; not a reviewed long-term diagnostic interface.
- Stronger native-deferral no-settlement assertions, staging changes and historical
  checkpoint decoding: require their own contract tests and evidence.
- Historical evidence includes unique negative-result sources and patches; keep it,
  do not bulk-add or delete it merely to make status clean.

Original staged/unstaged patches and all modified/untracked file hashes are
retained in the new evidence directory. No user hunk is discarded, no expected
SQL result is blessed, and no history/evidence cleanup is performed.

Integration fast-forwarded re-op to `f0a0ff0b`. Only three overlapping user files
were temporarily stashed; restoration used stash
`50fb3c17c658226de8422ec5a7581f15376dd78d`, which is retained as a recovery point.
The entire remaining diff, compared at zero context after excluding only Git
blob/line-position headers, is byte-identical before and after integration.
Its normalized SHA-256 is
`bf4982eb239dfd005c3d493b9b767b0e663e10c6aa18385d8620d8001c9350c9`.
All 83 untracked files remain byte-identical. Four previously modified file
hashes intentionally changed: engine, planner/mod, optimizer and closure tests,
because those files also contain the new reviewed implementation/test hunks.
The other original file hashes are unchanged; the index is empty.

## Boundaries

T2 ANALYZE/extended protocol, Detail, cross-run receipts and full Trace Matrix
remain deferred. Existing C2 regress failures and F2 evidence gaps are independent.
No performance campaign or fresh 99-query corpus is claimed by this repair.
