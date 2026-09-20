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
Final Rust integration and SQL results are recorded below after completion.

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
All 83 untracked files remain byte-identical. Four previously modified file
hashes intentionally changed: engine, planner/mod, optimizer and closure tests,
because those files also contain the new reviewed implementation/test hunks.
The other original file hashes are unchanged; the index is empty.

## Boundaries

T2 ANALYZE/extended protocol, Detail, cross-run receipts and full Trace Matrix
remain deferred. Existing C2 regress failures and F2 evidence gaps are independent.
No performance campaign or fresh 99-query corpus is claimed by this repair.
