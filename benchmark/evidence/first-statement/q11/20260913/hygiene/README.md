# Hygiene: clean-source tests, no fixes or blessing

Runtime/source92b904a5, clean worktree `/private/tmp/paro-tcwc-baseline-ZjwFtb`.
Release binarySHA256 `42a1231fb2b5a8379a9a9ad08953b352313d9a5a60f592b926ccb0cf54d71dcd`
matches L1 build attestation. User mixed changes in main worktree excluded.
No tests/builds ran concurrently with M or L1 measured queries.

## Optimizer

`cargo test -p paro-optimizer --lib`: **1213 passed,5 failed** (1218 total).
New atomic-batch budget counterexample passes, also independently filtered and
actually ran1 test. Independent Python contract oracle4/4 passes.
Mark-join failure reproduces in isolation; it is not only parallel-test noise.

- Statistics-cache test: unchanged equal-fingerprint assertion; cache invalidation
  does not necessarily imply a changed semantic fingerprint. Expired assertion /
  identity contract to clarify, not blessed.
- Nested RF: unchanged1-vs2 retention coverage failure, actual composition/identity
  defect per source and selected-edge evidence.
- CTE multi-output: archived allocation/publication defect remains open; current
  test stops earlier at undeclared expected grant and cannot retest its assertion.
- N-ary sharing: archived finite-budget representation/fingerprint contract still
  open; current assertion likewise masked by expected-grant precondition failure.
- New mark-join failure: one-class fixture disagrees with context-derived expected
  class; fail-closed entry rejection, not a demonstrated mark-join semantic error.

See TRIAGE.md for evidence/uncertainty and the distinction between archived and
current failures. No fixtures, expectations, allocation or domain identity fixed.

## Full SQL regress

Owned fresh data/port16433,4 workers/2GB, default policy, no E1/handoff/trace
switches, jobs1, update0/write_actual0. Runner's runtime-profile restarts inherit
the same limits. Initial soft FD limit256 caused EMFILE in the owned test server
and cascading failures; no full completion summary. Entire failed attempt is in
`raw/fd256/`. It is not a collection of independently diagnosed SQL regressions.

After archiving that attempt, new data directory and soft FD limit16384 for both
server and runner: **164 passed,20 failed,0 skipped,0 new,44.87s**, all184 cases
completed. Full command:

```
ulimit -n 16384
PARO_HOST=127.0.0.1 PARO_PORT=16433 PARO_USER=paro PARO_UPDATE=0 PARO_WRITE_ACTUAL=0 make -C regress check PYTHON=/Users/linjunhong/workspace/paro/regress/.venv/bin/python3
```

Failures span EXPLAIN/observability, aggregate/join/spill, CTE, memory/statistics,
search/fulltext and transaction settings. Raw expected/actual diffs are retained;
they are **not all declared stale assertions**, nor silently blessed. No repair
was required for the isolated measurement tasks, so no unrelated SQL changes.
Increasing FD capacity completed this run; it does not prove absence of a leak
or resolve resource lifecycle ownership. No live owned listener remains.

`make ping` reads its config rather than the supplied environment and attempted
6432; direct psycopg verification on owned16433 returned[(1,)] before the complete
run. No ping/runner framework repair included. Both fresh test data directories
remain in /private/tmp for inspection; no user data or baseline files deleted.

`raw/fd16384/` retains runner output, full report/error/server logs, manifests.
`raw/unit/` independently retains optimizer/oracle/isolated logs. All hashes bind
uncompressed and gzip bytes. This is an honest failing suite, not a green gate.
