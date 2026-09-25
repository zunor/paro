# Single planner ownership validation

This is a **correctness/refactor audit**, not a performance campaign or a new
baseline. Control is default planning at `5ba483059`; probe is the dirty-source
single-planner implementation. Binary and source/input identities are in
`manifest.json`; summary and raw log hashes are in `validation.json`.
The implementation was committed as `9f242d4d8` after validation; manifests
retain the actual dirty-source measurement identity rather than rewriting it.

## Results

- Workspace: 6,368 passed, 85 ignored; strict Clippy and workspace check passed.
- Real Q04 and SELECT 1 PgWire Detail captures validate as v4 with four ordered
  stage completions; their durations are diagnostic, not latency evidence.
- Benchmark unit tests: 219 passed; regress harness: 104 passed, one skipped.
- `corpus.json`: TPC-H 22 strict matches; TPC-DS 98 strict matches plus Q39's
  retained binary64 mismatch. All 121 EXPLAIN renderings are byte-identical.
- `q39.json`: independent `integer-welford-schedules-v1` certification and raw
  rerun outputs. It checks exact input bags, legal arithmetic schedules,
  predicate-boundary separation, output multiplicity and exact order prefixes.
  Both binaries pass. Observed ULP distances are descriptive, not tolerances.
- `regress.json`: whole-block audit of control 164 passed / 21 failed versus
  probe 162 passed / 23 failed. The two new failures are the retired Memo
  observability/profile surfaces. Four common transcripts differ: two owned
  IMPORTS paths, retired rule controls, and SHOW ALL's retired settings. The
  other 17 common failed transcripts are byte-identical. No expected updates.

Selected fingerprints include planning configuration dependencies, so they are
not equal after retirement of the policy configuration. The canonical physical
encoder itself was not changed to hide the difference. Plan rendering is not
a complete identity or SQL-equivalence proof. Summary capture and query execution
are separate invocations; no timing/structure attribution is made between them.

## Scope and reproduction

The audited target uses four threads, decimal 2 GB and verifier enabled. Paired
processes use private relocatable snapshots of one immutable seed. Complete
binary-protocol results are checked outside any timer using the maintained
typed result and numeric ORDER contracts. No timings from these concurrent
validation workloads are used as performance evidence.

The scratch audit scripts and logs remain in
`/private/tmp/paro-single-planner.hVFlzC`: `corpus_equivalence.py` (TPC-DS and
TPC-H), `q39_certify.py`, and `validate_regress.py`. Their output paths are
write-once; a repeat must use a new run directory. Numeric certification reuses
`benchmark/tools/verify_integer_aggregate_relation.py` and the versioned Q39
input/relationship specification, whose hashes are retained here.

The archive is capped at 3 MiB; the enlarged Q39 file is an explicit correctness
exception retaining an unresolved strict difference and its independent
certificate, not a routine trace dump. No raw event floods, binaries, data copies
or server logs are committed. Existing evidence and recovery refs are untouched.

This is not the full ordered JOB/CEB/TPC-DS/TPC-H/LDBC acceptance gate. It does
not certify performance, parity or all-green SQL regress. Header checking has
148 existing whole-tree findings (none in changed/new files); skill validation
is uncovered because its PyYAML dependency is unavailable.
