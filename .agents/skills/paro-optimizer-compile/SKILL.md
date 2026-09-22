---
name: paro-optimizer-compile
description: Diagnose Paro optimizer search with bounded EXPLAIN (COMPILE, DETAIL) and controlled Q04/Q11/Q74 cold/warm measurements.
---

# Optimizer compile workflow

Use this skill for optimizer design, chain replay, and compile-latency evidence.
It complements `paro-evidence`; it does not authorize baseline updates, result
blessing, or history cleanup.

1. Inspect every selected checkout's `HEAD`, status, and diff. Keep `re-op`
   clean. Comparisons may keep one small source worktree per arm (for example,
   current `re-op` plus a detached historical chain replay); for porting work
   branch from current `re-op`. Never reset or copy unrelated user changes.
2. Reuse one `CARGO_TARGET_DIR` sequentially for all source builds; never build
   worktrees concurrently. Reuse `benchmark/.venv` and the immutable TPC-DS
   seed. Save each binary with its source/build identity before rebuilding.
   Remove only explicitly owned temporary worktrees or targets.
3. Preflight the seed before spending compile time: every manifest/rowset path
   must be self-contained or relocatable into the collector snapshot. An
   absolute path back to the source seed is a hard `Uncovered` result; rebuild
   the seed with relocatable metadata or stop. If RunOutput reports a payload
   ownership/`sample_ids` error after a query failure, preserve the first query
   error as a harness defect instead of retrying or treating it as a benchmark
   result.
4. Before collection, register EvidenceId, source and binary identities,
   DuckDB identity from `benchmark/requirements.txt` plus the actual runtime,
   SQL/data seed, resource envelope, process-block/sample counts, and timer
   boundaries. Discover current collector flags from `--help`.
5. Use `benchmark/corpora/tpcds_compare.py` for normal measurements. C1 is
   target occurrence 0 in a fresh process with a verified cache miss; keep
   trace-off C1, warm, and diagnostics in separate cohorts. Validate complete
   typed results, multiplicities, and required ordering outside the timer.
6. Use `EXPLAIN (COMPILE, DETAIL, FORMAT JSON)` only as bounded structural
   evidence. Associate it with the same compile/admission receipt as the normal
   sample and record plan identity, search counters, rule/work ledger,
   candidates, grant, quality state, and stop reason. `SearchIncomplete` or
   `BudgetLimited` is not `ProofComplete`; fingerprints locate plans but do not
   prove equivalence. Detail time is never C1.
   A historical arm may predate the typed `COMPILE` protocol. In that case a
   plain `EXPLAIN` is an exploratory fallback only: record the protocol and
   source mismatch, keep its plan text/timing in a separate cell, and mark the
   arm `Uncovered`/`Incomparable` for receipt, search-state, and C1 claims. Do
   not infer typed counters or pair its wall time with a normal C1 sample.
7. Run registered Q04, Q11, and Q74 cells with identical resources and source
   policy. Preserve slow samples, failures, missing receipts, and unsupported
   modes. Report `Uncovered`, `Incomparable`, or `NotCertified` instead of
   filling missing fields or comparing stale builds.
8. Archive a bounded campaign (`manifest`, cell timings/receipts, one compile
   capture per cell, and README). Do not retain raw event floods or free-form
   server logs as routine evidence. State whether a historical chain arm is
   exploratory or comparable to the current source.
