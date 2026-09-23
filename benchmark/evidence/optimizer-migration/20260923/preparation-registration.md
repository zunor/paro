# Demand-driven compile preparation: registered matrix

EvidenceId: `compile-preparation-20260923-v1`. Control source: `bc5e6d06`.
Register before collecting any new timing samples. This is a finite engineering
pilot, not a powered latency/parity certification.

## Intervention and invariants

1. Fork a DISTINCT feasibility alternative only after the decomposition
   owner's read-only eligibility check. Keep eligible alternatives mandatory.
2. Ordinary compilation retains fixed summary counters and receipts, not a
   per-goal/frontier/source-payload matrix. Explicit Detail remains available;
   transfer finished diagnostic ownership rather than cloning it.
3. Prepare immutable enforcement geometry once per exact physical recipe/goal;
   filter pending recipes before copying handles and reuse frontier workspace.
   Do not cache resource feasibility, skip ReadSet validation, change active
   response membership, or certify unfinished work as complete.

No changes to budgets, optional deadline, quality policy, resource envelope,
verification, cost model or execution. The combined probe is the scoped code
commit following this registration; collection records its exact source and
binary, not a hand-edited attribution to an older build.

## Inputs and collection

- Reuse the relocatable SF1 seed, SQL, CSV and DuckDB database identities in
  `remaining-contracts-registration.md`. Seed validation succeeded with digest
  `256bf479ca982e30c96f9109fb9920d04233c9643b7bcc6a5fd3f5a50554c927`.
- DuckDB declaration is 1.5.5 in `benchmark/requirements.txt`; imported native
  SHA-256 is `85fad85339c7e345eabb66a33a25d8503c3ee8881f9d298fa8499a97617dbf68`.
  Loaded core_functions, icu, json and parquet are built-ins at v1.5.5.
- Four threads, 2GB, binary protocol, generator-declared metadata, `quality`,
  optimizer verifier off, default optional deadline (30 seconds), no other
  behavior interventions. This metadata track cannot certify engine parity.
- Normal fresh process / cache miss / trace off, with the existing bounded
  `PARO_COMPILE_WORK_EVIDENCE=1` observer. Check the first normal receipt before
  continuing. Compiler is `compiler_elapsed_us`, not Detail or optimizer time.
- Use the maintained TPC-DS collector and one shared Cargo target, sequentially.
  It builds from the selected checkout: after committing task changes, use
  clean detached control and re-op probe checkouts **in the same worktree**.
  No new worktree/target, source changes during a cell, custom timer or parser.
- Four batches, C/P/C/P; each batch visits Q04/Q11/Q74, three fresh process
  blocks per query, one warmup and one ABBA round per block. Thus six independent
  normal blocks per query/arm, with one separate Detail block per batch/cell.
  Random seeds 2026092301 and 2026092302 for the two paired batches; 10,000
  bootstrap draws. Archive both batches, including slow samples. Switching
  and rebuilding between batches is outside timing; no builds/tests overlap
  collection. Batching still leaves host drift, explicitly NotCertified.

## Gates and bounded delivery

Every sample validates all typed rows, multiplicities and ordering. Result
failure, source/seed/runtime drift, missing required receipt or capacity refusal
stops collection for investigation; no rerun-until-green or sample removal.
Different plans are reported as a separate effect, not identical-work speedup.

Report compiler samples, median/P90, C1/warm, plan/admission identities, search
counts and stop state. Inspect preparation/finish/recipe/subproblem exclusive
Detail buckets and enforcement build/reuse counts separately from normal time.
Q11 half-time remains an aspiration, not an assumed result. Any Q04/Q74 compiler
or warm median regression above 10% triggers analysis before integration;
do not modify baselines or thresholds to pass. All quality-policy stops remain
SearchIncomplete unless independently certified otherwise.

Bound all 12 maintained RunOutputs and referenced captures to 16MiB total;
archive no ordinary server logs or duplicate raw trace streams. Run optimizer
and workspace tests, strict Clippy, benchmark tests and high-FD SQL regress.
No expected-result changes are part of these performance interventions.

## Post-collection fixture-contract amendment

The initial full SQL run found two allocation-number-only profile mismatches.
Apply the existing `explain_logical_ids` alpha-renaming contract to the three
affected EXPLAIN ANALYZE blocks in `fulltext_score_identity` and
`select_topn_fallback_spill`, including their expected-side normalize headers.
Do not regenerate or alter expected SQL, operator/plan text, scores, rows or
numeric id payloads. This test-contract amendment preserves shared-node
identity and does not change the registered binaries, matrix samples,
performance gates or the recorded negative result.
