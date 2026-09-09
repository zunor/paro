# D0 Q11 measurement preregistration — 2026-09-09

This is the frozen measurement contract for the current Q11 evidence archive.
It separates evidence delivery from the performance milestones.

## Target and boundary

- Target: the original SF1 TPC-DS Q11 with the complete 90-row ordered result.
- Primary track: C1 is a fresh Paro or DuckDB engine process after common
  setup, with the target query neither prepared, explained, nor executed
  before timing. It includes parse, compile, admission, execution, fetch and
  native result metadata.
- Paro uses the binary result protocol. The normal cohort has statement trace,
  allocation profiling and debug tracing disabled; a post-timer cache-decision
  side channel must verify a miss. Diagnostic traces are a separate cohort and
  cannot enter C1 statistics.
- The result schema, complete typed multiset and peer-group ordering are
  validated outside the timed region. Data is privately copied per Paro
  process; the OS page cache is not flushed.

## Formal sample plan

- Envelope: SF1, four execution threads, one planning worker, 2 GiB memory,
  one concurrent target statement, metadata track `none`, fixed DuckDB build.
- Five fresh process blocks; three within-process W rounds per block and 30 W
  samples per engine. The cold sample is the first target statement in each
  fresh block. Execution order is seeded and randomized within each block.
- The two-block 90-row custom-shape check in `q11-native-shell-final-v1.json`
  is a refactor diagnostic anchor, not a replacement for this formal plan.

## Statistics and stop rules

- Report every C1 sample, p50, p95, paired log-ratio
  `R = exp(mean(log(Paro/DuckDB)))`, and a fresh-process-block bootstrap 95%
  interval. A result is invalid for the gate if identity, event validity,
  cache-miss proof, result completeness or monotonic timing fails.
- M0 reports the same-operation phase boundary and model-admission status;
  `registered_not_admitted` is not a pass.
- M1 requires Q11 C1 p50 and its one-sided 95% median upper bound to be at most
  200 ms. M2 requires the one-sided 95% upper bound of paired `R` and the
  p50 ratio to be at most 1.25. M3 requires the §12.1 strict `R` upper bound
  at most 1.00 and Paro p50 no higher than DuckDB.
- Every milestone also requires complete results, declared tail-latency,
  resource, W execution-quality and cross-family gates. No sample is added
  after seeing a favorable result, and a failure is not hidden by reducing the
  budget or replacing C1 with EXPLAIN/W.

## Current disposition

The formal report is evidence-valid and regression-compared, but has not
passed M1–M3: Paro C1 is `923.122625 ms` versus DuckDB `136.748000 ms`.
G-Stats/G-Cost remain `registered_not_admitted`; the E8 artifact is explicitly
unaligned and supplies no per-node q-error claim.
