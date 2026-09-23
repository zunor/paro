# Cardinality ownership: recovery validation registration

EvidenceId: `optimizer-migration-20260923-cardinality-ownership-v1`.
Registered after the exploratory two-block probes, before the final-source
collection below. Earlier probes are not retrospectively confirmatory.

## Intervention and fixed contracts

Equivalent logical publication must not count another tree's estimate as a
fresh observation. Transformed roots inherit the target relation's estimate;
new relations derive their own. Duplicate fact publication is a no-op. New
logical constraints, explicit statistics refreshes, real group merges and
their invalidation/rollback contracts remain active. No budget, timeout,
quality certificate, mandatory/optional coverage or grant variant is removed.

The candidate implementation is the task diff from `26061d40`, committed
before collection. The maintained collector records the exact clean source,
binary/build, configuration and receipt identities for every cell. The
receipt-validator correction is a separate change: QualityPolicySatisfied
may coexist with a budget obligation; it is still incomplete search.

## Collection

- Serial builds and experiments, one shared Cargo target, no other target
  workload. Maintained `benchmark/corpora/tpcds_compare.py`; no new timer.
- Immutable relocatable SF1 seed
  `/private/tmp/paro-migration-relative.u1PLBV`, SHA-256
  `256bf479ca982e30c96f9109fb9920d04233c9643b7bcc6a5fd3f5a50554c927`.
- DuckDB declaration `benchmark/requirements.txt`: 1.5.5. Runtime extension
  SHA-256 `85fad85339c7e345eabb66a33a25d8503c3ee8881f9d298fa8499a97617dbf68`.
  A changed runtime or data identity stops collection, not a new silent arm.
- Four execution threads, 2 GB, binary protocol, generator-declared metadata,
  default `quality` search policy. No diagnostic search-deadline override.
  The existing 30-second optional-search budget is unchanged.
- Q04 and Q74: two independent fresh blocks each, verifier on, plus one
  separate bounded Detail capture per cell. These are held-out pilots, not
  powered non-inferiority tests. Run Q04, Q74, then Q11.
- Q11: five independent fresh blocks with verifier on, then five with it off
  (the historical timing configuration). Keep the two cohorts separate;
  neither is a same-batch causal estimate of verifier overhead. One separate
  bounded Detail capture each. One warmup and one measurement round per block;
  keep the harness's seeded intra-block Paro/DuckDB rotation.
- Primary compile metric: the normal cache-miss statement receipt's unchanged
  `compiler_elapsed_us`, not EXPLAIN wall time or optimizer-only time. Report
  every value, median and P90. Target is approximately 12 ms; a median above
  12 ms is reported numerically, not rounded into a strict <=12 ms pass.
- Report C1, warm, selected plan/grant, search counters and stopping state.
  No parity certification: metadata track, sample size and between-cell
  sequence are not a preregistered paired-parity gate.

## Validation and stop conditions

Every SQL sample must pass complete typed result, multiplicity and required
order validation, with a verified cold miss and valid receipt. Preserve every
slow sample, failure and missing receipt. Any result error stops confirmatory
collection for investigation; never retry until green or bless expected output.
If a held-out query shows a large regression, inspect exact selected choices
and facts before admitting the change. Historical timing is a bridge, not a
cross-version plan-equivalence or ProofComplete claim.

Run the full workspace tests, strict Clippy, benchmark tests and high-FD SQL
regress. Compare regress with the existing 18-failure control without updating
expected files. Archive compact RunOutput cells/receipts, one capture per cell,
inputs/manifest and a conclusion. Raw logs and historical event floods remain
outside Git; no worktree, recovery ref or user dataset is deleted.

## Host-interference amendment (before the additional cell)

The original collection is retained unchanged. A separate MatrixOne Go build
started at 03:29:36 UTC, overlapping the final six seconds of the Q11
verifier-off run (03:29:22–03:29:42). Subsequent Go compilation/static analysis
also used substantial CPU. Its processes are outside this task and are not
stopped by this task. This invalidates an isolation claim for that cohort;
it does not invalidate SQL results or authorize removing individual samples.

After owned regress/tests finish and the known external Go build/lint has
ended, collect **one** additional five-block Q11 verifier-off cell, with one
separate diagnostic capture. Keep the same code, binary, seed, settings and
timer boundaries. Check for active build/lint processes before and after
collection; if they recur, label the additional cell NotCertified as well,
not another retry-until-green. No other target or criterion changes, and no
pooling with the original cells. This amendment cannot make the original
campaign an isolated or powered parity comparison.
