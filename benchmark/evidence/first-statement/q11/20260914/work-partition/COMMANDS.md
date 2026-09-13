# Reproduction

Clean detached worktree `/private/tmp/paro-tcwc-baseline-ZjwFtb`.
Sources: initial off0/on0/off1=459243d1; v2-off0/v2-on0/v2-off1=85bdd979;
probe-off0/probe-on0/probe-off1=4e29cd8f. Each group ran in that serial order.
No simultaneous tests/builds/benchmarks. Each harness command builds and attests
its clean source and binary. Original Q11 and seed hashes are in preregistration.

```sh
ulimit -n 16384
PARO_QUALITY_POLICY_HANDOFF=1 PARO_COMPILE_WORK_EVIDENCE=1 PARO_COLD_WORK_EVIDENCE=0 \
  /Users/linjunhong/workspace/paro/benchmark/.venv/bin/python benchmark/corpora/tpcds_compare.py \
  --server-data-dir /private/tmp/paro-necessary-domain-seed-direct-v1 \
  --listen 127.0.0.1:16432 \
  --duckdb-database /Users/linjunhong/workspace/tpcds-sf1/tpcds-sf1.duckdb \
  --dataset-source-dir /Users/linjunhong/workspace/tpcds-sf1/csv \
  --query-dir /private/tmp/paro-tcwc-baseline-ZjwFtb/benchmark/evidence/first-statement/q11/20260911/incremental-pricing-v1 \
  --report /private/tmp/paro-partition-ARM.json --start 11 --end 11 \
  --process-blocks 2 --diagnostic-process-blocks 1 --warmups-per-process 1 \
  --measurement-rounds-per-process 1 --bootstrap-samples 200 --random-seed 1 \
  --threads 4 --memory-limit 2GB --metadata-track generator-declared --paro-result-format binary
```

Replace ARM by the listed arm name. ON additionally sets
`PARO_DIAGNOSTIC_WORK_PARTITION=/private/tmp/paro-partition-ARM.ledger.jsonl`.
OFF leaves it unset. P1 snapshot/width/phase-timer options unset in every arm.
The new ledger includes an oracle process as well as measured and heavy trace
processes; analysis selects process IDs and exact same-interval scalars, not
the first/fastest ledger record.

Tests on clean4e29cd8f:
`cargo test -p paro-optimizer FILTER --lib`, with filters
`cascades::engine::tests`, `quality_domain`, `work_partition::tests`,
`dominance_preserves_equal_work_latency_span_tradeoffs`,
`incremental_frontier_matches_an_independent_exhaustive_pareto_oracle`.
Compile check: `cargo check -p paro-compiler` before first commit/performance run;
all three measured sources subsequently pass clean release compilation.
