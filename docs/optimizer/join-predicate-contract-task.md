# Join predicate execution contract

EvidenceId: join-predicate-contract-20260925-v1. Baseline source: 3c7d382b3.
This is a staged delivery, not default promotion or exhaustive-search proof.

## Order and constraints

1. Close the inner-region predicate contract. Safe binary comparisons from
   Filter and ON must share connectivity, costing and physical consumption.
   Equalities become all eligible hash keys; non-equalities remain join
   residuals. Preserve outer/reduction boundaries, NULL semantics, projection
   namespaces, OR, volatile/error fences and exactly-once predicate coverage.
2. Validate Q72's small-side date attachment and Q05/Q40/Q49/Q80, rather than
   assuming that all slow outer joins have the same cause. Reuse existing RF
   capabilities; a new adaptive build or range-filter mechanism requires its
   own causal evidence and resource/lifecycle contract.
3. Validate broad results and rank remaining outliers. C1 stays fresh-process;
   same-process policy alternation is only a warm experiment and must account
   for cache carry-over. Do not promote pipeline while writes/semantic/coverage
   gates remain open. Do not hide unsupported writes with implicit fallback.

## Registered exploratory pilot

Control binary is retained at /private/tmp/paro-join-contract.Cayz7A/control-parod,
SHA256 3f16f1c982c7e6887e11ddfe1cd0397113a2698ae0d9f698e010f8b5d7c81c35.
The probe is built sequentially in the existing target directory. No new
worktree, dependency upgrade or baseline update is authorized by this record.

Use maintained tpcds_compare with the existing relocatable SF1 seed
/private/tmp/paro-migration-relative.u1PLBV, DuckDB database and CSV under
/Users/linjunhong/workspace/tpcds-sf1, and DuckDB checkout SQL. DuckDB is pinned
to 1.5.5, native SHA256
85fad85339c7e345eabb66a33a25d8503c3ee8881f9d298fa8499a97617dbf68.
The collector records actual source/build/seed/SQL identities.

First run Q72 pipeline with 3 fresh blocks, 1 warmup and 2 measurement rounds
per block; one independent Detail capture. Then Q04/Q11/Q74 and the four
suspected outer-join outliers with the same envelope. Quality is a separately
identified reference, not an oracle for cardinality or SQL correctness.
4 threads, 2GB, verifier off in timing cohorts, binary protocol, trace off,
PARO_COMPILE_WORK_EVIDENCE=1, timeout 30s, bootstrap 1000. Verify full typed
results, bags and required ordering outside timers. Generator-declared keys
make this non-qualifying for parity unless live inventories match.

Existing VM/database/background load makes these exploratory, not causal
speedup certification. Preserve all valid slow samples and errors; stop
confirmatory collection on result errors. Do not subtract diagnostic times or
cross-cohort medians. Broader 121-query screening, if run, is a separately
registered coverage cohort, not additional independent samples of this pilot.
Bound archived structured evidence to 32MiB; exclude server logs and raw event
floods. Validate receipts/campaigns with maintained validators.
