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

## Amendment v2: reconstruction regression (before replacement samples)

The first probe passed all eight pipeline cells, but quality Q72 exhausted its
2GB quota. A fresh untimed SQL check reproduced that failure in the probe and
returned 100 rows in the retained control binary. Keep the failed cell; it is
not an environment exclusion or a successful performance comparison.

Native Memo join reconstruction attaches unconsumed relation-local predicates
at the region root even though the shared graph prices them at leaves. Close
this contract before further performance collection: attach filters at first
complete support, consume each once, and derive staging facts from actual
operators rather than marking a DP point estimate as an exact row bound.
Rerun Q72 quality before completing the same eight-query policy comparison.
Replacement cells use new run IDs and the original sampling envelope; they
are not retries accepted in place of the failed initial arm.

## Coverage cohort and corpus triage

After the reconstruction fix, run the maintained TPC-DS collector for Q01–Q99
under pipeline and quality separately: 2 fresh process blocks, 1 warmup,
1 measurement round, 1 diagnostic block, timeout 30s, bootstrap 1000, otherwise
the same envelope. This is exploratory coverage/triage, not confirmation:
retain each failure and continue to other queries to identify blockers.
Do not compare failed cells or pool them as zero-time observations. The shared
host and sequential policy campaigns prevent a causal cross-policy speedup claim.
The same binary and DuckDB oracle validate both arms. Archive cap is 64MiB
across this coverage cohort, replacing the smaller pilot-only 32MiB allowance.

The collector's bounded corpus-impact summary ranks absolute warm excess over
DuckDB, reports the top-five share among measured queries, and exposes missing
coverage. It points to original cells; it does not replace samples or infer
operator causes. A ratio above three recommends a separate execution diagnostic,
not an automatic performance failure. TPC-H's maintained 22-query workload is
a separate result/coverage check against its audited oracle, not a fresh C1 or
same-process cross-policy comparison.
