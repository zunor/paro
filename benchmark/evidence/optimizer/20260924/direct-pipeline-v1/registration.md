# Direct pipeline exploratory registration

EvidenceId: direct-pipeline-v1. Registered before normal timing collection.

Compare the same release binary's `quality` and `pipeline` policies. The
intervention replaces the Memo substrate with a committed relation tree,
bounded aggregate-region choices, maximal join-region DP and bottom-up local
physical selection. Shared execution, cost kernels and correctness checks
are unchanged. This changes the optimization domain; it is not an equivalent
search-work microbenchmark and is not a global-optimality claim.

Q04/Q11/Q74, SF1 immutable relocatable seed
`/private/tmp/paro-migration-relative.u1PLBV`; generator-declared metadata.
The maintained collector records source/dirty-source, build/binary, SQL, seed,
harness, imported DuckDB 1.5.5 native/extension and actual grant identities.
Declared DuckDB native SHA256:
`85fad85339c7e345eabb66a33a25d8503c3ee8881f9d298fa8499a97617dbf68`.

Three independent fresh processes per query/policy, two ABBA warm rounds per
process, one warmup, one separate diagnostic process. Normal samples use
binary PgWire, trace off, `PARO_COMPILE_WORK_EVIDENCE=1`, verifier off, four
workers and 2GB, 60s statement timeout. The optional-search deadline is 30s;
pipeline has no optional agenda but retains the join region's existing work
limits. No pre-touch. Complete typed/multiset/ORDER checks are mandatory.

Run order: Q04 quality/pipeline, Q11 pipeline/quality, Q74 quality/pipeline.
Policy cohorts rotate by query, not individual process. Ambient VM and OS
workload are present. Thus the report is exploratory: retain every sample,
including slow samples and failures, and do not certify parity, a p95 SLO or
a causal speedup. Stop on a correctness/receipt error; inspect any warm
regression exceeding 10% before considering promotion. No outlier deletion,
threshold changes or expected-result updates.

Use only `tpcds_compare.py` and maintained RunOutput/receipt validation.
Archive each cell's compact RunOutput once, at most 2MiB/cell (12MiB total),
plus this registration and findings; no duplicate raw logs or event dumps.
The SQL default is not changed by this pilot. Promotion requires broad
correctness/resource coverage and a separate registered performance gate.

## Amendment A: metadata table-function identity

The first pipeline Q04 cell stopped during metadata inventory, before any
normal target sample: the direct path requested a canonical physical identity
for an ordinary table function, which had no encoder. Preserve the failed cell
and its completed quality control as an interrupted cohort, not a comparison.
Add typed encoding for argument-bound table functions; opaque statement bind
data remains explicitly unsupported. After tests, restart all six cells on
that same new binary with the original resources, counts and run order.
Replacement output names use `a-`; no original output is overwritten.

## Amendment B: declared admission class

Replacement Q04 and Q11 cells passed SQL results, but pipeline receipt
association was Uncovered: compile expected class 0 while the single-class
portfolio lacked its expected-class declaration. Q11 had already started
before Q04's receipt was inspected. Do not certify these compiler samples or
pair diagnostics to them. Preserve all four cells, including Q04's slower
warm timings. Attach explicit single-class coverage (no optional classes) to
the portfolio and test receipt/portfolio agreement. Restart the original six
cells on the fixed binary, with output prefix `b-`; validate each completed
cell before starting the next. Counts, thresholds and other settings remain
unchanged.
