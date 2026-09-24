# Relation proof / ordinary-region pilot

EvidenceId: relation-convergence-v1. Registered before collection.

Intervention: semantic finite-domain/key transfer through CTE and disjoint UNION,
residual Filter estimate ownership, and shared physical-response DP for bounded
ordinary inner joins. This is a bundled intervention, not an isolated causal
test of uniqueness. No inferred customer_id functional dependency is admitted.

Same release binary: Q04/Q11/Q74 pipeline joint versus quality; Q04 additionally
pipeline single_stage. Three fresh process blocks, one warmup and two ABBA warm
rounds per block; one independent Detail block. Order: Q04 joint/quality/single,
Q11 quality/joint, Q74 joint/quality. Register four threads, 2GB, binary results,
verifier off for latency, 60s timeout, seed 20260925. Normal observer is
PARO_COMPILE_WORK_EVIDENCE=1, never Detail or statement tracing. Preserve every
sample and complete typed result/bag/order validation. A result failure stops
that cell for diagnosis, not retry-until-green.

Reuse /private/tmp/paro-migration-relative.u1PLBV immutable seed and workspace
tpcds-sf1 CSV/DuckDB files. DuckDB 1.5.5 native SHA256 must be
85fad85339c7e345eabb66a33a25d8503c3ee8881f9d298fa8499a97617dbf68.
Per-cell manifests record exact source, binary, inputs and effective settings.
Generator-declared metadata and observed background VM load make this a pilot,
not parity or non-inferiority certification. A >10% warm regression triggers
plan inspection. Historical samples are not matched controls for this change.

Collect under /private/tmp/paro-relation-convergence.BeCX81 with the maintained
TPC-DS collector. Retain bounded standard packages <=2MiB/cell, <=16MiB total;
no archived server logs or data snapshots. Separate known-cardinality fixtures
run with optimizer_verify on under both policies; their selector gaps remain
Uncovered. They do not certify every Q04 physical node's q-error.

Breadth is a separate correctness screen, not this timing gate. TPC-DS99 and
TPC-H22 require every query's declared numeric/order contract; known errors and
unsupported capabilities block default promotion. Do not extend this pilot's
statistics or relax its contracts to claim breadth, C1 parity or ProofComplete.

## Amendment: finite-domain selectivity cohort

The first seven cells at 1932674ba remain retained, including negative Q04
results. New independent fixtures exposed a shared model gap: a disjoint-tag
CTE join estimated zero for four rows, and overlapping branches estimated one
for sixteen. The second source additionally consumes semantic finite domains
for Filter estimation: tautologies/contradictions are proven separately from
a uniform point prior; an allowed domain is not a frequency histogram or a
tighter row-count upper bound. This is a new intervention, not replacement
samples for the initial cohort.

Repeat the same seven cells, ordering, sampling and resource contract under
the `post-domain-selectivity` directory after clean build and correctness
checks. Total retained capacity becomes 32MiB for both cohorts plus 2MiB for
known-cardinality fixtures. Cross-cohort deltas are exploratory, not matched
causal speedups. The independent 4/16-row expectations remain unchanged.

## Breadth screen registration

After both timing cohorts finish, run the maintained collector over TPC-DS
01–99 for pipeline/joint, then quality/joint, using the same binary, SF1 seed,
DuckDB and resource envelope. This is an exploratory correctness/coverage
screen, **not** the ordered full CORPORA gate or a performance certification.
Enable optimizer_verify; use two fresh blocks, one warmup, one ABBA round,
one separate Detail block, 100 bootstrap draws and a 20s statement timeout.
Keep independent query failures and continue to other registered queries;
do not retry failed queries until green. Timeout, unsupported SQL and result
differences are separate failures, not exclusions. A numeric difference does
not inherit a historical Q39 certificate for a different build or plan.

Retain bounded collector packages under `breadth/`, at most 64MiB total.
Do not compare these verifier-on, low-sample timings with the latency pilot.
TPC-H remains a separate gate: its SF1 `.tbl` input is not installed in the
selected environment; no smaller-scale data may use the SF1 expected results.

## Connected-region correction and second breadth attempt

The first breadth attempt was deliberately interrupted after Q18's timeout
was reproduced as an avoidable Cartesian input hidden inside a connected join
region. Its original partial campaign and completed/failed cells stay intact;
there is no fabricated campaign completion or quality-arm result. The next
source opens unconstrained Cartesian nodes during region extraction, preserving
control/evaluation fences and the explicit disconnected-region fallback.

Rerun the seven-cell latency pilot and the registered 99-query breadth screen
under `connected-regions/`, with unchanged settings and a separate 64MiB breadth
allowance. The older cohorts are not replaced or pooled. Run Q18 first as a
correctness diagnostic before spending the full breadth screen.

The local TPC-H 3.0.0 dbgen has now generated SF1 data in the owned directory
`/private/tmp/paro-tpch-sf1.LKW8GF`, using its adjacent dists.dss. Record generator,
distribution and input file hashes. Use the checked-in 22-query workload and
its existing full-result/digest oracles under both read policies, verifier on,
4 threads/2GB, one untimed warmup and one collected execution, 60s timeout.
Setup writes stay on quality; only read queries select the registered policy.
This engineering correctness screen supplies no cold/parity claim. Preserve
timeouts, unknown capabilities and exact numeric differences without blessing.
Its normal bounded RunOutput packages have a separate 8MiB allowance.
