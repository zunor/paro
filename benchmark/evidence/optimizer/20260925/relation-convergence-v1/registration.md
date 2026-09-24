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
