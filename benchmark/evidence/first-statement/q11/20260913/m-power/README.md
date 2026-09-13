# M-POWER: conditional sample size and formal T1 W gate

2026-09-13. Engine code unchanged. Preregistration committed as 771c9ff1 and
7e250349 before sampling; all 12 scheduled reports completed, 36 fresh blocks
per version. Source control6fce0fc0 versus T1c23ae52a, identical corpus harness,
4 workers/2GB, original Q11, handoff policy unchanged. Normal E1-off/trace-off,
first target cache miss, typed ordered90 validation; separate diagnostics/oracles.
All valid slow observations retained in raw reports and hashed manifest.

## Power

Seven archived strata, 28 blocks, including E2's slow pre-touch W observations.
Largest log-SD upper bound .245161 gives **N36** (multiple of4), power .9225 for
true P/D ratio .88, one-sided alpha .05. N32 gives .8924. This is conditional on
independent stationary normal log-block ratios, not a universal minimum or a
tail guarantee. Four samples per historical stratum leave substantial uncertainty.
See `estimate.py`, raw estimate and preregistration for assumptions and covariance.

N36 does not confer90% power for a T1/control zero-margin NI test near1.00.
At the exact equality boundary, the upper≤1 test passes with probability alpha.

## Fixed-sample results

| Metric | Control | T1 production contract |
|---|---:|---:|
| Paro W block-median median |91.543ms|91.728ms|
| W observed block p95 |93.431ms|93.661ms|
| Paro C1 median |262.721ms|199.013ms|
| C1 observed p95 |266.925ms|205.956ms|
| DuckDB C1 median |106.708ms|106.482ms|
| P/D W geometric ratio (one-sided95 upper) |.87871 (.88377)|.88056 (.88457)|
| P/D C1 geometric ratio (one-sided95 upper) |2.45023 (2.46155)|1.87328 (1.88424)|

T1/control W ratio **1.002238**. Block t upper **1.008241**, batch-cluster t
upper **1.011313**, bootstrap sensitivity upper1.007956. Lower bounds .996271
and .993244: **zero-margin NI not certified; degradation also not established**.
No post-hoc tolerance or extra samples. NI requirement remains open, not blessed.

The batch-sensitive result uses six adjacent report-pair means, not 36
independently randomized version assignments. Report-level engine-first order and
version order are balanced; temporal/cross-day dependence cannot be ruled out.
Observed p95 is not a formal tail guarantee. This handoff experiment is not a
default-policy parity campaign, M3, or ProofComplete claim. A median below200ms
alone does not close all M1 gates.

`analyze_campaign.py` reconstructs block medians from raw W samples rather than
substituting the harness's different aggregate estimator. `raw/manifest.json`
binds reports/logs; reports retain binary, source, SQL, seed, harness and resource
identities. `raw/paro-mpower-result.json.gz` includes all72 block observations.
Full SQL regress, cross-family and tail admission are outside this M campaign.
