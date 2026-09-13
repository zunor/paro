# M-POWER — conditional variance model and fixed T1 W campaign

Registered2026-09-13 before new sampling. Measurement only; no engine/harness
algorithm change. Analysis uses28 archived process blocks in7 declared strata:
E1 clean baseline/off/v2off, T1, E2 normal, E2 touch (W only, including135.035ms),
and E3 isolated repeat control. Different builds/preparation strata are not
silently pooled as identical means; scalar-on, stream and compromised E3 campaign
are excluded from estimation, not erased. E2 touch is a conservative tail stress
stratum, not normal C1. `estimate.py` reports P/D log variances and covariance.

Estimand is geometric mean of paired process-block W ratios, each block using
its measured ABBA warm median. Assume independent stationary normal log ratios,
alpha.05 one-sided, true ratio.88, target power.90. Use the maximum stratum
one-sided95% normal-theory sigma upper bound, .24516076, not the four tight E3
pairs. Smallest N divisible by4 passing noncentral-t power is36 (power.92254).
This conditional N is reusable only under these assumptions/effect/variance.
It does not establish tail stationarity, an unconditional minimum, or power at
ratio1.00: at an exactly equal boundary, a zero-margin upper≤1 test passes with
probability alpha, irrespective of N. Missing calibration cannot become zero.

T1/control NI is a DIFFERENT estimand, pilot91.800/90.363≈1.016, not.88. Use
the requested fixed36 blocks per version to perform a zero-margin one-sided95%
gate, but do not claim90% power to prove its NI. No positive tolerance invented.
If upper>1, NI is not certified; only lower>1 supports statistically worse.

Versions: clean6fce0fc0 control and clean c23ae52a T1 production; corpus harness
files identical (`git diff 6fce0fc0 c23ae52a -- benchmark/corpora` empty).
Original Q11, same seed/4 workers/2GB/model/budgets, handoff enabled, compile
scalars on, E1 scalar and statement trace off in normal, separate diagnostic1
and oracle each report. Before sampling, independent review corrected odd report
sizes:36 blocks/version in12 six-block reports, fixed chronological sequence
C6 → T6 → T6 → C6 → C6 → T6 → T6 → C6 → C6 → T6 → T6 → C6.
This yields six matched adjacent batches with version order balanced, and even
report sizes also balance the C1 engine-first schedule.
Warmup1/ABBA round1 within every block balances engine execution order; all slow
samples retained. No stopping on significance or extra samples after the result.
Pair corresponding block offsets within adjacent batches; use log(T1 W/control W)
with one-sided t upper and lower, and whole-pair bootstrap sensitivity (10000).
Report a second t interval on the six batch-mean contrasts to expose sensitivity
to temporal clustering; NI is certified only if both upper bounds≤1.00. The
block test assumes residual drift negligible within matched batches; this is
not random allocation of72 independent treatments. N36 power is conditional P/D
power only. The primary block-median statistic is recomputed from raw samples,
not substituted by the harness's differently aggregated ratio field.
Also report P/D W and C1 separately, p95 and all per-block observations. Building
occurs outside sampling, no tests/builds/other benchmarks during measured blocks.

Ladder measurements can use36 independent process pairs per rung as a conditional
baseline, not a claim that every rung has the same variance. Zero-fill/cache-hit
eligibility and typed validation mandatory. Q11 NI after any L2 change must
separately specify its effect/margin; N36 alone cannot magically prove equality.
