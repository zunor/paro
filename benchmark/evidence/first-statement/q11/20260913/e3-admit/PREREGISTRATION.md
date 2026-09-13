# E3-ADMIT diagnostic admission experiment

Registered before sampling, 2026-09-13. Default admission, sparse-gather policy,
optimizer, cost model, budgets and handoff default remain unchanged. Opt-in
`PARO_DIAGNOSTIC_STREAM_SEQUENTIAL=1` disables creation of full materializations
for Sequential access, including uncached fallback. It does not disable consuming
existing decoded entries or sparse-gather promotion. This is a causal probe, not
yet a second-touch production policy or a claim of zero decoded fills.

Original Q11/seed, 4 workers/2GB, binary typed90/order validation, fresh process,
first target occurrence/cache miss. Existing handoff and compile scalars enabled.
All source/harness/binary hashes recorded from clean committed source. Serial
normal trace-off/E1-off sequence: control2 → stream2 → stream2 → control2.
Each report warmup1/ABBA round1, bootstrap1000/seed1, independent trace diagnostic1.
Keep all valid slow samples, no optional stopping. Report paired adjacent block
C1 ratios and confidence intervals, W, compiler, plan fingerprint/completion.

Then separate existing E1 scalar cohorts control2 → stream2 (not normal timing)
measure fill bytes and same-occurrence execution wall. E1 overhead has not been
certified ≤2ms; its execution-wall gate is provisional, not silently pooled into
normal results. Direction gate: normal stream median C1≤175ms and scalar execution
wall≤105ms, with W explicitly reported. Passing does not prove parity/default
rollout or formal noninferiority. Failure stops blind second-touch implementation.

Independent pre-touch axis: same SQL from E2, execute twice and drain/validate
both, two fresh blocks control → two stream, no pooling with C1 cohorts. Record
both pre-touch timings for both engines and preparation total; second execution
includes cache/plan reuse and is not an isolated decoder microbenchmark. Target
after preparation remains diagnostic only, with unchanged strict miss gate.

Bounded rejection counters are diagnostic-only, by guard, not unbounded binding
events. They change no rule decision. No executor algorithm/allocation/domain/F/
parallel/B&B/budget changes or production prewarming. Byte-axis implementation
closed negative this round. T1 formal W noninferiority/power/tail campaign, full
SQL regress and four baseline failures remain open; do not bless or claim green.
