# E2-DECOUPLE — pre-touch transfer experiment

Registered2026-09-13 before measurements. No optimizer/executor/model/budget
changes. Same original Q11/seed/4 workers/2GB/metadata/binary result as E1/T1;
existing handoff enabled, compile scalar enabled, E1 scalars off for timed arms.

One custom SELECT computes exact checksums over every Q11-referenced column in
store_sales/web_sales/customer/date_dim. It runs and drains in each fresh Paro
and DuckDB process before the target, with distinct fingerprints and separately
reported execute/fetch and total preparation cost. Full pre-touch results must
match across engines. Target first fingerprint occurrence and cache miss must
remain verified, full Q11 typed90/order validation unchanged. Pre-touched target
timings are diagnostic and explicitly ineligible for C1/parity gates.

Normal/control and pre-touched arms run serially on the same clean committed
harness and binary: control2 → pre-touch2 → pre-touch2 → control2 fresh blocks.
No optional stopping/removal of valid slow samples. Per report warmup1 and ABBA
round1, bootstrap1000, seed1, separate mandatory trace diagnostic1. After this,
one control2 and one pre-touch2 scalar cohort use existing E1 observations to
check target fills/decoder work and same-image execution deltas; not pooled with
normal timing. No new instrumentation is required.

Recompute each block C1−compiler−W, compare changes and paired block target
timings, and report total preparation+target cost. User's budget arithmetic is
useful but a difference of medians is not an occurrence interval or a hard
physical lower bound. Do not certify any 'compiler zero' extrapolation.

Large target improvement with collapsed fills supports transferable resident
state. No improvement is evidence against this pre-touch intervention, not proof
that pages can never help unless coverage is verified. Pre-touch also warms
metadata, workers/allocator and OS caches; it alone does not uniquely identify
decoded pages or validate scan-through's net performance. A partial effect is a
valid result and will not be forced into an all-or-nothing causal conclusion.

The side investigation is read-only: late-payload rejection reason and decimal
width/encoding. Preserve T1's open formal W noninferiority, full SQL regress and
baseline4 failures. No bless, handoff-default switch, executor or allocation fix.
