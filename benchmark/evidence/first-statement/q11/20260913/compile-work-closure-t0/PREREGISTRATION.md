# T-CWC baseline and isolated mechanism campaigns

Registered before new Q11 measurements. Existing implementation baseline is
the prerequisite sequence c191f3ee / 12205905 / 3bb92ede; optional scalar
evidence is 086cd835. This sequence is integration, not performance improvement.
The source for every performance run is an unchanged clean detached worktree.
The withdrawn allocation repair and the leftover executor/debug modifications
are excluded. Old dirty-worktree timings are context only.

Normal: original archived Q11 at /private/tmp/paro-q11-query-s0bTho/11.sql,
SF1 data at /Users/linjunhong/workspace/tpcds-sf1/csv, DuckDB tpcds-sf1.duckdb,
immutable Paro seed /private/tmp/paro-necessary-domain-seed-direct-v1. Harness
records hashes; if corpus/seed differs from the preceding report, stop and
explain before comparing. 4 threads, 2GB decimal, binary, generator-declared
metadata, random seed 1, bootstrap 200, one warmup, one ABBA measurement round.
Each normal target runs in a fresh process/session, trace-off, with post-timer
cache-miss and complete typed/order/digest checks. Diagnostic block is separate.

Sequence: baseline handoff 4 blocks then default 2 blocks, each with one
diagnostic block. PARO_COMPILE_WORK_EVIDENCE=1 for scalar observations;
PARO_QUALITY_POLICY_HANDOFF=1 only for handoff. No deadline/budget changes.
Run one additional 2-block handoff campaign with compile evidence disabled to
screen instrumentation overhead; this small screen cannot prove zero overhead.
Keep all valid slow samples. Do not overlap builds/tests/profiling with timing.

T1 feasibility: isolate expected-grant optional searching against the baseline;
first gate is actual admitted fingerprint, not speed. A changed fingerprint
stops that item, preserving negative evidence; no admission comparison tuning.
All other grants retain their exact mandatory frozen candidate, not a relabelled
plan. Only after this gate is supported finish production/cache contracts.

T2: independent sampling of default optimizer work before prescribed allocator
changes. U is normalized residual, not direct synthesis duration. If allocation,
key comparison and duplicate grant constraints are not dominant, stop the
prescribed T2 change per task section 6. Do not count enforcer composition as
duplicate validation. Profile runs never enter normal C1 aggregates.

If both mechanisms are admitted, collect T1-only, T2-only, and combined separate
campaigns (4 handoff / 2 default each). If one is rejected, do not invent a
combined result. T3 records both successful and rejected items explicitly.
No M1/M2/M3 or parity conclusion from these engineering pilot sizes.
