# P1 frontier sensitivity (diagnostic, not production admission)

Control is e3d15422. Probe adds only an opt-in bounded final-frontier snapshot
and an opt-in frontier-width override; default width remains 256. All other
budgets, rules, model, expected grant and quality handoff remain unchanged.
Capture retains immutable candidate handles after search. Serialization occurs
after the optimizer timer, but remains inside diagnostic compiler/C1. Never
interpret its compiler/C1 as a normal performance result. No timing is removed
from normal statements. Snapshot cap 16384; any omission invalidates complete
counterfactual counts. Replacement/invalidation counts are not inferred from
archive minus live size and are explicitly unavailable in this initial slice.

Use clean committed sources and the existing tpcds_compare harness, original
Q11, seed 72ccb3dc02ce127f61c06b85920a0e75c17a55a60ee341d267251c67611f62b1,
4 workers/2GB, binary protocol, handoff1, compile evidence1, cold evidence0.
Two fresh blocks/arm and one separate statement-trace diagnostic; warmup1,
measurement round1, random seed1, metadata generator-declared (inherited Paro
declared keys versus DuckDB empty keys disclosed, not silently changed).
Every arm retains typed/order90, cache-miss, source/binary/SQL/data/harness hashes.
Serial runs only, no concurrent builds/tests. This is a pilot, not W NI/parity.

First compare snapshot off/on/off at width256, same binary. If capture raises
optimizer time over10%, reduce capture before interpreting snapshots. Heavy
statement trace is separate from trace-off optimizer scalars. Snapshot on
statements are diagnostic even when statement trace is off.

Then widths1/2/4/8/unbounded, one two-block report each, snapshot enabled. Changes to
admitted fingerprint, quality and completion are expected diagnostic outcomes,
not optimizations. Width infinity means no frontier cap (u32 maximum); all other
safety budgets and external timeout300s remain. Keep all valid slow samples.
Report N, optimizer, rules, current frontier sizes, archive count and fingerprints.

Counterfactual analysis must preserve exact source-response equivalence,
work/span, risk and task-supply gates. A slack local memory coordinate is NOT a
proof about all parent compositions or actual admission availability. Report an
optimistic sensitivity projection separately from any proved envelope quotient;
do not deploy it or invent a safe quantization threshold. Where the current
envelope contract supplies no compositional equivalence proof, mark unproven.
Counts from published history are not simultaneous frontier sizes.

Width changes also change plans, recipes and propagation, so five single runs
cannot by themselves uniquely identify kernel and per-entry CPU coefficients.
Choose P3 only if evidence resolves that ambiguity; otherwise report the missing
attribution rather than fitting a causal decomposition to correlated counts.

Before any measured query: the harness rejected process-blocks=1 at argument
validation. Its minimum is two; amended to two per arm, not a lowered validation
gate. The rejected command log is retained; it contains no observations.

## P2 independent slice, before P2 measured queries

Probe 2deb6450 shares the immutable deferral root eligibility guard with native
and owned rewriting, and rejects an ineligible shell before scoped matching.
It does not reject a root because its current child is not a join; input
frontier subscriptions remain. Seven targeted tests passed on the mixed tree;
repeat on the committed clean tree before measuring. Existing failures remain.
Run probe two fresh blocks, then control a476731b two fresh blocks, snapshot OFF,
same handoff/budgets/seed and normal trace-off with separate diagnostic. Compare
per-rule published counts, exact final choices/fingerprints, N and final admitted
fingerprint first. A timing difference in this small sequential pilot cannot
certify W NI. If Q11 rejection counts are unchanged, report no demonstrated
benefit on the requested path, not sixty avoided bindings.

## P1 additional bounded attribution, before its measured queries

Width1/2 changed logical work and admitted plans; width4/8 changed combination
history even when the final fingerprint matched. These points cannot uniquely
identify fixed kernel versus per-frontier cost. Add two opt-in cumulative
monotonic wall timers, no per-tuple events: (1) child summary reads, local fit,
task supply, source composition and both grant constraints; (2) cached candidate
admission including preview, witness construction and Memo insertion. Timers are
disjoint, include rejected/error/continue paths, and exclude subsequent parent
wakeups. Their sum is NOT total physical search. Remaining time is not assigned
to hypothetical cache misses or scheduling without evidence.

Run same binary, snapshot OFF, timing OFF/ON/OFF, two fresh blocks/report plus
one separate diagnostic. Compare exact N/choices/fingerprints and optimizer
overhead; >10% repeatable perturbation invalidates attribution. No production
policy/budget/model changes. If neither measured bucket dominates, do not start
P3-A/B simply because one was proposed. Report residual as unattributed.

## SQL identity correction before acceptance

The initial off0/on0/off1/w1/w2/w4/w8/winf and p2probe commands used the upstream
DuckDB 3008-byte SQL (aliases and `2001+1` spelling), not the exact historical
2418-byte archive. Results and complete-width fingerprints matched, but corpus
hash differed. Retain all these reports as exploratory, NOT acceptance data.
Repeat with `20260911/incremental-pricing-v1/11.sql`, already committed in this
repository; require historical corpus hash
`a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503`.
Corrected arm names have suffix `-exact`. Same sample counts, envelope, timing
boundaries and all other gates. This corrects the input identity, not results
or stopping rules. No valid slow sample is removed.
