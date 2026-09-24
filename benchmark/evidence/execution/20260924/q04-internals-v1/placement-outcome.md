# Placement pilot: validity boundary and follow-up registration

The first T/S cohorts completed with all six output rows, types, multiplicities
and ORDER checked. Their actual admission fingerprints and class are identical:
`[17485795560441276917,277312838062542375]`, class 2. Therefore the remaining S/T
cohorts are **NotCollected**: repeating them cannot isolate aggregate placement.
The two completed cohorts remain in `matrix/` without exclusions or replacement.

The separately registered quality-policy reference selected
`[5388449204632279338,1773016664838677076]`, also class 2, DOP 4, 2 GB. It has three
narrow partial aggregates plus three final merges. The budgeted alternative has
three wide aggregates, but **also** different join placement, four rather than
three RFs, repeated identical date filters and extra CTE filters. This is not a
pure one-factor stage comparison. Cold/warm clocks from the diagnostic profiles
are not used as normal timing, and physical fingerprints only locate candidates.

The two-stage candidate reduced fact input from 1,957,584 to 153,367 rows before
the customer joins; its final aggregates consume/emit 153,361 rows. The single
stage candidate's wide aggregates consume 1,930,237 rows. This establishes a
real narrow-key reduction, not its isolated end-to-end benefit, and does not
prove uniqueness or authorize removal of a final merge.

The budgeted plan repeats the same year predicate on date_dim: its estimates
step down from 721 to 541, 406, 305, ... despite the predicate already being
pushed into the scan. Both budgeted cohorts reach their registered search
deadline. Further candidate-quality work must repair guaranteed-domain
idempotence and costing of already-enforced predicates, not interpret either
operator counts or more exploration as a performance proof. No such optimizer
policy or budget change is included in this execution patch.

## Supplementary diagnostic failure

`single-profile.jsonl` is an incomplete attempt: cold/warm ANALYZE completed,
then SELECT lost its server connection. The first driver wrapped four potential
30-second compilations in a 90-second process watchdog and omitted the watchdog
failure field. Exact kill cause is Uncovered, not retroactively filled in.
`single-profile-complete.jsonl` uses a 180-second total watchdog (unchanged
60-second statement timeout and 3 GiB RSS cap), retains the watchdog result and
passes full result validation. Its peak RSS is 2,754,150,400 bytes. The incomplete
attempt is retained and never counted as a successful sample.

The profile archives omit only `profile_events`; their omission counts are
explicit. Operators, actual row counts, summaries, source identity, receipts,
full plan text and result validation remain. Original event-bearing SHA-256s:

- quality: `1e51c192b581d9272e5a741ca787c798c6fafd20e5c601b5adc350078f923a35`
- incomplete single: `4b437487acc989c1bd8872f1ca35995fd902654a82af28499d7ed56b32df6698`
- completed single: `9d27979251f4b54f2f88148cd9f6a2a2ee6c1e186b341510779283dbe8776bd9`

## Final implementation reference (registered before collection)

The placement pilot above precedes the additional flat primitive-SUM update
kernel. That kernel resolves the existing state cursor once per batch, preserving
each group's exact floating addition order, NULL handling and integer overflow
behavior; it does not alter candidate selection or aggregation placement.

After committing it, collect one final **quality-policy** reference cohort: two
fresh process blocks, three measured warm rounds, one warmup, one separate
Detail block; seed `20260928`; all other identities/settings in registration.md
unchanged. This is a final correctness/performance observation, not a causal A/B
speed claim against the earlier binary. Retain it even if slower. The same
ambient-load and sample-size limitations apply: no parity/gate certification.
