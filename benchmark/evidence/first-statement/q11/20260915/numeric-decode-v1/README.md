# Q11 numeric-page decode attribution v1

Date: 2026-09-15

This is the S0 attribution and bounded negative result for the numeric-page
decode task. The optimizer, selected plan, SQL, data, resource envelope,
search budget, stop policy, and handoff policy were not changed. Diagnostic
cohorts are separate from normal C1 and are not performance gate samples.

## Input identities

The clean control source was `f0e1dbd8e6e6ca8ad8eaf7a4213ab7702c4e24a5`.
The benchmark used the original Q11, seed data
`d086f41f77cf32e4201f8d0bbadc3514b64a57577964a13ae40db2f2bd577ef5`, four
execution threads, 2 GiB, private per-process data copies, cache miss, and
normal trace-off. The harness files and typed 90-row result contract are the
same as the neighbouring Q11 archives; the complete build and report
attestations are retained in the compressed JSON files.

The split-decode reports were intentionally run in a dirty experimental
worktree because the three new buckets were the measurement change itself.
They are therefore evidence for attribution, not clean-source release
benchmarks. The source status and working-tree hashes are recorded in each
report.

## S0 findings

The release ARM64 disassembly of the existing `BitShufflePageDecoder` showed
the generic scalar 8x8 transpose path (`ldrb`/`strb`), with no NEON sequence.
The bounded cold-work ledger then split the page path:

| bucket | cold count | bytes | exclusive worker CPU | wall-time union |
| --- | ---: | ---: | ---: | ---: |
| LZ4 decompress | 1,135 | 31,988,372 | 74.17 ms | 45.25 ms |
| bit-unshuffle | 1,114 | 157,269,200 | 66.11 ms | 43.15 ms |
| materialize | 1,114 | 157,269,200 | 0.155 ms | not materialized as a separate critical interval |

The second cold block was consistent: decompress 75.92 ms CPU / 48.21 ms
wall union, unshuffle 67.88 ms CPU / 45.86 ms wall union, and materialize
0.166 ms CPU. Worker CPU is cumulative across four workers; the wall union is
the safe critical-path comparison. The old ledger also recorded 987 buffer
fills and 161,929,711 bytes in the same cold statement.

Quality assessment is not the Q11 bottleneck in this cohort: the existing
work-partition control measured roughly 11 ms for evidence, domain,
production, freeze, and read work combined, while the normal compiler was
about 62.7–64.4 ms. The normal/diagnostic boundary is kept explicit; no
cross-cohort subtraction is used.

## Bounded implementation probe

An isolated fixed-width direct-store implementation was tested against the
existing scalar oracle. The focused bitshuffle suite passed 18 tests. A
release micro-probe changed 43.655 ms to 42.616 ms for the fixed-width case
(about 2.4%). In same-seed Q11 pilots, unshuffle worker CPU decreased from
about 65.9 ms to 52.8 ms and its wall union from about 44.3 ms to 37.1 ms,
but the execution wall stayed about 109.7 ms versus 110.0 ms and C1 stayed
about 173.6 ms versus 174.0 ms. The arms were separate short pilots and are
not a formal paired campaign; the result does not support retaining that
more complex production implementation.

The conclusion is therefore bounded and negative for this candidate: the
numeric decode/unshuffle work is real and large, but this particular typed
store/block specialization did not produce an attributable Q11 C1 or warm
gain. `materialize_into` is not a useful target. The next implementation must
address the measured decompress/unshuffle kernel or its representation with a
new causal experiment; it must not revive forced sparse gather or trade away
warm decoded-page reuse.

All normal pilot rows passed the complete typed schema, value, multiset, and
order checks. The normal reports contain two fresh blocks only, so they are
directional and do not certify M1, warm non-inferiority, or parity. The
`s0-ledger-off` perturbation is retained as a noisy diagnostic control and is
not used for a timing claim.

## Files and status

- `s0-current.json.gz`: clean f0 baseline/control;
- `s0-partition-2.json.gz` and `work-partition-2.jsonl.gz`: existing bounded
  compiler/quality partition;
- `s0-decode-patch.json.gz`: split page buckets;
- `s0-unshuffle-direct.json.gz`: isolated fixed-width direct-store probe;
- `s0-generic-final.json.gz`: same-seed generic comparison;
- `SHA256SUMS`: report and ledger checksums.

Task 1 is stopped at this evidence-backed negative result. Only the bounded
diagnostic bucket additions are retained in the source; no decode strategy,
page format, cache policy, or execution plan was changed.
