# Q11 latency recovery: candidate co-occurrence boundary

Status: incomplete. This investigation does not restore the historical 12 ms
latency and does not certify parity or search completeness.

## Same-seed historical bridge

Historical `d3097038` on the current relocatable SF1 seed still compiled in
12.499 / 12.046 ms, with 384 cost compositions. Complete Q11 result checks
passed. Therefore the old seed is not necessary for the old fast result.
The historical collector predates typed COMPILE receipts; this is an
exploratory bridge, not cross-version identity certification.

Only the historical collector's snapshot cwd/data-dir and CARGO_TARGET_DIR
binary resolution were patched; historical Rust was unchanged. The temporary
patch was restored after collection. Both source switches encountered stale
parser artifacts in the shared release target; scoped parser/common release
cleaning and rebuild resolved them. Failed builds are not query samples.

## Current-source observations

Base `f71cb12c`, with bounded fact-observation counters only. Two normal fresh
compiler samples: 75.876 / 71.271 ms; 1,467 compositions each. Full typed
results, multiplicities and ordering passed. A separate diagnostic capture
observed the following first times (optimizer clock, not normal compile time):

| Same-candidate evidence | First observation |
| --- | ---: |
| Predicate domain | 8.017 ms |
| Join region | 9.634 ms |
| Join region AND aggregate decomposition | 54.207 ms |
| Join region AND aggregate AND predicate domain | 68.044 ms |

The gap is not absence of every individual optimization. The next causal
investigation must trace exact child choices and affected ancestor consumption
which combines these decisions. Independent candidates' facts must never be
united into a fabricated certificate. Existing bounded fact-mask aggregation
now retains first observation times without another clock read or event log;
the test rejects cross-candidate conjunction of facts.

Mandatory-to-optional coverage repair `513e4da1` restores previously omitted
legal physical alternatives. It is a concrete semantic difference from the
historical build, not yet a quantified explanation of the Q11 gap. Do not
disable it or treat mandatory completion as optional completion to reproduce
12 ms. Keep safe incumbent reuse distinct from completion proofs.

## Rejected intervention

Allowing supported conjuncts in mixed native-domain bindings and routing
shallow Join/Filter through native closure passed targeted tests but increased
compositions from 1,467 to 2,572 and compile to 147.326 / 140.008 ms. The whole
behavior intervention was reverted. It is not part of this source change.

## Evidence and validation

The adjacent JSON projections preserve source/build/configuration identity,
normal receipt data and bounded diagnostic counters. Full exploratory runs
remain under `/private/tmp/paro-chain-latency-recovery-20260923` as
`fact-counts-run`, `mixed-native-run`, `fact-timing-v2-run`, and
`historical-new-seed-v2.json`. Historical raw traces are not copied into Git.
Do not pool these interventions or infer a confidence claim from two blocks.

Current engine tests: 124 passed; release build and Q11 collection passed.
No full-workspace/SQL regression claim is made for this counter-only change.
The historical worktree is restored; no data or other worktrees were deleted.
