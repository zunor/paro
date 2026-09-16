# Q11 response-delta physical progression v1

Date: 2026-09-16

This cohort evaluates the production physical-search change that propagates an
observable `(group, OptimizationGoal)` response to direct consumers only. It
keeps the existing `TaskRegistry`, `ReadSet`, `CandidateId`, recipe cursor,
budget, frontier, cost model, quality policy, and handoff contract. Recursive
child publications are staged until the current interleave drain boundary so a
direct consumer is not missed; completion changes are separate from priced
frontier changes.

## Source and input identity

The clean diagnostic control is a temporary legacy-behavior commit based on the
same source as the probe. The control restores the old transitive ancestor walk
and completion-as-dirty behavior only for the A/B measurement; it is not a
production commit. The probe is the clean response-delta build.

| item | legacy control | response probe |
| --- | --- | --- |
| source commit | `4619304060d454fb5c7ab58bd080f46fbd72ab32` | `d3a1c21583715dca0628fe71a76313bbfd59c236` |
| source dirty | false | false |
| binary SHA-256 | `cd7128625abae596e13901cb44ff1025dc6239fa4f6340d3a520b39ffa20243d` | `da21d5bb2f00d81ee454a5b8d4060f88cc978eac9683680f810821717db4b1ee` |
| report SHA-256 | `6d07ae5e3e932da28706812196930042c090068788071cc0f2d74a6d30e98d5f` | `6c399f24fc3ee279c2e30f11d7e4d411333162289fffc4d5a7608b98fbb93207` |

The two reports contain identical SQL corpus (`a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503`), DuckDB SHA-256
`568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`, Paro
data SHA-256 `d086f41f77cf32e4201f8d0bbadc3514b64a57577964a13ae40db2f2bd577ef5`,
4 threads, 2 GiB, binary protocol, generator-declared metadata, private data
copy per process, and seeded ABBA ordering. Harness file hashes are recorded in
the JSON reports. Normal cohorts were trace-off, allocation-profile-off,
cache-miss; the one diagnostic cohort was separate and trace-enabled.

Reports:

- `legacy-control-v1.json.gz`: clean control with compiler evidence enabled.
- `response-probe-v1.json.gz`: clean response-delta probe.

## Oracle and performance result

Both arms ran five serial fresh-process blocks. Every measured Q11 sample passed
the complete 90-row typed schema/value/multiset/order validation; cache miss was
verified for every Paro cold sample. Neither arm is a ProofComplete result:
both are `QualityPolicySatisfied + SearchIncomplete`.

| arm | Paro C1 p50 (ms) | DuckDB C1 p50 (ms) | C1 ratio, 95% CI | Paro W p50 (ms) | W ratio, 95% CI |
| --- | ---: | ---: | --- | ---: | --- |
| legacy control | 176.295 | 113.536 | 1.574401 [1.530475, 1.636397] | 73.480 | 0.668326 [0.647506, 0.687754] |
| response probe | 171.495 | 118.967 | 1.421277 [1.327233, 1.484286] | 75.277 | 0.680136 [0.650131, 0.720491] |

The probe compiler side channel was 54.683–56.915 ms versus 62.605–66.753 ms
for control. This is directionally faster, but the selected incomplete plan
changed: the control diagnostic winner had 5 aggregates, while the probe had 4.
Therefore the C1/compiler difference is not a same-selected-DAG causal proof.
Warm execution remained faster than DuckDB in both arms, but the probe W ratio
was slightly worse; formal warm non-inferiority and parity were not attempted.

## Response-work counters

The diagnostic statement used the same low-overhead counter set in both arms.
The probe reduced work, but the changes include different incomplete search
progress and must not be described as pure unit-cost savings.

| counter | legacy | probe |
| --- | ---: | ---: |
| child cost synthesis | 2061 | 1757 |
| published winners | 1285 | 1110 |
| physical subproblem requests / reuse | 1326 / 1413 | 713 / 751 |
| physical subproblem evaluations | 648 | 558 |
| ReadSet rebuilds | 1974 | 1271 |
| recipe reprocess | 1202 | 879 |
| first / repeat recipe processing | 725 / 477 | 590 / 289 |
| related goal notifications | 1725 | 641 |
| merged notifications | 9974 | 2024 |
| direct consumer enqueue | 0 (legacy path) | 325 |
| response unchanged | 745 | 279 |
| completion invalidation / notification | 47 / 0 | 25 / 214 |
| completion-only resume | 0 | 0 |
| TaskRegistry requests / reuse / unique eval | 1705 / 707 / 988 | 936 / 169 / 757 |

The counters establish that direct response notification and completion are now
separate mechanisms, and that unrelated goal invalidation remains zero. They do
not establish that every avoided operation would have been on the critical
path. The probe's lower work and compiler values must be re-evaluated after a
same-quality or completed-search control is available.

## Validation and remaining limits

`cargo check -p paro-optimizer` passed. The targeted physical-response tests,
goal-isolation tests, recipe-resume tests, and completion-only no-reprocess test
passed. Two existing planner tests still fail with their pre-existing expected
algorithm mismatch (`NestedLoopJoin` vs `SortRangeJoin`, and `HashJoin` vs
`HashJoinBuildLeft`); they were not changed or blessed. The full SQL regress
line remains separately tracked and was not reclassified by this cohort.

The production commits are `3cd9794b` (exact response staging and completion
separation) and `1716094d` (direct-consumer response propagation tests). The
working tree still contains unrelated user changes; this evidence was produced
from the clean temporary worktrees listed above.

Conclusion: the notification/continuation contract is implemented and tested,
and the pilot shows less response-driven work. The performance result is not
yet a same-plan compiler proof and does not pass the Q11 parity gate. The next
decision should be to preserve a comparable quality/completion prefix while
measuring whether the reduced ReadSet/recipe work remains after the selected
DAG is held constant; do not expand this route or claim parity from this pilot.
