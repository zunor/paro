# Q11 on-demand quality preflight v1

Date: 2026-09-16

This archive records the bounded experiment for changing quality handoff from
freezing every candidate before checking it to an exact selected-reference
preflight.  The handoff policy remained opt-in; this experiment did not change
the default production stop policy, search budget, candidate choice, cost
model, resource envelope, or execution plan policy.  It is a pilot, not a
formal M1/M2/parity campaign.

## Identity and measurement contract

The binary was built from dirty worktree commit `530fda788d676caf03e70d740a267d700a1c63b2`,
with binary SHA-256
`ba45d3b84e3f5a6ca234dbc274aa01adaca33e3ead8f9368b540d825c2eede38`.
Because the source was dirty and contained unrelated staged/unstaged history,
these measurements are directional only.  The exact source status and the
complete build attestation are retained in both raw reports.

Both arms used the original Q11 (`11.sql`, SHA-256
`1f3c2697b2a82f597f9e8a570413be65ab7a02da03c1fe4c3ea7b725735433b8`), SF1,
the same private Paro data copy (SHA-256
`d086f41f77cf32e4201f8d0bbadc3514b64a57577964a13ae40db2f2bd577ef5`),
generator-declared metadata, four service/execution threads, planning DOP 1,
2 GiB, binary results, and the same existing handoff policy.  The DuckDB
database SHA-256 is
`568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`.
The harness files and hashes are recorded in the JSON reports.  Normal arms
used five serial fresh-process blocks, cache-miss verification, trace-off,
one warmup and one measured round.  The diagnostic cohort was a separate
trace-on process and is excluded from C1/W.

The control used `PARO_QUALITY_PREFLIGHT=0`; the probe used
`PARO_QUALITY_PREFLIGHT=1`.  Both used `PARO_QUALITY_POLICY_HANDOFF=1` and
`PARO_COMPILE_WORK_EVIDENCE=1`.  Fine-grained work partitioning, allocation
profiling and statement trace were unset for normal measurements.

## Normal pilot

All ten Paro/DuckDB cold samples passed the complete typed 90-row schema,
value, multiset and order checks.  Cache-miss and trace-off checks passed for
every normal sample.  The complete sample lists are retained in the raw
reports; no slow sample was discarded.

| arm | Paro compiler median (ms) | Paro optimizer median (ms) | Paro C1 samples (ms) | DuckDB C1 samples (ms) | C1 ratio, 95% CI | Paro warm median (ms) | DuckDB warm median (ms) | warm ratio, 95% CI |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: | --- |
| preflight off | 57.843 | 57.122 | 242.563, 220.240, 194.001, 199.338, 205.292 | 136.342, 138.078, 138.264, 145.032, 143.303 | 1.510 [1.397, 1.660] | 94.448 | 127.889 | 0.738 [0.680, 0.814] |
| preflight on | 61.961 | 61.183 | 336.565, 229.728, 186.929, 190.696, 219.452 | 142.196, 143.084, 124.594, 132.107, 138.042 | 1.672 [1.489, 1.999] | 98.481 | 138.098 | 0.706 [0.674, 0.740] |

The cold result is not an attributable improvement: the five-block probe
compiler/optimizer medians were about 4.1/4.1 ms slower than control, and its
C1 distribution contains a 336.565 ms valid slow sample.  The warm ratio is
not a new non-inferiority result because the sample is a small pilot and the
DuckDB warm distributions differ between arms.  No formal W, M1, M2 or parity
claim is made.

## Same-occurrence diagnostic work

The diagnostic traces provide the causal work comparison without mixing their
client time into normal C1.  The preflight provider was called for 98 frontier
candidates in both arms and the search work was unchanged: 1,757 cost
syntheses, 353 implementation requests, 558 physical subproblem evaluations,
713 subproblem requests, 751 reuse returns, and 82 producer dispatches.  The
quality policy saw 98 candidates, 97 policy rejections and one certified root
candidate in each arm.

| metric | preflight off | preflight on |
| --- | ---: | ---: |
| quality preflight calls | 98 | 98 |
| preflight policy rejections | 0 | 97 |
| preflight-ready candidates | 0 | 0 |
| freezes avoided | 0 | 97 |
| search/freeze elapsed (us) | 2,770 | 152 |
| quality-policy satisfied (us) | 48,982 | 49,741 |
| physical ReadSet rebuilds | 879 | 879 |

The enabled path therefore avoided 97 rejected-candidate freezes and about
2.618 ms of this diagnostic freeze interval.  It did not reduce search,
physical requests, ReadSet rebuilds, or the selected plan.  The first complete
quality candidate was around 49.154 ms in the enabled diagnostic occurrence;
this is not a normal C1 phase subtraction.

## Contract and result

The production path now preflights an immutable graph of exact selected
`ChildWinnerRef`/logical/physical choices with a `ReadSet`, domain bindings and
native quality evidence.  A candidate whose graph cannot be formed falls back
to the existing full freeze/evidence path.  A candidate rejected by the
quality policy produces the existing precise production obligation without
freezing.  A candidate is accepted only after the original complete evidence
provider receives a real `FrozenCandidate` and the final verifier succeeds.
The candidate cursor is keyed by `(goal, CandidateId)` and remembers the exact
ReadSet identity, so unchanged facts skip a repeated preflight while a fact
version change reopens it.  This preserves MissingEvidence, task generation,
ReadSet invalidation, exact choices and final verification semantics.

Focused quality, production-request, and real engine re-open tests passed, and
the release optimizer check passed.  The full planner suite still has the
known mixed-worktree failures; they were not blessed or changed by this
experiment.

This is a contract/safety improvement with a negative production-performance
result.  The implementation still walks the complete selected reference graph
and constructs its selected-group ReadSet for each first-time candidate; it
has not yet become a demand-scoped local proof.  That remaining full-graph
walk is the only evidence-backed next investigation, if this line is resumed.
Adding another cache or more preflight summaries is not justified by this
pilot.  The final status remains `QualityPolicySatisfied + SearchIncomplete`,
not `ProofComplete`; the handoff policy remains opt-in and the default
production path is unchanged.

Raw artifacts:

- `off-v1.json.gz`
- `on-v1.json.gz`
- `off-diagnostic.parod.log.gz`
- `on-diagnostic.parod.log.gz`
- `11.sql`

The first earlier pilot with an invalid query path is excluded from this
archive.  Two-block and two-block diagnostic runs are retained in their
temporary locations only as prior directional context, not pooled with this
five-block comparison.
