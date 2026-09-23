# Demand-driven selected properties

EvidenceId: selected-local-properties-v1. See the committed
[registration](../selected-local-properties-registration.md). Engineering
pilot, not a powered performance/parity certification.

## Outcome

Q74's repeated quality work is reduced; normal compiler median falls 8.5%.
Q04 is essentially flat and Q11 does not improve. C1 does not improve
consistently. This is not Q11 <10ms, overall compiler convergence, warm
non-inferiority or DuckDB parity.

Normal cache-miss compiler milliseconds, six independent fresh processes per
query/arm; the collector's nearest-index P90 excludes the maximum at this small
sample size, so maxima and every sample are retained too:

| Query | Control median | Probe median | Change | Control P90 | Probe P90 | Control max | Probe max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Q04 | 19.1425 | 18.9735 | -0.9% | 19.477 | 19.448 | 77.083 | 19.825 |
| Q11 | 12.5250 | 12.9400 | +3.3% | 12.563 | 13.033 | 12.627 | 13.150 |
| Q74 | 38.2575 | 34.9870 | -8.5% | 39.016 | 35.880 | 40.758 | 38.077 |

| Query/arm | Compiler samples, batch/process order (ms) |
| --- | --- |
| Q04 control | 77.083, 19.126, 18.535, 19.000, 19.159, 19.477 |
| Q04 probe | 19.059, 18.836, 18.547, 19.448, 18.888, 19.825 |
| Q11 control | 12.492, 12.558, 12.563, 12.627, 12.341, 12.333 |
| Q11 probe | 12.925, 12.139, 13.033, 12.653, 13.150, 12.955 |
| Q74 control | 39.016, 38.010, 38.201, 40.758, 38.314, 37.900 |
| Q74 probe | 38.077, 32.760, 35.337, 35.880, 32.679, 34.637 |

The valid 77ms Q04 sample is not excluded or replaced. These are descriptive
medians, not causal ratios derived by pooling unrelated reports. The six-block
pilot and time-separated batches do not remove host drift.

| Query | Paro C1 control → probe | DuckDB C1 control → probe | Paro warm control → probe |
| --- | ---: | ---: | ---: |
| Q04 | 262.755 → 254.865 | 131.894 → 124.920 | 164.309 → 165.699 |
| Q11 | 147.510 → 156.282 | 59.635 → 67.994 | 81.480 → 82.113 |
| Q74 | 133.076 → 140.260 | 75.629 → 81.725 | 56.687 → 56.450 |

Warm has twelve measured calls nested in six processes per query/arm, not twelve
independent process samples. No compiler/warm median crosses the registered
10% investigation threshold. That is not a non-inferiority certification.
Q11/Q74 C1 and DuckDB both move unfavorably between arms; this does not establish
their cause and does not erase the C1 negative result.

## Implementation and architectural scope

- `b0082799`: selected-DAG consumers borrow one index instead of rebuilding
  maps. Local executable contracts share the existing property owner. Reuse
  compares exact expression keys, proof lineage, immutable payload ownership
  and implementation availability. Canonical child bindings, candidate
  liveness and exact-result guarantees remain live checks. Fact changes
  invalidate dependent properties, not an unrelated implementation contract.
- `b9d8bc9a`: a node distinguishes a completed local contract from completed
  subtree properties. Valid local work survives a later node's failure without
  claiming root readiness. A new counterexample initially rebuilt three local
  contracts on every failed attempt; after the fix repeated incomplete requests
  reuse those three and still return no candidate evidence. Restoring the missing
  proof produces the independent frozen-oracle result.
- CTE coverage is demanded only after prerequisite predicate-domain evidence
  exists and the selected candidate can consume it. Previously this work ran
  even when the function subsequently returned no evidence. No quality fact,
  rule alternative, budget charge or final verifier is skipped to certify a
  worse candidate.

This advances the local-property/required-work boundary of the existing
normalization → specialized optimization → Memo pipeline. It does not add a
parallel fast planner, behavior flag, query-specific rule or whole-root cache,
and does not claim to finish every construction-site migration. Exact keys and
mutable dependencies still need validation; not every node visit disappears.
The frozen-DAG oracle is test-only and final executable verification is intact.

Independent Detail captures (not normal timings):

| Query | QualityEvidence control (ms) | Probe (ms) | Evaluations |
| --- | ---: | ---: | ---: |
| Q04 | 0.381 / 0.367 | 0.412 / 0.393 | 1 → 1 |
| Q11 | 0.253 / 0.247 | 0.269 / 0.257 | 1 → 1 |
| Q74 | 10.718 / 10.898 | 6.313 / 5.679 | 140 → 140 |

Q74 retains 423 derived-node builds and 4,644 property reuses. Local contracts
now build 400 times and reuse 4,667 times; fact-only revisions do not rebuild
them. CTE coverage builds/reuses fall from 72/68 to 27/24: 89 incomplete requests
no longer demand this downstream property. Q04/Q11 each inspect one candidate,
so they have no local-contract reuse; this explains the limited opportunity,
not proof of zero overhead. The pilot tests the combined change, not separate
causal contributions of the index, local contracts and demand guard.

Cost synthesis remains Q04=782, Q11=401, Q74=1045. All other non-time search
counters match apart from the new contract counters and reduced CTE-property
work. Per-query artifact identity, actual selection and resources match across
all arms/batches; full typed rows, multiplicities and required ordering pass.
All runs stop QualityPolicySatisfied with SearchIncomplete, not ProofComplete.
No deadline, budget, objective, resource class or execution code was changed.

## Reproducibility and coverage

- Clean control: `048e4f20230c46e93989193f72bf3df9fbfc1e49`; code unchanged from
  registered `e7074450`. Clean probe: `b9d8bc9a1a22522e0f706beb1677eebf349a609b`.
- Control parod SHA-256:
  `cd19576f3992e072f295441d3072dcf36f1fe1b835064d8fd733ada06ad73b04`.
- Probe parod SHA-256:
  `2d69f746ab54b34d8db8a8cea2df8f05b73d3f5f7f16ab7f7bd26e3b4b105b95`.
- Same registered relocatable SF1 seed, SQL, DuckDB 1.5.5 native/built-in
  extensions, four threads, 2GB, binary results, quality policy and verifier off.
  Generator-declared metadata is not qualifying symmetric cross-engine parity
  evidence. Exact source/build/input identities are retained in inputs.json.
- One checkout/target; C/P/C/P; seeds 2026092401/2026092402, three fresh normal
  processes and one separate Detail process per query/batch. No exploratory
  cohort, removed slow samples or concurrent builds/tests during measurements.
  Normal timing uses the registered bounded PARO_COMPILE_WORK_EVIDENCE observer,
  never Detail or statement-trace time.
- Twelve unchanged maintained RunOutputs occupy about 3.1MB, under the 20MiB
  cap, without routine server logs or binaries. Shared validators check all
  campaigns, accepted attempts and 108 normal receipts; all twelve capture
  hashes and artifact/expected-grant/actual-variant associations match.
- Search counters, rules and portfolio variants have zero omissions. Detail
  event streams are bounded, **not exhaustive**: source/capture omissions are
  1511/756 (Q04), 664/119 (Q11), 2153/1887 (Q74). Encoding omissions are
  control→probe 1401→1402, 1399→1400, 1400→1400 respectively. New summary
  counters occupy some encoding space; retained-event counts are not used to
  infer complete search work. Nonexecuting COMPILE has no actual selected image;
  normal receipts supply actual admission, checked against portfolio membership.

Final checks: workspace tests/check and strict Clippy passed; optimizer 1,407,
benchmark 207, regression harness 103 passed (one optional skip). Full
compare-only SQL regress: **185 passed, zero failures, zero expected updates**.
Memory/vector guards and scoped formatting passed. See validation.json and
[the SQL report](sql-regress.txt). The test server has exited and the original
ignored regression report was restored. No new worktree, baseline bless or
history/data cleanup is part of this change.
