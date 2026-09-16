# Native relation completion evidence: edge sharing pilot

Date: 2026-09-17

This is a two-process-block directional pilot for the native relation-facts
work in the mixed working tree. It is not a formal C1, W, or parity campaign.
The diagnostic cohort is archived in the JSON report but is excluded from the
normal timers.

## Hypothesis and production change

`native_domain::refresh_statistics` now completes the immutable relation
evidence for a native node once per fact snapshot: the ordered column view,
`BoundRelationFacts`, and column fingerprints travel together. Parent edges
share those completed values instead of rebuilding the facts and positional
column vector for every edge. Native refresh also derives output and carrier
layouts in one edge walk and uses the bound operator directly for output-name,
scalar, and operator identity work; the final native attachment remains the
single identity conversion boundary. Settlement cache entries retain the
completed evidence for valid native hits.

This does not merge facts across snapshots, occurrences, consumers, or
domains. Cache matching still checks the exact input facts and column
fingerprints. Full verifier/freeze and ordinary physical search remain in the
path.

## Reproducibility

The report is `q11-native-relation-edge-sharing-v5.json.gz` (gzip of the JSON). It records the full
dirty-source manifest and hashes:

- source commit: `013e00d762a8b2d6948a4648798a305a84e42c2b`
- source was dirty; recorded working-tree hash:
  `805dd22a50cc72832e6b20beb9c7e469240828d53af93c2d1f843cf5a2dc4898`
- Paro release binary SHA-256:
  `c168af627247e603613f461e38eb36e3c132a519943f5ebb01e7a84985192e7f`
- Q11 corpus SHA-256:
  `a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503`
- Paro data snapshot SHA-256:
  `d086f41f77cf32e4201f8d0bbadc3514b64a57577964a13ae40db2f2bd577ef5`
- DuckDB database SHA-256:
  `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`
- DuckDB: 1.4.4; extension SHA-256:
  `1896b22a92d74e6f093476eeff35a0ac7767b68b5748fd569681e80feaad65df`

Normal settings were fresh process, cache miss, trace off, 4 execution
threads, planning DOP 1, 2 GiB, private per-process data copies, and the
existing quality-handoff policy. The two blocks were run serially. Q11's 90
rows, schema/types, values, multiset, and order passed validation. The pilot
uses two blocks only and must not be used for a formal confidence claim.

## Results

| measure | Paro | DuckDB / note |
| --- | ---: | ---: |
| cold C1 median (ms) | 159.989 | 114.332 |
| cold C1 samples (ms) | 161.819, 158.158 | 115.436, 113.227 |
| cold ratio | 1.399311 | 95% pilot interval [1.396816, 1.401811] |
| compiler samples (ms) | 48.970, 46.668 | — |
| optimizer samples (ms) | 48.156, 46.831 | — |
| warm median (ms) | 72.339 | 109.234 |
| warm ratio | 0.655166 | pilot interval [0.640539, 0.669371] |

The two normal samples kept the same 1,757 cost syntheses, 167 native
relation evaluations, 41 native relation cache hits, and the same selected
handoff plan/result. The new low-overhead counters recorded 148 ordered-column
view reuses and 41 completed-evidence reuses. These prove that the shared
objects are exercised, but the pilot does not show a repeatable compiler or
C1 reduction; it is consistent with the work being too small or off the
critical path. It also cannot separate the normal timing from the other dirty
working-tree changes.

The diagnostic cohort reported about 55.7 ms optimizer time and is excluded
from C1. Both normal and diagnostic status remain
`QualityPolicySatisfied + SearchIncomplete`; neither is `ProofComplete`.
The compiler <=30 ms target, C1 parity, and formal warm non-regression gate
are not met or certified by this pilot.

## Validation and disposition

Passing checks for this change include `cargo check --locked -p paro-optimizer`,
20 native-domain tests, 3 native-relation settlement tests, the production
settlement-contract test, the native-shell staging test, and the 8 quality
production tests. The full optimizer library currently reports 1,319 passed
and 13 failures in the mixed tree; the failures are existing runtime-filter /
physical-selection, bound-oracle, and singleton/dimension-sharing assertions,
not silently blessed here.

The change is retained as a contract-level reduction of repeated immutable
fact/layout work, with performance explicitly unproven. No default stop
policy, budget, cost model, search domain, execution path, or quality gate was
changed. The next performance decision should be based on a fresh low-overhead
partition of the remaining non-rule compiler critical path, rather than more
local fact/cache or clone optimizations.
