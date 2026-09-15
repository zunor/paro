# Q11 quality-evidence incremental-composition v1

Date: 2026-09-15

This archive records the bounded implementation experiment for composing
fact-free quality evidence from immutable selected candidates. It is separate
from the numeric-page decode task. The experiment kept the SQL, data seed,
metadata track, plan policy, resource envelope, budget, stop policy and quality
handoff policy fixed. Both reports are fresh-process, cache-miss, normal
trace-off Q11 measurements with one independent trace-on diagnostic block.

## Identities and validation

| arm | source commit | binary SHA-256 | source status |
| --- | --- | --- | --- |
| control | `c85f878b` | `a9f687dab9f34e8022ecce1233b960a0dc8250ad41a005a2dfc88af4a4026541` | clean |
| composed evidence | `4d4384b5` | `9dd8c37f30ab27a84e05a9c2858d93a47839069723eabe75c19307ec8ae71cb1` | clean |

Common input identities were data seed
`d086f41f77cf32e4201f8d0bbadc3514b64a57577964a13ae40db2f2bd577ef5`, the
original Q11 in `11.sql`, four execution threads, 2 GiB, private per-process
data copies, generator-declared metadata and binary result mode. The harness
performed typed schema, value, multiset and order validation for all samples;
both arms passed. The diagnostic cohort is excluded from C1/W gates.

The selected winner choice stream was identical in both diagnostic traces:
4,827 extracted choice fields, digest
`223a13742e176bba29ce1eb4282309eeb554be76eec678afde42f1e65ae7cd14`.
Search work was also identical: 2,061 winner proposals and cost syntheses,
1,285 published winners, 435 physical implementation requests, 1,203
physical subproblem requests and 82 quality evaluations. The quality status
was `QualityPolicySatisfied + SearchIncomplete`; neither arm was
`ProofComplete`.

## Pilot results

There are two fresh process blocks per arm. These are directional pilot data,
not formal M1, warm non-inferiority or parity evidence.

| arm | Paro C1 samples (ms) | DuckDB C1 samples (ms) | C1 ratio, 95% CI | warm Paro median (ms) | warm DuckDB median (ms) | warm ratio, 95% CI |
| --- | --- | --- | --- | ---: | ---: | --- |
| control | 180.474, 218.933 | 111.305, 127.874 | 1.666 [1.621, 1.712] | 79.542 | 121.717 | 0.640 [0.591, 0.667] |
| composed evidence | 170.534, 200.323 | 110.806, 115.455 | 1.634 [1.539, 1.735] | 72.262 | 107.809 | 0.663 [0.605, 0.717] |

The lower C1 point estimate is confounded by the lower DuckDB block times and
the two-block sample; it is not an attributable production gain. The same
plan and search work were selected, while the diagnostic optimizer increased
from 73.290 ms to 78.934 ms and the quality-policy interval increased from
57.288 ms to 61.927 ms. The probe recorded 222 local-summary hits, 337 local
summary misses/nodes, and the same 222/337 composed-summary hit/miss counts.

## Implementation conclusion

The first implementation cached local structural summaries but rebuilt and
merged mutable tree-shaped sets for every candidate evaluation. A second
implementation cached composed immutable summaries and the exact choice
stream; this removed the repeated composition on later evaluations, but the
clean probe still added about 5.6 ms to the diagnostic optimizer and 4.6 ms to
quality evaluation, with no reduction in search work or plan change.

The result is a negative stop decision for this quality-evidence representation:
it does not demonstrate a stable end-to-end C1 or warm improvement, so the
cache/composition path is withdrawn rather than left as a normal-path
overhead. The slow complete evidence derivation remains the semantic reference
in the committed source; no fact, ReadSet, CTE occurrence, UNION region or
candidate certificate was reused across changed facts.

Raw reports are retained as
`control-v1.json.gz` and `probe-composed-v1.json.gz`. The invalid earlier probe
that reused a control target through a temporary symlink is deliberately not
part of this archive. Formal multi-block power, M1/M2/parity and complete
search proof remain unrun.
