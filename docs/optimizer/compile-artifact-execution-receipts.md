# Compile artifact, admission and benchmark receipts

Status: T2 plus the Summary portion of T4-a/b/c/d. This document is a
support matrix and ownership contract, not a performance certification. Full
Detail remains T3; C2, F2 and TraceMatrixReady are not claimed.

## Lifecycle

| State | Owner | Meaning | What it may not claim |
| --- | --- | --- | --- |
| `CompiledArtifactReady` | compiler | `CompiledStatement` is immutable and has a versioned artifact/structure/dependency identity | no actual grant, image or execution |
| expected selection | compiler/portfolio | expected class and bounded variant summaries | no actual admission |
| actual selection | executor admission | real selected fingerprint, class, resources and fallback | no successful lowering or terminal |
| `ExecutableImageReady` (`image=Ready`) | executor | admission and lowering produced the image used by the result handler | no completed result |
| execution terminal | result handler | Completed, Failed, Cancelled or Dropped | no change to sealed compile record |

The compile record is sealed before rendering. ANALYZE attaches the execution
receipt by artifact identity; it never reopens or rewrites the compile record.
Unselected portfolio variants stay lazy. A resource or capability failure
preserves its original error and produces `NotExecuted`, not an invented image.

`ArtifactIdentity` is schema-versioned and contains independent artifact,
structure and dependency words. Pointers, process-local `CandidateId`s and
grant ordinals are not cross-run identities.

## EXPLAIN support matrix

| Entry | Result |
| --- | --- |
| simple query/CTE `EXPLAIN (COMPILE)` | one real compile, no target execution |
| `FORMAT JSON` | same typed document as TEXT |
| `COMPILE, ANALYZE` | one compile, one real admission, one execution, one attached receipt |
| extended Parse/Bind/Describe/Execute | known parameter types only; Describe never executes; incomplete Bind rejects |
| cache hit | current compilation is `NotExecuted`; receipt points to original artifact receipt |
| forced diagnostic compile | `ForcedCompile`; does not read or populate target plan cache |
| Detail | T3, not part of this delivery |
| DML/DDL/utility and unsupported parameter/protocol shapes | explicit unsupported/error result |

## Benchmark ownership

Every runner/gate command allocates an exclusive `RunId` below the configured
report root. Each source invocation is registered before work under
`sources/<SourceId>/attempts/<AttemptId>/`. Result, Summary, failure and attempt
metadata stay in that attempt; retries allocate a new AttemptId. A failed,
cancelled or incomplete attempt is retained. Existing RunIds are rejected, so
there is no `latest`, root `result.json`, or fixed `gate.json` overwrite path.

The runner, gate CLI, Make targets, SQL source, mixed source and Divan source
all consume the same owner. Archive append remains separate from live run
output. `RunOutput.register_cell` freezes the declared finite budget before
source execution using:

```
M = 32000 + 1024 * (A + Q + N + D)
T_i = 4096 + 1024*S_i + 2048*P_i + 512*R_i
B = M + 20000 + sum(T_i) + 200000*D <= 64 MiB
```

The bounded receipt channel is outside timed iterations and does not enable
Detail, force recompilation or statement traces. SQL query payloads carry a
`Verified` association only when compile and execution identities, occurrence,
actual selection and resources match. Missing, truncated, incompatible or
ambiguous data is `Uncovered`; timing and slow samples remain. Rust micro and
mixed-concurrency aggregate scenarios explicitly carry `Uncovered` when no
single SQL execution identity exists.

## Retention and deletion

Only files fully replaced by this run/source/attempt and accepted by their
consumer may be removed. Behavior experiments, NormalEvidence, mixed/uncovered
controls, old Detail signals and historical captures remain. `.parod.log` is
not part of the standard evidence package. Historical receipt-free reports are
not backfilled; they are marked Uncovered when consumed.

## Validation boundary

The benchmark schema validator checks ownership, attempt identity, receipt
schema, two-u64 identities, actual selection/resource fields and Summary size.
It can be run in strict receipt mode by a certified campaign, but normal
execution never converts an uncovered association into a failed timing sample.
Protocol and Rust tests separately cover cache hit/miss, ForcedCompile,
fallback/infeasible admission, image readiness, cancellation/drop, and sealed
receipt terminal transitions. Existing SQL regress differences remain
classified rather than blessed by this delivery.

The current clean release validation also exercised the real PgWire path for
TEXT, JSON, CTE, known-typed extended parameters, and `COMPILE, ANALYZE`; the
last case produced one completed execution receipt while non-ANALYZE cases
reported `NotExecuted`. The fresh SQL regression run was 177 passed with eight
retained EXPLAIN-only failures: `agg_join_subsumption`,
`agg_singleton_groups`, `explain_analyze`, `explain_basic`,
`join_explain_advanced`, `rowset_scan_pushdown`, `statistics_query`, and
`pgvector_topn_filter_flow`. No result mismatch or expected-file update was
accepted. The benchmark 99-query performance campaign, T3 Detail, C2 and F2
remain outside this delivery.
