# Native relation-facts reuse v1

Date: 2026-09-16
Scope: the optimizer's native `PredicateTransfer`/`AggregateDimensionDeferral`
vertical path; no execution-layer or stopping-policy change.

## What was changed

The native shell statistics path now uses the existing `SettlementCache` as the
owner of immutable relation entries.  A `NativeRelationEntry` carries the
operator/scalar/output identity, output layout, exact ordered input fact
snapshots and the derived column facts.  `native_domain::refresh_statistics`
forms the structural key before the expensive one-operator
`StatisticsPropagator`/`StatisticsGathering` fold.  A hit attaches the cached
operator to the current `NativeChild` links and publishes the existing
`ResidentNodeContract`; it does not re-run the owned assembly or statistics
fold.  A miss follows the existing propagation/gathering path and inserts the
complete immutable result.

The cache is transaction-scoped: native entries have a savepoint insertion
journal and are removed on planner rollback.  Memo-group inputs are validated
by their immutable `BoundRelationFacts`; compact local child nodes additionally
carry column-content/provenance fingerprints.  Thus a new occurrence can reuse
the relation result, while a changed fact/layout/statistics/column evidence
misses.  This is shared settlement ownership, not a second task registry or a
cross-query cache.  The native result still goes through the existing staging,
resident-contract, resource, ReadSet and FrozenCandidate validation.

## Measurement identity

The report and partition ledger are archived as:

* `q11-native-relation-v2.json.gz`
* `q11-native-relation-partition-v2.jsonl.gz`
* `q11-native-relation-v2.q11.*.parod.log.gz`

The release binary was rebuilt with:

```text
cargo build --locked --release -p paro-server --bin parod
```

The report records binary SHA-256
`33fb8a5e08660ce33dab73a52bb62aad83cff1ab64204ac7a1907c457377ec2c`, source
commit `1b6921cbf21731dc0468cb2a8bdb24f25e8821e6`, and source `dirty=true`.
Consequently this is a directional pilot, not clean-source causal evidence.
After the pilot, the latest pointer/fingerprint fast-path patch was rebuilt
successfully; its release binary SHA-256 is
`9048030edc6f8ebbe3ff564fa317a865837a9d2a7f9e26bcf487035d3936a38c` and it has
no separate performance sample in this archive.
The Q11 query corpus, SF1 data seed, DuckDB database, harness files, resource
envelope and all other identities are recorded in the compressed JSON report.

Normal measurements used fresh process blocks, cache-miss evidence,
trace-off, one target statement, four execution threads, 2 GiB and the existing
quality handoff policy.  The diagnostic cohort was separate, trace-enabled and
excluded from C1.  The query returned and validated all 90 rows, exact typed
schema, values, multiset and order against DuckDB.

## Q11 pilot result

| metric | Paro | DuckDB |
| --- | ---: | ---: |
| C1 samples (ms) | 163.588, 162.718, 163.587, 164.147, 162.189 | 104.960, 105.178, 105.831, 105.972, 106.043 |
| C1 median / p95 (ms) | 163.587 / 164.147 | 105.831 / 106.043 |
| C1 ratio, hierarchical 95% CI | 1.545931, [1.536853, 1.553963] | — |
| warm median / p95 (ms) | 67.710 / 68.990 | 103.208 / 104.076 |
| warm ratio | 0.655135 | — |

Compiler side-channel samples reported `58.690–59.915 ms`; optimizer samples
reported `57.859–58.994 ms`.  The diagnostic partition's target statement
was `61.643 ms`; this is diagnostic accounting, not a normal C1 subtraction.
The first diagnostic quality-policy-satisfied candidate appeared at about
`53.398 ms` and the diagnostic search stopped at about `54.299 ms`.

The main native-cache counters in the same diagnostic execution were:

| counter | value |
| --- | ---: |
| native relation entries / misses / hits | 196 / 196 / 41 |
| native fact evaluations / owned assemblies skipped | 196 / 41 |
| memo groups / logical expressions / physical expressions | 241 / 365 / 524 |
| child cost syntheses / recomputes / published winners | 2061 / 149 / 1285 |
| transformation bindings / quality candidate evaluations | 390 / 82 |
| physical subproblem requests / reuse | 1203 / 1425 |

The B3 sub-ledger for this target sums to `20.936 ms` across native producer,
statistics, owned fallback, settlement, staging, semantic guard, rollback and
encoding/validation buckets.  It is not claimed that all of that bucket is
duplicated work: the pilot's purpose is to establish the relation-level reuse
contract and its counters.  The normal compiler target of 30 ms was not met,
and this pilot does not establish a stable causal wall-time improvement over a
clean control.

## Correctness and regression status

The focused native-domain/settlement tests, the full optimizer library suite
and benchmark tests were run after the implementation.  Optimizer results were
`1326 passed, 2 failed`; the two failures are the pre-existing undeclared-grant
fixture cases
`nary_sharing_plan_is_stable_across_default_budget_envelope` and
`mark_join_to_semi_is_an_explicit_isolatable_transformation`.  They were not
modified or blessed.  Benchmark unit tests were `134 passed`.

The full SQL regression run used a clean temporary server data directory,
`ulimit -n 65536`, a single runner and an explicit temporary port: `153 passed,
31 failed, 0 skipped`.  The failures are existing EXPLAIN/PROFILE/fulltext,
graph, spill, vector and grant/identity expectation differences; no baseline
was updated and no failure was silently treated as unrelated success.

The optimizer state for the Q11 pilot is
`QualityPolicySatisfied + SearchIncomplete`, not `ProofComplete`.  Formal
parity, M1/M2, warm non-inferiority and compiler <=30 ms remain open.  The
latest small pointer/fingerprint fast-path adjustment was compiled and tested
but is not separately measured by this pilot.
