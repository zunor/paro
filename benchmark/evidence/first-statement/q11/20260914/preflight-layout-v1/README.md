# Q11 B3 preflight and native-layout reuse v1

This archive records the implementation selected by the 20260914 optimizer
work partition. B3 (`apply/settlement/rollback`) was the only bucket above the
40% implementation threshold. The change is a bounded production-path
optimization; it does not change the rule set, budget, cost model, frontier
policy, stop policy, handoff policy, or search domain.

## Implementation

Commit `402ca20d79861e54a292b61ab4a52d21774e68ba` adds two related pieces:

1. `TransformationPreflight` is a fail-closed, binding-local hook. The planner
   uses only the existing structural no-output proof. It runs after the
   existing fire/output reservations and current-read checks, but before
   `TransformContext` and rule-owned shell construction. A `NoOutput` result
   retains the binding/application read set, seeds the same observation and
   lifecycle state, records the diagnostic rejection, releases reservations,
   and completes the task. Unknown facts, estimates, output identity, and
   uncertain shapes return `Continue` and remain on the authoritative apply
   path. This preserves invalidation, retry, cancellation, rollback, and
   budget semantics.
2. Native predicate transfer passes the immutable logical output layouts already
   produced by pattern lowering into the transfer/closure walker. The old
   test-facing wrappers still compute layouts for direct unit tests; production
   `try_transfer` does not walk the same shell a second time. No owned full-tree
   bridge or second cache was added.

The new engine test proves that a structurally proven no-output binding does
not enter `apply`, is counted as `NoOutput`, and remains observable through the
normal task path. The native-domain production tests cover the existing
projection/aggregate/UNION and semantic boundary cases.

## Clean-source validation

The benchmark was built from a clean detached worktree at the commit above:

```text
cargo build --release --locked --bin parod
```

Targeted clean tests passed:

```text
cargo test --locked -p paro-optimizer --lib cascades::engine::tests -- --nocapture
112 passed, 0 failed

cargo test --locked -p paro-optimizer --lib cascades::planner::transformation::native_domain -- --nocapture
18 passed, 0 failed
```

The clean full optimizer library run passed 1299 tests and retained exactly
the five known failures from before this change:

```text
aggregate::dimension_sharing::tests::nary_sharing_plan_is_stable_across_default_budget_envelope
cascades::memo::tests::statistics_read_cache_revalidates_registry_rollback_reinsert_and_merge
cascades::planner::tests::mark_join_to_semi_is_an_explicit_isolatable_transformation
cascades::planner::tests::nested_filters_share_one_ordered_source_work_lane
cascades::planner::transformation::cte::tests::engine_admits_every_partition_discriminator_from_one_binding
```

They were not modified or blessed. Existing compiler warnings also remain.

## Q11 pilot

This is a two-process-block pilot, not the registered 36-block campaign and
not a parity or M1/M2/M3 claim. It used the original SF1 Q11, binary protocol,
4 execution threads, planning DOP 1, 2GB, cache miss, normal trace-off, one
warmup, one measured warm round, and one separate trace-enabled diagnostic
block. The diagnostic block is excluded from C1. Engine order was balanced
ABBA within each fresh block; all samples were retained.

| metric | Paro | DuckDB |
|---|---:|---:|
| C1 samples (ms) | 199.880, 202.841 | 105.914, 107.384 |
| C1 median (ms) | 201.361 | 106.649 |
| C1 ratio | 1.888061 | 95% CI [1.887190, 1.888933] |
| warm samples (ms) | 107.322, 104.732, 109.132, 104.477 | 104.127, 104.233, 104.745, 105.792 |
| warm median (ms) | 106.027 | 104.489 |
| warm ratio | 1.016003 | 95% CI [1.000868, 1.033428] |

Both normal blocks verified the Q11 plan-cache miss, trace-off empty log,
complete 90-row typed result and ordering contract. Search status was
`QualityPolicySatisfied + SearchIncomplete`, not `ProofComplete`.

The normal post-timer compiler side channel reported, per block:

| block | compiler (ms) | optimizer (ms) | rules (ms) | combination syntheses |
|---:|---:|---:|---:|---:|
| 0 | 55.148 | 54.388 | 14.748 | 2053 |
| 1 | 55.817 | 55.027 | 15.345 | 2053 |

The trace-enabled diagnostic occurrence ended at 69.063ms compiler time and
66.631ms optimizer time; it is not a normal C1 number. The current pilot is
directionally lower than the earlier 93903224 pilot (C1 median 212.348ms,
warm median 115.325ms), but that comparison uses different binaries, seeds of
randomized order, and machine state. DuckDB also moved from 111.825ms to
104.489ms, so this pilot does not establish a causal percentage or production
parity improvement.

The current diagnostic occurrence retained the same combination-synthesis
count (2053) while moving structural no-output handling ahead of rule-owned
construction. No claim is made that the bounded pilot proves fixed-work
counter equality for every scope; a larger fixed-work replay would be needed
for that. No default handoff change was made.

## Identity and raw artifacts

| item | SHA-256 / value |
|---|---|
| source commit | `402ca20d79861e54a292b61ab4a52d21774e68ba` |
| clean source attestation | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `parod` | `e1ee81109a90626a1c7868b3126f77b9c650ca16e6e38c5343946e94c5ba4df1` |
| `tpcds_compare.py` | `828c13abd199154985d82923d562dd9020d571acd00cfe7605963bd6a0bcb244` |
| `benchmark_evidence.py` | `58bd9d665d55e2eccf50c6be2a29c12e166b674589ca0aefbdb7616d2e31ec70` |
| `tpcds_result_contract.py` | `6b327a3f495f06e4363382014466c56b102b08af45c7ce4a25a7ed4f033e8c00` |
| `tpcds_setup.py` | `9db7956fc506d82008902f962013691b510691ba03ca23c92e89fd24abf2d171` |
| Q11 SQL | `1f3c2697b2a82f597f9e8a570413be65ab7a02da03c1fe4c3ea7b725735433b8` |
| DuckDB database | `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7` |
| Paro seed | `72ccb3dc02ce127f61c06b85920a0e75c17a55a60ee341d267251c67611f62b1` |
| Q11 fingerprint | `6078428509570048703` |
| admitted class-2 physical fingerprint | `c24392ee2773c58eea6048992173517e` |
| uncompressed report | `1f7e6f6be4b072ab74af153967306601d92b5e938f675b6985a57368d868e92e` |
| uncompressed diagnostic log | `803cb2e523bb04327028e45c0c5d9d15dec8a77c9b7648b89c2eb08e0fad6017` |

Compressed raw reports and all normal/diagnostic/oracle logs are retained in
this directory. The normal server logs are empty by design; the diagnostic
log is kept separately and is not used to explain normal C1.

## Decision

The B3 hypothesis is implemented and passes targeted semantic checks. The
pilot is consistent with a small C1/compiler reduction, but it is not a
causal or formal performance result. The remaining normal gap is about 94.7ms
to the same-batch DuckDB C1, and the search remains incomplete. The next
measurement should use a fixed-work control/probe or a larger registered
campaign before attributing the change; no width, budget, stop-policy, or
handoff tuning is justified by this pilot.
