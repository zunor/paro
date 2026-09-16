# Q11 native property construction and resident physical state v1

Date: 2026-09-16

This archive records two independent optimizer changes:

* `fbee3a0f` builds native relation properties directly from `BoundReference`
  children, reusing the existing demand/statistics contracts instead of
  detaching an `OwnedLogicalPlan` for every native node.
* `37dc2706` retains the exact physical subproblem task/read/dependency tuple,
  recipe cursor, and completion state in the existing engine registry. Resume
  uses an incremental `ReadSet` update and does not reprocess a recipe for a
  completion-only change.

Neither change alters the budget, frontier policy, cost model, handoff policy,
execution engine, or SQL. The search result remains
`QualityPolicySatisfied + SearchIncomplete`; it is not `ProofComplete`.

## Source and input identity

The control and probe were built in separate clean temporary worktrees. The
main worktree contained unrelated user changes and was not used as a dirty
performance source.

| item | control A | probe B |
| --- | --- | --- |
| source commit | `fbee3a0fa6d52b14d7f8880e7053086b5cee803c` | `37dc270667958267e5e547f2fe2ea2b0feabe3cf` |
| source dirty | false | false |
| `parod` SHA-256 | `b177d50cb23d6f2271ca2164ef2b7cfcf6de54dd37de5115e6b1cd253ee473f3` | `e19fe3453a684b997a4c19a211f05b3dd99f09a16a31c820dfc36ab17b9e3932` |
| build | `cargo build --release --locked --bin parod` | same |
| source working-tree SHA-256 | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` | same |

The SQL corpus SHA-256 is
`a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503`.
The SF1 CSV source SHA-256 is
`4c73cc53e1567cad44341276a09589c209919e1002a7a59fbbedbf9d8e7532f3`, and
the per-process Paro data snapshot SHA-256 is
`72ccb3dc02ce127f61c06b85920a0e75c17a55a60ee341d267251c67611f62b1`.
The DuckDB database SHA-256 is
`568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`.

The harness hashes were identical for all arms:

| file | SHA-256 |
| --- | --- |
| `benchmark/corpora/tpcds_compare.py` | `828c13abd199154985d82923d562dd9020d571acd00cfe7605963bd6a0bcb244` |
| `benchmark/corpora/benchmark_evidence.py` | `58bd9d665d55e2eccf50c6be2a29c12e166b674589ca0aefbdb7616d2e31ec70` |
| `benchmark/corpora/tpcds_result_contract.py` | `6b327a3f495f06e4363382014466c56b102b08af45c7ce4a25a7ed4f033e8c00` |
| `benchmark/corpora/tpcds_setup.py` | `9db7956fc506d82008902f962013691b510691ba03ca23c92e89fd24abf2d171` |

Normal runs used four execution threads, one planning worker, a 2 GiB limit,
binary protocol, generator-declared metadata, private data copy per process,
one warmup and one measured round, and five serial fresh-process blocks per
cohort. `PARO_DIAGNOSTIC_WORK_PARTITION`, `PARO_ALLOCATION_PROFILE`, and
`PARO_STATEMENT_TRACE` were unset; `PARO_COMPILE_WORK_EVIDENCE=1`,
`PARO_COLD_WORK_EVIDENCE=0`, and the existing quality handoff policy were used.
The target statement was a verified plan-cache miss in every Paro cold sample.
All samples passed the complete 90-row typed schema/value/multiset/order
contract. Diagnostic statement traces and work ledgers were run separately.

The four raw normal reports are compressed in this directory:

* `control-v1.json.gz`, `probe-v1.json.gz`
* `control-v2.json.gz`, `probe-v2.json.gz`

The two compact diagnostic ledgers are `control-diagnostic.jsonl.gz` and
`probe-diagnostic.jsonl.gz`.

## Normal Q11 result

The two five-block cohorts were run in opposite arm order. They are retained
separately because the machine and DuckDB timings moved materially between
cohorts; pooling them would hide that confounder. The values below are the
report medians for each independent cohort.

| cohort | arm | Paro C1 p50 (ms) | DuckDB C1 p50 (ms) | C1 ratio, 95% CI | Paro compiler samples (ms) | Paro W p50 (ms) | W ratio |
| --- | --- | ---: | ---: | --- | --- | ---: | ---: |
| v1 | control A | 165.653 | 113.979 | 1.442752 [1.380681, 1.492272] | 54.543–58.190 | 70.933 | 0.646210 |
| v1 | probe B | 211.684 | 147.899 | 1.583986 [1.479949, 1.730888] | 58.389–123.748 | 88.064 | 0.720515 |
| v2 | control A | 221.695 | 142.062 | 1.355518 [1.118659, 1.610113] | 60.850–80.749 | 98.604 | 0.739731 |
| v2 | probe B | 163.191 | 111.353 | 1.483104 [1.450927, 1.519292] | 53.638–65.208 | 69.803 | 0.644033 |

The reports are valid pilot evidence, but this is not a causal same-DAG
compiler improvement: the ambient/order effect is large and the selected
incomplete plan was not proven identical by the normal reports. No formal
parity or warm non-inferiority campaign was run. The result does not meet the
compiler `<=30 ms` stage target or the final C1 parity target.

The deterministic work stream that was held constant in all four reports was
`1757` child-combination cost syntheses. The probe therefore did not show a
stable reduction in this work. A compiler-side median over all ten records is
`59.520 ms` for A and `61.347 ms` for B; because the cohorts have opposing
machine/order effects, these pooled medians are descriptive only, not an A/B
claim. The selected search state stayed
`QualityPolicySatisfied + SearchIncomplete` in both arms.

## Diagnostic accounting

The Q11 ledger exports low-overhead top-level buckets plus nested B3/B11
drill-downs. The nested entries must not be added to the top-level entries.
The ledger is diagnostic-only and is not used to subtract phases from normal
C1. Across four Q11 ledger records per arm, the top-level Q11 duration and
unclassified residual were:

| arm | diagnostic Q11 total range (ms) | top-level bucket subtotal range (ms) | unclassified range (ms) |
| --- | ---: | ---: | ---: |
| control A | 63.218–87.576 | 24.440–40.813 | 3.604–5.593 |
| probe B | 55.988–63.392 | 21.715–29.595 | 2.354–3.087 |

The most stable Q11 control/probe bucket ranges (ms) were:

| bucket | control A | probe B |
| --- | ---: | ---: |
| B2 rule matching | 1.247–1.804 | 1.247–1.621 |
| B3 rule apply | 0.561–0.678 | 0.560–0.606 |
| B5 scheduling | 1.719–1.972 | 1.479–1.719 |
| B6 recipe | 2.706–2.819 | 2.334–2.898 |
| B7 physical subproblem | 6.511–7.386 | 5.855–5.993 |
| B8 combination kernel | 1.083–1.179 | 0.982–1.072 |
| B9 admission | 1.192–1.267 | 1.010–1.036 |
| B10 publication | 0.729–0.868 | 0.618–0.707 |
| B12 finish | 2.214–10.292 | 2.085–8.636 |

The nested B3 drill-down shows native producer, statistics, owned fallback,
settlement, staging, guard, rollback, and encoding work; it was retained to
make the A path auditable. The data does not establish that the remaining
compiler time is entirely physical costing or that the resident state change
reduced the normal critical path.

## Implementation and validation

### A: native relation-property construction

The native PredicateTransfer path now keeps `BoundReference` children through
demand application and uses shared native statistics propagation/gathering.
The old owned-plan arena wrapper remains only at the generic owned boundary;
the native path no longer detaches an owned tree per node or constructs a fresh
property context for every cache miss. A single property workspace is reused
for the native refresh, and the existing layout, scalar, fact, transaction,
and invalidation contracts remain authoritative. This removes the old
per-node owned-IR round trip, but the pilot does not isolate a repeatable
end-to-end gain for A alone.

Targeted tests passed:

* `cargo check -p paro-optimizer`
* native-domain tests: 20 passed
* statistics gathering tests: 22 passed
* statistics propagation tests: 12 passed

### B: resident physical subproblem progress

The engine now retains exact child `(group, goal)` dependencies, a recipe
cursor, the resident read set, task identity, and completion state in the
existing physical task registry. A resume refreshes only changed or stale
read entries. Completion-only changes can close waiting obligations without
marking a priced recipe dirty. Merge handling clears resident state so an old
revision cannot be reused. Exact child choices and non-selected candidates
remain under the existing frontier and frozen-candidate contracts.

Targeted physical-response, goal-isolation, recipe-resume, completion-only,
and child-frontier tests passed. The B pilot did not produce a stable compiler
or C1 improvement; no performance claim is attached to the state retention
alone.

## Repository validation and known failures

The benchmark harness test suite passed in the main worktree: 134 passed and 1
optional test skipped. The full optimizer library ran to completion with
1318 passed and 13 failures. The failures are existing/current expectation or
fixture mismatches (runtime-filter algorithm choices, declared-grant fixtures,
calibration choice, singleton/aggregate choice, and the two bound-count
tests); no expectations were changed or blessed in this task.

SQL regress was run against the clean B build with an explicit `ulimit -n
65536`: 151 passed, 33 failed, 0 skipped, and 0 new failures according to the
runner. The initial run under the shell's soft limit of 256 failed with
`Too many open files`; it was an environment-capacity failure, not silently
classified as a source regression. Baselines were not updated.

The service-side and benchmark diagnostics were stopped after their runs; no
Paro server is intentionally left running by this cohort.

## Conclusion

The two requested production contracts are implemented and separately
committed, but the compiler target is not met. A removes a real native owned-IR
property-construction round trip; B preserves exact physical progress and
completion semantics. The current normal evidence does not prove that either
change, independently or together, lowers the C1 critical path, and it does
not prove `ProofComplete`. The next optimization must be selected from a
same-plan, low-noise attribution of the remaining non-rule compiler work;
additional caching or task-state wrapping is not justified by this pilot.
