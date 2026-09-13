# Q11 B3 structural preflight v1

This archive records the first implementation of the B3 apply preflight. It is a
diagnostic/production-path experiment, not a default-policy change and not a
formal C1 campaign.

## Hypothesis and implementation

The largest classified bucket in the 20260914 work partition was B3
(`apply/settlement/rollback`). Many rule bindings can be shown, from the binding
and the already-read boundary/fact snapshot alone, to have no output for the
current structural shape. The implementation adds a fail-closed structural
preflight in `PlannerTransformationRule::apply_binding` after the existing
boundary and fact reads have been recorded. It returns the existing generic
`NoOutput` rejection for only those shapes that the native/owned rule path
cannot produce; all domain, liveness, cost, and output-contract checks remain
authoritative for visible shapes.

The preflight covers the exact no-output cases for AggregateJoinSubsumption,
DimensionDeferral, InputMaterialization, and PredicateTransfer. Opaque groups,
unknown facts, and shapes for which the recognizer cannot prove no output fail
closed and continue through the old apply path. It does not change the rule
set, budget, stop policy, cost model, native staging contract, candidate
ownership, or final verification.

The fact snapshot is recorded before the preflight. This preserves the existing
read-set and wake-up contract for a rejected task; a rejected binding is not
treated as having no dependencies merely because its owned plan is not built.

## Validation

Targeted checks passed on the implementation source:

```text
cargo check --locked -p paro-optimizer
cargo test --locked -p paro-optimizer cascades::planner::transformation::native_domain --lib
cargo test --locked -p paro-optimizer cascades::planner::transformation::staging --lib
cargo test --locked -p paro-optimizer cascades::planner::transformation::matching --lib
cargo test --locked -p paro-optimizer aggregate::join_subsumption --lib
cargo test --locked -p paro-optimizer cascades::engine::tests --lib
cargo test --locked -p paro-optimizer work_partition --lib
```

The new unit case
`structural_preflight_stops_only_opaque_no_output_shapes` checks that the
preflight rejects only provably impossible opaque shapes and leaves a possible
shape on the authoritative path. Existing native-domain, staging, matching,
join-subsumption, engine, and work-partition tests pass as well. Pre-existing
dead-code/unused-variable warnings remain and are not part of this experiment.

## Q11 run

The run used a clean detached worktree at source `74d6cd5767c4ed45b1425d9577dcaa3f5de94dcf`,
4 execution threads, 2GB, planning DOP 1, execution DOP 4, cache miss, normal
trace off, and a separate one-block diagnostic cohort with the work-partition
ledger enabled. The normal cohort had two fresh process blocks and is a pilot,
not the registered 36-block campaign. The diagnostic statement trace and
ledger are excluded from normal C1.

Build and harness identity:

| item | SHA-256 / value |
| --- | --- |
| source working-tree attestation | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `parod` | `e88820bb3836e0b013dfabc483c7262a6a0538b44aa3436e9e3b228e45c25936` |
| `tpcds_compare.py` | `828c13abd199154985d82923d562dd9020d571acd00cfe7605963bd6a0bcb244` |
| `benchmark_evidence.py` | `58bd9d665d55e2eccf50c6be2a29c12e166b674589ca0aefbdb7616d2e31ec70` |
| `tpcds_result_contract.py` | `6b327a3f495f06e4363382014466c56b102b08af45c7ce4a25a7ed4f033e8c00` |
| `tpcds_setup.py` | `9db7956fc506d82008902f962013691b510691ba03ca23c92e89fd24abf2d171` |
| DuckDB database | `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7` |
| Paro data snapshot | `72ccb3dc02ce127f61c06b85920a0e75c17a55a60ee341d267251c67611f62b1` |
| Q11 query fingerprint | `6078428509570048703` |
| admitted portfolio fingerprint | `5c29cf646706c8c8ba84000150211a6b` |

All four measured Paro samples and the DuckDB samples returned 90 rows. The
typed result digest was
`9108a5b43530c24778abc0d8d3bf1ee218a9815eb0d15310bb4d7c6bcc5f2c78` and the
order digest was
`65c8b356de484b4922445b09fad720b278db22a3678b81295018c9370be0c3d7`.
The run status is `passed`, but its search status is
`QualityPolicySatisfied + SearchIncomplete`; it is not `ProofComplete`.

Normal trace-off pilot samples (milliseconds):

| metric | Paro | DuckDB | ratio |
| --- | ---: | ---: | ---: |
| C1 median (2 blocks) | 192.317542 | 108.487626 | 1.772708 |
| C1 p95 | 192.985875 | 108.728084 | — |
| warm median (4 samples) | 94.557104 | 104.393167 | 0.903525 |
| warm p95 | 95.094084 | 104.624500 | — |

The pilot C1 ratio 95% interval is `[1.762647, 1.782826]`; it is not a
parity qualification. Warm quality remained better than DuckDB in this small
cohort. The selected final-winner event-stream fingerprint was identical to
the previous work-partition baseline, and the complete result/order checks
passed.

## Partition and attribution

The diagnostic ledger (one trace-enabled process, excluded from C1) measured
the Q11 optimizer as 57.171079ms total, with B3 at 21.318572ms and
3.400625ms unclassified. The historical work-partition baseline measured B3
at 23.6847ms, so this slice reduced the measured B3 interval by about 2.37ms
(about 10%). The result is directional because the source revisions and
bounded-search workload are not a fixed-work replay.

The new diagnostic counters were:

| counter | preflight run | earlier probe |
| --- | ---: | ---: |
| cost syntheses | 1,689 | 1,691 |
| recomputes | 389 | 299 |
| published winners | 835 | 891 |
| transformation bindings | 327 | 317 |
| physical implementation requests | 343 | 364 |
| physical subproblem requests | 1,042 | 1,075 |
| task-registry requests / reuse / unique | 1,341 / 541 / 790 | 1,370 / 619 / 741 |

The final candidate and result stayed unchanged, but these bounded-search
counts did not stay unchanged. Therefore the run cannot be claimed as a
same-work reduction or as a causal normal-C1 improvement. The most likely
interpretation is that avoiding owned-plan construction changed the order and
amount of work completed before the existing resource boundary; this needs a
fixed-work closure replay before production promotion. The implementation is
not enabled as a new default policy on the strength of this pilot.

Rule-level diagnostic output for the final run:

| rule | matched | applicable | constructed | published | rejected / no-output | elapsed (us) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| AggregateJoinSubsumption | 73 | 0 | 0 | 0 | 73 / 73 | 630 |
| AggregateDimensionDeferral | 86 | 22 | 22 | 22 | 64 / 64 | 3,955 |
| AggregateInputMaterialization | 36 | 16 | 16 | 16 | 14 / 14 | 2,283 |
| JoinRegionEnumeration | 38 | 34 | 34 | 24 | 4 / 4 | 5,190 |
| PredicateTransfer | 81 | 52 | 52 | 37 | 7 / 7 | 8,846 |

The largest remaining B3 rule interval is PredicateTransfer, followed by
JoinRegionEnumeration. A broad PredicateTransfer shell bridge was previously
measured as a regression and is not revived here; the next implementation
must first identify a narrower, fixed-work-safe source of its apply cost.

## Raw artifacts

The compressed report, normal logs, diagnostic log, oracle log, and B3 ledger
are kept under `raw/`:

| artifact | compressed SHA-256 |
| --- | --- |
| `q11-b3-preflight-v2.json.gz` | `035ca7eca5fd10b4aa9df263a99dd2673e2750a22bb85973b08e28e4592e709e` |
| `q11.block000.parod.log.gz` | `e707d0287583b3194d8b213849efe62e0ddb28aa3d9da910cb0c35dd1854fee6` |
| `q11.block001.parod.log.gz` | `ae0dfd63971836386b36a419228eb5478c96a77a011d51db16e658c5f7517c25` |
| `q11.diagnostic000.parod.log.gz` | `3ddf16c1e1da55d60d3c8cfee6e378ba26f7fadbfadbac7d9e50b18da2ab6613` |
| `q11.oracle.parod.log.gz` | `2197bc174581215326121e831107ba1c56bbd01466deffe02e94f333db45f7ea` |
| `work-partition.jsonl.gz` | `47ab074290dc2e80bebd2112c227e2ba0675143401fd963c9cdcfa46c47b7d08` |

The uncompressed report hash is
`2f15457afd08b09d7826b239441e8bc24fa5fc4f66be19230fd49ff0b0b0578f` and the
uncompressed ledger hash is
`97eb9e8a363c7d71eceb2f20ae9e150e1c56a906fb299f583663e019d33d7ae7`.

This evidence does not claim M1/M2/M3, does not alter the default handoff
policy, and leaves the known SQL-regression failures and T1 warm non-inferiority
campaign as open separately tracked work.
