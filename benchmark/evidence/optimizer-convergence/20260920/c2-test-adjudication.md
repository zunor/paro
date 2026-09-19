# Baseline test adjudication (not a blanket baseline exemption)

The fixture-repaired clean baseline has 15 failures. They remain explicit gate
inputs; no failure is silently blessed.

| Failure family | Evidence / current judgment |
| --- | --- |
| nary dimension sharing, mark-to-semi | Fixtures supplied only grant 0 but retained a three-class session budget; expected-grant selection correctly returned undeclared class 2. Align fixture `max_grant_classes=1` with its explicit domain. Both original semantic assertions pass unchanged. |
| native deferral: nonidentity projection spine, exact dimension reference | Obsolete `settlement_cache.misses > 0` asserted use of the replaced owned transport, not semantic correctness. Remove only that instrumentation assertion. Preserve exact dimension GroupRef, statistics fingerprint, column/aggregate bindings, build-side barrier, no-owned-materialization and rollback checks. All 7 production-deferral tests pass. |
| singleton aggregate selection | Still selects HashAggregate instead of SingletonAggregateProjection. Needs proof of candidate eligibility/selection; not declared stale merely because another valid algorithm executes. |
| two certified-pruning tests | The expected prune counters remain zero. Correct result alone does not validate the promised pruning proof. Keep red pending bounded-proof/continuation analysis. |
| seven RF/build-side cases | Selected HashJoin differs from expected RF or left-build implementation. Keep the original assertions pending precise frontier, source-response and role analysis; do not bless a shape change. |
| calibration revision selection | NestedLoopJoin wins over the two range-join implementations deliberately calibrated by the fixture. Needs pricing/estimation adjudication, not a rewritten winner expectation. |

The seven RF/build cases are: `nested_filters_do_not_merge_distinct_build_domains`,
`union_all_probe_owns_one_runtime_filter_with_two_scan_consumers`,
`passthrough_projection_keeps_the_runtime_filter_consumer_lineage`,
`build_left_semi_join_filters_every_union_all_probe_source`,
`preserved_build_can_filter_a_direct_non_preserved_probe`,
`direct_rowset_reference_admits_and_selects_runtime_filter_region`,
`memo_hash_join_can_select_logical_left_as_physical_build`.

Logs are retained in the private archive: `single-class-fixture-sharing.log`,
`single-class-fixture-mark.log`, `native-deferral-contract-tests.log`.
These fixture corrections did not prove all 99 queries or SQL regress.

## Subsequent root-cause closure

The eleven assertions kept above now pass **unchanged** after the
[implementation-phase coverage repair](c2-implementation-phase.md).
They were not obsolete expectations: resident progress omitted the
mandatory-to-optional domain transition and skipped legal implementations
when logical membership did not change. Full suite with the dedicated
no-logical-publication regression: 1344 pass / 0 fail. The table preserves
the earlier triage rather than rewriting it as if this cause was known then.

This closes the recorded optimizer unit failures, not the separate SQL facet
lifetime and Q39 result-contract gates.
