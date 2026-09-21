# Compile Evidence v3 工作区边界

本文件记录本轮开始时 `re-op` 的工作区边界。它不是把混合工作树纳入本任务，也不是恢复或清理清单。

## 起点

- branch: `re-op`
- HEAD: `1e3188d2fb9c94cddcbf1a242849e91362c3daf6`
- staged paths: `0`
- unstaged tracked paths observed at task entry: `87`
- untracked paths: `0`
- task-entry diff SHA-256: preserved in the task-entry shell evidence; task-owned files were staged by explicit path
- recovery snapshot: `/private/tmp/paro-reop-compile-evidence-v2-recovery-post-20260922/`
- existing recovery refs: `refs/codex/recovery/reop-after-transport-before-park`, `refs/codex/recovery/reop-before-transport`, `refs/codex/recovery/reop-wip-tracked-20260921`, `refs/paro-recovery/compile-evidence-v2-prework-20260921`

## 未提交路径分类

以下路径属于用户混合改动，除非另有明确主题归属，本轮不修改、不暂存、不提交。

### storage/execution-other

`crates/common/src/cold_work.rs`, `crates/common/src/lib.rs`, `crates/execution/src/operators/scan/table_function.rs`, `crates/execution/src/pipeline/mod.rs`, `crates/execution/src/pipeline/program.rs`, `crates/execution/src/query_executor/compiled.rs`, `crates/execution/src/query_executor/stream/mod.rs`, `crates/execution/src/runtime/pipeline_runtime.rs`, `crates/function/src/table/system/paro_optimizers.rs`, `crates/planner/src/binder/bind/statement/explain.rs`, `crates/planner/src/plan/arena.rs`, `crates/storage/src/buffer/page_cache.rs`, `crates/storage/src/buffer/prefetch.rs`, `crates/storage/src/rowset/column/column_reader.rs`, `crates/storage/src/rowset/page/page_io.rs`

### optimizer/native

`crates/optimizer/src/aggregate/late_payload.rs`, `crates/optimizer/src/aggregate/late_payload_tests.rs`, `crates/optimizer/src/cascades/budget.rs`, `crates/optimizer/src/cascades/engine.rs`, `crates/optimizer/src/cascades/engine/quality_production.rs`, `crates/optimizer/src/cascades/engine/tests.rs`, `crates/optimizer/src/cascades/engine/tests/closure.rs`, `crates/optimizer/src/cascades/engine/tests/grant_lazy.rs`, `crates/optimizer/src/cascades/engine/tests/quality_production.rs`, `crates/optimizer/src/cascades/memo.rs`, `crates/optimizer/src/cascades/memo/diagnostic_snapshot.rs`, `crates/optimizer/src/cascades/memo/tests.rs`, `crates/optimizer/src/cascades/oracle.rs`, `crates/optimizer/src/cascades/planner/boundary/tests.rs`, `crates/optimizer/src/cascades/planner/contracts.rs`, `crates/optimizer/src/cascades/planner/costing.rs`, `crates/optimizer/src/cascades/planner/domain_transfer.rs`, `crates/optimizer/src/cascades/planner/mod.rs`, `crates/optimizer/src/cascades/planner/predicate_order.rs`, `crates/optimizer/src/cascades/planner/quality_domain.rs`, `crates/optimizer/src/cascades/planner/scalar_facts.rs`, `crates/optimizer/src/cascades/planner/tests.rs`, `crates/optimizer/src/cascades/planner/transformation.rs`, `crates/optimizer/src/cascades/planner/transformation/cte.rs`, `crates/optimizer/src/cascades/planner/transformation/matching.rs`, `crates/optimizer/src/cascades/planner/transformation/matching_failure_tests.rs`, `crates/optimizer/src/cascades/planner/transformation/native_aggregate_topn_payload.rs`, `crates/optimizer/src/cascades/planner/transformation/native_aggregate_topn_payload_tests.rs`, `crates/optimizer/src/cascades/planner/transformation/native_deferral_tests.rs`, `crates/optimizer/src/cascades/planner/transformation/native_domain.rs`, `crates/optimizer/src/cascades/planner/transformation/native_domain_tests.rs`, `crates/optimizer/src/cascades/planner/transformation/native_join_elimination.rs`, `crates/optimizer/src/cascades/planner/transformation/native_join_preaggregation.rs`, `crates/optimizer/src/cascades/planner/transformation/native_join_subsumption.rs`, `crates/optimizer/src/cascades/planner/transformation/native_late_payload.rs`, `crates/optimizer/src/cascades/planner/transformation/native_materialization_tests.rs`, `crates/optimizer/src/cascades/planner/transformation/native_non_null_inputs.rs`, `crates/optimizer/src/cascades/planner/transformation/native_post_reduction.rs`, `crates/optimizer/src/cascades/planner/transformation/native_scalar_aggregate_window.rs`, `crates/optimizer/src/cascades/planner/transformation/native_selective_payload.rs`, `crates/optimizer/src/cascades/planner/transformation/native_topn_payload.rs`, `crates/optimizer/src/cascades/planner/transformation/native_topn_payload_tests.rs`, `crates/optimizer/src/cascades/planner/transformation/settlement.rs`, `crates/optimizer/src/cascades/planner/transformation/settlement/demand.rs`, `crates/optimizer/src/cascades/planner/transformation/settlement/native.rs`, `crates/optimizer/src/cascades/rules.rs`, `crates/optimizer/src/cascades/tasks.rs`, `crates/optimizer/src/cascades/verifier.rs`, `crates/optimizer/src/statistics/gathering.rs`, `crates/optimizer/src/statistics/propagator.rs`, `crates/optimizer/src/work_partition.rs`, `crates/optimizer/src/work_partition/b3.rs`

### session/server protocol

`crates/server/src/protocol/extended.rs`, `crates/server/src/protocol/simple.rs`, `crates/session/src/compile_explain.rs`, `crates/session/src/lib.rs`, `crates/session/src/result/sink.rs`, `crates/session/src/session.rs`, `crates/session/src/utility/discard.rs`

### 本轮拥有范围

本轮只在 Compile Evidence v3、context compile-diagnostics producer、EXPLAIN renderer/validator、RunOutput/collector/gate、相应测试和文档中逐 hunk 修改。若某文件同时含有用户 WIP，先保存恢复材料并只提交可独立证明的 hunk；无法安全分离的改动留在原工作树并标记为未准入。

## 本轮提交后的快照（2026-09-22）

- branch: `re-op`
- HEAD: `77677ece5fa0bbf5d151855d1235ea447abbbd6e`
- staged paths: `0`
- unstaged tracked paths before this document commit: `77`
- untracked paths: `0`
- current unstaged diff SHA-256 before this document commit: `816862d4c3b37f3cd9a550a7493317d453bb2da20ae368c9a7033711ce5c9793`
- the 77 paths consisted of the preserved mixed WIP plus the task-owned test
  and support documents that were still pending at that snapshot
- recovery material remains at `/private/tmp/paro-reop-compile-evidence-v2-recovery-post-20260922/`; no recovery ref or historical worktree was removed

The 74 mixed paths remain separated by the categories above. The task commits
contain only the compile-evidence/RunOutput/collector/renderer/validator/test/
document hunks listed in their commit summaries. The preserved optimizer/native,
storage/execution, and session/server paths remain unstaged; the optimizer
search-counter hunk was independently reviewable and was committed separately.

## Verification boundary

- `cargo check --workspace --locked`: pass.
- `cargo test --workspace --locked`: pass.
- strict workspace Clippy with `-D warnings`: pass.
- benchmark tests: 194 passed, 1 skipped.
- fresh high-FD SQL regress: 177 passed, 8 failures, 0 result mismatches; expected files were not changed.
- real PgWire smoke: `EXPLAIN (COMPILE, FORMAT JSON) SELECT 1` and
  `EXPLAIN (COMPILE, ANALYZE, FORMAT JSON) SELECT 1` were accepted by the shared v3 validator;
  ANALYZE carried an actual execution receipt, and ordinary `SELECT 1` returned successfully.

The physical identity implementation still contains a Debug-derived payload fallback. A targeted
six-shape real Detail producer smoke passed through PgWire and the shared validator, but the full
producer-to-gate campaign was not run. These remain explicit unadmitted boundaries; this snapshot
therefore does not claim TraceMatrixReady, C2, F2, parity, or a performance target.

## Post-topic-commit state

The independently reviewable task commits are now:

- `0a5a2bea` — v3 typed receipt and Detail validation contract;
- `775e9315` — bounded RunOutput, collector, gate, and benchmark lifecycle;
- `922820a6` — versioned physical-identity guardrails, with the typed-payload gate intentionally still blocked;
- `ff5188e4`, `f8188153`, `c7254bd9` — validation boundary, real Detail smoke, and support-matrix updates;
- `93992b09` — typed Rust search counters consumed by the v3 collector/gate;
- `77677ece` — declared-capture completion guard and regression tests.

After the final document commit the worktree will have no staged paths, no
untracked paths, and 74 unstaged tracked mixed-WIP paths. The recovery refs and
the post-2026-09-22 recovery directory remain unchanged. The final mixed diff
hash is intentionally recorded by the handoff command after this document is
committed; it is not used as a task source identity.
