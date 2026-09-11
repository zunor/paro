# Q11 same-Memo PReady → FrozenCandidate → execution

本轮只验证同一个 Memo 内的高质量候选交接：原生搜索发布候选后，由
planner-owned producer 检查同一份 `FrozenCandidate`，以完整的
`PReadyCertificate` 交给执行。没有跨 Memo seed、没有 NoPlanBelow、没有改变默认
停止策略，也没有把 `BudgetLimited` 或 `QualityPolicySatisfied` 当成
`ProofComplete`。

## 实验身份

- SQL：原始 SF1 Q11，`q11.sql` 的 SHA-256 为
  `1f3c2697b2a82f597f9e8a570413be65ab7a02da03c1fe4c3ea7b725735433b8`。
- Paro：binary protocol，4 threads，2 GiB，planning DOP 1；每档 5 个
  fresh-process normal blocks，normal trace-off、cache miss、完整 typed schema/
  result/order/digest 校验；另有 1 个 diagnostic trace-on block，诊断不计入 C1。
- DuckDB：同一数据与同一结果契约，成对运行。
- 最终二进制 SHA-256：
  `aed445533304ff74139f58b3a9724ed486815ee6c63264fd40957513f27b3b64`。
- 报告记录的源码基线为 `2ea460b7379dbaea4fc2f9b25bb86282abe059bd`，工作树当时
  有既有混合改动；二进制、SQL、harness 身份均写入每份 JSON。
- 所有样本的完整结果摘要均为
  `9108a5b43530c24778abc0d8d3bf1ee218a9815eb0d15310bb4d7c6bcc5f2c78`。

## C1/W 结果

单位为 ms；C1 是 normal trace-off 的真实首语句，W 是同一 fresh block 内的
暖态执行。ratio 是 Paro/DuckDB 的 paired log-ratio 聚合；CI 为 95% CI。

| cohort | Paro C1 p50 / p95 | DuckDB C1 p50 / p95 | C1 ratio [95% CI] | Paro W / DuckDB W | W ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| 10 ms | 484.613 / 552.416 | 119.256 / 122.153 | 4.222 [3.912, 4.556] | 354.725 / 114.624 | 3.093 |
| 20 ms | 219.694 / 265.332 | 116.262 / 133.299 | 1.896 [1.853, 1.950] | 147.830 / 110.062 | 1.287 |
| 50 ms | 249.268 / 256.917 | 112.427 / 116.387 | 2.214 [2.166, 2.263] | 148.006 / 108.651 | 1.387 |
| 100 ms | 249.929 / 252.790 | 114.073 / 115.619 | 2.200 [2.175, 2.231] | 149.430 / 108.450 | 1.387 |
| full + handoff | 247.025 / 256.091 | 114.090 / 124.646 | 2.132 [2.056, 2.179] | 143.682 / 105.841 | 1.352 |
| full control, handoff off | 1581.737 / 1903.822 | 130.402 / 166.993 | 12.366 [11.440, 13.366] | 112.396 / 120.873 | 0.928 |

20 ms 是本次最早满足质量政策的档位，但没有达到阶段目标 C1 ≤200 ms，且
其 W 仍比完整搜索控制差。full control 的 W 较好而 C1 很慢，说明本轮确实
把规划后的执行路径交接出来了，却没有恢复完整搜索控制的全部物理质量。

## 停止状态和交接时间线

| cohort | quality milestone | 实际 stop / policy satisfied | quality 证据 | completeness |
| --- | ---: | ---: | --- | --- |
| 10 ms | 无 | 10.002 ms / — | 无 root candidate | `Deadline`, 0 choices at checkpoint |
| 20 ms | 19.337 ms | 20.318 ms / 19.337 ms | 3 Completed + 1 NotApplicable，3 grants | `QualityPolicySatisfied`, search incomplete |
| 50 ms | 49.297 ms | 50.287 ms / 49.297 ms | 3 Completed + 1 NotApplicable，3 grants | `QualityPolicySatisfied`, search incomplete |
| 100 ms | 51.454 ms | 52.492 ms / 51.454 ms | 3 Completed + 1 NotApplicable，3 grants | `QualityPolicySatisfied`, search incomplete |
| full + handoff | 52.833 ms | 53.907 ms / 52.833 ms | 3 Completed + 1 NotApplicable，3 grants | `QualityPolicySatisfied`, search incomplete |
| full control | — | 1492.801 ms / — | no quality evaluation | `BudgetLimited`, search incomplete |

20/50 ms 的早停样本在 deadline 边界通过最终 root 重新评估后，stop reason
仍明确记录质量政策满足；这不是用较早的 Deadline 覆盖真实交接。每档的
`actual_stop_us`、profile stop/return、timeout tail、freeze 和 extraction
字段都在 JSON 与 diagnostic log 中。20 ms 的 freeze 为 550 µs、handoff
extraction 为 750 µs；full handoff 分别为 511/787 µs。它们没有把冻结开销
当作整段规划时间，也没有在交接后补跑完整搜索。

## 精确候选与依赖证据

每份 JSON 的 diagnostic target trace 都包含 `final_winner_N.choice_i` 的
`group/candidate/goal/logical/physical/logical_payload/physical_payload`、精确
`child_*` refs、规则产物和 `fact_read_*` fingerprints。20/50/100 ms 与 full
handoff 的三个 grant winner 均为 35-choice、35-read DAG，root group 30，
root logical 47、physical 31、payload 47/49，root candidates 分别为
`213/295/380`；20 ms 最后一项 `quality_policy_candidate` 为 380。full
control 的 root candidates 为 `3759/3762/3765`，为 37-choice、37-read DAG。

同一候选的形状摘要如下：

| plan | nodes | gets | filters | aggregates | joins | materialized CTE / refs | width | runtime filters | spill contracts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 ms fallback | 31 | 6 | 5 | 2 | 7 | 1 / 4 | 30 | 0 | 1 |
| 20–full handoff | 35 | 6 | 9 | 2 | 7 | 1 / 4 | 12 | 2 | 1 |
| full control | 37 | 6 | 9 | 4 | 7 | 1 / 4 | 10 | 4 | 1 |

selected rule ids 的 union 为：20 ms `10008,10015,10023,10026`；50 ms
再加入 `10016`；100 ms/full handoff 再加入 `10021`。对应名称见
`crates/optimizer/src/cascades/rules.rs`（CTE demand/filter、aggregate
deferral/materialization、join region、predicate transfer）。full control
还包含 `10013,10022,10024,10025`。

20 ms trace 的首个可解释时间点（µs，自 optional profile 起算）为：

- safe baseline：1,198；logical publication：2,803；
- CTE filter pushdown first applicable/published：2,788 / 2,823；
- predicate transfer：3,957 / 4,972；
- aggregate dimension deferral：13,250 / 13,257；
- CTE demand pushdown：12,360 / 12,385 首次 discovered/matched，但
  applicable/constructed/published 均为 0，且有 2 次 rejected；
- 20 ms quality policy：19,337；最终返回 profile：19,375，实际返回时刻
  20,318。

因此不能把 CTE demand 规则的发布时间当作该链已经闭合：当前候选实际使用
了可验证的 CTE filter/predicate/aggregate 组合，CTE demand upstream 仍是
下一轮需要定位的具体缺口。规则首次发布时间本身也不是因果瓶颈；目标
candidate 已在 19.337 ms 形成并被冻结，之后 diagnostic trace 显示
compiler return 为 27.784 ms，first result chunk 为 228.003 ms，fetch drain
为 228.378 ms。C1 仍需把这些诊断与 normal trace-off 样本分开理解。

## 工作量和内存诊断

20 ms / full handoff / full control 的 diagnostic counters 分别为：

| cohort | groups | physical exprs | proposal/synthesis | physical subproblem eval / request / reuse | transformations | observed working set |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 20 ms | 67 | 80 | 421 | 308 / 308 / 66 | 67 attempts / 15 inserted | 40,076,544 B |
| full handoff | 113 | 232 | 1,408 | 839 / 1,115 / 1,153 | 225 / 41 | 40,076,544 B |
| full control | 873 | 2,592 | 17,667 | 7,801 / 45,379 / 55,508 | 5,504 / 588 | 45,713,728 B |

这些是诊断快照的工作计数/working set，不把计数下降直接解释为性能成功。
10 ms 没有 root candidate，最终只能冻结 31-choice 安全 fallback；其诊断
working set 为 1,048,863,104 B，必须在后续 D6/内存 profile 中继续拆解，不能
猜成单一算子原因。

## 代码契约和验证

- `QualityEvidenceProvider` 只接收同一 Memo 的 exact `FrozenCandidate`；它
  校验 logical/physical payload、child refs、result guarantee，并生成同一
  root 的 native choices、facts 和 read set。不会重新发现候选、注入 owned
  logical tree 或复制 Memo。
- `QualityBundleRegistry` 对每次候选先清空旧结果；所有 Completed 包必须有同一
  CandidateId、choices、region 和 ReadSet，`MissingEvidence`/`Suspended` 不会
  变成 `NotApplicable`，全 NotApplicable 也不能形成 PReady。
- 多 grant 只有所有实际 grant 都有证书才会交接；交接存的是不可变的
  `GrantWinner/FrozenCandidate`，执行阶段直接消费其精确 child choices 和
  payload。未完成逻辑/物理 obligations 仍保留，质量满足不等于 ProofComplete。
- `cargo test -p paro-optimizer cascades::quality --locked`：8 passed；
  `cargo test -p paro-optimizer cascades::engine --lib --locked -- --test-threads=1`：
  83 passed；`make -C benchmark test`：126 passed。

## 结论和下一项

本轮已经打通并实测验证了同一 Memo 的
`PReady → FrozenCandidate → execution` 交接，但阶段目标未通过：20 ms 的
PReady C1 为 219.694 ms，W 为 147.830 ms，而 full control W 为 112.396 ms；
没有任何样本是 ProofComplete，默认完整控制为 BudgetLimited。当前第一条
具体阻断不是冻结/抽取（约 0.5–0.8 ms），而是交接候选缺少 full control 的
两个聚合阶段和两个 runtime-filter 选择，以及交接后的首执行/排空（诊断约
200 ms 级）。CTE demand pushdown 的 applicable=0 是可复现的上游证据，但不能
单凭规则时间声称它是全部瓶颈。

因此下一轮唯一建议是：先对“quality candidate 与 full-control 物理选择的
差异”做同一 Memo 内的实际首执行热点分解，并补齐最早阻断该候选的通用
producer/consumer 依赖；不要扩展跨 Memo seed、NoPlanBelow 或并行搜索。
