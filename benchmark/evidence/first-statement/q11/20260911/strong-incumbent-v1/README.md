# StrongIncumbentSeed v1

本归档验证一个问题：在全新 Memo 中提前提供一个合法、可执行且已按目标上下文重计价的强 incumbent，能否减少 Q11 的真实完整搜索工作。它不是 10 ms 或 C1 parity 验收；所有搜索均未 `ProofComplete`。

## 实验身份

- 查询：原始 TPC-DS SF1 Q11，query corpus SHA-256 `a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503`。
- Paro 数据副本 SHA-256：`acb0fea493d1afc186185c44a3861c69b5a9b4e0f33bf87f82bfa223d22101dd`。
- DuckDB SHA-256：`568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`。
- binary SHA-256：`efc1e3754c2e7a00e2831c30ace318fbab11189e82dbe1cad35f03648ad7d5f6`。
- harness：`tpcds_compare.py` `a3efda2b507939216cfd496c4adc059d9544440527a2065cb240fc21c60c899e`；`benchmark_evidence.py` `58bd9d665d55e2eccf50c6be2a29c12e166b674589ca0aefbdb7616d2e31ec70`；结果契约/数据准备脚本也记录在每份 JSON 的 `harness.files`。
- 资源：4 execution threads、planning DOP 1、2 GiB、单并发、5 个 fresh process blocks、每 block 一次冷样本和一次 warmup；normal trace-off，diagnostic trace-on 且排除出 C1。
- 四组：A 普通 incumbent/pruning off；B 普通 incumbent/pruning on；C 同一强 SeedPlan/pruning off；D 同一强 SeedPlan/pruning on。

成本契约把可执行 `SeedPlan` 与目标上下文的 `PricedIncumbent` 分开。C/D 不携带源 winner.cost 到目标 Memo，而是重放选中 DAG；只有一个实际 grant goal 完全匹配，另两个 grant goal 记录 mismatch 并拒绝。源搜索未完成，故种子不是全局最优计划。

## 正式结果

| 组 | Paro C1 / DuckDB C1（ms） | C1 ratio（95% CI） | Paro warm / DuckDB warm（ms） | warm ratio |
| --- | ---: | ---: | ---: | ---: |
| A | `1530.337 / 118.900` | `12.567479` (`11.290656–13.452816`) | `121.593 / 115.374` | `0.974447` |
| B | `1864.357 / 127.303` | `14.161180` (`13.290590–15.176035`) | `107.821 / 114.561` | `0.962644` |
| C | `7018.786 / 112.475` | `63.012280` (`61.810318–64.237616`) | `205.314 / 106.703` | `1.916040` |
| D | `7455.587 / 113.217` | `64.552615` (`62.918552–66.242728`) | `208.668 / 108.360` | `1.919857` |

`q11-{A,B,C,D}-v9.json` 全部 `status=passed`、`evidence_status=EvidenceValid`、90 行 typed schema/result/order 校验通过。normal Paro C1 p95（A/B/C/D）为 `3283.565/2162.350/7064.612/8243.946 ms`，DuckDB p95 为 `322.800/154.748/114.086/133.347 ms`；尾部如实保留。Paro 与 DuckDB 每个样本的结果摘要均为
`9108a5b43530c24778abc0d8d3bf1ee218a9815eb0d15310bb4d7c6bcc5f2c78`。C1 是 normal trace-off、cache-miss 的完整首语句计时；诊断样本不进入表格。

## 搜索与证明诊断

| 组 | 源种子阶段 | 目标搜索/停止 | seed reprice | 实际剪枝 |
| --- | --- | --- | ---: | ---: |
| A | 无 | actual stop `5,041,916 µs`，BudgetLimited | — | 0 |
| B | 无 | actual stop `1,531,414 µs`，BudgetLimited | — | 0 |
| C | 3 个候选；total `1,395,429 µs`，search `1,351,942 µs`，export `111 µs`；obligations `3510` | target total `5,359,861 µs`，actual stop `5,237,420 µs`，BudgetLimited | `282 µs`, count 1 | 0 |
| D | 3 个候选；total `1,408,815 µs`，search `1,366,365 µs`，export `105 µs`；obligations `3510` | target total `5,732,419 µs`，actual stop `5,618,774 µs`，BudgetLimited | `295 µs`, count 1 | 0 |

C/D 目标 freeze/handoff 分别为 `405/727 µs`、`386/744 µs`；源阶段和目标阶段都 `search_complete=0`。D 的强种子安装数为 1，但 `strong_incumbent_lookup_count=0`、`active_count=0`、`selected_for_bound_count=0`，因此上界没有进入耗时 bound 子问题。

实际被避免的工作为零：D 的 `certified_bound_pruned_before_children=0`、`...after_children=0`、`certified_recipe_prune_count=0`。A/B child combination synthesis 为 `17,667`，C/D 为 `44,121`。D 的 bound 检查 `36` 次，计算 `58,076 µs`，验证 `49,136 µs`，失效 `2,491` 次；原因计数为无 incumbent `5,883`、局部区间不确定 `31,062`、子任务未完成 `3,418`、source response 不支持 `23,991`、phase overlap 不支持 `128,378`、界不够紧 `36`。这些类别可重叠。B 同样零剪枝，计算/验证 `22,147/18,682 µs`。

## 结论

强种子成本契约和实际 `optimize_for_grants()` 入口已验证，但本次没有减少真实搜索工作；C/D 的额外源 Memo 使 C1 比普通控制组慢约 4.7–4.9 倍，warm 约慢 1.9 倍。D 相对 C 没有任何已证明跳过的子任务、recipe 或组合，不能把 C/D 的差异归因于 pruning。

下一轮唯一推荐：在现有 `BoundContext`、`TaskRegistry`、`ReadSet` 和冻结契约内，把安全上界传播到实际耗时子组，并接通保守的 `NoPlanBelow` 查询。无法覆盖 RF/source response、phase overlap、CTE 或未完成逻辑变换的上下文继续正常搜索；不改默认停止策略、不把估计区间端点当下界、不用 BudgetLimited 冒充完整证明。

旧的 v2–v8、debug 和 layout 文件仅是开发过程产物；本轮正式结论只引用 v9 四份 JSON 及其同名日志。

## 正式报告 SHA-256

| 文件 | SHA-256 |
| --- | --- |
| q11-A-v9.json | `4da79516b172e56f8d5961a8f5375a7ea7f89d40518f2ed9d21e192ff921671c` |
| q11-B-v9.json | `cf750af40197ec07c6eaf9c25f817a832a812246faff1a8461824edbc72ad66d` |
| q11-C-v9.json | `8f944df9050d3b241533bf48bfe4792d60bdf9584a97bb98de91301278cb0c86` |
| q11-D-v9.json | `e13003ce97b968fece4c7089386f0c17a3154e2ed0f2e1d4059e6cc2d6fcb2c2` |
