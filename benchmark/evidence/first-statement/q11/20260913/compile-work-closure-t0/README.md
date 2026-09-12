# T-CWC / T0：干净基线构建阻塞

> **2026-09-13 授权后更新**：构建阻塞已清理，T0 与同二进制 T1 A/B 已完成测量。
> T1/T2 触发任务书负结果止损，没有达到性能验收；探针已撤下，默认策略未变。
> 最新结论、真实样本、失败与未完成项见 [RESULTS.md](RESULTS.md)。下文保留原始阻塞记录。

2026-09-13。只完成前置复核，没有实现T1/T2，没有新Q11性能成绩。

## 复现

- source：`185512ca88cecc46309d678e5037f4b45297cddb`。
- 独立、无修改worktree：`/private/tmp/paro-tcwc-baseline-ZjwFtb`。
- rustc：`1.92.0 (ded5c06cf 2025-12-08)`；cargo：`1.92.0 (344c4567c 2025-10-21)`。
- `git worktree add --detach /private/tmp/paro-tcwc-baseline-ZjwFtb 185512ca`：exit 0。
- 在该worktree运行：

```sh
cargo build --release --locked -p paro-server --bin parod --target-dir /Users/linjunhong/workspace/paro/target
```

exit **101**，paro-optimizer有49个编译错误。完整编译错误输出见
[clean-head-release-build.log](clean-head-release-build.log)；该文件从optimizer编译阶段开始，
不包含此前无错误的依赖构建输出。构建后独立worktree的`git status --porcelain`为空。
共享target仅复用Cargo构建缓存，不复制混合源码；这不是性能实验。

没有生成本HEAD的新parod，不得为它登记旧二进制SHA。原target/release/parod仍为
`ec7473e4bd8ae3a7802d5d784db0767cbac04a90043311c1b29260d9533f0a2f`，
属于前轮混合源码构建，本次没有运行它或借用它的历史成绩。

## 缺失依赖（首轮编译暴露，不是完整修复范围）

| 已提交消费者引用 | 干净HEAD缺失 | 主工作树对应位置 |
| --- | --- | --- |
| engine的bounds/seed成本接口 | bounds模块及部分公开导出 | 未跟踪bounds.rs、未暂存mod.rs |
| engine/planner的规则调度 | QualityDependency、RootDispatch及trait方法 | rules.rs同时有暂存/未暂存改动 |
| engine/planner的Memo读取 | operator_tag、带tag插入、obligation判定 | memo.rs未暂存改动 |
| 成本上下文身份 | calibration stable_fingerprint | calibration.rs未暂存改动 |
| 原生domain_transfer | generic projection fence、group_filter_can_move | filter/pushdown.rs未暂存改动 |
| planner质量消费 | selected_proofs、pending_domain_transfers | state.rs未暂存、quality.rs暂存改动 |
| planner构建/种子路径 | JoinRegionCache、PatternWitnessCache、materialize_seed_logical_plan、stable_cardinality_recipe | state/transformation/extraction/contracts等混合修改 |

另外，当前生产路径使用的engine/quality_production.rs及相应测试仍在原暂存区，
并不属于HEAD。只修补缺少的导出不能保证重建出前轮handoff路径。
现有文件中的实现是否全部应该纳入基线、如何拆分及验证，尚需单独收口，
不能把49个错误当作49处无语义影响的编译修补。

## 任务假设的源码复核

这些是静态分析，不是性能负结果，T1/T2的停止条件尚未通过实验触发。

1. `optimize_for_grants`确实先建立多class mandatory incumbent，再为全部class
   创建optional goals。但最后还会调用`optimize_grant_classes`，停止/质量快照也
   使用全部classes。只过滤interleave入口不构成惰性化闭环。
2. admission按可行性和objective成本比较变体，DOP是后续tie-break；观察4个worker
   不能单独证明所有样本永远选择class 2。需直接记录admitted class/fingerprint。
3. 改变参与optional搜索的goals会改变共享预算的使用和交织顺序。即使每个class
   的合法备选域不变，也不能先验保证有限预算下最终fingerprint相同。依任务书，
   一旦实际改变就停止T1，不调整admission来凑结果。
4. 两次`constrain_composed_cost_to_grant`分别位于child成本合成后和
   `enforcer_phase.compose_after(cost)`之后，输入不同；其`cost.validate()`也不是
   recipe常量。不能直接删除第二次约束。enforcer_cost_input已经存于recipe。
5. U是包含匹配外所有剩余工作的归一化比率，不是已测的合成函数单位成本。
   需要采样/微基准证实分配和键比较的贡献，再决定是否实施T2指定改法。
6. 组合整数化不能使用随frontier重排/增长变化的ordinal；可用稳定CandidateId
   精确驻留句柄，并验证预算事件与priced状态不同生命周期，不能跨epoch继承证明。
7. post-timer读取不意味着采集免费。elapsed标量需要在原SELECT区间记录起止并
   随occurrence保留；rule计时若原来仅在diagnostic启用，也必须单列其新增开销。
   optimizer_elapsed不等于完整compile；`C1-W-compile`即使同image同block也只是
   冷暖残差，W不是同一SELECT内的一段互斥时长，不能据此认定执行初始化成本。

## 状态与边界

```text
task: T-CWC / T0
status: Blocked
source_before: 185512ca88cecc46309d678e5037f4b45297cddb
implementation_commits: none
tests: clean HEAD release build, exit 101, clean-head-release-build.log
normal scalar: 未实现；尚无可构建干净基线
T1: Pending
T2: Pending（未运行微基准，不宣称假设被推翻）
T3: Pending
performance / counters / plan_identity: 未运行、未验证
SQL regress / oracle / Q11: 未运行
milestones: 未声称M1/M2/M3或parity
removed_paths: none
```

遵循任务书不使用dirty源码测量、不提交他人混合改动的限制，本次没有复制、
暂存、覆盖或提交任何已有实现。原暂存/未暂存diff SHA256保持：

- staged：`124afbe2526653449cbd859b3cf7d85d8098e60c0d491049c17c59b829e58957`
- unstaged：`05e067b95985d016ae7c3e7757838d14953d1b1d6720c3df356e014e395045ec`

设计仓库的两份主文档及未跟踪任务书没有修改或纳入提交。
下一步需要用户确定：授权先把现有相关混合实现按依赖审查、测试、拆分提交为
干净基线，还是提供另一个已经可构建且包含目标handoff能力的基线commit。
不应退回旧版并假定继承前轮质量，也不重新手写一套缺失实现绕过改动归属。
