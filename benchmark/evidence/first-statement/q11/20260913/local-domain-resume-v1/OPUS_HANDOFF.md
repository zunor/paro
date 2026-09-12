# Opus 方向分析交接：必要域局部闭包与生产准入

2026-09-13。本次仅整理、核对已有证据并提交交接材料，没有重新运行性能实验、
修改优化策略或继续架构实现。请先区分下列三个对象，再分析下一方向：
**已提交局部补丁、当前混合工作树、未准入的完整实验组合**。

## 1. 工作区状态与提交边界

整理前 Paro HEAD：`634a30fde5c7e399b81208a1ead9b82fc662ac1c`。

已提交：
- `e45061c1`：NonNullInput 共享失败 DAG 的单次枚举负结果复用与真实访问计费。
- `815f2222`：原生必要条件在合法叶端 Filter 就地合并。
- `b282cd11`：本轮完整实验及未准入 allocation patch。
- `634a30fd`：最终测试输出。
- 独立设计仓库 `441cc29`：设计/任务状态回写。

已有混合源码与暂存内容没有丢弃、移动、取消暂存或混入交接提交。清理前快照：
- 20个暂存文件：2134 insertions / 35 deletions。
- 34个文件含未暂存改动：4754 insertions / 337 deletions；其中4个同时有暂存改动。
- 展开目录后124个未跟踪文件，含旧实验原始JSON、bounds.rs、cost_identity.rs等。
- 完整路径、状态、大小及源文件指纹见
  [workspace-inventory-20260913.json](workspace-inventory-20260913.json)。该清单保留清理前状态。

用户随后授权删除无用旧实验产物。本次仅清理两个强种子目录中被其已提交README
明确归为开发过程的旧轮次：54份未跟踪JSON及310份对应未跟踪/ignored日志，
共39,017,178 bytes。不是按内容重复判断；正式v9/v14报告、当前与负结果证据保留。
所有364个文件逐一验证未受Git跟踪，并在移动后重新校验SHA256。
它们已移至可恢复目录：
`/Users/linjunhong/.Trash/paro-obsolete-experiments-20260913-gnbdYN`，保留原相对路径。
精确文件与校验值见[cleanup-manifest-20260913.json](cleanup-manifest-20260913.json)。
原124个未跟踪文件中保留70个；本次新增交接文件另计。没有删除源码或唯一正式证据。

待整理内容按表面范围分为以下几组；这不是作者归属或测试通过的证明：
1. 已暂存的候选生产/质量调度、domain oracle与necessary-domain-v1证据。
2. 原生变换、匹配、发布、列布局与事实维护的未暂存实现。
3. bounds/成本上下文、Memo/提取/统计/验证器及相关测试。
4. execution project与EXPLAIN渲染修改。
5. 旧强种子和candidate-production实验产物。

当前暂存区并非自包含的已验证提交单元：engine.rs、engine/tests.rs、
planner/transformation.rs、rules.rs都有MM改动。请不要用普通git commit把原暂存区
全部提交，也不要git add -A。若要将遗留实现收口为干净基线，需要逐主题核对依赖、
归属与测试；本次没有替用户决定这些混合改动的去留。

**性能报告不是clean HEAD复现。** 测量使用dirty80a14161上的混合代码，之后仅将
本轮局部改动分别提交。当前HEAD单独检出并不保证包含报告所用的全部实现。
当前release binary SHA256核对仍为：
`ec7473e4bd8ae3a7802d5d784db0767cbac04a90043311c1b29260d9533f0a2f`。
未准入v2 binary为：
`50834d70837a865466c67d5a9f37cf7cccfe94c083a4d66a8e079f702fbfc17e`。

## 2. 当前性能位置

所有数字取自现有归档；单位ms。normal为原始SF1 Q11、fresh process/session、
trace-off、cache miss、完整90行typed/order校验，4 threads / 2 GB decimal。
小型顺序pilot有机器漂移，全部有效慢样本保留，不是正式准入campaign。

| 组合 | 路径 | blocks | Paro C1 median / p95 | Paro warm median | DuckDB C1 median | paired ratio [95%CI] |
|---|---|---:|---:|---:|---:|---|
| 当前保留v3 | 默认 | 2 | 1427.725 / 1479.463 | 107.383 | 120.926 | 11.863562 [11.078495,12.704264] |
| 当前保留v3 | handoff确认 | 4 | 261.148 / 263.011 | 92.200 | 106.461 | 2.437557 [2.396155,2.479674] |
| 未准入v2 | handoff确认 | 4 | 177.773 / 189.224 | 99.485 | 108.985 | 1.654483 [1.631166,1.693323] |
| 未准入v2 | 默认 | 2 | 22866.114 / 22976.581 | 143.203 | 134.776 | 169.676845 [168.014421,171.355717] |

当前v3的初始handoff两块C1为365.586ms、warm126.001ms、DuckDB C1为155.990ms；
不能只展示后面的261ms并隐藏这一慢campaign。全部样本在README及原始JSON中。

结论：
- 当前默认尚未接近DuckDB。
- 当前保留代码没有达到M1；所有正式milestone gate仍未通过。
- v2实验观察到sub-200ms且warm接近100ms，但默认回归使它未能准入。
- handoff是QualityPolicySatisfied + SearchIncomplete；默认BudgetLimited +
  SearchIncomplete。没有ProofComplete。

## 3. 已经证实的两个机制

### 发布身份不一致是第一处真实断点

staging.rs复用查找比较有序canonical child groups，但optional allocation身份
遗漏children。不同受限输入可能碰撞，长native closure构造后发布被回滚。
前轮debug和独立生产入口反例直接复现该错误，CTE多产物测试也同因失败。

补齐children后，长闭包能一次发布；不是必须冻结新根后才有能力继续每一跳。
然而修复也放开了普通默认搜索中此前被错误拒绝的备选，暴露严重扩张。
因此该全局修复仍撤回，保存在[allocation-not-admitted.patch](allocation-not-admitted.patch)。

### 叶端合并确实消除了多余往返

在allocation修复启用的相同策略实验中：

| 同SELECT诊断项 | 合并前v1 | 合并后v2 |
|---|---:|---:|
| 首次长闭包逻辑发布 | 29.897 | 30.134 |
| 首个合格根 | 85.909 | 33.276 |
| quality policy满足 | 89.388 | 34.594 |
| compiler返回（语句elapsed） | 106.844 | 46.709 |
| direct dispatch / work | 7 / 36 | 1 / 18 |
| synthesis / recompute | 2966 / 356 | 1361 / 176 |
| quality evaluations | 250 | 71 |

首个长binding并未更早生成。收益来自其内部直接合并合法输入落点的Filter，
不再发布中间串联Filter并等待约47ms的普通合并规则和祖先物理消费。

v2保留的精确候选链：
- 30.134ms：长binding发布logical108。
- 32.863/33.026ms：两个输入Filter候选发布，分别直接消费原scan group。
- 33.115ms：producer765/logical108发布。
- 33.124ms：root766发布；33.276ms合格。
- 34.594ms：quality政策满足事件另记candidate912。

以上是同诊断中按grant保留的候选身份，不能断言所有normal样本都执行root766。
未另造优化器，也没有减少预算或放宽逐分支质量门槛。

## 4. 默认扩张还没有解释完

allocation开启时，v1/v2默认的主要工作量一致：
- groups/logical/physical：1005 / 8656 / 14850。
- PredicateTransfer：128107 matches、7397 publications、19431 ineffective。
- synthesis541491、recompute123996。

最初暴露的NonNullInput共享失败遍历已修复：诊断记账从约19.2秒降到65.4ms；
但停止状态与探索工作不同，不能直接把差值当C1的因果收益。
当前PredicateTransfer记账约3.0秒，而optimizer约20.5秒，余量还没有完整分清
匹配、发布、事实维护、物理组合和等待。默认诊断大量事件丢失，不足以逐一
证明哪些组合重复。

撤allocation后的v3默认只有20340次synthesis、4151次recompute；长binding再次
未发布，当前质量确认约123.883ms、compiler145.195ms，direct21/204。

重要分析边界：
- 不能把7397次发布或541491次synthesis全部称为重复。
- 22.9秒可能混合“恢复此前被错误拒绝的合法搜索空间”与“真实冗余”；应先区分。
- 原1.43秒默认不能充当同一正确声明搜索域的完整搜索基准。
- 不能保留错误拒绝来伪造快，也不能默默把22秒回归作为修正后的生产能力上线。

## 5. 执行质量与测试边界

独立v2 EXPLAIN ANALYZE：
- 两日期扫描各730行；两partial输入1096053/289524行。
- final aggregate输入76098/22804；聚合输出残余保留。
- RF安装3，spill bytes0；observed workers/max tasks4/4。
- RSS369393664 bytes，声明working set95561216 bytes，二者不可混用。
- profile execution325.449ms带诊断开销，不能代替normal首次执行分账。
- 尚未完全建立profile与normal精确image同一性，不能跨cohort相减中位数。

最终源码验证：变换66通过/1个CTE多产物失败；domain45、engine102、
TaskRegistry22通过（有重叠），新增生产路径与独立NULL/重复键求值测试通过。
allocation开启时该CTE测试通过，撤回后失败；没有bless或全仓库全绿声明。
跨查询、不同grant/DOP、完整取消恢复矩阵及正式性能campaign仍未完成。

## 6. 请Opus重点分析的决策问题

1. 修正allocation身份后，新增受限视图中哪些是合法必需的备选，哪些可以依靠
   现有等价/域合同规范化复用？是否存在事实维护或发布身份造成的重复？
2. 在20.5秒optimizer账本中，除约3秒PredicateTransfer之外的主要工作到底在哪？
   应选择哪一个最小、可证伪的诊断切片，而不是再次扩建tracing平台？
3. 如何使allocation修复通过默认路径准入，同时保持已验证的33ms合格候选链，
   不恢复错误拒绝、不降预算、不丢non-selected child、不强制预聚合？
4. 下一轮是否应先把当前混合工作树拆成可重现的干净实现基线，再做性能干预？
   这与算法下一步应明确区分，不能把提交整理当作性能收益。

优先请给一个有证据的主方向；不预设必须新建任务框架、B&B、并行搜索或执行器
改造。现有证据已说明“在同一Memo快速形成合格候选”可行；未解决的是安全生产
发布与默认搜索扩张。首次执行是否成为下一主线，仍需要同image、同SELECT证据。

## 7. 阅读入口

- [本轮完整README、所有样本与身份](README.md)
- [未准入allocation修复及反例](allocation-not-admitted.patch)
- [工作区路径及指纹清单](workspace-inventory-20260913.json)
- 前轮：../local-domain-publication-v1/README.md
- 设计：/Users/linjunhong/workspace/paro-docs-design/optimizer/first-statement-latency-optimizer-design.md
- 任务：/Users/linjunhong/workspace/paro-docs-design/optimizer/first-statement-latency-implementation-tasks.md

除上述经用户授权、逐项审计的旧实验清理外，其他混合改动保持原状；本次不将其纳入提交。
