# T-CWC：授权后的干净基线、grant 探针与负结果

2026-09-13。本文取代同目录 README 的“尚未授权”现状，保留其历史记录。
**没有达到 T-CWC 的性能验收，也没有达到 M1/M2/M3。** T0 已取得干净构建与
normal 同 occurrence 标量；T1 的工作量机制有收益，但触发 §6.3 的模型止损；
T2 的采样不支持既定低分配改法，按 §6.2 停止。未完成的生产契约没有切默认。

## 1. 改动归属与构建

用户随后明确授权“清理阻塞，然后完成所有任务”。此前已提交消费者所需的混合
实现按依赖整合为 `c191f3ee`、`12205905`、`3bb92ede`；这是基线重建，不算
本轮性能收益。没有重新应用 withdrawn allocation/domain identity patch。
既有执行器 debug、extraction debug、first_statement_attribution.py 改动和旧
necessary-domain 暂存证据未混入本轮提交；没有整树 add、清理或 bless。

- `086cd835`：T0 occurrence-bound compile-work 侧信道。
- `a502a89d` / `fe14a367`：预注册及原始 SQL 路径修正。
- `6a5c8e4d`：仅 trace-on 输出实际 portfolio admission 尝试的 class/fingerprint。
- `3c74b4a1`：T1 可行性探针；不是生产契约完成。
- `94ac26f4`：按 §6.3 撤下探针，恢复全部 optional grant 调度。

全部被接纳的 normal 报告来自独立 worktree 的 committed、dirty=false 源码。
共享 Cargo target 仅复用构建缓存。最初 target 根符号链接被 Git 当作未跟踪路径，
因此 `paro-tcwc-t0-handoff/default.json.gz` 虽结果正确，身份为 dirty=true：
**仅保留为无效预演，不用于性能结论**。修正成 ignored target 目录后重新采样。
旧 /tmp SQL 目录的 hash 不符，在预检阶段拒绝；正式运行使用已归档原始 11.sql。

## 2. T0：同 SELECT 标量，不伪造执行分账

`PARO_COMPILE_WORK_EVIDENCE=1` 在已存在的 cache-miss occurrence 中保留
compiler/optimizer/rule elapsed 和 synthesis count 四个 u64。通过
`paro_optimizers()` 在 C1 计时结束后读取，normal 不启用 statement trace。
计时读取和固定大小拷贝实际发生在原 SELECT 内；“post-timer”是读出和序列化，
不是采集零成本，也没有把编译工作移出 C1。它不是字面上的仅一个 u64。
缓存命中不发布旧 compile work；同 query 的不同 occurrence 不相互覆盖。

干净 T0：handoff 4 blocks C1 280.292ms / W 102.002ms；default 2 blocks
C1 1389.676ms / W 109.821ms。开关关闭的 2 handoff blocks C1 271.426ms，
合成 4903、诊断 winner 2431 不变，结果一致、没有 compile-work 字段。
样本不足以证明计时开销为零或统计等价；不能将这两组中位数差认定为仪表开销。

`summary.json` 对每个 normal block 独立计算
`C1 - 同 image W - 本 occurrence compiler`；该量是冷暖残差，不是同 SELECT 的
互斥执行阶段。`optimizer_elapsed_us` 不等于 compiler，也不含 parse；没有用
diagnostic compiler 中位数去减 normal C1/W。

## 3. T1：同二进制 A/B

源码 `3c74b4a1792b467b8dab5ad16868e2747275126c`，二进制
`5aa27e0938574e86f2033ec27a62a72ccd045a39a99b22ec746c322b5f083418`。
SQL corpus `a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503`，
Paro seed `72ccb3dc02ce127f61c06b85920a0e75c17a55a60ee341d267251c67611f62b1`。
DuckDB/data/harness 的完整 hash 在每份原始报告中，不借用旧版成绩。
资源 4 threads / 2GB；原始 SQL、binary protocol、generator-declared metadata、
规则、成本模型和搜索预算一致。每种 handoff 4 blocks、default 2 blocks；normal
fresh/cache miss/trace off，与独立 diagnostic 分开。全部有效慢样本保留。

| normal 指标 | handoff control | handoff probe | default control | default probe |
| --- | ---: | ---: | ---: | ---: |
| C1 median ms | 282.266 | 201.805 | 1458.918 | 1156.900 |
| C1 p95 ms | 302.250 | 206.797 | 1473.760 | 1157.028 |
| W median ms | 98.523 | 95.827 | 101.174 | 100.140 |
| synthesis / SELECT | 4903 | 1691 | 20340 | 7351 |
| optimizer median ms | 136.211 | 65.986 | 1304.616 | 1016.378 |
| rule median ms | 29.217 | 24.561 | 465.338 | 431.199 |
| residual U median µs | 21.822 | 24.497 | 41.262 | 79.605 |
| W paired 95% CI | [.884,.955] | [.881,.951] | [.929,.974] | [.920,1.015] |

handoff probe 配对 C1 ratio 1.8294，95% CI [1.7919,1.8695]；default probe
7.745 左右，宽区间来自 DuckDB C1 漂移，准确值以 summary/raw 为准。
这些是任务规模的工程 A/B，不是正式 parity campaign。

诊断计数（不能混入 normal 分账）：

| 指标 | handoff control/probe | default control/probe |
| --- | --- | --- |
| groups | 180 / 180 | 816 / 816 |
| logical expressions | 288 / 278 | 1373 / 1373 |
| physical expressions | 526 / 495 | 2503 / 2503 |
| published winners | 2431 / 891 | 9042 / 3229 |
| necessary recomputations | 1012 / 299 | 4151 / 1457 |
| first_safe µs | 1232 / 1211 | 1264 / 1401 |
| quality satisfied µs | 127097 / 61173 | 不启用 handoff |

每组一个 diagnostic admission attempt，均 class 2，无该 SELECT 的重试；
handoff 两组 fingerprint 都是 `5c29cf646706c8c8ba84000150211a6b`，default
两组都是 `33eee07b2590b88a377c533ff2267634`。这直接核实了诊断样本的实际
admission，不是由 worker 数推断。**normal 每个 image 的逐位 attestation 尚未
另行补全**，不能把一个诊断样本冒充所有 normal 样本的计划证明。
所有 normal/oracle/diagnostic 结果均通过 90 行、类型、多重集和 ORDER BY
peer-group 契约校验。handoff 为 QualityPolicySatisfied + SearchIncomplete；
default 为 BudgetLimited + SearchIncomplete。都不是 ProofComplete。

探针保持全部 mandatory P_safe，仅对最大 envelope class 做 optional，其他 class
保留精确 frozen safe choices。它没有提供编译时实时可用度选择、显式 portfolio
状态或 cache key；因此只用于前提实验，未声称生产 T1 已完成。

### 为什么停止，而不是把 201ms 宣布成完成

handoff 合成减少 65.5%、winner 减少 63.3%，达到计数门槛，且执行质量未见退化。
但 default 合成减少 **63.86%**，optimizer 仅减少 **22.09%**；扣除各自同次
rule 时间后残余仅减少约 **30.3%**，U 几乎翻倍。default 的 groups、logical、
physical 数完全相同，不能把残余全部归入合成函数。
这否定了任务书以 U 近似固定单位计价成本进行收益预测的前提，不否定 grant
选择有局部收益。按 §6.3 停止该项，不继续实现生产策略/cache 来绕过停止条件。
探针 commit 可复现，运行开关及实现已在 `94ac26f4` 撤下。

## 4. T2：独立 fresh 采样，先验证瓶颈

两个独立新 parod + 私有 seed snapshot，执行原始 Q11；`/usr/bin/sample` 3s、
1ms，trace-off，但作为独立 **sampling diagnostic**，绝不是 normal C1。
脚本 `paro-tcwc-sample.py` 与 raw `.sample.txt.gz`、进程 metadata 均归档。
脚本使用现有 isolated server，结束后关闭自己启动的进程与私有副本。采样脚本只
核对行数/缓存 occurrence；完整 typed/order 校验由上述同二进制 campaign 提供。

`paro-tcwc-sample-summary.py` 按 call tree 计算 exclusive 自身样本，再归到直接
分支，不相加嵌套 inclusive 时间。optimizer 样本总数 1110 / 968：

| 互不重叠直接分支 | sample 1 | sample 2 |
| --- | ---: | ---: |
| rule apply_binding（含构造/staging） | 315 | 271 |
| drain_physical_interleave（整个物理子树） | 184 | 162 |
| schedule_transformation_dependents | 180 | 163 |
| rule bindings | 86 | 75 |
| enqueue_physical_ancestors | 55 | 50 |
| seed_transformation_observation | 38 | 35 |

在上述物理子树等位置内，组合 compose 仅 9 / 10 样本，combination identity
intern 2 / 1，task supply 3 / 4。它们是更细的子归因，**不能再加到表格总数**。
内联/符号折叠限制了单函数分配的精确归属，但整个物理交织子树也仅约 17%，
不支持将全部 residual U 视作该组合循环的分配成本。allocator 在其他规则/
依赖维护路径也出现，不应把全进程 malloc 样本算到 child combinations。
两次采样的热点结构一致；没有测得“每次合成自身花 23–42µs”。

结论：按 §6.2 停止指定 T2 改法；没有提交组合 integer/arena 重构，没有改变
比较、frontier 或 budget 语义。T2 指定 oracle 因未实施该机制而未运行，不能
用现有组合测试冒充新整数化实现验收。也不删除两次输入不同的 grant constraint。

## 5. 测试、失败与 T3 状态

- clean release（3bb92ede）与 T0 release：exit 0，日志归档。
- T0 `cargo check --locked -p paro-session`：exit 0。
- `cargo test -p paro-context compile_work_is_exact_occurrence`：1 passed。
- T1 实际 Memo/grant probe 反例：1 passed；独立 grant-sharing fixture：1 passed。
  这些不覆盖完整 production availability/cache/admission matrix，未冒称覆盖。
- 撤回后的干净 `94ac26f4` engine tests：**102 passed**，exit 0。
- 同一干净源码的 portfolio tests：**9 passed**，exit 0。
- harness unittest：compile evidence 2、first-statement manifest 3 passed。
  unittest 不会发现其他 pytest-style 函数，不宣称全套 benchmark tests 通过。
- 先前错误 cwd 的 harness import failure 原日志保留，正确 cwd 重跑通过。
- 集成基线完整 optimizer lib：**1200 passed / 4 failed**，exit 101。
  失败发生于本轮 T0/T1 之前，已读取具体断言：
  - statistics cache merge：等语义 producer merge 后 hash 相同，测试要求不同。
    缓存与重算相同；是否应调整 identity oracle 仍未收口，不擅自改断言。
  - nested filter source work：实际 lane 1，断言 2；语义还是表示契约问题未决。
  - CTE multi-output：一 binding 两产物没有发布；与明确排除的 allocation
    reservation 问题重叠。本轮没有借机恢复 withdrawn patch。
  - nary sharing：不同预算下相同成本 78.25、不同 fingerprint；未 bless。
- SQL regress 全量未运行；不能宣称基线全绿或完成通用生产准入。

T3 不存在一个已准入的 T1+T2 组合版本。不能通过缩短预算或修改门槛造出
“合并验收通过”；最终只对撤回后版本做 fresh 4 handoff + 2 default 的完整性
复测（见后续收口记录）。180ms / 700ms、U≤12µs 和默认 parity 均未通过。

### 撤回后最终复测（不是探针性能的保留声明）

clean source `94ac26f4`，release build exit 0，binary SHA256
`81becabcb84c497ee349bd68a05323706033d7bf666f0b7217624932db84ddfe`。
`paro-tcwc-t3-final-*` 报告：handoff C1 **269.284ms**、p95 **271.355ms**、
W **95.777ms**；default C1 **1446.374ms**、p95 **1456.148ms**、W **102.938ms**。
仍是原预算和原默认策略，全部90行 typed/order 校验与 cold miss 通过。
诊断 admitted class/fingerprint 与 T1 control 相同。handoff 是质量政策停止，
default 是 BudgetLimited；均 SearchIncomplete。源码恢复后计数回到4903/20340。
不能把探针201.805ms写成最终生产能力，更不能将撤回后的批次漂移算优化收益。

状态登记：T0 测量能力已验证（严格零开销未证明）；T1 为 §6.3 NegativeResult，
其计数收益保留但生产契约未完成；T2 为 §6.2 NegativeResult，未实施指定重构；
T3 负结果/撤回复测已归档，**合并性能验收未通过**。不存在伪造的“全部达标”。

## 6. 唯一下一步建议

先量化并减少 `schedule_transformation_dependents → root_dispatch →
cached_negative_root_reads / PatternRead::is_current` 的重复依赖证据维护，
以“同有效读取版本不重复检查/重建观察”为可证伪机制；必须先测命中/失效和直接
时间，再做局部改动，保持闭包、规则及预算不变。
理由是两次独立采样均将约 17% optimizer 样本归到这条直接分支，且不随减少
grant 合成等比例消失。它不是已经证明的重复工作，更不是要求新增全局缓存。
本轮没有启动此下一方向，也没有扩展执行器、域身份或并行架构。

重放：使用 `PREREGISTRATION.md`，在干净 `3c74b4a1` checkout 下设置/不设置
`PARO_DIAGNOSTIC_LAZY_GRANT=1`；normal compile evidence 为1，handoff 为独立
开关。每份 report 的 configuration/launch_argv/source/build/harness 是最终
命令与身份的权威记录。`summarize.py` 可从 gzip raw 重新生成 summary.json。
