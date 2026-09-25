# Pipeline 收尾与执行性能收敛计划

状态：设计完成，实施未完成。核查基点：`d4a50344e`。
实施范围已获授权：第 6 步切换 pipeline 默认，但保留生产 Cascades 的显式入口。
当前已完成默认入口和功能迁移切片；详见
[默认切换交付](pipeline-default-delivery.md)。P1/P3/P4 与全部发布门仍未完成，
不能将本次切默认等同于整份计划已完成。
不认证未经验证的性能、不修改结果基线，也不清理历史材料。
仅考虑长期架构：一个生产规划入口、一个执行诊断来源、一套性能证据协议。

## 1. 结论与证据边界

继续采用规范化流水线、有限区域代价优化和一次物理选择，不回到全局
Memo 搜索。当前重点从编译均值转向执行工作量与迁移准入，但仍守住编译
尾延迟、取消和有限搜索预算；总编译占比小不代表每条查询都没有规划问题。

[上一轮交付](../../benchmark/evidence/optimizer/20260925/join-predicate-contract-v1/README.md)
提供的是探索性证据：

- 最终 pipeline 的 97 条有效查询 warm 中位数之和为 9,277ms；不是完整
  99 条语料耗时，也不是生产流量加权耗时。Q39、Q58 不能作为零耗时填入。
- 最终 pipeline 与较早 quality 全语料不是同一最终二进制的交错对照。
  历史 24.1s 到 9.3s、不同批次的比值不能认证本轮因果收益。
- Q51、Q67、Q78、Q47、Q23 是当前绝对超额时间的优先调查对象。
  按 SQL 特征归入“窗口/ROLLUP”的 61% 不是窗口算子的归因占比。
- 首次执行约 40ms 的残差值得调查，但 `median(C1)-median(W)-median(compile)`
  不是同一语句的阶段测量，也不能区分首次读页、解压与执行计划差异。
- Q72 扫描量仍大是真实线索；不存在“尚无范围过滤，所以新建范围过滤器”
  这一已证实前提。现有实现已有该能力，先查选择、发布、消费和跳页效果。

最终目标分别验收：PipelineReady、ExecutionImproved、FirstStatementParity。
PipelineReady 不要求先追平 DuckDB；DuckDB parity 也不能替代写语句、资源和
错误路径的正确性覆盖。有限区域规划完成不改名为全局 ProofComplete。

## 2. 源码核查带来的设计修正

| 已核实边界 | 实施含义 |
| --- | --- |
| [compile_explain.rs](../../crates/session/src/compile_explain.rs) 的 ANALYZE 执行目标并关联 execution receipt，但未安装 ExplainProfiler | 接通同次执行观测，不重复执行目标，不新增文本解析器 |
| [D6 collector](../../benchmark/corpora/d6_execution_profile.py) 明确返回 `operators=[]`、`profile_status=Uncovered` | capture 成功不等于已能定位执行热点 |
| [profiler.rs](../../crates/execution/src/explain/profiler.rs) 的 startup/total 是相对执行起点的首末时刻；事件保留当前没有固定容量 | 不能用 total 作为独占时间；不能直接复制整个 snapshot 到有界文档 |
| [frame.rs](../../crates/execution/src/operators/window/runtime/frame.rs) 已增量消费 append-only frame，但每次 delta 仍构造输入向量/chunk，逐结果 finalize/reset/get_value | 优先检查批次输入准备和结果搬运；不能再以“增量窗口”名义重复立项 |
| [RF builder](../../crates/execution/src/runtime/breaker/join_runtime_filter.rs) 已维护成员与范围；[scan](../../crates/execution/src/operators/scan/rowset.rs) 按 key 接收动态谓词；[column iterator](../../crates/storage/src/rowset/column/column_iterator.rs) 有页级范围排除 | 复用现有安全与生命周期契约，逐跳定位未生效处 |
| [implementation.rs](../../crates/optimizer/src/physical/implementation.rs) 的 join_key_distinct_expected 对多等值键返回 None | 需要 per-key/source 响应估计；不能把一个 key 的 NDV 冒充联合 NDV |
| [pipeline.rs](../../crates/optimizer/src/optimizer/pipeline.rs) 明确拒绝非 Query 的 QueryStatementLayer | 不能通过切回 quality 初始化数据而声称 pipeline 写路径已通过 |
| [order_binder.rs](../../crates/planner/src/binder/bind/clause/order_binder.rs) 的裸列首先查显式 alias，缺独立的隐式输出名解析 | Q58 的修复归 binder 输出命名空间，不归 join DP，也不应放宽结果比较器 |

## 3. 实施顺序与出口

这不是先做完大型观测工程再优化。每个纵向切片必须到达真实 SQL、类型化
证据和独立结果检查。正确性任务不依赖全部观测能力；失败先保存反例再修复。

| 任务 | 依赖 | 交付出口 |
| --- | --- | --- |
| P0 正确性与证据终态 | 无 | Q58、Q39、fixture 和 campaign 终态逐项裁决 |
| P1 有界执行摘要 | 无 | Q51/Q72 的同次执行 operator 摘要可由 D6 验证消费 |
| P2 窗口执行 | P1 最小摘要 | Q51 减少确定性工作量，正常样本验证；再分诊 Q67/Q47/Q57 |
| P3 首次执行关键路径 | 复用 P1 身份，不依赖 P2 | 同一请求直接阶段账本，随后只优化测得的主因 |
| P4 复合 RF 与剩余计划退化 | P1、最小配对工具 | Q72 实际跳页；Q05 定位；Q04/Q11 build ablation 裁决 |
| P5 规划功能与准入 | P0，可与执行优化独立推进 | 写入、检索、图、资源和错误路径全覆盖 |
| P6 默认切换、保留显式 Cascades | P5 + 同二进制全语料门 | 默认 pipeline，quality/budgeted 仍显式可选；无静默策略回退 |

单个开发流推荐顺序：P0 的最小反例 → P1 → P2 → P3 → P4 → P5 → P6。
P0 中需要较长数值证明的工作可以单独推进，不把所有正确性工作挂在 P1 后。
不为此自动创建更多 worktree、target 或后台性能进程。

### P0：先关闭真实语义与证据错误

1. Q58：为 ORDER 的完整裸列引用建立结果列命名空间，正确处理显式 alias、
   隐式列名、重复输出名、限定列引用及 quoted 标识符。保持 WHERE、GROUP、
   HAVING 的独立绑定规则；不要向所有 AliasLookup 广播隐式名称。
   以独立小 SQL 验证优先级、歧义、表达式内名称和 CTE/UNION 边界，再重跑 Q58。
2. Q39：保留严格结果差异，判断是袋/NULL/类型/ORDER 错误还是浮点归约合法
   差异。后者需要同一 SQL/data/类型合同的独立有界数值认证；不得加全局 epsilon，
   不得借用旧构建的认证，也不得为了匹配 DuckDB 改写 SQL 数学语义。
3. 逐项裁决 TPC-H 六项静态/numeric fixture 失败和 regress 三项差异。
   两条 Paro 路径一致不是独立 oracle。仅在另行授权、预期独立验证后更新 expected。
4. 修共享 RunOutput 的容量失败终态：manifest Incomplete 时不能留下 summary
   Running；保留原始错误和已完成 attempt，不重写旧失败包来制造成功证据。
5. 保存并复现旧 spill 的 `Block handle is None`。新计划不再 spill 不算修复；
   用受控低内存强制走原生命周期，检查取消、释放和回放。

### P1：执行摘要，先摘要后有界细节

复用 ExplainProfiler 和现有 Compile/Execution receipt，不建立第二个 profiler、
第二套 D6 JSON 语义或新的环境 exporter。执行层生产数据，context 只承载无反向
依赖的类型，session 负责生命周期，benchmark 校验并投影。

- COMPILE ANALYZE 只执行已编译目标一次；profile 绑定实际 ExecutionReceiptId、
  admission、selected image 和 runtime occurrence。logical id 缺失保持缺失，
  不拿 runtime ordinal 伪造它。不能修改已 sealed compile 内容补写运行时事实。
- 普通无 ANALYZE 路径不启用逐算子计时和事件。ANALYZE 摘要按实际算子/阶段
  累加整数 duration 与计数；不先保存事件再求和。统一普通 ANALYZE 与 COMPILE
  ANALYZE 的来源，renderer 不是统计语义 owner。
- 分开 `first_start_offset`、`last_end_offset`、调用 elapsed 总和、显式等待时间、
  rows/loops、spill bytes 和扫描读/解码/排除计数。调用 elapsed 不是 CPU time；
  多 worker 总和不是 wall time；嵌套 finish phase 不与外层重复相加。
- 保留 pipeline/work occurrence 的作用域。输入/输出计数分 port；breaker sink 与
  source 不相加冒充输出基数。NULL/零与 Uncovered 不混淆。
- 容量按活跃 collector、worker-local buffer、共享汇总、snapshot 和编码全链路
  准入，计入现有诊断预算。Summary 不保存原始事件；Detail 只保留有上限的样本，
  溢出记 omission。禁止现有无界 events.clone() 后才裁剪。
- 成功、失败、取消、背压和放弃结果均有终态；不因为诊断容量不足改变目标计划。
  不能声称输出文档测到了它自身后续发送、客户端解码或 commit。
- 首个验收只需 Q51/Q72 及空输入、并行、spill/取消、容量溢出的真实入口测试，
  另加已知时序的计数/非重叠计时测试。先完成这个切片，禁止无限扩展通用事件框架。

### P2：窗口准备与结果批次化

先用 P1 区分排序、分区/peer 构造、表达式准备、aggregate update、finalize、结果
scatter。Q67 的 ROLLUP、Q47/Q57 的排序或 join 不能仅凭 SQL 名称归为同一根因。

1. 按窗口共享规格复用排序/分区和表达式输入；保持 volatile、FILTER、求值次数与
   错误边界。不能因为某个 aggregate 的 FILTER 不同而丢弃其他 aggregate 输入。
2. append-only 路径复用有界批次 workspace 和选择视图，避免为每个一行 delta
   新建向量/chunk。只复制确实必须拥有的值，不把整个大分区重复物化。
3. 不改变 bound aggregate ABI 的 observational finalize 契约。重复 peer frame
   只有在相同参数/frame/结果所有权得到保证后才复用 finalize 值；变长结果不可
   悬挂引用被 reset 的 scratch。每行需要不同前缀结果时不能“整批 update 后
   finalize 一次”。通用 moving frame 保留安全路径，不猜逆算子。
4. 若需 prefix kernel，以 aggregate capability 注册和共用测试落地，不按 SUM/MAX
   名字或 Q51 SQL 分流。不凭一次微基准新增另一套聚合语义。
5. oracle 覆盖 ROWS/RANGE、peer、NULL/空前缀、FILTER、chunk 边界、Decimal
   溢出、浮点、变长值、取消和析构。验证 update 行数及分配次数的复杂度，避免只看时间。

### P3：直接测首次执行，再优化最大项

复用当前 admission/lowering/pipeline 的真实边界，增加固定大小摘要，不复活
逐事件 statement trace 档案。区分编译前 catalog/statistics、admission/reservation、
lowering/image、初始化、首次取数、排空及事务/协议结束。存储和 worker 子项是
嵌套工作维度，不与父 wall interval 再相加。

- `pipeline initialized` 不等于第一批输出；first result 不等于整条语句完成。
  下层无法观测客户端 drain，需保持服务端与客户端时钟/范围独立。
- 按同一次执行收集页填充、读入字节、解压/解码工作、缓存命中和等待；不通过
  冷热样本差值分摊各项。不同来源争用的 shared page 只向正确 owner 归因。
- 选择 Q11 与库存扫描等代表查询，以及已注册的小查询组。Fresh process 只定义
  进程/引擎冷态，不声称 OS page cache 冷。禁止通过预读目标或移出 timer 达标。
- 观测开销需要独立测量；有重型采集的执行不能充当正常 C1。只对直接证实的
  关键路径主因实现下一项优化，可能是页读取、解码、调度或初始化，而非预定
  “线程池预热”答案。优化后在相同计划、资源和数据身份下复测。

### P4：RF、Q05 和外/反连接

Q72 先沿以下链条记录 bounded 决策与实际计数：
合法键/source → 两方向成本 → RF 选择 → build 完成/发布 → scan 安装 →
segment/page 排除 → 实际读/解码行数 → join 输出。

- 单列范围是复合键集合的必要条件，不是复合元组精确成员集合；不能用其证明
  join 等价、跳过残余，或把 per-column NDV 相乘当联合 NDV。
- 估计响应按 key/source 表达，未知相关性保守处理；同一 source 减少的工作
  不重复收费。统计选择性不承诺物理聚集带来的跳页率。
- 复用现有 RF builder、publication、scan predicate 和 zone map owner。
  对迟到、no-wait、空 build、NULL-safe equality、NaN、cast/比较序和 outer/anti
  保留侧逐项验证；特别不能过滤本应保留的 unmatched 行。
- 只有生效路径已通且计数证明减少读页后，才谈 inventory 家族 C1 收益；不能
  把“计划显示 Range”当成“跳过 80% 数据”。
- Q05 单独比较同二进制两 policy 的实际计划和工作，不预设复合外连接原因。
  Q78/Q75 先排除共同的多余匹配/物化、build 宽度与 spill，再优化哈希 kernel。
- Q04/Q11 先做受控 build-side ablation。若确认决策收益，先修统一成本/source
  response；只有反复不确定且资源合同允许时才设计 breaker 后定向。该机制必须
  解决保留两侧、调度无环、NULL/outer语义和内存峰值，不能当成廉价自动回退。

### P5/P6：功能准入和唯一生产入口

- 将 StatementPlan 的写层接入 pipeline 现有物理合同：INSERT/SELECT、UPDATE、
  DELETE、约束、Halloween 屏障及支持的 COPY/utility 路径。RETURNING 必须先
  核对 parser 的支持域；物理 WriteContract 有 returning 字段不代表 SQL 已支持。
  不以切换规划默认之名悄悄扩展 SQL 语法。utility
  共用 lowering 不等于 DML 已支持。必须从建表/写入起全程 pipeline 跑 regress，
  不静默调用 quality。测试 auto/explicit transaction、回滚、取消、失败后重用。
- 检索/图验证合法 access path 与 fallback、评分/统计快照、残余、overlay 和输出
  契约；并非必须每个查询都用索引。图有其合法区域决策，不强塞普通 join DP。
- 一个规划方案不免除资源合同：内存不足的 spilling、不可 spill 的失败、低 DOP、
  资源变化和取消都必须可解释。禁止靠猜测另一个 grant winner 回退。
- 两套 DP 的遗留覆盖按区域语义收口：共用图、谓词归属、估计与物理响应；每个
  区域有唯一 planner owner。必要特殊区域显式标记，不能隐藏第二轮全树重排。
- 默认切换前列举生产依赖，direct 与 Cascades 继续共用 identity/事实/验证合同。
  本轮不删除 Cascades；quality/budgeted 是显式策略，而不是 pipeline 遇错后的
  隐藏 fallback。session 默认和无 setting 的底层默认必须来自同一个 typed 值，
  SET/RESET、缓存键及 receipt 均保留实际策略。测试保留小域独立穷举 oracle，quality 从未被认证为
  全局最优，不能把 quality regret 叫 optimality gap。

## 4. 测量与发布门

复用 maintained benchmark/RunOutput/receipt validator。当前 corpus-impact 已存在，
只增强必要字段，不重复造排名脚本。每次只读审核 manifest 的可比性再作裁决。

### 配对方式

- warm 策略实验可同进程使用独立 session/cache 命名空间，并随机平衡 AB/BA 或
  ABBA 顺序。先证明 setting 进入缓存键、两臂实际 policy/selection 与 receipt
  一致；分别预热到注册状态。预热/编译不计入 warm。
- 共享进程仍会有缓存、allocator、RF/CTE materialization 和后台负载 carry-over；
  记录并用独立进程块交叉验证，不能宣称交替就“消除了噪声”。DuckDB 独立进程
  但同批轮转，不能与 Paro 使用不同资源或缓存定义。
- C1 始终是 fresh process 的目标 occurrence 0/cache miss；不得套用 warm
  交替方式。所有正常数据 trace-off，诊断独立。失败、timeout、slow sample 保留。
- 扫描 99+22 的探索性筛查可继续定位其他失败，但不能据有失败的包作正式推广或
  parity 认证。固定包容量，超额查询分预注册 campaign，禁止静默丢慢查询。

### 明确的准入规则

性能数字在确认性采集前随 source/binary/harness/SQL/data/metadata、DuckDB 1.5.5
及扩展身份入库；下面是候选发布政策，不是已通过或伪装预注册的历史结论。

1. 正确性：全部受支持查询/协议/写入/资源场景独立合同通过。允许经独立审计的
   明确数值合同，但不能有未裁决 wrong result 或将 Unsupported 计为 Pass。
2. pipeline 对 quality：建议 warm 几何均值比单侧 95% 上界 ≤1.02；关键单查询
   ≤1.20，超过不得用其他查询收益掩盖。接近计时噪声的小查询使用采集前定义的
   绝对差 SLO。另守住等频总时间和最慢查询，不能只看几何均值。
3. 资源：相同内存/DOP，峰值、spill、错误率和取消无未解释退化。结果不同、超时
   不进入纯成功样本平均值伪装非劣；它们单列为阻塞或正式例外裁决。
4. 编译：维持现有有限区域预算和同机配对回归；p50/尾部预算按查询规模注册。
   普通共享 CI 使用确定性工作量/复杂度门，绝对毫秒门放在受控性能机器，避免
   因宿主竞争把正确性 CI 变成随机失败。不得移走校验/准入或改 timer 缩窄口径。
5. 覆盖：TPC-DS 99 + TPC-H 22 不是所有 SQL 功能。写/检索/图/事务矩阵和精确
   反例同为门。正确性通过后才能跑用于发布裁决的性能确认集。
6. parity：单独注册同版本 DuckDB 的 C1 比值和尾延迟，不从 PipelineReady 推导。
   样本功效、block 数、排除规则在采集前确定；两块 pilot 不算发布证据。

## 5. 完成条件与停止无效路线

每个主题交付代码、独立反例、真实入口验证、bounded evidence 和未关闭清单；
先比较确定性工作量，再比较正常时间。没有工作量变化且正常样本无收益时，
不扩建缓存/调度框架；保留负结果，撤回仅为加速而加的复杂路径。

P1 能产生可信摘要就推进执行优化，不等待所有事件可视化。P2 不阻塞正确性和
写路径。P3 不先选“预热”方案。P4 不以未证实的 Q04 build 假设阻塞 Q72。
不引入查询 ID 特判、不放大搜索预算、不恢复质量形状计数门、不改性能口径。

本计划完成不代表上述实现完成。默认切换单独验证，C2/F2、PipelineReady、
FirstStatementParity 均需相应新证据；旧测试通过记录不能替代当前源码验证。
