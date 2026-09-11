# Q11 per-region aggregate coverage and local quality follow-up

本轮只修复同一 Memo 内的逐区域质量见证和局部依赖调度。没有引入跨
Memo seed、NoPlanBelow、并行搜索、Q11 特判或新的默认停止策略。

## 实验身份

- SQL：原始 TPC-DS SF1 Q11；使用
  `benchmark/evidence/first-statement/q11/20260911/incremental-pricing-v1/q11.sql`。
- binary：release `parod`，SHA-256
  `fe791a0974831df175523721c01b79c93a4df02d4cec54e39794c631890f3727`。
- source：`bae0d1c739a66e2541eae7be342320c7858cc434`，报告明确记录
  `dirty=true`；本报告不是干净提交的正式里程碑样本。
- harness：binary protocol，4 execution threads，planning DOP 1，2 GiB，
  `metadata-track=none`；normal cohort 为 fresh private-copy-per-process、
  trace-off、cache-miss；diagnostic cohort 单独 trace-on。
- seed：`acb0fea493d1afc186185c44a3861c69b5a9b4e0f33bf87f82bfa223d22101dd`。
- JSON SHA-256：
  `2f886d4c06f2c2d089ddce950b01e5f348b5cdd1e4aabe3ec7b713597865a280`。
- diagnostic log SHA-256：
  `162ee0ba332fa4749f7742e59779fd5a4ef1c476c791cf8857acc494c1c3ff9c`。

命令使用 `tpcds_compare.py`，`--warmups-per-process 1`、
`--process-blocks 2`、`--diagnostic-process-blocks 1`、4 threads、2 GiB、
`--paro-result-format binary`；完整 argv 和 harness 身份保存在 JSON。
本轮是针对性 fresh pilot，不是 M1–M3 的正式样本量。

## 质量断点和实现结果

旧的全局 `aggregate_witness` 已不再作为质量证明。planner-owned provider
现在从当前待交接的完整 `FrozenCandidate` 精确遍历 UNION ALL 的每个相关叶
区域，见证绑定以下内容：

- region path、当前根/区域 CandidateId 和 anchor CandidateId；
- 当前 Memo choices、物理 payload/child refs；
- 该区域的 logical/statistics fact fingerprint；
- 只有选中 DAG 中真实存在的 partial aggregate → join → final aggregate
  merge contract 才标记 `covered=true`。

`applied_rules` 只保留为审计数据。质量事实来自选中表达式的 equivalence
proof、精确物理合同和当前事实读集；尝试但空输出、预算拒绝、未被当前候选
消费或另一分支完成的规则不能认证本候选。事实读集进入质量评估 key，事实/统计
版本变化会重新评估同一个 CandidateId。

局部调度只沿当前物理交接形成的精确选中 DAG 推进：先把实际观察到的物理目标
重新入队，再提升该候选及其子树的质量依赖规则，最后由祖先组合消费；不再全局
提前调度 DimensionSharing。所有未选中的合法 alternatives 和未完成义务仍保留。

### 真实时间线

单位为 diagnostic trace 的微秒，来自同一次 SELECT；质量 trace 不是 normal
C1 计时：

| 事件 | 时间 |
| --- | ---: |
| first aggregate-region witness（候选 153，0/2 覆盖） | 9,849 |
| first complete two-region aggregate root（候选 6074） | 915,740 |
| quality policy satisfied（候选 6084） | 917,448 |
| search profile stop | 918,188 |
| actual stop | 919,289 |
| compiler return | 958,190 |
| execute entry | 958,347 |
| first 90-row result chunk | 1,089,143 |
| fetch drain | 1,089,652 |

该时间线把第一条区域的完成和两条区域都可交接明确分开。第一条断点仍在
第二个 UNION 区域的 transformed aggregate DAG 没有及时被根物理组合消费，
不是 freeze/extraction：此前同条件 diagnostic 的 freeze/handoff 约为 1 ms，
而本报告的完整双区域见证已在约 916 ms 才出现。

### 选中 DAG

最终交接候选为 root group 30 / CandidateId 6084，UNION group 31 / candidate
6067。两个相关 arm 都实际包含 aggregate → comparison join → partial
aggregate/final merge，并保留 runtime-filter join：

- arm 1：aggregate group 725 / candidate 4564，join group 724 / candidate
  4553，partial aggregate group 745 / candidate 4551；
- arm 2：aggregate group 846 / candidate 5720，join group 845 / candidate
  5709，partial aggregate group 868 / candidate 5705；
- root diagnostics：2 aggregate-region witnesses、4 aggregate nodes、7 joins、
  2 runtime-filter joins、1 materialized CTE；这些是观察值，不是质量门槛。

每个 choice 的 exact child refs、logical/physical payload、physical
fingerprint、selected proofs 和 fact/statistics reads 都在 JSON 的
`final_winner_2` trace 中。单个 arm 的 witness 不能满足另一个 arm；完整 PReady
只在两个 region witness 同属当前 CandidateId、choices 和事实快照时成立。

## C1/W 对照

这是 2 个 fresh normal blocks 的 pilot；C1 包含真实 parse/bind、规划、冻结、
交接、执行和客户端取数，trace-off 且 cache miss。完整 typed schema、90 行结果、
顺序和 digest 均通过。

| track | Paro median / p95 (ms) | DuckDB median / p95 (ms) | ratio / 95% CI |
| --- | ---: | ---: | ---: |
| normal C1 | 1089.655 / 1100.014 | 118.769 / 125.237 | 9.187823 / [8.618002, 9.795320] |
| warm/steady | 93.420 / 108.349 | 104.635 / 106.284 | 0.921237 / [0.879381, 0.987968] |

质量证书只说明当前候选满足声明的质量包，不说明完整逻辑/物理搜索已经完成。
本样本的 `search_actual_stop_us` 为 919,289，搜索停止前状态为质量政策交接，
仍存在未探索义务；没有 `ProofComplete`。因此阶段目标 C1 ≤200 ms、M1/M2/M3
和 DuckDB 冷首语句 parity 均未通过。

## 验证

- `cargo build --release -p paro-server --bin parod --locked`：通过。
- `cargo check -p paro-optimizer --lib --locked`：通过。
- `cargo test -p paro-optimizer --lib cascades::quality::tests --locked`：11
  passed。
- planner 选中 proof 测试：1 passed。
- matching tests：11 passed。
- optimizer lib 全量：1134 passed，4 failed；失败均来自工作树中已有的
  statistics-cache、nested-filter source lane、CTE partition shape 和
  dimension-sharing 回归，未纳入本任务，也未修改以掩盖问题。
- normal harness：结果/schema/order/cache-miss/trace-off 校验通过。

## 结论和下一步

逐区域见证和局部选中路径调度已修正了错误认证，并把完整双区域候选从先前约
1,124 ms 的同类诊断推进到约 916 ms；但它没有把高质量计划推进到几十毫秒，
normal C1 仍约 1.09 s。当前唯一推荐的下一项是继续沿同一 Memo 的精确 DAG，
定位第二个 arm 从已生成到根组合消费之间的物理组合/等待边界；不要重新扩大成
全局 quality lane、跨 Memo seed、NoPlanBelow 或并行搜索。
