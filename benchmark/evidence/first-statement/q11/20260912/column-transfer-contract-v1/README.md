# Q11 selected-path column-transfer contract (2026-09-12)

本轮只验证一个假设：`quality_domain` 的 selected-path binding 与 native domain
rewrite 对 Projection、Aggregate、`UNION ALL` 和 Join 边界使用不同列命名空间，
会使路径发现提前停止；两者应共享同一个算子级列传递契约。没有改执行器、并行
搜索、全局优先级、搜索预算或默认停止策略。

## 代码与输入身份

- repository HEAD: `5a1ca846c5d6c295964ba63489e471b45bbe7c94`, working tree dirty；
  报告明确记录了混合工作区，不能把它当作干净 HEAD 的对照。
- binary: `target/release/parod`, SHA-256
  `01c829c982d9e83ccf4337452b56b92cac32ebfc1e17ae7a3fe2c80295bfb016`。
- SQL: [`11.sql`](11.sql)，SHA-256
  `1f3c2697b2a82f597e9f8a570413be65ab7a02da03c1fe4c3ea7b725735433b8`。
- Paro data seed SHA-256:
  `72ccb3dc02ce127f61c06b85920a0e75c17a55a60ee341d267251c67611f62b1`。
- DuckDB: `tpcds-sf1.duckdb`, SHA-256
  `568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`。
- 4 execution threads, 2 GB limit, one target statement per process；normal 为
  fresh process、cache miss、trace-off；handoff 是独立 diagnostic policy，不能
  代表默认生产策略。

## 生产入口反例和实现

新增的生产入口测试从真实 Memo 构造 selected `FrozenCandidate`，经过
`selected_transfer_bindings`、native closure、staging 和候选消费；不是直接调用
native shell helper：

- Projection → Aggregate；
- `UNION ALL` 的两个分支按各自 output layout 重绑定；
- Aggregate 位于合法 Join 一侧；
- transparent projection 缺少 lineage、Aggregate 输出残余、NULL/重复值和
  `SUM(x-y)` 等边界保持 fail-closed。

`domain_transfer.rs` 现在是两条路径共同调用的契约。它返回每个 child 已重绑定的
谓词以及仍留在当前输出命名空间的 `remaining`。selected binding 只有在本层
没有未解释 residual 时才声明 exact path；native rewrite 不再用 ordinal 重新解释
foreign namespace。Outer join、`DISTINCT`、evaluation fence、graph/control 和
缺失布局/lineage 不被强行跨越。

## Fresh Q11 结果

以下都是同一当前 binary、同一 SQL/data/resource envelope 的 2 个 fresh blocks，
90 行、typed schema、顺序、结果摘要、cache miss 和 normal trace-off 均通过。
CI 是按 fresh-process block 重采样的 95% 区间；样本量仍是 pilot。

| 策略 | Paro C1 p50 (ms) | DuckDB C1 p50 (ms) | ratio (95% CI) | Paro warm p50 (ms) | DuckDB warm p50 (ms) | warm ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| handoff-on（诊断策略） | 249.695 | 109.613 | 2.279005 [2.224684, 2.334652] | 179.126 | 103.529 | 1.726473 |
| default（生产策略） | 1674.032 | 121.539 | 13.802703 [13.206, 14.426] | 112.277 | 111.080 | 1.000660 |

两份完整报告分别为 [`handoff-v1.json.gz`](handoff-v1.json.gz) 和
[`default-v1.json.gz`](default-v1.json.gz)。本轮没有达到 M1（C1 ≤200 ms），
更没有达到 M2/parity；warm 结果不替代 C1 gate。handoff 仍是
`QualityPolicySatisfied + SearchIncomplete`，default diagnostic 为
`BudgetLimited`；没有样本可称 `ProofComplete`。

## 同次诊断账本

handoff diagnostic 的当前 selected root 为 logical `87` / physical `145`，
35 个 choice，第一层 child group `57`/`72`、candidate `748`；冻结前已验证同一
候选的 child choices/payload。关键事件按搜索时钟记录如下：

- `quality_policy_satisfied_us`: `26,724`；`quality_policy_candidate`: `802`；
  `search_stop_profile_us`: `26,759`；`search_return_profile_us`: `26,781`；
- direct selected binding dispatch `1`、work units `4`，first/last dispatch
  `23,328`；producer dispatch `13`；
- freeze `1,784` us；runtime image `14` us；compiler return 时 elapsed
  `38,949` us；lower/admit `427` us；pipeline init `44` us；first page
  `217,030` us，fetch/drain `217,801` us；
- child-combination synthesis/new `1,221`、recompute `176`、frontier recheck
  `0`、budget rejection `0`；fact revalidation hit/miss `0/2`；working set
  `812,371,264` bytes。

default diagnostic 没有 direct binding（`0/0`），search profile `1,438,887` us，
freeze `315` us，child synthesis/new `20,971`、recompute `4,139`、fact
revalidation miss `1,272`，task-registry reuse `50,243`，working set
`45,713,728` bytes；其 search stop 是 `BudgetLimited`。这些是单次 diagnostic
账本，不把嵌套区间相加，也不将 tracing 时间解释为 normal C1。

历史 `necessary-domain-v1` 曾记录 `25` 个 direct bindings/`97` work units，
但它的 evidence identity、源码工作树和 binary 不同；本轮只能说当前实验观察到
`1/4`，不能把 25→1 归因于本契约 patch。当前契约测试和 fresh C1 也没有显示
可归因的性能收益；它收口的是列命名空间正确性和生产路径一致性，而不是重复
传播/事实维护/物理定价的已证削减。

## 验证结论

- 反例成立到可执行的生产测试层：旧的独立 native closure 测试不能覆盖真实
  binding；新增测试覆盖了真实 Memo/FrozenCandidate → staging → consumption。
- 统一契约后，发现路径和改写路径交叉使用相同的 output layout、child binding、
  residual 和 fence 判定；错误 ordinal/foreign namespace 会拒绝，而不是伪造
  可传播证据。
- 本轮没有证明 Q11 direct dispatch、事实更新或物理成本合成减少，也没有证明
  C1 改善。下一主耗时仍应定位 selected path 的父级 physical consumption / fact
  lifecycle，而不是继续堆列传递适配器。

## 测试

通过：`native_domain` 10 项、`quality_production` 7 项、`domain_` 39 项、
`make -C benchmark test` 126 项；production selected-binding 测试 3 项也已
单独通过。`cargo build --release --locked --bin parod` 通过。

`cargo test -p paro-optimizer transformation --locked -- --test-threads=1` 当前
为 69 passed / 1 failed。失败是混合工作区已有的
`cte::tests::engine_admits_every_partition_discriminator_from_one_binding`，在
`cte.rs:583` 的 two-output reservation 回滚断言；不是本轮新增生产 binding 测试。
没有静默 bless 该失败。

