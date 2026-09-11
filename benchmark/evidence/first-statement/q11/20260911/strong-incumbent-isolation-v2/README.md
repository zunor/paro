# Strong incumbent isolation v2

日期：2026-09-11

本轮只验证 strong incumbent 实验的两个变量是否被隔离：

1. 是否把源 winner 的逻辑树注入目标搜索 Memo；
2. 是否把同一 SeedPlan 在目标上下文重计价后作为强上界。

这不是 10 ms、C1 parity 或 `ProofComplete` 验收。诊断 cohort 的 trace 只用于归因，不能替代 normal C1。

## 身份与条件

- Paro binary SHA-256：`f3b1d42ef5cf5775fb46ca3df6ce7920af199120b7ea6392783fd46a56f7bc23`
- binary 构建：`cargo build --release --locked --bin parod --jobs 4`
- 源基线 commit：`a45ca7d5a27e66b7e00d6e56e3a59880136adabd`；构建时工作树包含既有未提交修改，本报告不把它们归属于本轮。
- SQL corpus SHA-256：`a1f151c2d5617427a9394f1353d47478682e4d0b79bc16cddcd8be4de438d503`
- Paro data SHA-256：`acb0fea493d1afc186185c44a3861c69b5a9b4e0f33bf87f82bfa223d22101dd`
- DuckDB SHA-256：`568e6c75c0c96a3ba82d6f24e8d515dcf2eb3a85640418c1702c2f9ed6fd8ed7`
- source CSV SHA-256：`07209980c5b556acbd1d1ea75c25319cc02cf954cb9ab3f5c09423611b13bb7d`
- DuckDB 版本：`1.4.4`
- 资源：4 threads、2 GiB、单并发 target statement；规则、预算、成本模型和默认停止策略均不变。
- 每组：5 个 fresh-process blocks；normal trace-off/cache-miss，另有 1 个独立 trace-on diagnostic block；完整 typed schema、90 行结果、顺序和摘要校验。
- 结果摘要：`9108a5b43530c24778abc0d8d3bf1ee218a9815eb0d15310bb4d7c6bcc5f2c78`
- harness：`tpcds_compare.py` `115ac52f63c5f626b9642d2c2250ad87f83c66696190640b777a27afe7160c8a`；其余 harness 文件的哈希记录在每份 JSON 的 `harness.files` 中。

四组都保持 `PARO_CERTIFIED_GROUP_PRUNING=1`，因此认证剪枝策略本身一致；A/C 没有 strong seed，B/D 才安装强上界。

| 组 | 目标逻辑树注入 | 目标强上界 | 说明 |
| --- | --- | --- | --- |
| A | 否 | 否 | ordinary control |
| B | 否 | 是 | 纯上界隔离 |
| C | 是 | 否 | 纯逻辑注入隔离 |
| D | 是 | 是 | 两变量同时开启 |

报告文件为：`q11-{A,B,C,D}-v14.json`；同名 `.parod.log` 为对应原始日志。v11–v13 是重建过程中的中间诊断，不是本归档的正式四组结果。

## Normal C1 与 warm

单位为 ms；C1 是实际 fresh process 的 parse/bind、编译、优化、验证、冻结、交接、执行和取数全路径。括号内为 C1 ratio 的 hierarchical 95% CI。

| 组 | Paro C1 p50 / p95 | DuckDB C1 p50 | C1 ratio（95% CI） | Paro warm p50 / p95 | DuckDB warm p50 | warm ratio（95% CI） |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A | `1592.500 / 1624.082` | `114.145` | `13.775` (`13.215–14.139`) | `100.377 / 108.179` | `107.341` | `0.936` (`0.917–0.956`) |
| B | `3262.342 / 3292.106` | `112.600` | `28.762` (`28.238–29.169`) | `99.342 / 106.624` | `107.069` | `0.928` (`0.910–0.944`) |
| C | `15214.467 / 16203.342` | `112.718` | `134.940` (`130.772–138.056`) | `100.232 / 123.854` | `107.637` | `0.934` (`0.900–0.963`) |
| D | `15404.529 / 16513.521` | `114.817` | `135.744` (`134.172–138.086`) | `100.681 / 104.519` | `107.951` | `0.922` (`0.898–0.943`) |

C1 的每个样本仍在 JSON 中；例如 Paro 冷样本为 A `1699.601, 1673.073, 1634.063, 1606.302, 1751.536`，B `3297.439, 3160.890, 3491.680, 3790.836, 3308.100`。C/D 的完整数组、DuckDB 配对和 p95 不以诊断 replay 代替。

四组均通过完整 typed schema、90 行结果、顺序和结果摘要校验；C/D 的源阶段和目标阶段均不是 fresh-process 之间的共享缓存，且 normal C1 包含源种子生成和目标重规划的真实工作。

## 目标搜索工作

诊断 trace 的时间只在同一阶段内解释，不能把嵌套的 source、target、freeze 计时相加当成新的 C1。`target_total` 包含 target search/reprice/freeze/handoff 的嵌套区间；`search_stop_profile` 是其中的搜索 profile。

| 组 | target groups / logical / physical | child synthesis / recompute / frontier recheck | target search profile / actual stop（µs） | freeze / handoff（µs） |
| --- | ---: | ---: | ---: | ---: |
| A | `873 / 1461 / 2592` | `17667 / 5376 / 0` | `1383133 / 1383954` | `331 / 622` |
| B | `873 / 1461 / 2592` | `17667 / 5376 / 0` | `1340919 / 1344495` | `298 / 615` |
| C | `4093 / 6909 / 11613` | `67409 / 19386 / 0` | `13140078 / 13140996` | `557 / 930` |
| D | `4093 / 6909 / 11613` | `67409 / 19386 / 0` | `13763352 / 13765190` | `547 / 969` |

所有组的目标搜索都是 `BudgetLimited`，不是 `ProofComplete`；实际停止和 timeout tail 也在 JSON/trace 中保留。认证剪枝在四组都没有真正省工作：`certified_bound_pruned_before_children=0`、`...after_children=0`、`certified_recipe_prune_count=0`。A/B 的 bound check/compute/validation 是 `36 / 20.192 ms / 17.477 ms`（B 为 `19.838 / 17.284 ms`）；C/D 是 `36 / 79.352–83.149 ms / 67.214–69.465 ms`。

### 目标域扩张的已证原因

在修复前，source search 与 target search 共享 binder 的 plan-id allocator。即使不注入逻辑树，source 阶段产生的新 plan IDs 也会改变 target 的后续候选身份/插入顺序；同条件 debug 目标域为 `1020 / 1665 / 2910`。改为独立 plan-id namespace、目标保留原始计划后，B 回到 A 的 `873 / 1461 / 2592`，且最终 winner 完全一致。

C/D 的 `4093 / 6909 / 11613` 则是显式 logical injection 的真实代价，不是强上界本身：源 winner 的逻辑 shell 被作为新的 target alternative 加入 Memo，再次参与逻辑/物理搜索。C 与 D 的域和组合数相同，说明 D 的强上界没有缩小这段扩张。

## 上界生命周期

B 的固定 SeedPlan 在独立、非搜索 pricing Memo 中重计价 3 个 grant，随后 target 安装 3 个 `PricedIncumbent`；D 走同一实际 grant 安装契约。源 SeedPlan 不是带成本的上界，目标成本只有重计价并通过事实/上下文校验后才可安装。

| 组 | source seed / isolated reprice | 安装 | lookup_total / missing_key / unrelated_key | valid / invalid / selected | active 最终值 |
| --- | ---: | ---: | ---: | ---: | ---: |
| A | `0 / 0` | `0` | `0 / 0 / 0` | `0 / 0 / 0` | `0` |
| B | `3 / 3` | `3` | `8874 / 8874 / 8874` | `0 / 0 / 0` | `0` |
| C | `3 / 0` | `0` | `0 / 0 / 0` | `0 / 0 / 0` | `0` |
| D | `3 / 0`（目标安装重计价 count=`3`） | `3` | `33685 / 33685 / 33685` | `0 / 0 / 0` | `0` |

B 的第一个目标安装在 `102 µs`，ReadSet 为 `31` 个事实依赖；第一个失效观察在 `7379 µs`，原因码 `1`（ReadSet facts 变化）。D 分别为 `838 µs`、`37` 个依赖和 `20144 µs`、原因码 `1`。安装时首个目标 grant 的成本为 expected `129592973.98`、upper `291584550.00`；B/D 的 target priced cost-context、plan identity 和 ReadSet 见 trace。

- 上界 map 的请求没有到达已安装的 root `(GroupId, Goal)`：B/D 的 lookup 全部是 `unrelated_key`，没有 group/goal mismatch、valid hit 或 selected-for-bound。
- 因此 `active_count=0` 是事实失效后的最终快照，而不是“lookup map 没有计数”；`lookup_total` 在 map 查询前计数，空 map 的 A/C 不被计入 lookup。
- `strong_incumbent_bound_request_count` 仍记录了认证界请求：A/B `70813`，C/D `379209`；它不能被误读成 strong upper bound 已经命中。

认证界原因计数是可重叠类别，不可相加为总工作。A/B（无逻辑注入）为：无 incumbent `2520`、局部区间不确定 `11117`、子任务未完成 `1457`、source response 不支持 `11607`、phase overlap 不支持 `47945`、界可用但不够紧 `36`。C/D 为：`9345 / 44000 / 4906 / 41952 / 292954 / 36`。这把“未请求到上界”“上界已失效”“下界准入不完整”和“界不够紧”分开；实际剪枝仍为零。

## 计划质量与 warm 翻倍复核

A/B 三档最终 winner 的 physical fingerprints（`hi, lo`）分别为：

```text
grant 0: (107244597052445314, 5900850447593414200)
grant 1: (13445193664689644239, 18130760894026177507)
grant 2: (13191148240871974889, 1063598273783278187)
```

C/D 三档最终 winner 的 physical fingerprints（`hi, lo）分别为：

```text
grant 0: (17190698190933454811, 14234376063281668538)
grant 1: (16527204373249046205, 502771430502908141)
grant 2: (17526469700273209052, 5494791203655107083)
```

A/B 的三个 final model cost score expected 都是 `9422225.236788`；C/D 都是 `9422433.486788`。四组的最终 shape 都是 `37 nodes / 6 gets / 9 filters / 4 aggregates / 7 joins`，都有 `4` 个 runtime-filter contracts 和 `1` 个 spill contract。源 seed 的 physical payload fingerprint 为 `hi=3020094472269971719, lo=7831599373275533599`，源 expected cost 约 `139010201.236788`；它不是当前最终 winner，且 B/D 首个目标重计价 expected 约 `129592973.98`，明显高于目标最终 winner 的约 `9.42M`。所以本实验不能把这个 seed 称作“全局最优”。

v14 没有复现 v9 的 warm 翻倍：四组 warm p50 均约 `99–100 ms`，C/D 没有回到约 `208 ms`。计划 manifest 显示四组都保留 4 个 RF contract 和 1 个 spill contract，未发现“RF=0”或显式 spill contract 数量变化这一类退化。C/D 与 A/B 的 physical fingerprint 仍不同，而本轮没有 D6 级别的扫描/解码/任务等待剖面，因此 v9 翻倍的唯一 runtime 微观原因不能从本实验确定；该未决项不被完整结果一致性掩盖，也不被猜测归因。当前可确认的是：修复共享 plan-id 污染后，目标搜索域控制和 warm 退化都不再按 v9 方式出现。

## 结论与下一方向

1. 强种子本轮没有减少真实搜索工作。B 与 A 的 target 域、synthesis、最终 winner 一致，但 B 额外支付约 `1.64 s` source seed search；D 与 C 的域、synthesis 和无剪枝结果一致。
2. 主要阻塞不是 SeedPlan 重计价（约 `1 ms`），而是请求没有到达安装的 root 上界，且该上界在很早阶段已因保守全 Memo facts ReadSet 失效；同时现有 lower-bound admission 对 phase overlap/source response/未完成 child 不足，不能安全闭合证明。
3. 当前差距仍是秒级冷规划到约 `0.11 s` DuckDB C1；所有组都 `BudgetLimited`，距离 10 ms 完整证明还有数量级差距。warm 接近不替代 C1。
4. 下一轮唯一推荐：在现有 `BoundContext`、`TaskRegistry`、`ReadSet` 和冻结契约内，把已验证 root 上界安全地传播到实际耗时最高的一个 child/phase-overlap 子问题，并为该子问题先推导覆盖全部合法完成方案的保守 `NoPlanBelow`；无法证明时继续不剪枝。不要先做全面传播、并行搜索或修改默认停止策略。

验证命令：`cargo fmt --all --check`、`cargo check --locked -p paro-optimizer`、强上界定向测试（1 passed）、release build。benchmark harness tests 此前通过 `126 passed`；本轮没有把工作树其余混合改动或历史 SQL regress 结果改写成 clean。
