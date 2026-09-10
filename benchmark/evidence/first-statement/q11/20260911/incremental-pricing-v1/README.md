# Q11 candidate-combination incremental pricing

本轮只修改候选组合级增量定价。代码提交为 `7d86a010`；没有修改默认停止策略、搜索预算、成本模型、B&B 或并行搜索，也没有 Q11 特判。

## Measurement contract

- 原始 Q11、binary protocol、4 execution threads、2 GiB、同一只读 seed 的 private copy。
- normal cohort 为 trace-off、fresh process、cache miss、完整 typed result/schema/order/digest 校验；diagnostic cohort 单独 trace-on，仅用于阶段和计数归因，不进入 C1/W。
- DuckDB 使用同一 SQL、数据文件、线程和内存限制。
- 每个档位 5 个 fresh process blocks、每 block 1 个冷样本和 1 个 warmup 后 ABBA 测量轮；bootstrap 为 10,000 次。
- `PARO_DIAGNOSTIC_SEARCH_STOP_MS` 只用于 20/50 ms 诊断停止实验；默认档位未设置该变量。

## Results

| 搜索档位 | Paro C1 / DuckDB C1 | C1 ratio（95% CI） | Paro warm / DuckDB warm | warm ratio（95% CI） | 实际停止 / 状态 |
| --- | ---: | ---: | ---: | ---: | --- |
| 20 ms | 174.932 / 109.619 ms | 1.588（1.557–1.616） | 110.157 / 105.749 ms | 1.051（1.034–1.077） | 20,010 µs / Deadline |
| 50 ms | 220.162 / 138.188 ms | 1.770（1.649–1.940） | 127.302 / 117.750 ms | 0.976（0.891–1.086） | 50,000 µs / Deadline |
| default | 1,524.124 / 111.708 ms | 13.562（13.353–13.708） | 97.086 / 105.928 ms | 0.916（0.900–0.933） | 1,341,693 µs / BudgetLimited |

所有正常样本都通过 cache-miss、trace-off 和完整结果校验；结果摘要均为
`9108a5b43530c24778abc0d8d3bf1ee218a9815eb0d15310bb4d7c6bcc5f2c78`。

## Combination counters and handoff

计数来自同次 diagnostic trace 的 search-work counters；`new` 包含 mandatory baseline，`synthesis` 是实际成本合成次数，`recompute` 是成本上下文改变后需要重新定价的已缓存条目，`frontier_recheck` 不重新合成成本。

| 档位 | new | recompute | cost synthesis | frontier recheck | budget rejection | interned event | working-set bytes | freeze / handoff µs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 20 ms | 746 | 93 | 746 | 219 | 0 | 257 | 53,416,448 | 233 / 456 |
| 50 ms | 1,588 | 111 | 1,588 | 1,020 | 0 | 631 | 53,416,448 | 285 / 504 |
| default | 25,474 | 5,084 | 25,473 | 29,616 | 1 | 13,149 | 45,713,728 | 369 / 649 |

`working-set bytes` 是诊断 trace 的资源交接值，不是独立 RSS 采样。冻结候选数三档均为 6；20/50 ms 的 search profile stop/return 分别为 `19,312/19,342` 与 `49,331/49,360 µs`，default 为 `1,340,877/1,340,903 µs`。三档均为 `search_complete=0`。

## Interpretation

实现满足组合身份、增量域、暂停恢复和上下文失效的工程契约；独立 oracle 位于 optimizer engine tests。本轮默认诊断实际完成了 25,473 次成本合成；历史基线报告记录的 55,022 是 cost proposal 次数，旧报告没有拆出同口径的 synthesis，因此两者不能直接宣称倍数收益。该计数变化也没有把完整冷 C1 变成 10–20 ms：default 仍被大量搜索/决策 closure 主导，20 ms 的 C1 约 175 ms，50 ms 本轮约 220 ms。warm 结果显示 20 ms 候选接近当前执行质量，50 ms 的绝对样本受同轮 DuckDB/Paro 尾部波动影响，CI 也未证明优于 DuckDB。

因此本轮不改变默认停止策略，也不宣称性能目标或 M1–M3 已达成。下一瓶颈是减少早期候选之后的高价值决策/closure，并继续用真实停止后直接执行的 C1 验收；不应以计数下降、candidate ID、EXPLAIN 或 warm ratio 替代它。

## Files and hashes

- `q11-20ms.json`: `40ab299f112b3563506d16cbb0bd845d090fbe6ad2c39cb79ffb9d62ef61fffc`
- `q11-50ms.json`: `4980edfe34ded784e2af02c40242f4321bc717e842f8231f57db39328098faab`
- `q11-default.json`: `eadfbcabb46c806365a54f7c78f51a2eb166f02f63bff3a203876e9fc7325781`
- tested `target/release/parod`: `4489848d1bd2c2d73b4c462219964b4989288062e23a0d7a83551b9b834a0004`

JSON reports and ignored raw `.parod.log` files are kept together in this directory. The report build attestation records the pre-existing unrelated dirty files that were deliberately preserved.
