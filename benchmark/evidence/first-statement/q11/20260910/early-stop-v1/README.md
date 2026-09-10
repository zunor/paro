# Q11 early-stop handoff evidence

本目录是最终代码的早期候选 → 停止 → 冻结 → 执行复测。所有报告均为
TPC-DS SF1 原始 Q11、5 个 fresh process blocks、binary 协议、normal cohort
trace-off、cache miss 和完整 typed result 校验；每个 block 从同一只读 seed
复制私有数据目录。每份报告的独立 diagnostic cohort 只有 1 个 trace-on
sample，明确排除在 C1 统计之外。

`PARO_DIAGNOSTIC_SEARCH_STOP_MS` 只是诊断/实验开关；未设置时仍使用原有
默认搜索政策。本轮没有把任何 checkpoint candidate 回放成 C1。

| 搜索设置 | Paro C1 median | DuckDB C1 median | C1 ratio (95% CI) | Paro warm median | warm ratio | stop / BudgetLimited | freeze / handoff | timeout tail |
| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| 0 ms baseline | 420.062 ms | 110.018 ms | 3.717 (3.522–3.883) | 361.231 ms | 3.346 | Deadline / no | 101 / 483 µs | 1 µs |
| 20 ms | 177.745 ms | 109.936 ms | 1.509 (1.387–1.621) | 111.772 ms | 1.026 | Deadline / no | 240 / 469 µs | 30 µs |
| 50 ms | 194.171 ms | 109.427 ms | 1.835 (1.740–2.014) | 99.544 ms | 0.914 | Deadline / yes | 293 / 572 µs | 43 µs |
| 100 ms | 249.479 ms | 112.629 ms | 2.232 (2.179–2.272) | 96.611 ms | 0.912 | Deadline / yes | 289 / 540 µs | 34 µs |
| default/full | 1452.006 ms | 114.837 ms | 12.867 (12.635–13.121) | 99.114 ms | 0.920 | BudgetLimited / yes | 353 / 638 µs | 26 µs |

停止点按 SearchControl 的查询时钟记录，包含 mandatory incumbent 构造：

- 0/20/50/100 ms 的配置 deadline 分别为 `0/20000/50000/100000 µs`，首次
  观察到停止的 `actual_stop_us` 分别为 `2038/20002/50013/100000`。
- 默认搜索配置为 `30000000 µs`，本次在 `1291418 µs` 因确定性搜索预算
  `BudgetLimited` 返回；它不是 `Complete`，也不是 deadline timeout。
- 20/50/100 ms 的 diagnostic search profile 停止/返回分别约为
  `19103/19133`、`49537/49580`、`99315/99349 µs`；尾部是返回前未继续
  搜索的收口工作，不应并入目标 deadline。

五份报告所有 Paro normal 样本和 DuckDB 对照都得到相同结果摘要：
`9108a5b43530c24778abc0d8d3bf1ee218a9815eb0d15310bb4d7c6bcc5f2c78`。
因此早停没有牺牲 SQL 结果正确性。阶段目标“50 ms 内获得接近最终方案的
执行质量并使 C1 明显下降”只部分成立：C1 相对完整搜索明显下降，但 50 ms
的 C1 仍约为 DuckDB 的 1.84 倍，不能宣称追平；warm 质量接近不替代 C1
验收。

报告文件为 `q11-initial-baseline-0ms.json`、`q11-early-20ms.json`、
`q11-early-50ms.json`、`q11-early-100ms.json` 和 `q11-full-search.json`；
同名 `.q11.*.parod.log` 是原始 oracle、normal blocks 和 diagnostic 日志。
`11.sql` 是本次原始查询副本。诊断字段中的 `frozen_candidate_count=3`
（baseline）或 `6`（其余样本）只用于核对冻结交接，不是性能验收指标。
