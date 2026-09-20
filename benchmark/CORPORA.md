# Optimizer correctness corpora

The optimizer correctness sequence uses external source repositories. Resolve
the selected Paro checkout from the user's task, not a fixed main-worktree
path. Set `PARO_WORKSPACE` to the verified directory containing the corpus
repositories below; an isolated Paro worktree need not share their parent.
Inspect source/status and use an owned test instance before building or loading
data. Preserve user changes and reuse agreed isolated checkouts.

| Corpus | Source repository | Query location |
| --- | --- | --- |
| JOB | `$PARO_WORKSPACE/duckdb` | `benchmark/imdb_plan_cost/queries` with answers in `benchmark/imdb/answers` |
| CEB | `$PARO_WORKSPACE/imdb-pg-dataset` | `ceb-imdb-3k` |
| CEB generation and metadata | `$PARO_WORKSPACE/ceb` | `templates` and `queries` |
| TPC-DS | `$PARO_WORKSPACE/duckdb` | `benchmark/tpcds` |
| TPC-H | `$PARO_WORKSPACE/duckdb` and this repository | DuckDB `benchmark/tpch`; Paro `benchmark/workloads/tpch` |
| LDBC SNB BI | `$PARO_WORKSPACE/duckdb` | `benchmark/ldbc/queries/bi-*.sql` |
| GRASP reference workloads | `$PARO_WORKSPACE/grasp` | `queries` |

Repository checkouts contain queries, schemas, generators, and expected
answers. Generated database files, exported CSV/Parquet data, server data
directories, logs, and comparison output stay outside every source repository.
Use an explicit data root such as `/tmp/paro-corpora` or another disposable
volume; never commit generated corpus data.

Run the correctness suites strictly in this order:

1. JOB
2. CEB
3. TPC-DS
4. TPC-H
5. LDBC SNB BI

Every Paro connection used by this gate enables `optimizer_verify`; record the
effective setting, rather than assuming a trace-off run has verification off.
A suite is complete only after each query executes and its decoded row multiset
matches the authoritative result. `EXPLAIN`-only success is not a correctness
result.

For a paired TPC-DS SF1 correctness and latency comparison, use
`corpora/tpcds_compare.py`. The harness builds the tested Paro image itself and
owns every server process. Each block uses a fresh Paro server and a fresh
spawned DuckDB process. Within the block, configurable measurement rounds use
independently seeded-random ABBA order, so an isolated scheduling outlier cannot
stand in for a whole process while the process remains the outer resampling
unit. The harness applies the same thread and memory limits and validates the
exact result schema plus the complete row multiset outside every timed sample.
The report records source, build, corpus, DDL, database, optimizer-visible key
inventories, processes, round orders, and per-sample result digests.

Pass `--metadata-track none` for a qualifying engine comparison. The
`generator-declared` track remains useful for measuring Paro's best known plan,
but it is marked non-qualifying unless DuckDB exposes the same live key
inventory. Generated SF1 database and CSV files belong under the workspace data
root, outside source repositories.

For a controlled performance claim, first read the repository's
[paro-evidence workflow](../.agents/skills/paro-evidence/SKILL.md) and
[comparison/evidence contract](../crates/optimizer/readme.md#comparison-validity).
Read the collector's live `--help` from the selected Python environment instead
of copying options from another worktree. Pin DuckDB's actual build and extension
identities as well as its version, and preregister the sample unit, thresholds
and resource envelope before confirmatory collection. Retain all valid slow
samples and explicit failures/exclusions. Target occurrence zero, cache state,
normal versus diagnostic cohorts and receipt association are distinct facts.
Current collectors do not yet implement the complete bounded
`EXPLAIN (COMPILE)` campaign contract; do not claim that capability from this
document or infer missing normal receipts from legacy trace data. Archiving,
baseline updates and cleanup are not automatic parts of a comparison run.

CEB targets PostgreSQL semantics. In particular, PostgreSQL widens
`REAL`/`NUMERIC` comparisons to double precision, while DuckDB narrows the
numeric operand to `FLOAT`. When DuckDB is used as the CEB execution oracle,
rewrite an explicit `value::float` operand to `value::float::double` on the
DuckDB side only. The Paro query remains unchanged.
