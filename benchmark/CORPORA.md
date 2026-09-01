# Optimizer correctness corpora

The optimizer correctness sequence uses source repositories kept as siblings of
the Paro checkout. Set the workspace root once when running the corpora:

```sh
export PARO_WORKSPACE=/Users/linjunhong/workspace
```

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

Every Paro connection used by this gate enables `optimizer_verify`. A suite is
complete only after each query executes and its decoded row multiset matches
the authoritative result. `EXPLAIN`-only success is not a correctness result.

CEB targets PostgreSQL semantics. In particular, PostgreSQL widens
`REAL`/`NUMERIC` comparisons to double precision, while DuckDB narrows the
numeric operand to `FLOAT`. When DuckDB is used as the CEB execution oracle,
rewrite an explicit `value::float` operand to `value::float::double` on the
DuckDB side only. The Paro query remains unchanged.
