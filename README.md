<p align="center">
  <img src="docs/logo/paro_logo.svg" alt="Paro" width="220">
</p>
<p align="center">
  <strong>An AI-native multi-model database built for agents, not just applications.</strong>
  <br />
  Vector search · full-text ranking · graph traversal · sandboxed Python — one SQL engine, one query.
</p>

Paro is a multi-model database written in Rust. It unifies relational, vector, full-text, and graph workloads — no sidecar services, no cross-system glue code. A single SQL statement can walk a social graph, rank documents by keyword relevance, re-rank by embedding similarity, and invoke a sandboxed Python UDF — served from a consistent snapshot while concurrent writers commit under serializable isolation.

> [!WARNING]
> **Paro is currently in beta.**
> Expect rough edges in driver/ORM compatibility, steady-state performance, and on-disk format stability. Best used for evaluation and prototyping today — not production-critical workloads yet.

## Features

- **Multi-model** — structured and semi-structured data as first-class citizens in one engine.
- **Columnar storage** — column-oriented layout optimized for scan-heavy analytical workloads.
- **SIMD execution** — vectorized compute kernels exploiting hardware-level parallelism for distance functions, expression evaluation, and more.
- **SQL-first** — PostgreSQL wire protocol and a familiar SQL dialect; `psql` works well today, while broader driver and ORM compatibility is still being validated.
- **Vector search** — pgvector-compatible operators (`<->`, `<+>`, `<=>`, `<#>`) with HNSW indexing.
- **Full-text search** — `GIN` indexes, `to_tsvector`, `plainto_tsquery`, `ts_rank`, and BM25 ranking.
- **Graph queries** — SQL/PGQ: `CREATE PROPERTY GRAPH`, `GRAPH_TABLE`, multi-hop path traversal.
- **Serializable transactions** — MVCC snapshot reads run lock-free alongside SSI-validated writes; predicate/range locks, `SELECT ... FOR UPDATE`, and savepoints available when needed.
- **Sandboxed Python UDFs** — batch-style handlers with Arrow/NumPy fast-path interop, executed in isolated worker processes — safe for running agent-generated code.

## Quick Start

Clone the repository and enter the project directory, then:

```bash
make run
```

From another terminal, connect with `psql`:

```bash
psql -h 127.0.0.1 -p 6432 -d postgres
```

`make run` compiles and starts `parod` on `127.0.0.1:6432` by default.

> Requires Rust ≥ 1.85. `psql` ≥ 14 is recommended; older versions may work but are not regularly tested.
>
> Override the listen address with `PARO_HOST` / `PARO_PORT` if needed, for example `make run PARO_HOST=0.0.0.0`. Authentication is not implemented yet, so do not expose Paro to untrusted networks.

## Query Example

An agent is researching "retrieval-augmented generation for autonomous agents." The query below walks Alice's collaboration graph up to two hops, pre-filters papers by semantic similarity, then ranks the results with a hybrid score that blends vector proximity and full-text relevance — graph traversal, vector search, and full-text ranking in one statement.

```sql
WITH network AS (
    SELECT * FROM GRAPH_TABLE(collab_graph
        MATCH (me:Researcher WHERE me.name = 'Alice')
              -[:CollaboratesWith]->{1,2}(peer:Researcher)
        COLUMNS (peer.id AS author_id, peer.name AS author_name)
    )
),
candidates AS (
    SELECT
        id,
        title,
        author_id,
        abstract,
        1.0 / (1.0 + (embedding <-> '[0.91, 0.10, 0.80, 0.22]')) AS vec_score
    FROM papers
    ORDER BY embedding <-> '[0.91, 0.10, 0.80, 0.22]'
    LIMIT 20
)
SELECT
    c.title,
    n.author_name,
    c.vec_score
      + ts_rank(
            to_tsvector('simple', c.abstract),
            plainto_tsquery('simple', 'retrieval augmented generation agents')
        ) AS score
FROM network n
JOIN candidates c ON c.author_id = n.author_id
WHERE to_tsvector('simple', c.abstract)
   @@ plainto_tsquery('simple', 'retrieval augmented generation agents')
ORDER BY score DESC
LIMIT 10;
```

One engine, one query — three retrieval models working together, served from a consistent snapshot while writers commit concurrently.

To run the full example locally, load [`regress/cases/example/quickstart.sql`](regress/cases/example/quickstart.sql):

```sql
\i regress/cases/example/quickstart.sql
```

## Python UDFs

Python handlers run in isolated worker processes, separate from the database engine — a safer default for agent-generated code. Python support is optional: `parod` starts and serves ordinary SQL without a Python interpreter.

Project-level entry points:

- `make python-udf-unit` runs ABI/runtime Rust tests plus Python worker and SDK unit tests.
- `make python-udf-startup-smoke` verifies `parod` still starts when the Python runtime is disabled.
- `make python-udf-regress` runs the dedicated SQL gate on the real worker / artifact execution path.
- `make python-udf-ci` runs the dedicated Python UDF unit, startup, and SQL-regress flow end-to-end.

More focused docs live here:

- [`regress/README.md`](regress/README.md) for SQL regression structure and fixture staging
- [`runtimes/python-worker/README.md`](runtimes/python-worker/README.md) for the worker-side loop and tests
- [`python/paro_udf/README.md`](python/paro_udf/README.md) for the user-facing SDK

## Roadmap

1. Performance headroom — Serializable validator overhead, derived-state catch-up SLOs, and AP scan throughput under mixed read/write workloads.
2. AI-native database capabilities — deeper integration of model inference, embedding generation, and agent-friendly query patterns directly inside the engine.
3. Ecosystem integration — potential Supabase compatibility layer and/or a standalone database agent for autonomous data workflows.
4. Expanded sandbox backends — the namespace-process backend ships today; restricted-WASM, mediated, and micro-VM sandbox backends are next on the selector path.

## Contributing

Contributions are welcome. Please open an issue to discuss significant changes before starting work.

Before submitting a pull request:

1. Run the static checks — `make static`
2. Run the full local CI pipeline — `make ci-local`
3. Consider using an AI assistant to review your changes before submitting

## License

Paro is licensed under the [Apache License 2.0](LICENSE).
