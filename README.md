<p align="center">
  <img src="docs/logo/paro_logo.svg" alt="Paro" width="220">
</p>
<p align="center">
  <strong>An AI-native analytical database built for agents, not just applications.</strong>
  <br />
  Vector search · full-text ranking · graph traversal — one SQL engine, one process.
</p>

Paro is a multi-model analytical database written in Rust. It unifies relational, vector, full-text, and graph workloads inside a single columnar engine — no sidecar services, no cross-system glue code. An agent can walk a social graph, rank documents by keyword relevance, and re-rank by embedding similarity in one SQL statement, against one process.

> [!WARNING]
> **Paro is currently in beta.**
> The project is still under active development and testing. Expect rough edges, incomplete coverage, and breaking changes across SQL features, performance characteristics, and on-disk behavior. It is best used for evaluation, prototyping, and early feedback today — not production-critical workloads yet.

## Features

- **Multi-model** — structured and semi-structured data as first-class citizens in one engine.
- **Columnar storage** — column-oriented layout optimized for scan-heavy analytical workloads.
- **SIMD execution** — vectorized compute kernels exploiting hardware-level parallelism for distance functions, expression evaluation, and more.
- **SQL-first** — PostgreSQL wire protocol and a familiar SQL dialect; `psql` works well today, while broader driver and ORM compatibility is still being validated.
- **Vector search** — pgvector-compatible operators (`<->`, `<+>`, `<=>`, `<#>`) with HNSW indexing.
- **Full-text search** — `GIN` indexes, `to_tsvector`, `plainto_tsquery`, `ts_rank`, and BM25 ranking.
- **Graph queries** — SQL/PGQ: `CREATE PROPERTY GRAPH`, `GRAPH_TABLE`, multi-hop path traversal.

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

One engine, one query — three retrieval models working together.

To run the full example locally, load [`regress/cases/example/quickstart.sql`](regress/cases/example/quickstart.sql):

```sql
\i regress/cases/example/quickstart.sql
```

## Roadmap

1. Performance and stability — hardening the storage engine, query optimizer, and execution pipeline for production-grade reliability.
2. Explore AI-native database capabilities — deeper integration of model inference, embedding generation, and agent-friendly query patterns directly inside the engine.
3. Ecosystem integration — potential Supabase compatibility layer and/or a standalone database agent for autonomous data workflows.

## Contributing

Contributions are welcome. Please open an issue to discuss significant changes before starting work.

Before submitting a pull request:

1. Run the full regression suite — `make ci-local`
2. Consider using an AI assistant to review your changes before submitting

## License

Paro is licensed under the [Apache License 2.0](LICENSE).
