# HNSW artifact rebuild required

The August 2026 HNSW storage redesign is intentionally incompatible with
previous artifacts. Recreate every HNSW/vector index from its
`CREATE VECTOR INDEX` definition after upgrading.

The rebuild is required because the durable contract now:

- stores only immutable graph-construction fields, separate from search policy;
- uses the fixed-width HNSW artifact envelope version 2, with distance owned
  solely by the build contract and no JSON in the open path;
- uses the version-2 hybrid CSR graph layout;
- requires HNSW provider-config version 2 and build-contract version 2, which
  select deterministic frozen-wave construction and barrier publication;
- persists per-point cosine inverse norms inside the HNSW artifact.

Search indexes are opened lazily. An old artifact therefore does not prevent
its segment or base table from opening: ordinary SQL remains available, and
vector search degrades to the exact scan path. The lazy segment capability
retains a rebuild reason and search telemetry reports the degraded capability
when the stale inline artifact is first opened, so
`DROP INDEX` followed by `CREATE VECTOR INDEX` remains an available recovery
path on the upgraded binary. Current-format checksum or structural corruption
is still a hard error when that search capability is used.

Paro does not infer missing fields, translate the previous graph format, or
silently rebuild an index during a foreground query.

FullText and Sparse definitions also gained strict provider-config version 1
in the same release. Recreate catalog definitions that still use unversioned
`{"config": ...}` or `{"physical_encoding": ...}` payloads; unknown and
missing fields are now rejected instead of defaulted by individual consumers.
