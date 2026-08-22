# HNSW artifact rebuild required

The August 2026 HNSW storage redesign is intentionally incompatible with
previous artifacts. Before starting a binary containing this change, drop and
recreate every HNSW/vector index from its `CREATE VECTOR INDEX` definition.

The rebuild is required because the durable contract now:

- stores only immutable graph-construction fields, separate from search policy;
- uses a versioned HNSW artifact envelope with distance owned solely by the
  build contract;
- uses the version-2 hybrid CSR graph layout;
- requires a strict, versioned provider configuration; and
- persists per-point cosine inverse norms inside the HNSW artifact.

Old artifacts are rejected during recovery or index open. Paro does not infer
missing fields, translate the previous graph format, or silently rebuild an
index during a foreground query.

FullText and Sparse definitions also gained strict provider-config version 1
in the same release. Recreate catalog definitions that still use unversioned
`{"config": ...}` or `{"physical_encoding": ...}` payloads; unknown and
missing fields are now rejected instead of defaulted by individual consumers.
