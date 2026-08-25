# HNSW artifact rebuild required

The August 2026 HNSW storage redesign is intentionally incompatible with
previous artifacts. Recreate every HNSW/vector index from its
`CREATE VECTOR INDEX` definition after upgrading.

The rebuild is required because the durable contract now:

- stores only immutable graph-construction fields, separate from search policy;
- uses the fixed-width HNSW artifact envelope version 9, with distance owned
  solely by the build contract, no JSON in the open path, and an authenticated
  hierarchy of 4 KiB payload checksums, 4 KiB checksum pages, and a compact
  root directory for lazy random-access integrity verification;
- uses the version-2 hybrid CSR graph layout;
- requires HNSW provider-config version 10 and build-contract version 10, which
  select deterministic frozen-wave construction, keyed Feistel point ordering,
  barrier publication, exact predicate-local covering runs, and the full
  configured construction beam on every graph layer;
- persists per-point cosine inverse norms inside the HNSW artifact;
- stores inline HNSW pages without block compression so graph links, inverse
  norms, and predicate topology open directly over the immutable segment mmap
  instead of pinning one page-cache allocation per segment. Inline and sidecar
  HNSW readers validate only metadata at open and authenticate graph chunks
  immediately before first use, rather than faulting the complete artifact for
  a monolithic page/package checksum.

Frozen-wave boundaries are durable implementation details rather than SQL
tuning policy. `proposal_wave_size` and `warmup_point_count` are therefore no
longer accepted by `CREATE VECTOR INDEX`; changing either requires a new build
contract and an index rebuild.

Search indexes are opened lazily. An old artifact therefore does not prevent
its segment or base table from opening: ordinary SQL remains available, and
vector search degrades to the exact scan path. The lazy segment capability
retains a rebuild reason and search telemetry reports the degraded capability
when the stale inline artifact is first opened, so
`DROP INDEX` followed by `CREATE VECTOR INDEX` remains an available recovery
path on the upgraded binary. `CREATE VECTOR INDEX` now materializes every
visible segment through the governed maintenance path before the catalog entry
can become `READY`; an admitted build that makes no coverage progress fails
explicitly instead of publishing a permanently incomplete index.
Current-format checksum or structural corruption is still a hard error when
that search capability is used.

Paro does not infer missing fields, translate the previous graph format, or
silently rebuild an index during a foreground query.

FullText and Sparse definitions also gained strict provider-config version 1
in the same release. Recreate catalog definitions that still use unversioned
`{"config": ...}` or `{"physical_encoding": ...}` payloads; unknown and
missing fields are now rejected instead of defaulted by individual consumers.
